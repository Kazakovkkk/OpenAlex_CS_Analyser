package ingest

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"math"
	"time"

	"go-etl/internal/metrics"
	"go-etl/internal/pipeline"
	"go-etl/internal/sink"
)

type Entity string

const (
	EntityWorks  Entity = "works"
	EntityTopics Entity = "topics"
)

// Portion is one buffer handed from a worker to the single writer. Ownership of
// Works/Topics transfers with the portion: the writer always returns it to the
// pool, even when it drops the portion after an error.
type Portion struct {
	Entity    Entity
	ObjectKey string
	Attempt   int
	Works     *sink.WorksBatch
	Topics    *sink.TopicsBatch
	EOF       bool
	Err       error
}

// CheckpointLedger is the part of pipeline.CheckpointStore the writer needs.
type CheckpointLedger interface {
	MarkDone(ctx context.Context, entity, objectKey string) error
	Has(ctx context.Context, entity, objectKey string) (bool, error)
}

// WriterOptions configures the single writer goroutine.
type WriterOptions struct {
	Sink            sink.BatchSink
	Checkpoints     CheckpointLedger
	KnownIDs        func(workID uint64)
	Metrics         *metrics.JobMetrics
	Pool            *sink.WorksBatchPool
	WorksBatchSize  int
	TopicsBatchSize int
	Logger          *slog.Logger
	FinalTimeout    time.Duration
}

type fileProgress struct {
	entity  Entity
	key     string
	pending int64
	eof     bool
	done    bool
}

type rowVersion struct {
	id      uint64
	updated int64
}

type Writer struct {
	opts WriterOptions

	works  *sink.WorksBatch
	topics *sink.TopicsBatch
	// accFiles mirrors the rows of the accumulators: the file each row belongs
	// to, so an acknowledged INSERT can settle its checkpoint.
	workFiles  []string
	topicFiles []string

	files map[string]*fileProgress
	seen  map[rowVersion]uint64

	result Result
	err    error

	finishCtx    context.Context
	finishCancel context.CancelFunc
}

func NewWriter(opts WriterOptions) (*Writer, error) {
	if opts.Sink == nil {
		return nil, errors.New("ingest: writer needs a sink")
	}
	if opts.WorksBatchSize <= 0 {
		opts.WorksBatchSize = sink.DefaultBatchCap
	}
	if opts.TopicsBatchSize <= 0 {
		opts.TopicsBatchSize = 5_000
	}
	if opts.Pool == nil {
		opts.Pool = sink.NewWorksBatchPool(opts.WorksBatchSize, sink.DefaultMaxPooledCap)
	}
	if opts.FinalTimeout <= 0 {
		opts.FinalTimeout = 60 * time.Second
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Writer{
		opts:   opts,
		works:  opts.Pool.Get(),
		topics: sink.NewTopicsBatch(opts.TopicsBatchSize),
		files:  make(map[string]*fileProgress, 64),
		seen:   make(map[rowVersion]uint64, 1<<20),
		result: Result{},
	}, nil
}

func (w *Writer) Result() Result { return w.result }

// Run consumes portions until the coordinator closes the channel. A cancelled
// runCtx starts a 60-second completion context: the writer keeps draining,
// finishes the current insert, flushes the remainder and settles only fully
// read files.
func (w *Writer) Run(runCtx context.Context, portions <-chan Portion) (Result, error) {
	if runCtx.Err() != nil && w.finishCtx == nil {
		w.startFinish(runCtx)
	}
	for p := range portions {
		if runCtx.Err() != nil && w.finishCtx == nil {
			w.startFinish(runCtx)
		}
		if w.err != nil {
			w.release(p)
			continue
		}
		if err := w.handle(runCtx, p); err != nil {
			w.err = err
		}
	}
	if w.err != nil {
		w.discardAccumulators()
		w.result.FilesDone, w.result.FilesSkipped = w.fileCounts()
		return w.result, w.err
	}
	ctx := w.writeCtx(runCtx)
	if err := w.flushWorks(ctx); err != nil {
		w.err = err
		w.discardAccumulators()
		w.result.FilesDone, w.result.FilesSkipped = w.fileCounts()
		return w.result, err
	}
	if err := w.flushTopics(ctx); err != nil {
		w.err = err
		w.discardAccumulators()
		w.result.FilesDone, w.result.FilesSkipped = w.fileCounts()
		return w.result, err
	}
	if err := w.settleCheckpoints(ctx); err != nil {
		w.err = err
	}
	w.result.FilesDone, w.result.FilesSkipped = w.fileCounts()
	return w.result, w.err
}

func (w *Writer) startFinish(runCtx context.Context) {
	base := context.WithoutCancel(runCtx)
	w.finishCtx, w.finishCancel = context.WithTimeout(base, w.opts.FinalTimeout)
}

func (w *Writer) writeCtx(runCtx context.Context) context.Context {
	if w.finishCtx != nil {
		return w.finishCtx
	}
	return runCtx
}

func (w *Writer) release(p Portion) {
	if p.Works != nil {
		w.opts.Pool.Put(p.Works)
	}
	// Topics are garbage-collected; nothing to return.
}

func (w *Writer) discardAccumulators() {
	if w.works != nil {
		w.opts.Pool.Put(w.works)
		w.works = nil
	}
	w.topics = nil
	w.workFiles = nil
	w.topicFiles = nil
}

func (w *Writer) handle(runCtx context.Context, p Portion) error {
	if p.Err != nil {
		return p.Err
	}
	file := w.file(p.Entity, p.ObjectKey)
	if p.EOF {
		file.eof = true
		if file.pending == 0 && !file.done {
			if err := w.mark(file, w.writeCtx(runCtx)); err != nil {
				return err
			}
		}
		return nil
	}
	switch p.Entity {
	case EntityWorks:
		if p.Works == nil {
			return nil
		}
		err := w.appendWorks(runCtx, file, p)
		w.opts.Pool.Put(p.Works)
		return err
	case EntityTopics:
		if p.Topics == nil {
			return nil
		}
		err := w.appendTopics(runCtx, file, p)
		p.Topics = nil
		return err
	default:
		return fmt.Errorf("ingest: unknown portion entity %q", p.Entity)
	}
}

func (w *Writer) file(entity Entity, key string) *fileProgress {
	k := string(entity) + "|" + key
	if f, ok := w.files[k]; ok {
		return f
	}
	f := &fileProgress{entity: entity, key: key}
	w.files[k] = f
	return f
}

func (w *Writer) appendWorks(runCtx context.Context, file *fileProgress, p Portion) error {
	b := p.Works
	for i := 0; i < b.Len(); i++ {
		rv := rowVersion{id: b.WorkIDs[i], updated: b.UpdatedDates[i].UnixMicro()}
		hash := fingerprintWorksRow(b, i)
		if prev, ok := w.seen[rv]; ok {
			if prev != hash {
				return fmt.Errorf("%w: work_id %d at %s has conflicting content",
					errCorpusConflict, rv.id, b.UpdatedDates[i].UTC().Format(time.RFC3339Nano))
			}
			w.result.DedupDropped++
			continue
		}
		w.seen[rv] = hash
		file.pending++
		w.copyWorksRow(b, i)
		w.workFiles = append(w.workFiles, string(file.entity)+"|"+file.key)
		if w.works.Len() >= w.opts.WorksBatchSize {
			if err := w.flushWorks(w.writeCtx(runCtx)); err != nil {
				return err
			}
		}
	}
	return nil
}

func (w *Writer) appendTopics(runCtx context.Context, file *fileProgress, p Portion) error {
	b := p.Topics
	for i := 0; i < b.Len(); i++ {
		file.pending++
		w.topics.TopicIDs = append(w.topics.TopicIDs, b.TopicIDs[i])
		w.topics.DisplayNames = append(w.topics.DisplayNames, b.DisplayNames[i])
		w.topics.Descriptions = append(w.topics.Descriptions, b.Descriptions[i])
		w.topics.SubfieldIDs = append(w.topics.SubfieldIDs, b.SubfieldIDs[i])
		w.topics.SubfieldNames = append(w.topics.SubfieldNames, b.SubfieldNames[i])
		w.topics.FieldIDs = append(w.topics.FieldIDs, b.FieldIDs[i])
		w.topics.FieldNames = append(w.topics.FieldNames, b.FieldNames[i])
		w.topics.DomainIDs = append(w.topics.DomainIDs, b.DomainIDs[i])
		w.topics.DomainNames = append(w.topics.DomainNames, b.DomainNames[i])
		w.topics.WorksCounts = append(w.topics.WorksCounts, b.WorksCounts[i])
		w.topics.CitedByCounts = append(w.topics.CitedByCounts, b.CitedByCounts[i])
		w.topics.UpdatedDates = append(w.topics.UpdatedDates, b.UpdatedDates[i])
		w.topicFiles = append(w.topicFiles, string(file.entity)+"|"+file.key)
		if w.topics.Len() >= w.opts.TopicsBatchSize {
			if err := w.flushTopics(w.writeCtx(runCtx)); err != nil {
				return err
			}
		}
	}
	return nil
}

func (w *Writer) flushWorks(ctx context.Context) error {
	if w.works == nil || w.works.Len() == 0 {
		return nil
	}
	n := w.works.Len()
	approxBytes := w.works.ApproxBytes()
	start := time.Now()
	if err := w.opts.Sink.WriteWorksBatch(ctx, w.works); err != nil {
		return err
	}
	w.recordBatch(start, n, approxBytes)
	w.result.Written += uint64(n)
	w.result.WorksWritten += uint64(n)
	for _, fileKey := range w.workFiles {
		w.settleRow(fileKey)
	}
	for _, id := range w.works.WorkIDs {
		if w.opts.KnownIDs != nil {
			w.opts.KnownIDs(id)
		}
	}
	w.works.Reset()
	w.workFiles = w.workFiles[:0]
	return w.settleCheckpoints(ctx)
}

func (w *Writer) flushTopics(ctx context.Context) error {
	if w.topics == nil || w.topics.Len() == 0 {
		return nil
	}
	n := w.topics.Len()
	approxBytes := w.topics.ApproxBytes()
	start := time.Now()
	if err := w.opts.Sink.WriteTopicsBatch(ctx, w.topics); err != nil {
		return err
	}
	w.recordBatch(start, n, approxBytes)
	w.result.Written += uint64(n)
	w.result.TopicsWritten += uint64(n)
	for _, fileKey := range w.topicFiles {
		w.settleRow(fileKey)
	}
	w.topics.Reset()
	w.topicFiles = w.topicFiles[:0]
	return w.settleCheckpoints(ctx)
}

func (w *Writer) recordBatch(start time.Time, rows int, bytes int64) {
	w.result.BatchesWritten++
	if w.opts.Metrics != nil {
		w.opts.Metrics.RecordBatchDuration(time.Since(start))
		w.opts.Metrics.AddWritten(uint64(rows), uint64(max(bytes, 0)))
	}
}

func (w *Writer) settleRow(fileKey string) {
	f, ok := w.files[fileKey]
	if !ok {
		return
	}
	if f.pending > 0 {
		f.pending--
	}
}

// settleCheckpoints marks every file that reached EOF and has no unacknowledged
// rows. A failed checkpoint write is re-read from the ledger; if the mark cannot
// be confirmed, the job fails.
func (w *Writer) settleCheckpoints(ctx context.Context) error {
	for _, f := range w.files {
		if f.done || !f.eof || f.pending != 0 {
			continue
		}
		if err := w.mark(f, ctx); err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) mark(f *fileProgress, ctx context.Context) error {
	if w.opts.Checkpoints == nil {
		f.done = true
		return nil
	}
	if err := w.opts.Checkpoints.MarkDone(ctx, string(f.entity), f.key); err != nil {
		ok, herr := w.opts.Checkpoints.Has(ctx, string(f.entity), f.key)
		if herr != nil || !ok {
			return fmt.Errorf("ingest: checkpoint %s/%s was not confirmed: %w", f.entity, f.key, err)
		}
	}
	f.done = true
	return nil
}

func (w *Writer) fileCounts() (done, skipped int) {
	for _, f := range w.files {
		if f.done {
			done++
		} else if f.eof {
			skipped++
		}
	}
	return done, skipped
}

func (w *Writer) copyWorksRow(b *sink.WorksBatch, i int) {
	w.works.WorkIDs = append(w.works.WorkIDs, b.WorkIDs[i])
	w.works.UpdatedDates = append(w.works.UpdatedDates, b.UpdatedDates[i])
	w.works.WorkTypes = append(w.works.WorkTypes, b.WorkTypes[i])
	w.works.PrimaryFieldIDs = append(w.works.PrimaryFieldIDs, b.PrimaryFieldIDs[i])
	w.works.IsRetracted = append(w.works.IsRetracted, b.IsRetracted[i])
	w.works.PubYears = append(w.works.PubYears, b.PubYears[i])
	if b.PubDates[i] != nil {
		w.works.PubDates = append(w.works.PubDates, w.works.AllocPubDate(*b.PubDates[i]))
	} else {
		w.works.PubDates = append(w.works.PubDates, nil)
	}
	w.works.PrimaryTopicIDs = append(w.works.PrimaryTopicIDs, b.PrimaryTopicIDs[i])
	w.works.TopicIDs = append(w.works.TopicIDs, append([]uint32(nil), b.TopicIDs[i]...))
	w.works.TopicScores = append(w.works.TopicScores, append([]float32(nil), b.TopicScores[i]...))
	w.works.PrimarySourceIDs = append(w.works.PrimarySourceIDs, b.PrimarySourceIDs[i])
	w.works.PrimarySourceTypes = append(w.works.PrimarySourceTypes, b.PrimarySourceTypes[i])
	if b.FWCIs[i] != nil {
		w.works.FWCIs = append(w.works.FWCIs, w.works.AllocFWCI(*b.FWCIs[i]))
	} else {
		w.works.FWCIs = append(w.works.FWCIs, nil)
	}
}

var errCorpusConflict = fmt.Errorf("%w: conflicting versions of one work", pipeline.ErrCorpusIncomplete)

func fingerprintWorksRow(b *sink.WorksBatch, i int) uint64 {
	h := fnv.New64a()
	var buf [8]byte
	put := func(v uint64) {
		binary.LittleEndian.PutUint64(buf[:], v)
		_, _ = h.Write(buf[:])
	}
	putStr := func(s string) {
		put(uint64(len(s)))
		_, _ = h.Write([]byte(s))
	}
	put(b.WorkIDs[i])
	put(uint64(b.UpdatedDates[i].UnixMicro()))
	putStr(b.WorkTypes[i])
	put(uint64(b.PrimaryFieldIDs[i]))
	put(uint64(b.IsRetracted[i]))
	put(uint64(b.PubYears[i]))
	if b.PubDates[i] != nil {
		put(uint64(b.PubDates[i].Unix()))
	} else {
		put(0)
	}
	put(uint64(b.PrimaryTopicIDs[i]))
	put(uint64(len(b.TopicIDs[i])))
	for j, id := range b.TopicIDs[i] {
		put(uint64(id))
		var s uint32
		if j < len(b.TopicScores[i]) {
			s = math.Float32bits(b.TopicScores[i][j])
		}
		put(uint64(s))
	}
	put(b.PrimarySourceIDs[i])
	putStr(b.PrimarySourceTypes[i])
	if b.FWCIs[i] != nil {
		put(uint64(math.Float32bits(*b.FWCIs[i])))
		put(1)
	} else {
		put(0)
	}
	put(0)
	return h.Sum64()
}
