package ingest

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go-etl/internal/pipeline"
	"go-etl/internal/sink"
)

type memLedger struct {
	mu   sync.Mutex
	done map[string]bool
}

func newMemLedger() *memLedger { return &memLedger{done: map[string]bool{}} }

func (l *memLedger) MarkDone(_ context.Context, entity, key string) error {
	l.mu.Lock()
	l.done[entity+"|"+key] = true
	l.mu.Unlock()
	return nil
}

func (l *memLedger) Has(_ context.Context, entity, key string) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.done[entity+"|"+key], nil
}

func (l *memLedger) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.done)
}

type recordingSink struct {
	mu      sync.Mutex
	works   [][]uint64
	failOn  int
	calls   int
	topics  int
}

func (s *recordingSink) WriteWorksBatch(_ context.Context, b *sink.WorksBatch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.failOn > 0 && s.calls == s.failOn {
		return errors.New("insert failed")
	}
	ids := append([]uint64(nil), b.WorkIDs...)
	s.works = append(s.works, ids)
	return nil
}

func (s *recordingSink) WriteTopicsBatch(_ context.Context, b *sink.TopicsBatch) error {
	s.mu.Lock()
	s.topics += b.Len()
	s.mu.Unlock()
	return nil
}

func (s *recordingSink) WriteInstitutionsBatch(context.Context, *sink.InstitutionsBatch) error {
	return nil
}

func (s *recordingSink) Close() error { return nil }

func worksPortion(key string, updated time.Time, typ string, ids ...uint64) Portion {
	b := sink.NewWorksBatch(len(ids))
	for _, id := range ids {
		b.WorkIDs = append(b.WorkIDs, id)
		b.UpdatedDates = append(b.UpdatedDates, updated)
		b.WorkTypes = append(b.WorkTypes, typ)
		b.PrimaryFieldIDs = append(b.PrimaryFieldIDs, 17)
		b.IsRetracted = append(b.IsRetracted, 0)
		b.PubYears = append(b.PubYears, 2024)
		b.PubDates = append(b.PubDates, nil)
		b.PrimaryTopicIDs = append(b.PrimaryTopicIDs, 1)
		b.TopicIDs = append(b.TopicIDs, []uint32{1})
		b.TopicScores = append(b.TopicScores, []float32{0.5})
		b.PrimarySourceIDs = append(b.PrimarySourceIDs, 0)
		b.PrimarySourceTypes = append(b.PrimarySourceTypes, "")
		b.FWCIs = append(b.FWCIs, nil)
	}
	return Portion{Entity: EntityWorks, ObjectKey: key, Attempt: 1, Works: b}
}

func eofPortion(key string) Portion {
	return Portion{Entity: EntityWorks, ObjectKey: key, Attempt: 1, EOF: true}
}

func TestWriterMergesFilesIntoLargerBatch(t *testing.T) {
	t.Parallel()

	snk := &recordingSink{}
	ledger := newMemLedger()
	w, err := NewWriter(WriterOptions{Sink: snk, Checkpoints: ledger, WorksBatchSize: 4})
	if err != nil {
		t.Fatal(err)
	}
	ch := make(chan Portion, 8)
	now := time.Date(2026, 10, 14, 0, 0, 0, 0, time.UTC)
	ch <- worksPortion("file-a", now, "article", 1, 2)
	ch <- worksPortion("file-b", now, "article", 3, 4)
	ch <- eofPortion("file-a")
	ch <- eofPortion("file-b")
	close(ch)

	res, err := w.Run(t.Context(), ch)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(snk.works) != 1 || len(snk.works[0]) != 4 {
		t.Fatalf("batches = %v, want one 4-row batch", snk.works)
	}
	if res.Written != 4 || res.FilesDone != 2 || ledger.count() != 2 {
		t.Fatalf("res=%+v checkpoints=%d", res, ledger.count())
	}
}

func TestWriterDeduplicatesSameVersionAndContent(t *testing.T) {
	t.Parallel()

	snk := &recordingSink{}
	ledger := newMemLedger()
	w, err := NewWriter(WriterOptions{Sink: snk, Checkpoints: ledger, WorksBatchSize: 10})
	if err != nil {
		t.Fatal(err)
	}
	ch := make(chan Portion, 8)
	now := time.Date(2026, 10, 14, 0, 0, 0, 0, time.UTC)
	ch <- worksPortion("file-a", now, "article", 7)
	ch <- worksPortion("file-b", now, "article", 7) // retry: equivalent row
	ch <- eofPortion("file-a")
	ch <- eofPortion("file-b")
	close(ch)

	res, err := w.Run(t.Context(), ch)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Written != 1 || res.DedupDropped != 1 {
		t.Fatalf("res=%+v, want written=1 dedup=1", res)
	}
	if res.FilesDone != 2 || ledger.count() != 2 {
		t.Fatalf("both retried files must be checkpointed: res=%+v checkpoints=%d", res, ledger.count())
	}
}

func TestWriterRejectsConflictingContent(t *testing.T) {
	t.Parallel()

	w, err := NewWriter(WriterOptions{Sink: &recordingSink{}, Checkpoints: newMemLedger(), WorksBatchSize: 10})
	if err != nil {
		t.Fatal(err)
	}
	ch := make(chan Portion, 4)
	now := time.Date(2026, 10, 14, 0, 0, 0, 0, time.UTC)
	ch <- worksPortion("file-a", now, "article", 9)
	ch <- worksPortion("file-b", now, "preprint", 9) // same id+version, other content
	close(ch)

	_, err = w.Run(t.Context(), ch)
	if !errors.Is(err, pipeline.ErrCorpusIncomplete) {
		t.Fatalf("err = %v, want corpus_incomplete", err)
	}
}

func TestWriterFlushesTailOnStop(t *testing.T) {
	t.Parallel()

	snk := &recordingSink{}
	w, err := NewWriter(WriterOptions{Sink: snk, WorksBatchSize: 10, FinalTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Stop happened before the writer saw the portion
	ch := make(chan Portion, 2)
	now := time.Date(2026, 10, 14, 0, 0, 0, 0, time.UTC)
	ch <- worksPortion("file-a", now, "article", 11)
	close(ch)

	res, err := w.Run(ctx, ch)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Written != 1 || len(snk.works) != 1 {
		t.Fatalf("tail must be inserted under the completion context: res=%+v batches=%v", res, snk.works)
	}
}
