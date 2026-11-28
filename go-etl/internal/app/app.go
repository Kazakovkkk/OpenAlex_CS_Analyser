package app

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go-etl/internal/adapters/api"
	"go-etl/internal/adapters/s3"
	"go-etl/internal/httpapi"
	"go-etl/internal/ingest"
	"go-etl/internal/jobs"
	"go-etl/internal/knownids"
	"go-etl/internal/loadtest"
	"go-etl/internal/normalizer"
	"go-etl/internal/pipeline"
	"go-etl/internal/sink"
)

type Config struct {
	S3          *s3.Client
	API         *api.Client
	Sink        sink.BatchSink
	Pipeline    *pipeline.Store
	Checkpoints *pipeline.CheckpointStore
	// BuildKnownIDs loads the work_ids already stored, once per incremental run.
	BuildKnownIDs func(ctx context.Context) (*knownids.Index, time.Duration, error)
	// DeleteWorks removes deleted works from ClickHouse and verifies it.
	DeleteWorks func(ctx context.Context, ids []uint64) (int64, error)
	// Offers issues restart permissions when the release changes mid-load.
	Offers     *pipeline.Offers
	InstanceID string

	CheckpointDir    string
	SamplePath       string
	DefaultWorkers   int
	DefaultBatchSize int
	FullGetThreshold int64

	// TestTarget marks a dedicated database where diagnostic runs may write.
	TestTarget bool
	TargetHost string
	TargetPort int

	LoadtestSink sink.BatchSink
	Keys         *api.KeyStore

	APIKey string
}

func (c Config) withDefaults() Config {
	if c.Sink == nil {
		c.Sink = sink.NewNoopSink()
	}
	if c.CheckpointDir == "" {
		c.CheckpointDir = filepath.Join("tmp", "checkpoints")
	}
	if c.SamplePath == "" {
		c.SamplePath = loadtest.DefaultSamplePath
	}
	if c.DefaultWorkers <= 0 {
		c.DefaultWorkers = ingest.DefaultWorkers
	}
	if c.DefaultBatchSize <= 0 {
		c.DefaultBatchSize = sink.DefaultBatchCap
	}
	if c.InstanceID == "" {
		c.InstanceID = newInstanceID()
	}
	if c.Offers == nil {
		c.Offers = pipeline.NewOffers(30 * time.Minute)
	}
	if c.Keys == nil {
		c.Keys = api.NewKeyStore()
	}
	return c
}

// newInstanceID returns a short random id; it changes on every process start.
func newInstanceID() string {
	var buf [6]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return fmt.Sprintf("inst-%d", time.Now().UnixNano())
	}
	return "inst-" + hex.EncodeToString(buf[:])
}

type App struct {
	cfg    Config
	logger *slog.Logger
}

var _ httpapi.Backend = (*App)(nil)

func New(cfg Config, logger *slog.Logger) *App {
	if logger == nil {
		logger = slog.Default()
	}
	return &App{cfg: cfg.withDefaults(), logger: logger}
}

func (a *App) RunIngest(ctx context.Context, j *jobs.Job, req httpapi.IngestRequest) error {
	switch req.Source {
	case httpapi.SourceAPI:
		return a.runIngestAPI(ctx, j, req)
	default:
		return a.runIngestS3(ctx, j, req)
	}
}

func (a *App) runIngestS3(ctx context.Context, j *jobs.Job, req httpapi.IngestRequest) error {
	if a.cfg.S3 == nil {
		return errors.New("app: s3 client is not configured")
	}
	if diagnosticRun(req) {
		return a.runDiagnosticS3(ctx, j, req)
	}
	if a.cfg.Pipeline == nil {
		return errors.New("app: working ingest requires ClickHouse state (use --dry-run for a diagnostic run)")
	}

	// The state always comes first: an unfinished load must be resumed or
	// reported as a confirmed snapshot change before a new plan is built.
	st, err := a.cfg.Pipeline.Read(ctx)
	if err != nil {
		return fmt.Errorf("app: read state: %w", err)
	}
	rel, err := LoadRelease(ctx, a.cfg.S3)
	if err != nil {
		return err
	}

	// A crashed reset is recovered before the plan: repeat all three clears and
	// move to loading/full on the same release.
	if st.Loading() && st.Phase == pipeline.PhaseResetting {
		if st.ManifestID != rel.ID {
			return a.snapshotChanged(ctx, j, st, rel)
		}
		snapDate := rel.Date
		if st.LoadingSnapshotDate != nil {
			snapDate = *st.LoadingSnapshotDate
		}
		if _, rerr := a.cfg.Pipeline.Restart(ctx, pipeline.RestartParams{
			InstanceID:   a.cfg.InstanceID,
			JobID:        j.ID(),
			ManifestID:   rel.ID,
			SnapshotDate: snapDate,
		}); rerr != nil {
			return fmt.Errorf("app: recover resetting: %w", rerr)
		}
		if st, err = a.cfg.Pipeline.Read(ctx); err != nil {
			return fmt.Errorf("app: read state after reset recovery: %w", err)
		}
	}

	resume := st.Loading() && st.ManifestID == rel.ID
	if st.Loading() && !resume {
		return a.snapshotChanged(ctx, j, st, rel)
	}

	lastIdle, idleErr := a.cfg.Pipeline.LastIdle(ctx)
	hasIdle := idleErr == nil && lastIdle.Published()
	if idleErr != nil && !errors.Is(idleErr, sql.ErrNoRows) {
		return fmt.Errorf("app: read published state: %w", idleErr)
	}

	var published *time.Time
	if hasIdle {
		published = lastIdle.SnapshotDate
	}
	if st.Published() {
		if st.ManifestID == rel.ID {
			a.logger.Info("ingest: release is already published", "job", j.ID(),
				"manifest_id", st.ManifestID, "snapshot_date", st.SnapshotDate)
			return nil
		}
		published = st.SnapshotDate
	}

	phase := pipeline.PhaseFull
	if published != nil {
		phase = pipeline.PhaseIncremental
	}
	if resume {
		phase = st.Phase
	}
	plan, err := buildLoadPlan(rel, published, phase, req.ShardIndex, req.ShardTotal, req.MaxFiles)
	if err != nil {
		return fmt.Errorf("app: build plan: %w", err)
	}

	batchSize := req.BatchSize
	if batchSize <= 0 {
		batchSize = a.cfg.DefaultBatchSize
	}
	readOpts := s3.DefaultParquetReadOptions()
	readOpts.BatchSize = batchSize
	readOpts.Metrics = j.Metrics()
	if a.cfg.FullGetThreshold > 0 {
		readOpts.FullGetThreshold = a.cfg.FullGetThreshold
	}

	normOpts := []normalizer.Option{normalizer.WithSnapshotYear(rel.Date.Year())}
	var knownIDs *knownids.Index
	if phase == pipeline.PhaseIncremental && a.cfg.BuildKnownIDs != nil {
		idx, dur, ierr := a.cfg.BuildKnownIDs(ctx)
		if ierr != nil {
			return fmt.Errorf("app: build known ids: %w", ierr)
		}
		a.logger.Info("ingest: known ids",
			"count", idx.Len(), "build", dur.Round(time.Millisecond),
			"memory_kib", idx.SizeInBytes()/1024)
		knownIDs = idx
		normOpts = append(normOpts, normalizer.WithKnownWork(idx.Contains))
	}

	a.logger.Info("ingest: starting", "job", j.ID(),
		"phase", phase, "release", rel.ID, "snapshot_date", rel.DateString(),
		"works_files", len(plan.Works), "topics_files", len(plan.Topics),
		"works_bytes", ingest.SelectionBytes(plan.Works), "topics_bytes", ingest.SelectionBytes(plan.Topics),
		"published_snapshot", published, "shard", req.ShardIndex, "of", req.ShardTotal)

	if _, berr := a.cfg.Pipeline.BeginLoad(ctx, pipeline.BeginParams{
		InstanceID:   a.cfg.InstanceID,
		JobID:        j.ID(),
		ManifestID:   rel.ID,
		SnapshotDate: rel.Date,
		Phase:        phase,
	}); berr != nil {
		return fmt.Errorf("app: begin load: %w", berr)
	}

	pool := sink.NewWorksBatchPool(batchSize, sink.DefaultMaxPooledCap)
	files, skipped, ferr := a.corpusFiles(ctx, plan)
	if ferr != nil {
		return ferr
	}
	if skipped > 0 {
		a.logger.Info("ingest: resuming from confirmed checkpoints", "job", j.ID(), "files_skipped", skipped)
	}
	writer, werr := ingest.NewWriter(ingest.WriterOptions{
		Sink:            a.cfg.Sink,
		Checkpoints:     a.checkpointLedger(),
		KnownIDs:        knownIDAdder(knownIDs),
		Metrics:         j.Metrics(),
		Pool:            pool,
		WorksBatchSize:  batchSize,
		TopicsBatchSize: 5_000,
		Logger:          a.logger,
	})
	if werr != nil {
		return werr
	}
	res, rerr := ingest.RunWriterJob(ctx, ingest.WriterJobOptions{
		Client:          a.cfg.S3,
		Normalizer:      normalizer.NewNormalizer(nil, normOpts...),
		Pool:            pool,
		ReadOptions:     readOpts,
		Workers:         a.cfg.DefaultWorkers,
		AttemptsPerFile: 2,
		Files:           files,
		Writer:          writer,
		Metrics:         j.Metrics(),
		Logger:          a.logger,
	})
	a.logIngestResult(j, res)
	if rerr != nil {
		return rerr
	}
	if res.Stopped {
		return nil
	}
	if len(plan.Topics) > 0 && res.TopicsWritten == 0 {
		return fmt.Errorf("%w: the required Topics dictionary is empty", pipeline.ErrCorpusIncomplete)
	}
	if err := a.verifyRequiredCheckpoints(ctx, plan); err != nil {
		return err
	}

	if plan.Deletions {
		if derr := a.applyDeletions(ctx, j, plan.PublishedAt, knownIDs); derr != nil {
			return fmt.Errorf("app: deletions: %w", derr)
		}
	}
	if serr := a.checkReleaseStable(ctx, j, rel); serr != nil {
		return serr
	}

	publishedState, perr := a.cfg.Pipeline.Publish(ctx, pipeline.PublishParams{
		InstanceID:   a.cfg.InstanceID,
		JobID:        j.ID(),
		ManifestID:   rel.ID,
		SnapshotDate: rel.Date,
		Bump:         true,
		Now:          time.Now(),
	})
	if perr != nil {
		return fmt.Errorf("app: publish: %w", perr)
	}
	a.logger.Info("ingest: published",
		"job", j.ID(), "data_version", publishedState.DataVersion,
		"snapshot_date", publishedState.SnapshotDate, "manifest_id", publishedState.ManifestID,
		"works_written", res.WorksWritten, "topics_written", res.TopicsWritten,
		"dedup_dropped", res.DedupDropped, "batches", res.BatchesWritten)
	return nil
}

func (a *App) checkpointLedger() ingest.CheckpointLedger {
	if a.cfg.Checkpoints == nil {
		return nil
	}
	return a.cfg.Checkpoints
}

func knownIDAdder(idx *knownids.Index) func(uint64) {
	if idx == nil {
		return nil
	}
	return idx.Add
}

// corpusFiles filters files whose checkpoints are already confirmed, so a
// resumed attempt does not reread them. The checkpoint counts still include
// those files for the final completeness check.
func (a *App) corpusFiles(ctx context.Context, plan *loadPlan) ([]ingest.JobFile, int, error) {
	doneWorks := map[string]struct{}{}
	doneTopics := map[string]struct{}{}
	if a.cfg.Checkpoints != nil {
		var err error
		doneWorks, err = a.cfg.Checkpoints.Load(ctx, "works")
		if err != nil {
			return nil, 0, fmt.Errorf("app: load works checkpoints: %w", err)
		}
		doneTopics, err = a.cfg.Checkpoints.Load(ctx, "topics")
		if err != nil {
			return nil, 0, fmt.Errorf("app: load topics checkpoints: %w", err)
		}
	}
	files := make([]ingest.JobFile, 0, len(plan.Works)+len(plan.Topics))
	skipped := 0
	for _, e := range plan.Works {
		if _, ok := doneWorks[e.URL]; ok {
			skipped++
			continue
		}
		files = append(files, ingest.JobFile{Entity: ingest.EntityWorks, Entry: e})
	}
	for _, e := range plan.Topics {
		if _, ok := doneTopics[e.URL]; ok {
			skipped++
			continue
		}
		files = append(files, ingest.JobFile{Entity: ingest.EntityTopics, Entry: e})
	}
	return files, skipped, nil
}

func (a *App) verifyRequiredCheckpoints(ctx context.Context, plan *loadPlan) error {
	if a.cfg.Checkpoints == nil {
		return nil
	}
	for entity, files := range map[string][]ingest.Entry{"works": plan.Works, "topics": plan.Topics} {
		n, err := a.cfg.Checkpoints.Count(ctx, entity)
		if err != nil {
			return fmt.Errorf("app: count %s checkpoints: %w", entity, err)
		}
		if n < uint64(len(files)) {
			return fmt.Errorf("%w: %d of %d %s checkpoint(s) confirmed",
				pipeline.ErrCorpusIncomplete, n, len(files), entity)
		}
	}
	return nil
}

func (a *App) runWorks(ctx context.Context, j *jobs.Job, entries []ingest.Entry, batchSize int,
	readOpts s3.ParquetReadOptions, normOpts []normalizer.Option) (ingest.Result, error) {
	if len(entries) == 0 {
		return ingest.Result{JobID: j.ID()}, nil
	}
	proc := &ingest.S3WorksProcessor{
		Client:      a.cfg.S3,
		Normalizer:  normalizer.NewNormalizer(normalizer.NewWorkDeduplicator(), normOpts...),
		Pool:        sink.NewWorksBatchPool(batchSize, sink.DefaultMaxPooledCap),
		Sink:        a.cfg.Sink,
		Metrics:     j.Metrics(),
		ReadOptions: readOpts,
	}
	opts := ingest.Options{
		Workers:         a.cfg.DefaultWorkers,
		AttemptsPerFile: 2,
		Order:           ingest.OrderBalanced,
		JobID:           j.ID(),
		Metrics:         j.Metrics(),
		Logger:          a.logger,
	}
	if a.cfg.Checkpoints != nil {
		opts.Checkpoints = clickhouseLedger{ctx: ctx, store: a.cfg.Checkpoints, entity: "works"}
	}
	res, err := ingest.Run(ctx, entries, proc, opts)
	a.logIngestResult(j, res)
	return res, err
}

func (a *App) runTopics(ctx context.Context, j *jobs.Job, entries []ingest.Entry,
	readOpts s3.ParquetReadOptions) (ingest.Result, error) {
	if len(entries) == 0 {
		return ingest.Result{JobID: j.ID()}, nil
	}
	proc := &ingest.S3TopicsProcessor{
		Client:      a.cfg.S3,
		Sink:        a.cfg.Sink,
		Metrics:     j.Metrics(),
		ReadOptions: readOpts,
	}
	opts := ingest.Options{
		Workers:         a.cfg.DefaultWorkers,
		AttemptsPerFile: 2,
		Order:           ingest.OrderManifest,
		JobID:           j.ID(),
		Metrics:         j.Metrics(),
		Logger:          a.logger,
	}
	if a.cfg.Checkpoints != nil {
		opts.Checkpoints = clickhouseLedger{ctx: ctx, store: a.cfg.Checkpoints, entity: "topics"}
	}
	res, err := ingest.Run(ctx, entries, proc, opts)
	if err != nil {
		return res, fmt.Errorf("app: topics: %w", err)
	}
	a.logger.Info("ingest: topics finished", "job", j.ID(),
		"files_done", res.FilesDone, "files_failed", res.FilesFailed,
		"written", res.Written, "bytes_read", res.BytesRead)
	return res, nil
}

// runDiagnosticS3 is the isolated path for limits, an arbitrary S3 lower bound
// and a separate shard: no pipeline rows, no checkpoints, and a ClickHouse
// sink only when the config marks a dedicated test target.
func (a *App) runDiagnosticS3(ctx context.Context, j *jobs.Job, req httpapi.IngestRequest) error {
	if _, isClickHouse := a.cfg.Sink.(*sink.ClickHouseSink); isClickHouse && !a.cfg.TestTarget {
		return errors.New("app: diagnostic run would write the working corpus; use --dry-run or a dedicated test target")
	}
	manifest, err := a.cfg.S3.FetchManifest(ctx, s3.ParquetManifestKey("works"))
	if err != nil {
		return fmt.Errorf("app: fetch works manifest: %w", err)
	}
	if err := manifest.Validate("works"); err != nil {
		return err
	}
	entries, err := manifest.SelectEntries(req.ShardIndex, req.ShardTotal, req.MaxFiles)
	if err != nil {
		return fmt.Errorf("app: select files: %w", err)
	}
	if req.FromDate != "" {
		before := len(entries)
		entries, err = ingest.FilterByPartitionDate(entries, req.FromDate)
		if err != nil {
			return err
		}
		a.logger.Info("ingest: date filter applied", "from_date", req.FromDate,
			"files_before", before, "files_after", len(entries))
		if len(entries) == 0 {
			return fmt.Errorf("app: no files with updated_date >= %s", req.FromDate)
		}
	}
	batchSize := req.BatchSize
	if batchSize <= 0 {
		batchSize = a.cfg.DefaultBatchSize
	}
	readOpts := s3.DefaultParquetReadOptions()
	readOpts.BatchSize = batchSize
	readOpts.Metrics = j.Metrics()
	if a.cfg.FullGetThreshold > 0 {
		readOpts.FullGetThreshold = a.cfg.FullGetThreshold
	}
	normOpts := []normalizer.Option{normalizer.WithSnapshotYear(manifestYear(manifest.Date))}
	proc := &ingest.S3WorksProcessor{
		Client:      a.cfg.S3,
		Normalizer:  normalizer.NewNormalizer(normalizer.NewWorkDeduplicator(), normOpts...),
		Pool:        sink.NewWorksBatchPool(batchSize, sink.DefaultMaxPooledCap),
		Sink:        a.cfg.Sink,
		Metrics:     j.Metrics(),
		ReadOptions: readOpts,
	}
	workers := req.Workers
	if workers <= 0 {
		workers = a.cfg.DefaultWorkers
	}
	journal, jerr := newProgressJournal(req)
	if jerr != nil {
		return jerr
	}
	defer journal.Close()
	res, err := ingest.Run(ctx, entries, proc, ingest.Options{
		Workers:         workers,
		AttemptsPerFile: 2,
		Order:           ingest.OrderBalanced,
		JobID:           j.ID(),
		Metrics:         j.Metrics(),
		Logger:          a.logger,
		MaxRecords:      int64(req.MaxRecords),
	})
	journal.Write(res)
	a.logIngestResult(j, res)
	if err != nil {
		return err
	}
	if res.FilesFailed > 0 {
		return fmt.Errorf("app: diagnostic run failed %d file(s)", res.FilesFailed)
	}
	return nil
}

func (a *App) snapshotChanged(ctx context.Context, j *jobs.Job, st pipeline.State, rel *Release) error {
	var previous *time.Time
	if lastIdle, err := a.cfg.Pipeline.LastIdle(ctx); err == nil {
		previous = lastIdle.SnapshotDate
	}
	available := rel.Date
	offer := a.cfg.Offers.Issue(pipeline.Offer{
		PreviousManifest:      st.ManifestID,
		NewManifest:           rel.ID,
		Database:              a.cfg.Pipeline.Database(),
		Revision:              st.Revision,
		JobID:                 j.ID(),
		PreviousSnapshotDate:  previous,
		AvailableSnapshotDate: &available,
		TargetDatabase: pipeline.TargetDatabase{
			Host:     a.cfg.TargetHost,
			Port:     a.cfg.TargetPort,
			Database: a.cfg.Pipeline.Database(),
		},
	})
	a.logger.Warn("ingest: the unfinished snapshot is gone, publication refused",
		"job", j.ID(), "previous", st.ManifestID, "available", rel.ID)
	return &pipeline.SnapshotChangedError{
		Token:                 offer.Token,
		PreviousManifest:      st.ManifestID,
		CurrentManifest:       rel.ID,
		PreviousSnapshotDate:  previous,
		AvailableSnapshotDate: &available,
		TargetDatabase:        offer.TargetDatabase,
	}
}

// checkReleaseStable re-reads both mandatory manifests before publication and
// refuses to publish if the release changed while the load was running.
func (a *App) checkReleaseStable(ctx context.Context, j *jobs.Job, started *Release) error {
	cur, err := LoadRelease(ctx, a.cfg.S3)
	if err != nil {
		// An unavailable mandatory manifest must block publication.
		return fmt.Errorf("app: final release check: %w", err)
	}
	if cur.ID == started.ID {
		return nil
	}
	var st pipeline.State
	if got, rerr := a.cfg.Pipeline.Read(ctx); rerr == nil {
		st = got
	}
	snap := pipeline.State{ManifestID: started.ID, Revision: st.Revision, SnapshotDate: &started.Date}
	return a.snapshotChanged(ctx, j, snap, cur)
}

func diagnosticRun(req httpapi.IngestRequest) bool {
	return req.MaxFiles > 0 || req.MaxRecords > 0 || req.ShardTotal > 1 || req.FromDate != ""
}

func publishDataChanged(res ingest.Result) bool {
	return res.Written > 0 || res.FilesDone > 0
}

func loadPhase(published *time.Time) pipeline.LoadPhase {
	if published != nil {
		return pipeline.PhaseIncremental
	}
	return pipeline.PhaseFull
}

func dimensionsPhase(cur pipeline.State) pipeline.LoadPhase {
	if cur.Published() {
		return pipeline.PhaseIncremental
	}
	return pipeline.PhaseFull
}

func (a *App) manifestID(ctx context.Context, works *s3.Manifest) string {
	id := works.CanonicalID()
	topics, err := a.cfg.S3.FetchManifest(ctx, s3.ParquetManifestKey("topics"))
	if err != nil {
		a.logger.Warn("ingest: topics manifest unavailable, id covers works only", "err", err)
		return id
	}
	return s3.CombineIDs(id, topics.CanonicalID())
}

type clickhouseLedger struct {
	ctx    context.Context
	store  *pipeline.CheckpointStore
	entity string
}

func (l clickhouseLedger) LoadDone() (map[string]struct{}, error) {
	return l.store.Load(l.ctx, l.entity)
}

func (l clickhouseLedger) MarkDone(objectKey string) error {
	return l.store.MarkDone(l.ctx, l.entity, objectKey)
}

// checkManifestStable re-reads the release manifest and refuses to publish if it
// changed while the load was running (spec §4.4). It returns a
// SnapshotChangedError carrying the token needed to confirm the restart.
func (a *App) checkManifestStable(ctx context.Context, started *s3.Manifest, publishedID string, j *jobs.Job) error {
	cur, err := a.cfg.S3.FetchManifest(ctx, s3.ParquetManifestKey("works"))
	if err != nil {
		a.logger.Warn("ingest: cannot re-read the manifest", "err", err)
		return nil
	}
	if cur.CanonicalID() == started.CanonicalID() {
		return nil
	}

	var rev uint64
	if st, rerr := a.cfg.Pipeline.Read(ctx); rerr == nil {
		rev = st.Revision
	}
	offer := a.cfg.Offers.Issue(pipeline.Offer{
		PreviousManifest: publishedID,
		NewManifest:      cur.CanonicalID(),
		Database:         a.cfg.Pipeline.Database(),
		Revision:         rev,
		JobID:            j.ID(),
	})
	a.logger.Warn("ingest: snapshot changed, publication refused",
		"published", publishedID, "bucket", cur.CanonicalID(),
		"restart_token", offer.Token, "expires_at", offer.ExpiresAt)

	return &pipeline.SnapshotChangedError{
		Token:            offer.Token,
		PreviousManifest: publishedID,
		CurrentManifest:  cur.CanonicalID(),
	}
}

// RestartIngest confirms a restart: it re-checks the release and the state the
// offer was issued for, clears the corpus and loads it again.
func (a *App) RestartIngest(ctx context.Context, j *jobs.Job, req httpapi.RestartRequest) error {
	if a.cfg.Pipeline == nil {
		return fmt.Errorf("app: restart is not available without ClickHouse")
	}
	st, err := a.cfg.Pipeline.Read(ctx)
	if err != nil {
		return fmt.Errorf("app: read state: %w", err)
	}
	offer, terr := a.cfg.Offers.Take(strings.TrimSpace(req.RestartToken), a.cfg.Pipeline.Database(), st.Revision)
	if terr != nil {
		return terr
	}
	rel, merr := LoadRelease(ctx, a.cfg.S3)
	if merr != nil {
		return fmt.Errorf("app: release: %w", merr)
	}
	if rel.ID != offer.NewManifest {
		return fmt.Errorf("%w: the release changed again", pipeline.ErrRestartOfferExpired)
	}
	if _, rerr := a.cfg.Pipeline.Restart(ctx, pipeline.RestartParams{
		InstanceID:   a.cfg.InstanceID,
		JobID:        j.ID(),
		ManifestID:   rel.ID,
		SnapshotDate: rel.Date,
	}); rerr != nil {
		return rerr
	}
	a.logger.Warn("ingest: restart confirmed, corpus cleared",
		"job", j.ID(), "manifest", offer.NewManifest, "previous", offer.PreviousManifest)
	return a.runIngestS3(ctx, j, httpapi.IngestRequest{Source: "s3", ShardTotal: 1})
}

// PreviewRestart validates a restart token without consuming it, so the HTTP
// layer can answer 409 restart_offer_expired before a job is created.
func (a *App) PreviewRestart(ctx context.Context, token string) error {
	if a.cfg.Pipeline == nil {
		return pipeline.ErrRestartOfferExpired
	}
	st, err := a.cfg.Pipeline.Read(ctx)
	if err != nil {
		return err
	}
	_, err = a.cfg.Offers.Lookup(token, a.cfg.Pipeline.Database(), st.Revision)
	return err
}

// applyDeletions streams the release deletion log, keeps ids we actually store
// and deletes them before the version is published.
func (a *App) applyDeletions(ctx context.Context, j *jobs.Job, since *time.Time, idx *knownids.Index) error {
	if a.cfg.S3 == nil || a.cfg.DeleteWorks == nil {
		return nil
	}
	var (
		batch   []uint64
		cand    int64
		deleted int64
	)
	if j != nil {
		j.Metrics().SetCurrentItem("deletions")
	}
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		n, err := a.cfg.DeleteWorks(ctx, batch)
		if err != nil {
			return err
		}
		deleted += n
		batch = batch[:0]
		return nil
	}

	streamed, err := a.cfg.S3.StreamDeletedIDs(ctx, func(id uint64, at time.Time, hasDate bool) error {
		if since != nil && hasDate && at.Before(*since) {
			return nil
		}
		if idx != nil && !idx.Contains(id) {
			return nil
		}
		cand++
		batch = append(batch, id)
		if len(batch) >= 5000 {
			return flush()
		}
		return nil
	})
	if err != nil {
		return err
	}
	if err := flush(); err != nil {
		return err
	}
	a.logger.Info("ingest: deletions", "job", j.ID(),
		"streamed", streamed, "stored_candidates", cand, "deleted", deleted)
	return nil
}

func (a *App) InstanceID() string { return a.cfg.InstanceID }

func ResolveAPIKey(explicit string) string {
	if v := strings.TrimSpace(explicit); v != "" {
		return v
	}
	return strings.TrimSpace(os.Getenv("OPENALEX_API_KEY"))
}

func (a *App) runIngestAPI(ctx context.Context, j *jobs.Job, req httpapi.IngestRequest) error {
	apiKey := ResolveAPIKey(req.APIKey)
	if apiKey == "" {
		apiKey = a.cfg.APIKey
	}
	if apiKey == "" {
		apiKey = a.cfg.Keys.Get()
	}
	client := api.NewClient(api.Options{
		APIKey:   apiKey,
		RPSLimit: float64(req.RPSLimit),
		Metrics:  j.Metrics(),
	})
	query := api.WorksQuery{FieldID: 17, PerPage: 100, MaxRecords: req.MaxRecords, Select: api.DefaultWorksSelect}
	if req.FromDate != "" {
		switch req.DateFilterMode {
		case httpapi.DateFilterCreated:
			query.FromCreatedDate = req.FromDate
		case httpapi.DateFilterPublication:
			query.FromPublicationDate = req.FromDate
		default:
			query.FromUpdatedDate = req.FromDate
		}
	}

	batchSize := req.BatchSize
	if batchSize <= 0 {
		batchSize = a.cfg.DefaultBatchSize
	}
	norm := normalizer.NewNormalizer(normalizer.NewWorkDeduplicator(), normalizer.WithSnapshotYear(time.Now().UTC().Year()))
	pool := sink.NewWorksBatchPool(batchSize, sink.DefaultMaxPooledCap)
	batch := pool.Get()
	defer pool.Put(batch)

	flush := func(fctx context.Context) error {
		if batch.Len() == 0 {
			return nil
		}
		start := time.Now()
		if err := a.cfg.Sink.WriteWorksBatch(fctx, batch); err != nil {
			return fmt.Errorf("app: write api batch: %w", err)
		}
		j.Metrics().RecordBatchDuration(time.Since(start))
		j.Metrics().AddWritten(uint64(batch.Len()), uint64(batch.ApproxBytes()))
		batch.Reset()
		return nil
	}

	stats, err := client.IterateWorks(ctx, query, func(raw []byte) error {
		j.Metrics().AddScanned(1)
		outcome, reason := norm.IngestRawJSON(raw, batch)
		switch outcome {
		case normalizer.OutcomeRejected:
			j.Metrics().AddRejected(string(reason))
		case normalizer.OutcomeAdded:
			j.Metrics().AddMatched(1)
		case normalizer.OutcomeDuplicate:
			j.Metrics().AddMatched(1)
			j.Metrics().AddDedupDropped(1)
		}
		if batch.Len() >= batchSize {
			return flush(ctx)
		}
		return nil
	})
	// A stopped job still flushes what it already read: the job context is
	// cancelled, so the final write gets its own short deadline.
	finalCtx, cancelFinal := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancelFinal()
	if ferr := flush(finalCtx); ferr != nil && err == nil {
		err = ferr
	}
	if stats.Bytes > 0 {
		j.Metrics().AddBytesRead(stats.Bytes)
	}
	if stats.Requests > 0 {
		j.Metrics().AddHTTPRequests(uint64(stats.Requests))
	}
	if err != nil {
		return err
	}
	a.logger.Info("ingest(api): finished", "job", j.ID(),
		"pages", stats.Pages, "records", stats.Records,
		"requests", stats.Requests, "retries", stats.Retries,
		"cost_usd", stats.CostUSD, "bytes", stats.Bytes, "has_key", apiKey != "",
		"elapsed", stats.Elapsed.Round(time.Millisecond))
	return nil
}

func (a *App) logIngestResult(j *jobs.Job, res ingest.Result) {
	a.logger.Info("ingest: result",
		"job", j.ID(),
		"files_done", res.FilesDone, "files_failed", res.FilesFailed, "files_skipped", res.FilesSkipped,
		"scanned", res.Scanned, "matched", res.Matched, "written", res.Written,
		"rejected", res.Rejected, "reasons", j.Metrics().RejectedByReason(),
		"bytes_read", res.BytesRead, "http_requests", res.HTTPRequests,
		"stopped", res.Stopped, "elapsed", res.Elapsed.Round(time.Millisecond))
}

func manifestYear(date string) int {
	if t, err := time.Parse("2006-01-02", strings.TrimSpace(date)); err == nil {
		return t.Year()
	}
	return time.Now().UTC().Year()
}

func (a *App) RunDimensions(ctx context.Context, j *jobs.Job, req httpapi.DimensionsRequest) error {
	if a.cfg.S3 == nil {
		return errors.New("app: s3 client is not configured")
	}
	plan, err := ingest.FetchDimensions(ctx, a.cfg.S3, 0, 1, 0)
	if err != nil {
		return err
	}
	a.logger.Info("sync-dims: starting", "job", j.ID(),
		"topics", len(plan.Topics), "institutions", len(plan.Institutions), "force_reload", req.ForceReload)

	var (
		manifestID string
		snapDate   time.Time
		changed    bool
	)
	if a.cfg.Pipeline != nil {
		topicsManifest, terr := a.cfg.S3.FetchManifest(ctx, s3.ParquetManifestKey("topics"))
		if terr != nil {
			return fmt.Errorf("app: topics manifest: %w", terr)
		}
		manifestID = s3.CombineIDs("topics", topicsManifest.CanonicalID())
		parsed, perr := time.Parse("2006-01-02", strings.TrimSpace(topicsManifest.Date))
		if perr != nil {
			return fmt.Errorf("app: topics manifest date %q: %w", topicsManifest.Date, perr)
		}
		snapDate = parsed
		cur, rerr := a.cfg.Pipeline.Read(ctx)
		if rerr != nil {
			return fmt.Errorf("app: read state: %w", rerr)
		}
		if cur.LoadState == pipeline.LoadLoading {
			return fmt.Errorf("app: cannot sync dims while a load is in progress")
		}
		if cur.SnapshotDate != nil {
			snapDate = *cur.SnapshotDate
		}
		changed = cur.ManifestID != manifestID
		if !changed && !req.ForceReload && cur.Published() {
			a.logger.Info("sync-dims: the dictionary is already published, skipping",
				"job", j.ID(), "manifest_id", manifestID)
			return nil
		}
		if _, berr := a.cfg.Pipeline.BeginLoad(ctx, pipeline.BeginParams{
			InstanceID:   a.cfg.InstanceID,
			JobID:        j.ID(),
			ManifestID:   manifestID,
			SnapshotDate: snapDate,
			Phase:        dimensionsPhase(cur),
		}); berr != nil {
			return fmt.Errorf("app: begin load: %w", berr)
		}
	}

	readOpts := s3.DefaultParquetReadOptions()
	readOpts.Metrics = j.Metrics()
	opts := ingest.Options{
		Workers:         a.cfg.DefaultWorkers,
		AttemptsPerFile: 2,
		Order:           ingest.OrderManifest,
		JobID:           j.ID(),
		Metrics:         j.Metrics(),
		Logger:          a.logger,
	}

	topicsRes, err := ingest.Run(ctx, plan.Topics, &ingest.S3TopicsProcessor{
		Client: a.cfg.S3, Sink: a.cfg.Sink, Metrics: j.Metrics(), ReadOptions: readOpts,
	}, opts)
	if err != nil {
		return fmt.Errorf("app: sync topics: %w", err)
	}
	var instRows, instBytes uint64
	if _, isClickHouse := a.cfg.Sink.(*sink.ClickHouseSink); isClickHouse {
		a.logger.Info("sync-dims: institutions skipped: no dim_institutions in the MVP schema")
	} else {
		instRes, err := ingest.Run(ctx, plan.Institutions, &ingest.S3InstitutionsProcessor{
			Client: a.cfg.S3, Sink: a.cfg.Sink, Metrics: j.Metrics(), ReadOptions: readOpts,
		}, opts)
		if err != nil {
			return fmt.Errorf("app: sync institutions: %w", err)
		}
		instRows, instBytes = instRes.Written, instRes.BytesRead
	}
	a.logger.Info("sync-dims: finished", "job", j.ID(),
		"topics_rows", topicsRes.Written, "institutions_rows", instRows,
		"bytes_read", topicsRes.BytesRead+instBytes)
	if a.cfg.Pipeline != nil {
		published, perr := a.cfg.Pipeline.Publish(ctx, pipeline.PublishParams{
			InstanceID:   a.cfg.InstanceID,
			JobID:        j.ID(),
			ManifestID:   manifestID,
			SnapshotDate: snapDate,
			Bump:         changed || req.ForceReload,
			Now:          time.Now(),
		})
		if perr != nil {
			return fmt.Errorf("app: publish dims: %w", perr)
		}
		a.logger.Info("sync-dims: published", "job", j.ID(),
			"data_version", published.DataVersion, "changed", changed)
	}
	return nil
}

func (a *App) RunLoadtest(ctx context.Context, j *jobs.Job, req httpapi.LoadtestRequest) error {
	samplePath := a.cfg.SamplePath
	sinkForLoad := a.cfg.LoadtestSink
	if sinkForLoad == nil {
		sinkForLoad = a.cfg.Sink
	}
	res, err := loadtest.Run(ctx, loadtest.Config{
		SamplePath:  samplePath,
		TargetRPS:   req.TargetRPS,
		DurationSec: req.DurationSec,
		Workers:     req.Workers,
		BatchSize:   req.BatchSize,
	}, sinkForLoad, j.Metrics())
	if err != nil {
		return err
	}
	a.logger.Info("loadtest: finished", "job", j.ID(),
		"records", res.Records, "batches", res.Batches, "written", res.Written,
		"target_rps", res.TargetRPS, "actual_rps", int(res.ActualRPS),
		"p50_ms", res.P50BatchMs, "p95_ms", res.P95BatchMs, "p99_ms", res.P99BatchMs)
	return nil
}

func (a *App) RequestAPIKey(ctx context.Context, email string) (api.APIKeyResult, error) {
	client := a.cfg.API
	if client == nil {
		client = api.NewClient(api.Options{})
	}
	res, err := client.RequestAPIKey(ctx, email)
	if err != nil {
		return res, err
	}
	// Keep the value inside Go for the next manual API job; the HTTP response
	// carries only the status message.
	a.cfg.Keys.Set(res.APIKey)
	return api.APIKeyResult{Message: res.Message}, nil
}
