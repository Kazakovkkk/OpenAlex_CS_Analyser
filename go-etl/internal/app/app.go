package app

import (
	"context"
	"crypto/rand"
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
	manifest, err := a.cfg.S3.FetchManifest(ctx, s3.ParquetManifestKey("works"))
	if err != nil {
		return fmt.Errorf("app: fetch works manifest: %w", err)
	}
	entries, err := manifest.SelectEntries(req.ShardIndex, req.ShardTotal, 0)
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
			return fmt.Errorf("app: no files with updated_date >= %s (snapshot keeps only recent partitions)", req.FromDate)
		}
	}
	diagnostic := diagnosticRun(req)
	usePipeline := a.cfg.Pipeline != nil && !diagnostic

	var (
		publishedSnap *time.Time
		pubManifest   string
	)
	if usePipeline {
		cur, rerr := a.cfg.Pipeline.Read(ctx)
		if rerr != nil {
			return fmt.Errorf("app: read state: %w", rerr)
		}
		if cur.LoadState == pipeline.LoadLoading && cur.InstanceID != a.cfg.InstanceID {
			return fmt.Errorf("app: a load is already running in instance %q", cur.InstanceID)
		}
		if cur.Published() {
			publishedSnap = cur.SnapshotDate
			pubManifest = cur.ManifestID
		}
	}
	if publishedSnap != nil {
		from := ingest.UpdateFrom(*publishedSnap).Format("2006-01-02")
		before := len(entries)
		entries, err = ingest.FilterByPartitionDate(entries, from)
		if err != nil {
			return err
		}
		a.logger.Info("ingest: incremental selection",
			"published_snapshot", publishedSnap.Format("2006-01-02"),
			"from_partition", from, "files_before", before, "files_after", len(entries))
		if len(entries) == 0 {
			a.logger.Info("ingest: nothing to update", "job", j.ID(),
				"published_snapshot", publishedSnap.Format("2006-01-02"))
			return nil
		}
	}

	a.logger.Info("ingest: starting", "job", j.ID(), "files", len(entries),
		"shard", req.ShardIndex, "of", req.ShardTotal,
		"snapshot_bytes", ingest.SelectionBytes(entries))

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
	var knownIDs *knownids.Index
	if usePipeline && publishedSnap != nil && a.cfg.BuildKnownIDs != nil {
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

	var (
		manifestID string
		snapDate   time.Time
	)
	if usePipeline {
		manifestID = a.manifestID(ctx, manifest)
		parsed, perr := time.Parse("2006-01-02", strings.TrimSpace(manifest.Date))
		if perr != nil {
			return fmt.Errorf("app: manifest date %q: %w", manifest.Date, perr)
		}
		snapDate = parsed
		if _, berr := a.cfg.Pipeline.BeginLoad(ctx, pipeline.BeginParams{
			InstanceID:   a.cfg.InstanceID,
			JobID:        j.ID(),
			ManifestID:   manifestID,
			SnapshotDate: snapDate,
		}); berr != nil {
			return fmt.Errorf("app: begin load: %w", berr)
		}
	}

	var (
		ledger ingest.Ledger
		ckPath string
	)
	switch {
	case diagnostic:
	case a.cfg.Checkpoints != nil:
		ledger = clickhouseLedger{ctx: ctx, store: a.cfg.Checkpoints}
	case a.cfg.CheckpointDir != "":
		ckPath = filepath.Join(a.cfg.CheckpointDir, fmt.Sprintf(
			"works-%s-shard%d-of%d-max%d.jsonl", req.Source, req.ShardIndex, req.ShardTotal, req.MaxFiles))
	}

	res, err := ingest.Run(ctx, entries, proc, ingest.Options{
		Workers:         workers,
		MaxFiles:        req.MaxFiles,
		AttemptsPerFile: 2,
		Order:           ingest.OrderBalanced,
		CheckpointPath:  ckPath,
		Checkpoints:     ledger,
		JobID:           j.ID(),
		Metrics:         j.Metrics(),
		Logger:          a.logger,
		MaxRecords:      int64(req.MaxRecords),
	})
	a.logIngestResult(j, res)
	if err != nil {
		return err
	}
	if res.Stopped {
		return nil
	}
	if res.FilesFailed > 0 {
		// A failed file means the corpus is not complete: the version must not be
		// published, and the state stays `loading` for the next attempt.
		return fmt.Errorf("%w: %d of %d file(s) failed, nothing was published",
			pipeline.ErrCorpusIncomplete, res.FilesFailed, res.FilesDone+res.FilesFailed)
	}
	if !usePipeline && ledger == nil && res.FilesDone == 0 && res.FilesSkipped > 0 {
		return fmt.Errorf("app: all %d selected file(s) were already processed according to checkpoint %q; "+
			"delete that file (or change max_files/shard) to run them again", res.FilesSkipped, ckPath)
	}
	if usePipeline {
		if derr := a.applyDeletions(ctx, j, publishedSnap, knownIDs); derr != nil {
			return fmt.Errorf("app: deletions: %w", derr)
		}
		if serr := a.checkManifestStable(ctx, manifest, pubManifest, j); serr != nil {
			return serr
		}
		published, perr := a.cfg.Pipeline.Publish(ctx, pipeline.PublishParams{
			InstanceID:   a.cfg.InstanceID,
			JobID:        j.ID(),
			ManifestID:   manifestID,
			SnapshotDate: snapDate,
			DataChanged:  publishDataChanged(res),
			Now:          time.Now(),
		})
		if perr != nil {
			return fmt.Errorf("app: publish: %w", perr)
		}
		a.logger.Info("ingest: published",
			"job", j.ID(), "data_version", published.DataVersion,
			"snapshot_date", published.SnapshotDate, "manifest_id", published.ManifestID)
	}
	return nil
}

func diagnosticRun(req httpapi.IngestRequest) bool {
	return req.MaxFiles > 0 || req.MaxRecords > 0 || req.ShardTotal > 1
}

func publishDataChanged(res ingest.Result) bool {
	return res.Written > 0 || res.FilesDone > 0
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
	ctx   context.Context
	store *pipeline.CheckpointStore
}

func (l clickhouseLedger) LoadDone() (map[string]struct{}, error) {
	return l.store.Load(l.ctx, "works")
}

func (l clickhouseLedger) MarkDone(objectKey string) error {
	return l.store.MarkDone(l.ctx, "works", objectKey)
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
	cur, merr := a.cfg.S3.FetchManifest(ctx, s3.ParquetManifestKey("works"))
	if merr != nil {
		return fmt.Errorf("app: manifest: %w", merr)
	}
	if cur.CanonicalID() != offer.NewManifest {
		return fmt.Errorf("%w: the release changed again", pipeline.ErrRestartOfferExpired)
	}
	if rerr := a.cfg.Pipeline.ResetCorpus(ctx); rerr != nil {
		return rerr
	}
	a.logger.Warn("ingest: restart confirmed, corpus cleared",
		"job", j.ID(), "manifest", offer.NewManifest, "previous", offer.PreviousManifest)
	return a.runIngestS3(ctx, j, httpapi.IngestRequest{Source: "s3", ShardTotal: 1})
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
			DataChanged:  changed,
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
	res, err := loadtest.Run(ctx, loadtest.Config{
		SamplePath:  samplePath,
		TargetRPS:   req.TargetRPS,
		DurationSec: req.DurationSec,
		Workers:     req.Workers,
		BatchSize:   req.BatchSize,
	}, a.cfg.Sink, j.Metrics())
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
	return client.RequestAPIKey(ctx, email)
}
