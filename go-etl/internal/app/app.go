package app

import (
	"context"
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
	"go-etl/internal/loadtest"
	"go-etl/internal/normalizer"
	"go-etl/internal/sink"
)

type Config struct {
	S3   *s3.Client
	API  *api.Client
	Sink sink.BatchSink

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
	return c
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
	// Select without the file limit first: max_files must be applied AFTER the date
	// filter, otherwise "from_date" would only see the oldest partitions of the
	// manifest (the first N entries) and drop them all.
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
	a.logger.Info("ingest: starting", "job", j.ID(), "files", len(entries),
		"shard", req.ShardIndex, "of", req.ShardTotal, "snapshot_bytes", manifest.ContentLength)

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

	proc := &ingest.S3WorksProcessor{
		Client:      a.cfg.S3,
		Normalizer:  normalizer.NewNormalizer(normalizer.NewWorkDeduplicator()),
		Pool:        sink.NewWorksBatchPool(batchSize, sink.DefaultMaxPooledCap),
		Sink:        a.cfg.Sink,
		Metrics:     j.Metrics(),
		ReadOptions: readOpts,
	}

	workers := req.Workers
	if workers <= 0 {
		workers = a.cfg.DefaultWorkers
	}
	// The checkpoint name must describe the selection, not the job id: job ids
	// restart at ing-1 with every process, so an id-based name silently reuses the
	// previous run's checkpoint and the new job "completes" without doing any work
	// (exactly what happened during the live HTTP smoke test).
	ckPath := ""
	if a.cfg.CheckpointDir != "" {
		ckPath = filepath.Join(a.cfg.CheckpointDir, fmt.Sprintf(
			"works-%s-shard%d-of%d-max%d.jsonl", req.Source, req.ShardIndex, req.ShardTotal, req.MaxFiles))
	}

	res, err := ingest.Run(ctx, entries, proc, ingest.Options{
		Workers:         workers,
		MaxFiles:        req.MaxFiles,
		AttemptsPerFile: 2,
		Order:           ingest.OrderBalanced,
		CheckpointPath:  ckPath,
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
	if res.FilesDone == 0 && res.FilesFailed > 0 {
		return fmt.Errorf("app: all %d files failed (like the first: %v)", res.FilesFailed, res.FailedFiles[0].Err)
	}
	if res.FilesDone == 0 && res.FilesSkipped > 0 {
		// Do not report "completed" for a run that did nothing: the operator must
		// know the checkpoint swallowed the selection.
		return fmt.Errorf("app: all %d selected file(s) were already processed according to checkpoint %q; "+
			"delete that file (or change max_files/shard) to run them again", res.FilesSkipped, ckPath)
	}
	return nil
}

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
	norm := normalizer.NewNormalizer(normalizer.NewWorkDeduplicator())
	pool := sink.NewWorksBatchPool(batchSize, sink.DefaultMaxPooledCap)
	batch := pool.Get()
	defer pool.Put(batch)

	flush := func() error {
		if batch.Len() == 0 {
			return nil
		}
		start := time.Now()
		if err := a.cfg.Sink.WriteWorksBatch(ctx, batch); err != nil {
			return fmt.Errorf("app: write api batch: %w", err)
		}
		j.Metrics().RecordBatchDuration(time.Since(start))
		j.Metrics().AddWritten(uint64(batch.Len()), uint64(batch.ApproxBytes()))
		batch.Reset()
		return nil
	}

	stats, err := client.IterateWorks(ctx, query, func(raw []byte) error {
		j.Metrics().AddScanned(1)
		outcome, aerr := norm.AppendRawJSONWork(raw, batch)
		if aerr != nil {
			return nil
		}
		switch outcome {
		case normalizer.OutcomeAdded:
			j.Metrics().AddMatched(1)
		case normalizer.OutcomeDuplicate:
			j.Metrics().AddMatched(1)
			j.Metrics().AddDedupDropped(1)
		}
		if batch.Len() >= batchSize {
			return flush()
		}
		return nil
	})
	if ferr := flush(); ferr != nil && err == nil {
		err = ferr
	}
	// The API adapter keeps its own counters; the /jobs snapshot reads JobMetrics,
	// so without this the API jobs reported bytes_read=0, http_requests=0.
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
		"bytes_read", res.BytesRead, "http_requests", res.HTTPRequests,
		"stopped", res.Stopped, "elapsed", res.Elapsed.Round(time.Millisecond))
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
	instRes, err := ingest.Run(ctx, plan.Institutions, &ingest.S3InstitutionsProcessor{
		Client: a.cfg.S3, Sink: a.cfg.Sink, Metrics: j.Metrics(), ReadOptions: readOpts,
	}, opts)
	if err != nil {
		return fmt.Errorf("app: sync institutions: %w", err)
	}
	a.logger.Info("sync-dims: finished", "job", j.ID(),
		"topics_rows", topicsRes.Written, "institutions_rows", instRes.Written,
		"bytes_read", topicsRes.BytesRead+instRes.BytesRead)
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
