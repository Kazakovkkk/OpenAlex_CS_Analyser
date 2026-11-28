package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/pprof"
	"syscall"
	"time"

	"go-etl/internal/adapters/api"
	"go-etl/internal/adapters/s3"
	"go-etl/internal/app"
	"go-etl/internal/dotenv"
	"go-etl/internal/httpapi"
	"go-etl/internal/jobs"
	"go-etl/internal/loadtest"
	"go-etl/internal/sink"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if applied, path, derr := dotenv.LoadDefault(); derr != nil {
		logger.Warn("could not load .env", "err", derr)
	} else if path != "" {
		logger.Info("loaded dotenv", "path", path, "variables", applied)
	}
	if app.ResolveAPIKey("") != "" {
		logger.Info("openalex api key: present")
	}

	var err error
	switch os.Args[1] {
	case "serve":
		err = runServe(ctx, logger, os.Args[2:])
	case "ingest":
		err = runIngest(ctx, logger, os.Args[2:])
	case "sync-dims":
		err = runSyncDims(ctx, logger, os.Args[2:])
	case "loadtest":
		err = runLoadtest(ctx, logger, os.Args[2:])
	case "bench":
		err = runBench(ctx, logger, os.Args[2:])
	case "sample":
		err = runSample(ctx, logger, os.Args[2:])
	case "-h", "--help", "help":
		usage()
		return
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", os.Args[1])
		usage()
		os.Exit(2)
	}
	if err != nil {
		logger.Error("etl failed", "err", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `go-etl — OpenAlex -> ClickHouse ETL

usage:
  etl serve     --config PATH [--addr :8081] [--dry-run]
  etl ingest    --config PATH [--source s3|api] [--workers N] [--batch-size N]
                [--shard-index I --shard-total N] [--max-files N] [--max-records N]
                [--from-date YYYY-MM-DD] [--date-filter-mode updated|created|publication]
                [--api-key KEY | $OPENALEX_API_KEY] [--rps-limit N]
                [--full-get-threshold-mb N] [--dry-run]
  etl sync-dims --config PATH [--force-reload] [--dry-run]
  etl loadtest  [--config PATH] [--target-rps N] [--duration 30] [--workers N]
                [--batch-size N] [--sample PATH] [--dry-run]

A working config is required. --dry-run selects the diagnostic NoopSink
(and the load test uses works_loadtest when a config is given).
`)
}

func newS3Client(ctx context.Context) (*s3.Client, error) {
	return s3.NewAnonymousClient(ctx)
}

func startCPUProfile(path string) (func(), error) {
	if path == "" {
		return func() {}, nil
	}
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create cpu profile: %w", err)
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		f.Close()
		return nil, fmt.Errorf("start cpu profile: %w", err)
	}
	return func() {
		pprof.StopCPUProfile()
		f.Close()
	}, nil
}

func runServe(ctx context.Context, logger *slog.Logger, args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "path to config/app.toml (required unless --dry-run)")
	dryRun := fs.Bool("dry-run", false, "explicit diagnostic run: NoopSink, no state/checkpoints")
	addr := fs.String("addr", ":8081", "HTTP listen address")
	sample := fs.String("sample", loadtest.DefaultSamplePath, "local JSONL.gz for the load test")
	ckDir := fs.String("checkpoint-dir", filepath.Join("tmp", "checkpoints"), "diagnostic checkpoint directory")
	if err := fs.Parse(args); err != nil {
		return err
	}

	s3Client, err := newS3Client(ctx)
	if err != nil {
		return fmt.Errorf("s3 client: %w", err)
	}
	st, err := openStores(ctx, *cfgPath, logger, *dryRun)
	if err != nil {
		return fmt.Errorf("sink: %w", err)
	}
	defer st.Close()

	backend := app.New(app.Config{
		S3:            s3Client,
		API:           api.NewClient(api.Options{APIKey: app.ResolveAPIKey("")}),
		Sink:          st.sink,
		Pipeline:      st.pipeline,
		Checkpoints:   st.checkpoints,
		BuildKnownIDs: st.buildKnownIDs,
		DeleteWorks:   st.deleteWorks,
		APIKey:        app.ResolveAPIKey(""),
		CheckpointDir: *ckDir,
		SamplePath:    *sample,
		TestTarget:    st.testTarget,
		TargetHost:    st.targetHost,
		TargetPort:    st.targetPort,
		LoadtestSink:  st.loadtestSink,
		Keys:          api.NewKeyStore(),
	}, logger)

	manager := jobs.NewManager()
	srv := httpapi.New(manager, backend, httpapi.WithInstanceID(backend.InstanceID()))

	httpSrv := &http.Server{
		Addr:              *addr,
		Handler:           srv,
		ReadHeaderTimeout: 10 * time.Second,
	}
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", *addr, err)
	}
	logger.Info("http api listening", "addr", ln.Addr().String())

	serveErr := make(chan error, 1)
	go func() {
		err := httpSrv.Serve(ln)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
			return
		}
		serveErr <- nil
	}()

	select {
	case err := <-serveErr:
		return err
	case <-ctx.Done():
		logger.Info("shutting down: stopping jobs first")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	// Close the admission of new write jobs first, then stop the active job and
	// wait for its final flush (spec step 1.4).
	srv.BeginShutdown()
	manager.CloseAdmission()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		logger.Warn("http shutdown", "err", err)
	}
	if err := manager.StopAll(shutdownCtx); err != nil {
		logger.Warn("jobs did not stop cleanly", "err", err)
	}
	return nil
}

func runOneJob(ctx context.Context, logger *slog.Logger, manager *jobs.Manager, kind jobs.Kind, run jobs.Runner) error {
	j, err := manager.Start(ctx, kind, run)
	if err != nil {
		return err
	}
	logger.Info("job started", "job", j.ID(), "kind", string(kind))

	<-j.Done()
	switch j.State() {
	case jobs.StateCompleted:
		snap := j.Snapshot()
		logger.Info("job completed", "job", j.ID(), "scanned", snap.ScannedRecords,
			"written", snap.WrittenRecords, "elapsed_sec", snap.ElapsedSec)
		return nil
	case jobs.StateStopped:
		logger.Warn("job stopped by signal", "job", j.ID())
		return nil
	default:
		return fmt.Errorf("job %s failed: %w", j.ID(), j.Err())
	}
}

func runIngest(ctx context.Context, logger *slog.Logger, args []string) error {
	fs := flag.NewFlagSet("ingest", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "path to config/app.toml (required unless --dry-run)")
	dryRun := fs.Bool("dry-run", false, "explicit diagnostic run: NoopSink, no state/checkpoints")
	source := fs.String("source", "s3", "data source: s3 or api")
	workers := fs.Int("workers", 4, "parallel file workers")
	batchSize := fs.Int("batch-size", sink.DefaultBatchCap, "rows per batch")
	shardIndex := fs.Int("shard-index", 0, "shard index (0-based)")
	shardTotal := fs.Int("shard-total", 1, "number of shards")
	shard := fs.String("shard", "", "shard as k/N (1-based), an alternative to --shard-index/--shard-total")
	maxFiles := fs.Int("max-files", 0, "limit the number of files (0 = all)")
	maxRecords := fs.Int("max-records", 0, "limit the number of records (0 = all)")
	fromDate := fs.String("from-date", "", "YYYY-MM-DD lower bound for the API source")
	dateMode := fs.String("date-filter-mode", "updated", "updated or created")
	apiKey := fs.String("api-key", "", "OpenAlex API key for the api source (default: $OPENALEX_API_KEY)")
	rpsLimit := fs.Int("rps-limit", 20, "requests per second for the api source")
	fullGetMB := fs.Int("full-get-threshold-mb", 0, "files smaller than this use one Full GET (0 = library default)")
	ckDir := fs.String("checkpoint-dir", filepath.Join("tmp", "checkpoints"), "checkpoint directory")
	cpuProfile := fs.String("cpuprofile", "", "write a pprof CPU profile here (Метрика №1)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	stopProfile, err := startCPUProfile(*cpuProfile)
	if err != nil {
		return err
	}
	defer stopProfile()

	s3Client, err := newS3Client(ctx)
	if err != nil {
		return fmt.Errorf("s3 client: %w", err)
	}
	st, err := openStores(ctx, *cfgPath, logger, *dryRun)
	if err != nil {
		return fmt.Errorf("sink: %w", err)
	}
	defer st.Close()

	resolvedKey := app.ResolveAPIKey(*apiKey)
	backend := app.New(app.Config{
		S3:               s3Client,
		API:              api.NewClient(api.Options{APIKey: resolvedKey}),
		Sink:             st.sink,
		Pipeline:         st.pipeline,
		Checkpoints:      st.checkpoints,
		BuildKnownIDs:    st.buildKnownIDs,
		DeleteWorks:      st.deleteWorks,
		APIKey:           resolvedKey,
		CheckpointDir:    *ckDir,
		FullGetThreshold: int64(*fullGetMB) << 20,
		TestTarget:       st.testTarget,
		TargetHost:       st.targetHost,
		TargetPort:       st.targetPort,
	}, logger)
	manager := jobs.NewManager()

	resolvedShardIndex, resolvedShardTotal := *shardIndex, *shardTotal
	if *shard != "" {
		idx, total, serr := parseShard(*shard)
		if serr != nil {
			return serr
		}
		if *shardIndex != 0 || *shardTotal != 1 {
			return fmt.Errorf("--shard cannot be combined with --shard-index/--shard-total")
		}
		resolvedShardIndex, resolvedShardTotal = idx, total
	}

	req := httpapi.IngestRequest{
		Source: *source, Workers: *workers, BatchSize: *batchSize,
		ShardIndex: resolvedShardIndex, ShardTotal: resolvedShardTotal, MaxFiles: *maxFiles,
		FromDate: *fromDate, DateFilterMode: *dateMode,
		APIKey: resolvedKey, RPSLimit: *rpsLimit, MaxRecords: *maxRecords,
	}
	return runOneJob(ctx, logger, manager, jobs.KindIngest, func(ctx context.Context, j *jobs.Job) error {
		return backend.RunIngest(ctx, j, req)
	})
}

func runSyncDims(ctx context.Context, logger *slog.Logger, args []string) error {
	fs := flag.NewFlagSet("sync-dims", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "path to config/app.toml (required unless --dry-run)")
	dryRun := fs.Bool("dry-run", false, "explicit diagnostic run: NoopSink, no state/checkpoints")
	force := fs.Bool("force-reload", true, "reload dictionaries even if they look fresh")
	cpuProfile := fs.String("cpuprofile", "", "write a pprof CPU profile here")
	if err := fs.Parse(args); err != nil {
		return err
	}
	stopProfile, err := startCPUProfile(*cpuProfile)
	if err != nil {
		return err
	}
	defer stopProfile()

	s3Client, err := newS3Client(ctx)
	if err != nil {
		return fmt.Errorf("s3 client: %w", err)
	}
	st, err := openStores(ctx, *cfgPath, logger, *dryRun)
	if err != nil {
		return fmt.Errorf("sink: %w", err)
	}
	defer st.Close()

	backend := app.New(app.Config{S3: s3Client, Sink: st.sink, Pipeline: st.pipeline, Checkpoints: st.checkpoints,
		TestTarget: st.testTarget, TargetHost: st.targetHost, TargetPort: st.targetPort}, logger)
	manager := jobs.NewManager()
	return runOneJob(ctx, logger, manager, jobs.KindDimensions, func(ctx context.Context, j *jobs.Job) error {
		return backend.RunDimensions(ctx, j, httpapi.DimensionsRequest{ForceReload: *force})
	})
}

func runLoadtest(ctx context.Context, logger *slog.Logger, args []string) error {
	fs := flag.NewFlagSet("loadtest", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "config/app.toml; empty = diagnostic NoopSink")
	dryRun := fs.Bool("dry-run", false, "explicit diagnostic run: NoopSink")
	targetRPS := fs.Int("target-rps", loadtest.DefaultTargetRPS, "written rows per second")
	duration := fs.Int("duration", loadtest.DefaultDurationSec, "duration in seconds")
	workers := fs.Int("workers", loadtest.DefaultWorkers, "generator goroutines")
	batchSize := fs.Int("batch-size", loadtest.DefaultBatchSize, "rows per batch")
	sample := fs.String("sample", loadtest.DefaultSamplePath, "local JSONL.gz with real works")
	cpuProfile := fs.String("cpuprofile", "", "write a pprof CPU profile here (Метрика №1)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	// A load test without a config stays diagnostic; with a config it writes the
	// dedicated works_loadtest table.
	if *cfgPath == "" {
		*dryRun = true
	}
	st, err := openStores(ctx, *cfgPath, logger, *dryRun)
	if err != nil {
		return fmt.Errorf("sink: %w", err)
	}
	defer st.Close()
	stopProfile, err := startCPUProfile(*cpuProfile)
	if err != nil {
		return err
	}
	defer stopProfile()

	backend := app.New(app.Config{
		Sink:         st.sink,
		LoadtestSink: st.loadtestSink,
		SamplePath:   *sample,
	}, logger)
	manager := jobs.NewManager()
	return runOneJob(ctx, logger, manager, jobs.KindLoadTest, func(ctx context.Context, j *jobs.Job) error {
		return backend.RunLoadtest(ctx, j, httpapi.LoadtestRequest{
			TargetRPS: *targetRPS, DurationSec: *duration, Workers: *workers, BatchSize: *batchSize,
		})
	})
}
