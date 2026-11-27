package ingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	s3 "go-etl/internal/adapters/s3"
	"go-etl/internal/metrics"
)

type Entry = s3.ManifestEntry

type FileResult = s3.FileReadResult

type Processor interface {
	Process(ctx context.Context, entry Entry, limit int64) (FileResult, error)
}

type OrderMode string

const (
	OrderManifest     OrderMode = "manifest"
	OrderSmallFirst   OrderMode = "small-first"
	OrderLargestFirst OrderMode = "largest-first"
	OrderBalanced     OrderMode = "balanced"
)

const (
	DefaultWorkers       = 4
	DefaultAttempts      = 2
	defaultFailStreak    = 10
	fileRetryBaseBackoff = 300 * time.Millisecond
)

type Options struct {
	Workers            int
	MaxFiles           int
	AttemptsPerFile    int
	Order              OrderMode
	CheckpointPath     string
	Checkpoints        Ledger
	JobID              string
	Metrics            *metrics.JobMetrics
	FatalFailureStreak int
	Logger             *slog.Logger
	MaxRecords         int64
}

func (o Options) withDefaults() Options {
	if o.Workers <= 0 {
		o.Workers = DefaultWorkers
	}
	if o.AttemptsPerFile <= 0 {
		o.AttemptsPerFile = DefaultAttempts
	}
	if o.Order == "" {
		o.Order = OrderBalanced
	}
	if o.FatalFailureStreak == 0 {
		o.FatalFailureStreak = defaultFailStreak
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	return o
}

type FileFailure struct {
	URL      string
	Attempts int
	Err      error
}

func (f FileFailure) Error() string {
	if f.Err == nil {
		return f.URL
	}
	return fmt.Sprintf("%s: %v", f.URL, f.Err)
}

func (f FileFailure) Unwrap() error { return f.Err }

type Result struct {
	JobID string

	FilesTotal   int
	FilesSkipped int
	FilesDone    int
	FilesFailed  int

	FailedFiles []FileFailure

	Scanned      uint64
	Matched      uint64
	Written      uint64
	DedupDropped uint64
	Rejected     uint64

	BytesRead    uint64
	HTTPRequests uint64
	PlanBytes    int64

	Stopped   bool
	Truncated bool
	Elapsed   time.Duration
	LastDone  string
}

func Run(ctx context.Context, entries []Entry, p Processor, opts Options) (Result, error) {
	res := Result{JobID: opts.JobID}
	if p == nil {
		return res, errors.New("ingest: nil processor")
	}
	opts = opts.withDefaults()
	logger := opts.Logger
	res.JobID = opts.JobID

	ordered := SortEntries(entries, opts.Order)
	if opts.MaxFiles > 0 && len(ordered) > opts.MaxFiles {
		ordered = ordered[:opts.MaxFiles]
	}
	res.FilesTotal = len(ordered)
	if res.FilesTotal == 0 {
		return res, nil
	}

	var ledger Ledger
	if opts.Checkpoints != nil {
		ledger = opts.Checkpoints
	} else if opts.CheckpointPath != "" {
		fileLedger, err := newFileLedger(opts.CheckpointPath)
		if err != nil {
			return res, fmt.Errorf("ingest: open checkpoint: %w", err)
		}
		defer func() {
			if cerr := fileLedger.Close(); cerr != nil {
				logger.Warn("ingest: close checkpoint", "err", cerr)
			}
		}()
		ledger = fileLedger
	}
	if ledger != nil {
		done, err := ledger.LoadDone()
		if err != nil {
			return res, fmt.Errorf("ingest: load checkpoint: %w", err)
		}
		if len(done) > 0 {
			kept := ordered[:0]
			for _, e := range ordered {
				if _, ok := done[e.URL]; ok {
					res.FilesSkipped++
					continue
				}
				kept = append(kept, e)
			}
			ordered = kept
			logger.Info("ingest: resuming from checkpoint",
				"job", opts.JobID, "already_done", res.FilesSkipped, "remaining", len(ordered))
		}
	}
	if len(ordered) == 0 {
		return res, nil
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	start := time.Now()
	files := make(chan Entry, opts.Workers)

	var (
		mu           sync.Mutex
		failStreak   int
		fatalErr     error
		writtenTotal atomic.Uint64
		wg           sync.WaitGroup
	)

	recordFailure := func(f FileFailure) {
		mu.Lock()
		res.FilesFailed++
		res.FailedFiles = append(res.FailedFiles, f)
		failStreak++
		streak := failStreak
		if opts.FatalFailureStreak > 0 && streak >= opts.FatalFailureStreak && fatalErr == nil {
			fatalErr = fmt.Errorf("ingest: %d consecutive file failures, last: %w", streak, f)
		}
		shouldAbort := fatalErr != nil
		mu.Unlock()
		if shouldAbort {
			logger.Error("ingest: aborting after consecutive failures", "streak", streak, "err", f.Err)
			cancel()
		}
	}

	recordSuccess := func(entry Entry, r FileResult) {
		mu.Lock()
		res.FilesDone++
		res.Scanned += r.ScannedRows
		res.Matched += r.MatchedRows
		res.Written += r.WrittenRows
		res.DedupDropped += r.DedupDropped
		res.Rejected += r.RejectedRows
		res.BytesRead += r.BytesRead
		res.HTTPRequests += r.HTTPRequests
		res.PlanBytes += r.PrefetchPlanBytes
		res.LastDone = entry.URL
		writtenTotal.Add(r.WrittenRows)
		failStreak = 0
		mu.Unlock()
	}

	remainingBudget := func() int64 {
		if opts.MaxRecords <= 0 {
			return 0
		}
		done := writtenTotal.Load()
		if done >= uint64(opts.MaxRecords) {
			return 0
		}
		return int64(uint64(opts.MaxRecords) - done)
	}

	if bs, ok := p.(interface{ SetRowBudget(*s3.RowBudget) }); ok && opts.MaxRecords > 0 {
		bs.SetRowBudget(s3.NewRowBudget(opts.MaxRecords))
	}

	worker := func() {
		defer wg.Done()
		for entry := range files {
			if runCtx.Err() != nil {
				return
			}
			if opts.Metrics != nil {
				opts.Metrics.SetCurrentItem(entry.URL)
			}

			limit := remainingBudget()
			if opts.MaxRecords > 0 && limit == 0 {
				return
			}
			var lastErr error
			done := false
			for attempt := 1; attempt <= opts.AttemptsPerFile; attempt++ {
				fileRes, err := p.Process(runCtx, entry, limit)
				if err == nil {
					recordSuccess(entry, fileRes)
					if ledger != nil && !fileRes.Truncated {
						if cerr := ledger.MarkDone(entry.URL); cerr != nil {
							logger.Warn("ingest: checkpoint write failed", "url", entry.URL, "err", cerr)
						}
					}
					done = true
					break
				}
				lastErr = err
				if runCtx.Err() != nil {
					break
				}
				if attempt < opts.AttemptsPerFile {
					logger.Warn("ingest: file failed, retrying",
						"url", entry.URL, "attempt", attempt, "err", err)
					if serr := sleepCtx(runCtx, backoffForAttempt(attempt)); serr != nil {
						break
					}
				}
			}
			if done {
				continue
			}
			if runCtx.Err() != nil {
				return
			}
			recordFailure(FileFailure{URL: entry.URL, Attempts: opts.AttemptsPerFile, Err: lastErr})
		}
	}

	wg.Add(opts.Workers)
	for range opts.Workers {
		go worker()
	}

feed:
	for _, e := range ordered {
		if opts.MaxRecords > 0 && writtenTotal.Load() >= uint64(opts.MaxRecords) {
			mu.Lock()
			res.Truncated = true
			mu.Unlock()
			logger.Info("ingest: max_records budget reached, stopping the feed",
				"job", opts.JobID, "written", writtenTotal.Load(), "budget", opts.MaxRecords)
			break feed
		}
		select {
		case files <- e:
		case <-runCtx.Done():
			break feed
		}
	}
	close(files)
	wg.Wait()

	res.Elapsed = time.Since(start)
	if ctx.Err() != nil {
		res.Stopped = true
	}

	mu.Lock()
	fatal := fatalErr
	mu.Unlock()
	if fatal != nil {
		return res, fatal
	}
	logger.Info("ingest: run finished",
		"job", res.JobID, "done", res.FilesDone, "failed", res.FilesFailed,
		"skipped", res.FilesSkipped, "stopped", res.Stopped,
		"rejected", res.Rejected, "elapsed", res.Elapsed.Round(time.Millisecond))
	return res, nil
}

func backoffForAttempt(attempt int) time.Duration {
	d := fileRetryBaseBackoff << min(attempt-1, 5)
	return min(d, 30*time.Second)
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
