package ingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	s3 "go-etl/internal/adapters/s3"
	"go-etl/internal/metrics"
	"go-etl/internal/normalizer"
	"go-etl/internal/pipeline"
	"go-etl/internal/sink"
)

// JobFile is one manifest file of the job. Works and Topics share the single
// writer and the single portions channel.
type JobFile struct {
	Entity Entity
	Entry  s3.ManifestEntry
}

type WriterJobOptions struct {
	Client          *s3.Client
	Normalizer      *normalizer.Normalizer
	Pool            *sink.WorksBatchPool
	ReadOptions     s3.ParquetReadOptions
	Workers         int
	AttemptsPerFile int
	Files           []JobFile
	Writer          *Writer
	Metrics         *metrics.JobMetrics
	Logger          *slog.Logger
}

// RunWriterJob feeds files to readers, routes their batches through one
// bounded channel and lets a single Writer perform every INSERT and checkpoint.
func RunWriterJob(ctx context.Context, opts WriterJobOptions) (Result, error) {
	if opts.Writer == nil {
		return Result{}, errors.New("ingest: nil writer")
	}
	if opts.Workers <= 0 {
		opts.Workers = DefaultWorkers
	}
	if opts.AttemptsPerFile <= 0 {
		opts.AttemptsPerFile = DefaultAttempts
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	res := Result{}
	if len(opts.Files) == 0 {
		written, err := opts.Writer.Run(ctx, closedPortions())
		res.Written = written.Written
		res.DedupDropped = written.DedupDropped
		res.FilesDone = written.FilesDone
		return res, err
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	portions := make(chan Portion, opts.Workers*2)
	type writerOutcome struct {
		result Result
		err    error
	}
	writerDone := make(chan writerOutcome, 1)
	go func() {
		r, err := opts.Writer.Run(runCtx, portions)
		writerDone <- writerOutcome{result: r, err: err}
	}()

	files := make(chan JobFile)
	go func() {
		defer close(files)
		for _, f := range opts.Files {
			select {
			case files <- f:
			case <-runCtx.Done():
				return
			}
		}
	}()

	type fileFailure struct {
		url string
		err error
	}
	var (
		mu        sync.Mutex
		workerRes s3.FileReadResult
		failures  []fileFailure
		wg        sync.WaitGroup
	)
	recordSuccess := func(r s3.FileReadResult) {
		mu.Lock()
		workerRes.ScannedRows += r.ScannedRows
		workerRes.MatchedRows += r.MatchedRows
		workerRes.RejectedRows += r.RejectedRows
		workerRes.RestoredYears += r.RestoredYears
		workerRes.BytesRead += r.BytesRead
		workerRes.HTTPRequests += r.HTTPRequests
		if workerRes.RejectedByReason == nil && len(r.RejectedByReason) > 0 {
			workerRes.RejectedByReason = make(map[string]uint64, len(r.RejectedByReason))
		}
		for reason, n := range r.RejectedByReason {
			workerRes.RejectedByReason[reason] += n
		}
		mu.Unlock()
	}
	recordFailure := func(url string, err error) {
		mu.Lock()
		failures = append(failures, fileFailure{url: url, err: err})
		mu.Unlock()
	}

	worker := func() {
		defer wg.Done()
		for file := range files {
			if runCtx.Err() != nil {
				return
			}
			result, err := processFile(runCtx, opts, file, portions)
			if err != nil {
				if runCtx.Err() != nil {
					return
				}
				recordFailure(file.Entry.URL, err)
				cancel() // a required file failed: stop the feed and the job
				return
			}
			recordSuccess(result)
		}
	}

	wg.Add(opts.Workers)
	for i := 0; i < opts.Workers; i++ {
		go worker()
	}
	wg.Wait()
	close(portions)
	out := <-writerDone

	mu.Lock()
	stats := workerRes
	failed := append([]fileFailure(nil), failures...)
	mu.Unlock()

	res.Scanned = stats.ScannedRows
	res.Matched = stats.MatchedRows
	res.Rejected = stats.RejectedRows
	res.RestoredYears = stats.RestoredYears
	res.BytesRead = stats.BytesRead
	res.HTTPRequests = stats.HTTPRequests
	res.Written = out.result.Written
	res.WorksWritten = out.result.WorksWritten
	res.TopicsWritten = out.result.TopicsWritten
	res.DedupDropped = out.result.DedupDropped
	res.FilesDone = out.result.FilesDone
	res.BatchesWritten = out.result.BatchesWritten
	res.FilesFailed = len(failed)
	res.FilesSkipped = out.result.FilesSkipped
	res.Stopped = ctx.Err() != nil && out.err == nil
	if opts.Metrics != nil {
		for reason, n := range stats.RejectedByReason {
			opts.Metrics.AddRejectedN(reason, n)
		}
	}

	if out.err != nil {
		return res, out.err
	}
	if len(failed) > 0 {
		first := failed[0]
		return res, fmt.Errorf("%w: required file %s failed after %d attempt(s): %v",
			pipeline.ErrCorpusIncomplete, first.url, opts.AttemptsPerFile, first.err)
	}
	if !res.Stopped && res.FilesSkipped > 0 {
		return res, fmt.Errorf("%w: %d file(s) reached EOF without a confirmed checkpoint",
			pipeline.ErrCorpusIncomplete, res.FilesSkipped)
	}
	return res, nil
}

func processFile(runCtx context.Context, opts WriterJobOptions, file JobFile, portions chan<- Portion) (s3.FileReadResult, error) {
	var lastErr error
	var lastRes s3.FileReadResult
	for attempt := 1; attempt <= opts.AttemptsPerFile; attempt++ {
		if runCtx.Err() != nil {
			return lastRes, runCtx.Err()
		}
		var err error
		switch file.Entity {
		case EntityWorks:
			lastRes, err = s3.StreamWorksFile(runCtx, opts.Client, file.Entry, opts.Normalizer, opts.Pool, opts.ReadOptions,
				func(ctx context.Context, b *sink.WorksBatch) error {
					select {
					case portions <- Portion{Entity: EntityWorks, ObjectKey: file.Entry.URL, Attempt: attempt, Works: b}:
						return nil
					case <-runCtx.Done():
						opts.Pool.Put(b)
						return runCtx.Err()
					}
				})
		case EntityTopics:
			lastRes, err = s3.StreamTopicsFile(runCtx, opts.Client, file.Entry, opts.ReadOptions,
				func(ctx context.Context, b *sink.TopicsBatch) error {
					select {
					case portions <- Portion{Entity: EntityTopics, ObjectKey: file.Entry.URL, Attempt: attempt, Topics: b}:
						return nil
					case <-runCtx.Done():
						return runCtx.Err()
					}
				})
		default:
			err = fmt.Errorf("ingest: unknown entity %q", file.Entity)
		}
		if err == nil {
			select {
			case portions <- Portion{Entity: file.Entity, ObjectKey: file.Entry.URL, Attempt: attempt, EOF: true}:
				return lastRes, nil
			case <-runCtx.Done():
				return lastRes, runCtx.Err()
			}
		}
		lastErr = err
		if runCtx.Err() != nil {
			return lastRes, runCtx.Err()
		}
		if attempt < opts.AttemptsPerFile {
			opts.Logger.Warn("ingest: file read failed, retrying",
				"entity", file.Entity, "url", file.Entry.URL, "attempt", attempt, "err", err)
			if werr := sleepCtx(runCtx, backoffForAttempt(attempt)); werr != nil {
				return lastRes, werr
			}
		}
	}
	return lastRes, lastErr
}

func closedPortions() <-chan Portion {
	ch := make(chan Portion)
	close(ch)
	return ch
}
