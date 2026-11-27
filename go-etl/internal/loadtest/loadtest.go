package loadtest

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	"go-etl/internal/metrics"
	"go-etl/internal/normalizer"
	"go-etl/internal/sink"
)

const (
	DefaultSamplePath  = "tmp/works_sample_12k.jsonl.gz"
	DefaultRecords     = 5000
	DefaultTargetRPS   = 10000
	DefaultDurationSec = 30
	DefaultWorkers     = 4
	DefaultBatchSize   = 10000
)

type Config struct {
	SamplePath  string
	Records     int
	TargetRPS   int
	DurationSec int
	Workers     int
	BatchSize   int
}

func (c Config) withDefaults() Config {
	if c.SamplePath == "" {
		c.SamplePath = DefaultSamplePath
	}
	if c.Records <= 0 {
		c.Records = DefaultRecords
	}
	if c.TargetRPS <= 0 {
		c.TargetRPS = DefaultTargetRPS
	}
	if c.DurationSec <= 0 {
		c.DurationSec = DefaultDurationSec
	}
	if c.Workers <= 0 {
		c.Workers = DefaultWorkers
	}
	if c.BatchSize <= 0 {
		c.BatchSize = DefaultBatchSize
	}
	return c
}

type Result struct {
	Records      int
	Workers      int
	BatchSize    int
	TargetRPS    int
	DurationSec  int
	Batches      uint64
	Scanned      uint64
	Matched      uint64
	Written      uint64
	DedupDropped uint64
	Rejected     uint64
	Elapsed      time.Duration
	ActualRPS    float64
	P50BatchMs   float64
	P95BatchMs   float64
	P99BatchMs   float64
	AvgBatchMs   float64
	Stopped      bool

	SampleScanned int
}

func LoadSample(path string, limit int) ([][]byte, int, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, fmt.Errorf("loadtest: open sample %s: %w", path, err)
	}
	defer f.Close()

	gz, err := gzip.NewReader(f)
	if err != nil {
		return nil, 0, fmt.Errorf("loadtest: gzip %s: %w", path, err)
	}
	defer gz.Close()

	if limit <= 0 {
		limit = DefaultRecords
	}
	out := make([][]byte, 0, limit)
	scanned := 0
	sc := bufio.NewScanner(gz)
	sc.Buffer(make([]byte, 256<<10), 32<<20)
	for sc.Scan() && len(out) < limit {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) < 2 || line[0] != '{' {
			continue
		}
		scanned++
		if !normalizer.FastJSONPreFilter(line) {
			continue
		}
		rec := make([]byte, len(line))
		copy(rec, line)
		out = append(out, rec)
	}
	if err := sc.Err(); err != nil {
		return out, scanned, fmt.Errorf("loadtest: scan sample: %w", err)
	}
	if len(out) == 0 {
		return nil, scanned, fmt.Errorf("loadtest: sample %s has %d JSON records but none passed the CS pre-filter", path, scanned)
	}
	return out, scanned, nil
}

func Run(ctx context.Context, cfg Config, out sink.BatchSink, jm *metrics.JobMetrics) (Result, error) {
	cfg = cfg.withDefaults()
	records, scanned, err := LoadSample(cfg.SamplePath, cfg.Records)
	if err != nil {
		return Result{}, err
	}
	res, err := RunWithRecords(ctx, cfg, records, out, jm)
	res.SampleScanned = scanned
	return res, err
}

func RunWithRecords(ctx context.Context, cfg Config, records [][]byte, out sink.BatchSink, jm *metrics.JobMetrics) (Result, error) {
	if out == nil {
		return Result{}, errors.New("loadtest: nil sink")
	}
	if len(records) == 0 {
		return Result{}, errors.New("loadtest: no records")
	}
	cfg = cfg.withDefaults()
	base := Result{
		Records:     len(records),
		Workers:     cfg.Workers,
		BatchSize:   cfg.BatchSize,
		TargetRPS:   cfg.TargetRPS,
		DurationSec: cfg.DurationSec,
	}

	norm := normalizer.NewNormalizer(nil, normalizer.WithSnapshotYear(time.Now().UTC().Year()))
	pool := sink.NewWorksBatchPool(cfg.BatchSize, max(cfg.BatchSize, sink.DefaultMaxPooledCap))

	runCtx, cancel := context.WithTimeout(ctx, time.Duration(cfg.DurationSec)*time.Second)
	defer cancel()

	limiter := rate.NewLimiter(rate.Limit(cfg.TargetRPS), max(cfg.BatchSize, 1))

	var (
		mu          sync.Mutex
		batches     uint64
		scanned     uint64
		matched     uint64
		written     uint64
		deduped     uint64
		rejected    uint64
		next        atomic.Uint64
		start       = time.Now()
		fatalErr    error
		deadlineHit atomic.Bool
		wg          sync.WaitGroup
	)

	worker := func() {
		defer wg.Done()
		batch := pool.Get()
		for {
			if runCtx.Err() != nil {
				break
			}
			batch.Reset()
			var batchScanned, batchMatched, batchDedup, batchRejected uint64
			for batch.Len() < cfg.BatchSize {
				if runCtx.Err() != nil {
					break
				}
				idx := next.Add(1) - 1
				raw := records[int(idx%uint64(len(records)))]
				batchScanned++
				outcome, _ := norm.IngestRawJSON(raw, batch)
				switch outcome {
				case normalizer.OutcomeRejected:
					batchRejected++
				case normalizer.OutcomeAdded:
					batchMatched++
				case normalizer.OutcomeDuplicate:
					batchMatched++
					batchDedup++
				}
			}
			if batch.Len() == 0 {
				continue
			}
			// A deadline hit must not drop the rows already in the batch: the job
			// context is cancelled, so the final write gets its own deadline.
			writeCtx := runCtx
			var flushCancel context.CancelFunc
			if err := limiter.WaitN(runCtx, batch.Len()); err != nil {
				deadlineHit.Store(true)
				writeCtx, flushCancel = context.WithTimeout(context.WithoutCancel(runCtx), 15*time.Second)
			}

			flushStart := time.Now()
			if err := out.WriteWorksBatch(writeCtx, batch); err != nil {
				if runCtx.Err() == nil {
					mu.Lock()
					if fatalErr == nil {
						fatalErr = fmt.Errorf("loadtest: sink write: %w", err)
					}
					mu.Unlock()
				}
				if flushCancel != nil {
					flushCancel()
				}
				break
			}
			flushDur := time.Since(flushStart)
			if jm != nil {
				jm.RecordBatchDuration(flushDur)
				jm.AddScanned(batchScanned)
				jm.AddMatched(batchMatched)
				if batchDedup > 0 {
					jm.AddDedupDropped(batchDedup)
				}
				jm.AddWritten(uint64(batch.Len()), uint64(batch.ApproxBytes()))
			}

			mu.Lock()
			batches++
			scanned += batchScanned
			matched += batchMatched
			written += uint64(batch.Len())
			deduped += batchDedup
			rejected += batchRejected
			mu.Unlock()

			if flushCancel != nil {
				flushCancel()
				break
			}

			select {
			case <-runCtx.Done():
			default:
			}
		}
		pool.Put(batch)
	}

	wg.Add(cfg.Workers)
	for range cfg.Workers {
		go worker()
	}
	wg.Wait()

	base.Elapsed = time.Since(start)
	if ctx.Err() != nil || runCtx.Err() != nil || deadlineHit.Load() {
		base.Stopped = true
	}
	mu.Lock()
	base.Batches = batches
	base.Scanned = scanned
	base.Matched = matched
	base.Written = written
	base.DedupDropped = deduped
	base.Rejected = rejected
	err := fatalErr
	mu.Unlock()
	if err := ctx.Err(); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		return base, err
	}
	if base.Elapsed > 0 {
		base.ActualRPS = float64(base.Written) / base.Elapsed.Seconds()
	}
	if jm != nil {
		snap := jm.Snapshot()
		base.AvgBatchMs = snap.AvgBatchMs
		base.P50BatchMs = snap.P50BatchMs
		base.P95BatchMs = snap.P95BatchMs
		base.P99BatchMs = snap.P99BatchMs
	}
	return base, err
}

func (r Result) MarshalJSON() ([]byte, error) {
	type alias Result
	return json.Marshal(alias(r))
}

var _ = io.Discard
