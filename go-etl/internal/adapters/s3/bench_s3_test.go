//go:build inspect

package s3

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	kgzip "github.com/klauspost/compress/gzip"

	"go-etl/internal/normalizer"
	"go-etl/internal/sink"
)

type countingReader struct {
	r     io.Reader
	bytes atomic.Uint64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	if n > 0 {
		c.bytes.Add(uint64(n))
	}
	return n, err
}

type s3BenchRun struct {
	WallMs          float64
	BytesDownloaded uint64
	HTTPRequests    uint64
	CacheHits       uint64
	MBPerSec        float64
	PeakRAMMiB      float64
	ScannedRows     uint64
	WrittenRows     uint64
}

func startPeakRAMSampler() func() float64 {
	var peak atomic.Uint64
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	peak.Store(ms.HeapInuse)

	stopCh := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		var sample runtime.MemStats
		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				runtime.ReadMemStats(&sample)
				for {
					cur := peak.Load()
					if sample.HeapInuse <= cur || peak.CompareAndSwap(cur, sample.HeapInuse) {
						break
					}
				}
			}
		}
	}()

	return func() float64 {
		close(stopCh)
		wg.Wait()
		var final runtime.MemStats
		runtime.ReadMemStats(&final)
		best := max(peak.Load(), final.HeapInuse)
		return float64(best) / float64(1<<20)
	}
}

func processJSONLGZFile(
	ctx context.Context,
	client *Client,
	entry ManifestEntry,
	norm *normalizer.Normalizer,
	pool *sink.WorksBatchPool,
	outSink sink.BatchSink,
	batchSize int,
) (FileReadResult, error) {
	res := FileReadResult{
		URL:      entry.URL,
		FileSize: entry.Meta.ContentLength,
	}

	bucket, key, err := ParseS3URL(entry.URL)
	if err != nil {
		return res, err
	}

	out, err := client.GetObject(ctx, bucket, key)
	if err != nil {
		return res, err
	}
	defer out.Body.Close()

	cr := &countingReader{r: out.Body}
	gz, err := kgzip.NewReader(cr)
	if err != nil {
		return res, fmt.Errorf("gzip reader %s: %w", entry.URL, err)
	}
	defer gz.Close()

	scanner := bufio.NewScanner(gz)
	scanner.Buffer(make([]byte, 256<<10), 16<<20)

	batch := pool.Get()
	defer func() {
		if batch != nil {
			pool.Put(batch)
		}
	}()

	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		res.ScannedRows++
		outcome, err := norm.AppendRawJSONWork(line, batch)
		if err != nil {
			continue
		}
		switch outcome {
		case normalizer.OutcomeAdded:
			res.MatchedRows++
			if batch.Len() >= batchSize {
				if err := outSink.WriteWorksBatch(ctx, batch); err != nil {
					return res, err
				}
				res.WrittenRows += uint64(batch.Len())
				pool.Put(batch)
				batch = pool.Get()
			}
		case normalizer.OutcomeDuplicate:
			res.MatchedRows++
			res.DedupDropped++
		}
	}
	if err := scanner.Err(); err != nil {
		return res, fmt.Errorf("scan jsonl.gz %s: %w", entry.URL, err)
	}

	if batch.Len() > 0 {
		if err := outSink.WriteWorksBatch(ctx, batch); err != nil {
			return res, err
		}
		res.WrittenRows += uint64(batch.Len())
	}

	res.BytesRead = cr.bytes.Load()
	res.HTTPRequests = 1
	return res, nil
}

func findMatchingJSONLEntry(parquetURL string, jsonlByURL map[string]ManifestEntry) (ManifestEntry, bool) {
	jsonlURL := strings.Replace(parquetURL, "/data/parquet/works/", "/data/jsonl/works/", 1)
	jsonlURL = strings.TrimSuffix(jsonlURL, ".parquet") + ".gz"
	e, ok := jsonlByURL[jsonlURL]
	return e, ok
}

func runMedian(t *testing.T, runs int, fn func() (FileReadResult, error)) s3BenchRun {
	t.Helper()
	if runs < 1 {
		runs = 1
	}

	results := make([]s3BenchRun, 0, runs)
	for i := range runs {
		runtime.GC()
		stopRAM := startPeakRAMSampler()
		start := time.Now()

		res, err := fn()
		elapsed := time.Since(start)
		peakRAM := stopRAM()
		if err != nil {
			t.Fatalf("benchmark run %d/%d failed: %v", i+1, runs, err)
		}

		mbPerSec := 0.0
		if elapsed > 0 {
			mbPerSec = (float64(res.BytesDownloadedOrZero()) / float64(1<<20)) / elapsed.Seconds()
		}
		results = append(results, s3BenchRun{
			WallMs:          float64(elapsed.Microseconds()) / 1000.0,
			BytesDownloaded: res.BytesRead,
			HTTPRequests:    res.HTTPRequests,
			CacheHits:       res.CacheHits,
			MBPerSec:        mbPerSec,
			PeakRAMMiB:      peakRAM,
			ScannedRows:     res.ScannedRows,
			WrittenRows:     res.WrittenRows,
		})
	}

	slices.SortFunc(results, func(a, b s3BenchRun) int {
		switch {
		case a.WallMs < b.WallMs:
			return -1
		case a.WallMs > b.WallMs:
			return 1
		default:
			return 0
		}
	})
	return results[len(results)/2]
}

func (r FileReadResult) BytesDownloadedOrZero() uint64 {
	return r.BytesRead
}

func parseBenchSizesMB(defaultSizes []int64) []int64 {
	raw := strings.TrimSpace(os.Getenv("BENCH_S3_SIZES_MB"))
	if raw == "" {
		return defaultSizes
	}
	var out []int64
	for part := range strings.SplitSeq(raw, ",") {
		v, err := strconv.ParseInt(strings.TrimSpace(part), 10, 64)
		if err == nil && v > 0 {
			out = append(out, v)
		}
	}
	if len(out) == 0 {
		return defaultSizes
	}
	return out
}

func parseBenchIntEnv(key string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v <= 0 {
		return fallback
	}
	return v
}

// TestBenchS3FormatsAndThreshold runs Metric #3 Part A:
// JSONL.gz vs Parquet Full GET vs Parquet Buffered Range across the file-size ladder.
//
// Env knobs:
//   - BENCH_S3_SIZES_MB="2,5,15,50,150,400"  (override for quick run, e.g. "2,5,15,50")
//   - BENCH_S3_RUNS=3                        (median of N runs; 3-5 recommended)
//   - BENCH_S3_RANGE_BUF_KB=2048             (sliding buffer size for Buffered Range)
func TestBenchS3FormatsAndThreshold(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	runs := parseBenchIntEnv("BENCH_S3_RUNS", 3)
	rangeBufBytes := int64(parseBenchIntEnv("BENCH_S3_RANGE_BUF_KB", 2048)) << 10
	sizeLadderMB := parseBenchSizesMB([]int64{2, 5, 15, 50, 150, 400})

	client, err := NewAnonymousClient(ctx)
	if err != nil {
		t.Fatalf("NewAnonymousClient: %v", err)
	}

	pqManifest, err := client.FetchManifest(ctx, ParquetManifestKey("works"))
	if err != nil {
		t.Fatalf("FetchManifest(parquet works): %v", err)
	}
	jsonlManifest, err := client.FetchManifest(ctx, "data/jsonl/works/manifest.json")
	if err != nil {
		t.Fatalf("FetchManifest(jsonl works): %v", err)
	}

	jsonlByURL := make(map[string]ManifestEntry, len(jsonlManifest.Files))
	for _, e := range jsonlManifest.Files {
		jsonlByURL[e.URL] = e
	}

	const mib = 1 << 20
	targets := make([]int64, len(sizeLadderMB))
	for i, mb := range sizeLadderMB {
		targets[i] = mb * mib
	}
	picked := PickClosestBySize(pqManifest.Files, targets)

	var report strings.Builder
	fmt.Fprintf(&report, "\n=== Metric #3 Part A: JSONL.gz vs Parquet Full GET vs Parquet Buffered Range (buf=%s, median of %d runs) ===\n",
		formatBytes(rangeBufBytes), runs)
	fmt.Fprintf(&report, "| Target | File (Parquet size) | Mode | wall_ms | downloaded | %% of PQ | http_reqs | MB/s | peak_RAM | scanned | cs_written |\n")
	fmt.Fprintf(&report, "|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|\n")

	for i, pqEntry := range picked {
		jsonlEntry, hasJSONL := findMatchingJSONLEntry(pqEntry.URL, jsonlByURL)
		partName := ExtractPartition(pqEntry.URL)
		fileLabel := fmt.Sprintf("%s (%s)", partName, formatBytes(pqEntry.Meta.ContentLength))

		// 1) JSONL.gz baseline (on the exact matching partition file)
		if hasJSONL {
			medJSONL := runMedian(t, runs, func() (FileReadResult, error) {
				pool := sink.NewWorksBatchPool(sink.DefaultBatchCap, sink.DefaultMaxPooledCap)
				norm := normalizer.NewNormalizer(normalizer.NewWorkDeduplicator())
				noop := sink.NewNoopSink()
				return processJSONLGZFile(ctx, client, jsonlEntry, norm, pool, noop, sink.DefaultBatchCap)
			})
			pctPQ := 100 * float64(medJSONL.BytesDownloaded) / float64(max(pqEntry.Meta.ContentLength, 1))
			fmt.Fprintf(&report, "| %d MiB | %s | JSONL.gz | %.1f | %s | %.1f%% | %d | %.2f | %.1f MiB | %d | %d |\n",
				sizeLadderMB[i], fileLabel, medJSONL.WallMs, formatBytes(int64(medJSONL.BytesDownloaded)),
				pctPQ, medJSONL.HTTPRequests, medJSONL.MBPerSec, medJSONL.PeakRAMMiB, medJSONL.ScannedRows, medJSONL.WrittenRows)
		}

		// 2) Parquet Full GET
		medFull := runMedian(t, runs, func() (FileReadResult, error) {
			pool := sink.NewWorksBatchPool(sink.DefaultBatchCap, sink.DefaultMaxPooledCap)
			norm := normalizer.NewNormalizer(normalizer.NewWorkDeduplicator())
			noop := sink.NewNoopSink()
			opts := DefaultParquetReadOptions()
			opts.Mode = ReadModeFullGet
			return ProcessWorksFile(ctx, client, pqEntry, norm, pool, noop, opts)
		})
		pctFull := 100 * float64(medFull.BytesDownloaded) / float64(max(pqEntry.Meta.ContentLength, 1))
		fmt.Fprintf(&report, "| %d MiB | %s | Parquet Full GET | %.1f | %s | %.1f%% | %d | %.2f | %.1f MiB | %d | %d |\n",
			sizeLadderMB[i], fileLabel, medFull.WallMs, formatBytes(int64(medFull.BytesDownloaded)),
			pctFull, medFull.HTTPRequests, medFull.MBPerSec, medFull.PeakRAMMiB, medFull.ScannedRows, medFull.WrittenRows)

		// 3) Parquet Buffered Range
		medRange := runMedian(t, runs, func() (FileReadResult, error) {
			pool := sink.NewWorksBatchPool(sink.DefaultBatchCap, sink.DefaultMaxPooledCap)
			norm := normalizer.NewNormalizer(normalizer.NewWorkDeduplicator())
			noop := sink.NewNoopSink()
			opts := DefaultParquetReadOptions()
			opts.Mode = ReadModeRange
			opts.RangeOpts = RangeOptions{
				BufferSize:      rangeBufBytes,
				FooterCacheSize: DefaultFooterCacheSize,
				MaxRetries:      DefaultRangeMaxRetries,
			}
			return ProcessWorksFile(ctx, client, pqEntry, norm, pool, noop, opts)
		})
		pctRange := 100 * float64(medRange.BytesDownloaded) / float64(max(pqEntry.Meta.ContentLength, 1))
		fmt.Fprintf(&report, "| %d MiB | %s | Parquet Range (%s) | %.1f | %s | %.1f%% | %d | %.2f | %.1f MiB | %d | %d |\n",
			sizeLadderMB[i], fileLabel, formatBytes(rangeBufBytes), medRange.WallMs, formatBytes(int64(medRange.BytesDownloaded)),
			pctRange, medRange.HTTPRequests, medRange.MBPerSec, medRange.PeakRAMMiB, medRange.ScannedRows, medRange.WrittenRows)

		t.Logf("Finished %d MiB point (%s): FullGET=%.1fms vs Range=%.1fms (Range saved %.1f%% traffic, %d reqs)",
			sizeLadderMB[i], fileLabel, medFull.WallMs, medRange.WallMs, 100-pctRange, medRange.HTTPRequests)
	}

	t.Log(report.String())
}

// TestBenchS3RangeBufferSizes runs Metric #3 Part B:
// Compares Range buffer sizes: Unbuffered (0) vs 512 KiB vs 2 MiB vs 4 MiB on one real file.
//
// Env knobs:
//   - BENCH_S3_BUF_TARGET_MB=50  (default 50 MiB file; set e.g. 15 or 50)
//   - BENCH_S3_RUNS=3            (median of N runs)
func TestBenchS3RangeBufferSizes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	runs := parseBenchIntEnv("BENCH_S3_RUNS", 3)
	targetMB := int64(parseBenchIntEnv("BENCH_S3_BUF_TARGET_MB", 50))

	client, err := NewAnonymousClient(ctx)
	if err != nil {
		t.Fatalf("NewAnonymousClient: %v", err)
	}
	pqManifest, err := client.FetchManifest(ctx, ParquetManifestKey("works"))
	if err != nil {
		t.Fatalf("FetchManifest(parquet works): %v", err)
	}

	picked := PickClosestBySize(pqManifest.Files, []int64{targetMB << 20})
	if len(picked) == 0 {
		t.Fatal("no file picked")
	}
	entry := picked[0]

	configs := []struct {
		label      string
		bufSize    int64
		footerSize int64
	}{
		{label: "Unbuffered (0 B)", bufSize: 0, footerSize: 0},
		{label: "Buffer 512 KiB", bufSize: 512 << 10, footerSize: DefaultFooterCacheSize},
		{label: "Buffer 2 MiB", bufSize: 2 << 20, footerSize: DefaultFooterCacheSize},
		{label: "Buffer 4 MiB", bufSize: 4 << 20, footerSize: DefaultFooterCacheSize},
	}

	var report strings.Builder
	fmt.Fprintf(&report, "\n=== Metric #3 Part B: Range Buffer Size Comparison on %s (%s, median of %d runs) ===\n",
		entry.URL, formatBytes(entry.Meta.ContentLength), runs)
	fmt.Fprintf(&report, "| Buffer Config | wall_ms | downloaded | %% of file | http_reqs | cache_hits | MB/s | peak_RAM | cs_written |\n")
	fmt.Fprintf(&report, "|---|---:|---:|---:|---:|---:|---:|---:|---:|\n")

	for _, cfg := range configs {
		med := runMedian(t, runs, func() (FileReadResult, error) {
			pool := sink.NewWorksBatchPool(sink.DefaultBatchCap, sink.DefaultMaxPooledCap)
			norm := normalizer.NewNormalizer(normalizer.NewWorkDeduplicator())
			noop := sink.NewNoopSink()
			opts := DefaultParquetReadOptions()
			opts.Mode = ReadModeRange
			opts.RangeOpts = RangeOptions{
				BufferSize:      cfg.bufSize,
				FooterCacheSize: cfg.footerSize,
				MaxRetries:      DefaultRangeMaxRetries,
			}
			return ProcessWorksFile(ctx, client, entry, norm, pool, noop, opts)
		})
		pctFile := 100 * float64(med.BytesDownloaded) / float64(max(entry.Meta.ContentLength, 1))
		fmt.Fprintf(&report, "| %s | %.1f | %s | %.1f%% | %d | %d | %.2f | %.1f MiB | %d |\n",
			cfg.label, med.WallMs, formatBytes(int64(med.BytesDownloaded)), pctFile,
			med.HTTPRequests, med.CacheHits, med.MBPerSec, med.PeakRAMMiB, med.WrittenRows)
		t.Logf("Finished %s: wall=%.1fms, downloaded=%s (%.1f%%), reqs=%d, hits=%d",
			cfg.label, med.WallMs, formatBytes(int64(med.BytesDownloaded)), pctFile, med.HTTPRequests, med.CacheHits)
	}

	t.Log(report.String())
}
