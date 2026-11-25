//go:build inspect

package s3

// Metric #3 v2: JSONL.gz vs Parquet Full GET vs Parquet Range+prefetch,
// compared on WHOLE PARTITIONS.
//
// Why partitions and not files: Diag 0b proved that `part_0071.parquet` and
// `part_0071.gz` inside the same `updated_date=` partition hold DIFFERENT works
// (only 27 524 of 176 609 ids in common) — the two writers shard the day
// differently. The manifests, however, report the SAME record_count for a
// partition in both formats (396/396 partitions), so a whole partition is the
// smallest unit where "same works" is guaranteed.
//
// The test also prints the projection floor computed from the parquet footer, so
// the Range row can be read as "% of the theoretical minimum traffic".
//
// Quick pass:
//
//	BENCH_PART_SIZES_MB="5,50" BENCH_PART_RUNS=1 go test -v -count=1 -timeout 30m \
//	  -tags=inspect ./internal/adapters/s3 -run TestBenchPartitionPairedFormats
//
// Full pass for the report:
//
//	BENCH_PART_RUNS=3 go test -v -count=1 -timeout 180m \
//	  -tags=inspect ./internal/adapters/s3 -run TestBenchPartitionPairedFormats
import (
	"context"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"go-etl/internal/normalizer"
	"go-etl/internal/sink"
)

type benchPartition struct {
	key     string
	parquet []ManifestEntry
	jsonl   []ManifestEntry

	pqBytes   int64
	jBytes    int64
	pqRecords int64
	jRecords  int64
}

func (p benchPartition) jsonlRatio() float64 {
	return float64(p.jBytes) / float64(max(p.pqBytes, 1))
}

func benchGroupByPartition(m *Manifest) map[string][]ManifestEntry {
	out := make(map[string][]ManifestEntry, 512)
	for _, e := range m.Files {
		key := diagPartitionKey(e.URL)
		out[key] = append(out[key], e)
	}
	for k := range out {
		slices.SortFunc(out[k], func(a, b ManifestEntry) int { return strings.Compare(a.URL, b.URL) })
	}
	return out
}

func benchPickPartitions(pqParts, jsonlParts map[string][]ManifestEntry, targets []int64, singleFile bool) []benchPartition {
	keys := make([]string, 0, len(pqParts))
	for k := range pqParts {
		if _, ok := jsonlParts[k]; ok {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)

	all := make([]benchPartition, 0, len(keys))
	for _, k := range keys {
		pq := pqParts[k]
		j := jsonlParts[k]
		if singleFile && (len(pq) != 1 || len(j) != 1) {
			continue
		}
		p := benchPartition{key: k, parquet: pq, jsonl: j}
		for _, e := range pq {
			p.pqBytes += e.Meta.ContentLength
			p.pqRecords += e.Meta.RecordCount
		}
		for _, e := range j {
			p.jBytes += e.Meta.ContentLength
			p.jRecords += e.Meta.RecordCount
		}
		if p.pqRecords != p.jRecords || p.pqRecords == 0 {
			continue
		}
		all = append(all, p)
	}

	used := make([]bool, len(all))
	picked := make([]benchPartition, 0, len(targets))
	for _, target := range targets {
		best := -1
		var bestDiff int64 = 1 << 62
		for i := range all {
			if used[i] {
				continue
			}
			diff := all[i].pqBytes - target
			if diff < 0 {
				diff = -diff
			}
			if diff < bestDiff {
				bestDiff = diff
				best = i
			}
		}
		if best >= 0 {
			used[best] = true
			picked = append(picked, all[best])
		}
	}
	return picked
}

// benchBaselineMBps measures raw single-stream throughput on the first partition
// file (1 MiB range GET). S3 latency for individual objects varies a lot — we have
// seen 30 s for a 2 MiB Full GET and 1.9 s for a 5 MiB one in the same minute — so
// a benchmark that does not check the baseline can silently record network noise.
func benchBaselineMBps(ctx context.Context, client *Client, entry ManifestEntry) (float64, error) {
	bucket, key, err := ParseS3URL(entry.URL)
	if err != nil {
		return 0, err
	}
	ra := NewS3ReaderAt(ctx, client, bucket, key, entry.Meta.ContentLength, RangeOptions{
		BufferSize:      0,
		FooterCacheSize: 0,
	})
	buf := make([]byte, 1<<20)
	start := time.Now()
	n, err := ra.ReadAt(buf, 0)
	elapsed := time.Since(start)
	if err != nil {
		return 0, err
	}
	return float64(n) / 1e6 / elapsed.Seconds(), nil
}

func processJSONLPartition(
	ctx context.Context,
	client *Client,
	entries []ManifestEntry,
	norm *normalizer.Normalizer,
	pool *sink.WorksBatchPool,
	out sink.BatchSink,
	batchSize int,
) (FileReadResult, error) {
	var total FileReadResult
	for _, entry := range entries {
		res, err := processJSONLGZFile(ctx, client, entry, norm, pool, out, batchSize)
		if err != nil {
			return total, err
		}
		total.ScannedRows += res.ScannedRows
		total.MatchedRows += res.MatchedRows
		total.WrittenRows += res.WrittenRows
		total.DedupDropped += res.DedupDropped
		total.BytesRead += res.BytesRead
		total.HTTPRequests += res.HTTPRequests
	}
	return total, nil
}

func processParquetPartition(
	ctx context.Context,
	client *Client,
	entries []ManifestEntry,
	norm *normalizer.Normalizer,
	pool *sink.WorksBatchPool,
	out sink.BatchSink,
	mode ReadMode,
) (FileReadResult, error) {
	var total FileReadResult
	for _, entry := range entries {
		opts := DefaultParquetReadOptions()
		opts.Mode = mode
		res, err := ProcessWorksFile(ctx, client, entry, norm, pool, out, opts)
		if err != nil {
			return total, err
		}
		total.ScannedRows += res.ScannedRows
		total.MatchedRows += res.MatchedRows
		total.WrittenRows += res.WrittenRows
		total.DedupDropped += res.DedupDropped
		total.BytesRead += res.BytesRead
		total.HTTPRequests += res.HTTPRequests
		total.CacheHits += res.CacheHits
		total.Retries += res.Retries
		total.PrefetchPlanBytes += res.PrefetchPlanBytes
		total.PrefetchBytes += res.PrefetchBytes
		total.PrefetchBursts += res.PrefetchBursts
		total.PlanMisses += res.PlanMisses
		total.PrefetchWindowed = total.PrefetchWindowed || res.PrefetchWindowed
		total.MaxGroupBytes = max(total.MaxGroupBytes, res.MaxGroupBytes)
		if res.PlanError != "" && total.PlanError == "" {
			total.PlanError = res.PlanError
		}
	}
	return total, nil
}

type benchPartRun struct {
	label         string
	wallMs        float64
	bytes         uint64
	pctPQ         float64
	pctFloor      float64
	requests      uint64
	bursts        uint64
	peakRAMMiB    float64
	scanned       uint64
	matched       uint64
	written       uint64
	dedup         uint64
	rps           float64
	planBytes     int64
	retries       uint64
	windowed      bool
	planErr       string
	maxGroupBytes int64
}

// runBenchPartition runs fn `runs` times and returns the median by wall time.
// Peak RAM is sampled on every run (a 10 ms ticker, negligible) and the maximum
// is reported.
func runBenchPartition(
	t *testing.T,
	runs int,
	label string,
	p benchPartition,
	fn func() (FileReadResult, error),
) benchPartRun {
	t.Helper()
	if runs < 1 {
		runs = 1
	}
	results := make([]benchPartRun, 0, runs)
	var peakMax float64
	for i := range runs {
		stopRAM := startPeakRAMSampler()
		start := time.Now()
		res, err := fn()
		elapsed := time.Since(start)
		peak := stopRAM()
		if err != nil {
			t.Fatalf("%s run %d/%d failed: %v", label, i+1, runs, err)
		}
		peakMax = max(peakMax, peak)

		r := benchPartRun{
			label:         label,
			wallMs:        float64(elapsed.Microseconds()) / 1000,
			bytes:         res.BytesRead,
			requests:      res.HTTPRequests,
			bursts:        res.PrefetchBursts,
			scanned:       res.ScannedRows,
			matched:       res.MatchedRows,
			written:       res.WrittenRows,
			dedup:         res.DedupDropped,
			planBytes:     res.PrefetchPlanBytes,
			retries:       res.Retries,
			windowed:      res.PrefetchWindowed,
			planErr:       res.PlanError,
			maxGroupBytes: res.MaxGroupBytes,
		}
		r.pctPQ = 100 * float64(r.bytes) / float64(max(p.pqBytes, 1))
		if r.planBytes > 0 {
			r.pctFloor = 100 * float64(r.bytes) / float64(r.planBytes)
		}
		if elapsed > 0 {
			r.rps = float64(r.scanned) / elapsed.Seconds()
		}
		results = append(results, r)
	}
	slices.SortFunc(results, func(a, b benchPartRun) int {
		switch {
		case a.wallMs < b.wallMs:
			return -1
		case a.wallMs > b.wallMs:
			return 1
		default:
			return 0
		}
	})
	median := results[len(results)/2]
	median.peakRAMMiB = peakMax
	return median
}

func TestBenchPartitionPairedFormats(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Hour)
	defer cancel()

	runs := parseBenchIntEnv("BENCH_PART_RUNS", 3)
	sizesMB := diagParseIntList(os.Getenv("BENCH_PART_SIZES_MB"), []int64{2, 5, 15, 50, 150, 400})
	singleFile := os.Getenv("BENCH_PART_ALL_FILES") != "1"

	modesEnv := strings.TrimSpace(os.Getenv("BENCH_PART_MODES"))
	if modesEnv == "" {
		modesEnv = "jsonl,full,range"
	}
	wantJSONL, wantFull, wantRange := false, false, false
	for _, m := range strings.Split(modesEnv, ",") {
		switch strings.TrimSpace(m) {
		case "jsonl":
			wantJSONL = true
		case "full":
			wantFull = true
		case "range":
			wantRange = true
		}
	}

	client, err := NewAnonymousClient(ctx)
	if err != nil {
		t.Fatalf("NewAnonymousClient: %v", err)
	}
	pqManifest, err := client.FetchManifest(ctx, ParquetManifestKey("works"))
	if err != nil {
		t.Fatalf("FetchManifest(parquet): %v", err)
	}
	jsonlManifest, err := client.FetchManifest(ctx, "data/jsonl/works/manifest.json")
	if err != nil {
		t.Fatalf("FetchManifest(jsonl): %v", err)
	}

	targets := make([]int64, len(sizesMB))
	for i, mb := range sizesMB {
		targets[i] = mb * diagMiB
	}
	picked := benchPickPartitions(
		benchGroupByPartition(pqManifest),
		benchGroupByPartition(jsonlManifest),
		targets,
		singleFile,
	)
	if len(picked) == 0 {
		t.Fatal("no paired partitions found")
	}

	var out strings.Builder

	// Network baseline guard: a benchmark that records 0.07 MB/s is measuring the
	// ISP, not the code.
	minMBps := 0.0
	if raw := strings.TrimSpace(os.Getenv("BENCH_PART_MIN_MBPS")); raw != "" {
		if v, err := strconv.ParseFloat(raw, 64); err == nil {
			minMBps = v
		}
	}
	baselineMBps, baseErr := benchBaselineMBps(ctx, client, picked[0].parquet[0])
	if baseErr != nil {
		t.Fatalf("network baseline probe: %v", baseErr)
	}
	t.Logf("network baseline: %.2f MB/s (1 MiB range GET on %s)", baselineMBps, picked[0].key)
	if minMBps > 0 && baselineMBps < minMBps {
		t.Skipf("network baseline %.2f MB/s is below BENCH_PART_MIN_MBPS=%.2f; refusing to record noisy numbers",
			baselineMBps, minMBps)
	}

	fmt.Fprintf(&out, "\n=== Metric #3 v2: same WORKS (paired partitions) | median of %d runs | network baseline %.2f MB/s ===\n", runs, baselineMBps)
	fmt.Fprintf(&out, "| target | partition | files pq/jsonl | records | jsonl/pq size | mode | wall_ms | bytes | %% of pq | floor | %% of floor | reqs | bursts | rps | peak_RAM | scanned | matched | written | dedup |\n")
	fmt.Fprintf(&out, "|---:|---|---:|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")

	for i, p := range picked {
		targetLabel := fmt.Sprintf("%d MiB", sizesMB[i])
		filesLabel := fmt.Sprintf("%d/%d", len(p.parquet), len(p.jsonl))
		if p.pqRecords != p.jRecords {
			t.Fatalf("partition %s: record mismatch pq=%d jsonl=%d", p.key, p.pqRecords, p.jRecords)
		}

		if wantJSONL {
			run := runBenchPartition(t, runs, "jsonl.gz", p, func() (FileReadResult, error) {
				pool := sink.NewWorksBatchPool(sink.DefaultBatchCap, sink.DefaultMaxPooledCap)
				norm := normalizer.NewNormalizer(normalizer.NewWorkDeduplicator())
				noop := sink.NewNoopSink()
				return processJSONLPartition(ctx, client, p.jsonl, norm, pool, noop, sink.DefaultBatchCap)
			})
			fmt.Fprintf(&out, "| %s | %s | %s | %d | %.2fx | JSONL.gz | %.0f | %s | %.1f%% | — | — | %d | 0 | %.0f | %.1f MiB | %d | %d | %d | %d |\n",
				targetLabel, p.key, filesLabel, p.pqRecords, p.jsonlRatio(),
				run.wallMs, formatBytes(int64(run.bytes)), run.pctPQ,
				run.requests, run.rps, run.peakRAMMiB,
				run.scanned, run.matched, run.written, run.dedup)
			t.Logf("%s / JSONL.gz: %.1fs, %s (%.1f%% of parquet size)", targetLabel, run.wallMs/1000,
				formatBytes(int64(run.bytes)), run.pctPQ)
		}

		if wantFull {
			run := runBenchPartition(t, runs, "parquet_full", p, func() (FileReadResult, error) {
				pool := sink.NewWorksBatchPool(sink.DefaultBatchCap, sink.DefaultMaxPooledCap)
				norm := normalizer.NewNormalizer(normalizer.NewWorkDeduplicator())
				noop := sink.NewNoopSink()
				return processParquetPartition(ctx, client, p.parquet, norm, pool, noop, ReadModeFullGet)
			})
			fmt.Fprintf(&out, "| %s | %s | %s | %d | %.2fx | Parquet Full GET | %.0f | %s | %.1f%% | — | — | %d | 0 | %.0f | %.1f MiB | %d | %d | %d | %d |\n",
				targetLabel, p.key, filesLabel, p.pqRecords, p.jsonlRatio(),
				run.wallMs, formatBytes(int64(run.bytes)), run.pctPQ,
				run.requests, run.rps, run.peakRAMMiB,
				run.scanned, run.matched, run.written, run.dedup)
			t.Logf("%s / Full GET: %.1fs, %s (%.1f%% of file)", targetLabel, run.wallMs/1000,
				formatBytes(int64(run.bytes)), run.pctPQ)
		}

		if wantRange {
			run := runBenchPartition(t, runs, "parquet_range", p, func() (FileReadResult, error) {
				pool := sink.NewWorksBatchPool(sink.DefaultBatchCap, sink.DefaultMaxPooledCap)
				norm := normalizer.NewNormalizer(normalizer.NewWorkDeduplicator())
				noop := sink.NewNoopSink()
				return processParquetPartition(ctx, client, p.parquet, norm, pool, noop, ReadModeRange)
			})
			fmt.Fprintf(&out, "| %s | %s | %s | %d | %.2fx | Parquet Range+prefetch | %.0f | %s | %.1f%% | %s | %.1f%% | %d | %d | %.0f | %.1f MiB | %d | %d | %d | %d |\n",
				targetLabel, p.key, filesLabel, p.pqRecords, p.jsonlRatio(),
				run.wallMs, formatBytes(int64(run.bytes)), run.pctPQ,
				formatBytes(run.planBytes), run.pctFloor,
				run.requests, run.bursts, run.rps, run.peakRAMMiB,
				run.scanned, run.matched, run.written, run.dedup)
			t.Logf("%s / Range+prefetch: %.1fs, %s (%.1f%% of file, %.1f%% of floor), %d reqs, %d bursts, max group %s, windowed=%t, retries=%d",
				targetLabel, run.wallMs/1000, formatBytes(int64(run.bytes)), run.pctPQ, run.pctFloor,
				run.requests, run.bursts, formatBytes(run.maxGroupBytes), run.windowed, run.retries)
			if run.planErr != "" {
				fmt.Fprintf(&out, "  !! %s plan error: %s\n", p.key, run.planErr)
			}
		}
	}

	t.Log(out.String())
}
