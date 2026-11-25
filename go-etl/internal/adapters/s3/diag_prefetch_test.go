//go:build inspect

package s3

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"go-etl/internal/normalizer"
	"go-etl/internal/sink"
)

func TestDiagPrefetchOnRealFiles(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	sizeLadderMB := diagParseIntList(os.Getenv("DIAG_PREFETCH_SIZES_MB"), []int64{15, 50})
	modes := strings.TrimSpace(os.Getenv("DIAG_PREFETCH_MODES"))
	if modes == "" {
		modes = "full,range"
	}

	client, err := NewAnonymousClient(ctx)
	if err != nil {
		t.Fatalf("NewAnonymousClient: %v", err)
	}
	manifest, err := client.FetchManifest(ctx, ParquetManifestKey("works"))
	if err != nil {
		t.Fatalf("FetchManifest: %v", err)
	}
	targets := make([]int64, len(sizeLadderMB))
	for i, mb := range sizeLadderMB {
		targets[i] = mb * diagMiB
	}
	picked := PickClosestBySize(manifest.Files, targets)

	var out strings.Builder
	fmt.Fprintf(&out, "\n=== Diag: Full GET vs Range+prefetch (real pipeline, projection floor from footer) ===\n")
	fmt.Fprintf(&out, "| file | mode | wall_ms | bytes_read | %% of file | floor | %% of floor | reqs | bursts | windowed | plan_misses | retries | cs_written |\n")
	fmt.Fprintf(&out, "|---|---|---:|---:|---:|---:|---:|---:|---:|---|---:|---:|---:|\n")

	for i, entry := range picked {
		label := fmt.Sprintf("%d MiB %s (%s)", sizeLadderMB[i], ExtractPartition(entry.URL), formatBytes(entry.Meta.ContentLength))
		modesToRun := []ReadMode{}
		for _, m := range strings.Split(modes, ",") {
			switch strings.TrimSpace(m) {
			case "full":
				modesToRun = append(modesToRun, ReadModeFullGet)
			case "range":
				modesToRun = append(modesToRun, ReadModeRange)
			}
		}

		for _, mode := range modesToRun {
			opts := DefaultParquetReadOptions()
			opts.Mode = mode
			pool := sink.NewWorksBatchPool(sink.DefaultBatchCap, sink.DefaultMaxPooledCap)
			norm := normalizer.NewNormalizer(normalizer.NewWorkDeduplicator())
			noop := sink.NewNoopSink()

			start := time.Now()
			res, err := ProcessWorksFile(ctx, client, entry, norm, pool, noop, opts)
			elapsed := time.Since(start)
			if err != nil {
				t.Fatalf("%s / %s: %v", label, mode, err)
			}

			pctFile := 100 * float64(res.BytesRead) / float64(max(res.FileSize, 1))
			pctFloor := 0.0
			floor := res.PrefetchPlanBytes
			if floor > 0 {
				pctFloor = 100 * float64(res.BytesRead) / float64(floor)
			}
			fmt.Fprintf(&out, "| %s | %s | %.0f | %s | %.1f%% | %s | %.1f%% | %d | %d | %t | %d | %d | %d |\n",
				label, mode, float64(elapsed.Microseconds())/1000,
				formatBytes(int64(res.BytesRead)), pctFile,
				formatBytes(floor), pctFloor,
				res.HTTPRequests, res.PrefetchBursts, res.PrefetchWindowed, res.PlanMisses, res.Retries, res.WrittenRows)
			if res.PlanError != "" {
				fmt.Fprintf(&out, "  !! %s: plan error: %s\n", label, res.PlanError)
			}
			t.Logf("%s / %s done in %.2fs", label, mode, elapsed.Seconds())
		}
	}
	t.Log(out.String())
}
