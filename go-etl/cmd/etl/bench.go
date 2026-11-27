package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"time"

	"go-etl/internal/adapters/s3"
	"go-etl/internal/normalizer"
	"go-etl/internal/sink"
)

// runBench reads one object and prints how much of it was transferred, so the
// two read modes can be compared on the same file (§6.16).
func runBench(ctx context.Context, logger *slog.Logger, args []string) error {
	fs := flag.NewFlagSet("bench", flag.ContinueOnError)
	file := fs.String("file", "", "s3:// URL of one parquet file")
	fullGet := fs.Int("full-get-threshold-mb", 0, "files smaller than this use one Full GET (0 = library default)")
	workers := fs.Int("workers", 1, "unused placeholder for symmetry with ingest")
	if err := fs.Parse(args); err != nil {
		return err
	}
	_ = workers
	if *file == "" {
		return fmt.Errorf("bench: --file is required")
	}

	client, err := newS3Client(ctx)
	if err != nil {
		return fmt.Errorf("s3 client: %w", err)
	}
	entry, err := client.StatURL(ctx, *file)
	if err != nil {
		return err
	}

	opts := s3.DefaultParquetReadOptions()
	if *fullGet > 0 {
		opts.FullGetThreshold = int64(*fullGet) << 20
	}

	norm := normalizer.NewNormalizer(nil, normalizer.WithSnapshotYear(time.Now().Year()))
	pool := sink.NewWorksBatchPool(sink.DefaultBatchCap, sink.DefaultMaxPooledCap)

	start := time.Now()
	res, err := s3.ProcessWorksFile(ctx, client, entry, norm, pool, sink.NewNoopSink(), opts)
	elapsed := time.Since(start)
	if err != nil {
		return err
	}

	mode := fmt.Sprintf("%v", res.UsedMode)
	if mode == "" {
		mode = "range+prefetch"
	}
	logger.Info("bench: result",
		"file", entry.URL,
		"file_bytes", entry.Meta.ContentLength,
		"mode", mode,
		"bytes_read", res.BytesRead,
		"http_requests", res.HTTPRequests,
		"plan_bytes", res.PrefetchPlanBytes,
		"scanned", res.ScannedRows,
		"written", res.WrittenRows,
		"elapsed", elapsed.Round(time.Millisecond))

	if entry.Meta.ContentLength > 0 {
		share := float64(res.BytesRead) / float64(entry.Meta.ContentLength) * 100
		fmt.Printf("file=%.1f MiB read=%.2f MiB share=%.1f%% requests=%d mode=%s elapsed=%s\n",
			float64(entry.Meta.ContentLength)/(1<<20),
			float64(res.BytesRead)/(1<<20),
			share, res.HTTPRequests, mode, elapsed.Round(time.Millisecond))
	}
	return nil
}
