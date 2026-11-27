package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"

	"go-etl/internal/loadtest"
)

// runSample rebuilds the load-test sample from real OpenAlex data.
func runSample(ctx context.Context, logger *slog.Logger, args []string) error {
	fs := flag.NewFlagSet("sample", flag.ContinueOnError)
	records := fs.Int("records", 12000, "how many works to write")
	out := fs.String("out", loadtest.DefaultSamplePath, "destination JSONL.gz")
	if err := fs.Parse(args); err != nil {
		return err
	}

	client, err := newS3Client(ctx)
	if err != nil {
		return fmt.Errorf("s3 client: %w", err)
	}

	logger.Info("sample: generating", "records", *records, "out", *out)
	n, err := loadtest.GenerateSample(ctx, client, *records, *out)
	if err != nil {
		return err
	}
	logger.Info("sample: done", "records", n, "path", *out)
	return nil
}
