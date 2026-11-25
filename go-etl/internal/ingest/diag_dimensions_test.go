//go:build inspect

package ingest

//	go test -v -count=1 -timeout 10m -tags=inspect ./internal/ingest -run TestDiagDimensionsSmoke
import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	s3 "go-etl/internal/adapters/s3"
	"go-etl/internal/metrics"
	"go-etl/internal/sink"
)

func TestDiagDimensionsSmoke(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	client, err := s3.NewAnonymousClient(ctx)
	if err != nil {
		t.Fatalf("NewAnonymousClient: %v", err)
	}
	plan, err := FetchDimensions(ctx, client, 0, 1, 1)
	if err != nil {
		t.Fatalf("FetchDimensions: %v", err)
	}
	if len(plan.Topics) == 0 || len(plan.Institutions) == 0 {
		t.Fatalf("empty plan: topics=%d institutions=%d", len(plan.Topics), len(plan.Institutions))
	}

	var out strings.Builder
	fmt.Fprintf(&out, "\n=== C2.2 smoke: real dimension dictionaries ===\n")
	fmt.Fprintf(&out, "| entity | file | size | wall | scanned | written | bytes | reqs | mode |\n")
	fmt.Fprintf(&out, "|---|---|---:|---:|---:|---:|---:|---:|---|\n")

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	readOpts := s3.DefaultParquetReadOptions()

	for _, run := range []struct {
		name  string
		entry s3.ManifestEntry
		proc  Processor
	}{
		{
			name:  "topics",
			entry: plan.Topics[0],
			proc: &S3TopicsProcessor{
				Client:      client,
				Sink:        sink.NewNoopSink(),
				Metrics:     metrics.NewJobMetrics(),
				ReadOptions: readOpts,
			},
		},
		{
			name:  "institutions",
			entry: plan.Institutions[0],
			proc: &S3InstitutionsProcessor{
				Client:      client,
				Sink:        sink.NewNoopSink(),
				Metrics:     metrics.NewJobMetrics(),
				ReadOptions: readOpts,
			},
		},
	} {
		start := time.Now()
		res, err := Run(ctx, []s3.ManifestEntry{run.entry},
			run.proc,
			Options{Workers: 1, Order: OrderManifest, FatalFailureStreak: -1, Logger: logger})
		if err != nil {
			t.Fatalf("%s: %v", run.name, err)
		}
		fmt.Fprintf(&out, "| %s | %s | %s | %.2fs | %d | %d | %s | %d | %s |\n",
			run.name,
			strings.TrimPrefix(run.entry.URL, "s3://openalex/data/parquet/"),
			formatBin(run.entry.Meta.ContentLength),
			time.Since(start).Seconds(),
			res.Scanned, res.Written,
			formatBin(int64(res.BytesRead)), res.HTTPRequests,
			map[bool]string{true: "full GET", false: "range+prefetch"}[run.entry.Meta.ContentLength < s3.DefaultFullGetThreshold],
		)
		if res.FilesFailed != 0 {
			t.Fatalf("%s: failed files %+v", run.name, res.FailedFiles)
		}
		if res.Written == 0 {
			t.Fatalf("%s: nothing was written to the sink", run.name)
		}
	}
	t.Log(out.String())
}

func formatBin(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}
