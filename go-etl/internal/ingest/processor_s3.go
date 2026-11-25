package ingest

import (
	"context"
	"fmt"

	"go-etl/internal/adapters/s3"
	"go-etl/internal/metrics"
	"go-etl/internal/normalizer"
	"go-etl/internal/sink"
)

type S3WorksProcessor struct {
	Client      *s3.Client
	Normalizer  *normalizer.Normalizer
	Pool        *sink.WorksBatchPool
	Sink        sink.BatchSink
	Metrics     *metrics.JobMetrics
	ReadOptions s3.ParquetReadOptions
}

func (p *S3WorksProcessor) Process(ctx context.Context, entry Entry, limit int64) (FileResult, error) {
	if p == nil || p.Client == nil {
		return FileResult{}, fmt.Errorf("ingest: works processor is not configured")
	}
	opts := p.ReadOptions
	if opts.Metrics == nil {
		opts.Metrics = p.Metrics
	}
	opts.MaxRecords = limit // job-level row budget handed down by the orchestrator
	return s3.ProcessWorksFile(ctx, p.Client, entry, p.Normalizer, p.Pool, p.Sink, opts)
}

type S3TopicsProcessor struct {
	Client      *s3.Client
	Sink        sink.BatchSink
	Metrics     *metrics.JobMetrics
	ReadOptions s3.ParquetReadOptions
}

func (p *S3TopicsProcessor) Process(ctx context.Context, entry Entry, limit int64) (FileResult, error) {
	if p == nil || p.Client == nil {
		return FileResult{}, fmt.Errorf("ingest: topics processor is not configured")
	}
	opts := p.ReadOptions
	if opts.Metrics == nil {
		opts.Metrics = p.Metrics
	}
	opts.MaxRecords = limit
	return s3.ProcessTopicsFile(ctx, p.Client, entry, p.Sink, opts)
}

type S3InstitutionsProcessor struct {
	Client      *s3.Client
	Sink        sink.BatchSink
	Metrics     *metrics.JobMetrics
	ReadOptions s3.ParquetReadOptions
}

func (p *S3InstitutionsProcessor) Process(ctx context.Context, entry Entry, limit int64) (FileResult, error) {
	if p == nil || p.Client == nil {
		return FileResult{}, fmt.Errorf("ingest: institutions processor is not configured")
	}
	opts := p.ReadOptions
	if opts.Metrics == nil {
		opts.Metrics = p.Metrics
	}
	opts.MaxRecords = limit
	return s3.ProcessInstitutionsFile(ctx, p.Client, entry, p.Sink, opts)
}

type DimensionsPlan struct {
	Topics       []Entry
	Institutions []Entry
}

func FetchDimensions(ctx context.Context, client *s3.Client, shardIndex, shardTotal, maxFiles int) (DimensionsPlan, error) {
	if client == nil {
		return DimensionsPlan{}, fmt.Errorf("ingest: nil s3 client")
	}
	var plan DimensionsPlan

	topicsManifest, err := client.FetchManifest(ctx, s3.ParquetManifestKey("topics"))
	if err != nil {
		return plan, fmt.Errorf("fetch topics manifest: %w", err)
	}
	plan.Topics, err = topicsManifest.SelectEntries(shardIndex, shardTotal, maxFiles)
	if err != nil {
		return plan, fmt.Errorf("select topics entries: %w", err)
	}

	instManifest, err := client.FetchManifest(ctx, s3.ParquetManifestKey("institutions"))
	if err != nil {
		return plan, fmt.Errorf("fetch institutions manifest: %w", err)
	}
	plan.Institutions, err = instManifest.SelectEntries(shardIndex, shardTotal, maxFiles)
	if err != nil {
		return plan, fmt.Errorf("select institutions entries: %w", err)
	}
	return plan, nil
}
