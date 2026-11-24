package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/parquet-go/parquet-go"

	"go-etl/internal/metrics"
	"go-etl/internal/normalizer"
	"go-etl/internal/sink"
)

const (
	DefaultFullGetThreshold = 16 << 20

	maxFullGetBytes = 512 << 20

	parquetRowReadBatch = 256
)

type ReadMode uint8

const (
	ReadModeHybrid ReadMode = iota
	ReadModeFullGet
	ReadModeRange
)

func (m ReadMode) String() string {
	switch m {
	case ReadModeHybrid:
		return "hybrid"
	case ReadModeFullGet:
		return "full_get"
	case ReadModeRange:
		return "buffered_range"
	default:
		return "unknown"
	}
}

type ParquetReadOptions struct {
	Mode             ReadMode
	FullGetThreshold int64
	RangeOpts        RangeOptions
	BatchSize        int
	Metrics          *metrics.JobMetrics
}

func DefaultParquetReadOptions() ParquetReadOptions {
	return ParquetReadOptions{
		Mode:             ReadModeHybrid,
		FullGetThreshold: DefaultFullGetThreshold,
		RangeOpts:        DefaultRangeOptions(),
		BatchSize:        sink.DefaultBatchCap,
	}
}

type FileReadResult struct {
	URL          string
	FileSize     int64
	UsedMode     ReadMode
	ScannedRows  uint64
	MatchedRows  uint64
	WrittenRows  uint64
	DedupDropped uint64
	BytesRead    uint64
	HTTPRequests uint64
	CacheHits    uint64
}

func openParquetEntry(
	ctx context.Context,
	client *Client,
	entry ManifestEntry,
	opts ParquetReadOptions,
) (*parquet.File, ReadMode, func() RangeStats, error) {
	bucket, key, err := ParseS3URL(entry.URL)
	if err != nil {
		return nil, ReadModeHybrid, nil, err
	}

	fileSize := entry.Meta.ContentLength
	threshold := opts.FullGetThreshold
	if threshold <= 0 {
		threshold = DefaultFullGetThreshold
	}

	useFullGet := false
	switch opts.Mode {
	case ReadModeFullGet:
		useFullGet = true
	case ReadModeRange:
		useFullGet = fileSize <= 0
	default:
		useFullGet = fileSize <= 0 || fileSize < threshold
	}

	if useFullGet {
		raw, err := client.ReadAll(ctx, bucket, key, maxFullGetBytes)
		if err != nil {
			return nil, ReadModeFullGet, nil, err
		}
		bytesLen := uint64(len(raw))
		if opts.Metrics != nil {
			opts.Metrics.IncHTTPRequests()
			opts.Metrics.AddBytesRead(bytesLen)
		}
		pf, err := parquet.OpenFile(bytes.NewReader(raw), int64(len(raw)))
		if err != nil {
			return nil, ReadModeFullGet, nil, fmt.Errorf("open full-get parquet %s: %w", entry.URL, err)
		}
		stats := RangeStats{BytesRead: bytesLen, HTTPRequests: 1}
		return pf, ReadModeFullGet, func() RangeStats { return stats }, nil
	}

	rangeOpts := opts.RangeOpts
	if rangeOpts.Metrics == nil {
		rangeOpts.Metrics = opts.Metrics
	}
	ra := NewS3ReaderAt(ctx, client, bucket, key, fileSize, rangeOpts)

	var fileOpts []parquet.FileOption
	fileOpts = append(fileOpts, parquet.OptimisticRead(true))
	if rangeOpts.BufferSize > 0 {
		colBuf := int(min(rangeOpts.BufferSize, 256<<10))
		fileOpts = append(fileOpts, parquet.ReadBufferSize(colBuf))
	}

	pf, err := parquet.OpenFile(ra, fileSize, fileOpts...)
	if err != nil {
		return nil, ReadModeRange, nil, fmt.Errorf("open range parquet %s: %w", entry.URL, err)
	}
	return pf, ReadModeRange, ra.Stats, nil
}

func ProcessWorksFile(
	ctx context.Context,
	client *Client,
	entry ManifestEntry,
	norm *normalizer.Normalizer,
	pool *sink.WorksBatchPool,
	outSink sink.BatchSink,
	opts ParquetReadOptions,
) (FileReadResult, error) {
	res := FileReadResult{
		URL:      entry.URL,
		FileSize: entry.Meta.ContentLength,
	}

	pf, usedMode, statsFn, err := openParquetEntry(ctx, client, entry, opts)
	if err != nil {
		return res, err
	}
	res.UsedMode = usedMode

	batchSize := opts.BatchSize
	if batchSize <= 0 {
		batchSize = sink.DefaultBatchCap
	}

	reader := parquet.NewGenericReader[normalizer.ParquetWork](pf)
	defer reader.Close()

	batch := pool.Get()
	defer func() {
		if batch != nil {
			pool.Put(batch)
		}
	}()

	rowBuf := make([]normalizer.ParquetWork, parquetRowReadBatch)

	for {
		if err := ctx.Err(); err != nil {
			if batch.Len() > 0 {
				flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				written, _ := flushWorksBatch(flushCtx, outSink, batch, opts.Metrics)
				cancel()
				res.WrittenRows += written
			}
			applyRangeStats(&res, statsFn())
			return res, err
		}

		clear(rowBuf)
		n, readErr := reader.Read(rowBuf)
		if n > 0 {
			res.ScannedRows += uint64(n)
			if opts.Metrics != nil {
				opts.Metrics.AddScanned(uint64(n))
			}

			var batchMatched, batchDedup uint64
			for i := range n {
				outcome := norm.AppendParquetWork(&rowBuf[i], batch)
				switch outcome {
				case normalizer.OutcomeAdded:
					batchMatched++
					if batch.Len() >= batchSize {
						written, err := flushWorksBatch(ctx, outSink, batch, opts.Metrics)
						if err != nil {
							applyRangeStats(&res, statsFn())
							return res, err
						}
						res.WrittenRows += written
						pool.Put(batch)
						batch = pool.Get()
					}
				case normalizer.OutcomeDuplicate:
					batchMatched++
					batchDedup++
				}
			}

			res.MatchedRows += batchMatched
			res.DedupDropped += batchDedup
			if opts.Metrics != nil {
				if batchMatched > 0 {
					opts.Metrics.AddMatched(batchMatched)
				}
				if batchDedup > 0 {
					opts.Metrics.AddDedupDropped(batchDedup)
				}
			}
		}

		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			applyRangeStats(&res, statsFn())
			return res, fmt.Errorf("read parquet works %s: %w", entry.URL, readErr)
		}
	}

	if batch.Len() > 0 {
		written, err := flushWorksBatch(ctx, outSink, batch, opts.Metrics)
		if err != nil {
			applyRangeStats(&res, statsFn())
			return res, err
		}
		res.WrittenRows += written
	}

	applyRangeStats(&res, statsFn())
	return res, nil
}

func flushWorksBatch(ctx context.Context, outSink sink.BatchSink, b *sink.WorksBatch, jm *metrics.JobMetrics) (uint64, error) {
	rows := uint64(b.Len())
	if rows == 0 {
		return 0, nil
	}
	approxBytes := uint64(b.ApproxBytes())

	start := time.Now()
	if err := outSink.WriteWorksBatch(ctx, b); err != nil {
		return 0, err
	}
	if jm != nil {
		jm.RecordBatchDuration(time.Since(start))
		jm.AddWritten(rows, approxBytes)
	}
	return rows, nil
}

func ProcessTopicsFile(
	ctx context.Context,
	client *Client,
	entry ManifestEntry,
	outSink sink.BatchSink,
	opts ParquetReadOptions,
) (FileReadResult, error) {
	res := FileReadResult{
		URL:      entry.URL,
		FileSize: entry.Meta.ContentLength,
	}

	pf, usedMode, statsFn, err := openParquetEntry(ctx, client, entry, opts)
	if err != nil {
		return res, err
	}
	res.UsedMode = usedMode

	batchSize := opts.BatchSize
	if batchSize <= 0 {
		batchSize = 5_000
	}

	reader := parquet.NewGenericReader[normalizer.ParquetTopic](pf)
	defer reader.Close()

	batch := sink.NewTopicsBatch(batchSize)
	rowBuf := make([]normalizer.ParquetTopic, parquetRowReadBatch)

	for {
		if err := ctx.Err(); err != nil {
			applyRangeStats(&res, statsFn())
			return res, err
		}

		clear(rowBuf)
		n, readErr := reader.Read(rowBuf)
		if n > 0 {
			res.ScannedRows += uint64(n)
			if opts.Metrics != nil {
				opts.Metrics.AddScanned(uint64(n))
			}
			for i := range n {
				if normalizer.AppendParquetTopic(&rowBuf[i], batch) {
					res.MatchedRows++
					if opts.Metrics != nil {
						opts.Metrics.AddMatched(1)
					}
					if batch.Len() >= batchSize {
						written, err := flushTopicsBatch(ctx, outSink, batch, opts.Metrics)
						if err != nil {
							applyRangeStats(&res, statsFn())
							return res, err
						}
						res.WrittenRows += written
						batch.Reset()
					}
				}
			}
		}

		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			applyRangeStats(&res, statsFn())
			return res, fmt.Errorf("read parquet topics %s: %w", entry.URL, readErr)
		}
	}

	if batch.Len() > 0 {
		written, err := flushTopicsBatch(ctx, outSink, batch, opts.Metrics)
		if err != nil {
			applyRangeStats(&res, statsFn())
			return res, err
		}
		res.WrittenRows += written
	}

	applyRangeStats(&res, statsFn())
	return res, nil
}

func flushTopicsBatch(ctx context.Context, outSink sink.BatchSink, b *sink.TopicsBatch, jm *metrics.JobMetrics) (uint64, error) {
	rows := uint64(b.Len())
	if rows == 0 {
		return 0, nil
	}
	approxBytes := uint64(b.ApproxBytes())

	start := time.Now()
	if err := outSink.WriteTopicsBatch(ctx, b); err != nil {
		return 0, err
	}
	if jm != nil {
		jm.RecordBatchDuration(time.Since(start))
		jm.AddWritten(rows, approxBytes)
	}
	return rows, nil
}

func ProcessInstitutionsFile(
	ctx context.Context,
	client *Client,
	entry ManifestEntry,
	outSink sink.BatchSink,
	opts ParquetReadOptions,
) (FileReadResult, error) {
	res := FileReadResult{
		URL:      entry.URL,
		FileSize: entry.Meta.ContentLength,
	}

	pf, usedMode, statsFn, err := openParquetEntry(ctx, client, entry, opts)
	if err != nil {
		return res, err
	}
	res.UsedMode = usedMode

	batchSize := opts.BatchSize
	if batchSize <= 0 {
		batchSize = sink.DefaultBatchCap
	}

	reader := parquet.NewGenericReader[normalizer.ParquetInstitution](pf)
	defer reader.Close()

	batch := sink.NewInstitutionsBatch(batchSize)
	rowBuf := make([]normalizer.ParquetInstitution, parquetRowReadBatch)

	for {
		if err := ctx.Err(); err != nil {
			applyRangeStats(&res, statsFn())
			return res, err
		}

		clear(rowBuf)
		n, readErr := reader.Read(rowBuf)
		if n > 0 {
			res.ScannedRows += uint64(n)
			if opts.Metrics != nil {
				opts.Metrics.AddScanned(uint64(n))
			}
			for i := range n {
				if normalizer.AppendParquetInstitution(&rowBuf[i], batch) {
					res.MatchedRows++
					if opts.Metrics != nil {
						opts.Metrics.AddMatched(1)
					}
					if batch.Len() >= batchSize {
						written, err := flushInstitutionsBatch(ctx, outSink, batch, opts.Metrics)
						if err != nil {
							applyRangeStats(&res, statsFn())
							return res, err
						}
						res.WrittenRows += written
						batch.Reset()
					}
				}
			}
		}

		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			applyRangeStats(&res, statsFn())
			return res, fmt.Errorf("read parquet institutions %s: %w", entry.URL, readErr)
		}
	}

	if batch.Len() > 0 {
		written, err := flushInstitutionsBatch(ctx, outSink, batch, opts.Metrics)
		if err != nil {
			applyRangeStats(&res, statsFn())
			return res, err
		}
		res.WrittenRows += written
	}

	applyRangeStats(&res, statsFn())
	return res, nil
}

func flushInstitutionsBatch(ctx context.Context, outSink sink.BatchSink, b *sink.InstitutionsBatch, jm *metrics.JobMetrics) (uint64, error) {
	rows := uint64(b.Len())
	if rows == 0 {
		return 0, nil
	}
	approxBytes := uint64(b.ApproxBytes())

	start := time.Now()
	if err := outSink.WriteInstitutionsBatch(ctx, b); err != nil {
		return 0, err
	}
	if jm != nil {
		jm.RecordBatchDuration(time.Since(start))
		jm.AddWritten(rows, approxBytes)
	}
	return rows, nil
}

func applyRangeStats(res *FileReadResult, st RangeStats) {
	res.BytesRead = st.BytesRead
	res.HTTPRequests = st.HTTPRequests
	res.CacheHits = st.CacheHits
}
