package s3

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/parquet-go/parquet-go"

	"go-etl/internal/normalizer"
	"go-etl/internal/sink"
)

// StreamWorksFile is the working-path reader: it normalizes a Works file and
// hands every filled batch to emit, transferring ownership of the buffer. The
// emitter must always take the batch (even on error) and return it to the pool;
// the writer is the only goroutine that inserts and acks.
func StreamWorksFile(
	ctx context.Context,
	client *Client,
	entry ManifestEntry,
	norm *normalizer.Normalizer,
	pool *sink.WorksBatchPool,
	opts ParquetReadOptions,
	emit func(context.Context, *sink.WorksBatch) error,
) (FileReadResult, error) {
	res := FileReadResult{
		URL:      entry.URL,
		FileSize: entry.Meta.ContentLength,
	}
	if emit == nil {
		return res, errors.New("s3: nil works emitter")
	}
	pf, usedMode, statsFn, err := openParquetEntry(ctx, client, entry, opts, &normalizer.ParquetWork{})
	if err != nil {
		return res, err
	}
	res.UsedMode = usedMode

	batchSize := opts.BatchSize
	if batchSize <= 0 {
		batchSize = 5_000
	}
	reader := parquet.NewGenericReader[normalizer.ParquetWork](pf)
	defer reader.Close()

	batch := pool.Get()
	rowBuf := make([]normalizer.ParquetWork, parquetRowReadBatch)

	flush := func() error {
		if batch == nil || batch.Len() == 0 {
			return nil
		}
		rows := batch.Len()
		if err := emit(ctx, batch); err != nil {
			return err
		}
		res.WrittenRows += uint64(rows)
		batch = pool.Get()
		return nil
	}

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
			var batchMatched, batchRejected uint64
			for i := range n {
				row := &rowBuf[i]
				if !norm.WantsParquet(row) {
					continue
				}
				yearMissing := row.PublicationYear == nil
				if reason := norm.PrepareParquet(row); reason != "" {
					batchRejected++
					if res.RejectedByReason == nil {
						res.RejectedByReason = make(map[string]uint64, 4)
					}
					res.RejectedByReason[string(reason)]++
					continue
				}
				if yearMissing && row.PublicationYear != nil {
					res.RestoredYears++
				}
				outcome := norm.AppendParquetWork(row, batch)
				if outcome != normalizer.OutcomeAdded {
					continue
				}
				batchMatched++
				if batch.Len() >= batchSize {
					if err := flush(); err != nil {
						return res, err
					}
				}
			}
			res.MatchedRows += batchMatched
			res.RejectedRows += batchRejected
		}

		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			applyRangeStats(&res, statsFn())
			return res, fmt.Errorf("read parquet works %s: %w", entry.URL, readErr)
		}
	}

	if err := flush(); err != nil {
		return res, err
	}
	applyRangeStats(&res, statsFn())
	return res, nil
}

// StreamTopicsFile is the working-path Topics reader with the same ownership
// contract as StreamWorksFile.
func StreamTopicsFile(
	ctx context.Context,
	client *Client,
	entry ManifestEntry,
	opts ParquetReadOptions,
	emit func(context.Context, *sink.TopicsBatch) error,
) (FileReadResult, error) {
	res := FileReadResult{
		URL:      entry.URL,
		FileSize: entry.Meta.ContentLength,
	}
	if emit == nil {
		return res, errors.New("s3: nil topics emitter")
	}
	pf, usedMode, statsFn, err := openParquetEntry(ctx, client, entry, opts, &normalizer.ParquetTopic{})
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

	flush := func() error {
		if batch.Len() == 0 {
			return nil
		}
		rows := batch.Len()
		if err := emit(ctx, batch); err != nil {
			return err
		}
		res.WrittenRows += uint64(rows)
		batch = sink.NewTopicsBatch(batchSize)
		return nil
	}

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
						if err := flush(); err != nil {
							return res, err
						}
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

	if err := flush(); err != nil {
		return res, err
	}
	if entry.Meta.RecordCount > 0 && res.ScannedRows != uint64(entry.Meta.RecordCount) {
		return res, fmt.Errorf("topics file %s: scanned %d row(s), manifest record_count is %d",
			entry.URL, res.ScannedRows, entry.Meta.RecordCount)
	}
	applyRangeStats(&res, statsFn())
	return res, nil
}
