package loadtest

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"go-etl/internal/adapters/s3"
	"go-etl/internal/normalizer"
	"go-etl/internal/sink"
)

// GenerateSample rebuilds the load-test sample from one real parquet file: the
// rows are normalised exactly like an ingest, then written back as OpenAlex JSON
// lines so the benchmark runs on genuine records.
func GenerateSample(ctx context.Context, client *s3.Client, limit int, outPath string) (int, error) {
	if limit <= 0 {
		return 0, fmt.Errorf("loadtest: sample needs a positive record limit")
	}
	manifest, err := client.FetchManifest(ctx, s3.ParquetManifestKey("works"))
	if err != nil {
		return 0, fmt.Errorf("loadtest: manifest: %w", err)
	}
	entries, err := manifest.SelectEntries(0, 1, 0)
	if err != nil {
		return 0, fmt.Errorf("loadtest: select files: %w", err)
	}
	if len(entries) == 0 {
		return 0, fmt.Errorf("loadtest: the manifest has no files")
	}

	if dir := filepath.Dir(outPath); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return 0, fmt.Errorf("loadtest: create %s: %w", dir, err)
		}
	}
	f, err := os.Create(outPath)
	if err != nil {
		return 0, fmt.Errorf("loadtest: create sample %s: %w", outPath, err)
	}
	defer func() { _ = f.Close() }()

	gz := gzip.NewWriter(f)
	capture := &sampleSink{w: gz, limit: limit}

	year := time.Now().Year()
	if _, perr := time.Parse("2006-01-02", manifest.Date); perr == nil {
		year = time.Now().UTC().Year()
	}
	norm := normalizer.NewNormalizer(nil, normalizer.WithSnapshotYear(year))
	pool := sink.NewWorksBatchPool(4096, sink.DefaultMaxPooledCap)

	// A single file is sparse in CS works, so walk the manifest until the
	// requested number of records is collected.
	for _, entry := range entries {
		if capture.n >= limit || ctx.Err() != nil {
			break
		}
		opts := s3.DefaultParquetReadOptions()
		opts.MaxRecords = int64(limit - capture.n)
		if _, ferr := s3.ProcessWorksFile(ctx, client, entry, norm, pool, capture, opts); ferr != nil {
			_ = gz.Close()
			return capture.n, ferr
		}
	}
	if cerr := gz.Close(); cerr != nil {
		return capture.n, fmt.Errorf("loadtest: close sample: %w", cerr)
	}
	if capture.n == 0 {
		return 0, fmt.Errorf("loadtest: %s produced no records; try another file", entries[0].URL)
	}
	return capture.n, nil
}

type sampleSink struct {
	w     *gzip.Writer
	limit int
	n     int
}

func (s *sampleSink) WriteWorksBatch(_ context.Context, b *sink.WorksBatch) error {
	for i := 0; i < b.Len() && s.n < s.limit; i++ {
		rec := sampleRecord{
			ID:              fmt.Sprintf("https://openalex.org/W%d", b.WorkIDs[i]),
			Type:            b.WorkTypes[i],
			PublicationYear: int32(b.PubYears[i]),
			UpdatedDate:     b.UpdatedDates[i].UTC().Format("2006-01-02T15:04:05.000"),
			IsRetracted:     b.IsRetracted[i] == 1,
		}
		if d := b.PubDates[i]; d != nil {
			rec.PublicationDate = d.Format("2006-01-02")
		}
		if f := b.FWCIs[i]; f != nil {
			v := float64(*f)
			rec.FWCI = &v
		}
		if field := b.PrimaryFieldIDs[i]; field != 0 {
			rec.PrimaryTopic = &sampleTopic{
				ID:    fmt.Sprintf("https://openalex.org/T%d", b.PrimaryTopicIDs[i]),
				Field: &sampleIDRef{ID: fmt.Sprintf("https://openalex.org/fields/%d", field)},
			}
		}
		for k, id := range b.TopicIDs[i] {
			score := float32(0)
			if k < len(b.TopicScores[i]) {
				score = b.TopicScores[i][k]
			}
			rec.Topics = append(rec.Topics, sampleTopic{ID: fmt.Sprintf("https://openalex.org/T%d", id), Score: score})
		}
		line, err := json.Marshal(rec)
		if err != nil {
			return fmt.Errorf("loadtest: encode sample record: %w", err)
		}
		if _, err := s.w.Write(append(line, '\n')); err != nil {
			return fmt.Errorf("loadtest: write sample: %w", err)
		}
		s.n++
	}
	return nil
}

func (s *sampleSink) WriteTopicsBatch(context.Context, *sink.TopicsBatch) error { return nil }
func (s *sampleSink) WriteInstitutionsBatch(context.Context, *sink.InstitutionsBatch) error {
	return nil
}
func (s *sampleSink) Close() error { return nil }

type sampleRecord struct {
	ID              string        `json:"id"`
	Type            string        `json:"type"`
	PublicationYear int32         `json:"publication_year"`
	PublicationDate string        `json:"publication_date,omitempty"`
	UpdatedDate     string        `json:"updated_date"`
	IsRetracted     bool          `json:"is_retracted"`
	IsXPAC          bool          `json:"is_xpac"`
	FWCI            *float64      `json:"fwci,omitempty"`
	PrimaryTopic    *sampleTopic  `json:"primary_topic,omitempty"`
	Topics          []sampleTopic `json:"topics,omitempty"`
}

type sampleTopic struct {
	ID    string       `json:"id"`
	Score float32      `json:"score,omitempty"`
	Field *sampleIDRef `json:"field,omitempty"`
}

type sampleIDRef struct {
	ID string `json:"id"`
}
