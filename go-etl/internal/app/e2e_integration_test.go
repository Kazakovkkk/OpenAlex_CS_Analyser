//go:build integration

package app

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/parquet-go/parquet-go"

	"go-etl/internal/adapters/s3"
	etlch "go-etl/internal/clickhouse"
	"go-etl/internal/httpapi"
	"go-etl/internal/jobs"
	"go-etl/internal/normalizer"
	"go-etl/internal/pipeline"
	"go-etl/internal/sink"
)

type rewriteTransport struct{ target *url.URL }

func (t rewriteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	clone := req.Clone(req.Context())
	clone.URL.Scheme = t.target.Scheme
	clone.URL.Host = t.target.Host
	clone.Host = t.target.Host
	return http.DefaultTransport.RoundTrip(clone)
}

func parquetBytes[T any](t *testing.T, rows []T) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[T](&buf)
	if _, err := w.Write(rows); err != nil {
		t.Fatalf("parquet write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("parquet close: %v", err)
	}
	return buf.Bytes()
}

func ptr[T any](v T) *T { return &v }

func manifestBytes(t *testing.T, entity string, files map[string][]byte) []byte {
	t.Helper()
	entries := make([]s3.ManifestEntry, 0, len(files))
	for key, data := range files {
		entries = append(entries, s3.ManifestEntry{
			URL:  "s3://openalex/" + key,
			Meta: s3.ManifestMeta{ContentLength: int64(len(data)), RecordCount: 0},
		})
	}
	// Stable order for the manifest itself; the release id sorts its own copy.
	for i := 1; i < len(entries); i++ {
		for j := i; j > 0 && entries[j].URL < entries[j-1].URL; j-- {
			entries[j], entries[j-1] = entries[j-1], entries[j]
		}
	}
	m := s3.Manifest{Entity: entity, Format: "parquet", Date: "2026-11-07", Files: entries}
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	return raw
}

func TestLocalIngestPublishesWorksAndTopics(t *testing.T) {
	etlPass := os.Getenv("CLICKHOUSE_ETL_WRITER_PASSWORD")
	pipePass := os.Getenv("CLICKHOUSE_GO_WRITER_PASSWORD")
	if etlPass == "" || pipePass == "" {
		t.Skip("CLICKHOUSE_ETL_WRITER_PASSWORD/CLICKHOUSE_GO_WRITER_PASSWORD are not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// Local stand cleanup through the passwordless default user.
	admin, err := ch.Open(&ch.Options{Addr: []string{"127.0.0.1:9000"}, Auth: ch.Auth{Database: "openalex"}})
	if err != nil {
		t.Skipf("local ClickHouse is not available: %v", err)
	}
	defer admin.Close()
	for _, q := range []string{
		"TRUNCATE TABLE openalex.works",
		"TRUNCATE TABLE openalex.dim_topics",
		"TRUNCATE TABLE openalex.etl_checkpoints",
		"TRUNCATE TABLE openalex.pipeline_state",
		`INSERT INTO openalex.pipeline_state
		 (id, state_revision, snapshot_date, data_updated_at, data_version, load_state,
		  loading_snapshot_date, loading_manifest_id, instance_id, job_id,
		  analysis_data_version, analysis_snapshot_date, analysis_completed_at, load_phase)
		 VALUES (1, 0, NULL, NULL, 0, 'idle', NULL, '', '', '', 0, NULL, NULL, 'none')`,
	} {
		if err := admin.Exec(ctx, q); err != nil {
			t.Fatalf("init local stand: %v", err)
		}
	}

	cs := &normalizer.ParquetPrimaryTopic{
		ID:    ptr("https://openalex.org/T10001"),
		Field: &normalizer.ParquetIDRef{ID: ptr("https://openalex.org/fields/17")},
	}
	workRows := []normalizer.ParquetWork{
		{
			ID: ptr("https://openalex.org/W900001"), Type: ptr("article"),
			PublicationYear: ptr(int32(2024)), PublicationDate: ptr(int32(20000)),
			UpdatedDate: ptr(int64(time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC).UnixMicro())),
			IsRetracted: ptr(false), PrimaryTopic: cs,
			Topics: []normalizer.ParquetWorkTopic{{ID: ptr("https://openalex.org/T10001"), Score: ptr(float32(0.9))}},
		},
		{
			ID: ptr("https://openalex.org/W900002"), Type: ptr("preprint"),
			PublicationYear: ptr(int32(2025)), PublicationDate: ptr(int32(20400)),
			UpdatedDate: ptr(int64(time.Date(2026, 11, 7, 0, 0, 0, 0, time.UTC).UnixMicro())),
			IsRetracted: ptr(false), PrimaryTopic: cs,
			Topics: []normalizer.ParquetWorkTopic{{ID: ptr("https://openalex.org/T10002"), Score: ptr(float32(0.4))}},
		},
	}
	topicRows := []normalizer.ParquetTopic{{
		ID: ptr("https://openalex.org/T10001"), DisplayName: ptr("Machine Learning"),
		Subfield: &normalizer.ParquetNamedRef{ID: ptr("https://openalex.org/subfields/1702"), DisplayName: ptr("AI")},
		Field:    &normalizer.ParquetNamedRef{ID: ptr("https://openalex.org/fields/17"), DisplayName: ptr("CS")},
		Domain:   &normalizer.ParquetNamedRef{ID: ptr("https://openalex.org/domains/1"), DisplayName: ptr("Physical")},
	}}

	worksKey := "data/parquet/works/updated_date=2026-11-07/part_0.parquet"
	topicsKey := "data/parquet/topics/part_0.parquet"
	worksParquet := parquetBytes(t, workRows)
	topicsParquet := parquetBytes(t, topicRows)
	objects := map[string][]byte{
		worksKey:  worksParquet,
		topicsKey: topicsParquet,
	}
	objects["data/parquet/works/manifest.json"] = manifestBytes(t, "works", map[string][]byte{worksKey: worksParquet})
	objects["data/parquet/topics/manifest.json"] = manifestBytes(t, "topics", map[string][]byte{topicsKey: topicsParquet})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/")
		data, ok := objects[key]
		if !ok {
			http.NotFound(w, r)
			return
		}
		rangeHdr := r.Header.Get("Range")
		if rangeHdr == "" {
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			_, _ = w.Write(data)
			return
		}
		parts := strings.Split(strings.TrimPrefix(rangeHdr, "bytes="), "-")
		start, _ := strconv.ParseInt(parts[0], 10, 64)
		end, _ := strconv.ParseInt(parts[1], 10, 64)
		if start < 0 || end >= int64(len(data)) || end < start {
			http.Error(w, "bad range", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		slice := data[start : end+1]
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
		w.Header().Set("Content-Length", strconv.Itoa(len(slice)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(slice)
	}))
	defer srv.Close()
	target, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	hc := &http.Client{Transport: rewriteTransport{target: target}}
	client, err := s3.NewAnonymousClient(ctx, s3.WithHTTPClient(hc))
	if err != nil {
		t.Fatal(err)
	}

	dataConn, err := etlch.Open(ctx, etlch.RoleETLWriter, etlch.Options{
		Addr: "127.0.0.1:9000", Database: "openalex", User: "etl_writer", Password: etlPass,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer dataConn.Close()
	pipeDB, err := etlch.OpenSQL(etlch.RolePipelineWriter, etlch.Options{
		Addr: "127.0.0.1:9000", Database: "openalex", User: "pipeline_writer", Password: pipePass,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pipeDB.Close()
	slot, err := pipeline.NewSlot(pipeDB)
	if err != nil {
		t.Fatal(err)
	}
	store, err := pipeline.NewStore(slot, "openalex")
	if err != nil {
		t.Fatal(err)
	}
	ckpt, err := pipeline.NewCheckpointStore(dataConn, "openalex")
	if err != nil {
		t.Fatal(err)
	}
	snk, err := sink.NewClickHouseSink(dataConn, "openalex")
	if err != nil {
		t.Fatal(err)
	}
	backend := New(Config{
		S3: client, Sink: snk, Pipeline: store, Checkpoints: ckpt,
		CheckpointDir: t.TempDir(),
		TargetHost:    "127.0.0.1", TargetPort: 9000,
	}, nil)

	manager := jobs.NewManager()
	job, err := manager.Start(ctx, jobs.KindIngest, func(ctx context.Context, j *jobs.Job) error {
		return backend.RunIngest(ctx, j, httpapi.IngestRequest{Source: "s3", ShardTotal: 1})
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-job.Done():
	case <-ctx.Done():
		t.Fatal("job did not finish")
	}
	if err := job.Err(); err != nil {
		t.Fatalf("ingest failed: %v", err)
	}

	st, err := store.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !st.Published() || st.DataVersion != 1 || st.SnapshotDate == nil ||
		st.SnapshotDate.Format("2006-01-02") != "2026-11-07" {
		t.Fatalf("state after ingest = %+v", st)
	}

	var works, topics, checkpoints uint64
	for _, q := range []struct {
		name string
		sql  string
		dst  *uint64
	}{
		{"works", "SELECT count() FROM openalex.works", &works},
		{"topics", "SELECT count() FROM openalex.dim_topics", &topics},
		{"checkpoints", "SELECT count() FROM openalex.etl_checkpoints", &checkpoints},
	} {
		if err := admin.QueryRow(ctx, q.sql).Scan(q.dst); err != nil {
			t.Fatalf("%s: %v", q.name, err)
		}
	}
	if works != 2 || topics != 1 || checkpoints != 2 {
		t.Fatalf("works=%d topics=%d checkpoints=%d, want 2/1/2", works, topics, checkpoints)
	}
}
