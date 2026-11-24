package s3

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"

	"go-etl/internal/metrics"
	"go-etl/internal/normalizer"
	"go-etl/internal/sink"
)

func testPtr[T any](v T) *T { return &v }

func newMockS3Client(t *testing.T, objects map[string][]byte) (*Client, func()) {
	t.Helper()

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
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(data)
			return
		}

		parts := strings.Split(strings.TrimPrefix(rangeHdr, "bytes="), "-")
		start, err1 := strconv.ParseInt(parts[0], 10, 64)
		end, err2 := strconv.ParseInt(parts[1], 10, 64)
		if err1 != nil || err2 != nil || start < 0 || end >= int64(len(data)) || end < start {
			http.Error(w, "invalid Range", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		slice := data[start : end+1]
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
		w.Header().Set("Content-Length", strconv.Itoa(len(slice)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(slice)
	}))

	srvURL, err := url.Parse(srv.URL)
	if err != nil {
		srv.Close()
		t.Fatalf("url.Parse: %v", err)
	}
	hc := &http.Client{
		Transport: &rewriteHostTransport{target: srvURL, base: srv.Client().Transport},
	}
	client, err := NewAnonymousClient(context.Background(), WithHTTPClient(hc))
	if err != nil {
		srv.Close()
		t.Fatalf("NewAnonymousClient: %v", err)
	}
	return client, srv.Close
}

func buildParquetBytes[T any](t *testing.T, rows []T) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[T](&buf)
	if _, err := w.Write(rows); err != nil {
		t.Fatalf("parquet Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("parquet Close: %v", err)
	}
	return buf.Bytes()
}

func TestProcessWorksFileFullGetAndRangeParity(t *testing.T) {
	t.Parallel()
	csTopic := &normalizer.ParquetPrimaryTopic{
		ID:    testPtr("https://openalex.org/T12799"),
		Score: testPtr(float32(0.9)),
		Field: &normalizer.ParquetIDRef{ID: testPtr("https://openalex.org/fields/17")},
	}
	bioTopic := &normalizer.ParquetPrimaryTopic{
		ID:    testPtr("https://openalex.org/T10089"),
		Score: testPtr(float32(0.8)),
		Field: &normalizer.ParquetIDRef{ID: testPtr("https://openalex.org/fields/23")},
	}

	rows := make([]normalizer.ParquetWork, 0, 12)
	for i := range 6 {
		rows = append(rows, normalizer.ParquetWork{
			ID:                    testPtr(fmt.Sprintf("https://openalex.org/W%d", 1000+i)),
			Title:                 testPtr(fmt.Sprintf("CS Paper %d", i)),
			PublicationYear:       testPtr(int32(2020 + i)),
			IsXPAC:                testPtr(false),
			PrimaryTopic:          csTopic,
			AbstractInvertedIndex: testPtr(`{"go":[0],"etl":[1]}`),
		})
	}
	rows = append(rows, rows[0])
	for i := range 2 {
		rows = append(rows, normalizer.ParquetWork{
			ID:           testPtr(fmt.Sprintf("https://openalex.org/W%d", 2000+i)),
			IsXPAC:       testPtr(true),
			PrimaryTopic: csTopic,
		})
	}
	for i := range 3 {
		rows = append(rows, normalizer.ParquetWork{
			ID:           testPtr(fmt.Sprintf("https://openalex.org/W%d", 3000+i)),
			IsXPAC:       testPtr(false),
			PrimaryTopic: bioTopic,
		})
	}

	raw := buildParquetBytes(t, rows)
	const key = "data/parquet/works/updated_date=2026-10-14/part_0000.parquet"
	client, cleanup := newMockS3Client(t, map[string][]byte{key: raw})
	defer cleanup()

	entry := ManifestEntry{
		URL:  "s3://" + DefaultBucket + "/" + key,
		Meta: ManifestMeta{ContentLength: int64(len(raw)), RecordCount: int64(len(rows))},
	}

	modes := []struct {
		name     string
		mode     ReadMode
		wantMode ReadMode
	}{
		{name: "FullGet", mode: ReadModeFullGet, wantMode: ReadModeFullGet},
		{name: "BufferedRange", mode: ReadModeRange, wantMode: ReadModeRange},
		{name: "HybridSmallSelectsFullGet", mode: ReadModeHybrid, wantMode: ReadModeFullGet},
	}

	for _, tc := range modes {
		t.Run(tc.name, func(t *testing.T) {
			pool := sink.NewWorksBatchPool(4, 16)
			noop := sink.NewNoopSink()
			norm := normalizer.NewNormalizer(normalizer.NewWorkDeduplicator())
			jm := metrics.NewJobMetrics()

			opts := DefaultParquetReadOptions()
			opts.Mode = tc.mode
			opts.BatchSize = 4
			opts.RangeOpts = RangeOptions{BufferSize: 4 << 10, FooterCacheSize: 2 << 10, MaxRetries: 2}
			opts.Metrics = jm

			res, err := ProcessWorksFile(context.Background(), client, entry, norm, pool, noop, opts)
			if err != nil {
				t.Fatalf("ProcessWorksFile(%s): %v", tc.name, err)
			}
			if res.UsedMode != tc.wantMode {
				t.Fatalf("UsedMode = %s, want %s", res.UsedMode, tc.wantMode)
			}
			if res.ScannedRows != 12 || res.MatchedRows != 7 || res.DedupDropped != 1 || res.WrittenRows != 6 {
				t.Fatalf("unexpected row counts: %+v", res)
			}
			if res.BytesRead == 0 || res.HTTPRequests == 0 {
				t.Fatalf("expected non-zero network stats: %+v", res)
			}
			st := noop.Stats()
			if st.WorksBatches != 2 || st.WorksRows != 6 {
				t.Fatalf("NoopSink stats = (%d batches, %d rows), want (2, 6)", st.WorksBatches, st.WorksRows)
			}
			snap := jm.Snapshot()
			if snap.ScannedRecords != 12 || snap.MatchedRecords != 7 || snap.WrittenRecords != 6 || snap.DedupDropped != 1 {
				t.Fatalf("JobMetrics mismatch: %+v", snap)
			}
		})
	}
}

func TestProcessDimensionsFiles(t *testing.T) {
	t.Parallel()

	topicBytes := buildParquetBytes(t, []normalizer.ParquetTopic{
		{ID: testPtr("https://openalex.org/T10001"), DisplayName: testPtr("Machine Learning")},
		{ID: testPtr("https://openalex.org/T10002"), DisplayName: testPtr("Databases")},
		{ID: testPtr("https://openalex.org/T10003"), DisplayName: testPtr("Compilers")},
	})
	instBytes := buildParquetBytes(t, []normalizer.ParquetInstitution{
		{ID: testPtr("https://openalex.org/I1"), DisplayName: testPtr("MIT"), CountryCode: testPtr("us")},
		{ID: testPtr("https://openalex.org/I2"), DisplayName: testPtr("ETH"), CountryCode: testPtr("ch")},
	})

	const (
		topicKey = "data/parquet/topics/part_0000.parquet"
		instKey  = "data/parquet/institutions/part_0000.parquet"
	)
	client, cleanup := newMockS3Client(t, map[string][]byte{
		topicKey: topicBytes,
		instKey:  instBytes,
	})
	defer cleanup()

	noop := sink.NewNoopSink()
	opts := DefaultParquetReadOptions()
	opts.BatchSize = 2

	tRes, err := ProcessTopicsFile(context.Background(), client, ManifestEntry{
		URL:  "s3://" + DefaultBucket + "/" + topicKey,
		Meta: ManifestMeta{ContentLength: int64(len(topicBytes))},
	}, noop, opts)
	if err != nil {
		t.Fatalf("ProcessTopicsFile: %v", err)
	}
	if tRes.WrittenRows != 3 || noop.Stats().TopicsBatches != 2 {
		t.Fatalf("topics result=%+v sink=%+v", tRes, noop.Stats())
	}

	iRes, err := ProcessInstitutionsFile(context.Background(), client, ManifestEntry{
		URL:  "s3://" + DefaultBucket + "/" + instKey,
		Meta: ManifestMeta{ContentLength: int64(len(instBytes))},
	}, noop, opts)
	if err != nil {
		t.Fatalf("ProcessInstitutionsFile: %v", err)
	}
	if iRes.WrittenRows != 2 || noop.Stats().InstitutionsBatches != 1 {
		t.Fatalf("institutions result=%+v sink=%+v", iRes, noop.Stats())
	}
}
