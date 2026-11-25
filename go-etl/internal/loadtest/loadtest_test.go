package loadtest

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"go-etl/internal/metrics"
	"go-etl/internal/sink"
)

func workJSON(id int) []byte {
	return []byte(fmt.Sprintf(`{"id":"https://openalex.org/W%d","title":"Paper %d",`+
		`"is_xpac":false,"publication_year":2024,`+
		`"primary_topic":{"id":"https://openalex.org/T10108","score":0.9,`+
		`"field":{"id":"https://openalex.org/fields/17"}},`+
		`"abstract_inverted_index":{"hello":[0],"world":[1]}}`, id, id))
}

func TestLoadSampleFromGzip(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "sample.jsonl.gz")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	gz := gzip.NewWriter(f)
	for i := range 5 {
		if _, err := gz.Write(append(workJSON(1000+i), '\n')); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	if _, err := gz.Write([]byte("\nnot-json-at-all\n")); err != nil {
		t.Fatalf("write broken: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("gz close: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("file close: %v", err)
	}

	recs, scanned, err := LoadSample(path, 3)
	if err != nil {
		t.Fatalf("LoadSample: %v", err)
	}
	if len(recs) != 3 {
		t.Fatalf("records = %d, want 3", len(recs))
	}
	if !bytes.Contains(recs[0], []byte("W1000")) {
		t.Fatalf("first record = %s", recs[0])
	}

	all, scannedAll, err := LoadSample(path, 100)
	if err != nil {
		t.Fatalf("LoadSample all: %v", err)
	}
	if len(all) != 5 {
		t.Fatalf("records = %d, want 5 (non-JSON line skipped)", len(all))
	}
	if scannedAll != 5 || scanned != 3 {
		t.Fatalf("scanned = %d/%d, want 3/5", scanned, scannedAll)
	}
}

func TestRunWithRecordsGeneratesBatches(t *testing.T) {
	t.Parallel()

	records := make([][]byte, 50)
	for i := range records {
		records[i] = workJSON(2000 + i)
	}

	jm := metrics.NewJobMetrics()
	out := sink.NewNoopSink()
	res, err := RunWithRecords(context.Background(), Config{
		TargetRPS:   100000,
		DurationSec: 1,
		Workers:     3,
		BatchSize:   10,
	}, records, out, jm)
	if err != nil {
		t.Fatalf("RunWithRecords: %v", err)
	}
	if res.Batches == 0 || res.Written == 0 {
		t.Fatalf("nothing was written: %+v", res)
	}
	if res.Scanned < res.Written {
		t.Fatalf("scanned %d < written %d", res.Scanned, res.Written)
	}
	if res.DedupDropped != 0 {
		t.Fatalf("dedup dropped %d rows, want 0", res.DedupDropped)
	}
	if res.Records != 50 || res.Workers != 3 || res.BatchSize != 10 {
		t.Fatalf("config echo = %+v", res)
	}
	if res.ActualRPS <= 0 {
		t.Fatalf("actual rps = %f", res.ActualRPS)
	}
	stats := out.Stats()
	if stats.WorksRows != res.Written {
		t.Fatalf("sink rows = %d, written = %d", stats.WorksRows, res.Written)
	}
	if snap := jm.Snapshot(); snap.WrittenRecords != res.Written {
		t.Fatalf("metrics written = %d, want %d", snap.WrittenRecords, res.Written)
	}
}

func TestRunWithRecordsRespectsDuration(t *testing.T) {
	t.Parallel()

	records := [][]byte{workJSON(1)}
	start := time.Now()
	res, err := RunWithRecords(context.Background(), Config{
		TargetRPS:   1000,
		DurationSec: 1,
		Workers:     1,
		BatchSize:   5,
	}, records, sink.NewNoopSink(), nil)
	if err != nil {
		t.Fatalf("RunWithRecords: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed < 900*time.Millisecond || elapsed > 3*time.Second {
		t.Fatalf("elapsed = %v, want ~1s", elapsed)
	}
	if !res.Stopped {
		t.Fatalf("a duration-limited run must be reported as stopped: %+v", res)
	}
}

func TestRunWithRecordsCancelledFromOutside(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := RunWithRecords(ctx, Config{TargetRPS: 100, DurationSec: 60, Workers: 2, BatchSize: 10},
			[][]byte{workJSON(1)}, sink.NewNoopSink(), nil)
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("cancel must not be an error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RunWithRecords did not return after cancellation")
	}
}

func TestRunWithRecordsErrors(t *testing.T) {
	t.Parallel()

	if _, err := RunWithRecords(context.Background(), Config{}, nil, sink.NewNoopSink(), nil); err == nil {
		t.Fatal("expected an error for nil sink")
	}
	if _, err := RunWithRecords(context.Background(), Config{}, nil, nil, nil); err == nil {
		t.Fatal("expected an error for no records")
	}
}

type failingSink struct{ calls atomic.Int32 }

func (f *failingSink) WriteWorksBatch(context.Context, *sink.WorksBatch) error {
	f.calls.Add(1)
	return fmt.Errorf("sink is down")
}
func (f *failingSink) WriteTopicsBatch(context.Context, *sink.TopicsBatch) error { return nil }
func (f *failingSink) WriteInstitutionsBatch(context.Context, *sink.InstitutionsBatch) error {
	return nil
}
func (f *failingSink) Close() error { return nil }

func TestRunWithRecordsSinkFailure(t *testing.T) {
	t.Parallel()

	fs := &failingSink{}
	_, err := RunWithRecords(context.Background(), Config{TargetRPS: 100000, DurationSec: 5, Workers: 1, BatchSize: 5},
		[][]byte{workJSON(1)}, fs, nil)
	if err == nil {
		t.Fatal("expected the sink error to surface")
	}
	if fs.calls.Load() == 0 {
		t.Fatal("sink was never called")
	}
}

func TestResultMarshalsToJSON(t *testing.T) {
	t.Parallel()

	res := Result{Records: 1, Written: 2, ActualRPS: 3.5, P95BatchMs: 4.5}
	raw, err := json.Marshal(res)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, want := range []string{`"Records":1`, `"Written":2`, `"ActualRPS":3.5`, `"P95BatchMs":4.5`} {
		if !bytes.Contains(raw, []byte(want)) {
			t.Fatalf("json %s does not contain %s", raw, want)
		}
	}
}
