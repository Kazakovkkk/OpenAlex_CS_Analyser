package loadtest

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"testing"
	"time"

	"go-etl/internal/normalizer"
	"go-etl/internal/sink"
)

// TestSampleSinkWritesAccepted records the JSON contract: the load-test sample
// must parse and pass the corpus gate, otherwise a benchmark measures nothing.
func TestSampleSinkWritesAccepted(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	s := &sampleSink{w: gz, limit: 1}

	b := sink.NewWorksBatch(1)
	b.WorkIDs = append(b.WorkIDs, 123)
	b.UpdatedDates = append(b.UpdatedDates, time.Date(2026, 8, 13, 0, 0, 0, 0, time.UTC))
	b.WorkTypes = append(b.WorkTypes, "article")
	b.PrimaryFieldIDs = append(b.PrimaryFieldIDs, 17)
	b.IsRetracted = append(b.IsRetracted, 0)
	b.PubYears = append(b.PubYears, 2020)
	b.PubDates = append(b.PubDates, nil)
	b.PrimaryTopicIDs = append(b.PrimaryTopicIDs, 10001)
	b.TopicIDs = append(b.TopicIDs, []uint32{10001, 10002})
	b.TopicScores = append(b.TopicScores, []float32{0.7, 0})
	b.PrimarySourceIDs = append(b.PrimarySourceIDs, 0)
	b.PrimarySourceTypes = append(b.PrimarySourceTypes, "")
	b.FWCIs = append(b.FWCIs, nil)

	if err := s.WriteWorksBatch(context.Background(), b); err != nil {
		t.Fatalf("WriteWorksBatch: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}

	gr, err := gzip.NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("gzip open: %v", err)
	}
	var line bytes.Buffer
	if _, err := line.ReadFrom(gr); err != nil {
		t.Fatalf("read: %v", err)
	}
	_ = gr.Close()

	var w normalizer.JSONWork
	if err := json.Unmarshal(bytes.TrimSpace(line.Bytes()), &w); err != nil {
		t.Fatalf("decode sample line: %v", err)
	}
	if !normalizer.IsCSJSONWork(&w) {
		t.Fatalf("sample record does not pass the corpus gate: %s", line.Bytes())
	}
	if len(w.Topics) != 2 {
		t.Fatalf("topics = %d, want 2", len(w.Topics))
	}
}
