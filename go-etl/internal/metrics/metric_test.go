package metrics

import (
	"math"
	"sync"
	"testing"
	"time"
)

func TestLatencyTrackerPercentiles(t *testing.T) {
	t.Parallel()
	empty := NewLatencyTracker(10)
	if st := empty.Snapshot(); st.Count != 0 || st.AvgMs != 0 || st.P99Ms != 0 {
		t.Fatalf("empty Snapshot = %+v, want all zeros", st)
	}
	neg := NewLatencyTracker(4)
	neg.Record(-10 * time.Millisecond)
	if st := neg.Snapshot(); st.Count != 1 || st.AvgMs != 0 || st.MaxMs != 0 {
		t.Fatalf("negative duration Snapshot = %+v, want Count=1 and 0ms", st)
	}

	tr := NewLatencyTracker(1000)
	for i := 100; i >= 1; i-- {
		tr.Record(time.Duration(i) * time.Millisecond)
	}

	st := tr.Snapshot()
	if st.Count != 100 {
		t.Fatalf("Count = %d, want 100", st.Count)
	}
	assertNear(t, "AvgMs", st.AvgMs, 50.5, 1e-6)
	assertNear(t, "P50Ms", st.P50Ms, 50.0, 1e-6)
	assertNear(t, "P95Ms", st.P95Ms, 95.0, 1e-6)
	assertNear(t, "P99Ms", st.P99Ms, 99.0, 1e-6)
	assertNear(t, "MaxMs", st.MaxMs, 100.0, 1e-6)
}

func TestLatencyTrackerRingBufferOverwrite(t *testing.T) {
	t.Parallel()
	tr := NewLatencyTracker(4)
	valuesMs := []int{100, 200, 10, 20, 30, 40}
	for _, ms := range valuesMs {
		tr.Record(time.Duration(ms) * time.Millisecond)
	}

	st := tr.Snapshot()
	if st.Count != 6 {
		t.Fatalf("Count = %d, want 6", st.Count)
	}
	assertNear(t, "AvgMs", st.AvgMs, 400.0/6.0, 1e-4)
	assertNear(t, "P50Ms", st.P50Ms, 20.0, 1e-6)
	assertNear(t, "P95Ms", st.P95Ms, 40.0, 1e-6)
	assertNear(t, "P99Ms", st.P99Ms, 40.0, 1e-6)
	assertNear(t, "MaxMs", st.MaxMs, 40.0, 1e-6)
}

func TestJobMetricsConcurrentAndFinish(t *testing.T) {
	t.Parallel()

	m := NewJobMetrics()

	const (
		workers = 8
		iters   = 50
	)

	stopReader := make(chan struct{})
	var readerWG sync.WaitGroup
	readerWG.Add(1)
	go func() {
		defer readerWG.Done()
		for {
			select {
			case <-stopReader:
				return
			default:
				_ = m.Snapshot()
			}
		}
	}()

	var writerWG sync.WaitGroup
	writerWG.Add(workers)
	for range workers {
		go func() {
			defer writerWG.Done()
			for range iters {
				m.AddScanned(10)
				m.AddMatched(2)
				m.AddDedupDropped(1)
				m.AddBytesRead(1024)
				m.AddWritten(2, 512)
				m.AddWritten(0, 0)
				m.IncHTTPRequests()
				m.IncRateLimitWaits()
				m.IncAPIRetries()
				m.IncAPI429()
				m.IncAPI5xx()
				m.SetCurrentItem("part_0012.parquet")
				m.RecordBatchDuration(10 * time.Millisecond)
			}
		}()
	}

	writerWG.Wait()
	close(stopReader)
	readerWG.Wait()

	time.Sleep(20 * time.Millisecond)
	m.Finish()
	frozenElapsed := m.Elapsed()
	if frozenElapsed <= 0 {
		t.Fatalf("frozenElapsed = %v, want > 0", frozenElapsed)
	}
	time.Sleep(20 * time.Millisecond)
	m.Finish()
	if got := m.Elapsed(); got != frozenElapsed {
		t.Fatalf("Elapsed() changed after Finish(): got %v, want %v", got, frozenElapsed)
	}

	snap := m.Snapshot()
	const totalOps = uint64(workers * iters)
	if snap.ScannedRecords != totalOps*10 ||
		snap.MatchedRecords != totalOps*2 ||
		snap.WrittenRecords != totalOps*2 ||
		snap.DedupDropped != totalOps ||
		snap.BatchesWritten != totalOps ||
		snap.BytesRead != totalOps*1024 ||
		snap.WrittenBytes != totalOps*512 {
		t.Fatalf("unexpected volume counters: %+v", snap)
	}
	if snap.HTTPRequests != totalOps ||
		snap.RateLimitWaits != totalOps ||
		snap.APIRetries != totalOps ||
		snap.API429Count != totalOps ||
		snap.API5xxCount != totalOps {
		t.Fatalf("unexpected API/HTTP counters: %+v", snap)
	}
	if snap.CurrentItem != "part_0012.parquet" {
		t.Fatalf("CurrentItem = %q, want part_0012.parquet", snap.CurrentItem)
	}
	if snap.ElapsedSec <= 0 || snap.ScanRPS <= 0 || snap.CurrentRPS <= 0 || snap.ScanMBPerSec <= 0 || snap.IngestMBPerSec <= 0 {
		t.Fatalf("expected positive throughput rates: %+v", snap)
	}
	assertNear(t, "AvgBatchMs", snap.AvgBatchMs, 10.0, 1e-6)
	assertNear(t, "P95BatchMs", snap.P95BatchMs, 10.0, 1e-6)
	assertNear(t, "P99BatchMs", snap.P99BatchMs, 10.0, 1e-6)
}

func assertNear(t *testing.T, name string, got, want, eps float64) {
	t.Helper()
	if math.Abs(got-want) > eps {
		t.Fatalf("%s = %.6f, want %.6f (±%g)", name, got, want, eps)
	}
}
