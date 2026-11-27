package metrics

import (
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

const (
	DefaultLatencyWindow = 10_000
)

type LatencyStats struct {
	Count uint64
	AvgMs float64
	P50Ms float64
	P95Ms float64
	P99Ms float64
	MaxMs float64
}

type LatencyTracker struct {
	mu         sync.Mutex
	samples    []int64
	maxSamples int
	writePos   int
	count      uint64
	sumNs      int64
}

func NewLatencyTracker(maxSamples int) *LatencyTracker {
	if maxSamples <= 0 {
		maxSamples = DefaultLatencyWindow
	}
	return &LatencyTracker{
		samples:    make([]int64, 0, min(maxSamples, 1024)),
		maxSamples: maxSamples,
	}
}

func (t *LatencyTracker) Record(d time.Duration) {
	ns := d.Nanoseconds()
	if ns < 0 {
		ns = 0
	}

	t.mu.Lock()
	t.count++
	t.sumNs += ns
	if len(t.samples) < t.maxSamples {
		t.samples = append(t.samples, ns)
	} else {
		t.samples[t.writePos] = ns
		t.writePos = (t.writePos + 1) % t.maxSamples
	}
	t.mu.Unlock()
}

func (t *LatencyTracker) Snapshot() LatencyStats {
	t.mu.Lock()
	if t.count == 0 {
		t.mu.Unlock()
		return LatencyStats{}
	}
	count := t.count
	sumNs := t.sumNs
	buf := make([]int64, len(t.samples))
	copy(buf, t.samples)
	t.mu.Unlock()

	slices.Sort(buf)

	const nsPerMs = float64(time.Millisecond)
	return LatencyStats{
		Count: count,
		AvgMs: (float64(sumNs) / float64(count)) / nsPerMs,
		P50Ms: float64(percentileInt64(buf, 0.50)) / nsPerMs,
		P95Ms: float64(percentileInt64(buf, 0.95)) / nsPerMs,
		P99Ms: float64(percentileInt64(buf, 0.99)) / nsPerMs,
		MaxMs: float64(buf[len(buf)-1]) / nsPerMs,
	}
}

func percentileInt64(sorted []int64, p float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(p*float64(len(sorted)))) - 1
	idx = max(0, min(idx, len(sorted)-1))
	return sorted[idx]
}

type Snapshot struct {
	ScannedRecords uint64
	MatchedRecords uint64
	WrittenRecords uint64
	DedupDropped   uint64
	BatchesWritten uint64

	RejectedRecords  uint64
	RejectedByReason map[string]uint64

	BytesRead    uint64
	WrittenBytes uint64

	HTTPRequests   uint64
	RateLimitWaits uint64
	APIRetries     uint64
	API429Count    uint64
	API5xxCount    uint64

	ElapsedSec     float64
	ScanRPS        float64
	CurrentRPS     float64
	ScanMBPerSec   float64
	IngestMBPerSec float64

	AvgBatchMs float64
	P50BatchMs float64
	P95BatchMs float64
	P99BatchMs float64
	MaxBatchMs float64

	CurrentItem string
}

type JobMetrics struct {
	startedAt        time.Time
	finalElapsedNano atomic.Int64

	scannedRecords atomic.Uint64
	matchedRecords atomic.Uint64
	writtenRecords atomic.Uint64
	dedupDropped   atomic.Uint64
	batchesWritten atomic.Uint64

	rejectedRecords atomic.Uint64
	rejectedMu      sync.Mutex
	rejectedReasons map[string]uint64

	bytesRead    atomic.Uint64
	writtenBytes atomic.Uint64

	httpRequests   atomic.Uint64
	rateLimitWaits atomic.Uint64
	apiRetries     atomic.Uint64
	api429Count    atomic.Uint64
	api5xxCount    atomic.Uint64

	currentItem  atomic.Pointer[string]
	batchLatency *LatencyTracker
}

func NewJobMetrics() *JobMetrics {
	m := &JobMetrics{
		startedAt:    time.Now(),
		batchLatency: NewLatencyTracker(DefaultLatencyWindow),
	}
	m.finalElapsedNano.Store(-1)
	return m
}

func (m *JobMetrics) Finish() {
	d := time.Since(m.startedAt)
	if d < 0 {
		d = 0
	}
	m.finalElapsedNano.CompareAndSwap(-1, int64(d))
}

func (m *JobMetrics) Elapsed() time.Duration {
	if ns := m.finalElapsedNano.Load(); ns >= 0 {
		return time.Duration(ns)
	}
	d := time.Since(m.startedAt)
	if d < 0 {
		return 0
	}
	return d
}

func (m *JobMetrics) AddScanned(n uint64)      { m.scannedRecords.Add(n) }
func (m *JobMetrics) AddMatched(n uint64)      { m.matchedRecords.Add(n) }
func (m *JobMetrics) AddDedupDropped(n uint64) { m.dedupDropped.Add(n) }

func (m *JobMetrics) AddRejected(reason string) {
	m.rejectedRecords.Add(1)
	if reason == "" {
		return
	}
	m.rejectedMu.Lock()
	if m.rejectedReasons == nil {
		m.rejectedReasons = make(map[string]uint64, 8)
	}
	m.rejectedReasons[reason]++
	m.rejectedMu.Unlock()
}

func (m *JobMetrics) RejectedByReason() map[string]uint64 {
	m.rejectedMu.Lock()
	defer m.rejectedMu.Unlock()
	if len(m.rejectedReasons) == 0 {
		return nil
	}
	out := make(map[string]uint64, len(m.rejectedReasons))
	for k, v := range m.rejectedReasons {
		out[k] = v
	}
	return out
}
func (m *JobMetrics) AddBytesRead(n uint64)    { m.bytesRead.Add(n) }
func (m *JobMetrics) AddHTTPRequests(n uint64) { m.httpRequests.Add(n) }
func (m *JobMetrics) IncHTTPRequests()         { m.httpRequests.Add(1) }
func (m *JobMetrics) IncRateLimitWaits()       { m.rateLimitWaits.Add(1) }
func (m *JobMetrics) IncAPIRetries()           { m.apiRetries.Add(1) }
func (m *JobMetrics) IncAPI429()               { m.api429Count.Add(1) }
func (m *JobMetrics) IncAPI5xx()               { m.api5xxCount.Add(1) }

func (m *JobMetrics) AddWritten(rows, approxBytes uint64) {
	if rows == 0 {
		return
	}
	m.batchesWritten.Add(1)
	m.writtenRecords.Add(rows)
	m.writtenBytes.Add(approxBytes)
}

func (m *JobMetrics) RecordBatchDuration(d time.Duration) {
	m.batchLatency.Record(d)
}

func (m *JobMetrics) SetCurrentItem(item string) {
	m.currentItem.Store(&item)
}

func (m *JobMetrics) CurrentItem() string {
	if p := m.currentItem.Load(); p != nil {
		return *p
	}
	return ""
}

func (m *JobMetrics) Snapshot() Snapshot {
	elapsed := m.Elapsed()
	elapsedSec := elapsed.Seconds()

	scanned := m.scannedRecords.Load()
	matched := m.matchedRecords.Load()
	written := m.writtenRecords.Load()
	bytesRead := m.bytesRead.Load()
	writtenBytes := m.writtenBytes.Load()

	var scanRPS, currentRPS, scanMBs, ingestMBs float64
	if elapsedSec > 0 {
		const mib = 1 << 20
		scanRPS = float64(scanned) / elapsedSec
		currentRPS = float64(written) / elapsedSec
		scanMBs = (float64(bytesRead) / mib) / elapsedSec
		ingestMBs = (float64(writtenBytes) / mib) / elapsedSec
	}

	lat := m.batchLatency.Snapshot()

	return Snapshot{
		ScannedRecords:   scanned,
		MatchedRecords:   matched,
		WrittenRecords:   written,
		DedupDropped:     m.dedupDropped.Load(),
		BatchesWritten:   m.batchesWritten.Load(),
		RejectedRecords:  m.rejectedRecords.Load(),
		RejectedByReason: m.RejectedByReason(),
		BytesRead:        bytesRead,
		WrittenBytes:     writtenBytes,
		HTTPRequests:     m.httpRequests.Load(),
		RateLimitWaits:   m.rateLimitWaits.Load(),
		APIRetries:       m.apiRetries.Load(),
		API429Count:      m.api429Count.Load(),
		API5xxCount:      m.api5xxCount.Load(),
		ElapsedSec:       elapsedSec,
		ScanRPS:          scanRPS,
		CurrentRPS:       currentRPS,
		ScanMBPerSec:     scanMBs,
		IngestMBPerSec:   ingestMBs,
		AvgBatchMs:       lat.AvgMs,
		P50BatchMs:       lat.P50Ms,
		P95BatchMs:       lat.P95Ms,
		P99BatchMs:       lat.P99Ms,
		MaxBatchMs:       lat.MaxMs,
		CurrentItem:      m.CurrentItem(),
	}
}
