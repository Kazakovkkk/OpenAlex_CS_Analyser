package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
)

type prefetchRowFile struct {
	ID   string `parquet:"id"`
	Big  string `parquet:"big_unused"`
	Year int32  `parquet:"year"`
}

type prefetchRowView struct {
	ID   string `parquet:"id"`
	Year int32  `parquet:"year"`
}

func prefetchNoise(n int, seed uint64) string {
	b := make([]byte, n)
	x := seed*6364136223846793005 + 1442695040888963407
	for i := range b {
		x = x*6364136223846793005 + 1442695040888963407
		b[i] = byte('a' + (x>>33)%26)
	}
	return string(b)
}

func writePrefetchTestFile(t *testing.T, rows int) (data []byte, expected []prefetchRowView) {
	t.Helper()

	in := make([]prefetchRowFile, rows)
	expected = make([]prefetchRowView, rows)
	for i := range rows {
		id := fmt.Sprintf("https://openalex.org/W%d", 1000+i)
		in[i] = prefetchRowFile{
			ID:   id,
			Big:  prefetchNoise(4096, uint64(i)),
			Year: int32(1990 + i%35),
		}
		expected[i] = prefetchRowView{ID: id, Year: in[i].Year}
	}

	var buf bytes.Buffer
	w := parquet.NewGenericWriter[prefetchRowFile](&buf, parquet.MaxRowsPerRowGroup(100))
	if _, err := w.Write(in); err != nil {
		t.Fatalf("parquet write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("parquet close: %v", err)
	}
	return buf.Bytes(), expected
}

func TestPrefetchPlanUsesProjection(t *testing.T) {
	t.Parallel()

	data, _ := writePrefetchTestFile(t, 300)
	pf, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	md := pf.Metadata()
	if len(md.RowGroups) < 3 {
		t.Fatalf("row groups = %d, want >= 3 (test file must exercise multi-group routing)", len(md.RowGroups))
	}

	plan, err := BuildPrefetchPlanFor(md, &prefetchRowView{})
	if err != nil {
		t.Fatalf("BuildPrefetchPlanFor: %v", err)
	}
	if len(plan.Groups) != len(md.RowGroups) {
		t.Fatalf("plan groups = %d, want %d", len(plan.Groups), len(md.RowGroups))
	}
	if plan.TotalBytes <= 0 || plan.TotalBytes >= int64(len(data))/2 {
		t.Fatalf("plan.TotalBytes = %d for a %d-byte file: projection did not drop the big column",
			plan.TotalBytes, len(data))
	}
	for i, g := range plan.Groups {
		if len(g.Spans) == 0 {
			t.Fatalf("group %d has no spans", i)
		}
		for j := 1; j < len(g.Spans); j++ {
			if g.Spans[j].Off < g.Spans[j-1].End() {
				t.Fatalf("group %d has overlapping spans: %s then %s", i, g.Spans[j-1], g.Spans[j])
			}
		}
		if g.Lo != g.Spans[0].Off || g.Hi != g.Spans[len(g.Spans)-1].End() {
			t.Fatalf("group %d envelope [%d-%d) does not match spans", i, g.Lo, g.Hi)
		}
		if i > 0 && g.Lo < plan.Groups[i-1].Hi {
			t.Fatalf("groups %d and %d overlap", i-1, i)
		}
	}
}

func TestPrefetchPlanRejectsUnknownProjection(t *testing.T) {
	t.Parallel()

	data, _ := writePrefetchTestFile(t, 100)
	pf, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := BuildPrefetchPlan(pf.Metadata(), []string{"id", "does_not_exist"}); err == nil {
		t.Fatal("expected an error for a projected column that is missing from the file")
	}
}

func readAllWithPlan(t *testing.T, data []byte, budget int64) ([]prefetchRowView, RangeStats, int64) {
	t.Helper()

	fetcher := &memRangeFetcher{data: data}
	ra := NewRangeReaderAt(context.Background(), fetcher, int64(len(data)), RangeOptions{
		BufferSize:      DefaultRangeBufferSize,
		FooterCacheSize: DefaultFooterCacheSize,
		FooterProbeSize: 64 << 10,
		PrefetchBudget:  budget,
		PrefetchWorkers: 4,
	})

	pf, err := parquet.OpenFile(ra, int64(len(data)), parquet.SkipPageIndex(true))
	if err != nil {
		t.Fatalf("OpenFile via RangeReaderAt: %v", err)
	}
	plan, err := BuildPrefetchPlanFor(pf.Metadata(), &prefetchRowView{})
	if err != nil {
		t.Fatalf("BuildPrefetchPlanFor: %v", err)
	}
	ra.SetPrefetchPlan(plan)

	reader := parquet.NewGenericReader[prefetchRowView](pf)
	defer reader.Close()

	var out []prefetchRowView
	buf := make([]prefetchRowView, 7)
	for {
		n, readErr := reader.Read(buf)
		out = append(out, buf[:n]...)
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			t.Fatalf("parquet read: %v", readErr)
		}
		if n == 0 {
			break
		}
	}
	return out, ra.Stats(), plan.TotalBytes
}

func TestPrefetchWholeGroupDownloadsEachByteOnce(t *testing.T) {
	t.Parallel()

	data, want := writePrefetchTestFile(t, 300)
	got, st, planBytes := readAllWithPlan(t, data, DefaultPrefetchBudget)
	if len(got) != len(want) {
		t.Fatalf("rows = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("row %d = %+v, want %+v", i, got[i], want[i])
		}
	}

	if !st.HasPlan {
		t.Fatal("plan was not installed")
	}
	if st.PlanMisses != 0 {
		t.Fatalf("plan misses = %d, want 0 (every read must hit the plan)", st.PlanMisses)
	}
	if st.PrefetchBytes != uint64(planBytes) {
		t.Fatalf("prefetched %d bytes, plan is %d", st.PrefetchBytes, planBytes)
	}
	maxAllowed := uint64(64<<10) + uint64(planBytes) + 16
	if st.BytesRead > maxAllowed {
		t.Fatalf("total read %d bytes > allowed %d (footer probe + plan + magic)", st.BytesRead, maxAllowed)
	}
	if st.PrefetchBursts == 0 {
		t.Fatal("expected at least one prefetch burst")
	}
}

func TestPrefetchWindowedModeStillExact(t *testing.T) {
	t.Parallel()
	data, want := writePrefetchTestFile(t, 300)

	const budget = 32 << 10
	got, st, planBytes := readAllWithPlan(t, data, budget)
	if len(got) != len(want) {
		t.Fatalf("rows = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("row %d = %+v, want %+v", i, got[i], want[i])
		}
	}
	if st.PrefetchBytes != uint64(planBytes) {
		t.Fatalf("prefetched %d bytes, plan is %d (windowed mode must not re-download)", st.PrefetchBytes, planBytes)
	}
	if st.PrefetchBursts < 2 {
		t.Fatalf("bursts = %d, want >= 2 in windowed mode", st.PrefetchBursts)
	}
	if st.PlanMisses != 0 {
		t.Fatalf("plan misses = %d, want 0", st.PlanMisses)
	}
}

func TestPrefetchMemoryStaysWithinBudget(t *testing.T) {
	t.Parallel()

	data, _ := writePrefetchTestFile(t, 300)
	fetcher := &memRangeFetcher{data: data}
	const budget = 48 << 10
	ra := NewRangeReaderAt(context.Background(), fetcher, int64(len(data)), RangeOptions{
		FooterCacheSize: DefaultFooterCacheSize,
		FooterProbeSize: 64 << 10,
		PrefetchBudget:  budget,
		PrefetchWorkers: 2,
	})
	pf, err := parquet.OpenFile(ra, int64(len(data)), parquet.SkipPageIndex(true))
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	plan, err := BuildPrefetchPlanFor(pf.Metadata(), &prefetchRowView{})
	if err != nil {
		t.Fatalf("BuildPrefetchPlanFor: %v", err)
	}
	ra.SetPrefetchPlan(plan)

	first := plan.Groups[0].Spans[0]
	buf := make([]byte, 32)
	if _, err := ra.ReadAt(buf, first.Off); err != nil {
		t.Fatalf("ReadAt: %v", err)
	}

	ra.mu.Lock()
	var windowCap int64
	for i := range ra.windows {
		windowCap += int64(cap(ra.windows[i].buf))
	}
	nSpans := len(ra.windows)
	ra.mu.Unlock()
	allowed := max(budget, int64(nSpans)*minWindowSize)
	if windowCap > allowed {
		t.Fatalf("window capacity %d > allowed %d (spans=%d budget=%d)", windowCap, allowed, nSpans, budget)
	}
	if nSpans != len(plan.Groups[0].Spans) {
		t.Fatalf("windows = %d, want %d", nSpans, len(plan.Groups[0].Spans))
	}
}

type gatedFetcher struct {
	data []byte

	mu          sync.Mutex
	blockStart  int64
	blockEnd    int64
	failStart   int64
	failEnd     int64
	failErr     error
	blockedOnce sync.Once
	blocked     chan struct{}
	release     chan struct{}
}

func newGatedFetcher(data []byte) *gatedFetcher {
	return &gatedFetcher{
		data:    data,
		blocked: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (f *gatedFetcher) FetchRangeInto(ctx context.Context, start, end int64, dst []byte) (int, error) {
	f.mu.Lock()
	blocking := f.blockStart != 0 || f.blockEnd != 0
	inBlock := blocking && start < f.blockEnd+1 && end >= f.blockStart
	inFail := f.failErr != nil && start < f.failEnd+1 && end >= f.failStart
	err := f.failErr
	f.mu.Unlock()

	if inFail {
		return 0, err
	}
	if inBlock {
		f.blockedOnce.Do(func() { close(f.blocked) })
		select {
		case <-f.release:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	if start < 0 || end >= int64(len(f.data)) || end < start {
		return 0, fmt.Errorf("invalid range [%d-%d] for size %d", start, end, len(f.data))
	}
	return copy(dst, f.data[start:end+1]), nil
}

func newPrefetchReaderAt(t *testing.T, fetcher RangeFetcher, data []byte) (*RangeReaderAt, *PrefetchPlan) {
	t.Helper()
	ra := NewRangeReaderAt(context.Background(), fetcher, int64(len(data)), RangeOptions{
		FooterCacheSize: DefaultFooterCacheSize,
		FooterProbeSize: 64 << 10,
		PrefetchBudget:  DefaultPrefetchBudget,
		PrefetchWorkers: 4,
	})
	pf, err := parquet.OpenFile(ra, int64(len(data)), parquet.SkipPageIndex(true))
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	plan, err := BuildPrefetchPlanFor(pf.Metadata(), &prefetchRowView{})
	if err != nil {
		t.Fatalf("BuildPrefetchPlanFor: %v", err)
	}
	ra.SetPrefetchPlan(plan)
	return ra, plan
}

func TestPrefetchSlowBackgroundFetchDoesNotBlock(t *testing.T) {
	t.Parallel()

	data, _ := writePrefetchTestFile(t, 300)
	fetcher := newGatedFetcher(data)
	ra, plan := newPrefetchReaderAt(t, fetcher, data)

	g := plan.Groups[0]
	if len(g.Spans) < 2 {
		t.Fatalf("test needs >= 2 spans in the first row group, got %d", len(g.Spans))
	}
	target, blockedSpan := g.Spans[0], g.Spans[1]

	fetcher.mu.Lock()
	fetcher.blockStart, fetcher.blockEnd = blockedSpan.Off, blockedSpan.End()-1
	fetcher.mu.Unlock()

	firstDone := make(chan error, 1)
	go func() {
		buf := make([]byte, 32)
		_, err := ra.ReadAt(buf, target.Off)
		firstDone <- err
	}()

	select {
	case <-fetcher.blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("the blocked window was never requested; the burst did not include other chunks")
	}

	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatalf("ReadAt of the target chunk returned %v while another chunk was blocked", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ReadAt blocked on an unrelated slow fetch: the burst is still synchronous")
	}
	close(fetcher.release)
	want := make([]byte, 32)
	if _, err := ra.ReadAt(want, blockedSpan.Off); err != nil {
		t.Fatalf("ReadAt of the released chunk: %v", err)
	}
	if !bytes.Equal(want, data[blockedSpan.Off:blockedSpan.Off+32]) {
		t.Fatal("released chunk returned wrong bytes")
	}
	if st := ra.Stats(); st.PrefetchBytes != uint64(g.TotalBytes) {
		t.Fatalf("prefetched %d bytes, row group 0 payload is %d", st.PrefetchBytes, g.TotalBytes)
	}
}

func TestPrefetchBackgroundErrorSurfaces(t *testing.T) {
	t.Parallel()

	data, _ := writePrefetchTestFile(t, 100)
	fetcher := newGatedFetcher(data)
	ra, plan := newPrefetchReaderAt(t, fetcher, data)

	g := plan.Groups[0]
	if len(g.Spans) < 2 {
		t.Fatalf("test needs >= 2 spans, got %d", len(g.Spans))
	}
	target, failing := g.Spans[0], g.Spans[1]

	fetcher.mu.Lock()
	fetcher.failStart, fetcher.failEnd = failing.Off, failing.End()-1
	fetcher.failErr = errors.New("injected range failure")
	fetcher.mu.Unlock()

	buf := make([]byte, 32)
	if _, err := ra.ReadAt(buf, target.Off); err != nil {
		t.Fatalf("read of the healthy chunk: %v", err)
	}
	if _, err := ra.ReadAt(buf, failing.Off); err == nil {
		t.Fatal("expected the injected background failure to surface")
	}
}
