package ingest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	s3 "go-etl/internal/adapters/s3"
)

func fakeEntry(i int, size int64) Entry {
	return Entry{
		URL:  fmt.Sprintf("s3://openalex/data/parquet/works/updated_date=2026-01-01/part_%04d.parquet", i),
		Meta: s3.ManifestMeta{ContentLength: size, RecordCount: 100},
	}
}

func fakeEntries(n int) []Entry {
	out := make([]Entry, n)
	for i := range out {
		out[i] = fakeEntry(i, int64(n-i)*1<<20)
	}
	return out
}

type fakeProcessor struct {
	mu              sync.Mutex
	seen            []string
	calls           map[string]int
	failURLs        map[string]error
	blockTillCancel bool
	started         chan struct{}
	startOnce       sync.Once
	delay           time.Duration
	rows            uint64
	truncated       bool
}

func newFakeProcessor() *fakeProcessor {
	return &fakeProcessor{calls: make(map[string]int), failURLs: make(map[string]error), started: make(chan struct{})}
}

func (f *fakeProcessor) Process(ctx context.Context, e Entry, limit int64) (FileResult, error) {
	f.mu.Lock()
	f.calls[e.URL]++
	f.seen = append(f.seen, e.URL)
	err := f.failURLs[e.URL]
	block := f.blockTillCancel
	delay := f.delay
	f.mu.Unlock()

	f.startOnce.Do(func() { close(f.started) })

	if block {
		<-ctx.Done()
		return FileResult{}, ctx.Err()
	}
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return FileResult{}, ctx.Err()
		}
	}
	if err != nil {
		return FileResult{}, err
	}
	written := uint64(9)
	if limit > 0 && uint64(limit) < written {
		written = uint64(limit)
	}
	return FileResult{
		ScannedRows:       100,
		MatchedRows:       10,
		WrittenRows:       written,
		DedupDropped:      1,
		Truncated:         f.truncated,
		BytesRead:         uint64(e.Meta.ContentLength),
		HTTPRequests:      1,
		PrefetchPlanBytes: 42,
	}, nil
}

func (f *fakeProcessor) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	total := 0
	for _, n := range f.calls {
		total += n
	}
	return total
}

func (f *fakeProcessor) uniqueVisited() map[string]struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]struct{}, len(f.seen))
	for _, u := range f.seen {
		out[u] = struct{}{}
	}
	return out
}

func quietOptions() Options {
	return Options{Workers: 4, Order: OrderManifest, FatalFailureStreak: -1, Logger: discardLogger()}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestRunProcessesEveryFile(t *testing.T) {
	t.Parallel()

	entries := fakeEntries(40)
	p := newFakeProcessor()
	res, err := Run(context.Background(), entries, p, quietOptions())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.FilesTotal != 40 || res.FilesDone != 40 || res.FilesFailed != 0 {
		t.Fatalf("result = %+v, want 40/40/0", res)
	}
	if len(p.uniqueVisited()) != 40 {
		t.Fatalf("visited %d unique files, want 40", len(p.uniqueVisited()))
	}
	if res.Scanned != 4000 || res.Written != 360 || res.DedupDropped != 40 {
		t.Fatalf("counters = scanned %d written %d dedup %d", res.Scanned, res.Written, res.DedupDropped)
	}
	if res.PlanBytes != 40*42 {
		t.Fatalf("plan bytes = %d, want %d", res.PlanBytes, 40*42)
	}
	if res.Stopped {
		t.Fatal("run must not be reported as stopped")
	}
}

func TestRunContinuesAfterFileFailure(t *testing.T) {
	t.Parallel()

	entries := fakeEntries(30)
	p := newFakeProcessor()
	for _, i := range []int{3, 7, 11} {
		p.failURLs[entries[i].URL] = errors.New("simulated corrupt object")
	}
	opts := quietOptions()
	opts.AttemptsPerFile = 2

	res, err := Run(context.Background(), entries, p, opts)
	if err != nil {
		t.Fatalf("Run must not fail on broken files: %v", err)
	}
	if res.FilesDone != 27 || res.FilesFailed != 3 {
		t.Fatalf("done=%d failed=%d, want 27/3", res.FilesDone, res.FilesFailed)
	}
	if len(res.FailedFiles) != 3 {
		t.Fatalf("FailedFiles = %d, want 3", len(res.FailedFiles))
	}
	for _, f := range res.FailedFiles {
		if f.Attempts != 2 {
			t.Fatalf("%s attempts = %d, want 2", f.URL, f.Attempts)
		}
	}
	if got := p.calls[entries[3].URL]; got != 2 {
		t.Fatalf("attempts for %s = %d, want 2", entries[3].URL, got)
	}
}

func TestRunAbortsOnConsecutiveFailures(t *testing.T) {
	t.Parallel()

	entries := fakeEntries(50)
	p := newFakeProcessor()
	for _, e := range entries {
		p.failURLs[e.URL] = errors.New("access denied")
	}
	opts := quietOptions()
	opts.Workers = 1
	opts.AttemptsPerFile = 1
	opts.FatalFailureStreak = 5

	res, err := Run(context.Background(), entries, p, opts)
	if err == nil {
		t.Fatal("expected a fatal error after consecutive failures")
	}
	if res.FilesFailed != 5 {
		t.Fatalf("failed = %d, want 5 (abort right after the streak)", res.FilesFailed)
	}
	if p.callCount() > len(entries) {
		t.Fatalf("call count = %d", p.callCount())
	}
}

func TestRunStopsGracefullyOnCancel(t *testing.T) {
	t.Parallel()

	entries := fakeEntries(20)
	p := newFakeProcessor()
	p.blockTillCancel = true

	ctx, cancel := context.WithCancel(context.Background())
	ckPath := filepath.Join(t.TempDir(), "checkpoint.jsonl")

	done := make(chan struct{})
	var res Result
	var err error
	go func() {
		defer close(done)
		opts := quietOptions()
		opts.CheckpointPath = ckPath
		res, err = Run(ctx, entries, p, opts)
	}()

	select {
	case <-p.started:
	case <-time.After(5 * time.Second):
		t.Fatal("no worker started")
	}
	cancel()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
	if err != nil {
		t.Fatalf("graceful stop must not be an error: %v", err)
	}
	if !res.Stopped {
		t.Fatal("result must be marked Stopped")
	}
	if res.FilesDone != 0 {
		t.Fatalf("done = %d, want 0 (files were interrupted mid-flight)", res.FilesDone)
	}
	done_, err := LoadDone(ckPath)
	if err != nil {
		t.Fatalf("LoadDone: %v", err)
	}
	if len(done_) != 0 {
		t.Fatalf("checkpoint must be empty, got %d entries", len(done_))
	}
}

func TestRunDoesNotCheckpointTruncatedFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	ckPath := filepath.Join(dir, "checkpoint.jsonl")

	p := newFakeProcessor()
	p.rows = 3
	p.truncated = true

	opts := quietOptions()
	opts.Workers = 1
	opts.AttemptsPerFile = 1
	opts.CheckpointPath = ckPath

	res, err := Run(context.Background(), fakeEntries(2), p, opts)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.FilesDone != 2 {
		t.Fatalf("done = %d, want 2 (truncated files still count as processed)", res.FilesDone)
	}
	done, err := LoadDone(ckPath)
	if err != nil {
		t.Fatalf("LoadDone: %v", err)
	}
	if len(done) != 0 {
		t.Fatalf("a file truncated by max_records must not be checkpointed, got %v", done)
	}
}

func TestRunCheckpointResumeSkipsDoneFiles(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	ckPath := filepath.Join(dir, "checkpoint.jsonl")
	entries := fakeEntries(10)

	p1 := newFakeProcessor()
	opts := quietOptions()
	opts.Workers = 1
	opts.CheckpointPath = ckPath
	if _, err := Run(context.Background(), entries[:6], p1, opts); err != nil {
		t.Fatalf("first run: %v", err)
	}

	p2 := newFakeProcessor()
	res, err := Run(context.Background(), entries, p2, opts)
	if err != nil {
		t.Fatalf("second run: %v", err)
	}
	if res.FilesSkipped != 6 {
		t.Fatalf("skipped = %d, want 6", res.FilesSkipped)
	}
	if res.FilesDone != 4 {
		t.Fatalf("done = %d, want 4", res.FilesDone)
	}
	if got := len(p2.uniqueVisited()); got != 4 {
		t.Fatalf("second run processed %d files, want 4", got)
	}
}

func TestRunMaxFilesAndOrder(t *testing.T) {
	t.Parallel()

	entries := fakeEntries(10)
	p := newFakeProcessor()
	opts := quietOptions()
	opts.Workers = 1
	opts.MaxFiles = 3
	opts.Order = OrderLargestFirst

	res, err := Run(context.Background(), entries, p, opts)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.FilesTotal != 3 || res.FilesDone != 3 {
		t.Fatalf("total/done = %d/%d, want 3/3", res.FilesTotal, res.FilesDone)
	}
	for i, want := range []string{entries[0].URL, entries[1].URL, entries[2].URL} {
		if p.seen[i] != want {
			t.Fatalf("order[%d] = %s, want %s", i, p.seen[i], want)
		}
	}
}

func TestSortEntriesModes(t *testing.T) {
	t.Parallel()

	entries := []Entry{fakeEntry(0, 5), fakeEntry(1, 1), fakeEntry(2, 9), fakeEntry(3, 3)}

	manifest := SortEntries(entries, OrderManifest)
	if manifest[0].URL != entries[0].URL {
		t.Fatal("manifest order must be preserved")
	}
	asc := SortEntries(entries, OrderSmallFirst)
	for i := 1; i < len(asc); i++ {
		if asc[i-1].Meta.ContentLength > asc[i].Meta.ContentLength {
			t.Fatalf("small-first is not ascending: %v", asc)
		}
	}
	desc := SortEntries(entries, OrderLargestFirst)
	for i := 1; i < len(desc); i++ {
		if desc[i-1].Meta.ContentLength < desc[i].Meta.ContentLength {
			t.Fatalf("largest-first is not descending: %v", desc)
		}
	}
	if entries[0].Meta.ContentLength != 5 {
		t.Fatal("SortEntries mutated the input slice")
	}
}

func TestCheckpointTolerantToTornLine(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "ck.jsonl")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := f.WriteString(`{"url":"s3://a/1.parquet","at":"2026-01-01T00:00:00Z"}` + "\n" + `{"url":"s3://a/2.par`); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = f.Close()

	done, err := LoadDone(path)
	if err != nil {
		t.Fatalf("LoadDone: %v", err)
	}
	if len(done) != 1 {
		t.Fatalf("done = %d, want 1 (torn line ignored)", len(done))
	}
	if _, ok := done["s3://a/1.parquet"]; !ok {
		t.Fatal("valid line was not loaded")
	}

}

func TestRunNilProcessor(t *testing.T) {
	t.Parallel()
	if _, err := Run(context.Background(), fakeEntries(1), nil, quietOptions()); err == nil {
		t.Fatal("expected an error for a nil processor")
	}
}

func TestRunEmptyInput(t *testing.T) {
	t.Parallel()
	res, err := Run(context.Background(), nil, newFakeProcessor(), quietOptions())
	if err != nil || res.FilesTotal != 0 {
		t.Fatalf("empty run = (%+v, %v)", res, err)
	}
}

func TestFilterByPartitionDate(t *testing.T) {
	t.Parallel()

	mk := func(day string) Entry {
		return Entry{URL: "s3://openalex/data/parquet/works/updated_date=" + day + "/part_0000.parquet"}
	}
	entries := []Entry{mk("2026-01-01"), mk("2026-10-01"), mk("2026-11-01")}
	entries = append(entries, Entry{URL: "s3://openalex/data/parquet/works/part_0000.parquet"}) // no partition

	kept, err := FilterByPartitionDate(entries, "2026-10-01")
	if err != nil {
		t.Fatalf("FilterByPartitionDate: %v", err)
	}
	if len(kept) != 3 { // 2026-10-01, 2026-11-01 and the unpartitioned file (fail open)
		t.Fatalf("kept = %d, want 3: %v", len(kept), kept)
	}

	if all, err := FilterByPartitionDate(entries, ""); err != nil || len(all) != 4 {
		t.Fatalf("empty from: kept=%d err=%v", len(all), err)
	}
	if _, err := FilterByPartitionDate(entries, "01.10.2026"); err == nil {
		t.Fatal("expected an error for a malformed date")
	}
}

// TestRunMaxRecordsIsJobLevel checks that max_records caps the whole job, not each
// file: feeding stops once the budget is spent (found in the live HTTP smoke test,
// where max_records=7 produced 14 written rows from 2 files).
func TestRunMaxRecordsIsJobLevel(t *testing.T) {
	t.Parallel()

	entries := fakeEntries(10)
	p := newFakeProcessor()
	opts := quietOptions()
	opts.Workers = 1
	opts.MaxRecords = 7

	res, err := Run(context.Background(), entries, p, opts)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !res.Truncated {
		t.Fatal("result must be marked Truncated")
	}
	if res.Written > uint64(opts.MaxRecords) {
		t.Fatalf("written = %d, must not exceed the budget %d", res.Written, opts.MaxRecords)
	}
	if res.FilesDone >= 10 {
		t.Fatalf("files done = %d, the feed must stop before the end", res.FilesDone)
	}
	if p.callCount() >= 10 {
		t.Fatalf("processor called %d times, want fewer than 10", p.callCount())
	}
}
