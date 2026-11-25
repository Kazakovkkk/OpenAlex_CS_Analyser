package s3

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/parquet-go/parquet-go"

	"go-etl/internal/metrics"
)

type memRangeReq struct {
	start int64
	end   int64
}

type memRangeFetcher struct {
	data []byte
	mu   sync.Mutex
	reqs []memRangeReq
}

func (m *memRangeFetcher) FetchRangeInto(ctx context.Context, start, end int64, dst []byte) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	m.mu.Lock()
	m.reqs = append(m.reqs, memRangeReq{start: start, end: end})
	m.mu.Unlock()

	if start < 0 || end >= int64(len(m.data)) || end < start {
		return 0, fmt.Errorf("invalid range [%d-%d] for size %d", start, end, len(m.data))
	}
	n := copy(dst, m.data[start:end+1])
	return n, nil
}

func makeDeterministicBytes(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte((i*31 + 7) & 0xff)
	}
	return data
}

func TestRangeReaderAtFooterCacheAndSlidingBuffer(t *testing.T) {
	t.Parallel()

	const (
		fileSize   = 512 << 10 // 512 KiB
		footerSize = 64 << 10  // 64 KiB
		bufferSize = 64 << 10  // 64 KiB
	)
	data := makeDeterministicBytes(fileSize)
	fetcher := &memRangeFetcher{data: data}
	jm := metrics.NewJobMetrics()

	ra := NewRangeReaderAt(context.Background(), fetcher, fileSize, RangeOptions{
		BufferSize:      bufferSize,
		FooterCacheSize: footerSize,
		FooterProbeSize: footerSize,
		Metrics:         jm,
	})

	tail := make([]byte, 8)
	if n, err := ra.ReadAt(tail, fileSize-8); err != nil || n != 8 {
		t.Fatalf("tail ReadAt = (%d, %v), want (8, nil)", n, err)
	}
	if !bytes.Equal(tail, data[fileSize-8:]) {
		t.Fatal("tail bytes mismatch")
	}

	metaBuf := make([]byte, 20<<10)
	if n, err := ra.ReadAt(metaBuf, fileSize-8-(20<<10)); err != nil || n != len(metaBuf) {
		t.Fatalf("meta ReadAt = (%d, %v)", n, err)
	}
	if !bytes.Equal(metaBuf, data[fileSize-8-(20<<10):fileSize-8]) {
		t.Fatal("meta bytes mismatch")
	}

	st := ra.Stats()
	if st.HTTPRequests != 1 || st.BytesRead != footerSize || st.CacheHits != 1 {
		t.Fatalf("after footer reads: stats = %+v, want reqs=1 bytes=%d hits=1", st, footerSize)
	}

	chunk := make([]byte, 16<<10)
	for i := range 4 {
		off := int64(i) * (16 << 10)
		if n, err := ra.ReadAt(chunk, off); err != nil || n != len(chunk) {
			t.Fatalf("seq read %d = (%d, %v)", i, n, err)
		}
		if !bytes.Equal(chunk, data[off:off+int64(len(chunk))]) {
			t.Fatalf("seq read %d data mismatch", i)
		}
	}
	st = ra.Stats()
	if st.HTTPRequests != 2 || st.BytesRead != footerSize+bufferSize || st.CacheHits != 4 {
		t.Fatalf("after 4x16KiB seq reads: stats = %+v, want reqs=2 bytes=%d hits=4", st, footerSize+bufferSize)
	}

	straddleOff := int64(56 << 10)
	if n, err := ra.ReadAt(chunk, straddleOff); err != nil || n != len(chunk) {
		t.Fatalf("straddle ReadAt = (%d, %v)", n, err)
	}
	if !bytes.Equal(chunk, data[straddleOff:straddleOff+int64(len(chunk))]) {
		t.Fatal("straddle bytes mismatch")
	}

	if lastReq := fetcher.reqs[len(fetcher.reqs)-1]; lastReq.start != 64<<10 {
		t.Fatalf("straddle refill started at %d, want %d (0 re-downloaded bytes)", lastReq.start, 64<<10)
	}

	snap := jm.Snapshot()
	st = ra.Stats()
	if snap.BytesRead != st.BytesRead || snap.HTTPRequests != st.HTTPRequests {
		t.Fatalf("JobMetrics (%d B, %d reqs) != RangeStats (%d B, %d reqs)",
			snap.BytesRead, snap.HTTPRequests, st.BytesRead, st.HTTPRequests)
	}
}

func TestRangeReaderAtUnbufferedAndEdgeCases(t *testing.T) {
	t.Parallel()

	const fileSize = 1024
	data := makeDeterministicBytes(fileSize)
	fetcher := &memRangeFetcher{data: data}

	ra := NewRangeReaderAt(context.Background(), fetcher, fileSize, RangeOptions{
		BufferSize:      0,
		FooterCacheSize: 0,
	})

	buf := make([]byte, 100)
	for _, off := range []int64{0, 200, 500} {
		n, err := ra.ReadAt(buf, off)
		if err != nil || n != 100 || !bytes.Equal(buf, data[off:off+100]) {
			t.Fatalf("unbuffered ReadAt(%d) = (%d, %v)", off, n, err)
		}
	}
	if st := ra.Stats(); st.HTTPRequests != 3 || st.BytesRead != 300 || st.CacheHits != 0 {
		t.Fatalf("unbuffered stats = %+v, want reqs=3 bytes=300 hits=0", st)
	}

	if _, err := ra.ReadAt(buf, -1); err == nil {
		t.Fatal("expected error for negative offset")
	}
	if n, err := ra.ReadAt(nil, 0); n != 0 || err != nil {
		t.Fatalf("empty ReadAt = (%d, %v), want (0, nil)", n, err)
	}
	if n, err := ra.ReadAt(buf, fileSize); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("ReadAt(EOF) = (%d, %v), want (0, io.EOF)", n, err)
	}
	tailBuf := make([]byte, 32)
	n, err := ra.ReadAt(tailBuf, fileSize-10)
	if n != 10 || !errors.Is(err, io.EOF) || !bytes.Equal(tailBuf[:10], data[fileSize-10:]) {
		t.Fatalf("partial EOF ReadAt = (%d, %v), want (10, io.EOF)", n, err)
	}

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	raCanceled := NewRangeReaderAt(canceledCtx, fetcher, fileSize, DefaultRangeOptions())
	if _, err := raCanceled.ReadAt(buf, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled ReadAt = %v, want context.Canceled", err)
	}
}

type rewriteHostTransport struct {
	target *url.URL
	base   http.RoundTripper
}

func (rt *rewriteHostTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	cloned := req.Clone(req.Context())
	cloned.URL.Scheme = rt.target.Scheme
	cloned.URL.Host = rt.target.Host
	cloned.Host = rt.target.Host
	return rt.base.RoundTrip(cloned)
}

func TestS3ReaderAtWithHTTPTestParquet(t *testing.T) {
	t.Parallel()

	type miniWork struct {
		ID    string `parquet:"id"`
		Year  int32  `parquet:"year"`
		Title string `parquet:"title"`
	}

	const totalRows = 200
	var parquetBuf bytes.Buffer
	pw := parquet.NewGenericWriter[miniWork](&parquetBuf)
	inRows := make([]miniWork, totalRows)
	for i := range totalRows {
		inRows[i] = miniWork{
			ID:    fmt.Sprintf("https://openalex.org/W%d", 1000+i),
			Year:  int32(2000 + i%26),
			Title: fmt.Sprintf("Paper #%d about buffered S3 range reads", i),
		}
	}
	if _, err := pw.Write(inRows); err != nil {
		t.Fatalf("parquet Write: %v", err)
	}
	if err := pw.Close(); err != nil {
		t.Fatalf("parquet Close: %v", err)
	}
	rawParquet := parquetBuf.Bytes()

	var totalServerReqs atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqNum := totalServerReqs.Add(1)
		rangeHdr := r.Header.Get("Range")
		if !strings.HasPrefix(rangeHdr, "bytes=") {
			http.Error(w, "missing Range header", http.StatusBadRequest)
			return
		}
		parts := strings.Split(strings.TrimPrefix(rangeHdr, "bytes="), "-")
		start, err1 := strconv.ParseInt(parts[0], 10, 64)
		end, err2 := strconv.ParseInt(parts[1], 10, 64)
		if err1 != nil || err2 != nil || start < 0 || end >= int64(len(rawParquet)) || end < start {
			http.Error(w, "invalid Range", http.StatusRequestedRangeNotSatisfiable)
			return
		}

		slice := rawParquet[start : end+1]
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(rawParquet)))
		w.Header().Set("Content-Length", strconv.Itoa(len(slice)))
		w.WriteHeader(http.StatusPartialContent)

		if reqNum == 1 && len(slice) > 4 {
			_, _ = w.Write(slice[:2])
			return
		}
		_, _ = w.Write(slice)
	}))
	defer srv.Close()

	srvURL, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("url.Parse: %v", err)
	}
	hc := &http.Client{
		Transport: &rewriteHostTransport{
			target: srvURL,
			base:   srv.Client().Transport,
		},
	}

	ctx := context.Background()
	client, err := NewAnonymousClient(ctx, WithHTTPClient(hc))
	if err != nil {
		t.Fatalf("NewAnonymousClient: %v", err)
	}

	ra := NewS3ReaderAt(ctx, client, DefaultBucket, "test/mini.parquet", int64(len(rawParquet)), RangeOptions{
		BufferSize:      4 << 10, // 4 KiB chunks to exercise multiple buffer refills
		FooterCacheSize: 2 << 10, // 2 KiB footer cache
		MaxRetries:      3,
	})

	pf, err := parquet.OpenFile(ra, int64(len(rawParquet)))
	if err != nil {
		t.Fatalf("parquet.OpenFile via S3ReaderAt: %v", err)
	}
	gr := parquet.NewGenericReader[miniWork](pf)
	defer gr.Close()

	got := make([]miniWork, totalRows)
	readTotal := 0
	for readTotal < totalRows {
		n, err := gr.Read(got[readTotal:])
		readTotal += n
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("parquet Read via S3ReaderAt: %v", err)
		}
	}
	if readTotal != totalRows {
		t.Fatalf("readTotal = %d, want %d", readTotal, totalRows)
	}
	if got[0] != inRows[0] || got[totalRows-1] != inRows[totalRows-1] {
		t.Fatalf("row mismatch: first=%+v last=%+v", got[0], got[totalRows-1])
	}
	if totalServerReqs.Load() < 2 {
		t.Fatalf("expected retry to fire after simulated drop, totalServerReqs=%d", totalServerReqs.Load())
	}
}

func makeFakeParquetFile(dataSize, footerSize int) []byte {
	out := make([]byte, dataSize+footerSize+8)
	for i := range dataSize {
		out[i] = byte((i*17 + 3) & 0xff)
	}
	for i := range footerSize {
		out[dataSize+i] = byte(0xA5)
	}
	binary.LittleEndian.PutUint32(out[len(out)-8:], uint32(footerSize))
	copy(out[len(out)-4:], "PAR1")
	return out
}

func TestRangeFooterProbe(t *testing.T) {
	t.Parallel()

	t.Run("probe covers footer: one request", func(t *testing.T) {
		const footerSize = 64 << 10
		data := makeFakeParquetFile(300<<10, footerSize)
		fetcher := &memRangeFetcher{data: data}
		ra := NewRangeReaderAt(context.Background(), fetcher, int64(len(data)), RangeOptions{
			FooterCacheSize: 1 << 20,
			FooterProbeSize: 128 << 10,
		})

		off := int64(len(data)) - 8 - 32<<10
		want := 16 << 10
		buf := make([]byte, want)
		n, err := ra.ReadAt(buf, off)
		if err != nil || n != want {
			t.Fatalf("ReadAt = (%d, %v), want (%d, nil)", n, err, want)
		}
		if !bytes.Equal(buf, data[off:off+int64(want)]) {
			t.Fatal("footer bytes mismatch")
		}
		st := ra.Stats()
		if st.HTTPRequests != 1 || st.BytesRead != 128<<10 {
			t.Fatalf("stats = %+v, want 1 request of 128 KiB (probe only)", st)
		}
	})

	t.Run("footer bigger than probe: probe + exact request", func(t *testing.T) {
		const footerSize = 200 << 10
		data := makeFakeParquetFile(300<<10, footerSize)
		fetcher := &memRangeFetcher{data: data}
		ra := NewRangeReaderAt(context.Background(), fetcher, int64(len(data)), RangeOptions{
			FooterCacheSize: 1 << 20,
			FooterProbeSize: 128 << 10,
		})

		off := int64(len(data)) - 8 - 150<<10
		want := 16 << 10
		buf := make([]byte, want)
		if n, err := ra.ReadAt(buf, off); err != nil || n != want {
			t.Fatalf("ReadAt = (%d, %v)", n, err)
		}
		if !bytes.Equal(buf, data[off:off+int64(want)]) {
			t.Fatal("footer bytes mismatch")
		}
		st := ra.Stats()
		wantBytes := uint64(128<<10) + uint64(footerSize+8)
		if st.HTTPRequests != 2 || st.BytesRead != wantBytes {
			t.Fatalf("stats = %+v, want 2 requests / %d bytes", st, wantBytes)
		}
	})

	t.Run("non-parquet data: blind probe fallback, still correct", func(t *testing.T) {
		data := makeDeterministicBytes(256 << 10)
		fetcher := &memRangeFetcher{data: data}
		ra := NewRangeReaderAt(context.Background(), fetcher, int64(len(data)), RangeOptions{
			FooterCacheSize: 1 << 20,
			FooterProbeSize: 64 << 10,
		})
		off := int64(len(data)) - 20<<10
		buf := make([]byte, 16<<10)
		if n, err := ra.ReadAt(buf, off); err != nil || n != len(buf) {
			t.Fatalf("ReadAt = (%d, %v)", n, err)
		}
		if !bytes.Equal(buf, data[off:off+int64(len(buf))]) {
			t.Fatal("fallback bytes mismatch")
		}
	})
}

func TestRangeMatches(t *testing.T) {
	t.Parallel()
	cases := []struct {
		header string
		start  int64
		end    int64
		want   bool
	}{
		{"bytes 0-1023/5000", 0, 1023, true},
		{"Bytes 100-199/5000", 100, 199, true},
		{"bytes 100-198/5000", 100, 199, false},
		{"bytes 0-1023", 0, 1023, false},
		{"bytes */5000", 0, 1023, false},
		{"", 0, 1023, false},
	}
	for _, tc := range cases {
		if got := rangeMatches(tc.header, tc.start, tc.end); got != tc.want {
			t.Errorf("rangeMatches(%q, %d, %d) = %v, want %v", tc.header, tc.start, tc.end, got, tc.want)
		}
	}
}

func TestParseRetryAfter(t *testing.T) {
	t.Parallel()
	mk := func(h http.Header) *smithyhttp.Response {
		return &smithyhttp.Response{Response: &http.Response{Header: h}}
	}
	if d := parseRetryAfter(nil); d != 0 {
		t.Fatalf("nil response = %v, want 0", d)
	}
	if d := parseRetryAfter(mk(http.Header{})); d != 0 {
		t.Fatalf("no header = %v, want 0", d)
	}
	if d := parseRetryAfter(mk(http.Header{"Retry-After": {"3"}})); d != 3*time.Second {
		t.Fatalf("Retry-After: 3 = %v, want 3s", d)
	}
	future := time.Now().Add(5 * time.Second).UTC().Format(http.TimeFormat)
	if d := parseRetryAfter(mk(http.Header{"Retry-After": {future}})); d <= 0 || d > 6*time.Second {
		t.Fatalf("Retry-After date = %v, want (0, 6s]", d)
	}
	if d := parseRetryAfter(mk(http.Header{"Retry-After": {"bogus"}})); d != 0 {
		t.Fatalf("bogus Retry-After = %v, want 0", d)
	}
}

func serveTestRange(w http.ResponseWriter, r *http.Request, data []byte) {
	rangeHdr := r.Header.Get("Range")
	parts := strings.Split(strings.TrimPrefix(rangeHdr, "bytes="), "-")
	start, err1 := strconv.ParseInt(parts[0], 10, 64)
	end, err2 := strconv.ParseInt(parts[1], 10, 64)
	if err1 != nil || err2 != nil || start < 0 || end >= int64(len(data)) || end < start {
		http.Error(w, "invalid Range", http.StatusRequestedRangeNotSatisfiable)
		return
	}
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
	w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
	w.WriteHeader(http.StatusPartialContent)
	_, _ = w.Write(data[start : end+1])
}

func newTestRangeReaderAt(t *testing.T, srv *httptest.Server, fileSize int64, opts RangeOptions) *RangeReaderAt {
	t.Helper()
	srvURL, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("url.Parse: %v", err)
	}
	hc := &http.Client{Transport: &rewriteHostTransport{target: srvURL, base: srv.Client().Transport}}
	client, err := NewAnonymousClient(context.Background(), WithHTTPClient(hc))
	if err != nil {
		t.Fatalf("NewAnonymousClient: %v", err)
	}
	return NewS3ReaderAt(context.Background(), client, DefaultBucket, "test/obj.bin", fileSize, opts)
}

func TestS3RangeFetcherRetriesOn503(t *testing.T) {
	t.Parallel()

	data := makeDeterministicBytes(64 << 10)
	var reqs atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := reqs.Add(1)
		if n <= 2 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`<Error><Code>SlowDown</Code></Error>`))
			return
		}
		serveTestRange(w, r, data)
	}))
	defer srv.Close()

	ra := newTestRangeReaderAt(t, srv, int64(len(data)), RangeOptions{
		BufferSize:      0,
		FooterCacheSize: 0,
		MaxRetries:      3,
		AttemptTimeout:  2 * time.Second,
	})

	buf := make([]byte, 4<<10)
	n, err := ra.ReadAt(buf, 0)
	if err != nil || n != len(buf) {
		t.Fatalf("ReadAt = (%d, %v), want (%d, nil)", n, err, len(buf))
	}
	if !bytes.Equal(buf, data[:len(buf)]) {
		t.Fatal("payload mismatch")
	}
	if got := reqs.Load(); got != 3 {
		t.Fatalf("server requests = %d, want 3 (2x503 + 1x206)", got)
	}
	if st := ra.Stats(); st.Retries != 2 || st.HTTPRequests != 1 {
		t.Fatalf("stats = %+v, want retries=2 httpRequests=1 (logical fetch counted once)", st)
	}
}

func TestS3RangeFetcherNoRetryOn404(t *testing.T) {
	t.Parallel()

	var reqs atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqs.Add(1)
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`<Error><Code>NoSuchKey</Code><Message>missing</Message></Error>`))
	}))
	defer srv.Close()

	ra := newTestRangeReaderAt(t, srv, 4096, RangeOptions{
		BufferSize:      0,
		FooterCacheSize: 0,
		MaxRetries:      3,
		AttemptTimeout:  2 * time.Second,
	})

	buf := make([]byte, 1024)
	if _, err := ra.ReadAt(buf, 0); err == nil {
		t.Fatal("expected error for 404")
	}
	if got := reqs.Load(); got != 1 {
		t.Fatalf("server requests = %d, want 1 (permanent error must not be retried)", got)
	}
}

func TestS3RangeFetcherShortBodyIsRetried(t *testing.T) {
	t.Parallel()

	data := makeDeterministicBytes(32 << 10)
	var reqs atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := reqs.Add(1)
		if n == 1 {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-1023/%d", len(data)))
			w.Header().Set("Content-Length", "1024")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(data[:100]) // truncated body
			return
		}
		serveTestRange(w, r, data)
	}))
	defer srv.Close()

	ra := newTestRangeReaderAt(t, srv, int64(len(data)), RangeOptions{
		BufferSize:      0,
		FooterCacheSize: 0,
		MaxRetries:      3,
		AttemptTimeout:  2 * time.Second,
	})
	buf := make([]byte, 4<<10)
	if _, err := ra.ReadAt(buf, 0); err != nil {
		t.Fatalf("ReadAt after short body: %v", err)
	}
	if got := reqs.Load(); got != 2 {
		t.Fatalf("server requests = %d, want 2", got)
	}
}
