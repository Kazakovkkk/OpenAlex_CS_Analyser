package s3

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"go-etl/internal/metrics"
)

const (
	DefaultRangeBufferSize = 2 << 20

	DefaultFooterCacheSize = 1 << 20

	DefaultFooterProbeSize = 128 << 10
	maxFooterBytes         = 32 << 20

	smallExactReadLimit = 8 << 10

	DefaultRangeMaxRetries = 3

	DefaultRangeAttemptTimeout = 25 * time.Second

	rangeRetryBaseBackoff = 150 * time.Millisecond
	rangeRetryMaxBackoff  = 4 * time.Second
	maxReadWaitIterations = 128
)

type RangeFetcher interface {
	FetchRangeInto(ctx context.Context, start, end int64, dst []byte) (int, error)
}

type RangeOptions struct {
	BufferSize      int64
	FooterCacheSize int64
	FooterProbeSize int64
	MaxRetries      int
	AttemptTimeout  time.Duration
	PrefetchBudget  int64
	PrefetchWorkers int

	Metrics *metrics.JobMetrics
}

func DefaultRangeOptions() RangeOptions {
	return RangeOptions{
		BufferSize:      DefaultRangeBufferSize,
		FooterCacheSize: DefaultFooterCacheSize,
		FooterProbeSize: DefaultFooterProbeSize,
		MaxRetries:      DefaultRangeMaxRetries,
		AttemptTimeout:  DefaultRangeAttemptTimeout,
		PrefetchBudget:  DefaultPrefetchBudget,
		PrefetchWorkers: DefaultPrefetchWorkers,
	}
}

func (o RangeOptions) withDefaults() RangeOptions {
	if o.MaxRetries < 1 {
		o.MaxRetries = DefaultRangeMaxRetries
	}
	if o.FooterProbeSize <= 0 {
		o.FooterProbeSize = DefaultFooterProbeSize
	}
	if o.AttemptTimeout == 0 {
		o.AttemptTimeout = DefaultRangeAttemptTimeout
	}
	if o.PrefetchBudget <= 0 {
		o.PrefetchBudget = DefaultPrefetchBudget
	}
	if o.PrefetchWorkers <= 0 {
		o.PrefetchWorkers = DefaultPrefetchWorkers
	}
	return o
}

type RangeStats struct {
	BytesRead         uint64
	HTTPRequests      uint64
	CacheHits         uint64
	Retries           uint64
	PrefetchBytes     uint64
	PrefetchPlanBytes int64
	PrefetchBursts    uint64
	PrefetchGroups    uint64
	PlanMisses        uint64
	HasPlan           bool
	Windowed          bool
	MaxGroupBytes     int64
	PlanError         string
}

type RangeReaderAt struct {
	ctx      context.Context
	fetcher  RangeFetcher
	fileSize int64

	bufferSize      int64
	footerCacheSize int64
	footerProbeSize int64
	prefetchBudget  int64
	prefetchWorkers int
	jobMetrics      *metrics.JobMetrics

	mu           sync.Mutex
	footerLoaded bool
	footerStart  int64
	footerBuf    []byte

	buf         []byte
	bufStart    int64
	bufLen      int64
	plan        *PrefetchPlan
	activeGroup int
	windows     []spanWindow
	wholeGroup  bool
	winSize     int64

	bytesRead     atomic.Uint64
	httpRequests  atomic.Uint64
	cacheHits     atomic.Uint64
	retries       atomic.Uint64
	prefetchBytes atomic.Uint64
	prefetchBurst atomic.Uint64
	groupsLoaded  atomic.Uint64
	planMisses    atomic.Uint64
}

var _ io.ReaderAt = (*RangeReaderAt)(nil)

func NewS3ReaderAt(ctx context.Context, client *Client, bucket, key string, fileSize int64, opts RangeOptions) *RangeReaderAt {
	if bucket == "" {
		bucket = client.Bucket()
	}
	opts = opts.withDefaults()
	r := newRangeReaderAt(ctx, nil, fileSize, opts)
	r.fetcher = &s3RangeFetcher{
		api:            client.API(),
		bucket:         bucket,
		key:            key,
		maxAttempts:    opts.MaxRetries,
		attemptTimeout: opts.AttemptTimeout,
		retries:        &r.retries,
	}
	return r
}

func NewRangeReaderAt(ctx context.Context, fetcher RangeFetcher, fileSize int64, opts RangeOptions) *RangeReaderAt {
	return newRangeReaderAt(ctx, fetcher, fileSize, opts.withDefaults())
}

func newRangeReaderAt(ctx context.Context, fetcher RangeFetcher, fileSize int64, opts RangeOptions) *RangeReaderAt {
	if fileSize < 0 {
		fileSize = 0
	}
	probe := opts.FooterProbeSize
	if probe <= 0 {
		probe = DefaultFooterProbeSize
	}
	return &RangeReaderAt{
		ctx:             ctx,
		fetcher:         fetcher,
		fileSize:        fileSize,
		bufferSize:      max(int64(0), opts.BufferSize),
		footerCacheSize: max(int64(0), opts.FooterCacheSize),
		footerProbeSize: probe,
		prefetchBudget:  max(int64(1), opts.PrefetchBudget),
		prefetchWorkers: max(1, opts.PrefetchWorkers),
		activeGroup:     -1,
		jobMetrics:      opts.Metrics,
	}
}

type readOutcome struct {
	n    int
	err  error
	done bool
	wait chan struct{}
}

func readDone(n int, err error) readOutcome { return readOutcome{n: n, err: err, done: true} }

func readWait(ch chan struct{}) readOutcome { return readOutcome{wait: ch} }

func (r *RangeReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, errors.New("s3ReaderAt: negative offset")
	}
	if len(p) == 0 {
		return 0, nil
	}
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	if off >= r.fileSize {
		return 0, io.EOF
	}

	eofAtEnd := false
	want := len(p)
	if remaining := r.fileSize - off; int64(want) > remaining {
		want = int(remaining)
		eofAtEnd = true
	}
	target := p[:want]

	for iter := 0; ; iter++ {
		if iter >= maxReadWaitIterations {
			return 0, fmt.Errorf("s3ReaderAt: no progress reading %d bytes at offset %d after %d attempts",
				len(target), off, iter)
		}

		r.mu.Lock()
		out := r.readLocked(target, off, eofAtEnd)
		r.mu.Unlock()
		if out.done {
			return out.n, out.err
		}
		select {
		case <-out.wait:
		case <-r.ctx.Done():
			return 0, r.ctx.Err()
		}
	}
}

func (r *RangeReaderAt) readLocked(target []byte, off int64, eofAtEnd bool) readOutcome {
	want := len(target)

	if r.plan != nil {
		r.sweepPendingLocked()
	}

	if r.footerCacheSize > 0 {
		footerMin := max(int64(0), r.fileSize-r.footerCacheSize)
		if off >= footerMin {
			wasLoaded := r.footerLoaded
			if !r.footerLoaded {
				if err := r.loadFooter(); err != nil {
					return readDone(0, err)
				}
			}
			if rel := off - r.footerStart; rel >= 0 && rel+int64(want) <= int64(len(r.footerBuf)) {
				n := copy(target, r.footerBuf[rel:rel+int64(want)])
				if wasLoaded {
					r.cacheHits.Add(1)
				}
				if eofAtEnd {
					return readDone(n, io.EOF)
				}
				return readDone(n, nil)
			}
		}
	}

	if r.plan != nil {
		if out, ok := r.readPlanLocked(target, off, eofAtEnd); ok {
			return out
		}
		r.planMisses.Add(1)
		n, err := r.fetchInto(off, target)
		if err != nil {
			return readDone(n, err)
		}
		if eofAtEnd {
			return readDone(n, io.EOF)
		}
		return readDone(n, nil)
	}

	if int64(want) <= smallExactReadLimit {
		n, err := r.fetchInto(off, target)
		if err != nil {
			return readDone(n, err)
		}
		if eofAtEnd {
			return readDone(n, io.EOF)
		}
		return readDone(n, nil)
	}

	if r.bufferSize <= 0 || int64(want) >= r.bufferSize {
		n, err := r.fetchInto(off, target)
		if err != nil {
			return readDone(n, err)
		}
		if eofAtEnd {
			return readDone(n, io.EOF)
		}
		return readDone(n, nil)
	}

	copied := 0
	curOff := off
	for copied < want {
		if r.bufLen > 0 && curOff >= r.bufStart && curOff < r.bufStart+r.bufLen {
			rel := curOff - r.bufStart
			take := min(want-copied, int(r.bufLen-rel))
			copy(target[copied:copied+take], r.buf[rel:rel+int64(take)])
			copied += take
			curOff += int64(take)
			r.cacheHits.Add(1)
			continue
		}

		if err := r.refillBuffer(curOff); err != nil {
			return readDone(copied, err)
		}
		take := min(want-copied, int(r.bufLen))
		copy(target[copied:copied+take], r.buf[:take])
		copied += take
		curOff += int64(take)
	}

	if eofAtEnd {
		return readDone(copied, io.EOF)
	}
	return readDone(copied, nil)
}

func (r *RangeReaderAt) Stats() RangeStats {
	st := RangeStats{
		BytesRead:      r.bytesRead.Load(),
		HTTPRequests:   r.httpRequests.Load(),
		CacheHits:      r.cacheHits.Load(),
		Retries:        r.retries.Load(),
		PrefetchBytes:  r.prefetchBytes.Load(),
		PrefetchBursts: r.prefetchBurst.Load(),
		PrefetchGroups: r.groupsLoaded.Load(),
		PlanMisses:     r.planMisses.Load(),
	}
	r.mu.Lock()
	if r.plan != nil {
		st.HasPlan = true
		st.PrefetchPlanBytes = r.plan.TotalBytes
		st.MaxGroupBytes = r.plan.MaxGroup
		st.Windowed = !r.wholeGroup && r.activeGroup >= 0
	}
	r.mu.Unlock()
	return st
}

func (r *RangeReaderAt) loadFooter() error {
	probeLen := min(r.footerProbeSize, r.fileSize)
	if probeLen < 8 {
		return fmt.Errorf("s3ReaderAt: file of %d bytes is too small for a parquet footer", r.fileSize)
	}
	probeStart := r.fileSize - probeLen
	probe := make([]byte, probeLen)
	n, err := r.fetchInto(probeStart, probe)
	if err != nil {
		return fmt.Errorf("read parquet footer probe [%d-%d): %w", probeStart, probeStart+probeLen, err)
	}
	probe = probe[:n]
	if len(probe) < 8 {
		return fmt.Errorf("s3ReaderAt: short footer probe (%d bytes)", len(probe))
	}
	footerSize := int64(binary.LittleEndian.Uint32(probe[len(probe)-8:]))
	total := footerSize + 8
	if total <= 8 || total > r.fileSize || total > maxFooterBytes {
		r.footerBuf = probe
		r.footerStart = probeStart
		r.footerLoaded = true
		return nil
	}

	if total <= int64(len(probe)) {
		start := int64(len(probe)) - total
		r.footerBuf = probe[start:]
		r.footerStart = probeStart + start
		r.footerLoaded = true
		return nil
	}

	full := make([]byte, total)
	n, err = r.fetchInto(r.fileSize-total, full)
	if err != nil {
		return fmt.Errorf("read parquet footer [%d-%d): %w", r.fileSize-total, r.fileSize, err)
	}
	r.footerBuf = full[:n]
	r.footerStart = r.fileSize - total
	r.footerLoaded = true
	return nil
}

func (r *RangeReaderAt) refillBuffer(start int64) error {
	chunkLen := int(min(r.bufferSize, r.fileSize-start))
	if cap(r.buf) < chunkLen {
		r.buf = make([]byte, chunkLen)
	}
	dst := r.buf[:chunkLen]
	n, err := r.fetchInto(start, dst)
	if err != nil {
		r.bufLen = 0
		return err
	}
	r.buf = dst
	r.bufStart = start
	r.bufLen = int64(n)
	return nil
}

func (r *RangeReaderAt) fetchInto(start int64, dst []byte) (int, error) {
	end := start + int64(len(dst)) - 1
	r.httpRequests.Add(1)
	if r.jobMetrics != nil {
		r.jobMetrics.IncHTTPRequests()
	}

	n, err := r.fetcher.FetchRangeInto(r.ctx, start, end, dst)
	if n > 0 {
		r.bytesRead.Add(uint64(n))
		if r.jobMetrics != nil {
			r.jobMetrics.AddBytesRead(uint64(n))
		}
	}
	return n, err
}

type s3RangeFetcher struct {
	api            *awss3.Client
	bucket         string
	key            string
	maxAttempts    int
	attemptTimeout time.Duration
	retries        *atomic.Uint64
}

func (f *s3RangeFetcher) FetchRangeInto(ctx context.Context, start, end int64, dst []byte) (int, error) {
	attempts := max(f.maxAttempts, 1)
	var lastErr error
	for attempt := range attempts {
		attemptCtx := ctx
		cancel := context.CancelFunc(func() {})
		if f.attemptTimeout > 0 {
			attemptCtx, cancel = context.WithTimeout(ctx, f.attemptTimeout)
		}
		n, err := f.fetchOnce(attemptCtx, start, end, dst)
		cancel()
		if err == nil {
			return n, nil
		}
		lastErr = err
		if parentErr := ctx.Err(); parentErr != nil {
			return 0, parentErr
		}
		retryable, retryAfter := classifyRangeError(err)
		if !retryable || attempt+1 >= attempts {
			break
		}
		if f.retries != nil {
			f.retries.Add(1)
		}
		if err := sleepCtx(ctx, retryBackoff(attempt, retryAfter)); err != nil {
			return 0, err
		}
	}
	return 0, fmt.Errorf("s3 range s3://%s/%s [%d-%d] failed after %d attempt(s): %w",
		f.bucket, f.key, start, end, attempts, lastErr)
}

func (f *s3RangeFetcher) fetchOnce(ctx context.Context, start, end int64, dst []byte) (int, error) {
	rangeHeader := fmt.Sprintf("bytes=%d-%d", start, end)
	out, err := f.api.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(f.bucket),
		Key:    aws.String(f.key),
		Range:  aws.String(rangeHeader),
	}, func(o *awss3.Options) {
		o.Retryer = aws.NopRetryer{}
		o.DisableLogOutputChecksumValidationSkipped = true
	})
	if err != nil {
		return 0, err
	}
	defer out.Body.Close()

	if out.ContentRange != nil {
		if got := *out.ContentRange; got != "" && !rangeMatches(got, start, end) {
			return 0, fmt.Errorf("s3 range mismatch: asked bytes=%d-%d, got %q", start, end, got)
		}
	}

	n, err := io.ReadFull(out.Body, dst)
	if err != nil {
		return n, err
	}
	return n, nil
}

func rangeMatches(contentRange string, start, end int64) bool {
	rest, ok := cutPrefixFold(contentRange, "bytes ")
	if !ok {
		return false
	}
	dash := indexByte(rest, '-')
	if dash < 0 {
		return false
	}
	gotStart, err := strconv.ParseInt(rest[:dash], 10, 64)
	if err != nil {
		return false
	}
	tail := rest[dash+1:]
	slash := indexByte(tail, '/')
	if slash < 0 {
		return false
	}
	gotEnd, err := strconv.ParseInt(tail[:slash], 10, 64)
	if err != nil {
		return false
	}
	return gotStart == start && gotEnd == end
}

func cutPrefixFold(s, prefix string) (string, bool) {
	if len(s) < len(prefix) {
		return "", false
	}
	head := s[:len(prefix)]
	for i := range len(prefix) {
		c := head[i]
		if 'A' <= c && c <= 'Z' {
			c += 'a' - 'A'
		}
		p := prefix[i]
		if 'A' <= p && p <= 'Z' {
			p += 'a' - 'A'
		}
		if c != p {
			return "", false
		}
	}
	return s[len(prefix):], true
}

func indexByte(s string, b byte) int {
	for i := range len(s) {
		if s[i] == b {
			return i
		}
	}
	return -1
}

func classifyRangeError(err error) (retryable bool, retryAfter time.Duration) {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NoSuchKey", "NoSuchBucket", "AccessDenied", "InvalidRange",
			"InvalidObjectState", "InvalidArgument", "InvalidRequest":
			return false, 0
		}
	}

	var respErr *awshttp.ResponseError
	if errors.As(err, &respErr) {
		status := respErr.HTTPStatusCode()
		switch {
		case status == http.StatusTooManyRequests || status == http.StatusRequestTimeout ||
			status >= 500:
			return true, parseRetryAfter(respErr.HTTPResponse())
		case status >= 400:
			return false, 0
		}
	}

	if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) ||
		errors.Is(err, context.DeadlineExceeded) {
		return true, 0
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true, 0
	}
	return true, 0
}

func parseRetryAfter(resp *smithyhttp.Response) time.Duration {
	if resp == nil || resp.Response == nil || resp.Header == nil {
		return 0
	}
	if raw := resp.Header.Get("Retry-After"); raw != "" {
		if secs, err := strconv.ParseInt(raw, 10, 64); err == nil && secs >= 0 {
			return time.Duration(secs) * time.Second
		}
		if when, err := http.ParseTime(raw); err == nil {
			if d := time.Until(when); d > 0 {
				return d
			}
		}
	}
	return 0
}

func retryBackoff(attempt int, retryAfter time.Duration) time.Duration {
	if retryAfter > 0 {
		return min(retryAfter, rangeRetryMaxBackoff)
	}
	backoff := rangeRetryBaseBackoff << min(attempt, 8)
	backoff = min(backoff, rangeRetryMaxBackoff)
	if backoff <= 0 {
		return rangeRetryBaseBackoff
	}
	return backoff + rand.N(backoff/2+1)
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
