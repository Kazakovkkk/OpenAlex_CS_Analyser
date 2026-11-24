package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"go-etl/internal/metrics"
)

const (
	DefaultRangeBufferSize = 2 << 20

	DefaultFooterCacheSize = 1 << 20

	DefaultRangeMaxRetries = 3

	rangeRetryBaseBackoff = 150 * time.Millisecond
)

type RangeFetcher interface {
	FetchRangeInto(ctx context.Context, start, end int64, dst []byte) (int, error)
}

type RangeOptions struct {
	BufferSize      int64
	FooterCacheSize int64
	MaxRetries      int
	Metrics         *metrics.JobMetrics
}

func DefaultRangeOptions() RangeOptions {
	return RangeOptions{
		BufferSize:      DefaultRangeBufferSize,
		FooterCacheSize: DefaultFooterCacheSize,
		MaxRetries:      DefaultRangeMaxRetries,
	}
}

type RangeStats struct {
	BytesRead    uint64
	HTTPRequests uint64
	CacheHits    uint64
}

type RangeReaderAt struct {
	ctx      context.Context
	fetcher  RangeFetcher
	fileSize int64

	bufferSize      int64
	footerCacheSize int64
	jobMetrics      *metrics.JobMetrics

	mu           sync.Mutex
	footerLoaded bool
	footerStart  int64
	footerBuf    []byte

	buf      []byte
	bufStart int64
	bufLen   int64

	bytesRead    atomic.Uint64
	httpRequests atomic.Uint64
	cacheHits    atomic.Uint64
}

var _ io.ReaderAt = (*RangeReaderAt)(nil)

func NewS3ReaderAt(ctx context.Context, client *Client, bucket, key string, fileSize int64, opts RangeOptions) *RangeReaderAt {
	if bucket == "" {
		bucket = client.Bucket()
	}
	retries := opts.MaxRetries
	if retries < 1 {
		retries = 1
	}
	fetcher := &s3RangeFetcher{
		api:        client.API(),
		bucket:     bucket,
		key:        key,
		maxRetries: retries,
	}
	return NewRangeReaderAt(ctx, fetcher, fileSize, opts)
}

func NewRangeReaderAt(ctx context.Context, fetcher RangeFetcher, fileSize int64, opts RangeOptions) *RangeReaderAt {
	if fileSize < 0 {
		fileSize = 0
	}
	bufSize := max(int64(0), opts.BufferSize)
	footerSize := max(int64(0), opts.FooterCacheSize)
	return &RangeReaderAt{
		ctx:             ctx,
		fetcher:         fetcher,
		fileSize:        fileSize,
		bufferSize:      bufSize,
		footerCacheSize: footerSize,
		jobMetrics:      opts.Metrics,
	}
}

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

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.footerCacheSize > 0 {
		footerMin := max(int64(0), r.fileSize-r.footerCacheSize)
		if off >= footerMin {
			wasLoaded := r.footerLoaded
			if !r.footerLoaded {
				if err := r.loadFooterCache(footerMin); err != nil {
					return 0, err
				}
			}
			rel := off - r.footerStart
			n := copy(target, r.footerBuf[rel:rel+int64(want)])
			if wasLoaded {
				r.cacheHits.Add(1)
			}
			if eofAtEnd {
				return n, io.EOF
			}
			return n, nil
		}
	}

	if r.bufferSize <= 0 || int64(want) >= r.bufferSize {
		n, err := r.fetchInto(off, target)
		if err != nil {
			return n, err
		}
		if eofAtEnd {
			return n, io.EOF
		}
		return n, nil
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
			return copied, err
		}
		take := min(want-copied, int(r.bufLen))
		copy(target[copied:copied+take], r.buf[:take])
		copied += take
		curOff += int64(take)
	}

	if eofAtEnd {
		return copied, io.EOF
	}
	return copied, nil
}

func (r *RangeReaderAt) Stats() RangeStats {
	return RangeStats{
		BytesRead:    r.bytesRead.Load(),
		HTTPRequests: r.httpRequests.Load(),
		CacheHits:    r.cacheHits.Load(),
	}
}

func (r *RangeReaderAt) loadFooterCache(footerMin int64) error {
	size := int(r.fileSize - footerMin)
	buf := make([]byte, size)
	n, err := r.fetchInto(footerMin, buf)
	if err != nil {
		return err
	}
	r.footerBuf = buf[:n]
	r.footerStart = footerMin
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
	api        *awss3.Client
	bucket     string
	key        string
	maxRetries int
}

func (f *s3RangeFetcher) FetchRangeInto(ctx context.Context, start, end int64, dst []byte) (int, error) {
	var lastErr error
	for attempt := range f.maxRetries {
		n, err := f.fetchOnce(ctx, start, end, dst)
		if err == nil {
			return n, nil
		}
		lastErr = err
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		if attempt+1 < f.maxRetries {
			backoff := rangeRetryBaseBackoff << attempt
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return 0, ctx.Err()
			case <-timer.C:
			}
		}
	}
	return 0, fmt.Errorf("s3 range s3://%s/%s [%d-%d] failed after %d attempts: %w",
		f.bucket, f.key, start, end, f.maxRetries, lastErr)
}

func (f *s3RangeFetcher) fetchOnce(ctx context.Context, start, end int64, dst []byte) (int, error) {
	rangeHeader := fmt.Sprintf("bytes=%d-%d", start, end)
	out, err := f.api.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(f.bucket),
		Key:    aws.String(f.key),
		Range:  aws.String(rangeHeader),
	})
	if err != nil {
		return 0, err
	}
	defer out.Body.Close()

	n, err := io.ReadFull(out.Body, dst)
	if err != nil {
		return n, err
	}
	return n, nil
}
