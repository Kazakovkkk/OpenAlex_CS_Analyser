package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	"go-etl/internal/metrics"
)

const (
	DefaultBaseURL = "https://api.openalex.org"
	DefaultRPS     = 10.0
	// MaxRPS is the contract ceiling for the whole service (spec §6.8).
	MaxRPS          = 30.0
	DefaultBurst    = 2
	DefaultAttempts = 5

	DefaultAttemptTimeout = 30 * time.Second
	defaultPageSize       = 100
	maxPageSize           = 200

	retryBaseBackoff = 500 * time.Millisecond
	retryMaxBackoff  = 30 * time.Second
)

var ErrStopIteration = errors.New("api: stop iteration")

var ErrPlanRequired = errors.New("api: this query requires a paid OpenAlex plan (daily sync / Member+); " +
	"on the free tier use date_filter_mode=publication (from_publication_date)")

type Options struct {
	BaseURL        string
	APIKey         string
	Email          string
	RPSLimit       float64
	Burst          int
	MaxAttempts    int
	AttemptTimeout time.Duration
	HTTPClient     *http.Client
	Metrics        *metrics.JobMetrics
}

func (o Options) withDefaults() Options {
	if o.BaseURL == "" {
		o.BaseURL = DefaultBaseURL
	}
	o.BaseURL = strings.TrimRight(o.BaseURL, "/")
	if o.RPSLimit <= 0 {
		o.RPSLimit = DefaultRPS
	}
	// The MVP contract caps the request rate at 30 rps regardless of what a
	// caller asks for: exceeding it gets the whole team rate-limited.
	if o.RPSLimit > MaxRPS {
		o.RPSLimit = MaxRPS
	}
	if o.Burst <= 0 {
		o.Burst = DefaultBurst
	}
	if o.MaxAttempts <= 0 {
		o.MaxAttempts = DefaultAttempts
	}
	if o.AttemptTimeout == 0 {
		o.AttemptTimeout = DefaultAttemptTimeout
	}
	if o.HTTPClient == nil {
		o.HTTPClient = &http.Client{Timeout: 60 * time.Second}
	}
	return o
}

type Stats struct {
	Pages    int
	Records  int
	Requests int
	Retries  int
	Waited   time.Duration
	Elapsed  time.Duration
	CostUSD  float64
	Bytes    uint64
}

type Client struct {
	opts    Options
	limiter *rate.Limiter

	// keyMu guards opts.APIKey: a key can be set by a job while other workers
	// read it for their requests.
	keyMu sync.RWMutex

	requests  atomic.Uint64
	retries   atomic.Uint64
	bytesRead atomic.Uint64
}

func NewClient(opts Options) *Client {
	opts = opts.withDefaults()
	return &Client{
		opts:    opts,
		limiter: rate.NewLimiter(rate.Limit(opts.RPSLimit), opts.Burst),
	}
}

func (c *Client) BaseURL() string { return c.opts.BaseURL }

func (c *Client) Requests() uint64 { return c.requests.Load() }

func (c *Client) Retries() uint64 { return c.retries.Load() }

func (c *Client) BytesRead() uint64 { return c.bytesRead.Load() }

func (c *Client) rateLimitWait(ctx context.Context) error {
	if c.limiter == nil {
		return nil
	}
	start := time.Now()
	if err := c.limiter.Wait(ctx); err != nil {
		return err
	}
	if waited := time.Since(start); waited > time.Millisecond {
		if c.opts.Metrics != nil {
			c.opts.Metrics.IncRateLimitWaits()
		}
	}
	return nil
}

func (c *Client) request(ctx context.Context, method, rawURL string, body []byte, contentType string) ([]byte, error) {
	return c.requestWithAttempts(ctx, method, rawURL, body, contentType, c.opts.MaxAttempts)
}

func (c *Client) requestWithAttempts(ctx context.Context, method, rawURL string, body []byte, contentType string, maxAttempts int) ([]byte, error) {
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err := c.rateLimitWait(ctx); err != nil {
			return nil, err
		}

		attemptCtx := ctx
		cancel := context.CancelFunc(func() {})
		if c.opts.AttemptTimeout > 0 {
			attemptCtx, cancel = context.WithTimeout(ctx, c.opts.AttemptTimeout)
		}

		var reader io.Reader
		if body != nil {
			reader = bytes.NewReader(body)
		}
		req, err := http.NewRequestWithContext(attemptCtx, method, rawURL, reader)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("api: build request: %w", err)
		}
		if contentType != "" {
			req.Header.Set("Content-Type", contentType)
		}
		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", "go-etl/1.0 (+https://github.com/openalex)")

		c.requests.Add(1)
		resp, err := c.opts.HTTPClient.Do(req)
		if err != nil {
			cancel()
			lastErr = err
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if attempt+1 >= maxAttempts {
				break
			}
			c.countRetry()
			if serr := sleepCtx(ctx, backoff(attempt, 0)); serr != nil {
				return nil, serr
			}
			continue
		}

		payload, readErr := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
		retryAfter := parseRetryAfter(resp.Header.Get("Retry-After"))
		resp.Body.Close()
		cancel()

		if readErr != nil {
			lastErr = fmt.Errorf("api: read body: %w", readErr)
			if attempt+1 >= maxAttempts {
				break
			}
			c.countRetry()
			if serr := sleepCtx(ctx, backoff(attempt, 0)); serr != nil {
				return nil, serr
			}
			continue
		}

		switch {
		case resp.StatusCode >= 200 && resp.StatusCode < 300:
			c.bytesRead.Add(uint64(len(payload)))
			return payload, nil
		case resp.StatusCode == http.StatusTooManyRequests:
			if c.opts.Metrics != nil {
				c.opts.Metrics.IncAPI429()
			}
			lastErr = fmt.Errorf("api: 429 too many requests: %s", trimmed(payload))
		case resp.StatusCode >= 500:
			if c.opts.Metrics != nil {
				c.opts.Metrics.IncAPI5xx()
			}
			lastErr = fmt.Errorf("api: %d %s: %s", resp.StatusCode, http.StatusText(resp.StatusCode), trimmed(payload))
		case resp.StatusCode == http.StatusForbidden && strings.Contains(string(payload), "Plan upgrade required"):
			return nil, fmt.Errorf("%w (server said: %s)", ErrPlanRequired, trimmed(payload))
		case resp.StatusCode == http.StatusUnauthorized ||
			(resp.StatusCode == http.StatusForbidden && strings.Contains(string(payload), "API key required")):
			// The real API answers 401 {"error":"API key required for cursor paging"}
			// when cursor paging goes past the first page without a key (verified
			// against api.openalex.org in 2026-11).
			return nil, fmt.Errorf("%w (server said: %s)", ErrAPIKeyRequired, trimmed(payload))
		default:
			return nil, fmt.Errorf("api: %d %s: %s", resp.StatusCode, http.StatusText(resp.StatusCode), trimmed(payload))
		}

		if attempt+1 >= maxAttempts {
			break
		}
		c.countRetry()
		if serr := sleepCtx(ctx, backoff(attempt, retryAfter)); serr != nil {
			return nil, serr
		}
	}
	return nil, fmt.Errorf("api: %s %s failed after %d attempt(s): %w", method, rawURL, maxAttempts, lastErr)
}

func (c *Client) countRetry() {
	c.retries.Add(1)
	if c.opts.Metrics != nil {
		c.opts.Metrics.IncAPIRetries()
	}
}

func (c *Client) getJSON(ctx context.Context, path string, params url.Values, out any) error {
	if params == nil {
		params = url.Values{}
	}
	if c.apiKey() != "" {
		params.Set("api_key", c.apiKey())
	}
	if c.opts.Email != "" {
		params.Set("mailto", c.opts.Email)
	}
	rawURL := c.opts.BaseURL + path
	if encoded := params.Encode(); encoded != "" {
		rawURL += "?" + encoded
	}
	payload, err := c.request(ctx, http.MethodGet, rawURL, nil, "")
	if err != nil {
		return err
	}
	if err := json.Unmarshal(payload, out); err != nil {
		return fmt.Errorf("api: decode %s: %w", path, err)
	}
	return nil
}

func backoff(attempt int, retryAfter time.Duration) time.Duration {
	if retryAfter > 0 {
		return min(retryAfter, retryMaxBackoff)
	}
	d := retryBaseBackoff << min(attempt, 6)
	d = min(d, retryMaxBackoff)
	return d + rand.N(d/2+1)
}

func parseRetryAfter(raw string) time.Duration {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0
	}
	if secs, err := strconv.ParseFloat(raw, 64); err == nil && secs >= 0 {
		return time.Duration(secs * float64(time.Second))
	}
	if when, err := http.ParseTime(raw); err == nil {
		if d := time.Until(when); d > 0 {
			return d
		}
	}
	return 0
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func trimmed(b []byte) string {
	s := strings.TrimSpace(string(b))
	if len(s) > 200 {
		return s[:200] + "…"
	}
	return s
}
