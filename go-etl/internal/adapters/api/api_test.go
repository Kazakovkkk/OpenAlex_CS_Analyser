package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go-etl/internal/metrics"
)

func pageServer(t *testing.T, pages map[string]worksPage, hits *atomic.Int32, mutate func(reqNum int32, w http.ResponseWriter) bool) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := hits.Add(1)
		if mutate != nil && mutate(n, w) {
			return
		}
		cursor := r.URL.Query().Get("cursor")
		page, ok := pages[cursor]
		if !ok {
			http.Error(w, "unknown cursor", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(page)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func rawWork(id int) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(`{"id":"https://openalex.org/W%d"}`, id))
}

func makePage(next string, ids ...int) worksPage {
	var p worksPage
	p.Meta.NextCursor = next
	p.Meta.PerPage = 100
	p.Meta.Count = 5
	for _, id := range ids {
		p.Results = append(p.Results, rawWork(id))
	}
	return p
}

func TestIterateWorksPaginates(t *testing.T) {
	t.Parallel()

	var hits atomic.Int32
	srv := pageServer(t, map[string]worksPage{
		"*":  makePage("c2", 100, 101),
		"c2": makePage("c3", 102, 103),
		"c3": makePage("", 104),
	}, &hits, nil)

	c := NewClient(Options{BaseURL: srv.URL, APIKey: "k-1", RPSLimit: 1000, Burst: 10})

	var got []string
	stats, err := c.IterateWorks(context.Background(), WorksQuery{FieldID: 17, FromUpdatedDate: "2026-10-15"}, func(raw []byte) error {
		got = append(got, string(raw))
		return nil
	})
	if err != nil {
		t.Fatalf("IterateWorks: %v", err)
	}
	if len(got) != 5 || stats.Records != 5 || stats.Pages != 3 {
		t.Fatalf("records=%d stats=%+v, want 5 records / 3 pages", len(got), stats)
	}
	if !strings.Contains(got[0], "W100") || !strings.Contains(got[4], "W104") {
		t.Fatalf("unexpected records: %v", got)
	}
	if hits.Load() != 3 {
		t.Fatalf("server hits = %d, want 3", hits.Load())
	}
	if !c.HasAPIKey() {
		t.Fatal("client must report the configured key")
	}
}

func TestIterateWorksQueryShape(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var query []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		query = append(query, r.URL.RawQuery)
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(makePage("", 1))
	}))
	defer srv.Close()

	c := NewClient(Options{BaseURL: srv.URL, APIKey: "secret", Email: "me@example.com", RPSLimit: 1000})
	if _, err := c.IterateWorks(context.Background(), WorksQuery{
		FieldID: 17, FromUpdatedDate: "2026-10-15", FromCreatedDate: "2026-11-01", PerPage: 500,
	}, func([]byte) error { return nil }); err != nil {
		t.Fatalf("IterateWorks: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(query) != 1 {
		t.Fatalf("queries = %d", len(query))
	}
	q := query[0]
	for _, want := range []string{
		"primary_topic.field.id%3A17",
		"from_updated_date%3A2026-10-15",
		"from_created_date%3A2026-11-01",
		"per-page=200",
		"cursor=%2A",
		"api_key=secret",
		"mailto=me%40example.com",
	} {
		if !strings.Contains(q, want) {
			t.Fatalf("query %q does not contain %q", q, want)
		}
	}
}

func TestIterateWorksRequiresAPIKeyBeyondFirstPage(t *testing.T) {
	t.Parallel()

	var hits atomic.Int32
	srv := pageServer(t, map[string]worksPage{
		"*":  makePage("c2", 1),
		"c2": makePage("", 2),
	}, &hits, nil)

	c := NewClient(Options{BaseURL: srv.URL, RPSLimit: 1000})
	stats, err := c.IterateWorks(context.Background(), WorksQuery{}, func([]byte) error { return nil })
	if !errors.Is(err, ErrAPIKeyRequired) {
		t.Fatalf("err = %v, want ErrAPIKeyRequired", err)
	}
	if stats.Pages != 1 || hits.Load() != 1 {
		t.Fatalf("pages=%d hits=%d, want 1/1 (must not request page 2 without a key)", stats.Pages, hits.Load())
	}
}

func TestIterateWorksRetries429WithRetryAfter(t *testing.T) {
	t.Parallel()

	var hits atomic.Int32
	srv := pageServer(t, map[string]worksPage{"*": makePage("", 1)}, &hits, func(n int32, w http.ResponseWriter) bool {
		if n == 1 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":"rate limited"}`))
			return true
		}
		return false
	})

	jm := metrics.NewJobMetrics()
	c := NewClient(Options{BaseURL: srv.URL, RPSLimit: 1000, Metrics: jm})
	stats, err := c.IterateWorks(context.Background(), WorksQuery{}, func([]byte) error { return nil })
	if err != nil {
		t.Fatalf("IterateWorks: %v", err)
	}
	if stats.Records != 1 || stats.Requests != 2 || stats.Retries != 1 {
		t.Fatalf("stats = %+v, want 1 record / 2 requests / 1 retry", stats)
	}
	if snap := jm.Snapshot(); snap.API429Count != 1 || snap.APIRetries != 1 {
		t.Fatalf("metrics = %+v, want api429=1 retries=1", snap)
	}
}

func TestIterateWorksRetries5xx(t *testing.T) {
	t.Parallel()

	var hits atomic.Int32
	srv := pageServer(t, map[string]worksPage{"*": makePage("", 7)}, &hits, func(n int32, w http.ResponseWriter) bool {
		if n <= 2 {
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte("bad gateway"))
			return true
		}
		return false
	})

	jm := metrics.NewJobMetrics()
	c := NewClient(Options{BaseURL: srv.URL, RPSLimit: 1000, Metrics: jm, MaxAttempts: 4})
	if _, err := c.IterateWorks(context.Background(), WorksQuery{}, func([]byte) error { return nil }); err != nil {
		t.Fatalf("IterateWorks: %v", err)
	}
	if hits.Load() != 3 {
		t.Fatalf("hits = %d, want 3", hits.Load())
	}
	if snap := jm.Snapshot(); snap.API5xxCount != 2 {
		t.Fatalf("5xx = %d, want 2", snap.API5xxCount)
	}
}

func TestIterateWorksNoRetryOn400(t *testing.T) {
	t.Parallel()

	var hits atomic.Int32
	srv := pageServer(t, nil, &hits, func(n int32, w http.ResponseWriter) bool {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"bad filter"}`))
		return true
	})

	c := NewClient(Options{BaseURL: srv.URL, RPSLimit: 1000, MaxAttempts: 4})
	_, err := c.IterateWorks(context.Background(), WorksQuery{}, func([]byte) error { return nil })
	if err == nil {
		t.Fatal("expected an error")
	}
	if hits.Load() != 1 {
		t.Fatalf("hits = %d, want 1 (4xx must not be retried)", hits.Load())
	}
}

func TestIterateWorksMaxRecordsAndStop(t *testing.T) {
	t.Parallel()

	var hits atomic.Int32
	srv := pageServer(t, map[string]worksPage{
		"*":  makePage("c2", 1, 2, 3),
		"c2": makePage("", 4, 5, 6),
	}, &hits, nil)

	c := NewClient(Options{BaseURL: srv.URL, APIKey: "k", RPSLimit: 1000})
	stats, err := c.IterateWorks(context.Background(), WorksQuery{MaxRecords: 2}, func([]byte) error { return nil })
	if err != nil {
		t.Fatalf("IterateWorks: %v", err)
	}
	if stats.Records != 2 || stats.Pages != 1 {
		t.Fatalf("stats = %+v, want 2 records / 1 page", stats)
	}

	c2 := NewClient(Options{BaseURL: srv.URL, APIKey: "k", RPSLimit: 1000})
	stats, err = c2.IterateWorks(context.Background(), WorksQuery{}, func([]byte) error { return ErrStopIteration })
	if err != nil {
		t.Fatalf("ErrStopIteration must not surface: %v", err)
	}
	if stats.Records != 0 {
		t.Fatalf("records = %d, want 0 (stopped on the first callback)", stats.Records)
	}
}

func TestIterateWorksPropagatesCallbackError(t *testing.T) {
	t.Parallel()

	var hits atomic.Int32
	srv := pageServer(t, map[string]worksPage{"*": makePage("", 1)}, &hits, nil)
	c := NewClient(Options{BaseURL: srv.URL, RPSLimit: 1000})
	boom := errors.New("boom")
	_, err := c.IterateWorks(context.Background(), WorksQuery{}, func([]byte) error { return boom })
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want boom", err)
	}
}

func TestRequestAPIKey(t *testing.T) {
	t.Parallel()

	var body atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api-keys" {
			http.Error(w, "unexpected endpoint", http.StatusNotFound)
			return
		}
		raw, _ := io.ReadAll(r.Body)
		body.Store(string(raw))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"message":"key issued","api_key":"new-key"}`))
	}))
	defer srv.Close()

	c := NewClient(Options{BaseURL: srv.URL, RPSLimit: 1000})
	res, err := c.RequestAPIKey(context.Background(), "  me@example.com ")
	if err != nil {
		t.Fatalf("RequestAPIKey: %v", err)
	}
	if res.APIKey != "new-key" || res.Message != "key issued" {
		t.Fatalf("result = %+v", res)
	}
	if got, _ := body.Load().(string); !strings.Contains(got, `"email":"me@example.com"`) {
		t.Fatalf("request body = %q", got)
	}
	if !c.HasAPIKey() {
		t.Fatal("client must keep the issued key")
	}
}

func TestRequestAPIKeyRequiresEmail(t *testing.T) {
	t.Parallel()

	c := NewClient(Options{RPSLimit: 1000})
	if _, err := c.RequestAPIKey(context.Background(), "not-an-email"); err == nil {
		t.Fatal("expected an error for a malformed email")
	}
}

func TestRateLimiterCountsWaits(t *testing.T) {
	t.Parallel()

	var hits atomic.Int32
	srv := pageServer(t, map[string]worksPage{
		"*":  makePage("c2", 1),
		"c2": makePage("", 2),
	}, &hits, nil)

	jm := metrics.NewJobMetrics()
	c := NewClient(Options{BaseURL: srv.URL, APIKey: "k", RPSLimit: 5, Burst: 1, Metrics: jm})
	start := time.Now()
	if _, err := c.IterateWorks(context.Background(), WorksQuery{}, func([]byte) error { return nil }); err != nil {
		t.Fatalf("IterateWorks: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 100*time.Millisecond {
		t.Fatalf("expected the token bucket to pace the second request, elapsed=%v", elapsed)
	}
	if snap := jm.Snapshot(); snap.RateLimitWaits < 1 {
		t.Fatalf("rate limit waits = %d, want >= 1", snap.RateLimitWaits)
	}
}

func TestBackoffAndRetryAfterParsing(t *testing.T) {
	t.Parallel()

	if d := parseRetryAfter("2"); d != 2*time.Second {
		t.Fatalf("parseRetryAfter(2) = %v", d)
	}
	if d := parseRetryAfter("1.5"); d != 1500*time.Millisecond {
		t.Fatalf("parseRetryAfter(1.5) = %v", d)
	}
	if d := parseRetryAfter("garbage"); d != 0 {
		t.Fatalf("parseRetryAfter(garbage) = %v", d)
	}
	future := time.Now().Add(3 * time.Second).UTC().Format(http.TimeFormat)
	if d := parseRetryAfter(future); d <= 0 || d > 4*time.Second {
		t.Fatalf("parseRetryAfter(date) = %v", d)
	}
	if d := backoff(0, 5*time.Second); d != min(5*time.Second, retryMaxBackoff) {
		t.Fatalf("backoff with Retry-After = %v", d)
	}
	d := backoff(2, 0)
	if d < retryBaseBackoff || d > retryMaxBackoff+retryMaxBackoff/2 {
		t.Fatalf("backoff(2) = %v out of range", d)
	}
}

func TestFilterBuilding(t *testing.T) {
	t.Parallel()

	q := WorksQuery{FieldID: 17, FromUpdatedDate: "2026-10-15"}
	if got, want := q.Filter(), "primary_topic.field.id:17,from_updated_date:2026-10-15"; got != want {
		t.Fatalf("filter = %q, want %q", got, want)
	}
	q.FromCreatedDate = "2026-11-01"
	if got, want := q.Filter(), "primary_topic.field.id:17,from_updated_date:2026-10-15,from_created_date:2026-11-01"; got != want {
		t.Fatalf("filter = %q, want %q", got, want)
	}
	if got, want := (WorksQuery{}).Filter(), "primary_topic.field.id:17"; got != want {
		t.Fatalf("default filter = %q, want %q", got, want)
	}
}

func TestIterateWorksStopsExactlyOnMaxRecords(t *testing.T) {
	t.Parallel()

	var hits atomic.Int32
	srv := pageServer(t, map[string]worksPage{
		"*":  makePage("c2", 1, 2),
		"c2": makePage("", 3, 4),
	}, &hits, nil)

	c := NewClient(Options{BaseURL: srv.URL, RPSLimit: 1000})
	stats, err := c.IterateWorks(context.Background(), WorksQuery{MaxRecords: 2}, func([]byte) error { return nil })
	if err != nil {
		t.Fatalf("IterateWorks: %v", err)
	}
	if stats.Records != 2 || stats.Pages != 1 || hits.Load() != 1 {
		t.Fatalf("stats=%+v hits=%d, want 2 records / 1 page / 1 request", stats, hits.Load())
	}
}

func TestIterateWorksMapsServerAPIKeyRequired(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"API key required for cursor paging"}`))
	}))
	defer srv.Close()

	c := NewClient(Options{BaseURL: srv.URL, RPSLimit: 1000})
	_, err := c.IterateWorks(context.Background(), WorksQuery{}, func([]byte) error { return nil })
	if !errors.Is(err, ErrAPIKeyRequired) {
		t.Fatalf("err = %v, want ErrAPIKeyRequired", err)
	}
}

func TestRequestAPIKeySingleAttempt(t *testing.T) {
	t.Parallel()

	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":"Too Many Requests","code":"rate_limit_network"}`))
	}))
	defer srv.Close()

	c := NewClient(Options{BaseURL: srv.URL, RPSLimit: 1000, MaxAttempts: 5})
	if _, err := c.RequestAPIKey(context.Background(), "me@example.com"); err == nil {
		t.Fatal("expected an error for 429")
	}
	if hits.Load() != 1 {
		t.Fatalf("requests = %d, want 1 (api-keys must not be retried)", hits.Load())
	}
}

func TestIterateWorksParsesCostUSD(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"meta":{"count":2,"per_page":2,"next_cursor":"","cost_usd":0.0001},"results":[{"id":"https://openalex.org/W1"},{"id":"https://openalex.org/W2"}]}`))
	}))
	defer srv.Close()

	c := NewClient(Options{BaseURL: srv.URL, RPSLimit: 1000})
	stats, err := c.IterateWorks(context.Background(), WorksQuery{}, func([]byte) error { return nil })
	if err != nil {
		t.Fatalf("IterateWorks: %v", err)
	}
	if stats.Records != 2 {
		t.Fatalf("records = %d", stats.Records)
	}
	if stats.CostUSD < 0.00009 || stats.CostUSD > 0.00011 {
		t.Fatalf("cost_usd = %v, want ~0.0001", stats.CostUSD)
	}
}

func TestDefaultWorksSelectIsValid(t *testing.T) {
	t.Parallel()

	for _, field := range DefaultWorksSelect {
		if _, ok := validSelectFields[field]; !ok {
			t.Errorf("%q is not a valid select field for /works (live API would answer 400)", field)
		}
	}
	if len(DefaultWorksSelect) == 0 {
		t.Fatal("empty projection")
	}
	found := false
	for _, f := range DefaultWorksSelect {
		if f == "authorships" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("authorships must be selected: authors_count is computed from it")
	}
}

func TestRPSLimitIsCapped(t *testing.T) {
	t.Parallel()
	if got := (Options{RPSLimit: 500}).withDefaults().RPSLimit; got != MaxRPS {
		t.Fatalf("capped rate = %v, want %v", got, MaxRPS)
	}
	if got := (Options{RPSLimit: 5}).withDefaults().RPSLimit; got != 5 {
		t.Fatalf("a lower rate must be kept, got %v", got)
	}
	if got := (Options{}).withDefaults().RPSLimit; got != DefaultRPS {
		t.Fatalf("default rate = %v, want %v", got, DefaultRPS)
	}
}
