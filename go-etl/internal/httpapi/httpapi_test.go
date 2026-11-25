package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"go-etl/internal/adapters/api"
	"go-etl/internal/jobs"
)

type fakeBackend struct {
	mu sync.Mutex

	ingest     IngestRequest
	dimensions DimensionsRequest
	loadtest   LoadtestRequest
	emails     []string

	block     bool
	ingestErr error
	keyResult api.APIKeyResult
	keyErr    error
}

func (f *fakeBackend) RunIngest(ctx context.Context, j *jobs.Job, req IngestRequest) error {
	f.mu.Lock()
	f.ingest = req
	err := f.ingestErr
	block := f.block
	f.mu.Unlock()
	if block {
		<-ctx.Done()
		return ctx.Err()
	}
	if err != nil {
		return err
	}
	j.Metrics().AddScanned(100)
	j.Metrics().AddWritten(10, 1024)
	return nil
}

func (f *fakeBackend) RunDimensions(ctx context.Context, j *jobs.Job, req DimensionsRequest) error {
	f.mu.Lock()
	f.dimensions = req
	f.mu.Unlock()
	return nil
}

func (f *fakeBackend) RunLoadtest(ctx context.Context, j *jobs.Job, req LoadtestRequest) error {
	f.mu.Lock()
	f.loadtest = req
	f.mu.Unlock()
	return nil
}

func (f *fakeBackend) RequestAPIKey(ctx context.Context, email string) (api.APIKeyResult, error) {
	f.mu.Lock()
	f.emails = append(f.emails, email)
	res, err := f.keyResult, f.keyErr
	f.mu.Unlock()
	return res, err
}

func newTestServer(t *testing.T, be Backend) (*Server, *jobs.Manager) {
	t.Helper()
	m := jobs.NewManager()
	return New(m, be), m
}

func doRequest(t *testing.T, srv *Server, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	var reader *strings.Reader
	if body == "" {
		reader = strings.NewReader("")
	} else {
		reader = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, reader)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	return rec
}

func decodeBody[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	var out T
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode %T from %q: %v", out, rec.Body.String(), err)
	}
	return out
}

func TestHealth(t *testing.T) {
	t.Parallel()

	srv, _ := newTestServer(t, &fakeBackend{})
	rec := doRequest(t, srv, http.MethodGet, "/health", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if got := decodeBody[healthResponse](t, rec); got.Status != "ok" {
		t.Fatalf("body = %+v", got)
	}
}

func TestIngestStartDefaultsAndContract(t *testing.T) {
	t.Parallel()

	be := &fakeBackend{}
	srv, m := newTestServer(t, be)

	rec := doRequest(t, srv, http.MethodPost, "/api/v1/ingest/start", `{"source":"s3","max_files":2}`)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body)
	}
	accepted := decodeBody[jobAccepted](t, rec)
	if accepted.JobID != "ing-1" || accepted.State != jobs.StateRunning {
		t.Fatalf("accepted = %+v", accepted)
	}

	waitState(t, m, "ing-1", jobs.StateCompleted)

	be.mu.Lock()
	got := be.ingest
	be.mu.Unlock()
	if got.Source != SourceS3 || got.Workers != 4 || got.BatchSize != 10000 ||
		got.ShardTotal != 1 || got.DateFilterMode != DateFilterUpdated || got.RPSLimit != 20 {
		t.Fatalf("defaults not applied: %+v", got)
	}
	if got.MaxFiles != 2 {
		t.Fatalf("max_files = %d", got.MaxFiles)
	}

	snapRec := doRequest(t, srv, http.MethodGet, "/api/v1/jobs/ing-1", "")
	if snapRec.Code != http.StatusOK {
		t.Fatalf("job status = %d", snapRec.Code)
	}
	snap := decodeBody[jobs.Snapshot](t, snapRec)
	if snap.JobID != "ing-1" || snap.State != jobs.StateCompleted {
		t.Fatalf("snapshot = %+v", snap)
	}
	if snap.ScannedRecords != 100 || snap.WrittenRecords != 10 {
		t.Fatalf("snapshot counters = %+v", snap)
	}
}

func TestIngestValidation(t *testing.T) {
	t.Parallel()

	srv, _ := newTestServer(t, &fakeBackend{})
	cases := []struct {
		name string
		body string
	}{
		{"bad source", `{"source":"ftp"}`},
		{"bad date mode", `{"date_filter_mode":"published"}`},
		{"shard out of range", `{"shard_index":2,"shard_total":2}`},
		{"negative workers", `{"workers":-1}`},
		{"unknown field", `{"surce":"s3"}`},
		{"bad from_date", `{"from_date":"15.10.2026"}`},
	}
	for _, tc := range cases {
		rec := doRequest(t, srv, http.MethodPost, "/api/v1/ingest/start", tc.body)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400 (body %s)", tc.name, rec.Code, rec.Body)
		}
	}
}

func TestDimensionsSync(t *testing.T) {
	t.Parallel()

	be := &fakeBackend{}
	srv, m := newTestServer(t, be)
	rec := doRequest(t, srv, http.MethodPost, "/api/v1/dimensions/sync", `{"force_reload":true}`)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d", rec.Code)
	}
	accepted := decodeBody[jobAccepted](t, rec)
	if accepted.JobID != "dim-1" {
		t.Fatalf("job id = %s, want dim-1", accepted.JobID)
	}
	waitState(t, m, "dim-1", jobs.StateCompleted)
	be.mu.Lock()
	defer be.mu.Unlock()
	if !be.dimensions.ForceReload {
		t.Fatal("force_reload was not passed through")
	}
}

func TestLoadtestStartDefaults(t *testing.T) {
	t.Parallel()

	be := &fakeBackend{}
	srv, m := newTestServer(t, be)
	rec := doRequest(t, srv, http.MethodPost, "/api/v1/loadtest/start", `{"target_rps":5000}`)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body)
	}
	if accepted := decodeBody[jobAccepted](t, rec); accepted.JobID != "lt-1" {
		t.Fatalf("job id = %s, want lt-1", accepted.JobID)
	}
	waitState(t, m, "lt-1", jobs.StateCompleted)
	be.mu.Lock()
	defer be.mu.Unlock()
	if be.loadtest.TargetRPS != 5000 || be.loadtest.DurationSec != 30 || be.loadtest.Workers != 4 {
		t.Fatalf("loadtest request = %+v", be.loadtest)
	}
}

func TestJobStop(t *testing.T) {
	t.Parallel()

	be := &fakeBackend{block: true}
	srv, m := newTestServer(t, be)
	rec := doRequest(t, srv, http.MethodPost, "/api/v1/ingest/start", `{}`)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d", rec.Code)
	}

	time.Sleep(20 * time.Millisecond)
	stopRec := doRequest(t, srv, http.MethodPost, "/api/v1/jobs/ing-1/stop", "")
	if stopRec.Code != http.StatusOK {
		t.Fatalf("stop status = %d", stopRec.Code)
	}
	if got := decodeBody[stopResponse](t, stopRec); !got.Stopped {
		t.Fatal("stopped = false, want true")
	}
	waitState(t, m, "ing-1", jobs.StateStopped)

	second := doRequest(t, srv, http.MethodPost, "/api/v1/jobs/ing-1/stop", "")
	if second.Code != http.StatusOK {
		t.Fatalf("second stop status = %d", second.Code)
	}
	if got := decodeBody[stopResponse](t, second); got.Stopped {
		t.Fatal("second stop reported stopped=true")
	}
}

func TestJobNotFound(t *testing.T) {
	t.Parallel()

	srv, _ := newTestServer(t, &fakeBackend{})
	if rec := doRequest(t, srv, http.MethodGet, "/api/v1/jobs/nope-1", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("GET status = %d, want 404", rec.Code)
	}
	if rec := doRequest(t, srv, http.MethodPost, "/api/v1/jobs/nope-1/stop", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("stop status = %d, want 404", rec.Code)
	}
}

func TestMethodNotAllowed(t *testing.T) {
	t.Parallel()

	srv, _ := newTestServer(t, &fakeBackend{})
	if rec := doRequest(t, srv, http.MethodGet, "/api/v1/ingest/start", ""); rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET ingest/start = %d, want 405", rec.Code)
	}
	if rec := doRequest(t, srv, http.MethodPost, "/health", ""); rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST health = %d, want 405", rec.Code)
	}
}

func TestRequestKeyEndpoint(t *testing.T) {
	t.Parallel()

	be := &fakeBackend{keyResult: api.APIKeyResult{Message: "check your inbox"}}
	srv, _ := newTestServer(t, be)
	rec := doRequest(t, srv, http.MethodPost, "/api/v1/openalex/request-key", `{"email":"me@example.com"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body)
	}
	body := decodeBody[requestKeyResponse](t, rec)
	if !body.Requested || body.Message != "check your inbox" {
		t.Fatalf("body = %+v", body)
	}
	be.mu.Lock()
	defer be.mu.Unlock()
	if len(be.emails) != 1 || be.emails[0] != "me@example.com" {
		t.Fatalf("emails = %v", be.emails)
	}

	if rec := doRequest(t, srv, http.MethodPost, "/api/v1/openalex/request-key", `{"email":"nope"}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("invalid email status = %d, want 400", rec.Code)
	}
}

func TestRequestKeyBackendFailure(t *testing.T) {
	t.Parallel()

	be := &fakeBackend{keyErr: errors.New("openalex is down")}
	srv, _ := newTestServer(t, be)
	rec := doRequest(t, srv, http.MethodPost, "/api/v1/openalex/request-key", `{"email":"me@example.com"}`)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "openalex is down") {
		t.Fatalf("body = %s", rec.Body)
	}
}

func TestJobsLimitResponse(t *testing.T) {
	t.Parallel()

	be := &fakeBackend{block: true}
	srv, _ := newTestServer(t, be)
	for i := range jobs.MaxRunningJobs {
		rec := doRequest(t, srv, http.MethodPost, "/api/v1/ingest/start", `{}`)
		if rec.Code != http.StatusAccepted {
			t.Fatalf("start %d status = %d", i, rec.Code)
		}
	}
	rec := doRequest(t, srv, http.MethodPost, "/api/v1/ingest/start", `{}`)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429, body=%s", rec.Code, rec.Body)
	}
}

func TestInvalidJSON(t *testing.T) {
	t.Parallel()

	srv, _ := newTestServer(t, &fakeBackend{})
	rec := doRequest(t, srv, http.MethodPost, "/api/v1/ingest/start", `{"workers":`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/ingest/start", bytes.NewReader([]byte("{}")))
	req.Header.Set("Content-Type", "text/plain")
	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("text/plain status = %d, want 415", rec.Code)
	}
}

func TestNoBackend(t *testing.T) {
	t.Parallel()

	srv, _ := newTestServer(t, nil)
	if rec := doRequest(t, srv, http.MethodPost, "/api/v1/ingest/start", `{}`); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
}

func TestJobBodyFailureExposesError(t *testing.T) {
	t.Parallel()

	be := &fakeBackend{ingestErr: fmt.Errorf("s3 exploded")}
	srv, m := newTestServer(t, be)
	if rec := doRequest(t, srv, http.MethodPost, "/api/v1/ingest/start", `{}`); rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d", rec.Code)
	}
	waitState(t, m, "ing-1", jobs.StateFailed)
	snapRec := doRequest(t, srv, http.MethodGet, "/api/v1/jobs/ing-1", "")
	snap := decodeBody[jobs.Snapshot](t, snapRec)
	if snap.State != jobs.StateFailed || !strings.Contains(snap.Error, "s3 exploded") {
		t.Fatalf("snapshot = %+v", snap)
	}
}

func waitState(t *testing.T, m *jobs.Manager, id string, want jobs.State) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if snap, ok := m.Snapshot(id); ok && snap.State == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	snap, _ := m.Snapshot(id)
	t.Fatalf("job %s state = %s, want %s", id, snap.State, want)
}
