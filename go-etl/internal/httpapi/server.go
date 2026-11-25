package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"go-etl/internal/adapters/api"
	"go-etl/internal/jobs"
)

const (
	DefaultMaxBodyBytes = 1 << 20

	pathHealth      = "/health"
	pathDimsSync    = "/api/v1/dimensions/sync"
	pathIngestStart = "/api/v1/ingest/start"
	pathLoadtest    = "/api/v1/loadtest/start"
	pathRequestKey  = "/api/v1/openalex/request-key"
	pathJob         = "/api/v1/jobs/{job_id}"
	pathJobStop     = "/api/v1/jobs/{job_id}/stop"
)

type Backend interface {
	RunIngest(ctx context.Context, j *jobs.Job, req IngestRequest) error
	RunDimensions(ctx context.Context, j *jobs.Job, req DimensionsRequest) error
	RunLoadtest(ctx context.Context, j *jobs.Job, req LoadtestRequest) error
	RequestAPIKey(ctx context.Context, email string) (api.APIKeyResult, error)
}

type Option func(*Server)

func WithMaxBodyBytes(n int64) Option {
	return func(s *Server) {
		if n > 0 {
			s.maxBody = n
		}
	}
}

func WithBaseContext(ctx context.Context) Option {
	return func(s *Server) {
		if ctx != nil {
			s.baseCtx = ctx
		}
	}
}

type Server struct {
	manager *jobs.Manager
	backend Backend
	maxBody int64
	baseCtx context.Context
	mux     *http.ServeMux
}

func New(manager *jobs.Manager, backend Backend, opts ...Option) *Server {
	s := &Server{
		manager: manager,
		backend: backend,
		maxBody: DefaultMaxBodyBytes,
		baseCtx: context.Background(),
		mux:     http.NewServeMux(),
	}
	for _, opt := range opts {
		opt(s)
	}
	s.routes()
	return s
}

func (s *Server) routes() {
	s.mux.HandleFunc("GET "+pathHealth, s.handleHealth)
	s.mux.HandleFunc("POST "+pathDimsSync, s.handleDimensionsSync)
	s.mux.HandleFunc("POST "+pathIngestStart, s.handleIngestStart)
	s.mux.HandleFunc("POST "+pathLoadtest, s.handleLoadtestStart)
	s.mux.HandleFunc("GET "+pathJob, s.handleJob)
	s.mux.HandleFunc("POST "+pathJobStop, s.handleJobStop)
	s.mux.HandleFunc("POST "+pathRequestKey, s.handleRequestKey)
}

func (s *Server) Handler() http.Handler { return s.mux }

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

type errorResponse struct {
	Error string `json:"error"`
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if payload == nil {
		return
	}
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(payload)
}

func writeError(w http.ResponseWriter, status int, format string, args ...any) {
	writeJSON(w, status, errorResponse{Error: fmt.Sprintf(format, args...)})
}

func (s *Server) decode(w http.ResponseWriter, r *http.Request, dst any) bool {
	if ct := r.Header.Get("Content-Type"); ct != "" && !strings.HasPrefix(ct, "application/json") {
		writeError(w, http.StatusUnsupportedMediaType, "expected application/json, got %q", ct)
		return false
	}
	body := http.MaxBytesReader(w, r.Body, s.maxBody)
	dec := json.NewDecoder(body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		if errors.Is(err, io.EOF) {
			return true
		}
		writeError(w, http.StatusBadRequest, "invalid json body: %v", err)
		return false
	}
	return true
}

func (s *Server) requireBackend(w http.ResponseWriter) bool {
	if s.backend == nil {
		writeError(w, http.StatusServiceUnavailable, "backend is not configured")
		return false
	}
	return true
}

func (s *Server) startJob(w http.ResponseWriter, kind jobs.Kind, run jobs.Runner) {
	j, err := s.manager.Start(s.baseCtx, kind, run)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, jobs.ErrJobsLimit) {
			status = http.StatusTooManyRequests
		}
		writeError(w, status, "%v", err)
		return
	}
	writeJSON(w, http.StatusAccepted, jobAccepted{JobID: j.ID(), State: jobs.StateRunning})
}
