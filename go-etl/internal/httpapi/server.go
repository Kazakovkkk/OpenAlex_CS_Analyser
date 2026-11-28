package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"

	"go-etl/internal/adapters/api"
	"go-etl/internal/jobs"
)

const (
	DefaultMaxBodyBytes = 1 << 20

	pathHealth        = "/health"
	pathDimsSync      = "/api/v1/dimensions/sync"
	pathIngestStart   = "/api/v1/ingest/start"
	pathIngestRestart = "/api/v1/ingest/restart"
	pathLoadtest      = "/api/v1/loadtest/start"
	pathRequestKey    = "/api/v1/openalex/request-key"
	pathJob           = "/api/v1/jobs/{job_id}"
	pathJobStop       = "/api/v1/jobs/{job_id}/stop"
)

type Backend interface {
	RunIngest(ctx context.Context, j *jobs.Job, req IngestRequest) error
	RestartIngest(ctx context.Context, j *jobs.Job, req RestartRequest) error
	PreviewRestart(ctx context.Context, token string) error
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

func WithInstanceID(id string) Option {
	return func(s *Server) {
		if id != "" {
			s.instanceID = id
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
	manager    *jobs.Manager
	backend    Backend
	maxBody    int64
	baseCtx    context.Context
	instanceID string
	mux        *http.ServeMux
	draining   atomic.Bool
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
	s.mux.HandleFunc("POST "+pathIngestRestart, s.handleIngestRestart)
	s.mux.HandleFunc("POST "+pathLoadtest, s.handleLoadtestStart)
	s.mux.HandleFunc("GET "+pathJob, s.handleJob)
	s.mux.HandleFunc("POST "+pathJobStop, s.handleJobStop)
	s.mux.HandleFunc("POST "+pathRequestKey, s.handleRequestKey)
}

func (s *Server) Handler() http.Handler { return s.mux }

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

type errorResponse struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Error     string `json:"error"`
	ErrorCode string `json:"error_code,omitempty"`
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
	writeAPIError(w, status, "invalid_request", fmt.Sprintf(format, args...))
}

func writeErrorCode(w http.ResponseWriter, status int, code, format string, args ...any) {
	writeAPIError(w, status, code, fmt.Sprintf(format, args...))
}

// writeAPIError emits the error contract: code, message and a compatible
// legacy "error" string.
func writeAPIError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, errorResponse{Code: code, Message: msg, Error: msg, ErrorCode: code})
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
		switch {
		case errors.Is(err, jobs.ErrJobsLimit):
			status = http.StatusTooManyRequests
		case errors.Is(err, jobs.ErrAdmissionClosed):
			status = http.StatusServiceUnavailable
		}
		writeError(w, status, "%v", err)
		return
	}
	s.accepted(w, j)
}

// startExclusive registers a corpus-writing job under the single write slot.
// A repeat of the same operation (or the same restart token) returns 202 with
// the existing job id; a competing operation gets 409 session_busy.
func (s *Server) startExclusive(w http.ResponseWriter, kind jobs.Kind, token string, run jobs.Runner) {
	j, duplicate, err := s.manager.StartExclusive(s.baseCtx, kind, token, run)
	switch {
	case errors.Is(err, jobs.ErrSessionBusy):
		writeAPIError(w, http.StatusConflict, "session_busy", err.Error())
		return
	case errors.Is(err, jobs.ErrAdmissionClosed):
		writeAPIError(w, http.StatusServiceUnavailable, "shutting_down", err.Error())
		return
	case errors.Is(err, jobs.ErrJobsLimit):
		writeAPIError(w, http.StatusTooManyRequests, "too_many_jobs", err.Error())
		return
	case err != nil:
		writeAPIError(w, http.StatusInternalServerError, "internal_error", err.Error())
		return
	}
	if duplicate {
		s.accepted(w, j)
		return
	}
	s.accepted(w, j)
}

// BeginShutdown closes the admission of new write jobs before the active job
// is stopped and awaited.
func (s *Server) BeginShutdown() { s.draining.Store(true) }

func (s *Server) writable(w http.ResponseWriter) bool {
	if s.draining.Load() {
		writeAPIError(w, http.StatusServiceUnavailable, "shutting_down", "server is shutting down")
		return false
	}
	return true
}

// accepted answers with the existing job (idempotent start).
func (s *Server) accepted(w http.ResponseWriter, j *jobs.Job) {
	writeJSON(w, http.StatusAccepted, jobAccepted{JobID: j.ID(), InstanceID: s.instanceID, State: j.State()})
}
