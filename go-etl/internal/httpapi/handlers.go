package httpapi

import (
	"context"
	"net/http"
	"strings"

	"go-etl/internal/jobs"
)

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, healthResponse{Status: "ok"})
}

func (s *Server) handleDimensionsSync(w http.ResponseWriter, r *http.Request) {
	if !s.requireBackend(w) {
		return
	}
	var req DimensionsRequest
	if !s.decode(w, r, &req) {
		return
	}
	s.startJob(w, jobs.KindDimensions, func(ctx context.Context, j *jobs.Job) error {
		return s.backend.RunDimensions(ctx, j, req)
	})
}

func (s *Server) handleIngestStart(w http.ResponseWriter, r *http.Request) {
	if !s.requireBackend(w) {
		return
	}
	var req IngestRequest
	if !s.decode(w, r, &req) {
		return
	}
	if err := req.normalize(); err != nil {
		writeError(w, http.StatusBadRequest, "%v", err)
		return
	}
	s.startJob(w, jobs.KindIngest, func(ctx context.Context, j *jobs.Job) error {
		return s.backend.RunIngest(ctx, j, req)
	})
}

func (s *Server) handleLoadtestStart(w http.ResponseWriter, r *http.Request) {
	if !s.requireBackend(w) {
		return
	}
	var req LoadtestRequest
	if !s.decode(w, r, &req) {
		return
	}
	if err := req.normalize(); err != nil {
		writeError(w, http.StatusBadRequest, "%v", err)
		return
	}
	s.startJob(w, jobs.KindLoadTest, func(ctx context.Context, j *jobs.Job) error {
		return s.backend.RunLoadtest(ctx, j, req)
	})
}

func (s *Server) handleJob(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.PathValue("job_id"))
	if id == "" {
		writeError(w, http.StatusBadRequest, "job_id is required")
		return
	}
	snap, ok := s.manager.Snapshot(id)
	if !ok {
		writeError(w, http.StatusNotFound, "job %q not found", id)
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

func (s *Server) handleJobStop(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.PathValue("job_id"))
	if id == "" {
		writeError(w, http.StatusBadRequest, "job_id is required")
		return
	}
	if _, ok := s.manager.Get(id); !ok {
		writeError(w, http.StatusNotFound, "job %q not found", id)
		return
	}
	writeJSON(w, http.StatusOK, stopResponse{Stopped: s.manager.Stop(id)})
}

func (s *Server) handleRequestKey(w http.ResponseWriter, r *http.Request) {
	if !s.requireBackend(w) {
		return
	}
	var req RequestKeyRequest
	if !s.decode(w, r, &req) {
		return
	}
	email := strings.TrimSpace(req.Email)
	if email == "" || !strings.Contains(email, "@") {
		writeError(w, http.StatusBadRequest, "a valid email is required")
		return
	}
	res, err := s.backend.RequestAPIKey(r.Context(), email)
	if err != nil {
		writeError(w, http.StatusBadGateway, "openalex api-keys request failed: %v", err)
		return
	}
	msg := strings.TrimSpace(res.Message)
	if msg == "" {
		msg = "request accepted"
	}
	writeJSON(w, http.StatusOK, requestKeyResponse{Requested: true, Message: msg})
}
