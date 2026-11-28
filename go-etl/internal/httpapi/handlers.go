package httpapi

import (
	"context"
	"net/http"
	"strings"

	"go-etl/internal/jobs"
)

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, healthResponse{Status: "ok", InstanceID: s.instanceID})
}

// handleIngestRestart accepts a one-shot restart token. A token already
// accepted in this process returns the same job before the common write limit
// is checked; an expired or unknown token is 409 restart_offer_expired.
func (s *Server) handleIngestRestart(w http.ResponseWriter, r *http.Request) {
	if !s.requireBackend(w) || !s.writable(w) {
		return
	}
	var req RestartRequest
	if !s.decode(w, r, &req) {
		return
	}
	token := strings.TrimSpace(req.RestartToken)
	if token == "" {
		writeAPIError(w, http.StatusBadRequest, "restart_offer_expired", "restart_token is required")
		return
	}
	if j, ok := s.manager.FindByToken(token); ok {
		s.accepted(w, j)
		return
	}
	if err := s.backend.PreviewRestart(r.Context(), token); err != nil {
		code := jobs.ErrorCode(err)
		if code == "" {
			code = jobs.CodeRestartOfferExpired
		}
		status := http.StatusConflict
		if code == jobs.CodeClickHouseUnavailable {
			status = http.StatusServiceUnavailable
		}
		writeAPIError(w, status, code, err.Error())
		return
	}
	s.startExclusive(w, jobs.KindIngest, token, func(ctx context.Context, j *jobs.Job) error {
		return s.backend.RestartIngest(ctx, j, req)
	})
}

func (s *Server) handleDimensionsSync(w http.ResponseWriter, r *http.Request) {
	if !s.requireBackend(w) || !s.writable(w) {
		return
	}
	var req DimensionsRequest
	if !s.decode(w, r, &req) {
		return
	}
	s.startExclusive(w, jobs.KindDimensions, "", func(ctx context.Context, j *jobs.Job) error {
		return s.backend.RunDimensions(ctx, j, req)
	})
}

func (s *Server) handleIngestStart(w http.ResponseWriter, r *http.Request) {
	if !s.requireBackend(w) || !s.writable(w) {
		return
	}
	var req IngestRequest
	if !s.decode(w, r, &req) {
		return
	}
	if err := req.normalize(); err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	// The REST source is a manual/test mode; the stock HTTP path always loads
	// the S3 corpus. Python must not switch the working corpus to a REST plan.
	if req.Source == SourceAPI {
		writeAPIError(w, http.StatusBadRequest, "rest_manual_only",
			"REST ingest is available only in manual CLI mode on a dedicated target")
		return
	}
	// The stock HTTP path owns the whole corpus: one shard only.
	if req.ShardTotal != 1 || req.ShardIndex != 0 {
		writeAPIError(w, http.StatusBadRequest, "invalid_request",
			"the HTTP ingest runs one shard (shard_total=1, shard_index=0); use the CLI for a separate shard")
		return
	}
	s.startExclusive(w, jobs.KindIngest, "", func(ctx context.Context, j *jobs.Job) error {
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
		writeAPIError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	s.startJob(w, jobs.KindLoadTest, func(ctx context.Context, j *jobs.Job) error {
		return s.backend.RunLoadtest(ctx, j, req)
	})
}

func (s *Server) handleJob(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.PathValue("job_id"))
	if id == "" {
		writeAPIError(w, http.StatusBadRequest, "invalid_request", "job_id is required")
		return
	}
	snap, ok := s.manager.Snapshot(id)
	if !ok {
		writeAPIError(w, http.StatusNotFound, "not_found", "job not found")
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

func (s *Server) handleJobStop(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.PathValue("job_id"))
	if id == "" {
		writeAPIError(w, http.StatusBadRequest, "invalid_request", "job_id is required")
		return
	}
	if _, ok := s.manager.Get(id); !ok {
		writeAPIError(w, http.StatusNotFound, "not_found", "job not found")
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
		writeAPIError(w, http.StatusBadRequest, "email_required", "a valid email is required")
		return
	}
	res, err := s.backend.RequestAPIKey(r.Context(), email)
	if err != nil {
		writeAPIError(w, http.StatusBadGateway, "openalex_request_failed",
			"openalex api-keys request failed: "+err.Error())
		return
	}
	msg := strings.TrimSpace(res.Message)
	if msg == "" {
		msg = "request accepted"
	}
	writeJSON(w, http.StatusOK, requestKeyResponse{Requested: true, Message: msg})
}
