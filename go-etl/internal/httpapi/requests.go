package httpapi

import (
	"fmt"
	"strings"
	"time"

	"go-etl/internal/jobs"
)

type (
	DimensionsRequest struct {
		ForceReload bool `json:"force_reload"`
	}

	// RestartRequest confirms a restart after the release changed mid-load.
	RestartRequest struct {
		RestartToken string `json:"restart_token"`
	}

	IngestRequest struct {
		Source         string `json:"source"`
		Workers        int    `json:"workers"`
		BatchSize      int    `json:"batch_size"`
		ShardIndex     int    `json:"shard_index"`
		ShardTotal     int    `json:"shard_total"`
		MaxFiles       int    `json:"max_files"`
		FromDate       string `json:"from_date"`
		DateFilterMode string `json:"date_filter_mode"`
		APIKey         string `json:"api_key"`
		RPSLimit       int    `json:"rps_limit"`
		MaxRecords     int    `json:"max_records"`
	}

	LoadtestRequest struct {
		TargetRPS        int  `json:"target_rps"`
		DurationSec      int  `json:"duration_sec"`
		Workers          int  `json:"workers"`
		BatchSize        int  `json:"batch_size"`
		WriteToTempTable bool `json:"write_to_temp_table"`
	}

	RequestKeyRequest struct {
		Email string `json:"email"`
	}
)

const (
	SourceS3  = "s3"
	SourceAPI = "api"

	DateFilterUpdated     = "updated"
	DateFilterCreated     = "created"
	DateFilterPublication = "publication"
)

func (r *IngestRequest) normalize() error {
	r.Source = strings.ToLower(strings.TrimSpace(r.Source))
	if r.Source == "" {
		r.Source = SourceS3
	}
	if r.Source != SourceS3 && r.Source != SourceAPI {
		return fmt.Errorf("source must be %q or %q, got %q", SourceS3, SourceAPI, r.Source)
	}

	r.DateFilterMode = strings.ToLower(strings.TrimSpace(r.DateFilterMode))
	if r.DateFilterMode == "" {
		r.DateFilterMode = DateFilterUpdated
	}
	switch r.DateFilterMode {
	case DateFilterUpdated, DateFilterCreated, DateFilterPublication:
	default:
		return fmt.Errorf("date_filter_mode must be %q, %q or %q, got %q",
			DateFilterUpdated, DateFilterCreated, DateFilterPublication, r.DateFilterMode)
	}

	if r.Workers == 0 {
		r.Workers = 4
	}
	if r.Workers < 0 || r.Workers > 64 {
		return fmt.Errorf("workers must be in 1..64, got %d", r.Workers)
	}
	if r.BatchSize == 0 {
		r.BatchSize = 10000
	}
	if r.BatchSize < 0 || r.BatchSize > 100000 {
		return fmt.Errorf("batch_size must be in 1..100000, got %d", r.BatchSize)
	}
	if r.ShardTotal == 0 {
		r.ShardTotal = 1
	}
	if r.ShardTotal < 0 {
		return fmt.Errorf("shard_total must be >= 1, got %d", r.ShardTotal)
	}
	if r.ShardIndex < 0 || r.ShardIndex >= r.ShardTotal {
		return fmt.Errorf("shard_index must be in 0..%d, got %d", r.ShardTotal-1, r.ShardIndex)
	}
	if r.RPSLimit == 0 {
		r.RPSLimit = 20
	}
	if r.RPSLimit < 0 {
		return fmt.Errorf("rps_limit must be >= 0, got %d", r.RPSLimit)
	}
	if r.MaxFiles < 0 || r.MaxRecords < 0 {
		return fmt.Errorf("max_files and max_records must be >= 0")
	}
	if r.FromDate != "" {
		if _, err := time.Parse("2006-01-02", r.FromDate); err != nil {
			return fmt.Errorf("from_date must be YYYY-MM-DD, got %q", r.FromDate)
		}
	}
	return nil
}

func (r *LoadtestRequest) normalize() error {
	if r.TargetRPS == 0 {
		r.TargetRPS = 10000
	}
	if r.TargetRPS < 0 {
		return fmt.Errorf("target_rps must be >= 0, got %d", r.TargetRPS)
	}
	if r.DurationSec == 0 {
		r.DurationSec = 30
	}
	if r.DurationSec < 0 || r.DurationSec > 3600 {
		return fmt.Errorf("duration_sec must be in 1..3600, got %d", r.DurationSec)
	}
	if r.Workers == 0 {
		r.Workers = 4
	}
	if r.Workers < 0 || r.Workers > 64 {
		return fmt.Errorf("workers must be in 1..64, got %d", r.Workers)
	}
	if r.BatchSize == 0 {
		r.BatchSize = 10000
	}
	if r.BatchSize < 0 || r.BatchSize > 100000 {
		return fmt.Errorf("batch_size must be in 1..100000, got %d", r.BatchSize)
	}
	return nil
}

// Responses.
type (
	healthResponse struct {
		Status     string `json:"status"`
		InstanceID string `json:"instance_id"`
	}

	jobAccepted struct {
		JobID      string     `json:"job_id"`
		InstanceID string     `json:"instance_id"`
		State      jobs.State `json:"state"`
	}

	stopResponse struct {
		Stopped bool `json:"stopped"`
	}

	requestKeyResponse struct {
		Requested bool   `json:"requested"`
		Message   string `json:"message"`
	}
)
