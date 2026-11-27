package jobs

import (
	"errors"

	"go-etl/internal/adapters/api"
	"go-etl/internal/clickhouse"
	"go-etl/internal/pipeline"
)

const (
	CodeSnapshotChanged       = "snapshot_changed"
	CodeRestartOfferExpired   = "restart_offer_expired"
	CodeManifestUnavailable   = "manifest_unavailable"
	CodeDiskFull              = "disk_full"
	CodeClickHouseUnavailable = "clickhouse_unavailable"
	CodeSessionBusy           = "session_busy"
	CodeAuthFailed            = "auth_failed"
	CodeAccessDenied          = "access_denied"
	CodeCorpusIncomplete      = "corpus_incomplete"
	CodePlanRequired          = "plan_required"
	CodeAPIKeyRequired        = "api_key_required"
	CodeRateLimited           = "rate_limited"
)

func ErrorCode(err error) string {
	if err == nil {
		return ""
	}

	var chErr *clickhouse.Error
	if errors.As(err, &chErr) {
		switch chErr.Kind {
		case clickhouse.KindDiskFull:
			return CodeDiskFull
		case clickhouse.KindSessionBusy:
			return CodeSessionBusy
		case clickhouse.KindAuthFailed:
			return CodeAuthFailed
		case clickhouse.KindAccessDenied:
			return CodeAccessDenied
		case clickhouse.KindUnavailable:
			return CodeClickHouseUnavailable
		}
	}

	var changed *pipeline.SnapshotChangedError
	if errors.As(err, &changed) {
		return CodeSnapshotChanged
	}
	switch {
	case errors.Is(err, pipeline.ErrCorpusIncomplete):
		return CodeCorpusIncomplete
	case errors.Is(err, pipeline.ErrRestartOfferExpired):
		return CodeRestartOfferExpired
	case errors.Is(err, api.ErrPlanRequired):
		return CodePlanRequired
	case errors.Is(err, api.ErrAPIKeyRequired):
		return CodeAPIKeyRequired
	case errors.Is(err, pipeline.ErrLoadInProgress):
		return CodeSessionBusy
	}
	return ""
}
