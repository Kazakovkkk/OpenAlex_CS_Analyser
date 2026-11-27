package jobs

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"go-etl/internal/adapters/api"
	"go-etl/internal/clickhouse"
	"go-etl/internal/pipeline"
)

func TestErrorCode(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"nil", nil, ""},
		{"plain", errors.New("boom"), ""},
		{"disk full", &clickhouse.Error{Kind: clickhouse.KindDiskFull}, CodeDiskFull},
		{"session busy", &clickhouse.Error{Kind: clickhouse.KindSessionBusy}, CodeSessionBusy},
		{"auth", &clickhouse.Error{Kind: clickhouse.KindAuthFailed}, CodeAuthFailed},
		{"access", &clickhouse.Error{Kind: clickhouse.KindAccessDenied}, CodeAccessDenied},
		{"unavailable", &clickhouse.Error{Kind: clickhouse.KindUnavailable}, CodeClickHouseUnavailable},
		{"wrapped clickhouse", fmt.Errorf("insert: %w", &clickhouse.Error{Kind: clickhouse.KindDiskFull}), CodeDiskFull},
		{"plan required", fmt.Errorf("api: %w", api.ErrPlanRequired), CodePlanRequired},
		{"api key", api.ErrAPIKeyRequired, CodeAPIKeyRequired},
		{"load in progress", fmt.Errorf("app: %w", pipeline.ErrLoadInProgress), CodeSessionBusy},
		{"unknown pipeline error", pipeline.ErrNotLoading, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ErrorCode(tc.err); got != tc.want {
				t.Fatalf("ErrorCode(%v) = %q, want %q", tc.err, got, tc.want)
			}
		})
	}
}

func TestFailedJobCarriesCode(t *testing.T) {
	t.Parallel()
	m := NewManager()
	boom := &clickhouse.Error{Kind: clickhouse.KindDiskFull}
	j, err := m.Start(t.Context(), KindIngest, func(context.Context, *Job) error { return boom })
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	<-j.Done()

	snap := j.Snapshot()
	if snap.State != StateFailed {
		t.Fatalf("state = %s, want failed", snap.State)
	}
	if snap.ErrorCode != CodeDiskFull {
		t.Fatalf("error_code = %q, want %q", snap.ErrorCode, CodeDiskFull)
	}
	if snap.Error == "" {
		t.Fatal("error text must stay available for the log")
	}
}

func TestSnapshotChangedCarriesRestartToken(t *testing.T) {
	t.Parallel()
	err := &pipeline.SnapshotChangedError{Token: "tok-1", PreviousManifest: "a", CurrentManifest: "b"}
	if got := ErrorCode(err); got != CodeSnapshotChanged {
		t.Fatalf("ErrorCode = %q, want %q", got, CodeSnapshotChanged)
	}

	m := NewManager()
	j, serr := m.Start(t.Context(), KindIngest, func(context.Context, *Job) error { return err })
	if serr != nil {
		t.Fatalf("Start: %v", serr)
	}
	<-j.Done()

	snap := j.Snapshot()
	if snap.ErrorCode != CodeSnapshotChanged {
		t.Fatalf("error_code = %q, want %q", snap.ErrorCode, CodeSnapshotChanged)
	}
	if snap.RestartToken != "tok-1" {
		t.Fatalf("restart_token = %q, want tok-1", snap.RestartToken)
	}
}

func TestCorpusIncompleteCode(t *testing.T) {
	t.Parallel()
	err := fmt.Errorf("%w: 3 of 100 file(s) failed", pipeline.ErrCorpusIncomplete)
	if got := ErrorCode(err); got != CodeCorpusIncomplete {
		t.Fatalf("ErrorCode = %q, want %q", got, CodeCorpusIncomplete)
	}
}
