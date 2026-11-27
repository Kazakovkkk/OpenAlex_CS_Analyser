package app

import (
	"os"
	"testing"

	"go-etl/internal/httpapi"
	"go-etl/internal/ingest"
)

func TestResolveAPIKey(t *testing.T) {
	t.Setenv("OPENALEX_API_KEY", "")

	if got := ResolveAPIKey("explicit"); got != "explicit" {
		t.Fatalf("explicit key = %q", got)
	}
	if got := ResolveAPIKey("  "); got != "" {
		t.Fatalf("blank explicit key with empty env = %q, want empty", got)
	}
	if err := os.Setenv("OPENALEX_API_KEY", " from-env "); err != nil {
		t.Fatalf("setenv: %v", err)
	}
	defer func() { _ = os.Unsetenv("OPENALEX_API_KEY") }()

	if got := ResolveAPIKey(""); got != "from-env" {
		t.Fatalf("env fallback = %q, want from-env", got)
	}
	if got := ResolveAPIKey("explicit"); got != "explicit" {
		t.Fatalf("explicit must win over env, got %q", got)
	}
}

// TestDiagnosticRun pins the spec §2 rule: a bounded run must never publish a
// version or leave checkpoints that a later full load would trust.
func TestDiagnosticRun(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		req  httpapi.IngestRequest
		want bool
	}{
		{"full load", httpapi.IngestRequest{ShardTotal: 1}, false},
		{"default shard total", httpapi.IngestRequest{}, false},
		{"max files", httpapi.IngestRequest{ShardTotal: 1, MaxFiles: 5}, true},
		{"max records", httpapi.IngestRequest{ShardTotal: 1, MaxRecords: 100}, true},
		{"single shard of many", httpapi.IngestRequest{ShardTotal: 4, ShardIndex: 0}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := diagnosticRun(tc.req); got != tc.want {
				t.Fatalf("diagnosticRun(%+v) = %v, want %v", tc.req, got, tc.want)
			}
		})
	}
}

func TestPublishDataChanged(t *testing.T) {
	t.Parallel()
	if publishDataChanged(ingest.Result{}) {
		t.Fatal("an empty run must not bump the data version")
	}
	if publishDataChanged(ingest.Result{FilesSkipped: 10}) {
		t.Fatal("a fully skipped resume must keep the published metadata")
	}
	if !publishDataChanged(ingest.Result{FilesDone: 1}) {
		t.Fatal("processing files changes the dataset")
	}
	if !publishDataChanged(ingest.Result{Written: 3}) {
		t.Fatal("written rows change the dataset")
	}
}

func TestInstanceIDGenerated(t *testing.T) {
	t.Parallel()
	a := New(Config{}, nil)
	if a.InstanceID() == "" {
		t.Fatal("InstanceID() is empty")
	}
	if other := New(Config{}, nil); other.InstanceID() == a.InstanceID() {
		t.Fatal("two processes must get different instance ids")
	}
	if fixed := New(Config{InstanceID: "inst-fixed"}, nil); fixed.InstanceID() != "inst-fixed" {
		t.Fatalf("configured InstanceID = %q, want inst-fixed", fixed.InstanceID())
	}
}
