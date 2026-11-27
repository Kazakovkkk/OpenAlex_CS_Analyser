package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"

	ch "github.com/ClickHouse/clickhouse-go/v2"
)

func TestClassifyServerCodes(t *testing.T) {
	cases := []struct {
		name string
		code int32
		msg  string
		want Kind
	}{
		{"auth failed", 516, "Authentication failed: password is incorrect", KindAuthFailed},
		{"unknown user", 192, "Unknown user", KindAuthFailed},
		{"wrong password", 193, "Wrong password", KindAuthFailed},
		{"access denied", 497, "Access denied for user etl_writer", KindAccessDenied},
		{"disk full", 243, "Cannot reserve 1.00 MiB, not enough space", KindDiskFull},
		{"unknown table", 60, "Table openalex.works does not exist", KindUnknown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := &ch.Exception{Code: tc.code, Message: tc.msg}
			got := Classify(err)
			if got == nil {
				t.Fatal("Classify = nil")
			}
			if got.Kind != tc.want {
				t.Fatalf("Kind = %q, want %q", got.Kind, tc.want)
			}
			if got.Code != tc.code {
				t.Fatalf("Code = %d, want %d", got.Code, tc.code)
			}
		})
	}
}

func TestClassifySessionBusyByMessage(t *testing.T) {
	for _, msg := range []string{
		"Too many sessions for user pipeline_writer",
		"Session 1 is locked by a concurrent client",
		"max_sessions_for_user limit reached",
	} {
		got := Classify(&ch.Exception{Code: 373, Message: msg})
		if got.Kind != KindSessionBusy {
			t.Fatalf("message %q classified as %q, want %q", msg, got.Kind, KindSessionBusy)
		}
	}
}

func TestClassifyWrappedException(t *testing.T) {
	base := &ch.Exception{Code: 497, Message: "Access denied"}
	wrapped := fmt.Errorf("insert works: %w", base)
	got := Classify(wrapped)
	if got.Kind != KindAccessDenied {
		t.Fatalf("Kind = %q, want %q", got.Kind, KindAccessDenied)
	}
	if !errors.Is(got, base) {
		t.Fatal("wrapped exception is not reachable via errors.Is")
	}
}

func TestClassifyTransportAndContext(t *testing.T) {
	if got := Classify(&net.OpError{Op: "dial", Err: errors.New("connection refused")}); got.Kind != KindUnavailable {
		t.Fatalf("dial error Kind = %q, want %q", got.Kind, KindUnavailable)
	}
	if got := Classify(context.Canceled); got.Kind != KindCanceled {
		t.Fatalf("canceled Kind = %q, want %q", got.Kind, KindCanceled)
	}
	if got := Classify(context.DeadlineExceeded); got.Kind != KindCanceled {
		t.Fatalf("deadline Kind = %q, want %q", got.Kind, KindCanceled)
	}
	if got := Classify(nil); got != nil {
		t.Fatalf("Classify(nil) = %v, want nil", got)
	}
}

func TestClassifyIsIdempotent(t *testing.T) {
	first := Classify(&ch.Exception{Code: 243, Message: "not enough space"})
	second := Classify(fmt.Errorf("outer: %w", first))
	if second != first {
		t.Fatalf("re-classification produced a new error: %v vs %v", first, second)
	}
}

func TestErrorTextCarriesNoSecrets(t *testing.T) {
	err := Classify(&ch.Exception{Code: 516, Message: "Authentication failed: password is incorrect"})
	if err.Error() == "" {
		t.Fatal("empty error text")
	}
	if got := KindOf(err); got != KindAuthFailed {
		t.Fatalf("KindOf = %q, want %q", got, KindAuthFailed)
	}
}

func TestRoleDefaultsPoolSize(t *testing.T) {
	if o := (Options{}).withDefaults(RolePipelineWriter); o.MaxOpenConns != 1 {
		t.Fatalf("pipeline writer MaxOpenConns = %d, want 1", o.MaxOpenConns)
	}
	if o := (Options{}).withDefaults(RoleETLWriter); o.MaxOpenConns <= 1 {
		t.Fatalf("etl writer MaxOpenConns = %d, want > 1", o.MaxOpenConns)
	}
}

func TestCompressionMapping(t *testing.T) {
	if got := compressionFor("zstd").Method; got != ch.CompressionZSTD {
		t.Fatalf("zstd mapped to %v", got)
	}
	if got := compressionFor("none").Method; got != ch.CompressionNone {
		t.Fatalf("none mapped to %v", got)
	}
	if got := compressionFor("").Method; got != ch.CompressionLZ4 {
		t.Fatalf("default mapped to %v, want LZ4", got)
	}
}

func TestOpenRejectsEmptyOptions(t *testing.T) {
	ctx := context.Background()
	if _, err := Open(ctx, RoleETLWriter, Options{Addr: ""}); err == nil {
		t.Fatal("Open(empty addr) = nil error")
	}
	if _, err := Open(ctx, RoleETLWriter, Options{Addr: "127.0.0.1:9000", Database: ""}); err == nil {
		t.Fatal("Open(empty database) = nil error")
	}
	if _, err := Open(ctx, RoleETLWriter, Options{Addr: "127.0.0.1:9000", Database: "db", User: ""}); err == nil {
		t.Fatal("Open(empty user) = nil error")
	}
}

func TestOpenUnreachableIsUnavailable(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping dial test in -short")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2e9)
	defer cancel()
	_, err := Open(ctx, RoleETLWriter, Options{
		Addr: "127.0.0.1:1", Database: "openalex", User: "etl_writer",
		DialTimeout: 500e6, PingTimeout: 500e6,
	})
	if err == nil {
		t.Skip("unexpectedly connected to 127.0.0.1:1")
	}
	if got := KindOf(err); got != KindUnavailable {
		t.Fatalf("KindOf = %q, want %q (err: %v)", got, KindUnavailable, err)
	}
}
