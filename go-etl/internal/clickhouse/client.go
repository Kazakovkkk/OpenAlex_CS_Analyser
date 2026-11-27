package clickhouse

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strings"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
)

type Role string

const (
	RoleETLWriter      Role = "etl_writer"
	RolePipelineWriter Role = "pipeline_writer"
)

const (
	codeNotEnoughSpace     = 243 // NOT_ENOUGH_SPACE
	codeUnknownUser        = 192
	codeWrongPassword      = 193
	codeRequiredPassword   = 194
	codeAuthenticationFail = 516 // AUTHENTICATION_FAILED
	codeAccessDenied       = 497
	codeUnknownDatabase    = 81
	codeUnknownTable       = 60
)

type Options struct {
	Addr            string
	Database        string
	User            string
	Password        string
	Secure          bool
	ServerName      string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	DialTimeout     time.Duration
	ReadTimeout     time.Duration
	Compression     string
	PingTimeout     time.Duration
}

func (o Options) withDefaults(role Role) Options {
	if o.MaxOpenConns <= 0 {
		if role == RolePipelineWriter {
			o.MaxOpenConns = 1
		} else {
			o.MaxOpenConns = 8
		}
	}
	if o.MaxIdleConns <= 0 {
		o.MaxIdleConns = o.MaxOpenConns
	}
	if o.DialTimeout <= 0 {
		o.DialTimeout = 10 * time.Second
	}
	if o.ReadTimeout <= 0 {
		o.ReadTimeout = 120 * time.Second
	}
	if o.PingTimeout <= 0 {
		o.PingTimeout = 5 * time.Second
	}
	if o.Compression == "" {
		o.Compression = "lz4"
	}
	return o
}

func Open(ctx context.Context, role Role, o Options) (ch.Conn, error) {
	o = o.withDefaults(role)
	if strings.TrimSpace(o.Addr) == "" {
		return nil, errors.New("clickhouse: empty address")
	}
	if strings.TrimSpace(o.Database) == "" {
		return nil, errors.New("clickhouse: empty database")
	}
	if strings.TrimSpace(o.User) == "" {
		return nil, fmt.Errorf("clickhouse: empty user for role %s", role)
	}

	opts := &ch.Options{
		Addr:            []string{o.Addr},
		Auth:            ch.Auth{Database: o.Database, Username: o.User, Password: o.Password},
		DialTimeout:     o.DialTimeout,
		ReadTimeout:     o.ReadTimeout,
		MaxOpenConns:    o.MaxOpenConns,
		MaxIdleConns:    o.MaxIdleConns,
		ConnMaxLifetime: o.ConnMaxLifetime,
		Compression:     compressionFor(o.Compression),
	}
	if o.Secure {
		server := o.ServerName
		if server == "" {
			host := o.Addr
			if i := strings.LastIndex(host, ":"); i > 0 {
				host = host[:i]
			}
			server = host
		}
		opts.TLS = &tls.Config{MinVersion: tls.VersionTLS12, ServerName: server}
	}
	if role == RolePipelineWriter {
		opts.Settings = ch.Settings{"max_execution_time": 0}
	}

	conn, err := ch.Open(opts)
	if err != nil {
		return nil, Classify(err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, o.PingTimeout)
	defer cancel()
	if err := conn.Ping(pingCtx); err != nil {
		_ = conn.Close()
		return nil, Classify(err)
	}
	return conn, nil
}

func compressionFor(name string) *ch.Compression {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "none", "off":
		return &ch.Compression{Method: ch.CompressionNone}
	case "zstd":
		return &ch.Compression{Method: ch.CompressionZSTD}
	default:
		return &ch.Compression{Method: ch.CompressionLZ4}
	}
}

type Kind string

const (
	KindAuthFailed   Kind = "auth_failed"
	KindAccessDenied Kind = "access_denied"
	KindSessionBusy  Kind = "session_busy"
	KindDiskFull     Kind = "disk_full"
	KindUnavailable  Kind = "clickhouse_unavailable"
	KindCanceled     Kind = "canceled"
	KindUnknown      Kind = ""
)

type Error struct {
	Kind    Kind
	Code    int32
	Message string
	Err     error
}

func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	parts := make([]string, 0, 3)
	if e.Kind != KindUnknown {
		parts = append(parts, string(e.Kind))
	}
	if e.Code != 0 {
		parts = append(parts, fmt.Sprintf("code %d", e.Code))
	}
	if e.Message != "" {
		parts = append(parts, e.Message)
	} else if e.Err != nil {
		parts = append(parts, e.Err.Error())
	}
	return "clickhouse: " + strings.Join(parts, ": ")
}

func (e *Error) Unwrap() error { return e.Err }

func Classify(err error) *Error {
	if err == nil {
		return nil
	}
	var already *Error
	if errors.As(err, &already) {
		return already
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return &Error{Kind: KindCanceled, Err: err}
	}
	var exc *ch.Exception
	if errors.As(err, &exc) {
		return &Error{
			Kind:    kindForServerError(exc.Code, exc.Message),
			Code:    exc.Code,
			Message: truncate(exc.Message, 400),
			Err:     err,
		}
	}
	return &Error{Kind: KindUnavailable, Message: truncate(err.Error(), 400), Err: err}
}

func kindForServerError(code int32, message string) Kind {
	switch code {
	case codeAuthenticationFail, codeUnknownUser, codeWrongPassword, codeRequiredPassword:
		return KindAuthFailed
	case codeAccessDenied:
		return KindAccessDenied
	case codeNotEnoughSpace:
		return KindDiskFull
	}
	lower := strings.ToLower(message)
	if strings.Contains(lower, "too many sessions") ||
		strings.Contains(lower, "max_sessions_for_user") ||
		strings.Contains(lower, "is locked by a concurrent") ||
		strings.Contains(lower, "session is locked") ||
		strings.Contains(lower, "too many simultaneous") {
		return KindSessionBusy
	}
	if strings.Contains(lower, "not enough space") || strings.Contains(lower, "cannot reserve") {
		return KindDiskFull
	}
	return KindUnknown
}

func truncate(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

func KindOf(err error) Kind {
	if e := Classify(err); e != nil {
		return e.Kind
	}
	return KindUnknown
}
