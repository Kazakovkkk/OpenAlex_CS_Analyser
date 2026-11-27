package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go-etl/internal/clickhouse"
	"go-etl/internal/config"
	"go-etl/internal/sink"
)

// openSink builds the write path for one process.
//
// No --config means diagnostic mode: NoopSink, everything else unchanged. That
// keeps the pre-ClickHouse workflows (S3 diagnostics, benchmarks, load tests on
// the ring) working without a database.
//
// With --config the process opens the etl_writer pool and inserts every batch
// synchronously. The pool is closed by the returned function, which the caller
// must defer.
func openSink(ctx context.Context, path string, logger *slog.Logger) (sink.BatchSink, func(), error) {
	cfg, err := config.Load(path)
	if errors.Is(err, config.ErrNoPath) {
		logger.Info("clickhouse: disabled, using diagnostic NoopSink")
		return sink.NewNoopSink(), func() {}, nil
	}
	if err != nil {
		return nil, nil, err
	}

	user, password, err := cfg.ETLWriterCredentials()
	if err != nil {
		return nil, nil, err
	}

	dialCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	conn, err := clickhouse.Open(dialCtx, clickhouse.RoleETLWriter, clickhouse.Options{
		Addr:     cfg.Addr(),
		Database: cfg.ClickHouse.Database,
		User:     user,
		Password: password.Value(),
		Secure:   cfg.ClickHouse.Secure,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("clickhouse: connect %s: %w", cfg.Redacted(), err)
	}

	snk, err := sink.NewClickHouseSink(conn, cfg.ClickHouse.Database)
	if err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	logger.Info("clickhouse: sink enabled", "target", cfg.Redacted(), "user", user)

	return snk, func() {
		if cerr := conn.Close(); cerr != nil {
			logger.Warn("clickhouse: close", "err", cerr)
		}
	}, nil
}
