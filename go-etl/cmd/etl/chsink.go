package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go-etl/internal/clickhouse"
	"go-etl/internal/config"
	"go-etl/internal/knownids"
	"go-etl/internal/pipeline"
	"go-etl/internal/sink"
)

// stores is everything a process needs from ClickHouse: the write path plus the
// publication protocol. All of it is nil/Noop when no --config was given.
type stores struct {
	sink          sink.BatchSink
	pipeline      *pipeline.Store
	checkpoints   *pipeline.CheckpointStore
	buildKnownIDs func(ctx context.Context) (*knownids.Index, time.Duration, error)
	deleteWorks   func(ctx context.Context, ids []uint64) (int64, error)
	closeFn       func()
}

func (s *stores) Close() {
	if s.closeFn != nil {
		s.closeFn()
	}
}

// openStores builds the ClickHouse side of the process.
//
// No --config means diagnostic mode: NoopSink, no protocol, everything else
// unchanged, so the pre-ClickHouse workflows keep working.
//
// With --config two pools are opened: etl_writer for data and checkpoints, and
// pipeline_writer for the state machine (that user owns the TRUNCATE used when a
// new load starts).
func openStores(ctx context.Context, path string, logger *slog.Logger) (*stores, error) {
	cfg, err := config.Load(path)
	if errors.Is(err, config.ErrNoPath) {
		logger.Info("clickhouse: disabled, using diagnostic NoopSink")
		return &stores{sink: sink.NewNoopSink(), closeFn: func() {}}, nil
	}
	if err != nil {
		return nil, err
	}

	dataUser, dataPass, err := cfg.ETLWriterCredentials()
	if err != nil {
		return nil, err
	}
	pipeUser, pipePass, err := cfg.PipelineWriterCredentials()
	if err != nil {
		return nil, err
	}

	dialCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	dataConn, err := clickhouse.Open(dialCtx, clickhouse.RoleETLWriter, clickhouse.Options{
		Addr:     cfg.Addr(),
		Database: cfg.ClickHouse.Database,
		User:     dataUser,
		Password: dataPass.Value(),
		Secure:   cfg.ClickHouse.Secure,
	})
	if err != nil {
		return nil, fmt.Errorf("clickhouse: connect %s: %w", cfg.Redacted(), err)
	}

	pipeConn, err := clickhouse.Open(dialCtx, clickhouse.RolePipelineWriter, clickhouse.Options{
		Addr:     cfg.Addr(),
		Database: cfg.ClickHouse.Database,
		User:     pipeUser,
		Password: pipePass.Value(),
		Secure:   cfg.ClickHouse.Secure,
	})
	if err != nil {
		_ = dataConn.Close()
		return nil, fmt.Errorf("clickhouse: connect pipeline writer %s: %w", cfg.Redacted(), err)
	}

	snk, err := sink.NewClickHouseSink(dataConn, cfg.ClickHouse.Database)
	if err != nil {
		_ = dataConn.Close()
		_ = pipeConn.Close()
		return nil, err
	}
	store, err := pipeline.NewStore(pipeConn, cfg.ClickHouse.Database)
	if err != nil {
		_ = dataConn.Close()
		_ = pipeConn.Close()
		return nil, err
	}
	ckpt, err := pipeline.NewCheckpointStore(dataConn, cfg.ClickHouse.Database)
	if err != nil {
		_ = dataConn.Close()
		_ = pipeConn.Close()
		return nil, err
	}

	logger.Info("clickhouse: ready", "target", cfg.Redacted(),
		"data_user", dataUser, "pipeline_user", pipeUser)

	return &stores{
		sink:        snk,
		pipeline:    store,
		checkpoints: ckpt,
		deleteWorks: func(ctx context.Context, ids []uint64) (int64, error) {
			return pipeline.DeleteWorks(ctx, dataConn, cfg.ClickHouse.Database, ids)
		},
		buildKnownIDs: func(ctx context.Context) (*knownids.Index, time.Duration, error) {
			return knownids.Build(ctx, dataConn, cfg.ClickHouse.Database, "works")
		},
		closeFn: func() {
			if cerr := dataConn.Close(); cerr != nil {
				logger.Warn("clickhouse: close data pool", "err", cerr)
			}
			if cerr := pipeConn.Close(); cerr != nil {
				logger.Warn("clickhouse: close pipeline pool", "err", cerr)
			}
		},
	}, nil
}
