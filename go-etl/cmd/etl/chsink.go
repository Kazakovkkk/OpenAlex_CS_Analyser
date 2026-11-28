package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"go-etl/internal/clickhouse"
	"go-etl/internal/config"
	"go-etl/internal/knownids"
	"go-etl/internal/pipeline"
	"go-etl/internal/sink"
)

// stores is everything a process needs from ClickHouse: the write path plus the
// publication protocol. All of it is nil/Noop in an explicit dry run.
type stores struct {
	sink           sink.BatchSink
	loadtestSink   sink.BatchSink
	pipeline       *pipeline.Store
	checkpoints    *pipeline.CheckpointStore
	buildKnownIDs  func(ctx context.Context) (*knownids.Index, time.Duration, error)
	deleteWorks    func(ctx context.Context, ids []uint64) (int64, error)
	closeFn        func()

	targetHost string
	targetPort int
	testTarget bool
}

func (s *stores) Close() {
	if s.closeFn != nil {
		s.closeFn()
	}
}

// openStores builds the ClickHouse side of the process.
//
// A working config is mandatory: a missing --config is a startup error, not a
// silent NoopSink. The diagnostic sink is available only through the explicit
// --dry-run flag or in tests (spec step 1.1).
//
// Two handles are opened: a pool for etl_writer (data, checkpoints, known ids,
// deletions) and a lazy database/sql handle for pipeline_writer. The management
// handle does not connect on startup, so an idle Serve leaves the single
// management slot free for Python.
func openStores(ctx context.Context, path string, logger *slog.Logger, dryRun bool) (*stores, error) {
	if dryRun {
		logger.Info("clickhouse: dry run, using NoopSink")
		return &stores{sink: sink.NewNoopSink(), closeFn: func() {}}, nil
	}
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("config path is required for a working run (use --dry-run for the diagnostic NoopSink)")
	}
	cfg, err := config.Load(path)
	if errors.Is(err, config.ErrNoPath) {
		return nil, errors.New("config path is required for a working run (use --dry-run for the diagnostic NoopSink)")
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

	// Lazy: no connection is opened until the first transition.
	pipeDB, err := clickhouse.OpenSQL(clickhouse.RolePipelineWriter, clickhouse.Options{
		Addr:     cfg.Addr(),
		Database: cfg.ClickHouse.Database,
		User:     pipeUser,
		Password: pipePass.Value(),
		Secure:   cfg.ClickHouse.Secure,
	})
	if err != nil {
		_ = dataConn.Close()
		return nil, fmt.Errorf("clickhouse: management handle %s: %w", cfg.Redacted(), err)
	}
	slot, err := pipeline.NewSlot(pipeDB)
	if err != nil {
		_ = dataConn.Close()
		_ = pipeDB.Close()
		return nil, err
	}

	snk, err := sink.NewClickHouseSink(dataConn, cfg.ClickHouse.Database)
	if err != nil {
		_ = dataConn.Close()
		_ = pipeDB.Close()
		return nil, err
	}
	loadtestSink, err := sink.NewClickHouseSink(dataConn, cfg.ClickHouse.Database,
		sink.WithTables("works_loadtest", "dim_topics"))
	if err != nil {
		_ = dataConn.Close()
		_ = pipeDB.Close()
		return nil, err
	}
	store, err := pipeline.NewStore(slot, cfg.ClickHouse.Database)
	if err != nil {
		_ = dataConn.Close()
		_ = pipeDB.Close()
		return nil, err
	}
	ckpt, err := pipeline.NewCheckpointStore(dataConn, cfg.ClickHouse.Database)
	if err != nil {
		_ = dataConn.Close()
		_ = pipeDB.Close()
		return nil, err
	}

	logger.Info("clickhouse: ready", "target", cfg.Redacted(),
		"data_user", dataUser, "pipeline_user", pipeUser)

	return &stores{
		sink:         snk,
		loadtestSink: loadtestSink,
		pipeline:     store,
		checkpoints:  ckpt,
		targetHost:   cfg.ClickHouse.Host,
		targetPort:   cfg.ClickHouse.Port,
		testTarget:   cfg.GoETL.TestTarget,
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
			if cerr := pipeDB.Close(); cerr != nil {
				logger.Warn("clickhouse: close management handle", "err", cerr)
			}
		},
	}, nil
}
