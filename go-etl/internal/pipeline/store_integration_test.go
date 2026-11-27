//go:build integration

package pipeline

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"

	etlch "go-etl/internal/clickhouse"
)

func env(k, def string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v
	}
	return def
}

func openConn(t *testing.T, user, password, db string) ch.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := etlch.Open(ctx, etlch.RoleETLWriter, etlch.Options{
		Addr:        env("ETL_TEST_CH_ADDR", "127.0.0.1:9000"),
		Database:    db,
		User:        user,
		Password:    password,
		DialTimeout: 3 * time.Second,
		PingTimeout: 3 * time.Second,
	})
	if err != nil {
		t.Skipf("clickhouse is not available (%v); start it with: docker compose -f local/docker-compose.yml up -d", err)
	}
	return conn
}

func TestIntegrationPipelineProtocol(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	db := env("ETL_TEST_CH_DB", "openalex")
	admin := openConn(t, env("ETL_TEST_CH_ADMIN_USER", "default"), os.Getenv("ETL_TEST_CH_ADMIN_PASSWORD"), db)
	defer admin.Close()
	pipeConn := openConn(t, env("ETL_TEST_CH_PIPELINE_USER", "pipeline_writer"),
		env("ETL_TEST_CH_PIPELINE_PASSWORD", "dev-go-writer-password"), db)
	defer pipeConn.Close()
	dataConn := openConn(t, env("ETL_TEST_CH_USER", "etl_writer"),
		env("ETL_TEST_CH_PASSWORD", "dev-etl-writer-password"), db)
	defer dataConn.Close()

	reset := func() {
		for _, q := range []string{
			"TRUNCATE TABLE `" + db + "`.`pipeline_state`",
			"TRUNCATE TABLE `" + db + "`.`etl_checkpoints`",
			"INSERT INTO `" + db + "`.`pipeline_state` (id, state_revision, snapshot_date, data_updated_at, data_version, load_state, loading_snapshot_date, loading_manifest_id, instance_id, job_id, analysis_data_version, analysis_snapshot_date, analysis_completed_at) " +
				"VALUES (1, 0, NULL, NULL, 0, 'idle', NULL, '', '', '', 7, toDate('2026-09-01'), toDateTime64('2026-09-02 03:04:05.000006', 6, 'UTC'))",
		} {
			if err := admin.Exec(ctx, q); err != nil {
				t.Fatalf("reset: %v", err)
			}
		}
	}
	reset()

	store, err := NewStore(pipeConn, db)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	ckpt, err := NewCheckpointStore(dataConn, db)
	if err != nil {
		t.Fatalf("NewCheckpointStore: %v", err)
	}

	// 1. Initial row: idle, version 0, analysis data from "Python".
	st, err := store.Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if st.LoadState != LoadIdle || st.Revision != 0 || st.DataVersion != 0 {
		t.Fatalf("initial state = %+v, want idle revision 0", st)
	}
	if st.AnalysisDataVersion != 7 || st.AnalysisSnapshotDate == nil || st.AnalysisSnapshotDate.Format("2006-01-02") != "2026-09-01" {
		t.Fatalf("initial analysis fields = %+v, want version 7 and 2026-09-01", st)
	}

	// 2. Begin: loading, published metadata untouched, checkpoints empty.
	begin := BeginParams{
		InstanceID: "inst-test", JobID: "ing-1", ManifestID: "works-2026-10-14-abc",
		SnapshotDate: time.Date(2026, 10, 14, 0, 0, 0, 0, time.UTC),
	}
	st, err = store.BeginLoad(ctx, begin)
	if err != nil {
		t.Fatalf("BeginLoad: %v", err)
	}
	if st.LoadState != LoadLoading || st.Revision != 1 {
		t.Fatalf("begin state = %+v, want loading revision 1", st)
	}
	if st.Published() {
		t.Fatalf("a loading state must not count as published: %+v", st)
	}
	if st.DataVersion != 0 || st.SnapshotDate != nil {
		t.Fatalf("begin must not publish: %+v", st)
	}
	if st.LoadingSnapshotDate == nil || st.LoadingSnapshotDate.Format("2006-01-02") != "2026-10-14" {
		t.Fatalf("loading_snapshot_date = %v", st.LoadingSnapshotDate)
	}

	// A second instance must be refused while loading.
	if _, err := store.BeginLoad(ctx, BeginParams{
		InstanceID: "other-instance", JobID: "ing-2", ManifestID: "m2",
		SnapshotDate: time.Date(2027, 1, 13, 0, 0, 0, 0, time.UTC),
	}); !errors.Is(err, ErrLoadInProgress) {
		t.Fatalf("second BeginLoad error = %v, want ErrLoadInProgress", err)
	}

	// 3. Checkpoints: written by the data writer, read back with FINAL.
	if err := ckpt.MarkDone(ctx, "works", "s3://openalex/data/parquet/works/updated_date=2026-10-14/part_0000.parquet"); err != nil {
		t.Fatalf("MarkDone: %v", err)
	}
	if err := ckpt.MarkDone(ctx, "works", "s3://openalex/data/parquet/works/updated_date=2026-10-14/part_0001.parquet"); err != nil {
		t.Fatalf("MarkDone: %v", err)
	}
	done, err := ckpt.Load(ctx, "works")
	if err != nil {
		t.Fatalf("Load checkpoints: %v", err)
	}
	if len(done) != 2 {
		t.Fatalf("checkpoints = %v, want 2 marks", done)
	}
	if n, err := ckpt.Count(ctx, "works"); err != nil || n != 2 {
		t.Fatalf("Count = %d, %v; want 2", n, err)
	}

	// 4. Publish: idle, version+1, analysis fields preserved verbatim.
	now := time.Date(2026, 11, 27, 12, 0, 0, 0, time.UTC)
	st, err = store.Publish(ctx, PublishParams{
		InstanceID: "inst-test", JobID: "ing-1", ManifestID: begin.ManifestID,
		SnapshotDate: begin.SnapshotDate, DataChanged: true, Now: now,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if st.LoadState != LoadIdle || st.Revision != 2 || st.DataVersion != 1 {
		t.Fatalf("published state = %+v, want idle revision 2 version 1", st)
	}
	if st.SnapshotDate == nil || st.SnapshotDate.Format("2006-01-02") != "2026-10-14" {
		t.Fatalf("published snapshot_date = %v", st.SnapshotDate)
	}
	if st.DataUpdatedAt == nil || !st.DataUpdatedAt.Equal(now) {
		t.Fatalf("data_updated_at = %v, want %v", st.DataUpdatedAt, now)
	}
	if st.AnalysisDataVersion != 7 || st.AnalysisSnapshotDate == nil || st.AnalysisSnapshotDate.Format("2006-01-02") != "2026-09-01" {
		t.Fatalf("analysis fields lost on publish: %+v", st)
	}
	if !st.ManifestMatches(begin.ManifestID) {
		t.Fatalf("published manifest id = %q, want %q", st.ManifestID, begin.ManifestID)
	}

	// 5. A new load clears the previous ledger.
	if _, err := store.BeginLoad(ctx, BeginParams{
		InstanceID: "inst-test", JobID: "ing-2", ManifestID: "works-2027-01-13-def",
		SnapshotDate: time.Date(2027, 1, 13, 0, 0, 0, 0, time.UTC),
	}); err != nil {
		t.Fatalf("second BeginLoad: %v", err)
	}
	if n, err := ckpt.Count(ctx, "works"); err != nil || n != 0 {
		t.Fatalf("checkpoints after new load = %d, %v; want 0", n, err)
	}

	// 6. A lost race is detected: another writer takes the revision first.
	cur, err := store.Read(ctx)
	if err != nil {
		t.Fatalf("Read before race: %v", err)
	}
	race := "INSERT INTO `" + db + "`.`pipeline_state` (id, state_revision, snapshot_date, data_updated_at, data_version, load_state, loading_snapshot_date, loading_manifest_id, instance_id, job_id, analysis_data_version, analysis_snapshot_date, analysis_completed_at) " +
		"VALUES (1, ?, NULL, NULL, 0, 'loading', toDate('2027-01-13'), 'stolen', 'intruder', 'ing-x', 0, NULL, NULL)"
	if err := admin.Exec(ctx, race, cur.Revision+1); err != nil {
		t.Fatalf("competing insert: %v", err)
	}
	if _, err := store.Publish(ctx, PublishParams{
		InstanceID: "inst-test", JobID: "ing-2", ManifestID: "works-2027-01-13-def",
		SnapshotDate: time.Date(2027, 1, 13, 0, 0, 0, 0, time.UTC), DataChanged: true, Now: now,
	}); !errors.Is(err, ErrRevisionConflict) {
		t.Fatalf("Publish after race error = %v, want ErrRevisionConflict", err)
	}

	// 7. Publish without an active load is refused.
	reset()
	if _, err := store.Publish(ctx, PublishParams{
		InstanceID: "nobody", JobID: "none", DataChanged: false, Now: now,
	}); !errors.Is(err, ErrNotLoading) {
		t.Fatalf("Publish without load error = %v, want ErrNotLoading", err)
	}
}
