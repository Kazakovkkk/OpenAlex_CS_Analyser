//go:build integration

package knownids

import (
	"context"
	"os"
	"testing"
	"time"

	etlch "go-etl/internal/clickhouse"
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func TestIntegrationBuild(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	db := envOr("ETL_TEST_CH_DB", "openalex")
	conn, err := etlch.Open(ctx, etlch.RoleETLWriter, etlch.Options{
		Addr:        envOr("ETL_TEST_CH_ADDR", "127.0.0.1:9000"),
		Database:    db,
		User:        envOr("ETL_TEST_CH_USER", "etl_writer"),
		Password:    envOr("ETL_TEST_CH_PASSWORD", "dev-etl-writer-password"),
		DialTimeout: 3 * time.Second,
		PingTimeout: 3 * time.Second,
	})
	if err != nil {
		t.Skipf("clickhouse is not available (%v); start it with: docker compose -f local/docker-compose.yml up -d", err)
	}
	defer conn.Close()

	idx, dur, err := Build(ctx, conn, db, "works")
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	t.Logf("indexed %d work ids in %s, %d KiB in memory",
		idx.Len(), dur.Round(time.Millisecond), idx.SizeInBytes()/1024)

	var maxID uint64
	if err := conn.QueryRow(ctx, "SELECT max(work_id) FROM `"+db+"`.`works`").Scan(&maxID); err != nil {
		t.Fatalf("max(work_id): %v", err)
	}
	if maxID > 0 && !idx.Contains(maxID) {
		t.Fatalf("index misses work_id %d", maxID)
	}
	if idx.Contains(maxID + 1) {
		t.Fatalf("index reports a foreign id %d", maxID+1)
	}
}
