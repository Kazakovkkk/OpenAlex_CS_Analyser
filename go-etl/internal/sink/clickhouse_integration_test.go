//go:build integration

package sink

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"

	etlch "go-etl/internal/clickhouse"
)

type chEnv struct {
	addr           string
	adminUser      string
	adminPassword  string
	writerUser     string
	writerPassword string
	pipeUser       string
	pipePassword   string
	database       string
}

func envOr(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func loadCHEnv() chEnv {
	return chEnv{
		addr:           envOr("ETL_TEST_CH_ADDR", "127.0.0.1:9000"),
		adminUser:      envOr("ETL_TEST_CH_ADMIN_USER", "default"),
		adminPassword:  os.Getenv("ETL_TEST_CH_ADMIN_PASSWORD"),
		writerUser:     envOr("ETL_TEST_CH_USER", "etl_writer"),
		writerPassword: envOr("ETL_TEST_CH_PASSWORD", "dev-etl-writer-password"),
		pipeUser:       envOr("ETL_TEST_CH_PIPELINE_USER", "pipeline_writer"),
		pipePassword:   envOr("ETL_TEST_CH_PIPELINE_PASSWORD", "dev-go-writer-password"),
		database:       envOr("ETL_TEST_CH_DB", "openalex"),
	}
}

func openCH(ctx context.Context, e chEnv, role etlch.Role, user, pass, db string) (ch.Conn, error) {
	return etlch.Open(ctx, role, etlch.Options{
		Addr:        e.addr,
		Database:    db,
		User:        user,
		Password:    pass,
		DialTimeout: 3 * time.Second,
		PingTimeout: 3 * time.Second,
	})
}

func TestIntegrationClickHouseSink(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	env := loadCHEnv()

	boot, err := openCH(ctx, env, etlch.RoleETLWriter, env.adminUser, env.adminPassword, "default")
	if err != nil {
		t.Skipf("clickhouse is not available at %s (%v); start it with: docker compose -f local/docker-compose.yml up -d", env.addr, err)
	}
	if err := boot.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+env.database); err != nil {
		_ = boot.Close()
		t.Fatalf("create database: %v", err)
	}
	_ = boot.Close()

	admin, err := openCH(ctx, env, etlch.RoleETLWriter, env.adminUser, env.adminPassword, env.database)
	if err != nil {
		t.Fatalf("connect to %s: %v", env.database, err)
	}
	defer admin.Close()

	version := ""
	if err := admin.QueryRow(ctx, "SELECT version()").Scan(&version); err != nil {
		t.Fatalf("select version(): %v", err)
	}
	t.Logf("ClickHouse %s at %s, database %s", version, env.addr, env.database)

	repoRoot, err := filepath.Abs("../..")
	if err != nil {
		t.Fatalf("repo root: %v", err)
	}
	for _, file := range []string{"schema.sql", "schema_loadtest.sql"} {
		if err := applySQLFile(ctx, admin, filepath.Join(repoRoot, file)); err != nil {
			t.Fatalf("apply %s: %v", file, err)
		}
	}
	t.Log("schema.sql and schema_loadtest.sql applied")

	for _, table := range []string{"works", "dim_topics", "works_loadtest"} {
		if err := admin.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE `%s`.`%s`", env.database, table)); err != nil {
			t.Fatalf("truncate %s: %v", table, err)
		}
	}

	writerConn, err := openCH(ctx, env, etlch.RoleETLWriter, env.writerUser, env.writerPassword, env.database)
	if err != nil {
		t.Fatalf("connect as %s: %v", env.writerUser, err)
	}
	defer writerConn.Close()

	snk, err := NewClickHouseSink(writerConn, env.database)
	if err != nil {
		t.Fatalf("NewClickHouseSink: %v", err)
	}

	batch := buildSmokeWorks(100)
	if err := snk.WriteWorksBatch(ctx, batch); err != nil {
		t.Fatalf("WriteWorksBatch: %v", err)
	}

	topics := NewTopicsBatch(3)
	for i, name := range []string{"Machine learning", "Computer vision", "Information retrieval"} {
		topics.TopicIDs = append(topics.TopicIDs, uint32(10001+i))
		topics.DisplayNames = append(topics.DisplayNames, name)
		topics.SubfieldIDs = append(topics.SubfieldIDs, uint32(1702))
		topics.SubfieldNames = append(topics.SubfieldNames, "Artificial Intelligence")
	}
	if err := snk.WriteTopicsBatch(ctx, topics); err != nil {
		t.Fatalf("WriteTopicsBatch: %v", err)
	}

	assertCount(t, ctx, admin, env.database, "works", 0, 100)
	assertCount(t, ctx, admin, env.database, "dim_topics", 0, 3)

	var (
		pubDate  uint32
		hasFWCI  bool
		fwci     float64
		ids      []uint32
		scores   []float32
		rowYear  uint16
		rowField uint16
	)
	err = admin.QueryRow(ctx, fmt.Sprintf(`
		SELECT publication_date, has_fwci, fwci, topic_ids, topic_scores, publication_year, primary_field_id
		FROM `+"`%s`.`works`"+` FINAL WHERE work_id = ?`, env.database), uint64(1000002)).
		Scan(&pubDate, &hasFWCI, &fwci, &ids, &scores, &rowYear, &rowField)
	if err != nil {
		t.Fatalf("select sample row: %v", err)
	}
	if pubDate != 20260315 {
		t.Fatalf("publication_date = %d, want 20260315", pubDate)
	}
	if !hasFWCI || fwci != 1.5 {
		t.Fatalf("fwci = (%v, %v), want (1.5, true)", fwci, hasFWCI)
	}
	if len(ids) != len(scores) || len(ids) != 2 {
		t.Fatalf("topic arrays = %v / %v, want 2+2", ids, scores)
	}
	if scores[0] != 0 || scores[1] != 0.7 {
		t.Fatalf("topic_scores = %v, want [0 0.7] (invalid score stored as 0, position kept)", scores)
	}
	if rowYear != 2002 || rowField != 17 {
		t.Fatalf("year/field = %d/%d, want 2002/17", rowYear, rowField)
	}

	var zeroDateRows, emptyTopicRows uint64
	if err := admin.QueryRow(ctx, fmt.Sprintf(
		"SELECT countIf(publication_date = 0), countIf(empty(topic_ids)) FROM `%s`.`works` FINAL", env.database)).
		Scan(&zeroDateRows, &emptyTopicRows); err != nil {
		t.Fatalf("aggregate: %v", err)
	}
	if zeroDateRows == 0 || emptyTopicRows == 0 {
		t.Fatalf("expected some rows with publication_date=0 (%d) and empty topics (%d)", zeroDateRows, emptyTopicRows)
	}

	versions := NewWorksBatch(2)
	for i, at := range []time.Time{
		time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC),
	} {
		versions.WorkIDs = append(versions.WorkIDs, 999)
		versions.UpdatedDates = append(versions.UpdatedDates, at)
		versions.WorkTypes = append(versions.WorkTypes, []string{"preprint", "article"}[i])
		versions.PrimaryFieldIDs = append(versions.PrimaryFieldIDs, 17)
		versions.IsRetracted = append(versions.IsRetracted, 0)
		versions.PubYears = append(versions.PubYears, 2020)
		versions.PubDates = append(versions.PubDates, nil)
		versions.PrimaryTopicIDs = append(versions.PrimaryTopicIDs, 0)
		versions.TopicIDs = append(versions.TopicIDs, nil)
		versions.TopicScores = append(versions.TopicScores, nil)
		versions.PrimarySourceIDs = append(versions.PrimarySourceIDs, 0)
		versions.PrimarySourceTypes = append(versions.PrimarySourceTypes, "")
		versions.FWCIs = append(versions.FWCIs, nil)
	}
	unrelated := buildSmokeWorks(1)
	unrelated.WorkIDs[0] = 1000
	if err := snk.WriteWorksBatch(ctx, versions); err != nil {
		t.Fatalf("WriteWorksBatch(versions): %v", err)
	}
	if err := snk.WriteWorksBatch(ctx, unrelated); err != nil {
		t.Fatalf("WriteWorksBatch(unrelated): %v", err)
	}

	var latestType string
	if err := admin.QueryRow(ctx, fmt.Sprintf(
		"SELECT type FROM `%s`.`works` FINAL WHERE work_id = 999", env.database)).Scan(&latestType); err != nil {
		t.Fatalf("select latest version: %v", err)
	}
	if latestType != "article" {
		t.Fatalf("FINAL type = %q, want the newer version %q", latestType, "article")
	}

	if err := admin.Exec(ctx, fmt.Sprintf("DELETE FROM `%s`.`works` WHERE work_id = 999", env.database)); err != nil {
		t.Fatalf("lightweight DELETE: %v", err)
	}
	assertCount(t, ctx, admin, env.database, "works", 999, 0)
	assertCount(t, ctx, admin, env.database, "works", 1000, 1)
	t.Log("lightweight DELETE hides every version of a work and leaves others intact")

	loadSink, err := NewClickHouseSink(writerConn, env.database, WithTables("works_loadtest", "dim_topics"))
	if err != nil {
		t.Fatalf("loadtest sink: %v", err)
	}
	if err := loadSink.WriteWorksBatch(ctx, buildSmokeWorks(5)); err != nil {
		t.Fatalf("write works_loadtest: %v", err)
	}
	assertRawCount(t, ctx, admin, env.database, "works_loadtest", 0, 5)
}

func TestIntegrationPipelineWriterSessionLimit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	env := loadCHEnv()
	first, err := openCH(ctx, env, etlch.RolePipelineWriter, env.pipeUser, env.pipePassword, "default")
	if err != nil {
		t.Skipf("clickhouse is not available at %s (%v)", env.addr, err)
	}
	defer first.Close()

	second, err := openCH(ctx, env, etlch.RolePipelineWriter, env.pipeUser, env.pipePassword, "default")
	if err == nil {
		_ = second.Close()
		t.Skipf("max_sessions_for_user is not enforced on this server for native TCP: "+
			"second session as %q succeeded; the classifier is still covered by unit tests", env.pipeUser)
	}
	cls := etlch.Classify(err)
	t.Logf("second session rejected: kind=%q code=%d message=%q", cls.Kind, cls.Code, cls.Message)
	if cls.Kind != etlch.KindSessionBusy {
		t.Fatalf("second session classified as %q, want %q (code %d, message %q)",
			cls.Kind, etlch.KindSessionBusy, cls.Code, cls.Message)
	}
}

func buildSmokeWorks(n int) *WorksBatch {
	b := NewWorksBatch(n)
	for i := range n {
		id := uint64(1000000 + i)
		b.WorkIDs = append(b.WorkIDs, id)
		b.UpdatedDates = append(b.UpdatedDates, time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC).Add(time.Duration(i)*time.Second))
		b.WorkTypes = append(b.WorkTypes, []string{"article", "preprint", "conference-paper"}[i%3])
		b.PrimaryFieldIDs = append(b.PrimaryFieldIDs, 17)
		if i%50 == 0 {
			b.IsRetracted = append(b.IsRetracted, 1)
		} else {
			b.IsRetracted = append(b.IsRetracted, 0)
		}
		b.PubYears = append(b.PubYears, uint16(2000+i%25))
		if i%7 == 0 {
			b.PubDates = append(b.PubDates, nil)
		} else {
			b.PubDates = append(b.PubDates, b.AllocPubDate(time.Date(2026, 3, 15, 0, 0, 0, 0, time.UTC)))
		}

		b.PrimaryTopicIDs = append(b.PrimaryTopicIDs, uint32(10001+i%3))
		if i%5 == 0 {
			b.TopicIDs = append(b.TopicIDs, nil)
			b.TopicScores = append(b.TopicScores, nil)
		} else {
			b.TopicIDs = append(b.TopicIDs, []uint32{uint32(10001 + i%3), uint32(10004 + i%3)})
			b.TopicScores = append(b.TopicScores, []float32{0, 0.7})
		}

		b.PrimarySourceIDs = append(b.PrimarySourceIDs, uint64(5000+i))
		b.PrimarySourceTypes = append(b.PrimarySourceTypes, "journal")

		switch i % 3 {
		case 0:
			b.FWCIs = append(b.FWCIs, nil)
		case 1:
			b.FWCIs = append(b.FWCIs, b.AllocFWCI(0))
		default:
			b.FWCIs = append(b.FWCIs, b.AllocFWCI(1.5))
		}
	}
	return b
}

func assertCount(t *testing.T, ctx context.Context, conn ch.Conn, db, table string, workID uint64, want uint64) {
	t.Helper()
	countRows(t, ctx, conn, db, table, workID, want, true)
}

func assertRawCount(t *testing.T, ctx context.Context, conn ch.Conn, db, table string, workID uint64, want uint64) {
	t.Helper()
	countRows(t, ctx, conn, db, table, workID, want, false)
}

func countRows(t *testing.T, ctx context.Context, conn ch.Conn, db, table string, workID uint64, want uint64, final bool) {
	t.Helper()
	query := fmt.Sprintf("SELECT count() FROM `%s`.`%s`", db, table)
	if final {
		query += " FINAL"
	}
	var args []any
	if workID != 0 {
		query += " WHERE work_id = ?"
		args = append(args, workID)
	}
	var got uint64
	if err := conn.QueryRow(ctx, query, args...).Scan(&got); err != nil {
		t.Fatalf("count %s (work_id=%d): %v", table, workID, err)
	}
	if got != want {
		t.Fatalf("count %s (work_id=%d) = %d, want %d", table, workID, got, want)
	}
}

func applySQLFile(ctx context.Context, conn ch.Conn, path string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var cleaned strings.Builder
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "--") {
			continue
		}
		cleaned.WriteString(line)
		cleaned.WriteByte('\n')
	}
	for _, stmt := range strings.Split(cleaned.String(), ";") {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		upper := strings.ToUpper(stmt)
		if strings.HasPrefix(upper, "CREATE DATABASE") || strings.HasPrefix(upper, "USE ") {
			continue
		}
		if err := conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("statement %q: %w", firstLine(stmt), err)
		}
	}
	return nil
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	if len(s) > 120 {
		return s[:120]
	}
	return s
}
