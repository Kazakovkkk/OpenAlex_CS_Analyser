//go:build integration

package pipeline

import (
	"context"
	"os"
	"testing"
	"time"

	"go-etl/internal/sink"
)

func TestIntegrationDeleteWorks(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	db := env("ETL_TEST_CH_DB", "openalex")
	admin := openConn(t, env("ETL_TEST_CH_ADMIN_USER", "default"), os.Getenv("ETL_TEST_CH_ADMIN_PASSWORD"), db)
	defer admin.Close()
	dataConn := openConn(t, env("ETL_TEST_CH_USER", "etl_writer"),
		env("ETL_TEST_CH_PASSWORD", "dev-etl-writer-password"), db)
	defer dataConn.Close()

	if err := admin.Exec(ctx, "TRUNCATE TABLE `"+db+"`.`works`"); err != nil {
		t.Fatalf("truncate works: %v", err)
	}

	snk, err := sink.NewClickHouseSink(dataConn, db)
	if err != nil {
		t.Fatalf("sink: %v", err)
	}

	// Two versions of the deleted id plus an unrelated work (spec R1).
	b := sink.NewWorksBatch(3)
	appendRow := func(id uint64, at time.Time, kind string) {
		b.WorkIDs = append(b.WorkIDs, id)
		b.UpdatedDates = append(b.UpdatedDates, at)
		b.WorkTypes = append(b.WorkTypes, kind)
		b.PrimaryFieldIDs = append(b.PrimaryFieldIDs, 17)
		b.IsRetracted = append(b.IsRetracted, 0)
		b.PubYears = append(b.PubYears, 2020)
		b.PubDates = append(b.PubDates, nil)
		b.PrimaryTopicIDs = append(b.PrimaryTopicIDs, 0)
		b.TopicIDs = append(b.TopicIDs, nil)
		b.TopicScores = append(b.TopicScores, nil)
		b.PrimarySourceIDs = append(b.PrimarySourceIDs, 0)
		b.PrimarySourceTypes = append(b.PrimarySourceTypes, "")
		b.FWCIs = append(b.FWCIs, nil)
	}
	appendRow(777, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), "preprint")
	appendRow(777, time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC), "article")
	appendRow(778, time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC), "article")
	if err := snk.WriteWorksBatch(ctx, b); err != nil {
		t.Fatalf("write works: %v", err)
	}

	deleted, err := DeleteWorks(ctx, dataConn, db, []uint64{777, 999})
	if err != nil {
		t.Fatalf("DeleteWorks: %v", err)
	}
	if deleted != 2 {
		t.Fatalf("deleted = %d, want 2 ids", deleted)
	}

	var left777, left778 uint64
	if err := admin.QueryRow(ctx,
		"SELECT count() FROM `"+db+"`.`works` WHERE work_id = 777").Scan(&left777); err != nil {
		t.Fatalf("count 777: %v", err)
	}
	if err := admin.QueryRow(ctx,
		"SELECT count() FROM `"+db+"`.`works` WHERE work_id = 778").Scan(&left778); err != nil {
		t.Fatalf("count 778: %v", err)
	}
	if left777 != 0 {
		t.Fatalf("work 777 still readable: %d rows", left777)
	}
	if left778 != 1 {
		t.Fatalf("neighbour work rows = %d, want 1", left778)
	}
	t.Logf("deleted both versions of 777 and left 778 intact")
}
