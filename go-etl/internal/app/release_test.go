package app

import (
	"testing"
	"time"

	"go-etl/internal/adapters/s3"
	"go-etl/internal/pipeline"
)

func testManifest(entity, date string, files ...s3.ManifestEntry) *s3.Manifest {
	return &s3.Manifest{Entity: entity, Format: "parquet", Date: date, Files: files}
}

func entry(url string, size int64) s3.ManifestEntry {
	return s3.ManifestEntry{URL: url, Meta: s3.ManifestMeta{ContentLength: size}}
}

func testRelease() *Release {
	works := testManifest("works", "2026-11-07",
		entry("s3://b/works/updated_date=2026-11-01/part_0.parquet", 10),
		entry("s3://b/works/updated_date=2026-11-07/part_1.parquet", 20),
	)
	topics := testManifest("topics", "2026-11-07",
		entry("s3://b/topics/part_0.parquet", 30),
	)
	return &Release{Works: works, Topics: topics, Date: time.Date(2026, 11, 7, 0, 0, 0, 0, time.UTC),
		ID: computeReleaseID(works, topics)}
}

func TestReleaseIDIsReproducibleAndOrderIndependent(t *testing.T) {
	t.Parallel()

	id1 := testRelease().ID
	rel := testRelease()
	// Reversing the file order must not change the identity; the original order
	// is only used for sharding.
	rel.Works.Files[0], rel.Works.Files[1] = rel.Works.Files[1], rel.Works.Files[0]
	if got := computeReleaseID(rel.Works, rel.Topics); got != id1 {
		t.Fatalf("release id depends on file order: %s != %s", got, id1)
	}

	rel2 := testRelease()
	rel2.Works.Files[0].Meta.ContentLength++
	if got := computeReleaseID(rel2.Works, rel2.Topics); got == id1 {
		t.Fatal("release id must change with content_length")
	}
	if len(id1) != 64 {
		t.Fatalf("release id length = %d, want 64 hex chars", len(id1))
	}
}

func TestBuildLoadPlanFullAndIncremental(t *testing.T) {
	t.Parallel()

	rel := testRelease()
	full, err := buildLoadPlan(rel, nil, pipeline.PhaseFull, 0, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(full.Works) != 2 || len(full.Topics) != 1 || full.Deletions {
		t.Fatalf("full plan = %+v", full)
	}

	published := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	inc, err := buildLoadPlan(rel, &published, pipeline.PhaseIncremental, 0, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(inc.Works) != 1 || inc.Works[0].URL != rel.Works.Files[1].URL {
		t.Fatalf("incremental works = %+v", inc.Works)
	}
	if len(inc.Topics) != 1 || !inc.Deletions {
		t.Fatalf("Topics and deletions must be part of an incremental plan: %+v", inc)
	}

	// No new Works files is not the end: the plan still has Topics/deletions.
	same, err := buildLoadPlan(rel, &rel.Date, pipeline.PhaseIncremental, 0, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(same.Works) != 0 || len(same.Topics) != 1 || !same.Deletions {
		t.Fatalf("same release plan = %+v", same)
	}
}

func TestFirstTransitionFrom1410IncludesThatDay(t *testing.T) {
	t.Parallel()

	published := time.Date(2026, 10, 14, 0, 0, 0, 0, time.UTC)
	release := time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)

	entries := []s3.ManifestEntry{
		entry("s3://b/works/updated_date=2026-10-07/part_0.parquet", 10),
		entry("s3://b/works/updated_date=2026-10-14/part_1.parquet", 10),
		entry("s3://b/works/updated_date=2026-10-21/part_2.parquet", 10),
	}
	got, err := filterWorksSince(entries, published, release)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("first transition selected %d files, want 2 (14.10 and 21.10)", len(got))
	}

	// A later transition does not include the old release day again.
	later, err := filterWorksSince(entries, release, release.AddDate(0, 0, 7))
	if err != nil {
		t.Fatal(err)
	}
	if len(later) != 0 {
		t.Fatalf("later transition selected %+v, want none before 2026-10-28", later)
	}
}

func TestManifestValidationRejectsDuplicateKeys(t *testing.T) {
	t.Parallel()

	m := testManifest("works", "2026-11-07",
		entry("s3://b/works/part_0.parquet", 10),
		entry("s3://b/works/part_0.parquet", 10),
	)
	if err := m.Validate("works"); err == nil {
		t.Fatal("duplicate file key must be rejected")
	}
	if err := testManifest("topics", "2026-11-07").Validate("topics"); err == nil {
		t.Fatal("empty file list must be rejected")
	}
}
