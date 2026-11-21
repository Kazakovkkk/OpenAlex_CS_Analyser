package s3

import "testing"

func TestParseS3URL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		raw        string
		wantBucket string
		wantKey    string
		wantErr    bool
	}{
		{
			name:       "valid openalex parquet url",
			raw:        "s3://openalex/data/parquet/works/updated_date=2026-10-14/part_000.parquet",
			wantBucket: "openalex",
			wantKey:    "data/parquet/works/updated_date=2026-10-14/part_000.parquet",
		},
		{
			name:       "trims extra leading slashes in key",
			raw:        "s3://openalex///data/parquet/topics/part_000.parquet",
			wantBucket: "openalex",
			wantKey:    "data/parquet/topics/part_000.parquet",
		},
		{
			name:    "missing scheme",
			raw:     "openalex/data/parquet/works/part_000.parquet",
			wantErr: true,
		},
		{
			name:    "missing key",
			raw:     "s3://openalex/",
			wantErr: true,
		},
		{
			name:    "missing bucket",
			raw:     "s3:///data/file.parquet",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			bucket, key, err := ParseS3URL(tc.raw)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ParseS3URL(%q) succeeded (%q, %q), expected error", tc.raw, bucket, key)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseS3URL(%q) unexpected error: %v", tc.raw, err)
			}
			if bucket != tc.wantBucket || key != tc.wantKey {
				t.Fatalf("ParseS3URL(%q) = (%q, %q), want (%q, %q)", tc.raw, bucket, key, tc.wantBucket, tc.wantKey)
			}
		})
	}
}

func TestParseManifestAndSelectEntries(t *testing.T) {
	t.Parallel()

	raw := []byte(`{
		"date": "2026-10-14",
		"format": "parquet",
		"entity": "works",
		"record_count": 150,
		"content_length": 1500,
		"files": [
			{"url": "s3://openalex/data/parquet/works/p0.parquet", "meta": {"content_length": 100, "record_count": 10}},
			{"url": "s3://openalex/data/parquet/works/p1.parquet", "meta": {"content_length": 200, "record_count": 20}},
			{"url": "s3://openalex/data/parquet/works/p2.parquet", "meta": {"content_length": 300, "record_count": 30}},
			{"url": "s3://openalex/data/parquet/works/p3.parquet", "meta": {"content_length": 400, "record_count": 40}},
			{"url": "s3://openalex/data/parquet/works/p4.parquet", "meta": {"content_length": 500, "record_count": 50}}
		]
	}`)

	m, err := ParseManifest(raw)
	if err != nil {
		t.Fatalf("ParseManifest error: %v", err)
	}
	if m.Date != "2026-10-14" || m.Format != "parquet" || m.Entity != "works" {
		t.Fatalf("unexpected manifest header: %+v", m)
	}
	if len(m.Files) != 5 || m.ContentLength != 1500 || m.RecordCount != 150 {
		t.Fatalf("unexpected manifest totals: %+v", m)
	}

	seen := make(map[string]int)
	for shard := range 3 {
		part, err := m.SelectEntries(shard, 3, 0)
		if err != nil {
			t.Fatalf("SelectEntries(%d, 3, 0) error: %v", shard, err)
		}
		for _, e := range part {
			seen[e.URL]++
		}
	}
	if len(seen) != 5 {
		t.Fatalf("shards covered %d unique entries, want 5", len(seen))
	}
	for url, cnt := range seen {
		if cnt != 1 {
			t.Fatalf("entry %s seen %d times across shards, want 1", url, cnt)
		}
	}

	capped, err := m.SelectEntries(0, 1, 2)
	if err != nil {
		t.Fatalf("SelectEntries cap error: %v", err)
	}
	if len(capped) != 2 {
		t.Fatalf("got %d entries with maxFiles=2, want 2", len(capped))
	}

	if _, err := m.SelectEntries(0, 0, 0); err == nil {
		t.Fatal("expected error for shardTotal=0")
	}
	if _, err := m.SelectEntries(2, 2, 0); err == nil {
		t.Fatal("expected error for shardIndex == shardTotal")
	}
	if _, err := m.SelectEntries(0, 1, -1); err == nil {
		t.Fatal("expected error for negative maxFiles")
	}
}

func TestComputeSizeStatsAndPickClosest(t *testing.T) {
	t.Parallel()

	const mb = 1 << 20
	entries := []ManifestEntry{
		{URL: "s3://openalex/f1", Meta: ManifestMeta{ContentLength: 2 * mb, RecordCount: 100}},
		{URL: "s3://openalex/f2", Meta: ManifestMeta{ContentLength: 10 * mb, RecordCount: 500}},
		{URL: "s3://openalex/f3", Meta: ManifestMeta{ContentLength: 18 * mb, RecordCount: 900}},
		{URL: "s3://openalex/f4", Meta: ManifestMeta{ContentLength: 120 * mb, RecordCount: 6000}},
	}

	stats := ComputeSizeStats(entries)
	if stats.FileCount != 4 {
		t.Fatalf("FileCount = %d, want 4", stats.FileCount)
	}
	if stats.TotalBytes != 150*mb {
		t.Fatalf("TotalBytes = %d, want %d", stats.TotalBytes, 150*mb)
	}
	if stats.TotalRecords != 7500 {
		t.Fatalf("TotalRecords = %d, want 7500", stats.TotalRecords)
	}
	if stats.MinBytes != 2*mb || stats.MaxBytes != 120*mb {
		t.Fatalf("min/max = (%d, %d), want (%d, %d)", stats.MinBytes, stats.MaxBytes, 2*mb, 120*mb)
	}
	if stats.Under15MBCount != 2 || stats.Under20MBCount != 3 {
		t.Fatalf("under thresholds = (%d, %d), want (2, 3)", stats.Under15MBCount, stats.Under20MBCount)
	}

	picked := PickClosestBySize(entries, []int64{3 * mb, 100 * mb})
	if len(picked) != 2 || picked[0].URL != "s3://openalex/f1" || picked[1].URL != "s3://openalex/f4" {
		t.Fatalf("unexpected PickClosestBySize result: %+v", picked)
	}
}

func TestExtractPartition(t *testing.T) {
	t.Parallel()

	got := ExtractPartition("s3://openalex/data/parquet/works/updated_date=2026-10-14/part_000.parquet")
	if got != "updated_date=2026-10-14" {
		t.Fatalf("ExtractPartition = %q, want updated_date=2026-10-14", got)
	}
	if got := ExtractPartition("s3://openalex/data/parquet/topics/part_000.parquet"); got != "" {
		t.Fatalf("ExtractPartition(no partition) = %q, want empty", got)
	}
}
