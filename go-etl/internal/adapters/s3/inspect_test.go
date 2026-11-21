//go:build inspect

package s3

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestInspectManifests(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	client, err := NewAnonymousClient(ctx)
	if err != nil {
		t.Fatalf("NewAnonymousClient: %v", err)
	}

	entities := []string{"works", "topics", "institutions"}
	var worksParquetManifest *Manifest

	for _, entity := range entities {
		key := ParquetManifestKey(entity)
		raw, err := client.ReadAll(ctx, DefaultBucket, key, MaxManifestBytes)
		if err != nil {
			t.Fatalf("read %s: %v", key, err)
		}

		previewLen := min(len(raw), 500)
		t.Logf("\n=== %s (%d bytes raw) ===\nRaw snippet:\n%s\n", key, len(raw), string(raw[:previewLen]))

		m, err := ParseManifest(raw)
		if err != nil {
			t.Fatalf("parse %s: %v", key, err)
		}
		if entity == "works" {
			worksParquetManifest = m
		}

		st := ComputeSizeStats(m.Files)
		if st.FileCount == 0 {
			t.Fatalf("parsed 0 files from %s — check manifest JSON schema", key)
		}
		t.Logf("Header  [%s]: date=%s | format=%s | entity=%s", entity, m.Date, m.Format, m.Entity)
		t.Logf("Summary [%s]: files=%d | total=%s (%d bytes) | records=%d",
			entity, st.FileCount, formatBytes(st.TotalBytes), st.TotalBytes, st.TotalRecords)
		t.Logf("Totals  [%s]: content_length=%d (matches sum: %v) | record_count=%d (matches sum: %v)",
			entity,
			m.ContentLength, m.ContentLength == st.TotalBytes,
			m.RecordCount, m.RecordCount == st.TotalRecords,
		)
		t.Logf("Sizes [%s]: min=%s | p25=%s | median=%s | p75=%s | p95=%s | max=%s",
			entity,
			formatBytes(st.MinBytes), formatBytes(st.P25Bytes), formatBytes(st.MedianBytes),
			formatBytes(st.P75Bytes), formatBytes(st.P95Bytes), formatBytes(st.MaxBytes),
		)
		if st.FileCount > 0 {
			t.Logf("Small files [%s]: <15 MiB: %d (%.1f%%) | <20 MiB: %d (%.1f%%)",
				entity,
				st.Under15MBCount, 100*float64(st.Under15MBCount)/float64(st.FileCount),
				st.Under20MBCount, 100*float64(st.Under20MBCount)/float64(st.FileCount),
			)
		}
	}

	if worksParquetManifest != nil {
		const mb = 1 << 20
		targets := []int64{2 * mb, 5 * mb, 15 * mb, 50 * mb, 150 * mb, 400 * mb}
		picked := PickClosestBySize(worksParquetManifest.Files, targets)
		t.Log("\n=== Candidate works/ Parquet files for Benchmark #3 (2/5/15/50/150/400 MiB) ===")
		for i, e := range picked {
			t.Logf("  target=%3d MiB -> actual=%8s | records=%8d | %s",
				targets[i]/mb, formatBytes(e.Meta.ContentLength), e.Meta.RecordCount, e.URL)
		}
	}

	t.Log("\n=== Probing JSONL works/ layout for Benchmark #3 ===")
	jsonlCandidates := []string{
		"data/jsonl/works/manifest.json",
		"data/jsonl/works/manifest",
		"data/works/manifest",
	}
	var jsonlManifest *Manifest
	for _, key := range jsonlCandidates {
		raw, err := client.ReadAll(ctx, DefaultBucket, key, MaxManifestBytes)
		if err != nil {
			t.Logf("  %s -> not available (%v)", key, err)
			continue
		}
		m, err := ParseManifest(raw)
		if err != nil {
			t.Logf("  %s -> downloaded (%d bytes), parse error: %v", key, len(raw), err)
			continue
		}
		preview := min(len(raw), 350)
		st := ComputeSizeStats(m.Files)
		t.Logf("  FOUND %s: date=%s | format=%s | files=%d | total=%s | records=%d",
			key, m.Date, m.Format, st.FileCount, formatBytes(st.TotalBytes), st.TotalRecords)
		t.Logf("  JSONL raw snippet:\n%s", string(raw[:preview]))
		if len(m.Files) > 0 {
			t.Logf("  sample JSONL entry URL: %s", m.Files[0].URL)
		}
		jsonlManifest = m
		break
	}

	if jsonlManifest == nil {
		keys, err := client.ListKeys(ctx, "data/jsonl/", 10)
		if err != nil {
			t.Logf("  ListKeys(data/jsonl/) error: %v", err)
		} else {
			t.Logf("  First keys under data/jsonl/: %v", keys)
		}
	} else if worksParquetManifest != nil {
		parquetParts := make(map[string]struct{}, len(worksParquetManifest.Files))
		for _, e := range worksParquetManifest.Files {
			if p := ExtractPartition(e.URL); p != "" {
				parquetParts[p] = struct{}{}
			}
		}
		overlap := 0
		seenJSONL := make(map[string]struct{})
		for _, e := range jsonlManifest.Files {
			p := ExtractPartition(e.URL)
			if p == "" {
				continue
			}
			if _, dup := seenJSONL[p]; dup {
				continue
			}
			seenJSONL[p] = struct{}{}
			if _, ok := parquetParts[p]; ok {
				overlap++
			}
		}
		t.Logf("  Partition overlap (updated_date=...): parquet=%d unique, jsonl=%d unique, shared=%d",
			len(parquetParts), len(seenJSONL), overlap)
	}
}

func formatBytes(b int64) string {
	const (
		kiB = 1 << 10
		miB = 1 << 20
		giB = 1 << 30
	)
	switch {
	case b >= giB:
		return fmt.Sprintf("%.2f GiB", float64(b)/float64(giB))
	case b >= miB:
		return fmt.Sprintf("%.2f MiB", float64(b)/float64(miB))
	case b >= kiB:
		return fmt.Sprintf("%.2f KiB", float64(b)/float64(kiB))
	default:
		return fmt.Sprintf("%d B", b)
	}
}
