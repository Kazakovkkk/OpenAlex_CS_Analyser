//go:build inspect

package s3

import (
	"context"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"
	"time"
)

type diagPartAgg struct {
	files   int
	bytes   int64
	records int64
}

func (a *diagPartAgg) add(e ManifestEntry) {
	a.files++
	a.bytes += e.Meta.ContentLength
	a.records += e.Meta.RecordCount
}

func diagPartitionKey(url string) string {
	_, after, ok := strings.Cut(url, "/works/")
	if !ok {
		return ""
	}
	dir, _, found := strings.Cut(after, "/")
	if !found {
		return ""
	}
	return dir
}

func TestDiagManifestPartitions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	client, err := NewAnonymousClient(ctx)
	if err != nil {
		t.Fatalf("NewAnonymousClient: %v", err)
	}
	pqManifest, err := client.FetchManifest(ctx, ParquetManifestKey("works"))
	if err != nil {
		t.Fatalf("FetchManifest(parquet): %v", err)
	}
	jsonlManifest, err := client.FetchManifest(ctx, "data/jsonl/works/manifest.json")
	if err != nil {
		t.Fatalf("FetchManifest(jsonl): %v", err)
	}

	agg := func(m *Manifest) map[string]*diagPartAgg {
		out := make(map[string]*diagPartAgg, 4096)
		for _, e := range m.Files {
			k := diagPartitionKey(e.URL)
			a := out[k]
			if a == nil {
				a = &diagPartAgg{}
				out[k] = a
			}
			a.add(e)
		}
		return out
	}
	pqParts := agg(pqManifest)
	jsonlParts := agg(jsonlManifest)

	var out strings.Builder
	fmt.Fprintf(&out, "\n=== Diag 0c: snapshot partitioning (manifest-only) ===\n")
	fmt.Fprintf(&out, "snapshot date: parquet=%q jsonl=%q\n", pqManifest.Date, jsonlManifest.Date)
	fmt.Fprintf(&out, "\n| format | files | partitions | bytes | records |\n|---|---:|---:|---:|---:|\n")
	fmt.Fprintf(&out, "| parquet | %d | %d | %s | %d |\n", len(pqManifest.Files), len(pqParts),
		formatBytes(pqManifest.ContentLength), pqManifest.RecordCount)
	fmt.Fprintf(&out, "| jsonl.gz | %d | %d | %s | %d |\n", len(jsonlManifest.Files), len(jsonlParts),
		formatBytes(jsonlManifest.ContentLength), jsonlManifest.RecordCount)
	type cand struct {
		key     string
		pqFiles int
		jFiles  int
		pqBytes int64
		jBytes  int64
		records int64
	}
	var sameCount, diffCount int
	var cands []cand
	for k, p := range pqParts {
		j, ok := jsonlParts[k]
		if !ok {
			continue
		}
		if p.records != j.records {
			diffCount++
			continue
		}
		sameCount++
		cands = append(cands, cand{
			key: k, pqFiles: p.files, jFiles: j.files,
			pqBytes: p.bytes, jBytes: j.bytes, records: p.records,
		})
	}
	fmt.Fprintf(&out, "\npartitions in both formats: %d | same record_count: %d | different record_count: %d\n",
		sameCount+diffCount, sameCount, diffCount)

	slices.SortFunc(cands, func(a, b cand) int {
		switch {
		case a.pqBytes < b.pqBytes:
			return -1
		case a.pqBytes > b.pqBytes:
			return 1
		default:
			return strings.Compare(a.key, b.key)
		}
	})

	fmt.Fprintf(&out, "\nsame-content partitions cheapest for Metric #3 (identical works, parquet total <= 60 MiB):\n")
	fmt.Fprintf(&out, "| partition | pq files | jsonl files | pq bytes | jsonl bytes | jsonl/pq | records | CS share est |\n")
	fmt.Fprintf(&out, "|---|---:|---:|---:|---:|---:|---:|---|\n")
	shown := 0
	for _, c := range cands {
		if c.pqBytes > 60*diagMiB || c.records < 5_000 {
			continue
		}
		fmt.Fprintf(&out, "| %s | %d | %d | %s | %s | %.2fx | %d | - |\n",
			c.key, c.pqFiles, c.jFiles, formatBytes(c.pqBytes), formatBytes(c.jBytes),
			float64(c.jBytes)/float64(max(c.pqBytes, 1)), c.records)
		shown++
		if shown >= 12 {
			break
		}
	}
	if shown == 0 {
		fmt.Fprintln(&out, "| (none) | | | | | | | |")
	}

	for _, key := range []string{"updated_date=2026-08-13"} {
		p := pqParts[key]
		j := jsonlParts[key]
		if p == nil || j == nil {
			continue
		}
		fmt.Fprintf(&out, "\npartition %s: parquet %d files / %s / %d records; jsonl %d files / %s / %d records\n",
			key, p.files, formatBytes(p.bytes), p.records, j.files, formatBytes(j.bytes), j.records)
		fmt.Fprintf(&out, "  -> per-file averages: parquet %.2f MiB, jsonl %.2f MiB\n",
			float64(p.bytes)/float64(max(p.files, 1))/float64(diagMiB),
			float64(j.bytes)/float64(max(j.files, 1))/float64(diagMiB))
	}

	fmt.Fprintf(&out, "\nfull-snapshot compression ratio jsonl/parquet: %.2fx\n",
		float64(jsonlManifest.ContentLength)/float64(max(pqManifest.ContentLength, 1)))
	mid := make([]cand, 0, 32)
	for _, c := range cands {
		if c.pqBytes >= 8*diagMiB && c.pqBytes <= 600*diagMiB {
			mid = append(mid, c)
		}
	}
	fmt.Fprintf(&out, "\npartitions with total parquet size 8..600 MiB (fair format comparison candidates): %d\n", len(mid))
	if len(mid) > 0 {
		fmt.Fprintf(&out, "| partition | pq files | jsonl files | pq bytes | jsonl bytes | jsonl/pq | records |\n|---|---:|---:|---:|---:|---:|---:|\n")
		for i, c := range mid {
			if i >= 20 {
				fmt.Fprintf(&out, "| ... %d more | | | | | | |\n", len(mid)-20)
				break
			}
			fmt.Fprintf(&out, "| %s | %d | %d | %s | %s | %.2fx | %d |\n", c.key, c.pqFiles, c.jFiles,
				formatBytes(c.pqBytes), formatBytes(c.jBytes),
				float64(c.jBytes)/float64(max(c.pqBytes, 1)), c.records)
		}
	}

	buckets := []struct {
		label   string
		minByte int64
		maxByte int64
	}{
		{"< 5 MiB", 0, 5 * diagMiB},
		{"5..15 MiB", 5 * diagMiB, 15 * diagMiB},
		{"15..50 MiB", 15 * diagMiB, 50 * diagMiB},
		{"50..150 MiB", 50 * diagMiB, 150 * diagMiB},
		{"150..500 MiB", 150 * diagMiB, 500 * diagMiB},
		{"500 MiB..2 GiB", 500 * diagMiB, 2048 * diagMiB},
		{">= 2 GiB", 2048 * diagMiB, math.MaxInt64},
	}
	fmt.Fprintf(&out, "\npartition size distribution (parquet):\n| bucket | partitions | files | bytes |\n|---|---:|---:|---:|\n")
	for _, b := range buckets {
		var nParts, nFiles int
		var nBytes int64
		for _, c := range cands {
			if c.pqBytes >= b.minByte && c.pqBytes < b.maxByte {
				nParts++
				nFiles += c.pqFiles
				nBytes += c.pqBytes
			}
		}
		fmt.Fprintf(&out, "| %s | %d | %d | %s |\n", b.label, nParts, nFiles, formatBytes(nBytes))
	}

	t.Log(out.String())
}
