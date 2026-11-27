//go:build inspect

// Diagnostic: what work types are actually inside the CS corpus (field 17,
// non-XPAC)? It projects only id/type/is_xpac/primary_topic.field.id, so it
// costs a few percent of each file instead of a full download.
//
//	go test -v -count=1 -timeout 15m -tags=inspect ./internal/adapters/s3 -run TestDiagWorkTypes
package s3

import (
	"context"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	"go-etl/internal/normalizer"
)

const (
	typeDiagCSFieldID     = "https://openalex.org/fields/17"
	typeDiagCSFieldSuffix = "/fields/17"
)

type diagIDRef struct {
	ID *string `parquet:"id,optional"`
}

type diagPrimaryTopic struct {
	Field *diagIDRef `parquet:"field,optional"`
}

type diagWorkRow struct {
	ID           *string           `parquet:"id,optional"`
	Type         *string           `parquet:"type,optional"`
	IsXPAC       *bool             `parquet:"is_xpac,optional"`
	PrimaryTopic *diagPrimaryTopic `parquet:"primary_topic,optional"`
}

type typeTally struct {
	total   int64
	xpac    int64
	field17 int64 // non-XPAC works with primary_topic.field.id == 17
	byType  map[string]int64
}

func newTypeTally() *typeTally {
	return &typeTally{byType: make(map[string]int64, 16)}
}

func (t *typeTally) merge(o *typeTally) {
	t.total += o.total
	t.xpac += o.xpac
	t.field17 += o.field17
	for k, v := range o.byType {
		t.byType[k] += v
	}
}

func TestDiagWorkTypes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	client, err := NewAnonymousClient(ctx)
	if err != nil {
		t.Fatalf("NewAnonymousClient: %v", err)
	}
	manifest, err := client.FetchManifest(ctx, ParquetManifestKey("works"))
	if err != nil {
		t.Fatalf("FetchManifest(works): %v", err)
	}
	t.Logf("snapshot date=%s files=%d", manifest.Date, len(manifest.Files))

	const mb = 1 << 20
	sample := envInt("DIAG_TYPES_SAMPLE", 18)
	picked := spreadSample(manifest.Files, sample)
	if len(picked) == 0 {
		t.Fatal("no candidate files in the manifest")
	}
	t.Logf("sampling %d of %d files (evenly spaced, Range mode, 4-column projection)", len(picked), len(manifest.Files))

	combined := newTypeTally()
	var totalBytes uint64
	partitions := make(map[string]struct{}, len(picked))
	const readBudget = 600 << 20

	for _, entry := range picked {
		partitions[ExtractPartition(entry.URL)] = struct{}{}
		tally := newTypeTally()
		opts := DefaultParquetReadOptions()
		opts.Mode = ReadModeRange
		start := time.Now()
		pf, mode, statsFn, err := openParquetEntry(ctx, client, entry, opts, &diagWorkRow{})
		if err != nil {
			t.Logf("skip %s: %v", entry.URL, err)
			continue
		}
		reader := parquet.NewGenericReader[diagWorkRow](pf)
		buf := make([]diagWorkRow, 512)
		for {
			clear(buf)
			n, rerr := reader.Read(buf)
			for i := range n {
				tally.observe(&buf[i])
			}
			if rerr != nil {
				break
			}
		}
		_ = reader.Close()
		stats := statsFn()
		totalBytes += stats.BytesRead
		combined.merge(tally)

		t.Logf("\n=== %s (%s)\n    mode=%s size=%.2f MiB read=%.2f MiB elapsed=%s rows=%d xpac=%d field17=%d",
			ExtractPartition(entry.URL), entry.URL[len(entry.URL)-24:], mode,
			float64(entry.Meta.ContentLength)/mb, float64(stats.BytesRead)/mb,
			time.Since(start).Round(time.Millisecond), tally.total, tally.xpac, tally.field17)
		if totalBytes > readBudget {
			t.Logf("read budget %d MiB reached, stopping the sample", readBudget/mb)
			break
		}
	}

	t.Logf("\n=== COMBINED: files=%d partitions=%d read=%.2f MiB rows=%d xpac=%d field17=%d",
		len(picked), len(partitions), float64(totalBytes)/mb, combined.total, combined.xpac, combined.field17)
	logTypeTable(t, combined)

	kept := int64(0)
	for typ, n := range combined.byType {
		if normalizer.IsCorpusType(typ) {
			kept += n
		}
	}
	if combined.field17 > 0 {
		t.Logf("corpus whitelist keeps %d of %d field-17 non-XPAC works (%.1f%%), drops %d (%.1f%%)",
			kept, combined.field17, 100*float64(kept)/float64(combined.field17),
			combined.field17-kept, 100*float64(combined.field17-kept)/float64(combined.field17))

		withOther := kept + combined.byType["other"]
		withOtherAndDiss := withOther + combined.byType["dissertation"]
		t.Logf("what-if: +other -> %d (%.1f%%); +other+dissertation -> %d (%.1f%%)",
			withOther, typePct(withOther, combined.field17),
			withOtherAndDiss, typePct(withOtherAndDiss, combined.field17))
	}
}

func typePct(n, total int64) float64 {
	if total == 0 {
		return 0
	}
	return 100 * float64(n) / float64(total)
}

func spreadSample(entries []ManifestEntry, n int) []ManifestEntry {
	if n <= 0 || n >= len(entries) {
		return entries
	}
	out := make([]ManifestEntry, 0, n)
	for k := range n {
		out = append(out, entries[k*len(entries)/n])
	}
	return out
}

func envInt(key string, def int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return def
	}
	return n
}

func (t *typeTally) observe(r *diagWorkRow) {
	t.total++
	if r.IsXPAC != nil && *r.IsXPAC {
		t.xpac++
		return
	}
	if r.PrimaryTopic == nil || r.PrimaryTopic.Field == nil || r.PrimaryTopic.Field.ID == nil {
		return
	}
	fid := *r.PrimaryTopic.Field.ID
	if fid != typeDiagCSFieldID && !strings.HasSuffix(fid, typeDiagCSFieldSuffix) {
		return
	}
	t.field17++
	typ := ""
	if r.Type != nil {
		typ = *r.Type
	}
	t.byType[typ]++
}

func logTypeTable(t *testing.T, tally *typeTally) {
	t.Helper()
	if tally.field17 == 0 {
		t.Log("    (no field-17 works)")
		return
	}
	type row struct {
		typ string
		n   int64
	}
	rows := make([]row, 0, len(tally.byType))
	for typ, n := range tally.byType {
		rows = append(rows, row{typ, n})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].n != rows[j].n {
			return rows[i].n > rows[j].n
		}
		return rows[i].typ < rows[j].typ
	})
	t.Logf("    %-22s %8s %8s  whitelist", "type", "count", "share")
	for _, r := range rows {
		label := r.typ
		if label == "" {
			label = "<empty>"
		}
		mark := "no"
		if normalizer.IsCorpusType(r.typ) {
			mark = "YES"
		}
		t.Logf("    %-22s %8d %7.2f%%  %s", label, r.n, 100*float64(r.n)/float64(tally.field17), mark)
	}
}
