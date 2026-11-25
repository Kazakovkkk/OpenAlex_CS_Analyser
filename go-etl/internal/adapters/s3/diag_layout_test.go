//go:build inspect

package s3

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"

	"go-etl/internal/normalizer"
)

const diagMiB = int64(1 << 20)

type diagSpan struct {
	off  int64
	size int64
}

func (s diagSpan) end() int64 { return s.off + s.size }

func (s diagSpan) String() string {
	return fmt.Sprintf("[%d-%d)", s.off, s.end())
}

func diagParseIntList(raw string, fallback []int64) []int64 {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fallback
	}
	var out []int64
	for part := range strings.SplitSeq(raw, ",") {
		v, err := strconv.ParseInt(strings.TrimSpace(part), 10, 64)
		if err == nil && v >= 0 {
			out = append(out, v)
		}
	}
	if len(out) == 0 {
		return fallback
	}
	return out
}

func diagProjectedPaths() (exact []string) {
	for _, p := range parquet.SchemaOf(&normalizer.ParquetWork{}).Columns() {
		exact = append(exact, strings.Join(p, "."))
	}
	return exact
}

func diagNormPath(p []string) string {
	out := make([]string, 0, len(p))
	for _, seg := range p {
		if seg == "list" || seg == "element" {
			continue
		}
		out = append(out, seg)
	}
	return strings.Join(out, ".")
}

func diagColumnSpan(md *format.ColumnMetaData) diagSpan {
	start := md.DataPageOffset
	if md.DictionaryPageOffset > 0 && md.DictionaryPageOffset < start {
		start = md.DictionaryPageOffset
	}
	return diagSpan{off: start, size: md.TotalCompressedSize}
}

func diagCoalesce(spans []diagSpan, mergeGap int64) []diagSpan {
	if len(spans) == 0 {
		return nil
	}
	sorted := append([]diagSpan(nil), spans...)
	slices.SortFunc(sorted, func(a, b diagSpan) int {
		switch {
		case a.off < b.off:
			return -1
		case a.off > b.off:
			return 1
		default:
			return 0
		}
	})
	out := make([]diagSpan, 0, len(sorted))
	cur := sorted[0]
	for _, s := range sorted[1:] {
		if s.off-cur.end() <= mergeGap {
			if s.end() > cur.end() {
				cur.size = s.end() - cur.off
			}
			continue
		}
		out = append(out, cur)
		cur = s
	}
	out = append(out, cur)
	return out
}

func diagSumBytes(spans []diagSpan) int64 {
	var total int64
	for _, s := range spans {
		total += s.size
	}
	return total
}

func TestDiagRowGroupLayout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	sizeLadderMB := diagParseIntList(os.Getenv("DIAG_SIZES_MB"), []int64{15, 50})
	gaps := make([]int64, 0, 4)
	for _, kb := range diagParseIntList(os.Getenv("DIAG_GAPS_KB"), []int64{0, 128, 512, 1024}) {
		gaps = append(gaps, kb<<10)
	}

	client, err := NewAnonymousClient(ctx)
	if err != nil {
		t.Fatalf("NewAnonymousClient: %v", err)
	}
	manifest, err := client.FetchManifest(ctx, ParquetManifestKey("works"))
	if err != nil {
		t.Fatalf("FetchManifest: %v", err)
	}

	targets := make([]int64, len(sizeLadderMB))
	for i, mb := range sizeLadderMB {
		targets[i] = mb * diagMiB
	}
	picked := PickClosestBySize(manifest.Files, targets)

	projected := diagProjectedPaths()
	projectedSet := make(map[string]struct{}, len(projected))
	projectedNormSet := make(map[string]struct{}, len(projected))
	for _, p := range projected {
		projectedSet[p] = struct{}{}
		projectedNormSet[diagNormPath(strings.Split(p, "."))] = struct{}{}
	}

	var out strings.Builder
	fmt.Fprintf(&out, "\n=== Diag 0a: row-group layout of the projected columns (footer-only, 1 GET/file) ===\n")
	fmt.Fprintf(&out, "projected leaves from SchemaOf(ParquetWork): %d\n", len(projected))

	for i, entry := range picked {
		bucket, key, err := ParseS3URL(entry.URL)
		if err != nil {
			t.Fatalf("ParseS3URL(%s): %v", entry.URL, err)
		}
		size := entry.Meta.ContentLength

		ra := NewS3ReaderAt(ctx, client, bucket, key, size, RangeOptions{
			BufferSize:      0,
			FooterCacheSize: DefaultFooterCacheSize,
			MaxRetries:      2,
		})

		var trailer [8]byte
		if _, err := ra.ReadAt(trailer[:], size-8); err != nil {
			t.Fatalf("read trailer %s: %v", entry.URL, err)
		}
		footerSize := int64(binary.LittleEndian.Uint32(trailer[:4]))

		pf, err := parquet.OpenFile(ra, size)
		if err != nil {
			t.Fatalf("open parquet %s: %v", entry.URL, err)
		}
		st := ra.Stats()
		md := pf.Metadata()

		exactMatched, normMatched := 0, 0
		var unmatched []string
		fileLeaves := 0
		filePathSet := make(map[string]struct{})
		for rgIdx := range md.RowGroups {
			for colIdx := range md.RowGroups[rgIdx].Columns {
				cmd := &md.RowGroups[rgIdx].Columns[colIdx].MetaData
				p := strings.Join(cmd.PathInSchema, ".")
				if _, seen := filePathSet[p]; seen {
					continue
				}
				filePathSet[p] = struct{}{}
				fileLeaves++
			}
		}
		fileNormSet := make(map[string]struct{}, len(filePathSet))
		for p := range filePathSet {
			fileNormSet[diagNormPath(strings.Split(p, "."))] = struct{}{}
		}
		for _, p := range projected {
			if _, ok := filePathSet[p]; ok {
				exactMatched++
				continue
			}
			if _, ok := fileNormSet[diagNormPath(strings.Split(p, "."))]; ok {
				normMatched++
				continue
			}
			unmatched = append(unmatched, p)
		}

		fmt.Fprintf(&out, "\n--- %d MiB target -> %s | size %s | rows %d ---\n",
			sizeLadderMB[i], ExtractPartition(entry.URL), formatBytes(size), md.NumRows)
		fmt.Fprintf(&out, "footer: %s (1 MiB cache %s); reads to open: %d reqs / %s\n",
			formatBytes(footerSize),
			map[bool]string{true: "fits", false: "DOES NOT FIT"}[footerSize <= DefaultFooterCacheSize],
			st.HTTPRequests, formatBytes(int64(st.BytesRead)))
		fmt.Fprintf(&out, "columns: file leaves %d | projected %d (exact %d, normalized %d, UNMATCHED %d)\n",
			fileLeaves, len(projected), exactMatched, normMatched, len(unmatched))
		if len(unmatched) > 0 {
			fmt.Fprintf(&out, "  !! unmatched paths: %s\n", strings.Join(unmatched, ", "))
		}
		fmt.Fprintf(&out, "row groups: %d\n", len(md.RowGroups))

		fmt.Fprintf(&out, "\n| RG | rows | all_cols | needed | share | spans gap=0 | %s |\n",
			diagGapHeader(gaps))
		fmt.Fprintf(&out, "|---:|---:|---:|---:|---:|---:|---|\n")

		type colAgg struct {
			needed bool
			bytes  int64
		}
		colTotals := make(map[string]*colAgg)
		var fileNeeded, fileAll, maxRGNeeded int64
		gapSpanCount := make([]int, len(gaps))
		gapBytes := make([]int64, len(gaps))
		overlaps := 0
		var firstSpans []diagSpan

		for rgIdx := range md.RowGroups {
			rg := &md.RowGroups[rgIdx]
			var rgAll, rgNeeded int64
			var neededSpans []diagSpan
			var prevEnd int64 = -1
			sortedCheck := make([]diagSpan, 0, len(rg.Columns))
			for colIdx := range rg.Columns {
				cmd := &rg.Columns[colIdx].MetaData
				sp := diagColumnSpan(cmd)
				if sp.size <= 0 {
					continue
				}
				sortedCheck = append(sortedCheck, sp)
				rgAll += sp.size
				path := strings.Join(cmd.PathInSchema, ".")
				agg := colTotals[path]
				if agg == nil {
					agg = &colAgg{}
					colTotals[path] = agg
				}
				agg.bytes += sp.size
				if _, ok := projectedSet[path]; ok {
					agg.needed = true
					rgNeeded += sp.size
					neededSpans = append(neededSpans, sp)
				} else if _, ok := projectedNormSet[diagNormPath(cmd.PathInSchema)]; ok {
					agg.needed = true
					rgNeeded += sp.size
					neededSpans = append(neededSpans, sp)
				}
			}
			slices.SortFunc(sortedCheck, func(a, b diagSpan) int {
				switch {
				case a.off < b.off:
					return -1
				case a.off > b.off:
					return 1
				default:
					return 0
				}
			})
			for _, sp := range sortedCheck {
				if prevEnd > sp.off {
					overlaps++
				}
				prevEnd = max(prevEnd, sp.end())
			}

			fileAll += rgAll
			fileNeeded += rgNeeded
			maxRGNeeded = max(maxRGNeeded, rgNeeded)

			share := 100 * float64(rgNeeded) / float64(max(rgAll, 1))
			fmt.Fprintf(&out, "| %d | %d | %s | %s | %.1f%% | %d |", rgIdx, rg.NumRows,
				formatBytes(rgAll), formatBytes(rgNeeded), share, len(neededSpans))

			for gi, gap := range gaps {
				merged := diagCoalesce(neededSpans, gap)
				gapSpanCount[gi] += len(merged)
				gapBytes[gi] += diagSumBytes(merged)
				fmt.Fprintf(&out, " %d sp / %s |", len(merged), formatBytes(diagSumBytes(merged)))
				if gap == 256<<10 && firstSpans == nil {
					firstSpans = merged
				}
			}
			fmt.Fprintf(&out, "\n")
		}

		shareTotal := 100 * float64(fileNeeded) / float64(max(fileAll, 1))
		fmt.Fprintf(&out, "\nfile totals: all cols %s | needed %s | FLOOR %.1f%% | max RG needed %s (prefetch RAM)\n",
			formatBytes(fileAll), formatBytes(fileNeeded), shareTotal, formatBytes(maxRGNeeded))
		fmt.Fprintf(&out, "coalesced prefetch over the whole file (footer %s included):\n", formatBytes(footerSize))
		for gi, gap := range gaps {
			predicted := gapBytes[gi] + footerSize + 5
			fmt.Fprintf(&out, "  gap=%6s: %4d spans, %10s payload -> predicted total %10s (%.1f%% of file)\n",
				formatBytes(gap), gapSpanCount[gi], formatBytes(gapBytes[gi]),
				formatBytes(predicted), 100*float64(predicted)/float64(size))
		}
		if overlaps > 0 {
			fmt.Fprintf(&out, "  !! column chunk spans overlap: %d (footer layout is not what we assume)\n", overlaps)
		}
		if len(firstSpans) > 0 {
			var sb strings.Builder
			for k, sp := range firstSpans {
				if k == 4 {
					sb.WriteString(", ...")
					break
				}
				if k > 0 {
					sb.WriteString(", ")
				}
				sb.WriteString(sp.String())
			}
			fmt.Fprintf(&out, "  first RG spans (gap=256K): %s\n", sb.String())
		}

		type colRow struct {
			path   string
			bytes  int64
			needed bool
		}
		rows := make([]colRow, 0, len(colTotals))
		for p, agg := range colTotals {
			rows = append(rows, colRow{path: p, bytes: agg.bytes, needed: agg.needed})
		}
		slices.SortFunc(rows, func(a, b colRow) int {
			switch {
			case a.bytes > b.bytes:
				return -1
			case a.bytes < b.bytes:
				return 1
			default:
				return 0
			}
		})
		fmt.Fprintf(&out, "\ntop 15 columns by compressed bytes:\n| path | bytes | %% of file | needed |\n|---|---:|---:|---|\n")
		for k := 0; k < len(rows) && k < 15; k++ {
			mark := "no (dropped)"
			if rows[k].needed {
				mark = "YES"
			}
			fmt.Fprintf(&out, "| %s | %s | %.1f%% | %s |\n", rows[k].path,
				formatBytes(rows[k].bytes), 100*float64(rows[k].bytes)/float64(max(fileAll, 1)), mark)
		}

		if os.Getenv("DIAG_FULL_COLS") == "1" {
			fmt.Fprintf(&out, "\nall file leaves (name order) with compressed bytes:\n| # | path | bytes | needed |\n|---:|---|---:|---|\n")
			names := make([]string, 0, len(colTotals))
			for p := range colTotals {
				names = append(names, p)
			}
			slices.Sort(names)
			for k, p := range names {
				mark := "no"
				if colTotals[p].needed {
					mark = "YES"
				}
				fmt.Fprintf(&out, "| %d | %s | %s | %s |\n", k, p, formatBytes(colTotals[p].bytes), mark)
			}
		}
	}

	t.Log(out.String())
}

func diagGapHeader(gaps []int64) string {
	parts := make([]string, len(gaps))
	for i, g := range gaps {
		parts[i] = "gap=" + formatBytes(g)
	}
	return strings.Join(parts, " | ")
}
