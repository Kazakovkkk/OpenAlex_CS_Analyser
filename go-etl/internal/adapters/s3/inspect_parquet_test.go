//go:build inspect

package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
)

var plannedWorksColumns = map[string]bool{
	"id":                     true,
	"doi":                    true,
	"title":                  true,
	"publication_year":       true,
	"publication_date":       true,
	"created_date":           true,
	"updated_date":           true,
	"type":                   true,
	"language":               true,
	"is_retracted":           true,
	"is_xpac":                true,
	"cited_by_count":         true,
	"referenced_works_count": true,
	"authors_count":          true,
	"fwci":                   true,

	"primary_topic.id":          true,
	"primary_topic.score":       true,
	"primary_topic.field.id":    true,
	"primary_topic.subfield.id": true,
	"primary_topic.domain.id":   true,

	"topics.list.element.id":    true,
	"topics.list.element.score": true,

	"keywords.list.element.display_name": true,
	"keywords.list.element.score":        true,

	"authorships.list.element.author.id":                              true,
	"authorships.list.element.author_position":                        true,
	"authorships.list.element.countries.list.element":                 true,
	"authorships.list.element.institutions.list.element.id":           true,
	"authorships.list.element.institutions.list.element.country_code": true,

	"primary_location.source.id":           true,
	"primary_location.source.display_name": true,
	"primary_location.source.type":         true,
	"primary_location.is_oa":               true,

	"counts_by_year.list.element.year":           true,
	"counts_by_year.list.element.cited_by_count": true,

	"abstract_inverted_index": true,
}

type columnWeight struct {
	Path         string
	PhysicalType string
	Codec        string
	Compressed   int64
	Uncompressed int64
	Planned      bool
}

type formattedRow struct {
	RowIdx int
	Tag    string
	Fields []formattedField
}

type formattedField struct {
	Name   string
	Values []string
}

func TestInspectParquetSchemasAndSamples(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	client, err := NewAnonymousClient(ctx)
	if err != nil {
		t.Fatalf("NewAnonymousClient: %v", err)
	}

	targets := []struct {
		entity     string
		targetSize int64
		maxReadCap int64
	}{
		{entity: "topics", targetSize: 5 << 20, maxReadCap: 16 << 20},
		{entity: "institutions", targetSize: 1 << 20, maxReadCap: 16 << 20},
		{entity: "works", targetSize: 5 << 20, maxReadCap: 32 << 20},
	}

	for _, tc := range targets {
		t.Run(tc.entity, func(t *testing.T) {
			manifest, err := client.FetchManifest(ctx, ParquetManifestKey(tc.entity))
			if err != nil {
				t.Fatalf("FetchManifest(%s): %v", tc.entity, err)
			}
			picked := PickClosestBySize(manifest.Files, []int64{tc.targetSize})
			if len(picked) == 0 {
				t.Fatalf("no entries in %s manifest", tc.entity)
			}
			entry := picked[0]
			bucket, key, err := ParseS3URL(entry.URL)
			if err != nil {
				t.Fatalf("ParseS3URL(%s): %v", entry.URL, err)
			}

			start := time.Now()
			raw, err := client.ReadAll(ctx, bucket, key, tc.maxReadCap)
			if err != nil {
				t.Fatalf("ReadAll(%s): %v", entry.URL, err)
			}
			t.Logf("\n============================================================")
			t.Logf("ENTITY: %s", tc.entity)
			t.Logf("FILE:   %s (%s downloaded in %s)", entry.URL, formatBytes(int64(len(raw))), time.Since(start).Round(time.Millisecond))

			pf, err := parquet.OpenFile(bytes.NewReader(raw), int64(len(raw)))
			if err != nil {
				t.Fatalf("parquet.OpenFile: %v", err)
			}
			meta := pf.Metadata()
			t.Logf("META:   rows=%d | row_groups=%d | created_by=%q", meta.NumRows, len(meta.RowGroups), meta.CreatedBy)

			var schemaBuf strings.Builder
			parquet.PrintSchema(&schemaBuf, tc.entity, pf.Schema())
			t.Logf("\n--- Parquet Schema (%s) ---\n%s", tc.entity, schemaBuf.String())

			logColumnWeights(t, tc.entity, pf)

			samples, err := collectSamples(pf, tc.entity)
			if err != nil {
				t.Fatalf("collectSamples(%s): %v", tc.entity, err)
			}
			logSamples(t, tc.entity, samples)
		})
	}
}

func logColumnWeights(t *testing.T, entity string, pf *parquet.File) {
	t.Helper()

	meta := pf.Metadata()
	byPath := make(map[string]*columnWeight)
	order := make([]string, 0)

	var totalComp, totalUncomp int64
	for _, rg := range meta.RowGroups {
		for _, col := range rg.Columns {
			p := strings.Join(col.MetaData.PathInSchema, ".")
			cw, ok := byPath[p]
			if !ok {
				cw = &columnWeight{
					Path:         p,
					PhysicalType: col.MetaData.Type.String(),
					Codec:        col.MetaData.Codec.String(),
					Planned:      entity == "works" && plannedWorksColumns[p],
				}
				byPath[p] = cw
				order = append(order, p)
			}
			cw.Compressed += col.MetaData.TotalCompressedSize
			cw.Uncompressed += col.MetaData.TotalUncompressedSize
			totalComp += col.MetaData.TotalCompressedSize
			totalUncomp += col.MetaData.TotalUncompressedSize
		}
	}

	weights := make([]columnWeight, 0, len(order))
	for _, p := range order {
		weights = append(weights, *byPath[p])
	}
	slices.SortFunc(weights, func(a, b columnWeight) int {
		switch {
		case a.Compressed > b.Compressed:
			return -1
		case a.Compressed < b.Compressed:
			return 1
		default:
			return strings.Compare(a.Path, b.Path)
		}
	})

	var sb strings.Builder
	fmt.Fprintf(&sb, "\n--- Column Sizes (%s, %d leaf columns) ---\n", entity, len(weights))
	fmt.Fprintf(&sb, "%-3s %-32s %-12s %-10s %11s %11s %7s\n", "PRJ", "COLUMN", "PHYS_TYPE", "CODEC", "COMPRESSED", "UNCOMP", "%COMP")

	var plannedComp, plannedUncomp int64
	plannedFound := make(map[string]bool)

	for _, w := range weights {
		pct := 0.0
		if totalComp > 0 {
			pct = 100 * float64(w.Compressed) / float64(totalComp)
		}
		mark := " "
		if w.Planned {
			mark = "*"
			plannedComp += w.Compressed
			plannedUncomp += w.Uncompressed
			plannedFound[w.Path] = true
		}
		fmt.Fprintf(&sb, "%-3s %-32s %-12s %-10s %11s %11s %6.2f%%\n",
			mark, w.Path, w.PhysicalType, w.Codec,
			formatBytes(w.Compressed), formatBytes(w.Uncompressed), pct)
	}
	fmt.Fprintf(&sb, "TOTAL: compressed=%s | uncompressed=%s (ratio %.2fx)\n",
		formatBytes(totalComp), formatBytes(totalUncomp), float64(totalUncomp)/float64(max(totalComp, 1)))

	if entity == "works" && totalComp > 0 {
		keepPct := 100 * float64(plannedComp) / float64(totalComp)
		fmt.Fprintf(&sb, "PROJECTION PUSHDOWN (* = %d/%d planned cols): keep=%s (%.2f%%) | skipped=%s (%.2f%% saved!)\n",
			len(plannedFound), len(plannedWorksColumns),
			formatBytes(plannedComp), keepPct,
			formatBytes(totalComp-plannedComp), 100-keepPct)

		var missing []string
		for col := range plannedWorksColumns {
			if !plannedFound[col] {
				missing = append(missing, col)
			}
		}
		if len(missing) > 0 {
			slices.Sort(missing)
			fmt.Fprintf(&sb, "WARNING - planned columns NOT found by exact name: %v\n", missing)
		}
	}

	t.Log(sb.String())
}

func collectSamples(pf *parquet.File, entity string) ([]formattedRow, error) {
	colPaths := pf.Schema().Columns()
	colNames := make([]string, len(colPaths))
	colIndexByName := make(map[string]int, len(colPaths))
	for i, p := range colPaths {
		name := strings.Join(p, ".")
		colNames[i] = name
		colIndexByName[name] = i
	}

	var samples []formattedRow
	haveCSWithAbstract := false
	haveSecondInteresting := false

	rowBuf := make([]parquet.Row, 128)
	globalRowIdx := 0

	for _, rg := range pf.RowGroups() {
		rows := rg.Rows()
		for {
			n, err := rows.ReadRows(rowBuf)
			for i := range n {
				row := rowBuf[i]
				if entity != "works" {
					if len(samples) < 3 {
						samples = append(samples, formatRow(globalRowIdx, fmt.Sprintf("row #%d", globalRowIdx), row, colNames))
					}
				} else {
					if len(samples) == 0 {
						samples = append(samples, formatRow(globalRowIdx, "first row in file", row, colNames))
					} else if !haveCSWithAbstract && isCSWorkWithAbstract(row, colIndexByName) {
						samples = append(samples, formatRow(globalRowIdx, "CS work (fields/17) with abstract", row, colNames))
						haveCSWithAbstract = true
					} else if haveCSWithAbstract && !haveSecondInteresting && isInterestingWork(row, colIndexByName) {
						samples = append(samples, formatRow(globalRowIdx, "CS / xpac / edge sample", row, colNames))
						haveSecondInteresting = true
					}
				}
				globalRowIdx++
			}

			done := len(samples) >= 3
			if err == io.EOF || done {
				break
			}
			if err != nil {
				_ = rows.Close()
				return nil, fmt.Errorf("read rows: %w", err)
			}
		}
		if err := rows.Close(); err != nil {
			return nil, fmt.Errorf("close rows: %w", err)
		}
		if len(samples) >= 3 {
			break
		}
	}

	return samples, nil
}

func isCSWorkWithAbstract(row parquet.Row, colIdx map[string]int) bool {
	ptIdx, okPT := colIdx["primary_topic.field.id"]
	abIdx, okAB := colIdx["abstract_inverted_index"]
	if !okPT || !okAB {
		return false
	}
	hasCS, hasAbs := false, false
	for _, v := range row {
		if v.IsNull() {
			continue
		}
		switch v.Column() {
		case ptIdx:
			if bytes.Contains(v.ByteArray(), []byte("fields/17")) {
				hasCS = true
			}
		case abIdx:
			if len(v.ByteArray()) > 2 {
				hasAbs = true
			}
		}
	}
	return hasCS && hasAbs
}

func isInterestingWork(row parquet.Row, colIdx map[string]int) bool {
	ptIdx, okPT := colIdx["primary_topic.field.id"]
	xpacIdx, okXpac := colIdx["is_xpac"]
	for _, v := range row {
		if v.IsNull() {
			continue
		}
		if okXpac && v.Column() == xpacIdx && v.Boolean() {
			return true
		}
		if okPT && v.Column() == ptIdx && bytes.Contains(v.ByteArray(), []byte("fields/17")) {
			return true
		}
	}
	return false
}

func formatRow(rowIdx int, tag string, row parquet.Row, colNames []string) formattedRow {
	grouped := make([][]string, len(colNames))
	for _, v := range row {
		ci := v.Column()
		if ci < 0 || ci >= len(colNames) {
			continue
		}
		if v.IsNull() {
			grouped[ci] = append(grouped[ci], "<NULL>")
		} else {
			grouped[ci] = append(grouped[ci], v.String())
		}
	}

	fields := make([]formattedField, 0, len(colNames))
	for i, name := range colNames {
		vals := grouped[i]
		if len(vals) == 0 {
			vals = []string{"<EMPTY>"}
		}
		fields = append(fields, formattedField{Name: name, Values: vals})
	}
	return formattedRow{RowIdx: rowIdx, Tag: tag, Fields: fields}
}

func logSamples(t *testing.T, entity string, samples []formattedRow) {
	t.Helper()

	var sb strings.Builder
	fmt.Fprintf(&sb, "\n--- %d Sample Rows (%s) ---\n", len(samples), entity)
	for i, s := range samples {
		fmt.Fprintf(&sb, "\n[Sample %d | rowIdx=%d | %s]\n", i+1, s.RowIdx, s.Tag)
		for _, f := range s.Fields {
			maxLen := 320
			if entity == "works" && len(f.Values) == 1 && (f.Values[0] == "<NULL>" || f.Values[0] == "<EMPTY>") {
				continue
			}
			if entity == "works" {
				if plannedWorksColumns[f.Name] {
					maxLen = 450
				} else {
					maxLen = 100
				}
			}
			var valStr string
			if len(f.Values) == 1 {
				valStr = formatFieldVal(f.Values[0], maxLen)
			} else {
				joined := strings.Join(f.Values, ", ")
				valStr = fmt.Sprintf("[%d items] %s", len(f.Values), formatFieldVal(joined, maxLen))
			}
			fmt.Fprintf(&sb, "  %-28s = %s\n", f.Name, valStr)
		}
	}
	t.Log(sb.String())
}

func formatFieldVal(s string, maxRunes int) string {
	if s == "<NULL>" || s == "<EMPTY>" {
		return s
	}
	runes := []rune(s)
	if len(runes) <= maxRunes {
		return fmt.Sprintf("%q", s)
	}
	return fmt.Sprintf("(len=%d B) %q...", len(s), string(runes[:maxRunes]))
}
