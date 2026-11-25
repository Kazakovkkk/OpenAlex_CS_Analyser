//go:build inspect

package s3

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/bytedance/sonic"
	kgzip "github.com/klauspost/compress/gzip"
	"github.com/parquet-go/parquet-go"

	"go-etl/internal/normalizer"
)

const diagCSFieldID = "https://openalex.org/fields/17"

type diagWorkSig struct {
	xpac    bool
	topic   bool
	fieldID string
}

func (s diagWorkSig) isCS() bool {
	if s.xpac || !s.topic {
		return false
	}
	return s.fieldID == diagCSFieldID || strings.HasSuffix(s.fieldID, "/fields/17")
}

func (s diagWorkSig) String() string {
	field := s.fieldID
	if field == "" {
		field = "<none>"
	}
	return fmt.Sprintf("is_xpac=%t primary_topic=%t field.id=%s", s.xpac, s.topic, field)
}

type diagFormatStats struct {
	label string
	bytes int64

	rows   int64
	xpac   int64
	noTop  int64
	csRows int64
	dupIDs int64
	noID   int64

	prefilterRejected int64

	ids    map[uint64]diagWorkSig
	csIDs  map[uint64]diagWorkSig
	fields map[string]int64
}

func newDiagFormatStats(label string) *diagFormatStats {
	return &diagFormatStats{
		label:  label,
		ids:    make(map[uint64]diagWorkSig, 1<<19),
		csIDs:  make(map[uint64]diagWorkSig, 1<<10),
		fields: make(map[string]int64, 64),
	}
}

func (s *diagFormatStats) observe(id uint64, sig diagWorkSig) {
	s.rows++
	if sig.xpac {
		s.xpac++
	}
	if !sig.topic {
		s.noTop++
	}
	if sig.isCS() {
		s.csRows++
	}
	field := sig.fieldID
	if field == "" {
		field = "<null>"
	}
	s.fields[field]++
	if id == 0 {
		s.noID++
		return
	}
	if _, seen := s.ids[id]; seen {
		s.dupIDs++
		return
	}
	s.ids[id] = sig
	if sig.isCS() {
		s.csIDs[id] = sig
	}
}

func (s *diagFormatStats) topFields(n int) []string {
	type kv struct {
		k string
		v int64
	}
	rows := make([]kv, 0, len(s.fields))
	for k, v := range s.fields {
		rows = append(rows, kv{k: k, v: v})
	}
	slices.SortFunc(rows, func(a, b kv) int {
		switch {
		case a.v > b.v:
			return -1
		case a.v < b.v:
			return 1
		default:
			return strings.Compare(a.k, b.k)
		}
	})
	out := make([]string, 0, n)
	for i := 0; i < len(rows) && i < n; i++ {
		out = append(out, fmt.Sprintf("| %s | %d | %.2f%% |", rows[i].k, rows[i].v, 100*float64(rows[i].v)/float64(max(s.rows, 1))))
	}
	return out
}

type diagMiniTopic struct {
	Field *struct {
		ID string `json:"id"`
	} `json:"field"`
}

type diagMiniJSON struct {
	ID           string         `json:"id"`
	IsXPAC       *bool          `json:"is_xpac"`
	PrimaryTopic *diagMiniTopic `json:"primary_topic"`
}

func TestDiagPairedFormats(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	targetMB := diagParseIntList(os.Getenv("DIAG_PAIR_MB"), []int64{50})[0]

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
	jsonlByURL := make(map[string]ManifestEntry, len(jsonlManifest.Files))
	for _, e := range jsonlManifest.Files {
		jsonlByURL[e.URL] = e
	}

	picked := PickClosestBySize(pqManifest.Files, []int64{targetMB * diagMiB})
	if len(picked) == 0 {
		t.Fatal("no parquet file picked")
	}
	pqEntry := picked[0]
	jsonlEntry, ok := findMatchingJSONLEntry(pqEntry.URL, jsonlByURL)
	if !ok {
		t.Fatalf("no jsonl twin for %s", pqEntry.URL)
	}
	bucket, key, err := ParseS3URL(pqEntry.URL)
	if err != nil {
		t.Fatalf("ParseS3URL: %v", err)
	}

	var out strings.Builder
	fmt.Fprintf(&out, "\n=== Diag 0b: JSONL.gz vs Parquet on the same partition ===\n")
	fmt.Fprintf(&out, "parquet: %s (%s)\n", pqEntry.URL, formatBytes(pqEntry.Meta.ContentLength))
	fmt.Fprintf(&out, "jsonl  : %s (%s)\n", jsonlEntry.URL, formatBytes(jsonlEntry.Meta.ContentLength))

	pqStart := time.Now()
	raw, err := client.ReadAll(ctx, bucket, key, 600<<20)
	if err != nil {
		t.Fatalf("readall parquet: %v", err)
	}
	pf, err := parquet.OpenFile(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		t.Fatalf("open parquet: %v", err)
	}
	pqStats := newDiagFormatStats("parquet")
	pqStats.bytes = int64(len(raw))
	reader := parquet.NewGenericReader[normalizer.ParquetWork](pf)
	defer reader.Close()
	rowBuf := make([]normalizer.ParquetWork, 512)
	for {
		clear(rowBuf)
		n, readErr := reader.Read(rowBuf)
		for i := range n {
			w := &rowBuf[i]
			sig := diagWorkSig{xpac: w.IsXPAC != nil && *w.IsXPAC, topic: w.PrimaryTopic != nil}
			if w.PrimaryTopic != nil && w.PrimaryTopic.Field != nil && w.PrimaryTopic.Field.ID != nil {
				sig.fieldID = *w.PrimaryTopic.Field.ID
			}
			id := normalizer.ParseWorkID(derefOrEmpty(w.ID))
			pqStats.observe(id, sig)
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			t.Fatalf("parquet read: %v", readErr)
		}
	}
	pqElapsed := time.Since(pqStart)

	jStart := time.Now()
	jsonlBucket, jsonlKey, err := ParseS3URL(jsonlEntry.URL)
	if err != nil {
		t.Fatalf("ParseS3URL jsonl: %v", err)
	}
	obj, err := client.GetObject(ctx, jsonlBucket, jsonlKey)
	if err != nil {
		t.Fatalf("get jsonl: %v", err)
	}
	defer obj.Body.Close()

	counter := &countingReader{r: obj.Body}
	gz, err := kgzip.NewReader(counter)
	if err != nil {
		t.Fatalf("gzip: %v", err)
	}
	scanner := bufio.NewScanner(gz)
	scanner.Buffer(make([]byte, 256<<10), 16<<20)

	jStats := newDiagFormatStats("jsonl.gz")
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		if !normalizer.FastJSONPreFilter(line) {
			jStats.prefilterRejected++
		}
		var mini diagMiniJSON
		if err := sonic.Unmarshal(line, &mini); err != nil {
			t.Fatalf("sonic: %v", err)
		}
		sig := diagWorkSig{
			xpac:  mini.IsXPAC != nil && *mini.IsXPAC,
			topic: mini.PrimaryTopic != nil,
		}
		if mini.PrimaryTopic != nil && mini.PrimaryTopic.Field != nil {
			sig.fieldID = mini.PrimaryTopic.Field.ID
		}
		jStats.observe(normalizer.ParseWorkID(mini.ID), sig)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan jsonl: %v", err)
	}
	jStats.bytes = int64(counter.bytes.Load())
	jElapsed := time.Since(jStart)

	fmt.Fprintf(&out, "\n| metric | parquet | jsonl.gz | delta |\n|---|---:|---:|---:|\n")
	row := func(name string, a, b int64) {
		fmt.Fprintf(&out, "| %s | %d | %d | %+d |\n", name, a, b, b-a)
	}
	row("rows scanned", pqStats.rows, jStats.rows)
	row("bytes on wire", pqStats.bytes, jStats.bytes)
	row("is_xpac=true", pqStats.xpac, jStats.xpac)
	row("primary_topic=null", pqStats.noTop, jStats.noTop)
	row("CS rows (filter match)", pqStats.csRows, jStats.csRows)
	row("duplicate work_id", pqStats.dupIDs, jStats.dupIDs)
	row("id parse -> 0", pqStats.noID, jStats.noID)
	row("distinct ids", int64(len(pqStats.ids)), int64(len(jStats.ids)))
	row("distinct CS ids", int64(len(pqStats.csIDs)), int64(len(jStats.csIDs)))
	fmt.Fprintf(&out, "\nwall: parquet %.2fs (%.2f MB/s), jsonl.gz %.2fs (%.2f MB/s)\n",
		pqElapsed.Seconds(), float64(pqStats.bytes)/float64(diagMiB)/pqElapsed.Seconds(),
		jElapsed.Seconds(), float64(jStats.bytes)/float64(diagMiB)/jElapsed.Seconds())
	if jStats.prefilterRejected > 0 {
		fmt.Fprintf(&out, "jsonl prefilter rejected %d rows (%.2f%%) before sonic\n",
			jStats.prefilterRejected, 100*float64(jStats.prefilterRejected)/float64(max(jStats.rows, 1)))
	}

	var onlyPQ, onlyJSONL int
	for id := range pqStats.ids {
		if _, ok := jStats.ids[id]; !ok {
			onlyPQ++
		}
	}
	for id := range jStats.ids {
		if _, ok := pqStats.ids[id]; !ok {
			onlyJSONL++
		}
	}
	fmt.Fprintf(&out, "\nid sets: parquet-only %d, jsonl-only %d (same %d)\n",
		onlyPQ, onlyJSONL, len(pqStats.ids)-onlyPQ)

	var csOnlyPQ, csOnlyJSONL []uint64
	for id := range pqStats.csIDs {
		if _, ok := jStats.csIDs[id]; !ok {
			csOnlyPQ = append(csOnlyPQ, id)
		}
	}
	for id := range jStats.csIDs {
		if _, ok := pqStats.csIDs[id]; !ok {
			csOnlyJSONL = append(csOnlyJSONL, id)
		}
	}
	slices.Sort(csOnlyPQ)
	slices.Sort(csOnlyJSONL)
	fmt.Fprintf(&out, "CS id sets: parquet-only %d, jsonl-only %d (same %d)\n",
		len(csOnlyPQ), len(csOnlyJSONL), len(pqStats.csIDs)-len(csOnlyPQ))
	if len(csOnlyPQ) > 0 && len(csOnlyPQ) == len(csOnlyJSONL) {
		fmt.Fprintf(&out, "  (exactly the same cardinality: the files contain %d rows more/less, or different works)\n", len(csOnlyPQ))
	}

	evidence := func(title string, ids []uint64, from, to *diagFormatStats) {
		if len(ids) == 0 {
			return
		}
		fmt.Fprintf(&out, "\n%s (up to 20):\n| work_id | %s says | %s says |\n|---:|---|---|\n", title, from.label, to.label)
		for i := 0; i < len(ids) && i < 20; i++ {
			id := ids[i]
			a, aOK := from.ids[id]
			b, bOK := to.ids[id]
			aStr, bStr := "<row absent>", "<row absent>"
			if aOK {
				aStr = a.String()
			}
			if bOK {
				bStr = b.String()
			}
			fmt.Fprintf(&out, "| W%d | %s | %s |\n", id, aStr, bStr)
		}
	}
	evidence("CS rows only in PARQUET", csOnlyPQ, pqStats, jStats)
	evidence("CS rows only in JSONL.GZ", csOnlyJSONL, jStats, pqStats)

	fmt.Fprintf(&out, "\ntop primary_topic.field.id (parquet):\n| field.id | rows | share |\n|---|---:|---:|\n")
	for _, line := range pqStats.topFields(10) {
		fmt.Fprintln(&out, line)
	}
	fmt.Fprintf(&out, "\ntop primary_topic.field.id (jsonl.gz):\n| field.id | rows | share |\n|---|---:|---:|\n")
	for _, line := range jStats.topFields(10) {
		fmt.Fprintln(&out, line)
	}

	t.Log(out.String())
}

func derefOrEmpty(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}
