package normalizer

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/sonic"

	"go-etl/internal/sink"
)

var (
	benchIntSink int
	benchStrSink string
	benchAoSSink []aosWork
)

const minBenchRecords = 10_000

type benchDataset struct {
	sourceLabel string

	// Pair 1: 10,000+ full OpenAlex work JSON records
	workJSONs      [][]byte
	totalJSONBytes int64

	// Pair 2: 10,000+ raw primary_topic JSON objects (~3-5% CS, ~95-97% non-CS)
	primaryTopicJSONs      [][]byte
	totalPrimaryTopicBytes int64

	// Pair 3: 10,000 inverted index maps for abstract reconstruction
	abstractMaps       []map[string][]int
	totalAbstractBytes int64

	// Pair 4: 10,000 decoded CS works for 1 full batch (AoS vs SoA+Pool)
	csWorks          []JSONWork
	batchApproxBytes int64
}

var (
	benchDataOnce sync.Once
	benchData     *benchDataset
)

func getBenchDataset(tb testing.TB) *benchDataset {
	tb.Helper()
	benchDataOnce.Do(func() {
		if path := os.Getenv("BENCH_WORKS_FILE"); path != "" {
			ds, err := loadRealJSONLGZ(path)
			if err != nil {
				panic(fmt.Sprintf("failed to load BENCH_WORKS_FILE=%q: %v", path, err))
			}
			benchData = ds
			return
		}
		benchData = buildSyntheticDataset(minBenchRecords)
	})
	return benchData
}

func loadRealJSONLGZ(path string) (*benchDataset, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var reader *bufio.Scanner
	if strings.HasSuffix(path, ".gz") {
		gz, err := gzip.NewReader(f)
		if err != nil {
			return nil, err
		}
		defer gz.Close()
		reader = bufio.NewScanner(gz)
	} else {
		reader = bufio.NewScanner(f)
	}
	reader.Buffer(make([]byte, 64<<10), 16<<20)

	type topicExtractor struct {
		PrimaryTopic json.RawMessage `json:"primary_topic"`
	}

	ds := &benchDataset{
		sourceLabel:       "real file: " + path,
		workJSONs:         make([][]byte, 0, minBenchRecords),
		primaryTopicJSONs: make([][]byte, 0, minBenchRecords),
		abstractMaps:      make([]map[string][]int, 0, minBenchRecords),
		csWorks:           make([]JSONWork, 0, minBenchRecords),
	}

	for reader.Scan() {
		line := bytes.TrimSpace(reader.Bytes())
		if len(line) == 0 {
			continue
		}
		cp := append([]byte(nil), line...)
		ds.workJSONs = append(ds.workJSONs, cp)
		ds.totalJSONBytes += int64(len(cp))

		var ext topicExtractor
		if err := sonic.Unmarshal(cp, &ext); err == nil && len(ext.PrimaryTopic) > 0 && !bytes.Equal(ext.PrimaryTopic, []byte("null")) {
			pt := append([]byte(nil), ext.PrimaryTopic...)
			ds.primaryTopicJSONs = append(ds.primaryTopicJSONs, pt)
			ds.totalPrimaryTopicBytes += int64(len(pt))
		}

		var jw JSONWork
		if err := sonic.Unmarshal(cp, &jw); err == nil {
			if len(jw.AbstractInvertedIdx) > 0 && len(ds.abstractMaps) < minBenchRecords {
				ds.abstractMaps = append(ds.abstractMaps, jw.AbstractInvertedIdx)
			}
			if IsCSJSONWork(&jw) && len(ds.csWorks) < minBenchRecords {
				ds.csWorks = append(ds.csWorks, jw)
			}
		}
	}
	if err := reader.Err(); err != nil {
		return nil, err
	}
	if len(ds.workJSONs) == 0 {
		return nil, fmt.Errorf("file %s contained 0 JSON lines", path)
	}

	finalizeDataset(ds)
	return ds, nil
}

func buildSyntheticDataset(n int) *benchDataset {
	ds := &benchDataset{
		sourceLabel:       fmt.Sprintf("synthetic OpenAlex-like (%d records)", n),
		workJSONs:         make([][]byte, 0, n),
		primaryTopicJSONs: make([][]byte, 0, n),
		abstractMaps:      make([]map[string][]int, 0, n),
		csWorks:           make([]JSONWork, 0, n),
	}

	sampleWords := []string{
		"distributed", "columnar", "analytics", "engine", "processes", "millions", "scientific",
		"publications", "using", "vectorized", "execution", "compression", "memory", "pooling",
		"streaming", "parquet", "storage", "emerging", "research", "topics", "computer", "science",
		"algorithms", "graph", "neural", "networks", "transformer", "scaling", "latency", "throughput",
		"optimization", "pipeline", "benchmark", "evaluation", "system", "architecture", "database",
	}

	for i := range n {
		isCS := i%25 == 0
		fieldID := "https://openalex.org/fields/23"
		topicID := "https://openalex.org/T10089"
		subfieldID := "https://openalex.org/subfields/2303"
		if isCS {
			fieldID = "https://openalex.org/fields/17"
			topicID = "https://openalex.org/T12799"
			subfieldID = "https://openalex.org/subfields/1710"
		}

		inv := make(map[string][]int, 32)
		wordCount := 80 + (i % 70)
		for pos := range wordCount {
			w := sampleWords[(pos+i)%len(sampleWords)]
			inv[w] = append(inv[w], pos)
		}

		fwci := 1.25 + float64(i%10)*0.3
		jw := JSONWork{
			ID:                   fmt.Sprintf("https://openalex.org/W%d", 2700000000+i),
			DOI:                  fmt.Sprintf("https://doi.org/10.1000/oa.%d", i),
			Title:                fmt.Sprintf("Scalable Analysis of Emerging Directions #%d", i),
			PublicationYear:      int32(2015 + i%12),
			PublicationDate:      fmt.Sprintf("%04d-06-%02d", 2015+i%12, 1+i%28),
			CreatedDate:          "2025-10-10T00:00:00Z",
			UpdatedDate:          "2026-10-14T06:19:08.300824Z",
			Type:                 "article",
			Language:             "en",
			IsRetracted:          false,
			IsXPAC:               false,
			CitedByCount:         int32(i % 500),
			ReferencedWorksCount: int32(20 + i%40),
			AuthorsCount:         2,
			FWCI:                 &fwci,
			PrimaryTopic: &JSONPrimaryTopic{
				ID:       topicID,
				Score:    0.85,
				Subfield: &JSONIDRef{ID: subfieldID},
				Field:    &JSONIDRef{ID: fieldID},
				Domain:   &JSONIDRef{ID: "https://openalex.org/domains/3"},
			},
			PrimaryLocation: &JSONPrimaryLoc{
				Source: &JSONPrimarySource{ID: "https://openalex.org/S4306402476", Type: "journal"},
				IsOA:   i%2 == 0,
			},
			Authorships: []JSONAuthorship{
				{
					Author:         &JSONIDRef{ID: fmt.Sprintf("https://openalex.org/A%d", 1000+i)},
					AuthorPosition: "first",
					Countries:      []string{"US"},
					Institutions:   []JSONWorkInstitution{{ID: "https://openalex.org/I100", CountryCode: "US"}},
				},
				{
					Author:         &JSONIDRef{ID: fmt.Sprintf("https://openalex.org/A%d", 2000+i)},
					AuthorPosition: "last",
					Countries:      []string{"DE"},
					Institutions:   []JSONWorkInstitution{{ID: "https://openalex.org/I200", CountryCode: "DE"}},
				},
			},
			Topics: []JSONWorkTopic{
				{ID: topicID, Score: 0.85},
				{ID: "https://openalex.org/T10616", Score: 0.31},
			},
			Keywords: []JSONWorkKeyword{
				{DisplayName: "Machine Learning"},
				{DisplayName: "Big Data"},
				{DisplayName: "ClickHouse"},
			},
			CountsByYear: []JSONYearCount{
				{Year: 2025, CitedByCount: 15},
				{Year: 2024, CitedByCount: 9},
			},
			AbstractInvertedIdx: inv,
		}

		raw, _ := sonic.Marshal(&jw)
		ds.workJSONs = append(ds.workJSONs, raw)
		ds.totalJSONBytes += int64(len(raw))

		ptRaw, _ := sonic.Marshal(jw.PrimaryTopic)
		ds.primaryTopicJSONs = append(ds.primaryTopicJSONs, ptRaw)
		ds.totalPrimaryTopicBytes += int64(len(ptRaw))

		ds.abstractMaps = append(ds.abstractMaps, inv)
		if isCS {
			ds.csWorks = append(ds.csWorks, jw)
		}
	}

	finalizeDataset(ds)
	return ds
}

// finalizeDataset ensures every pair processes at least 10,000 items and precomputes exact byte totals.
func finalizeDataset(ds *benchDataset) {
	// Pad slices by cycling existing real records if a real partition had fewer than 10k abstracts/CS works.
	for len(ds.workJSONs) < minBenchRecords {
		src := ds.workJSONs[len(ds.workJSONs)%max(len(ds.workJSONs), 1)]
		ds.workJSONs = append(ds.workJSONs, src)
		ds.totalJSONBytes += int64(len(src))
	}
	if len(ds.primaryTopicJSONs) > 0 {
		orig := len(ds.primaryTopicJSONs)
		for len(ds.primaryTopicJSONs) < minBenchRecords {
			src := ds.primaryTopicJSONs[len(ds.primaryTopicJSONs)%orig]
			ds.primaryTopicJSONs = append(ds.primaryTopicJSONs, src)
			ds.totalPrimaryTopicBytes += int64(len(src))
		}
	}
	if len(ds.abstractMaps) > 0 {
		orig := len(ds.abstractMaps)
		for len(ds.abstractMaps) < minBenchRecords {
			ds.abstractMaps = append(ds.abstractMaps, ds.abstractMaps[len(ds.abstractMaps)%orig])
		}
	}
	if len(ds.csWorks) > 0 {
		orig := len(ds.csWorks)
		for len(ds.csWorks) < minBenchRecords {
			ds.csWorks = append(ds.csWorks, ds.csWorks[len(ds.csWorks)%orig])
		}
	}

	for _, m := range ds.abstractMaps {
		ds.totalAbstractBytes += int64(len(ReconstructAbstract(m)))
	}
	tmpBatch := sink.NewWorksBatch(len(ds.csWorks))
	norm := NewNormalizer(nil)
	for i := range ds.csWorks {
		norm.AppendJSONWork(&ds.csWorks[i], tmpBatch)
	}
	ds.batchApproxBytes = tmpBatch.ApproxBytes()
}

// --- Naive "Before" implementations for Pair 3 and Pair 4 ---

type wordPos struct {
	pos  int
	word string
}

// reconstructAbstractNaive is the O(N log N) baseline without sync.Pool or strings.Builder.Grow.
func reconstructAbstractNaive(inv map[string][]int) string {
	if len(inv) == 0 {
		return ""
	}
	var pairs []wordPos
	for word, positions := range inv {
		if len(word) == 0 {
			continue
		}
		for _, pos := range positions {
			if pos < 0 || pos >= maxAbstractPositions {
				continue
			}
			pairs = append(pairs, wordPos{pos: pos, word: word})
		}
	}
	if len(pairs) == 0 {
		return ""
	}
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].pos < pairs[j].pos
	})
	words := make([]string, len(pairs))
	for i, p := range pairs {
		words[i] = p.word
	}
	out := strings.Join(words, " ")
	if len(out) > MaxAbstractBytes {
		return truncateUTF8Bytes(out, MaxAbstractBytes)
	}
	return out
}

// aosWork represents the naive Array-of-Structs (AoS) layout with per-row heap pointers and no sync.Pool.
type aosWork struct {
	WorkID             uint64
	DOI                string
	Title              string
	Abstract           string
	PubYear            uint16
	PubDate            *time.Time
	CreatedDate        time.Time
	UpdatedDate        time.Time
	WorkType           string
	Language           string
	IsRetracted        uint8
	CitedByCount       uint32
	RefWorksCount      uint32
	AuthorsCount       uint16
	FWCI               *float32
	PrimaryTopicID     uint32
	PrimaryTopicScore  float32
	PrimarySubfieldID  uint32
	PrimaryFieldID     uint16
	PrimaryDomainID    uint8
	PrimarySourceID    uint64
	PrimarySourceType  string
	PrimaryIsOA        uint8
	FirstAuthorCountry string
	CountryCodes       []string
	AuthorIDs          []uint64
	InstitutionIDs     []uint64
	TopicIDs           []uint32
	TopicScores        []float32
	Keywords           []string
	CitedByYears       []uint16
	CitedByYearCounts  []uint32
}

func appendAoSWork(w *JSONWork, dst []aosWork) []aosWork {
	if !IsCSJSONWork(w) {
		return dst
	}
	var pubDatePtr *time.Time
	if t, ok := ParseDate32String(w.PublicationDate); ok {
		dt := new(time.Time)
		*dt = t
		pubDatePtr = dt
	}
	var fwciPtr *float32
	if w.FWCI != nil {
		f := new(float32)
		*f = float32(*w.FWCI)
		fwciPtr = f
	}
	firstCountry, countries, authorIDs, instIDs := extractJSONAuthorships(w.Authorships)
	topicIDs, topicScores := extractJSONTopics(w.Topics)
	keywords := extractJSONKeywords(w.Keywords)
	citedYears, citedCounts := extractJSONCountsByYear(w.CountsByYear)

	var pTopicID, pSubfieldID uint32
	var pTopicScore float32
	var pFieldID uint16
	var pDomainID uint8
	if pt := w.PrimaryTopic; pt != nil {
		pTopicID = ParseTopicID(pt.ID)
		pTopicScore = pt.Score
		if pt.Subfield != nil {
			pSubfieldID = ParseSubfieldID(pt.Subfield.ID)
		}
		if pt.Field != nil {
			pFieldID = ParseFieldID(pt.Field.ID)
		}
		if pt.Domain != nil {
			pDomainID = ParseDomainID(pt.Domain.ID)
		}
	}
	var pSourceID uint64
	var pSourceType string
	var pIsOA uint8
	if pl := w.PrimaryLocation; pl != nil {
		pIsOA = boolToUint8(pl.IsOA)
		if pl.Source != nil {
			pSourceID = ParseSourceID(pl.Source.ID)
			pSourceType = pl.Source.Type
		}
	}

	return append(dst, aosWork{
		WorkID:             ParseWorkID(w.ID),
		DOI:                w.DOI,
		Title:              w.Title,
		Abstract:           ReconstructAbstract(w.AbstractInvertedIdx),
		PubYear:            ClampUint16(w.PublicationYear),
		PubDate:            pubDatePtr,
		CreatedDate:        ParseTimestampString(w.CreatedDate),
		UpdatedDate:        ParseTimestampString(w.UpdatedDate),
		WorkType:           w.Type,
		Language:           w.Language,
		IsRetracted:        boolToUint8(w.IsRetracted),
		CitedByCount:       ClampUint32(w.CitedByCount),
		RefWorksCount:      ClampUint32(w.ReferencedWorksCount),
		AuthorsCount:       ClampUint16(w.AuthorsCount),
		FWCI:               fwciPtr,
		PrimaryTopicID:     pTopicID,
		PrimaryTopicScore:  pTopicScore,
		PrimarySubfieldID:  pSubfieldID,
		PrimaryFieldID:     pFieldID,
		PrimaryDomainID:    pDomainID,
		PrimarySourceID:    pSourceID,
		PrimarySourceType:  pSourceType,
		PrimaryIsOA:        pIsOA,
		FirstAuthorCountry: firstCountry,
		CountryCodes:       countries,
		AuthorIDs:          authorIDs,
		InstitutionIDs:     instIDs,
		TopicIDs:           topicIDs,
		TopicScores:        topicScores,
		Keywords:           keywords,
		CitedByYears:       citedYears,
		CitedByYearCounts:  citedCounts,
	})
}

func reportGCStats(b *testing.B, before, after *runtime.MemStats) {
	b.Helper()
	if b.N <= 0 {
		return
	}
	gcCount := float64(after.NumGC - before.NumGC)
	gcPauseNs := float64(after.PauseTotalNs - before.PauseTotalNs)
	b.ReportMetric(gcCount/float64(b.N), "gc/op")
	b.ReportMetric(gcPauseNs/float64(b.N), "gc-pause-ns/op")
}
func TestNaiveAndFastAbstractParity(t *testing.T) {
	t.Parallel()
	inv := map[string][]int{
		"Presentation": {0}, "of": {1}, "NaturaQui": {2}, "at": {3, 5}, "SKiM": {4}, "Italy": {6},
	}
	gotNaive := reconstructAbstractNaive(inv)
	gotFast := ReconstructAbstract(inv)
	if gotNaive != gotFast {
		t.Fatalf("naive=%q != fast=%q", gotNaive, gotFast)
	}
}

// --- Metric #4: Paired Microbenchmarks (Before vs After on 10,000+ records) ---

func BenchmarkPair1_JSONUnmarshal(b *testing.B) {
	ds := getBenchDataset(b)
	b.Logf("dataset: %s (%d works, %.2f MiB JSON)", ds.sourceLabel, len(ds.workJSONs), float64(ds.totalJSONBytes)/(1<<20))

	b.Run("Before_StdEncodingJSON", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(ds.totalJSONBytes)
		var mBefore, mAfter runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&mBefore)
		b.ResetTimer()

		var matched int
		for range b.N {
			for _, raw := range ds.workJSONs {
				var jw JSONWork
				if err := json.Unmarshal(raw, &jw); err == nil && jw.ID != "" {
					matched++
				}
			}
		}

		b.StopTimer()
		runtime.ReadMemStats(&mAfter)
		benchIntSink = matched
		reportGCStats(b, &mBefore, &mAfter)
	})

	b.Run("After_BytedanceSonic", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(ds.totalJSONBytes)
		var mBefore, mAfter runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&mBefore)
		b.ResetTimer()

		var matched int
		for range b.N {
			for _, raw := range ds.workJSONs {
				var jw JSONWork
				if err := sonic.Unmarshal(raw, &jw); err == nil && jw.ID != "" {
					matched++
				}
			}
		}

		b.StopTimer()
		runtime.ReadMemStats(&mAfter)
		benchIntSink = matched
		reportGCStats(b, &mBefore, &mAfter)
	})
}

func BenchmarkPair2_PreFilter(b *testing.B) {
	ds := getBenchDataset(b)

	b.Run("Before_FullJSONParsePrimaryTopic", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(ds.totalPrimaryTopicBytes)
		var mBefore, mAfter runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&mBefore)
		b.ResetTimer()

		var csCount int
		for range b.N {
			for _, raw := range ds.primaryTopicJSONs {
				var pt JSONPrimaryTopic
				if err := sonic.Unmarshal(raw, &pt); err == nil && pt.Field != nil && pt.Field.ID == csFieldID {
					csCount++
				}
			}
		}

		b.StopTimer()
		runtime.ReadMemStats(&mAfter)
		benchIntSink = csCount
		reportGCStats(b, &mBefore, &mAfter)
	})

	b.Run("After_EarlyBytesContains", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(ds.totalPrimaryTopicBytes)
		var mBefore, mAfter runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&mBefore)
		b.ResetTimer()

		var csCount int
		for range b.N {
			for _, raw := range ds.primaryTopicJSONs {
				if PrimaryTopicBytesIsCS(raw) {
					csCount++
				}
			}
		}

		b.StopTimer()
		runtime.ReadMemStats(&mAfter)
		benchIntSink = csCount
		reportGCStats(b, &mBefore, &mAfter)
	})
}

func BenchmarkPair3_AbstractAssembly(b *testing.B) {
	ds := getBenchDataset(b)

	b.Run("Before_NaiveSortSlice", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(ds.totalAbstractBytes)
		var mBefore, mAfter runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&mBefore)
		b.ResetTimer()

		var totalLen int
		for range b.N {
			for _, inv := range ds.abstractMaps {
				totalLen += len(reconstructAbstractNaive(inv))
			}
		}

		b.StopTimer()
		runtime.ReadMemStats(&mAfter)
		benchIntSink = totalLen
		reportGCStats(b, &mBefore, &mAfter)
	})

	b.Run("After_LinearPoolAndGrow", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(ds.totalAbstractBytes)
		var mBefore, mAfter runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&mBefore)
		b.ResetTimer()

		var totalLen int
		for range b.N {
			for _, inv := range ds.abstractMaps {
				totalLen += len(ReconstructAbstract(inv))
			}
		}

		b.StopTimer()
		runtime.ReadMemStats(&mAfter)
		benchIntSink = totalLen
		reportGCStats(b, &mBefore, &mAfter)
	})
}

func BenchmarkPair4_BatchLayout(b *testing.B) {
	ds := getBenchDataset(b)

	b.Run("Before_AoSNoPool", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(ds.batchApproxBytes)
		var mBefore, mAfter runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&mBefore)
		b.ResetTimer()

		var rows int
		for range b.N {
			var batch []aosWork
			for i := range ds.csWorks {
				batch = appendAoSWork(&ds.csWorks[i], batch)
			}
			rows += len(batch)
			benchAoSSink = batch
		}

		b.StopTimer()
		runtime.ReadMemStats(&mAfter)
		benchIntSink = rows
		reportGCStats(b, &mBefore, &mAfter)
	})

	b.Run("After_SoAWithPool", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(ds.batchApproxBytes)
		pool := sink.NewWorksBatchPool(len(ds.csWorks), sink.DefaultMaxPooledCap)
		norm := NewNormalizer(nil)
		pool.Put(pool.Get())

		var mBefore, mAfter runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&mBefore)
		b.ResetTimer()

		var rows int
		for range b.N {
			batch := pool.Get()
			for i := range ds.csWorks {
				norm.AppendJSONWork(&ds.csWorks[i], batch)
			}
			rows += batch.Len()
			pool.Put(batch)
		}

		b.StopTimer()
		runtime.ReadMemStats(&mAfter)
		benchIntSink = rows
		reportGCStats(b, &mBefore, &mAfter)
	})
}
