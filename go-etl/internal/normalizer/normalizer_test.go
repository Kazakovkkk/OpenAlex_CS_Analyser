package normalizer

import (
	"bytes"
	"errors"
	"io"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	"go-etl/internal/sink"
)

func ptr[T any](v T) *T {
	return &v
}

func TestWorkDeduplicatorConcurrent(t *testing.T) {
	t.Parallel()

	d := NewWorkDeduplicator()
	const (
		workers   = 8
		uniqueIDs = 500
		zeroCalls = 10
	)

	var addedNonZero atomic.Uint64
	var addedZero atomic.Uint64

	var wg sync.WaitGroup
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			for id := uint64(1); id <= uniqueIDs; id++ {
				if d.AddIfNew(id) {
					addedNonZero.Add(1)
				}
			}
			for range zeroCalls {
				if d.AddIfNew(0) {
					addedZero.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	if got := addedNonZero.Load(); got != uniqueIDs {
		t.Fatalf("addedNonZero = %d, want %d", got, uniqueIDs)
	}
	if got := addedZero.Load(); got != workers*zeroCalls {
		t.Fatalf("addedZero = %d, want %d (workID=0 must never be deduped)", got, workers*zeroCalls)
	}
	if got := d.Len(); got != uniqueIDs {
		t.Fatalf("Len() = %d, want %d", got, uniqueIDs)
	}
	if d.SizeInBytes() == 0 {
		t.Fatal("SizeInBytes() = 0 for non-empty deduplicator")
	}

	d.Clear()
	if d.Len() != 0 {
		t.Fatalf("Len() after Clear = %d, want 0", d.Len())
	}
	if !d.AddIfNew(42) {
		t.Fatal("AddIfNew(42) after Clear = false, want true")
	}
}

func TestPreFilters(t *testing.T) {
	t.Parallel()
	if !PrimaryTopicBytesIsCS([]byte(`{"field":{"id":"https://openalex.org/fields/17"}}`)) {
		t.Fatal("expected CS primary_topic to match")
	}
	if PrimaryTopicBytesIsCS([]byte(`{"subfield":{"id":"https://openalex.org/subfields/1702"},"field":{"id":"https://openalex.org/fields/19"}}`)) {
		t.Fatal("subfields/1702 must NOT falsely match fields/17")
	}
	if PrimaryTopicBytesIsCS([]byte(`{"field":{"id":"https://openalex.org/fields/170"}}`)) {
		t.Fatal("fields/170 must NOT falsely match fields/17")
	}
	if PrimaryTopicBytesIsCS([]byte(`{"field":{"id":"https://openalex.org/fields/23"}}`)) {
		t.Fatal("fields/23 must NOT match fields/17")
	}
	if !FastJSONPreFilter([]byte(`{"is_xpac":false,"primary_topic":{"field":{"id":"https://openalex.org/fields/17"}}}`)) {
		t.Fatal("expected valid CS non-xpac JSON to pass FastJSONPreFilter")
	}
	if FastJSONPreFilter([]byte(`{"is_xpac":true,"primary_topic":{"field":{"id":"https://openalex.org/fields/17"}}}`)) {
		t.Fatal("compact is_xpac:true must be rejected")
	}
	if FastJSONPreFilter([]byte(`{"is_xpac": true,"primary_topic":{"field":{"id":"https://openalex.org/fields/17"}}}`)) {
		t.Fatal("spaced is_xpac: true must be rejected")
	}
	if FastJSONPreFilter([]byte(`{"is_xpac":false,"primary_topic":{"field":{"id":"https://openalex.org/fields/23"}}}`)) {
		t.Fatal("non-CS JSON must be rejected")
	}
	secondaryCS := []byte(`{
		"id": "https://openalex.org/W999",
		"is_xpac": false,
		"primary_topic": {"id": "https://openalex.org/T1", "field": {"id": "https://openalex.org/fields/23"}},
		"topics": [{"id": "https://openalex.org/T2", "field": {"id": "https://openalex.org/fields/17"}}]
	}`)
	n := NewNormalizer(nil)
	b := sink.NewWorksBatch(4)
	outcome, err := n.AppendRawJSONWork(secondaryCS, b)
	if err != nil {
		t.Fatalf("AppendRawJSONWork unexpected error: %v", err)
	}
	if outcome != OutcomeFilteredOut || b.Len() != 0 {
		t.Fatalf("secondary-only CS work: outcome=%d len=%d, want OutcomeFilteredOut and len=0", outcome, b.Len())
	}

	// Nil and xpac ParquetWork checks.
	if IsCSParquetWork(nil) {
		t.Fatal("IsCSParquetWork(nil) = true, want false")
	}
	xpacCS := &ParquetWork{
		IsXPAC: ptr(true),
		PrimaryTopic: &ParquetPrimaryTopic{
			Field: &ParquetIDRef{ID: ptr("https://openalex.org/fields/17")},
		},
	}
	if IsCSParquetWork(xpacCS) {
		t.Fatal("IsCSParquetWork(xpac=true) = true, want false")
	}
}

func TestParquetAndJSONParity(t *testing.T) {
	t.Parallel()

	pubDate := time.Date(2024, 5, 10, 0, 0, 0, 0, time.UTC)
	pubDays := int32(pubDate.Unix() / 86400)
	updated := time.Date(2026, 4, 11, 6, 19, 8, 300824000, time.UTC)

	pw := &ParquetWork{
		ID:              ptr("https://openalex.org/W2741809807"),
		PublicationYear: ptr(int32(2024)),
		PublicationDate: ptr(pubDays),
		UpdatedDate:     ptr(updated.UnixMicro()),
		Type:            ptr("article"),
		IsRetracted:     ptr(false),
		IsXPAC:          ptr(false),
		FWCI:            ptr(2.75),
		PrimaryTopic: &ParquetPrimaryTopic{
			ID:    ptr("https://openalex.org/T12799"),
			Field: &ParquetIDRef{ID: ptr("https://openalex.org/fields/17")},
		},
		PrimaryLocation: &ParquetPrimaryLocation{
			Source: &ParquetPrimarySource{
				ID:   ptr("https://openalex.org/S4306402476"),
				Type: ptr("journal"),
			},
		},
		Topics: []ParquetWorkTopic{
			{ID: ptr("https://openalex.org/T12799"), Score: ptr(float32(0.95))},
			{ID: ptr("https://openalex.org/T10616"), Score: ptr(float32(0.42))},
		},
	}

	rawJSON := []byte(`{
		"id": "https://openalex.org/W2741809807",
		"publication_year": 2024,
		"publication_date": "2024-05-10",
		"updated_date": "` + updated.Format(time.RFC3339Nano) + `",
		"type": "article",
		"is_retracted": false,
		"is_xpac": false,
		"fwci": 2.75,
		"primary_topic": {
			"id": "https://openalex.org/T12799",
			"field": {"id": "https://openalex.org/fields/17"}
		},
		"primary_location": {
			"source": {"id": "https://openalex.org/S4306402476", "type": "journal"}
		},
		"topics": [
			{"id": "https://openalex.org/T12799", "score": 0.95},
			{"id": "https://openalex.org/T10616", "score": 0.42}
		]
	}`)

	nNoDedup := NewNormalizer(nil)
	bP := sink.NewWorksBatch(4)
	bJ := sink.NewWorksBatch(4)

	if out := nNoDedup.AppendParquetWork(pw, bP); out != OutcomeAdded {
		t.Fatalf("AppendParquetWork outcome = %d, want OutcomeAdded", out)
	}
	if out, err := nNoDedup.AppendRawJSONWork(rawJSON, bJ); err != nil || out != OutcomeAdded {
		t.Fatalf("AppendRawJSONWork = (%d, %v), want (OutcomeAdded, nil)", out, err)
	}

	assertWorksBatchColumnLengths(t, bP, 1)
	assertWorksBatchColumnLengths(t, bJ, 1)
	if bP.WorkIDs[0] != 2741809807 || bP.WorkIDs[0] != bJ.WorkIDs[0] {
		t.Fatalf("WorkIDs mismatch: P=%d J=%d", bP.WorkIDs[0], bJ.WorkIDs[0])
	}
	if bP.WorkTypes[0] != "article" || bJ.WorkTypes[0] != "article" {
		t.Fatalf("WorkTypes mismatch: P=%q J=%q", bP.WorkTypes[0], bJ.WorkTypes[0])
	}
	if bP.PubYears[0] != 2024 || bJ.PubYears[0] != 2024 {
		t.Fatalf("PubYears mismatch: P=%d J=%d", bP.PubYears[0], bJ.PubYears[0])
	}
	if bP.PubDates[0] == nil || bJ.PubDates[0] == nil || !bP.PubDates[0].Equal(pubDate) || !bJ.PubDates[0].Equal(pubDate) {
		t.Fatalf("PubDates mismatch: P=%v J=%v", bP.PubDates[0], bJ.PubDates[0])
	}
	if !bP.UpdatedDates[0].Equal(updated) || !bJ.UpdatedDates[0].Equal(updated) {
		t.Fatalf("UpdatedDates mismatch: P=%v J=%v", bP.UpdatedDates[0], bJ.UpdatedDates[0])
	}
	if bP.IsRetracted[0] != 0 || bJ.IsRetracted[0] != 0 {
		t.Fatalf("IsRetracted mismatch: P=%d J=%d", bP.IsRetracted[0], bJ.IsRetracted[0])
	}
	if bP.FWCIs[0] == nil || bJ.FWCIs[0] == nil || *bP.FWCIs[0] != 2.75 || *bJ.FWCIs[0] != 2.75 {
		t.Fatalf("FWCIs mismatch: P=%v J=%v", bP.FWCIs[0], bJ.FWCIs[0])
	}
	if bP.PrimaryTopicIDs[0] != 12799 || bJ.PrimaryTopicIDs[0] != 12799 ||
		bP.PrimaryFieldIDs[0] != 17 || bJ.PrimaryFieldIDs[0] != 17 ||
		bP.PrimarySourceIDs[0] != 4306402476 || bJ.PrimarySourceIDs[0] != 4306402476 ||
		bP.PrimarySourceTypes[0] != "journal" || bJ.PrimarySourceTypes[0] != "journal" {
		t.Fatal("primary topic/location mismatch between Parquet and JSON")
	}
	if !slices.Equal(bP.TopicIDs[0], []uint32{12799, 10616}) || !slices.Equal(bJ.TopicIDs[0], []uint32{12799, 10616}) {
		t.Fatalf("TopicIDs: P=%v J=%v", bP.TopicIDs[0], bJ.TopicIDs[0])
	}
	if !slices.Equal(bP.TopicScores[0], []float32{0.95, 0.42}) || !slices.Equal(bJ.TopicScores[0], []float32{0.95, 0.42}) {
		t.Fatalf("TopicScores: P=%v J=%v", bP.TopicScores[0], bJ.TopicScores[0])
	}
	if out, err := nNoDedup.AppendRawJSONWork(rawJSON, bJ); err != nil || out != OutcomeAdded || bJ.Len() != 2 {
		t.Fatalf("nil dedup should allow repeated work: out=%d err=%v len=%d", out, err, bJ.Len())
	}

	nWithDedup := NewNormalizer(NewWorkDeduplicator())
	bD := sink.NewWorksBatch(4)
	if out := nWithDedup.AppendParquetWork(pw, bD); out != OutcomeAdded {
		t.Fatalf("first dedup call = %d, want OutcomeAdded", out)
	}
	if out := nWithDedup.AppendParquetWork(pw, bD); out != OutcomeDuplicate || bD.Len() != 1 {
		t.Fatalf("second dedup call = %d len=%d, want OutcomeDuplicate len=1", out, bD.Len())
	}
}

func TestAppendPreservesRawFieldsWithoutValidation(t *testing.T) {
	t.Parallel()

	csTopic := &ParquetPrimaryTopic{
		ID:    ptr("https://openalex.org/T10001"),
		Field: &ParquetIDRef{ID: ptr("https://openalex.org/fields/17")},
	}

	n := NewNormalizer(NewWorkDeduplicator())
	b := sink.NewWorksBatch(8)
	w1400 := &ParquetWork{
		ID:              ptr("https://openalex.org/W1001"),
		PublicationYear: ptr(int32(1400)),
		PublicationDate: ptr(int32(-208188)),
		PrimaryTopic:    csTopic,
	}
	wFuture := &JSONWork{
		ID:              "https://openalex.org/W1002",
		PublicationYear: 2099,
		PublicationDate: "2500-01-01",
		PrimaryTopic:    &JSONPrimaryTopic{Field: &JSONIDRef{ID: "https://openalex.org/fields/17"}},
	}
	wDirty := &ParquetWork{
		ID:           ptr("broken-id"),
		Type:         ptr(""),
		IsRetracted:  ptr(true),
		FWCI:         ptr(math.NaN()),
		PrimaryTopic: csTopic,
	}
	wDirty2 := &ParquetWork{
		ID:           ptr(""),
		PrimaryTopic: csTopic,
	}

	if n.AppendParquetWork(w1400, b) != OutcomeAdded ||
		n.AppendJSONWork(wFuture, b) != OutcomeAdded ||
		n.AppendParquetWork(wDirty, b) != OutcomeAdded ||
		n.AppendParquetWork(wDirty2, b) != OutcomeAdded {
		t.Fatal("all 4 dirty CS records must be preserved for Task 1")
	}

	assertWorksBatchColumnLengths(t, b, 4)

	if b.PubYears[0] != 1400 || b.PubDates[0] == nil || b.PubDates[0].Format("2006-01-02") != "1400-01-01" {
		t.Fatalf("row 0 (1400): year=%d date=%v, want 1400 & 1400-01-01", b.PubYears[0], b.PubDates[0])
	}
	if b.PubYears[1] != 2099 || b.PubDates[1] == nil || b.PubDates[1].Format("2006-01-02") != "2500-01-01" {
		t.Fatalf("row 1 (2099): year=%d date=%v, want 2099 & 2500-01-01", b.PubYears[1], b.PubDates[1])
	}
	if b.WorkIDs[2] != 0 || b.PubYears[2] != 0 || b.PubDates[2] != nil || b.FWCIs[2] != nil ||
		b.IsRetracted[2] != 1 || b.WorkTypes[2] != "" {
		t.Fatalf("row 2 (dirty retracted record) not preserved properly")
	}
	if b.WorkIDs[3] != 0 {
		t.Fatalf("row 3 WorkID = %d, want 0", b.WorkIDs[3])
	}
}

func TestDimensionNormalizers(t *testing.T) {
	t.Parallel()

	tb := sink.NewTopicsBatch(4)
	if AppendParquetTopic(nil, tb) {
		t.Fatal("AppendParquetTopic(nil) = true, want false")
	}
	topic := &ParquetTopic{
		ID:          ptr("https://openalex.org/T10001"),
		DisplayName: ptr("Distributed Systems"),
		Subfield:    &ParquetNamedRef{ID: ptr("https://openalex.org/subfields/1705"), DisplayName: ptr("Computer Networks")},
		Field:       &ParquetNamedRef{ID: ptr("https://openalex.org/fields/17"), DisplayName: ptr("Computer Science")},
		Domain:      &ParquetNamedRef{ID: ptr("https://openalex.org/domains/3"), DisplayName: ptr("Physical Sciences")},
		WorksCount:  ptr(int32(50000)),
	}
	if !AppendParquetTopic(topic, tb) || tb.Len() != 1 {
		t.Fatalf("AppendParquetTopic failed: len=%d", tb.Len())
	}
	if tb.TopicIDs[0] != 10001 || tb.FieldIDs[0] != 17 || tb.DomainIDs[0] != 3 || !tb.UpdatedDates[0].Equal(unixEpochUTC) {
		t.Fatalf("unexpected TopicsBatch row 0: id=%d field=%d domain=%d upd=%v",
			tb.TopicIDs[0], tb.FieldIDs[0], tb.DomainIDs[0], tb.UpdatedDates[0])
	}

	ib := sink.NewInstitutionsBatch(4)
	if AppendParquetInstitution(nil, ib) {
		t.Fatal("AppendParquetInstitution(nil) = true, want false")
	}
	inst := &ParquetInstitution{
		ID:          ptr("https://openalex.org/I2799651439"),
		ROR:         ptr("https://ror.org/019cc4r33"),
		DisplayName: ptr("ETH Zurich"),
		Type:        ptr("education"),
		Lineage:     []string{"https://openalex.org/I2799651439", "bad-url", "https://openalex.org/I123"},
		Geo: &ParquetInstitutionGeo{
			City:        ptr("Zurich"),
			Country:     ptr("Switzerland"),
			CountryCode: ptr("ch"),
			Latitude:    ptr(47.3769),
			Longitude:   ptr(8.5417),
		},
		SummaryStats: &ParquetInstitutionStats{HIndex: ptr(int32(412))},
	}
	if !AppendParquetInstitution(inst, ib) || ib.Len() != 1 {
		t.Fatalf("AppendParquetInstitution failed: len=%d", ib.Len())
	}
	if ib.InstitutionIDs[0] != 2799651439 || ib.CountryCodes[0] != "CH" || ib.HIndexes[0] != 412 ||
		!slices.Equal(ib.LineageIDs[0], []uint64{2799651439, 123}) {
		t.Fatalf("unexpected InstitutionsBatch row 0: id=%d cc=%q h=%d lineage=%v",
			ib.InstitutionIDs[0], ib.CountryCodes[0], ib.HIndexes[0], ib.LineageIDs[0])
	}
}

func TestParquetStructTagsRoundTrip(t *testing.T) {
	t.Parallel()
	workIn := []ParquetWork{
		{
			ID:              ptr("https://openalex.org/W555"),
			Type:            ptr("article"),
			PublicationYear: ptr(int32(2026)),
			PublicationDate: ptr(int32(20454)),
			UpdatedDate:     ptr(int64(1775888348300824)),
			IsRetracted:     ptr(false),
			IsXPAC:          ptr(false),
			FWCI:            ptr(1.25),
			PrimaryTopic: &ParquetPrimaryTopic{
				ID:    ptr("https://openalex.org/T10001"),
				Field: &ParquetIDRef{ID: ptr("https://openalex.org/fields/17")},
			},
			Topics: []ParquetWorkTopic{{ID: ptr("https://openalex.org/T10001"), Score: ptr(float32(0.8))}},
		},
	}

	workOut := roundTripParquet(t, workIn)
	b := sink.NewWorksBatch(4)
	if out := NewNormalizer(nil).AppendParquetWork(&workOut[0], b); out != OutcomeAdded {
		t.Fatalf("round-trip AppendParquetWork = %d, want OutcomeAdded", out)
	}
	if b.WorkIDs[0] != 555 || b.WorkTypes[0] != "article" || b.PrimaryFieldIDs[0] != 17 {
		t.Fatalf("unexpected round-trip work: id=%d type=%q field=%d", b.WorkIDs[0], b.WorkTypes[0], b.PrimaryFieldIDs[0])
	}

	topicOut := roundTripParquet(t, []ParquetTopic{{
		ID:          ptr("https://openalex.org/T123"),
		DisplayName: ptr("AI"),
		UpdatedDate: ptr(int64(1791946884000000)),
	}})
	tb := sink.NewTopicsBatch(2)
	if !AppendParquetTopic(&topicOut[0], tb) || tb.TopicIDs[0] != 123 {
		t.Fatalf("round-trip topic failed: %+v", tb.TopicIDs)
	}

	instOut := roundTripParquet(t, []ParquetInstitution{{
		ID:          ptr("https://openalex.org/I456"),
		DisplayName: ptr("TU Delft"),
		Lineage:     []string{"https://openalex.org/I456"},
		UpdatedDate: ptr(int64(1790584817000000)),
	}})
	ib := sink.NewInstitutionsBatch(2)
	if !AppendParquetInstitution(&instOut[0], ib) || ib.InstitutionIDs[0] != 456 || !slices.Equal(ib.LineageIDs[0], []uint64{456}) {
		t.Fatalf("round-trip institution failed: ids=%v lineage=%v", ib.InstitutionIDs, ib.LineageIDs)
	}
}

func roundTripParquet[T any](t *testing.T, in []T) []T {
	t.Helper()

	var buf bytes.Buffer
	w := parquet.NewGenericWriter[T](&buf)
	if _, err := w.Write(in); err != nil {
		t.Fatalf("parquet Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("parquet Close: %v", err)
	}

	r := parquet.NewGenericReader[T](bytes.NewReader(buf.Bytes()))
	defer r.Close()

	out := make([]T, len(in))
	n, err := r.Read(out)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("parquet Read: %v", err)
	}
	if n != len(in) {
		t.Fatalf("parquet Read got %d rows, want %d", n, len(in))
	}
	return out
}

func assertWorksBatchColumnLengths(t *testing.T, b *sink.WorksBatch, want int) {
	t.Helper()

	cols := map[string]int{
		"WorkIDs":            len(b.WorkIDs),
		"UpdatedDates":       len(b.UpdatedDates),
		"WorkTypes":          len(b.WorkTypes),
		"PrimaryFieldIDs":    len(b.PrimaryFieldIDs),
		"IsRetracted":        len(b.IsRetracted),
		"PubYears":           len(b.PubYears),
		"PubDates":           len(b.PubDates),
		"PrimaryTopicIDs":    len(b.PrimaryTopicIDs),
		"TopicIDs":           len(b.TopicIDs),
		"TopicScores":        len(b.TopicScores),
		"PrimarySourceIDs":   len(b.PrimarySourceIDs),
		"PrimarySourceTypes": len(b.PrimarySourceTypes),
		"FWCIs":              len(b.FWCIs),
	}
	for name, got := range cols {
		if got != want {
			t.Fatalf("WorksBatch column %s len = %d, want %d", name, got, want)
		}
	}
}
