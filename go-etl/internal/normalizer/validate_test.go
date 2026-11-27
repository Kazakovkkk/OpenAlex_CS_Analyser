package normalizer

import (
	"math"
	"slices"
	"testing"
	"time"

	"go-etl/internal/sink"
)

func validWork() *ParquetWork {
	return &ParquetWork{
		ID:              ptr("https://openalex.org/W1001"),
		Type:            ptr("article"),
		UpdatedDate:     ptr(int64(1_791_946_884_000_000)),
		IsRetracted:     ptr(false),
		PublicationYear: ptr(int32(2020)),
		PrimaryTopic: &ParquetPrimaryTopic{
			ID:    ptr("https://openalex.org/T10001"),
			Field: &ParquetIDRef{ID: ptr("https://openalex.org/fields/17")},
		},
	}
}

func TestPrepareParquetRules(t *testing.T) {
	t.Parallel()
	n := NewNormalizer(nil, WithSnapshotYear(2026))

	dateDays := func(y int, m time.Month, d int) int32 {
		return int32(time.Date(y, m, d, 0, 0, 0, 0, time.UTC).Unix() / 86400)
	}

	cases := []struct {
		name   string
		mutate func(*ParquetWork)
		want   RejectReason
	}{
		{"valid", func(*ParquetWork) {}, ""},
		{"bad work id", func(w *ParquetWork) { w.ID = ptr("broken") }, RejectBadWorkID},
		{"missing work id", func(w *ParquetWork) { w.ID = nil }, RejectBadWorkID},
		{"missing updated_date", func(w *ParquetWork) { w.UpdatedDate = nil }, RejectBadUpdatedDate},
		{"zero updated_date", func(w *ParquetWork) { w.UpdatedDate = ptr(int64(0)) }, RejectBadUpdatedDate},
		{"missing is_retracted", func(w *ParquetWork) { w.IsRetracted = nil }, RejectBadIsRetracted},
		{"empty type", func(w *ParquetWork) { w.Type = ptr("") }, RejectBadType},
		{"no year and no date", func(w *ParquetWork) { w.PublicationYear = nil }, RejectBadYear},
		{"year zero and no date", func(w *ParquetWork) { w.PublicationYear = ptr(int32(0)) }, RejectBadYear},
		{"year after snapshot", func(w *ParquetWork) { w.PublicationYear = ptr(int32(2027)) }, RejectBadYear},
		{"year before 1", func(w *ParquetWork) { w.PublicationYear = ptr(int32(-5)) }, RejectBadYear},
		{
			"year and date disagree",
			func(w *ParquetWork) {
				w.PublicationYear = ptr(int32(2020))
				w.PublicationDate = ptr(dateDays(2021, 5, 1))
			},
			RejectYearDateMismatch,
		},
		{"no topics at all", func(w *ParquetWork) {
			w.PrimaryTopic = &ParquetPrimaryTopic{Field: &ParquetIDRef{ID: ptr("https://openalex.org/fields/17")}}
		}, RejectMissingTopics},
		{"no field", func(w *ParquetWork) { w.PrimaryTopic = &ParquetPrimaryTopic{ID: ptr("https://openalex.org/T1")} }, RejectMissingField},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := validWork()
			tc.mutate(w)
			if got := n.PrepareParquet(w); got != tc.want {
				t.Fatalf("PrepareParquet = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestPrepareParquetDerivesYearFromDate(t *testing.T) {
	t.Parallel()
	n := NewNormalizer(nil, WithSnapshotYear(2026))

	w := validWork()
	w.PublicationYear = nil
	w.PublicationDate = ptr(int32(20679)) // 2026-08-14
	if reason := n.PrepareParquet(w); reason != "" {
		t.Fatalf("PrepareParquet = %q, want accepted", reason)
	}
	if w.PublicationYear == nil || *w.PublicationYear != 2026 {
		t.Fatalf("derived publication_year = %v, want 2026", w.PublicationYear)
	}

	w2 := validWork()
	w2.PublicationYear = nil
	w2.PublicationDate = ptr(int32(0))
	if reason := n.PrepareParquet(w2); reason != "" {
		t.Fatalf("1970 date must be derived, got %q", reason)
	}
	if *w2.PublicationYear != 1970 {
		t.Fatalf("derived year = %d, want 1970", *w2.PublicationYear)
	}
}

func TestPre1900YearsAreValid(t *testing.T) {
	t.Parallel()
	n := NewNormalizer(nil, WithSnapshotYear(2026))

	w := validWork()
	w.PublicationYear = ptr(int32(1400))
	w.PublicationDate = ptr(int32(-208188)) // 1400-01-01
	if reason := n.PrepareParquet(w); reason != "" {
		t.Fatalf("1400 with matching date rejected: %q", reason)
	}
}

func TestPrepareJSONRules(t *testing.T) {
	t.Parallel()
	n := NewNormalizer(nil, WithSnapshotYear(2026))

	valid := func() *JSONWork {
		return &JSONWork{
			ID:              "https://openalex.org/W2002",
			Type:            "preprint",
			UpdatedDate:     "2026-10-14T12:00:00.000000",
			IsRetracted:     ptr(false),
			PublicationYear: 2024,
			PrimaryTopic: &JSONPrimaryTopic{
				ID:    "https://openalex.org/T10001",
				Field: &JSONIDRef{ID: "https://openalex.org/fields/17"},
			},
		}
	}

	if reason := n.PrepareJSON(valid()); reason != "" {
		t.Fatalf("valid JSON record rejected: %q", reason)
	}

	noUpdated := valid()
	noUpdated.UpdatedDate = ""
	if reason := n.PrepareJSON(noUpdated); reason != RejectBadUpdatedDate {
		t.Fatalf("missing updated_date: %q", reason)
	}

	noRetracted := valid()
	noRetracted.IsRetracted = nil
	if reason := n.PrepareJSON(noRetracted); reason != RejectBadIsRetracted {
		t.Fatalf("missing is_retracted: %q", reason)
	}

	// Year comes from the date when the scalar is absent.
	fromDate := valid()
	fromDate.PublicationYear = 0
	fromDate.PublicationDate = "2023-11-20"
	if reason := n.PrepareJSON(fromDate); reason != "" {
		t.Fatalf("derivable year rejected: %q", reason)
	}
	if fromDate.PublicationYear != 2023 {
		t.Fatalf("derived year = %d, want 2023", fromDate.PublicationYear)
	}
}

func TestCorpusGate(t *testing.T) {
	t.Parallel()

	cs := &ParquetPrimaryTopic{Field: &ParquetIDRef{ID: ptr("https://openalex.org/fields/17")}}

	article := validWork()
	article.PrimaryTopic = cs
	if !IsCSParquetWork(article) {
		t.Fatal("article with field 17 must be in the corpus")
	}

	emptyType := validWork()
	emptyType.Type = ptr("")
	emptyType.PrimaryTopic = cs
	if !IsCSParquetWork(emptyType) {
		t.Fatal("empty type must reach validation (it is a bad_type rejection, not a filter)")
	}

	review := validWork()
	review.Type = ptr("review")
	review.PrimaryTopic = cs
	if IsCSParquetWork(review) {
		t.Fatal("review must stay outside the agreed corpus")
	}

	xpac := validWork()
	xpac.IsXPAC = ptr(true)
	xpac.PrimaryTopic = cs
	if IsCSParquetWork(xpac) {
		t.Fatal("xpac records must be filtered out")
	}

	noField := validWork()
	noField.PrimaryTopic = &ParquetPrimaryTopic{ID: ptr("https://openalex.org/T1")}
	if IsCSParquetWork(noField) {
		t.Fatal("a record without a field id cannot be known to be CS")
	}
}

func TestTopicDedupAndScoreNormalisation(t *testing.T) {
	t.Parallel()
	n := NewNormalizer(nil, WithSnapshotYear(2026))

	w := validWork()
	w.Topics = []ParquetWorkTopic{
		{ID: ptr("https://openalex.org/T10001"), Score: ptr(float32(0.7))},
		{ID: ptr("https://openalex.org/T10001"), Score: ptr(float32(0.9))}, // repeat: first wins
		{ID: ptr("https://openalex.org/T10002"), Score: ptr(float32(math.NaN()))},
		{ID: ptr("https://openalex.org/T10003"), Score: ptr(float32(-1))},
		{ID: ptr("https://openalex.org/T10004"), Score: nil},
		{ID: ptr("broken"), Score: ptr(float32(0.5))},
	}

	b := sink.NewWorksBatch(1)
	if reason := n.PrepareParquet(w); reason != "" {
		t.Fatalf("PrepareParquet = %q, want accepted", reason)
	}
	if got := n.AppendParquetWork(w, b); got != OutcomeAdded {
		t.Fatalf("AppendParquetWork = %v, want added", got)
	}

	wantIDs := []uint32{10001, 10002, 10003, 10004}
	if !slices.Equal(b.TopicIDs[0], wantIDs) {
		t.Fatalf("topic ids = %v, want %v", b.TopicIDs[0], wantIDs)
	}
	wantScores := []float32{0.7, 0, 0, 0}
	if !slices.Equal(b.TopicScores[0], wantScores) {
		t.Fatalf("topic scores = %v, want %v", b.TopicScores[0], wantScores)
	}
	if len(b.TopicIDs[0]) != len(b.TopicScores[0]) {
		t.Fatal("topic arrays must stay equal in length")
	}
}

func TestKnownWorkLeavingCorpusIsWritten(t *testing.T) {
	t.Parallel()
	known := map[uint64]struct{}{1001: {}}
	n := NewNormalizer(nil, WithSnapshotYear(2026), WithKnownWork(func(id uint64) bool {
		_, ok := known[id]
		return ok
	}))

	w := validWork()
	w.PrimaryTopic = nil
	if reason := n.PrepareParquet(w); reason != "" {
		t.Fatalf("known work rejected: %q", reason)
	}
	b := sink.NewWorksBatch(1)
	if got := n.AppendParquetWork(w, b); got != OutcomeAdded {
		t.Fatalf("known work not written: %v", got)
	}
	if b.PrimaryFieldIDs[0] != 0 || len(b.TopicIDs[0]) != 0 {
		t.Fatalf("supersede row = field %d, %d topics; want 0/0", b.PrimaryFieldIDs[0], len(b.TopicIDs[0]))
	}

	unknown := validWork()
	unknown.ID = ptr("https://openalex.org/W9999")
	unknown.PrimaryTopic = nil
	if got := n.AppendParquetWork(unknown, sink.NewWorksBatch(1)); got != OutcomeFilteredOut {
		t.Fatalf("unknown out-of-corpus work = %v, want filtered", got)
	}
}
