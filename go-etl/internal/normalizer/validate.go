package normalizer

import "strings"

type RejectReason string

const (
	RejectParseError       RejectReason = "parse_error"
	RejectBadWorkID        RejectReason = "bad_work_id"
	RejectBadUpdatedDate   RejectReason = "bad_updated_date"
	RejectBadIsRetracted   RejectReason = "bad_is_retracted"
	RejectBadType          RejectReason = "bad_type"
	RejectBadYear          RejectReason = "bad_year"
	RejectYearDateMismatch RejectReason = "year_date_mismatch"
	RejectMissingField     RejectReason = "missing_field"
	RejectMissingTopics    RejectReason = "missing_topics"
)

var corpusTypes = map[string]struct{}{
	"article":          {},
	"book-chapter":     {},
	"conference-paper": {},
	"data-paper":       {},
	"software-paper":   {},
	"preprint":         {},
}

func IsCorpusType(t string) bool {
	_, ok := corpusTypes[strings.ToLower(strings.TrimSpace(t))]
	return ok
}

func corpusTypeAllows(t string) bool {
	t = strings.ToLower(strings.TrimSpace(t))
	return t == "" || IsCorpusType(t)
}

type Option func(*Normalizer)

func WithSnapshotYear(year int) Option {
	return func(n *Normalizer) {
		if year > 0 {
			n.snapshotYear = year
		}
	}
}

func WithKnownWork(known func(workID uint64) bool) Option {
	return func(n *Normalizer) { n.knownWork = known }
}

func (n *Normalizer) PrepareParquet(w *ParquetWork) RejectReason {
	if w == nil {
		return RejectParseError
	}
	workID := ParseWorkID(derefString(w.ID))
	if workID == 0 {
		return RejectBadWorkID
	}
	if w.UpdatedDate == nil || *w.UpdatedDate <= 0 {
		return RejectBadUpdatedDate
	}
	if w.IsRetracted == nil {
		return RejectBadIsRetracted
	}
	if strings.TrimSpace(derefString(w.Type)) == "" {
		return RejectBadType
	}

	year, reason := n.resolveParquetYear(w)
	if reason != "" {
		return reason
	}
	if w.PublicationYear == nil || *w.PublicationYear <= 0 {
		derived := int32(year)
		w.PublicationYear = &derived
	}

	if n.knownWork != nil && n.knownWork(workID) {
		return ""
	}
	if parquetFieldMissing(w) {
		return RejectMissingField
	}
	if parquetTopicsMissing(w) {
		return RejectMissingTopics
	}
	return ""
}

func (n *Normalizer) PrepareJSON(w *JSONWork) RejectReason {
	if w == nil {
		return RejectParseError
	}
	workID := ParseWorkID(w.ID)
	if workID == 0 {
		return RejectBadWorkID
	}
	if _, ok := ParseUpdatedDate(w.UpdatedDate); !ok {
		return RejectBadUpdatedDate
	}
	if w.IsRetracted == nil {
		return RejectBadIsRetracted
	}
	if strings.TrimSpace(w.Type) == "" {
		return RejectBadType
	}

	year, reason := n.resolveJSONYear(w)
	if reason != "" {
		return reason
	}
	if w.PublicationYear <= 0 {
		w.PublicationYear = int32(year)
	}

	if n.knownWork != nil && n.knownWork(workID) {
		return ""
	}
	if jsonFieldMissing(w) {
		return RejectMissingField
	}
	if jsonTopicsMissing(w) {
		return RejectMissingTopics
	}
	return ""
}

func (n *Normalizer) resolveParquetYear(w *ParquetWork) (int, RejectReason) {
	hasField := w.PublicationYear != nil && *w.PublicationYear > 0
	dateYear, hasDate := 0, false
	if w.PublicationDate != nil {
		if t, ok := ParseDateDays(*w.PublicationDate); ok {
			dateYear, hasDate = t.Year(), true
		}
	}

	var year int
	switch {
	case hasField && hasDate:
		if int(*w.PublicationYear) != dateYear {
			return 0, RejectYearDateMismatch
		}
		year = int(*w.PublicationYear)
	case hasField:
		year = int(*w.PublicationYear)
	case hasDate:
		year = dateYear
	default:
		return 0, RejectBadYear
	}
	if !n.yearInRange(year) {
		return 0, RejectBadYear
	}
	return year, ""
}

func (n *Normalizer) resolveJSONYear(w *JSONWork) (int, RejectReason) {
	hasField := w.PublicationYear > 0
	dateYear, hasDate := 0, false
	if t, ok := ParseDate32String(w.PublicationDate); ok {
		dateYear, hasDate = t.Year(), true
	}

	var year int
	switch {
	case hasField && hasDate:
		if int(w.PublicationYear) != dateYear {
			return 0, RejectYearDateMismatch
		}
		year = int(w.PublicationYear)
	case hasField:
		year = int(w.PublicationYear)
	case hasDate:
		year = dateYear
	default:
		return 0, RejectBadYear
	}
	if !n.yearInRange(year) {
		return 0, RejectBadYear
	}
	return year, ""
}

func (n *Normalizer) yearInRange(year int) bool {
	if year < 1 {
		return false
	}
	if n.snapshotYear > 0 && year > n.snapshotYear {
		return false
	}
	return true
}

func parquetFieldMissing(w *ParquetWork) bool {
	if w.PrimaryTopic == nil || w.PrimaryTopic.Field == nil {
		return true
	}
	return ParseFieldID(derefString(w.PrimaryTopic.Field.ID)) == 0
}

func parquetTopicsMissing(w *ParquetWork) bool {
	if w.PrimaryTopic != nil && ParseTopicID(derefString(w.PrimaryTopic.ID)) != 0 {
		return false
	}
	for i := range w.Topics {
		if ParseTopicID(derefString(w.Topics[i].ID)) != 0 {
			return false
		}
	}
	return true
}

func jsonFieldMissing(w *JSONWork) bool {
	if w.PrimaryTopic == nil || w.PrimaryTopic.Field == nil {
		return true
	}
	return ParseFieldID(w.PrimaryTopic.Field.ID) == 0
}

func jsonTopicsMissing(w *JSONWork) bool {
	if w.PrimaryTopic != nil && ParseTopicID(w.PrimaryTopic.ID) != 0 {
		return false
	}
	for i := range w.Topics {
		if ParseTopicID(w.Topics[i].ID) != 0 {
			return false
		}
	}
	return true
}
