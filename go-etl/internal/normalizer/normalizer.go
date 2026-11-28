package normalizer

import (
	"bytes"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/bytedance/sonic"

	"go-etl/internal/sink"
)

const (
	csFieldID     = "https://openalex.org/fields/17"
	csFieldSuffix = "/fields/17"
)

var (
	csFieldJSONMarker = []byte(`/fields/17"`)
	xpacTrueCompact   = []byte(`"is_xpac":true`)
	xpacTrueSpaced    = []byte(`"is_xpac": true`)
)

type WorkOutcome uint8

const (
	OutcomeFilteredOut WorkOutcome = iota
	OutcomeDuplicate
	OutcomeAdded
	OutcomeRejected
)

type Normalizer struct {
	dedup        *WorkDeduplicator
	snapshotYear int
	knownWork    func(workID uint64) bool
}

func NewNormalizer(dedup *WorkDeduplicator, opts ...Option) *Normalizer {
	n := &Normalizer{dedup: dedup}
	for _, opt := range opts {
		opt(n)
	}
	return n
}

func PrimaryTopicBytesIsCS(rawPrimaryTopic []byte) bool {
	return bytes.Contains(rawPrimaryTopic, csFieldJSONMarker)
}

func FastJSONPreFilter(rawWorkJSON []byte) bool {
	if !bytes.Contains(rawWorkJSON, csFieldJSONMarker) {
		return false
	}
	if bytes.Contains(rawWorkJSON, xpacTrueCompact) || bytes.Contains(rawWorkJSON, xpacTrueSpaced) {
		return false
	}
	return true
}

func IsCSParquetWork(w *ParquetWork) bool {
	if w == nil || (w.IsXPAC != nil && *w.IsXPAC) {
		return false
	}
	if w.IsRetracted != nil && *w.IsRetracted {
		return false
	}
	if !corpusTypeAllows(derefString(w.Type)) {
		return false
	}
	if w.PrimaryTopic == nil || w.PrimaryTopic.Field == nil || w.PrimaryTopic.Field.ID == nil {
		return false
	}
	fid := *w.PrimaryTopic.Field.ID
	return fid == csFieldID || strings.HasSuffix(fid, csFieldSuffix)
}

func IsCSJSONWork(w *JSONWork) bool {
	if w == nil || w.IsXPAC {
		return false
	}
	if w.IsRetracted != nil && *w.IsRetracted {
		return false
	}
	if !corpusTypeAllows(w.Type) {
		return false
	}
	if w.PrimaryTopic == nil || w.PrimaryTopic.Field == nil {
		return false
	}
	fid := w.PrimaryTopic.Field.ID
	return fid == csFieldID || strings.HasSuffix(fid, csFieldSuffix)
}

// IsKnown reports whether the work id is already stored. Known works are
// written even when they leave the corpus, so FINAL selects the new version and
// analytics stops counting them.
func (n *Normalizer) IsKnown(workID uint64) bool {
	return n != nil && n.isKnown(workID)
}

// WantsParquet reports whether the row must reach the normalizer: either it is
// a new corpus candidate or a known work whose updated version must be stored.
func (n *Normalizer) WantsParquet(w *ParquetWork) bool {
	if w == nil {
		return false
	}
	return n.IsKnown(ParseWorkID(derefString(w.ID))) || IsCSParquetWork(w)
}

// WantsJSON is the REST equivalent of WantsParquet.
func (n *Normalizer) WantsJSON(w *JSONWork) bool {
	if w == nil {
		return false
	}
	return n.IsKnown(ParseWorkID(w.ID)) || IsCSJSONWork(w)
}

func (n *Normalizer) AppendParquetWork(w *ParquetWork, b *sink.WorksBatch) WorkOutcome {
	workID := ParseWorkID(derefString(w.ID))
	if !IsCSParquetWork(w) && !n.isKnown(workID) {
		return OutcomeFilteredOut
	}
	if n.dedup != nil && !n.dedup.AddIfNew(workID) {
		return OutcomeDuplicate
	}

	var pubDatePtr *time.Time
	if w.PublicationDate != nil {
		if t, ok := ParseParquetDate32(*w.PublicationDate); ok {
			pubDatePtr = b.AllocPubDate(t)
		}
	}
	updatedDate := unixEpochUTC
	if w.UpdatedDate != nil {
		updatedDate = ParseParquetTimestampMicros(*w.UpdatedDate)
	}

	var fwciPtr *float32
	if w.FWCI != nil && !math.IsNaN(*w.FWCI) && !math.IsInf(*w.FWCI, 0) {
		fwciPtr = b.AllocFWCI(float32(*w.FWCI))
	}

	var pubYear uint16
	if w.PublicationYear != nil {
		pubYear = ClampUint16(*w.PublicationYear)
	}
	var pTopicID uint32
	var pFieldID uint16
	if pt := w.PrimaryTopic; pt != nil {
		pTopicID = ParseTopicID(derefString(pt.ID))
		if pt.Field != nil {
			pFieldID = ParseFieldID(derefString(pt.Field.ID))
		}
	}
	// A service version of a known work (is_xpac) is written with
	// primary_field_id=0 so the FINAL-selected row drops out of the analysis.
	if n.isKnown(workID) && derefBool(w.IsXPAC) {
		pFieldID = 0
	}

	var pSourceID uint64
	var pSourceType string
	if pl := w.PrimaryLocation; pl != nil && pl.Source != nil {
		pSourceID = ParseSourceID(derefString(pl.Source.ID))
		pSourceType = derefString(pl.Source.Type)
	}

	topicIDs, topicScores := extractParquetTopics(w.Topics)

	b.WorkIDs = append(b.WorkIDs, workID)

	b.PubYears = append(b.PubYears, pubYear)
	b.PubDates = append(b.PubDates, pubDatePtr)
	b.UpdatedDates = append(b.UpdatedDates, updatedDate)

	b.WorkTypes = append(b.WorkTypes, derefString(w.Type))
	b.IsRetracted = append(b.IsRetracted, boolToUint8(derefBool(w.IsRetracted)))
	b.FWCIs = append(b.FWCIs, fwciPtr)

	b.PrimaryTopicIDs = append(b.PrimaryTopicIDs, pTopicID)
	b.PrimaryFieldIDs = append(b.PrimaryFieldIDs, pFieldID)
	b.PrimarySourceIDs = append(b.PrimarySourceIDs, pSourceID)
	b.PrimarySourceTypes = append(b.PrimarySourceTypes, pSourceType)

	b.TopicIDs = append(b.TopicIDs, topicIDs)
	b.TopicScores = append(b.TopicScores, topicScores)

	return OutcomeAdded
}

func (n *Normalizer) AppendRawJSONWork(raw []byte, b *sink.WorksBatch) (WorkOutcome, error) {
	var jw JSONWork
	if err := sonic.Unmarshal(raw, &jw); err != nil {
		return OutcomeFilteredOut, fmt.Errorf("sonic unmarshal work: %w", err)
	}
	if !n.WantsJSON(&jw) {
		return OutcomeFilteredOut, nil
	}
	return n.AppendJSONWork(&jw, b), nil
}

func (n *Normalizer) IngestRawJSON(raw []byte, b *sink.WorksBatch) (WorkOutcome, RejectReason) {
	var jw JSONWork
	if err := sonic.Unmarshal(raw, &jw); err != nil {
		return OutcomeRejected, RejectParseError
	}
	if !n.WantsJSON(&jw) {
		return OutcomeFilteredOut, ""
	}
	if reason := n.PrepareJSON(&jw); reason != "" {
		return OutcomeRejected, reason
	}
	return n.AppendJSONWork(&jw, b), ""
}

func (n *Normalizer) AppendJSONWork(w *JSONWork, b *sink.WorksBatch) WorkOutcome {
	workID := ParseWorkID(w.ID)
	if !IsCSJSONWork(w) && !n.isKnown(workID) {
		return OutcomeFilteredOut
	}
	if n.dedup != nil && !n.dedup.AddIfNew(workID) {
		return OutcomeDuplicate
	}

	var pubDatePtr *time.Time
	if t, ok := ParseDate32String(derefString(w.PublicationDate)); ok {
		pubDatePtr = b.AllocPubDate(t)
	}
	updatedDate := ParseTimestampString(derefString(w.UpdatedDate))

	var fwciPtr *float32
	if w.FWCI != nil && !math.IsNaN(*w.FWCI) && !math.IsInf(*w.FWCI, 0) {
		fwciPtr = b.AllocFWCI(float32(*w.FWCI))
	}

	var pTopicID uint32
	var pFieldID uint16
	if pt := w.PrimaryTopic; pt != nil {
		pTopicID = ParseTopicID(pt.ID)
		if pt.Field != nil {
			pFieldID = ParseFieldID(pt.Field.ID)
		}
	}
	if n.isKnown(workID) && w.IsXPAC {
		pFieldID = 0
	}

	var pSourceID uint64
	var pSourceType string
	if pl := w.PrimaryLocation; pl != nil && pl.Source != nil {
		pSourceID = ParseSourceID(pl.Source.ID)
		pSourceType = pl.Source.Type
	}

	topicIDs, topicScores := extractJSONTopics(w.Topics)

	b.WorkIDs = append(b.WorkIDs, workID)

	b.PubYears = append(b.PubYears, ClampUint16(derefInt32(w.PublicationYear)))
	b.PubDates = append(b.PubDates, pubDatePtr)
	b.UpdatedDates = append(b.UpdatedDates, updatedDate)

	b.WorkTypes = append(b.WorkTypes, w.Type)
	b.IsRetracted = append(b.IsRetracted, boolToUint8(derefBool(w.IsRetracted)))
	b.FWCIs = append(b.FWCIs, fwciPtr)

	b.PrimaryTopicIDs = append(b.PrimaryTopicIDs, pTopicID)
	b.PrimaryFieldIDs = append(b.PrimaryFieldIDs, pFieldID)
	b.PrimarySourceIDs = append(b.PrimarySourceIDs, pSourceID)
	b.PrimarySourceTypes = append(b.PrimarySourceTypes, pSourceType)

	b.TopicIDs = append(b.TopicIDs, topicIDs)
	b.TopicScores = append(b.TopicScores, topicScores)

	return OutcomeAdded
}

func AppendParquetTopic(t *ParquetTopic, b *sink.TopicsBatch) bool {
	if t == nil {
		return false
	}
	topicID := ParseTopicID(derefString(t.ID))
	if topicID == 0 {
		return false
	}
	// The dictionary must carry a real hierarchy: a topic without a valid
	// subfield/field is not usable by the report.
	if t.Subfield == nil {
		return false
	}
	sfID := ParseSubfieldID(derefString(t.Subfield.ID))
	if sfID == 0 || strings.TrimSpace(derefString(t.Subfield.DisplayName)) == "" {
		return false
	}
	if t.Field == nil || ParseFieldID(derefString(t.Field.ID)) == 0 {
		return false
	}
	sfName := derefString(t.Subfield.DisplayName)
	var fID uint16
	var fName string
	if t.Field != nil {
		fID = ParseFieldID(derefString(t.Field.ID))
		fName = derefString(t.Field.DisplayName)
	}
	var dID uint8
	var dName string
	if t.Domain != nil {
		dID = ParseDomainID(derefString(t.Domain.ID))
		dName = derefString(t.Domain.DisplayName)
	}
	updatedDate := unixEpochUTC
	if t.UpdatedDate != nil {
		updatedDate = ParseParquetTimestampMicros(*t.UpdatedDate)
	}

	b.TopicIDs = append(b.TopicIDs, topicID)
	b.DisplayNames = append(b.DisplayNames, derefString(t.DisplayName))
	b.Descriptions = append(b.Descriptions, derefString(t.Description))
	b.SubfieldIDs = append(b.SubfieldIDs, sfID)
	b.SubfieldNames = append(b.SubfieldNames, sfName)
	b.FieldIDs = append(b.FieldIDs, fID)
	b.FieldNames = append(b.FieldNames, fName)
	b.DomainIDs = append(b.DomainIDs, dID)
	b.DomainNames = append(b.DomainNames, dName)
	b.WorksCounts = append(b.WorksCounts, ClampUint32(derefInt32(t.WorksCount)))
	b.CitedByCounts = append(b.CitedByCounts, ClampUint32(derefInt32(t.CitedByCount)))
	b.UpdatedDates = append(b.UpdatedDates, updatedDate)
	return true
}

func AppendParquetInstitution(inst *ParquetInstitution, b *sink.InstitutionsBatch) bool {
	if inst == nil {
		return false
	}

	countryCode := NormalizeCountryCode(derefString(inst.CountryCode))
	var city, region, country string
	var lat, lon float64
	if inst.Geo != nil {
		city = derefString(inst.Geo.City)
		region = derefString(inst.Geo.Region)
		country = derefString(inst.Geo.Country)
		if countryCode == "" {
			countryCode = NormalizeCountryCode(derefString(inst.Geo.CountryCode))
		}
		if inst.Geo.Latitude != nil {
			lat = *inst.Geo.Latitude
		}
		if inst.Geo.Longitude != nil {
			lon = *inst.Geo.Longitude
		}
	}

	var lineage []uint64
	if len(inst.Lineage) > 0 {
		lineage = make([]uint64, 0, len(inst.Lineage))
		for _, raw := range inst.Lineage {
			if lid := ParseInstitutionID(raw); lid != 0 {
				lineage = append(lineage, lid)
			}
		}
		if len(lineage) == 0 {
			lineage = nil
		}
	}

	var hIndex uint32
	if inst.SummaryStats != nil {
		hIndex = ClampUint32(derefInt32(inst.SummaryStats.HIndex))
	}
	updatedDate := unixEpochUTC
	if inst.UpdatedDate != nil {
		updatedDate = ParseParquetTimestampMicros(*inst.UpdatedDate)
	}

	b.InstitutionIDs = append(b.InstitutionIDs, ParseInstitutionID(derefString(inst.ID)))
	b.RORs = append(b.RORs, derefString(inst.ROR))
	b.DisplayNames = append(b.DisplayNames, derefString(inst.DisplayName))
	b.CountryCodes = append(b.CountryCodes, countryCode)
	b.Types = append(b.Types, derefString(inst.Type))
	b.Cities = append(b.Cities, city)
	b.Regions = append(b.Regions, region)
	b.Countries = append(b.Countries, country)
	b.Latitudes = append(b.Latitudes, lat)
	b.Longitudes = append(b.Longitudes, lon)
	b.LineageIDs = append(b.LineageIDs, lineage)
	b.WorksCounts = append(b.WorksCounts, ClampUint32(derefInt32(inst.WorksCount)))
	b.CitedByCounts = append(b.CitedByCounts, ClampUint32(derefInt32(inst.CitedByCount)))
	b.HIndexes = append(b.HIndexes, hIndex)
	b.UpdatedDates = append(b.UpdatedDates, updatedDate)
	return true
}

func extractParquetTopics(topics []ParquetWorkTopic) ([]uint32, []float32) {
	if len(topics) == 0 {
		return nil, nil
	}
	ids := make([]uint32, 0, len(topics))
	scores := make([]float32, 0, len(topics))
	for i := range topics {
		tid := ParseTopicID(derefString(topics[i].ID))
		if tid == 0 || slices.Contains(ids, tid) {
			// Duplicate topic ids keep the first occurrence and its score
			// (round-2 answer Q2); the repeat never overrides it.
			continue
		}
		ids = append(ids, tid)
		scores = append(scores, normalizeTopicScore(derefFloat32(topics[i].Score)))
	}
	if len(ids) == 0 {
		return nil, nil
	}
	return ids, scores
}

func extractJSONTopics(topics []JSONWorkTopic) ([]uint32, []float32) {
	if len(topics) == 0 {
		return nil, nil
	}
	ids := make([]uint32, 0, len(topics))
	scores := make([]float32, 0, len(topics))
	for i := range topics {
		tid := ParseTopicID(topics[i].ID)
		if tid == 0 || slices.Contains(ids, tid) {
			continue
		}
		ids = append(ids, tid)
		scores = append(scores, normalizeTopicScore(topics[i].Score))
	}
	if len(ids) == 0 {
		return nil, nil
	}
	return ids, scores
}

func normalizeTopicScore(v float32) float32 {
	if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) || v < 0 {
		return 0
	}
	return v
}

func derefString(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func derefInt32(p *int32) int32 {
	if p == nil {
		return 0
	}
	return *p
}

func derefFloat32(p *float32) float32 {
	if p == nil {
		return 0
	}
	return *p
}

func derefBool(p *bool) bool {
	if p == nil {
		return false
	}
	return *p
}

func boolToUint8(b bool) uint8 {
	if b {
		return 1
	}
	return 0
}
