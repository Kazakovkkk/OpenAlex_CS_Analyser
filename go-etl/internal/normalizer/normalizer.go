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
)

type Normalizer struct {
	dedup *WorkDeduplicator
}

func NewNormalizer(dedup *WorkDeduplicator) *Normalizer {
	return &Normalizer{dedup: dedup}
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
	if w.PrimaryTopic == nil || w.PrimaryTopic.Field == nil {
		return false
	}
	fid := w.PrimaryTopic.Field.ID
	return fid == csFieldID || strings.HasSuffix(fid, csFieldSuffix)
}

func (n *Normalizer) AppendParquetWork(w *ParquetWork, b *sink.WorksBatch) WorkOutcome {
	if !IsCSParquetWork(w) {
		return OutcomeFilteredOut
	}

	workID := ParseWorkID(derefString(w.ID))
	if n.dedup != nil && !n.dedup.AddIfNew(workID) {
		return OutcomeDuplicate
	}

	var pubDatePtr *time.Time
	if w.PublicationDate != nil {
		if t, ok := ParseParquetDate32(*w.PublicationDate); ok {
			pubDatePtr = b.AllocPubDate(t)
		}
	}
	createdDate := unixEpochUTC
	if w.CreatedDate != nil {
		createdDate = ParseParquetTimestampMicros(*w.CreatedDate)
	}
	updatedDate := unixEpochUTC
	if w.UpdatedDate != nil {
		updatedDate = ParseParquetTimestampMicros(*w.UpdatedDate)
	}

	var fwciPtr *float32
	if w.FWCI != nil && !math.IsNaN(*w.FWCI) && !math.IsInf(*w.FWCI, 0) {
		fwciPtr = b.AllocFWCI(float32(*w.FWCI))
	}

	var abstract string
	if w.AbstractInvertedIndex != nil && len(*w.AbstractInvertedIndex) > 2 {
		abstract = ReconstructAbstractJSON([]byte(*w.AbstractInvertedIndex))
	}

	var pubYear uint16
	if w.PublicationYear != nil {
		pubYear = ClampUint16(*w.PublicationYear)
	}
	var authorsCount uint16
	if w.AuthorsCount != nil {
		authorsCount = ClampUint16(*w.AuthorsCount)
	} else {
		authorsCount = ClampUint16(int32(len(w.Authorships)))
	}

	var pTopicID, pSubfieldID uint32
	var pTopicScore float32
	var pFieldID uint16
	var pDomainID uint8
	if pt := w.PrimaryTopic; pt != nil {
		pTopicID = ParseTopicID(derefString(pt.ID))
		pTopicScore = derefFloat32(pt.Score)
		if pt.Subfield != nil {
			pSubfieldID = ParseSubfieldID(derefString(pt.Subfield.ID))
		}
		if pt.Field != nil {
			pFieldID = ParseFieldID(derefString(pt.Field.ID))
		}
		if pt.Domain != nil {
			pDomainID = ParseDomainID(derefString(pt.Domain.ID))
		}
	}

	var pSourceID uint64
	var pSourceType string
	var pIsOA uint8
	if pl := w.PrimaryLocation; pl != nil {
		pIsOA = boolToUint8(derefBool(pl.IsOA))
		if pl.Source != nil {
			pSourceID = ParseSourceID(derefString(pl.Source.ID))
			pSourceType = derefString(pl.Source.Type)
		}
	}

	firstCountry, countries, authorIDs, instIDs := extractParquetAuthorships(w.Authorships)
	topicIDs, topicScores := extractParquetTopics(w.Topics)
	keywords := extractParquetKeywords(w.Keywords)
	citedYears, citedCounts := extractParquetCountsByYear(w.CountsByYear)

	b.WorkIDs = append(b.WorkIDs, workID)
	b.DOIs = append(b.DOIs, derefString(w.DOI))
	b.Titles = append(b.Titles, derefString(w.Title))
	b.Abstracts = append(b.Abstracts, abstract)

	b.PubYears = append(b.PubYears, pubYear)
	b.PubDates = append(b.PubDates, pubDatePtr)
	b.CreatedDates = append(b.CreatedDates, createdDate)
	b.UpdatedDates = append(b.UpdatedDates, updatedDate)

	b.WorkTypes = append(b.WorkTypes, derefString(w.Type))
	b.Languages = append(b.Languages, derefString(w.Language))
	b.IsRetracted = append(b.IsRetracted, boolToUint8(derefBool(w.IsRetracted)))
	b.CitedByCounts = append(b.CitedByCounts, ClampUint32(derefInt32(w.CitedByCount)))
	b.RefWorksCounts = append(b.RefWorksCounts, ClampUint32(derefInt32(w.ReferencedWorksCount)))
	b.AuthorsCounts = append(b.AuthorsCounts, authorsCount)
	b.FWCIs = append(b.FWCIs, fwciPtr)

	b.PrimaryTopicIDs = append(b.PrimaryTopicIDs, pTopicID)
	b.PrimaryTopicScores = append(b.PrimaryTopicScores, pTopicScore)
	b.PrimarySubfieldIDs = append(b.PrimarySubfieldIDs, pSubfieldID)
	b.PrimaryFieldIDs = append(b.PrimaryFieldIDs, pFieldID)
	b.PrimaryDomainIDs = append(b.PrimaryDomainIDs, pDomainID)
	b.PrimarySourceIDs = append(b.PrimarySourceIDs, pSourceID)
	b.PrimarySourceTypes = append(b.PrimarySourceTypes, pSourceType)
	b.PrimaryIsOA = append(b.PrimaryIsOA, pIsOA)

	b.FirstAuthorCountries = append(b.FirstAuthorCountries, firstCountry)
	b.CountryCodes = append(b.CountryCodes, countries)
	b.AuthorIDs = append(b.AuthorIDs, authorIDs)
	b.InstitutionIDs = append(b.InstitutionIDs, instIDs)
	b.TopicIDs = append(b.TopicIDs, topicIDs)
	b.TopicScores = append(b.TopicScores, topicScores)
	b.Keywords = append(b.Keywords, keywords)
	b.CitedByYears = append(b.CitedByYears, citedYears)
	b.CitedByYearCounts = append(b.CitedByYearCounts, citedCounts)

	return OutcomeAdded
}

func (n *Normalizer) AppendRawJSONWork(raw []byte, b *sink.WorksBatch) (WorkOutcome, error) {
	if !FastJSONPreFilter(raw) {
		return OutcomeFilteredOut, nil
	}
	var jw JSONWork
	if err := sonic.Unmarshal(raw, &jw); err != nil {
		return OutcomeFilteredOut, fmt.Errorf("sonic unmarshal work: %w", err)
	}
	return n.AppendJSONWork(&jw, b), nil
}

func (n *Normalizer) AppendJSONWork(w *JSONWork, b *sink.WorksBatch) WorkOutcome {
	if !IsCSJSONWork(w) {
		return OutcomeFilteredOut
	}

	workID := ParseWorkID(w.ID)
	if n.dedup != nil && !n.dedup.AddIfNew(workID) {
		return OutcomeDuplicate
	}

	var pubDatePtr *time.Time
	if t, ok := ParseDate32String(w.PublicationDate); ok {
		pubDatePtr = b.AllocPubDate(t)
	}
	createdDate := ParseTimestampString(w.CreatedDate)
	updatedDate := ParseTimestampString(w.UpdatedDate)

	var fwciPtr *float32
	if w.FWCI != nil && !math.IsNaN(*w.FWCI) && !math.IsInf(*w.FWCI, 0) {
		fwciPtr = b.AllocFWCI(float32(*w.FWCI))
	}

	abstract := ReconstructAbstract(w.AbstractInvertedIdx)

	authorsCount := ClampUint16(w.AuthorsCount)
	if authorsCount == 0 && len(w.Authorships) > 0 {
		authorsCount = ClampUint16(int32(len(w.Authorships)))
	}

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

	firstCountry, countries, authorIDs, instIDs := extractJSONAuthorships(w.Authorships)
	topicIDs, topicScores := extractJSONTopics(w.Topics)
	keywords := extractJSONKeywords(w.Keywords)
	citedYears, citedCounts := extractJSONCountsByYear(w.CountsByYear)

	b.WorkIDs = append(b.WorkIDs, workID)
	b.DOIs = append(b.DOIs, w.DOI)
	b.Titles = append(b.Titles, w.Title)
	b.Abstracts = append(b.Abstracts, abstract)

	b.PubYears = append(b.PubYears, ClampUint16(w.PublicationYear))
	b.PubDates = append(b.PubDates, pubDatePtr)
	b.CreatedDates = append(b.CreatedDates, createdDate)
	b.UpdatedDates = append(b.UpdatedDates, updatedDate)

	b.WorkTypes = append(b.WorkTypes, w.Type)
	b.Languages = append(b.Languages, w.Language)
	b.IsRetracted = append(b.IsRetracted, boolToUint8(w.IsRetracted))
	b.CitedByCounts = append(b.CitedByCounts, ClampUint32(w.CitedByCount))
	b.RefWorksCounts = append(b.RefWorksCounts, ClampUint32(w.ReferencedWorksCount))
	b.AuthorsCounts = append(b.AuthorsCounts, authorsCount)
	b.FWCIs = append(b.FWCIs, fwciPtr)

	b.PrimaryTopicIDs = append(b.PrimaryTopicIDs, pTopicID)
	b.PrimaryTopicScores = append(b.PrimaryTopicScores, pTopicScore)
	b.PrimarySubfieldIDs = append(b.PrimarySubfieldIDs, pSubfieldID)
	b.PrimaryFieldIDs = append(b.PrimaryFieldIDs, pFieldID)
	b.PrimaryDomainIDs = append(b.PrimaryDomainIDs, pDomainID)
	b.PrimarySourceIDs = append(b.PrimarySourceIDs, pSourceID)
	b.PrimarySourceTypes = append(b.PrimarySourceTypes, pSourceType)
	b.PrimaryIsOA = append(b.PrimaryIsOA, pIsOA)

	b.FirstAuthorCountries = append(b.FirstAuthorCountries, firstCountry)
	b.CountryCodes = append(b.CountryCodes, countries)
	b.AuthorIDs = append(b.AuthorIDs, authorIDs)
	b.InstitutionIDs = append(b.InstitutionIDs, instIDs)
	b.TopicIDs = append(b.TopicIDs, topicIDs)
	b.TopicScores = append(b.TopicScores, topicScores)
	b.Keywords = append(b.Keywords, keywords)
	b.CitedByYears = append(b.CitedByYears, citedYears)
	b.CitedByYearCounts = append(b.CitedByYearCounts, citedCounts)

	return OutcomeAdded
}

func AppendParquetTopic(t *ParquetTopic, b *sink.TopicsBatch) bool {
	if t == nil {
		return false
	}
	var sfID uint32
	var sfName string
	if t.Subfield != nil {
		sfID = ParseSubfieldID(derefString(t.Subfield.ID))
		sfName = derefString(t.Subfield.DisplayName)
	}
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

	b.TopicIDs = append(b.TopicIDs, ParseTopicID(derefString(t.ID)))
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

func extractParquetAuthorships(auths []ParquetWorkAuthorship) (firstCountry string, countries []string, authorIDs []uint64, instIDs []uint64) {
	if len(auths) == 0 {
		return "", nil, nil, nil
	}
	authorIDs = make([]uint64, 0, len(auths))

	for i := range auths {
		a := &auths[i]
		isFirst := derefString(a.AuthorPosition) == "first" || (i == 0 && firstCountry == "")

		if a.Author != nil {
			if aid := ParseAuthorID(derefString(a.Author.ID)); aid != 0 {
				authorIDs = append(authorIDs, aid)
			}
		}
		for _, rawCC := range a.Countries {
			cc := NormalizeCountryCode(rawCC)
			if cc == "" {
				continue
			}
			if isFirst && firstCountry == "" {
				firstCountry = cc
			}
			if !slices.Contains(countries, cc) {
				countries = append(countries, cc)
			}
		}
		for j := range a.Institutions {
			inst := &a.Institutions[j]
			if iid := ParseInstitutionID(derefString(inst.ID)); iid != 0 && !slices.Contains(instIDs, iid) {
				instIDs = append(instIDs, iid)
			}
			if cc := NormalizeCountryCode(derefString(inst.CountryCode)); cc != "" {
				if isFirst && firstCountry == "" {
					firstCountry = cc
				}
				if !slices.Contains(countries, cc) {
					countries = append(countries, cc)
				}
			}
		}
	}
	if len(authorIDs) == 0 {
		authorIDs = nil
	}
	return firstCountry, countries, authorIDs, instIDs
}

func extractJSONAuthorships(auths []JSONAuthorship) (firstCountry string, countries []string, authorIDs []uint64, instIDs []uint64) {
	if len(auths) == 0 {
		return "", nil, nil, nil
	}
	authorIDs = make([]uint64, 0, len(auths))

	for i := range auths {
		a := &auths[i]
		isFirst := a.AuthorPosition == "first" || (i == 0 && firstCountry == "")

		if a.Author != nil {
			if aid := ParseAuthorID(a.Author.ID); aid != 0 {
				authorIDs = append(authorIDs, aid)
			}
		}
		for _, rawCC := range a.Countries {
			cc := NormalizeCountryCode(rawCC)
			if cc == "" {
				continue
			}
			if isFirst && firstCountry == "" {
				firstCountry = cc
			}
			if !slices.Contains(countries, cc) {
				countries = append(countries, cc)
			}
		}
		for j := range a.Institutions {
			inst := &a.Institutions[j]
			if iid := ParseInstitutionID(inst.ID); iid != 0 && !slices.Contains(instIDs, iid) {
				instIDs = append(instIDs, iid)
			}
			if cc := NormalizeCountryCode(inst.CountryCode); cc != "" {
				if isFirst && firstCountry == "" {
					firstCountry = cc
				}
				if !slices.Contains(countries, cc) {
					countries = append(countries, cc)
				}
			}
		}
	}
	if len(authorIDs) == 0 {
		authorIDs = nil
	}
	return firstCountry, countries, authorIDs, instIDs
}

func extractParquetTopics(topics []ParquetWorkTopic) ([]uint32, []float32) {
	if len(topics) == 0 {
		return nil, nil
	}
	ids := make([]uint32, 0, len(topics))
	scores := make([]float32, 0, len(topics))
	for i := range topics {
		if tid := ParseTopicID(derefString(topics[i].ID)); tid != 0 {
			ids = append(ids, tid)
			scores = append(scores, derefFloat32(topics[i].Score))
		}
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
		if tid := ParseTopicID(topics[i].ID); tid != 0 {
			ids = append(ids, tid)
			scores = append(scores, topics[i].Score)
		}
	}
	if len(ids) == 0 {
		return nil, nil
	}
	return ids, scores
}

func extractParquetKeywords(kws []ParquetWorkKeyword) []string {
	if len(kws) == 0 {
		return nil
	}
	out := make([]string, 0, len(kws))
	for i := range kws {
		kw := NormalizeKeyword(derefString(kws[i].DisplayName))
		if kw != "" && !slices.Contains(out, kw) {
			out = append(out, kw)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func extractJSONKeywords(kws []JSONWorkKeyword) []string {
	if len(kws) == 0 {
		return nil
	}
	out := make([]string, 0, len(kws))
	for i := range kws {
		kw := NormalizeKeyword(kws[i].DisplayName)
		if kw != "" && !slices.Contains(out, kw) {
			out = append(out, kw)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func extractParquetCountsByYear(cby []ParquetYearCount) ([]uint16, []uint32) {
	if len(cby) == 0 {
		return nil, nil
	}
	years := make([]uint16, 0, len(cby))
	counts := make([]uint32, 0, len(cby))
	for i := range cby {
		if cby[i].Year == nil {
			continue
		}
		years = append(years, ClampUint16(*cby[i].Year))
		counts = append(counts, ClampUint32(derefInt32(cby[i].CitedByCount)))
	}
	if len(years) == 0 {
		return nil, nil
	}
	return years, counts
}

func extractJSONCountsByYear(cby []JSONYearCount) ([]uint16, []uint32) {
	if len(cby) == 0 {
		return nil, nil
	}
	years := make([]uint16, 0, len(cby))
	counts := make([]uint32, 0, len(cby))
	for i := range cby {
		years = append(years, ClampUint16(cby[i].Year))
		counts = append(counts, ClampUint32(cby[i].CitedByCount))
	}
	return years, counts
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
