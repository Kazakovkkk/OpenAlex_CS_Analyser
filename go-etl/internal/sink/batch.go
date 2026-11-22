package sink

import (
	"sync"
	"time"
)

const (
	DefaultBatchCap     = 10_000  // matches default batch_size in spec
	DefaultMaxPooledCap = 100_000 // matches max batch_size in Benchmark #5

	// Logical uncompressed fixed-width bytes per row (matches ClickHouse column widths):
	// WorkID(8) + PubYear(2) + PubDate(4) + Created(8) + Updated(8) + IsRetracted(1) +
	// CitedBy(4) + RefWorks(4) + Authors(2) + FWCI(4) + PrimaryTopicID(4) + PrimaryTopicScore(4) +
	// PrimarySubfieldID(4) + PrimaryFieldID(2) + PrimaryDomainID(1) + PrimarySourceID(8) + PrimaryIsOA(1) = 69 B.
	worksFixedBytesPerRow = 8 + 2 + 4 + 8 + 8 + 1 + 4 + 4 + 2 + 4 + 4 + 4 + 4 + 2 + 1 + 8 + 1

	// TopicID(4) + SubfieldID(4) + FieldID(2) + DomainID(1) + WorksCount(4) + CitedByCount(4) + UpdatedDate(8) = 27 B.
	topicsFixedBytesPerRow = 4 + 4 + 2 + 1 + 4 + 4 + 8

	// InstitutionID(8) + Lat(8) + Lon(8) + WorksCount(4) + CitedByCount(4) + HIndex(4) + UpdatedDate(8) = 44 B.
	institutionsFixedBytesPerRow = 8 + 8 + 8 + 4 + 4 + 4 + 8
)

type WorksBatch struct {
	WorkIDs   []uint64
	DOIs      []string
	Titles    []string
	Abstracts []string

	PubYears     []uint16
	PubDates     []*time.Time
	CreatedDates []time.Time
	UpdatedDates []time.Time

	WorkTypes      []string
	Languages      []string
	IsRetracted    []uint8
	CitedByCounts  []uint32
	RefWorksCounts []uint32
	AuthorsCounts  []uint16
	FWCIs          []*float32

	PrimaryTopicIDs    []uint32
	PrimaryTopicScores []float32
	PrimarySubfieldIDs []uint32
	PrimaryFieldIDs    []uint16
	PrimaryDomainIDs   []uint8
	PrimarySourceIDs   []uint64
	PrimarySourceTypes []string
	PrimaryIsOA        []uint8

	FirstAuthorCountries []string
	CountryCodes         [][]string
	AuthorIDs            [][]uint64
	InstitutionIDs       [][]uint64
	TopicIDs             [][]uint32
	TopicScores          [][]float32
	Keywords             [][]string
	CitedByYears         [][]uint16
	CitedByYearCounts    [][]uint32

	pubDateSlab []time.Time
	fwciSlab    []float32
}

func NewWorksBatch(capacity int) *WorksBatch {
	if capacity <= 0 {
		capacity = DefaultBatchCap
	}
	return &WorksBatch{
		WorkIDs:              make([]uint64, 0, capacity),
		DOIs:                 make([]string, 0, capacity),
		Titles:               make([]string, 0, capacity),
		Abstracts:            make([]string, 0, capacity),
		PubYears:             make([]uint16, 0, capacity),
		PubDates:             make([]*time.Time, 0, capacity),
		CreatedDates:         make([]time.Time, 0, capacity),
		UpdatedDates:         make([]time.Time, 0, capacity),
		WorkTypes:            make([]string, 0, capacity),
		Languages:            make([]string, 0, capacity),
		IsRetracted:          make([]uint8, 0, capacity),
		CitedByCounts:        make([]uint32, 0, capacity),
		RefWorksCounts:       make([]uint32, 0, capacity),
		AuthorsCounts:        make([]uint16, 0, capacity),
		FWCIs:                make([]*float32, 0, capacity),
		PrimaryTopicIDs:      make([]uint32, 0, capacity),
		PrimaryTopicScores:   make([]float32, 0, capacity),
		PrimarySubfieldIDs:   make([]uint32, 0, capacity),
		PrimaryFieldIDs:      make([]uint16, 0, capacity),
		PrimaryDomainIDs:     make([]uint8, 0, capacity),
		PrimarySourceIDs:     make([]uint64, 0, capacity),
		PrimarySourceTypes:   make([]string, 0, capacity),
		PrimaryIsOA:          make([]uint8, 0, capacity),
		FirstAuthorCountries: make([]string, 0, capacity),
		CountryCodes:         make([][]string, 0, capacity),
		AuthorIDs:            make([][]uint64, 0, capacity),
		InstitutionIDs:       make([][]uint64, 0, capacity),
		TopicIDs:             make([][]uint32, 0, capacity),
		TopicScores:          make([][]float32, 0, capacity),
		Keywords:             make([][]string, 0, capacity),
		CitedByYears:         make([][]uint16, 0, capacity),
		CitedByYearCounts:    make([][]uint32, 0, capacity),
		pubDateSlab:          make([]time.Time, 0, capacity),
		fwciSlab:             make([]float32, 0, capacity),
	}
}

func (b *WorksBatch) AllocPubDate(t time.Time) *time.Time {
	idx := len(b.pubDateSlab)
	b.pubDateSlab = append(b.pubDateSlab, t)
	return &b.pubDateSlab[idx]
}

func (b *WorksBatch) AllocFWCI(v float32) *float32 {
	idx := len(b.fwciSlab)
	b.fwciSlab = append(b.fwciSlab, v)
	return &b.fwciSlab[idx]
}

func (b *WorksBatch) Len() int {
	return len(b.WorkIDs)
}

func (b *WorksBatch) Reset() {
	b.WorkIDs = b.WorkIDs[:0]
	b.DOIs = resetWithClear(b.DOIs)
	b.Titles = resetWithClear(b.Titles)
	b.Abstracts = resetWithClear(b.Abstracts)

	b.PubYears = b.PubYears[:0]
	b.PubDates = resetWithClear(b.PubDates)
	b.CreatedDates = b.CreatedDates[:0]
	b.UpdatedDates = b.UpdatedDates[:0]

	b.WorkTypes = resetWithClear(b.WorkTypes)
	b.Languages = resetWithClear(b.Languages)
	b.IsRetracted = b.IsRetracted[:0]
	b.CitedByCounts = b.CitedByCounts[:0]
	b.RefWorksCounts = b.RefWorksCounts[:0]
	b.AuthorsCounts = b.AuthorsCounts[:0]
	b.FWCIs = resetWithClear(b.FWCIs)

	b.PrimaryTopicIDs = b.PrimaryTopicIDs[:0]
	b.PrimaryTopicScores = b.PrimaryTopicScores[:0]
	b.PrimarySubfieldIDs = b.PrimarySubfieldIDs[:0]
	b.PrimaryFieldIDs = b.PrimaryFieldIDs[:0]
	b.PrimaryDomainIDs = b.PrimaryDomainIDs[:0]
	b.PrimarySourceIDs = b.PrimarySourceIDs[:0]
	b.PrimarySourceTypes = resetWithClear(b.PrimarySourceTypes)
	b.PrimaryIsOA = b.PrimaryIsOA[:0]

	b.FirstAuthorCountries = resetWithClear(b.FirstAuthorCountries)
	b.CountryCodes = resetWithClear(b.CountryCodes)
	b.AuthorIDs = resetWithClear(b.AuthorIDs)
	b.InstitutionIDs = resetWithClear(b.InstitutionIDs)
	b.TopicIDs = resetWithClear(b.TopicIDs)
	b.TopicScores = resetWithClear(b.TopicScores)
	b.Keywords = resetWithClear(b.Keywords)
	b.CitedByYears = resetWithClear(b.CitedByYears)
	b.CitedByYearCounts = resetWithClear(b.CitedByYearCounts)

	b.pubDateSlab = b.pubDateSlab[:0]
	b.fwciSlab = b.fwciSlab[:0]
}

func (b *WorksBatch) ApproxBytes() int64 {
	n := len(b.WorkIDs)
	if n == 0 {
		return 0
	}
	total := int64(n) * worksFixedBytesPerRow
	total += sumStringBytes(b.DOIs) + sumStringBytes(b.Titles) + sumStringBytes(b.Abstracts)
	total += sumStringBytes(b.WorkTypes) + sumStringBytes(b.Languages)
	total += sumStringBytes(b.PrimarySourceTypes) + sumStringBytes(b.FirstAuthorCountries)
	total += sumNestedStringBytes(b.CountryCodes) + sumNestedStringBytes(b.Keywords)
	for _, s := range b.AuthorIDs {
		total += int64(len(s)) * 8
	}
	for _, s := range b.InstitutionIDs {
		total += int64(len(s)) * 8
	}
	for _, s := range b.TopicIDs {
		total += int64(len(s)) * 4
	}
	for _, s := range b.TopicScores {
		total += int64(len(s)) * 4
	}
	for _, s := range b.CitedByYears {
		total += int64(len(s)) * 2
	}
	for _, s := range b.CitedByYearCounts {
		total += int64(len(s)) * 4
	}
	return total
}

type WorksBatchPool struct {
	pool         sync.Pool
	maxPooledCap int
}

func NewWorksBatchPool(initialCap, maxPooledCap int) *WorksBatchPool {
	if initialCap <= 0 {
		initialCap = DefaultBatchCap
	}
	if maxPooledCap <= 0 {
		maxPooledCap = DefaultMaxPooledCap
	}
	if maxPooledCap < initialCap {
		maxPooledCap = initialCap
	}
	p := &WorksBatchPool{maxPooledCap: maxPooledCap}
	p.pool.New = func() any {
		return NewWorksBatch(initialCap)
	}
	return p
}

func (p *WorksBatchPool) Get() *WorksBatch {
	return p.pool.Get().(*WorksBatch)
}

func (p *WorksBatchPool) Put(b *WorksBatch) {
	if b == nil {
		return
	}
	b.Reset()
	if cap(b.WorkIDs) > p.maxPooledCap {
		return
	}
	p.pool.Put(b)
}

func resetWithClear[T any](s []T) []T {
	clear(s)
	return s[:0]
}

func sumStringBytes(ss []string) int64 {
	var total int64
	for _, s := range ss {
		total += int64(len(s))
	}
	return total
}

func sumNestedStringBytes(groups [][]string) int64 {
	var total int64
	for _, g := range groups {
		for _, s := range g {
			total += int64(len(s))
		}
	}
	return total
}

type TopicsBatch struct {
	TopicIDs      []uint32
	DisplayNames  []string
	Descriptions  []string
	SubfieldIDs   []uint32
	SubfieldNames []string
	FieldIDs      []uint16
	FieldNames    []string
	DomainIDs     []uint8
	DomainNames   []string
	WorksCounts   []uint32
	CitedByCounts []uint32
	UpdatedDates  []time.Time
}

func NewTopicsBatch(capacity int) *TopicsBatch {
	if capacity <= 0 {
		capacity = 5_000
	}
	return &TopicsBatch{
		TopicIDs:      make([]uint32, 0, capacity),
		DisplayNames:  make([]string, 0, capacity),
		Descriptions:  make([]string, 0, capacity),
		SubfieldIDs:   make([]uint32, 0, capacity),
		SubfieldNames: make([]string, 0, capacity),
		FieldIDs:      make([]uint16, 0, capacity),
		FieldNames:    make([]string, 0, capacity),
		DomainIDs:     make([]uint8, 0, capacity),
		DomainNames:   make([]string, 0, capacity),
		WorksCounts:   make([]uint32, 0, capacity),
		CitedByCounts: make([]uint32, 0, capacity),
		UpdatedDates:  make([]time.Time, 0, capacity),
	}
}

func (b *TopicsBatch) Len() int { return len(b.TopicIDs) }

func (b *TopicsBatch) Reset() {
	b.TopicIDs = b.TopicIDs[:0]
	b.DisplayNames = resetWithClear(b.DisplayNames)
	b.Descriptions = resetWithClear(b.Descriptions)
	b.SubfieldIDs = b.SubfieldIDs[:0]
	b.SubfieldNames = resetWithClear(b.SubfieldNames)
	b.FieldIDs = b.FieldIDs[:0]
	b.FieldNames = resetWithClear(b.FieldNames)
	b.DomainIDs = b.DomainIDs[:0]
	b.DomainNames = resetWithClear(b.DomainNames)
	b.WorksCounts = b.WorksCounts[:0]
	b.CitedByCounts = b.CitedByCounts[:0]
	b.UpdatedDates = b.UpdatedDates[:0]
}

func (b *TopicsBatch) ApproxBytes() int64 {
	n := len(b.TopicIDs)
	if n == 0 {
		return 0
	}
	return int64(n)*topicsFixedBytesPerRow +
		sumStringBytes(b.DisplayNames) + sumStringBytes(b.Descriptions) +
		sumStringBytes(b.SubfieldNames) + sumStringBytes(b.FieldNames) + sumStringBytes(b.DomainNames)
}

type InstitutionsBatch struct {
	InstitutionIDs []uint64
	RORs           []string
	DisplayNames   []string
	CountryCodes   []string
	Types          []string
	Cities         []string
	Regions        []string
	Countries      []string
	Latitudes      []float64
	Longitudes     []float64
	LineageIDs     [][]uint64
	WorksCounts    []uint32
	CitedByCounts  []uint32
	HIndexes       []uint32
	UpdatedDates   []time.Time
}

func NewInstitutionsBatch(capacity int) *InstitutionsBatch {
	if capacity <= 0 {
		capacity = DefaultBatchCap
	}
	return &InstitutionsBatch{
		InstitutionIDs: make([]uint64, 0, capacity),
		RORs:           make([]string, 0, capacity),
		DisplayNames:   make([]string, 0, capacity),
		CountryCodes:   make([]string, 0, capacity),
		Types:          make([]string, 0, capacity),
		Cities:         make([]string, 0, capacity),
		Regions:        make([]string, 0, capacity),
		Countries:      make([]string, 0, capacity),
		Latitudes:      make([]float64, 0, capacity),
		Longitudes:     make([]float64, 0, capacity),
		LineageIDs:     make([][]uint64, 0, capacity),
		WorksCounts:    make([]uint32, 0, capacity),
		CitedByCounts:  make([]uint32, 0, capacity),
		HIndexes:       make([]uint32, 0, capacity),
		UpdatedDates:   make([]time.Time, 0, capacity),
	}
}

func (b *InstitutionsBatch) Len() int { return len(b.InstitutionIDs) }

func (b *InstitutionsBatch) Reset() {
	b.InstitutionIDs = b.InstitutionIDs[:0]
	b.RORs = resetWithClear(b.RORs)
	b.DisplayNames = resetWithClear(b.DisplayNames)
	b.CountryCodes = resetWithClear(b.CountryCodes)
	b.Types = resetWithClear(b.Types)
	b.Cities = resetWithClear(b.Cities)
	b.Regions = resetWithClear(b.Regions)
	b.Countries = resetWithClear(b.Countries)
	b.Latitudes = b.Latitudes[:0]
	b.Longitudes = b.Longitudes[:0]
	b.LineageIDs = resetWithClear(b.LineageIDs)
	b.WorksCounts = b.WorksCounts[:0]
	b.CitedByCounts = b.CitedByCounts[:0]
	b.HIndexes = b.HIndexes[:0]
	b.UpdatedDates = b.UpdatedDates[:0]
}

func (b *InstitutionsBatch) ApproxBytes() int64 {
	n := len(b.InstitutionIDs)
	if n == 0 {
		return 0
	}
	total := int64(n)*institutionsFixedBytesPerRow +
		sumStringBytes(b.RORs) + sumStringBytes(b.DisplayNames) + sumStringBytes(b.CountryCodes) +
		sumStringBytes(b.Types) + sumStringBytes(b.Cities) + sumStringBytes(b.Regions) + sumStringBytes(b.Countries)
	for _, s := range b.LineageIDs {
		total += int64(len(s)) * 8
	}
	return total
}
