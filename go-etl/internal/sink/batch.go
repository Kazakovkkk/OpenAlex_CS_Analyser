package sink

import (
	"sync"
	"time"
)

const (
	DefaultBatchCap     = 10_000  // matches default batch_size in spec
	DefaultMaxPooledCap = 100_000 // matches max batch_size in Benchmark #5

	// Logical uncompressed fixed-width bytes per row, matching the ClickHouse
	// column widths of openalex.works (MVP schema): work_id(8) +
	// source_updated_at(8) + primary_field_id(2) + is_retracted(1) +
	// publication_year(2) + publication_date(4) + primary_topic_id(4) +
	// primary_source_id(8) + fwci(8) + has_fwci(1) = 46 B.
	worksFixedBytesPerRow = 8 + 8 + 2 + 1 + 2 + 4 + 4 + 8 + 8 + 1

	// Topics: topic_id(4) + subfield_id(2) + works_count(4) + cited_by_count(4) + updated(8).
	topicsFixedBytesPerRow = 4 + 2 + 4 + 4 + 8

	// Institutions (diagnostics only for now): id(8) + lat(8) + lon(8) +
	// works(4) + cited(4) + h(4) + updated(8).
	institutionsFixedBytesPerRow = 8 + 8 + 8 + 4 + 4 + 4 + 8
)

type WorksBatch struct {
	WorkIDs []uint64

	UpdatedDates []time.Time
	WorkTypes    []string

	PrimaryFieldIDs []uint16
	IsRetracted     []uint8
	PubYears        []uint16

	PubDates []*time.Time

	PrimaryTopicIDs    []uint32
	TopicIDs           [][]uint32
	TopicScores        [][]float32
	PrimarySourceIDs   []uint64
	PrimarySourceTypes []string

	FWCIs []*float32

	pubDateSlab []time.Time
	fwciSlab    []float32
}

func NewWorksBatch(capacity int) *WorksBatch {
	if capacity <= 0 {
		capacity = DefaultBatchCap
	}
	return &WorksBatch{
		WorkIDs:            make([]uint64, 0, capacity),
		UpdatedDates:       make([]time.Time, 0, capacity),
		WorkTypes:          make([]string, 0, capacity),
		PrimaryFieldIDs:    make([]uint16, 0, capacity),
		IsRetracted:        make([]uint8, 0, capacity),
		PubYears:           make([]uint16, 0, capacity),
		PubDates:           make([]*time.Time, 0, capacity),
		PrimaryTopicIDs:    make([]uint32, 0, capacity),
		TopicIDs:           make([][]uint32, 0, capacity),
		TopicScores:        make([][]float32, 0, capacity),
		PrimarySourceIDs:   make([]uint64, 0, capacity),
		PrimarySourceTypes: make([]string, 0, capacity),
		FWCIs:              make([]*float32, 0, capacity),
		pubDateSlab:        make([]time.Time, 0, capacity),
		fwciSlab:           make([]float32, 0, capacity),
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

// Trim keeps the first n rows, so a row-level cap is enforced exactly.
func (b *WorksBatch) Trim(n int) {
	if n < 0 || n >= b.Len() {
		return
	}
	b.WorkIDs = b.WorkIDs[:n]
	b.UpdatedDates = b.UpdatedDates[:n]
	for i := range b.WorkTypes[:n] {
		b.WorkTypes[i] = ""
	}
	b.WorkTypes = b.WorkTypes[:n]
	b.PrimaryFieldIDs = b.PrimaryFieldIDs[:n]
	b.IsRetracted = b.IsRetracted[:n]
	b.PubYears = b.PubYears[:n]
	b.PubDates = b.PubDates[:n]
	b.PrimaryTopicIDs = b.PrimaryTopicIDs[:n]
	for i := range b.TopicIDs[:n] {
		b.TopicIDs[i] = nil
	}
	b.TopicIDs = b.TopicIDs[:n]
	for i := range b.TopicScores[:n] {
		b.TopicScores[i] = nil
	}
	b.TopicScores = b.TopicScores[:n]
	b.PrimarySourceIDs = b.PrimarySourceIDs[:n]
	for i := range b.PrimarySourceTypes[:n] {
		b.PrimarySourceTypes[i] = ""
	}
	b.PrimarySourceTypes = b.PrimarySourceTypes[:n]
	b.FWCIs = b.FWCIs[:n]
}

func (b *WorksBatch) Reset() {
	b.WorkIDs = b.WorkIDs[:0]
	b.UpdatedDates = b.UpdatedDates[:0]
	b.WorkTypes = resetWithClear(b.WorkTypes)

	b.PrimaryFieldIDs = b.PrimaryFieldIDs[:0]
	b.IsRetracted = b.IsRetracted[:0]
	b.PubYears = b.PubYears[:0]
	b.PubDates = resetWithClear(b.PubDates)

	b.PrimaryTopicIDs = b.PrimaryTopicIDs[:0]
	b.TopicIDs = resetWithClear(b.TopicIDs)
	b.TopicScores = resetWithClear(b.TopicScores)
	b.PrimarySourceIDs = b.PrimarySourceIDs[:0]
	b.PrimarySourceTypes = resetWithClear(b.PrimarySourceTypes)

	b.FWCIs = resetWithClear(b.FWCIs)

	b.pubDateSlab = b.pubDateSlab[:0]
	b.fwciSlab = b.fwciSlab[:0]
}

func (b *WorksBatch) ApproxBytes() int64 {
	n := len(b.WorkIDs)
	if n == 0 {
		return 0
	}
	total := int64(n) * worksFixedBytesPerRow
	total += sumStringBytes(b.WorkTypes)
	total += sumStringBytes(b.PrimarySourceTypes)
	total += sumNestedUint32Bytes(b.TopicIDs)
	total += sumNestedFloat32Bytes(b.TopicScores)
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

func sumNestedUint32Bytes(groups [][]uint32) int64 {
	var total int64
	for _, g := range groups {
		total += int64(len(g)) * 4
	}
	return total
}

func sumNestedFloat32Bytes(groups [][]float32) int64 {
	var total int64
	for _, g := range groups {
		total += int64(len(g)) * 4
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
