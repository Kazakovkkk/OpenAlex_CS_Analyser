package normalizer

type ParquetWork struct {
	ID                   *string  `parquet:"id,optional"`
	DOI                  *string  `parquet:"doi,optional"`
	Title                *string  `parquet:"title,optional"`
	PublicationYear      *int32   `parquet:"publication_year,optional"`
	PublicationDate      *int32   `parquet:"publication_date,date,optional"`
	CreatedDate          *int64   `parquet:"created_date,timestamp(microsecond),optional"`
	UpdatedDate          *int64   `parquet:"updated_date,timestamp(microsecond),optional"`
	Type                 *string  `parquet:"type,optional"`
	Language             *string  `parquet:"language,optional"`
	IsRetracted          *bool    `parquet:"is_retracted,optional"`
	IsXPAC               *bool    `parquet:"is_xpac,optional"`
	CitedByCount         *int32   `parquet:"cited_by_count,optional"`
	ReferencedWorksCount *int32   `parquet:"referenced_works_count,optional"`
	AuthorsCount         *int32   `parquet:"authors_count,optional"`
	FWCI                 *float64 `parquet:"fwci,optional"`

	PrimaryTopic    *ParquetPrimaryTopic    `parquet:"primary_topic,optional"`
	Topics          []ParquetWorkTopic      `parquet:"topics,list,optional" parquet-element:",optional"`
	Keywords        []ParquetWorkKeyword    `parquet:"keywords,list,optional" parquet-element:",optional"`
	Authorships     []ParquetWorkAuthorship `parquet:"authorships,list,optional" parquet-element:",optional"`
	PrimaryLocation *ParquetPrimaryLocation `parquet:"primary_location,optional"`
	CountsByYear    []ParquetYearCount      `parquet:"counts_by_year,list,optional" parquet-element:",optional"`

	AbstractInvertedIndex *string `parquet:"abstract_inverted_index,optional"`
}

type ParquetIDRef struct {
	ID *string `parquet:"id,optional"`
}

type ParquetPrimaryTopic struct {
	ID       *string       `parquet:"id,optional"`
	Score    *float32      `parquet:"score,optional"`
	Subfield *ParquetIDRef `parquet:"subfield,optional"`
	Field    *ParquetIDRef `parquet:"field,optional"`
	Domain   *ParquetIDRef `parquet:"domain,optional"`
}

type ParquetWorkTopic struct {
	ID    *string  `parquet:"id,optional"`
	Score *float32 `parquet:"score,optional"`
}

type ParquetWorkKeyword struct {
	DisplayName *string `parquet:"display_name,optional"`
}

type ParquetWorkInstitution struct {
	ID          *string `parquet:"id,optional"`
	CountryCode *string `parquet:"country_code,optional"`
}

type ParquetWorkAuthorship struct {
	Author         *ParquetIDRef            `parquet:"author,optional"`
	AuthorPosition *string                  `parquet:"author_position,optional"`
	Countries      []string                 `parquet:"countries,list,optional" parquet-element:",optional"`
	Institutions   []ParquetWorkInstitution `parquet:"institutions,list,optional" parquet-element:",optional"`
}

type ParquetPrimarySource struct {
	ID   *string `parquet:"id,optional"`
	Type *string `parquet:"type,optional"`
}

type ParquetPrimaryLocation struct {
	Source *ParquetPrimarySource `parquet:"source,optional"`
	IsOA   *bool                 `parquet:"is_oa,optional"`
}

type ParquetYearCount struct {
	Year         *int32 `parquet:"year,optional"`
	CitedByCount *int32 `parquet:"cited_by_count,optional"`
}

type ParquetNamedRef struct {
	ID          *string `parquet:"id,optional"`
	DisplayName *string `parquet:"display_name,optional"`
}

type ParquetTopic struct {
	ID           *string          `parquet:"id,optional"`
	DisplayName  *string          `parquet:"display_name,optional"`
	Description  *string          `parquet:"description,optional"`
	Subfield     *ParquetNamedRef `parquet:"subfield,optional"`
	Field        *ParquetNamedRef `parquet:"field,optional"`
	Domain       *ParquetNamedRef `parquet:"domain,optional"`
	WorksCount   *int32           `parquet:"works_count,optional"`
	CitedByCount *int32           `parquet:"cited_by_count,optional"`
	UpdatedDate  *int64           `parquet:"updated_date,timestamp(microsecond),optional"`
}

type ParquetInstitutionGeo struct {
	City        *string  `parquet:"city,optional"`
	Region      *string  `parquet:"region,optional"`
	Country     *string  `parquet:"country,optional"`
	CountryCode *string  `parquet:"country_code,optional"`
	Latitude    *float64 `parquet:"latitude,optional"`
	Longitude   *float64 `parquet:"longitude,optional"`
}

type ParquetInstitutionStats struct {
	HIndex *int32 `parquet:"h_index,optional"`
}

type ParquetInstitution struct {
	ID           *string                  `parquet:"id,optional"`
	ROR          *string                  `parquet:"ror,optional"`
	DisplayName  *string                  `parquet:"display_name,optional"`
	CountryCode  *string                  `parquet:"country_code,optional"`
	Type         *string                  `parquet:"type,optional"`
	Lineage      []string                 `parquet:"lineage,list,optional" parquet-element:",optional"`
	Geo          *ParquetInstitutionGeo   `parquet:"geo,optional"`
	WorksCount   *int32                   `parquet:"works_count,optional"`
	CitedByCount *int32                   `parquet:"cited_by_count,optional"`
	SummaryStats *ParquetInstitutionStats `parquet:"summary_stats,optional"`
	UpdatedDate  *int64                   `parquet:"updated_date,timestamp(microsecond),optional"`
}

type JSONWork struct {
	ID                   string            `json:"id"`
	DOI                  string            `json:"doi"`
	Title                string            `json:"title"`
	PublicationYear      int32             `json:"publication_year"`
	PublicationDate      string            `json:"publication_date"`
	CreatedDate          string            `json:"created_date"`
	UpdatedDate          string            `json:"updated_date"`
	Type                 string            `json:"type"`
	Language             string            `json:"language"`
	IsRetracted          bool              `json:"is_retracted"`
	IsXPAC               bool              `json:"is_xpac"`
	CitedByCount         int32             `json:"cited_by_count"`
	ReferencedWorksCount int32             `json:"referenced_works_count"`
	AuthorsCount         int32             `json:"authors_count"`
	FWCI                 *float64          `json:"fwci"`
	PrimaryTopic         *JSONPrimaryTopic `json:"primary_topic"`
	Topics               []JSONWorkTopic   `json:"topics"`
	Keywords             []JSONWorkKeyword `json:"keywords"`
	Authorships          []JSONAuthorship  `json:"authorships"`
	PrimaryLocation      *JSONPrimaryLoc   `json:"primary_location"`
	CountsByYear         []JSONYearCount   `json:"counts_by_year"`
	AbstractInvertedIdx  map[string][]int  `json:"abstract_inverted_index"`
}

type JSONIDRef struct {
	ID string `json:"id"`
}

type JSONPrimaryTopic struct {
	ID       string     `json:"id"`
	Score    float32    `json:"score"`
	Subfield *JSONIDRef `json:"subfield"`
	Field    *JSONIDRef `json:"field"`
	Domain   *JSONIDRef `json:"domain"`
}

type JSONWorkTopic struct {
	ID    string  `json:"id"`
	Score float32 `json:"score"`
}

type JSONWorkKeyword struct {
	DisplayName string `json:"display_name"`
}

type JSONWorkInstitution struct {
	ID          string `json:"id"`
	CountryCode string `json:"country_code"`
}

type JSONAuthorship struct {
	Author         *JSONIDRef            `json:"author"`
	AuthorPosition string                `json:"author_position"`
	Countries      []string              `json:"countries"`
	Institutions   []JSONWorkInstitution `json:"institutions"`
}

type JSONPrimarySource struct {
	ID   string `json:"id"`
	Type string `json:"type"`
}

type JSONPrimaryLoc struct {
	Source *JSONPrimarySource `json:"source"`
	IsOA   bool               `json:"is_oa"`
}

type JSONYearCount struct {
	Year         int32 `json:"year"`
	CitedByCount int32 `json:"cited_by_count"`
}
