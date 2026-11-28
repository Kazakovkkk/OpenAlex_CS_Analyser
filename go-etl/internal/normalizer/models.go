package normalizer

type ParquetWork struct {
	ID              *string  `parquet:"id,optional"`
	Type            *string  `parquet:"type,optional"`
	PublicationYear *int32   `parquet:"publication_year,optional"`
	PublicationDate *int32   `parquet:"publication_date,date,optional"`
	UpdatedDate     *int64   `parquet:"updated_date,timestamp(microsecond),optional"`
	IsRetracted     *bool    `parquet:"is_retracted,optional"`
	IsXPAC          *bool    `parquet:"is_xpac,optional"`
	FWCI            *float64 `parquet:"fwci,optional"`

	PrimaryTopic    *ParquetPrimaryTopic    `parquet:"primary_topic,optional"`
	Topics          []ParquetWorkTopic      `parquet:"topics,list,optional" parquet-element:",optional"`
	PrimaryLocation *ParquetPrimaryLocation `parquet:"primary_location,optional"`
}

type ParquetIDRef struct {
	ID *string `parquet:"id,optional"`
}

type ParquetPrimaryTopic struct {
	ID    *string       `parquet:"id,optional"`
	Field *ParquetIDRef `parquet:"field,optional"`
}

type ParquetWorkTopic struct {
	ID    *string  `parquet:"id,optional"`
	Score *float32 `parquet:"score,optional"`
}

type ParquetPrimarySource struct {
	ID   *string `parquet:"id,optional"`
	Type *string `parquet:"type,optional"`
}

type ParquetPrimaryLocation struct {
	Source *ParquetPrimarySource `parquet:"source,optional"`
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
	ID              string            `json:"id"`
	Type            string            `json:"type"`
	PublicationYear *int32            `json:"publication_year"`
	PublicationDate *string           `json:"publication_date"`
	UpdatedDate     *string           `json:"updated_date"`
	IsRetracted     *bool             `json:"is_retracted"`
	IsXPAC          bool              `json:"is_xpac"`
	FWCI            *float64          `json:"fwci"`
	PrimaryTopic    *JSONPrimaryTopic `json:"primary_topic"`
	Topics          []JSONWorkTopic   `json:"topics"`
	PrimaryLocation *JSONPrimaryLoc   `json:"primary_location"`
}

type JSONIDRef struct {
	ID string `json:"id"`
}

type JSONPrimaryTopic struct {
	ID    string     `json:"id"`
	Field *JSONIDRef `json:"field"`
}

type JSONWorkTopic struct {
	ID    string  `json:"id"`
	Score float32 `json:"score"`
}

type JSONPrimarySource struct {
	ID   string `json:"id"`
	Type string `json:"type"`
}

type JSONPrimaryLoc struct {
	Source *JSONPrimarySource `json:"source"`
}
