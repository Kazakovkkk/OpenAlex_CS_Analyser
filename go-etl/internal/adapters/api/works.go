package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type WorksQuery struct {
	FieldID             int
	FromUpdatedDate     string
	FromCreatedDate     string
	FromPublicationDate string
	Select              []string
	PerPage             int
	MaxRecords          int
	Sort                string
}

var DefaultWorksSelect = []string{
	"id", "doi", "title", "publication_year", "publication_date",
	"created_date", "updated_date", "type", "language",
	"is_retracted", "is_xpac", "cited_by_count", "referenced_works_count",
	"fwci", "primary_topic", "topics", "keywords",
	"authorships", "primary_location", "counts_by_year", "abstract_inverted_index",
}

var validSelectFields = map[string]struct{}{
	"id": {}, "doi": {}, "title": {}, "display_name": {}, "alternate_titles": {},
	"relevance_score": {}, "rerank_score": {}, "publication_year": {}, "publication_date": {},
	"ids": {}, "language": {}, "primary_location": {}, "parent_works": {}, "lineage": {},
	"child_works_count": {}, "pull_notice_works": {}, "sources": {}, "type": {},
	"is_traditional": {}, "type_crossref": {}, "indexed_in": {}, "open_access": {},
	"authorships": {}, "institution_assertions": {}, "institutions": {},
	"countries_distinct_count": {}, "institutions_distinct_count": {},
	"corresponding_author_ids": {}, "corresponding_institution_ids": {},
	"apc_list": {}, "apc_paid": {}, "fwci": {}, "is_authors_truncated": {},
	"has_fulltext": {}, "fulltext_origin": {}, "cited_by_count": {},
	"reviewed_by_count": {}, "citation_normalized_percentile": {},
	"cited_by_percentile_year": {}, "biblio": {}, "is_retracted": {}, "is_pulled": {},
	"is_paratext": {}, "is_xpac": {}, "primary_topic": {}, "topics": {}, "keywords": {},
	"concepts": {}, "mesh": {}, "locations_count": {}, "locations": {},
	"best_oa_location": {}, "sustainable_development_goals": {}, "study_designs": {},
	"awards": {}, "funders": {}, "datasets": {}, "versions": {}, "has_content": {},
	"content_urls": {}, "referenced_works_count": {}, "referenced_works": {},
	"related_works": {}, "abstract_inverted_index": {}, "cited_by_api_url": {},
	"counts_by_year": {}, "updated_date": {}, "created_date": {},
}

func (q WorksQuery) Filter() string {
	fieldID := q.FieldID
	if fieldID == 0 {
		fieldID = 17
	}
	filter := "primary_topic.field.id:" + strconv.Itoa(fieldID)
	if q.FromUpdatedDate != "" {
		filter += ",from_updated_date:" + q.FromUpdatedDate
	}
	if q.FromCreatedDate != "" {
		filter += ",from_created_date:" + q.FromCreatedDate
	}
	if q.FromPublicationDate != "" {
		filter += ",from_publication_date:" + q.FromPublicationDate
	}
	return filter
}

func (q WorksQuery) pageSize() int {
	switch {
	case q.PerPage <= 0:
		return defaultPageSize
	case q.PerPage > maxPageSize:
		return maxPageSize
	default:
		return q.PerPage
	}
}

type worksPage struct {
	Meta struct {
		Count      int     `json:"count"`
		PerPage    int     `json:"per_page"`
		NextCursor string  `json:"next_cursor"`
		CostUSD    float64 `json:"cost_usd"`
	} `json:"meta"`
	Results []json.RawMessage `json:"results"`
}

var ErrAPIKeyRequired = errors.New("api: cursor pagination beyond the first page requires an api_key " +
	"(use POST /api-keys with your email, then pass api_key)")

func (c *Client) IterateWorks(ctx context.Context, q WorksQuery, fn func(raw []byte) error) (stats Stats, err error) {
	if fn == nil {
		return stats, errors.New("api: nil works callback")
	}
	if q.FieldID == 0 {
		q.FieldID = 17
	}
	start := time.Now()
	requestsBefore := c.Requests()
	retriesBefore := c.Retries()
	bytesBefore := c.BytesRead()
	defer func() {
		stats.Elapsed = time.Since(start)
		stats.Requests = int(c.Requests() - requestsBefore)
		stats.Retries = int(c.Retries() - retriesBefore)
	}()

	cursor := "*"
	seenCursors := make(map[string]struct{}, 64)
	for {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		if stats.Pages > 0 && c.apiKey() == "" {
			return stats, ErrAPIKeyRequired
		}
		if _, dup := seenCursors[cursor]; dup {
			return stats, fmt.Errorf("api: cursor loop detected at %q", cursor)
		}
		seenCursors[cursor] = struct{}{}

		params := url.Values{}
		params.Set("filter", q.Filter())
		params.Set("per-page", strconv.Itoa(q.pageSize()))
		params.Set("cursor", cursor)
		if q.Sort != "" {
			params.Set("sort", q.Sort)
		}
		if len(q.Select) > 0 {
			params.Set("select", strings.Join(q.Select, ","))
		}

		var page worksPage
		if err := c.getJSON(ctx, "/works", params, &page); err != nil {
			return stats, err
		}
		stats.Pages++
		stats.CostUSD += page.Meta.CostUSD
		stats.Bytes = c.BytesRead() - bytesBefore

		for _, raw := range page.Results {
			if q.MaxRecords > 0 && stats.Records >= q.MaxRecords {
				return stats, nil
			}
			if err := fn(raw); err != nil {
				if errors.Is(err, ErrStopIteration) {
					return stats, nil
				}
				return stats, err
			}
			stats.Records++
		}

		if q.MaxRecords > 0 && stats.Records >= q.MaxRecords {
			return stats, nil
		}
		if len(page.Results) == 0 || page.Meta.NextCursor == "" {
			return stats, nil
		}
		cursor = page.Meta.NextCursor
	}
}
