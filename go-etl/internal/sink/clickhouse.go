package sink

import (
	"context"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	etlch "go-etl/internal/clickhouse"
)

type Conn interface {
	PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error)
	Ping(ctx context.Context) error
}

var worksColumns = []string{
	"work_id",
	"source_updated_at",
	"type",
	"primary_field_id",
	"is_retracted",
	"publication_year",
	"publication_date",
	"primary_topic_id",
	"topic_ids",
	"topic_scores",
	"primary_source_id",
	"primary_source_type",
	"fwci",
	"has_fwci",
}

var topicsColumns = []string{
	"topic_id",
	"display_name",
	"subfield_id",
	"subfield_name",
}

var identifierRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

var ErrUnsupported = errors.New("sink: entity is not part of the MVP schema")

type UnsupportedError struct{ Entity string }

func (e *UnsupportedError) Error() string {
	return fmt.Sprintf("%s: %s (see the MVP schema: only works and dim_topics are loaded)", ErrUnsupported, e.Entity)
}

func (e *UnsupportedError) Is(target error) bool { return target == ErrUnsupported }

type ClickHouseSink struct {
	conn     Conn
	database string
	works    string
	topics   string
	timeout  time.Duration
}

type Option func(*ClickHouseSink)

func WithInsertTimeout(d time.Duration) Option {
	return func(s *ClickHouseSink) {
		if d > 0 {
			s.timeout = d
		}
	}
}

func WithTables(works, topics string) Option {
	return func(s *ClickHouseSink) {
		if works != "" {
			s.works = works
		}
		if topics != "" {
			s.topics = topics
		}
	}
}

func NewClickHouseSink(conn Conn, database string, opts ...Option) (*ClickHouseSink, error) {
	database = strings.TrimSpace(database)
	if !identifierRE.MatchString(database) {
		return nil, fmt.Errorf("sink: invalid database name %q", database)
	}
	if conn == nil {
		return nil, errors.New("sink: nil clickhouse connection")
	}
	s := &ClickHouseSink{
		conn:     conn,
		database: database,
		works:    "works",
		topics:   "dim_topics",
		timeout:  2 * time.Minute,
	}
	for _, opt := range opts {
		opt(s)
	}
	if !identifierRE.MatchString(s.works) || !identifierRE.MatchString(s.topics) {
		return nil, fmt.Errorf("sink: invalid table name works=%q topics=%q", s.works, s.topics)
	}
	return s, nil
}

func (s *ClickHouseSink) Database() string { return s.database }

func (s *ClickHouseSink) Ping(ctx context.Context) error {
	return etlch.Classify(s.conn.Ping(ctx))
}

func (s *ClickHouseSink) WriteWorksBatch(ctx context.Context, b *WorksBatch) error {
	if b == nil || b.Len() == 0 {
		return nil
	}
	if err := validateWorksBatch(b); err != nil {
		return err
	}
	insertCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	batch, err := s.conn.PrepareBatch(insertCtx, s.insertSQL(s.works, worksColumns))
	if err != nil {
		return etlch.Classify(err)
	}
	for i := range b.WorkIDs {
		var pubDate uint32
		if b.PubDates[i] != nil {
			pubDate = publicationDateValue(*b.PubDates[i])
		}
		fwci, hasFWCI := fwciValue(b.FWCIs[i])
		if err := batch.Append(
			b.WorkIDs[i],
			b.UpdatedDates[i],
			b.WorkTypes[i],
			b.PrimaryFieldIDs[i],
			b.IsRetracted[i] != 0,
			b.PubYears[i],
			pubDate,
			b.PrimaryTopicIDs[i],
			b.TopicIDs[i],
			b.TopicScores[i],
			b.PrimarySourceIDs[i],
			b.PrimarySourceTypes[i],
			fwci,
			hasFWCI,
		); err != nil {
			_ = batch.Abort()
			return etlch.Classify(fmt.Errorf("sink: append works row %d: %w", i, err))
		}
	}
	if err := batch.Send(); err != nil {
		return etlch.Classify(fmt.Errorf("sink: send works batch (%d rows): %w", b.Len(), err))
	}
	return nil
}

func (s *ClickHouseSink) WriteTopicsBatch(ctx context.Context, b *TopicsBatch) error {
	if b == nil || b.Len() == 0 {
		return nil
	}
	if err := validateTopicsBatch(b); err != nil {
		return err
	}
	insertCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	batch, err := s.conn.PrepareBatch(insertCtx, s.insertSQL(s.topics, topicsColumns))
	if err != nil {
		return etlch.Classify(err)
	}
	for i := range b.TopicIDs {
		if err := batch.Append(
			b.TopicIDs[i],
			b.DisplayNames[i],
			clampUint16(b.SubfieldIDs[i]),
			b.SubfieldNames[i],
		); err != nil {
			_ = batch.Abort()
			return etlch.Classify(fmt.Errorf("sink: append dim_topics row %d: %w", i, err))
		}
	}
	if err := batch.Send(); err != nil {
		return etlch.Classify(fmt.Errorf("sink: send dim_topics batch (%d rows): %w", b.Len(), err))
	}
	return nil
}

func (s *ClickHouseSink) WriteInstitutionsBatch(ctx context.Context, b *InstitutionsBatch) error {
	if b == nil || b.Len() == 0 {
		return nil
	}
	return &UnsupportedError{Entity: "institutions"}
}

func (s *ClickHouseSink) Close() error { return nil }

func (s *ClickHouseSink) insertSQL(table string, cols []string) string {
	var b strings.Builder
	b.Grow(64 + len(cols)*16)
	b.WriteString("INSERT INTO `")
	b.WriteString(s.database)
	b.WriteString("`.`")
	b.WriteString(table)
	b.WriteString("` (")
	b.WriteString(strings.Join(cols, ", "))
	b.WriteString(")")
	return b.String()
}

func publicationDateValue(t time.Time) uint32 {
	if t.IsZero() {
		return 0
	}
	y, m, d := t.UTC().Date()
	if y < 1 || y > 9999 {
		return 0
	}
	return uint32(y)*10000 + uint32(m)*100 + uint32(d)
}

func fwciValue(p *float32) (float64, bool) {
	if p == nil {
		return 0, false
	}
	v := *p
	if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
		return 0, false
	}
	return float64(v), true
}

func clampUint16(v uint32) uint16 {
	if v > math.MaxUint16 {
		return math.MaxUint16
	}
	return uint16(v)
}

func validateWorksBatch(b *WorksBatch) error {
	n := b.Len()
	checks := []struct {
		name string
		len  int
	}{
		{"updated_dates", len(b.UpdatedDates)},
		{"work_types", len(b.WorkTypes)},
		{"primary_field_ids", len(b.PrimaryFieldIDs)},
		{"is_retracted", len(b.IsRetracted)},
		{"pub_years", len(b.PubYears)},
		{"pub_dates", len(b.PubDates)},
		{"primary_topic_ids", len(b.PrimaryTopicIDs)},
		{"topic_ids", len(b.TopicIDs)},
		{"topic_scores", len(b.TopicScores)},
		{"primary_source_ids", len(b.PrimarySourceIDs)},
		{"primary_source_types", len(b.PrimarySourceTypes)},
		{"fwcis", len(b.FWCIs)},
	}
	for _, c := range checks {
		if c.len != n {
			return fmt.Errorf("sink: works batch is not rectangular: work_id has %d rows, %s has %d", n, c.name, c.len)
		}
	}
	for i := range b.TopicIDs {
		if len(b.TopicIDs[i]) != len(b.TopicScores[i]) {
			return fmt.Errorf("sink: row %d has %d topic ids and %d scores (schema CHECK requires equal lengths)",
				i, len(b.TopicIDs[i]), len(b.TopicScores[i]))
		}
	}
	return nil
}

func validateTopicsBatch(b *TopicsBatch) error {
	n := b.Len()
	checks := []struct {
		name string
		len  int
	}{
		{"display_names", len(b.DisplayNames)},
		{"subfield_ids", len(b.SubfieldIDs)},
		{"subfield_names", len(b.SubfieldNames)},
	}
	for _, c := range checks {
		if c.len != n {
			return fmt.Errorf("sink: topics batch is not rectangular: topic_id has %d rows, %s has %d", n, c.name, c.len)
		}
	}
	return nil
}
