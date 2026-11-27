package sink

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type fakeConn struct{}

func (f *fakeConn) Ping(context.Context) error { return nil }

func (f *fakeConn) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	return nil, errors.New("fakeConn: PrepareBatch is not implemented")
}

func TestPublicationDateValue(t *testing.T) {
	cases := []struct {
		name string
		in   time.Time
		want uint32
	}{
		{"zero", time.Time{}, 0},
		{"regular", time.Date(2026, 3, 15, 0, 0, 0, 0, time.UTC), 20260315},
		{"with time of day", time.Date(2026, 3, 15, 23, 59, 59, 0, time.UTC), 20260315},
		{"january", time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC), 20200101},
		{"snapshot min year accepted", time.Date(1400, 1, 1, 0, 0, 0, 0, time.UTC), 14000101},
		{"out of range year", time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC), 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := publicationDateValue(tc.in); got != tc.want {
				t.Fatalf("publicationDateValue(%s) = %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}

func TestPublicationDateValueUsesUTC(t *testing.T) {
	loc := time.FixedZone("MSK", 3*3600)
	in := time.Date(2026, 3, 15, 1, 0, 0, 0, loc)
	if got, want := publicationDateValue(in), uint32(20260314); got != want {
		t.Fatalf("publicationDateValue = %d, want %d", got, want)
	}
}

func TestFWCIValue(t *testing.T) {
	zero := float32(0)
	oneAndHalf := float32(1.5)
	nan := float32(math.NaN())
	inf := float32(math.Inf(1))

	if v, ok := fwciValue(nil); ok || v != 0 {
		t.Fatalf("nil fwci = (%v, %v), want (0, false)", v, ok)
	}
	if v, ok := fwciValue(&zero); !ok || v != 0 {
		t.Fatalf("known zero fwci = (%v, %v), want (0, true)", v, ok)
	}
	if v, ok := fwciValue(&oneAndHalf); !ok || v != 1.5 {
		t.Fatalf("fwci = (%v, %v), want (1.5, true)", v, ok)
	}
	if _, ok := fwciValue(&nan); ok {
		t.Fatal("NaN fwci reported as known")
	}
	if _, ok := fwciValue(&inf); ok {
		t.Fatal("Inf fwci reported as known")
	}
}

func TestClampUint16(t *testing.T) {
	if got := clampUint16(0); got != 0 {
		t.Fatalf("clampUint16(0) = %d", got)
	}
	if got := clampUint16(3607); got != 3607 {
		t.Fatalf("clampUint16(3607) = %d", got)
	}
	if got := clampUint16(70000); got != math.MaxUint16 {
		t.Fatalf("clampUint16(70000) = %d, want %d", got, math.MaxUint16)
	}
}

func TestNewClickHouseSinkRejectsBadNames(t *testing.T) {
	if _, err := NewClickHouseSink(nil, "openalex"); err == nil {
		t.Fatal("nil connection accepted")
	}
	if _, err := NewClickHouseSink(&fakeConn{}, "open-alex"); err == nil {
		t.Fatal("invalid database accepted")
	}
	if _, err := NewClickHouseSink(&fakeConn{}, "openalex", WithTables("works; DROP", "dim_topics")); err == nil {
		t.Fatal("invalid table accepted")
	}
}

func TestInsertSQLListsColumnsForTable(t *testing.T) {
	s, err := NewClickHouseSink(&fakeConn{}, "openalex")
	if err != nil {
		t.Fatalf("NewClickHouseSink: %v", err)
	}
	sql := s.insertSQL(s.works, worksColumns)
	for _, want := range []string{
		"INSERT INTO `openalex`.`works` (",
		"work_id", "source_updated_at", "publication_date", "has_fwci", ")",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("insert SQL %q does not contain %q", sql, want)
		}
	}
	if strings.Contains(sql, "VALUES") {
		t.Fatalf("insert SQL must not spell VALUES: %q", sql)
	}
}

func TestValidateWorksBatch(t *testing.T) {
	b := NewWorksBatch(2)
	b.WorkIDs = append(b.WorkIDs, 1, 2)

	if err := validateWorksBatch(b); err == nil {
		t.Fatal("ragged batch accepted")
	}

	full := newFullBatch(2)
	if err := validateWorksBatch(full); err != nil {
		t.Fatalf("valid batch rejected: %v", err)
	}

	full.TopicScores[0] = append(full.TopicScores[0], 0.5)
	if err := validateWorksBatch(full); err == nil {
		t.Fatal("unequal topic arrays accepted")
	} else if !strings.Contains(err.Error(), "scores") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateTopicsBatch(t *testing.T) {
	b := NewTopicsBatch(1)
	b.TopicIDs = append(b.TopicIDs, uint32(1701))
	if err := validateTopicsBatch(b); err == nil {
		t.Fatal("ragged topics batch accepted")
	}
	b.DisplayNames = append(b.DisplayNames, "Machine learning")
	b.SubfieldIDs = append(b.SubfieldIDs, 1702)
	b.SubfieldNames = append(b.SubfieldNames, "AI")
	if err := validateTopicsBatch(b); err != nil {
		t.Fatalf("valid topics batch rejected: %v", err)
	}
}

func TestWriteInstitutionsIsUnsupported(t *testing.T) {
	s, err := NewClickHouseSink(&fakeConn{}, "openalex")
	if err != nil {
		t.Fatalf("NewClickHouseSink: %v", err)
	}
	b := NewInstitutionsBatch(1)
	b.InstitutionIDs = append(b.InstitutionIDs, 1)
	err = s.WriteInstitutionsBatch(t.Context(), b)
	if !errors.Is(err, ErrUnsupported) {
		t.Fatalf("error = %v, want ErrUnsupported", err)
	}
	if !strings.Contains(err.Error(), "institutions") {
		t.Fatalf("error does not name the entity: %v", err)
	}
}

func TestEmptyBatchIsNoop(t *testing.T) {
	s, err := NewClickHouseSink(&fakeConn{}, "openalex")
	if err != nil {
		t.Fatalf("NewClickHouseSink: %v", err)
	}
	if err := s.WriteWorksBatch(t.Context(), NewWorksBatch(1)); err != nil {
		t.Fatalf("empty works batch: %v", err)
	}
	if err := s.WriteTopicsBatch(t.Context(), NewTopicsBatch(1)); err != nil {
		t.Fatalf("empty topics batch: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func newFullBatch(n int) *WorksBatch {
	b := NewWorksBatch(n)
	for i := range n {
		b.WorkIDs = append(b.WorkIDs, uint64(2741809807+i))
		b.UpdatedDates = append(b.UpdatedDates, time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC))
		b.WorkTypes = append(b.WorkTypes, "article")
		b.PrimaryFieldIDs = append(b.PrimaryFieldIDs, 17)
		b.IsRetracted = append(b.IsRetracted, 0)
		b.PubYears = append(b.PubYears, 2026)
		b.PubDates = append(b.PubDates, b.AllocPubDate(time.Date(2026, 3, 15, 0, 0, 0, 0, time.UTC)))
		b.PrimaryTopicIDs = append(b.PrimaryTopicIDs, uint32(10001))
		b.TopicIDs = append(b.TopicIDs, []uint32{10001, 10002})
		b.TopicScores = append(b.TopicScores, []float32{0.7, 0.3})
		b.PrimarySourceIDs = append(b.PrimarySourceIDs, uint64(1234))
		b.PrimarySourceTypes = append(b.PrimarySourceTypes, "journal")
		b.FWCIs = append(b.FWCIs, b.AllocFWCI(1.5))
	}
	return b
}
