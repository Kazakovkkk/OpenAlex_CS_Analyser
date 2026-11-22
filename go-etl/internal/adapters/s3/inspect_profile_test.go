//go:build inspect

package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
)

const (
	maxAbstractBytes = 32 << 10
	exactCSFieldID   = "https://openalex.org/fields/17"
)

var (
	minDate32Days = int32(time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC).Unix() / 86400)
	maxDate32Days = int32(time.Date(2299, 12, 31, 0, 0, 0, 0, time.UTC).Unix() / 86400)
)

type profileTopicField struct {
	ID *string `parquet:"id,optional"`
}

type profilePrimaryTopic struct {
	Field *profileTopicField `parquet:"field,optional"`
}

type profileWorkRow struct {
	DOI                  *string              `parquet:"doi,optional"`
	Title                *string              `parquet:"title,optional"`
	PublicationDate      *int32               `parquet:"publication_date,date,optional"`
	PublicationYear      *int32               `parquet:"publication_year,optional"`
	AuthorsCount         *int32               `parquet:"authors_count,optional"`
	ReferencedWorksCount *int32               `parquet:"referenced_works_count,optional"`
	CitedByCount         *int32               `parquet:"cited_by_count,optional"`
	FWCI                 *float64             `parquet:"fwci,optional"`
	IsRetracted          *bool                `parquet:"is_retracted,optional"`
	IsXPAC               *bool                `parquet:"is_xpac,optional"`
	AbstractInvertedIdx  *string              `parquet:"abstract_inverted_index,optional"`
	PrimaryTopic         *profilePrimaryTopic `parquet:"primary_topic,optional"`
}

type worksProfile struct {
	TotalRows int64

	MaxRefWorksCount   int32
	RefWorksOverUint16 int64
	RefWorksNegative   int64

	MaxAuthorsCount   int32
	AuthorsOverUint16 int64
	AuthorsZero       int64
	AuthorsNull       int64

	MaxCitedByCount   int32
	CitedByOverUint16 int64

	HasPubYear        bool
	MinPubYear        int32
	MaxPubYear        int32
	PubYearNull       int64
	PubYearZeroOrNeg  int64
	PubYearBefore1900 int64
	PubYearAfter2026  int64
	PubYearOverUint16 int64

	HasPubDate        bool
	MinPubDateDays    int32
	MaxPubDateDays    int32
	PubDateNull       int64
	PubDateBefore1900 int64
	PubDateAfter2299  int64

	DOINullOrEmpty      int64
	TitleNullOrEmpty    int64
	FWCINull            int64
	RetractedCount      int64
	AbstractNullOrEmpty int64
	AbstractOver32KB    int64
	MaxAbstractBytes    int
	PrimaryTopicNull    int64

	XPACTrue            int64
	XPACFalse           int64
	XPACNull            int64
	CSExactAll          int64
	CSSuffixAll         int64
	CSContainsAll       int64
	TargetCSNonXPAC     int64
	FilterMismatchCount int64
	MismatchSamples     []string
}

func TestInspectWorksProfile(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	client, err := NewAnonymousClient(ctx)
	if err != nil {
		t.Fatalf("NewAnonymousClient: %v", err)
	}

	manifest, err := client.FetchManifest(ctx, ParquetManifestKey("works"))
	if err != nil {
		t.Fatalf("FetchManifest(works): %v", err)
	}

	const mb = 1 << 20
	targets := []int64{5 * mb, 15 * mb, 30 * mb}
	picked := PickClosestBySize(manifest.Files, targets)
	if len(picked) == 0 {
		t.Fatal("no works files found in manifest")
	}

	var combined worksProfile
	var totalBytes int64
	var totalScanTime time.Duration

	for idx, entry := range picked {
		bucket, key, err := ParseS3URL(entry.URL)
		if err != nil {
			t.Fatalf("ParseS3URL(%s): %v", entry.URL, err)
		}

		dlStart := time.Now()
		raw, err := client.ReadAll(ctx, bucket, key, 64*mb)
		if err != nil {
			t.Fatalf("ReadAll(%s): %v", entry.URL, err)
		}
		dlElapsed := time.Since(dlStart)

		prof, scanElapsed, err := profileWorksParquet(raw)
		if err != nil {
			t.Fatalf("profileWorksParquet(%s): %v", entry.URL, err)
		}

		label := fmt.Sprintf("File %d/%d: %s (%s, dl=%s)",
			idx+1, len(picked), entry.URL, formatBytes(int64(len(raw))), dlElapsed.Round(time.Millisecond))
		logWorksProfile(t, label, prof, scanElapsed)

		combined.merge(prof)
		totalBytes += int64(len(raw))
		totalScanTime += scanElapsed
	}

	summaryLabel := fmt.Sprintf("COMBINED (%d files, %s total)", len(picked), formatBytes(totalBytes))
	logWorksProfile(t, summaryLabel, &combined, totalScanTime)
}

func profileWorksParquet(raw []byte) (*worksProfile, time.Duration, error) {
	reader := parquet.NewGenericReader[profileWorkRow](bytes.NewReader(raw))
	defer reader.Close()

	var prof worksProfile
	buf := make([]profileWorkRow, 256)

	start := time.Now()
	for {
		clear(buf)

		n, err := reader.Read(buf)
		for i := range n {
			prof.observe(&buf[i])
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, 0, fmt.Errorf("read generic rows: %w", err)
		}
	}
	return &prof, time.Since(start), nil
}

func (p *worksProfile) observe(r *profileWorkRow) {
	p.TotalRows++

	if r.ReferencedWorksCount != nil {
		v := *r.ReferencedWorksCount
		if v > p.MaxRefWorksCount {
			p.MaxRefWorksCount = v
		}
		if v > math.MaxUint16 {
			p.RefWorksOverUint16++
		}
		if v < 0 {
			p.RefWorksNegative++
		}
	}

	if r.AuthorsCount == nil {
		p.AuthorsNull++
	} else {
		v := *r.AuthorsCount
		if v > p.MaxAuthorsCount {
			p.MaxAuthorsCount = v
		}
		if v > math.MaxUint16 {
			p.AuthorsOverUint16++
		}
		if v == 0 {
			p.AuthorsZero++
		}
	}

	if r.CitedByCount != nil {
		v := *r.CitedByCount
		if v > p.MaxCitedByCount {
			p.MaxCitedByCount = v
		}
		if v > math.MaxUint16 {
			p.CitedByOverUint16++
		}
	}

	if r.PublicationYear == nil {
		p.PubYearNull++
	} else {
		y := *r.PublicationYear
		if !p.HasPubYear {
			p.MinPubYear, p.MaxPubYear, p.HasPubYear = y, y, true
		} else {
			if y < p.MinPubYear {
				p.MinPubYear = y
			}
			if y > p.MaxPubYear {
				p.MaxPubYear = y
			}
		}
		if y <= 0 {
			p.PubYearZeroOrNeg++
		}
		if y < 1900 {
			p.PubYearBefore1900++
		}
		if y > 2026 {
			p.PubYearAfter2026++
		}
		if y > math.MaxUint16 {
			p.PubYearOverUint16++
		}
	}

	if r.PublicationDate == nil {
		p.PubDateNull++
	} else {
		d := *r.PublicationDate
		if !p.HasPubDate {
			p.MinPubDateDays, p.MaxPubDateDays, p.HasPubDate = d, d, true
		} else {
			if d < p.MinPubDateDays {
				p.MinPubDateDays = d
			}
			if d > p.MaxPubDateDays {
				p.MaxPubDateDays = d
			}
		}
		if d < minDate32Days {
			p.PubDateBefore1900++
		}
		if d > maxDate32Days {
			p.PubDateAfter2299++
		}
	}

	if r.DOI == nil || *r.DOI == "" {
		p.DOINullOrEmpty++
	}
	if r.Title == nil || *r.Title == "" {
		p.TitleNullOrEmpty++
	}
	if r.FWCI == nil {
		p.FWCINull++
	}
	if r.IsRetracted != nil && *r.IsRetracted {
		p.RetractedCount++
	}

	if r.AbstractInvertedIdx == nil || len(*r.AbstractInvertedIdx) <= 2 {
		p.AbstractNullOrEmpty++
	} else {
		n := len(*r.AbstractInvertedIdx)
		if n > p.MaxAbstractBytes {
			p.MaxAbstractBytes = n
		}
		if n > maxAbstractBytes {
			p.AbstractOver32KB++
		}
	}

	isXPAC := false
	switch {
	case r.IsXPAC == nil:
		p.XPACNull++
	case *r.IsXPAC:
		p.XPACTrue++
		isXPAC = true
	default:
		p.XPACFalse++
	}

	if r.PrimaryTopic == nil || r.PrimaryTopic.Field == nil || r.PrimaryTopic.Field.ID == nil || *r.PrimaryTopic.Field.ID == "" {
		p.PrimaryTopicNull++
		return
	}

	fid := *r.PrimaryTopic.Field.ID
	exact := fid == exactCSFieldID
	suffix := strings.HasSuffix(fid, "/fields/17")
	contains := strings.Contains(fid, "fields/17")

	if exact {
		p.CSExactAll++
		if !isXPAC {
			p.TargetCSNonXPAC++
		}
	}
	if suffix {
		p.CSSuffixAll++
	}
	if contains {
		p.CSContainsAll++
	}
	if exact != suffix || exact != contains {
		p.FilterMismatchCount++
		if len(p.MismatchSamples) < 5 {
			p.MismatchSamples = append(p.MismatchSamples, fid)
		}
	}
}

func (p *worksProfile) merge(o *worksProfile) {
	p.TotalRows += o.TotalRows

	p.MaxRefWorksCount = max(p.MaxRefWorksCount, o.MaxRefWorksCount)
	p.RefWorksOverUint16 += o.RefWorksOverUint16
	p.RefWorksNegative += o.RefWorksNegative

	p.MaxAuthorsCount = max(p.MaxAuthorsCount, o.MaxAuthorsCount)
	p.AuthorsOverUint16 += o.AuthorsOverUint16
	p.AuthorsZero += o.AuthorsZero
	p.AuthorsNull += o.AuthorsNull

	p.MaxCitedByCount = max(p.MaxCitedByCount, o.MaxCitedByCount)
	p.CitedByOverUint16 += o.CitedByOverUint16

	if o.HasPubYear {
		if !p.HasPubYear {
			p.MinPubYear, p.MaxPubYear, p.HasPubYear = o.MinPubYear, o.MaxPubYear, true
		} else {
			p.MinPubYear = min(p.MinPubYear, o.MinPubYear)
			p.MaxPubYear = max(p.MaxPubYear, o.MaxPubYear)
		}
	}
	p.PubYearNull += o.PubYearNull
	p.PubYearZeroOrNeg += o.PubYearZeroOrNeg
	p.PubYearBefore1900 += o.PubYearBefore1900
	p.PubYearAfter2026 += o.PubYearAfter2026
	p.PubYearOverUint16 += o.PubYearOverUint16

	if o.HasPubDate {
		if !p.HasPubDate {
			p.MinPubDateDays, p.MaxPubDateDays, p.HasPubDate = o.MinPubDateDays, o.MaxPubDateDays, true
		} else {
			p.MinPubDateDays = min(p.MinPubDateDays, o.MinPubDateDays)
			p.MaxPubDateDays = max(p.MaxPubDateDays, o.MaxPubDateDays)
		}
	}
	p.PubDateNull += o.PubDateNull
	p.PubDateBefore1900 += o.PubDateBefore1900
	p.PubDateAfter2299 += o.PubDateAfter2299

	p.DOINullOrEmpty += o.DOINullOrEmpty
	p.TitleNullOrEmpty += o.TitleNullOrEmpty
	p.FWCINull += o.FWCINull
	p.RetractedCount += o.RetractedCount
	p.AbstractNullOrEmpty += o.AbstractNullOrEmpty
	p.AbstractOver32KB += o.AbstractOver32KB
	p.MaxAbstractBytes = max(p.MaxAbstractBytes, o.MaxAbstractBytes)
	p.PrimaryTopicNull += o.PrimaryTopicNull

	p.XPACTrue += o.XPACTrue
	p.XPACFalse += o.XPACFalse
	p.XPACNull += o.XPACNull
	p.CSExactAll += o.CSExactAll
	p.CSSuffixAll += o.CSSuffixAll
	p.CSContainsAll += o.CSContainsAll
	p.TargetCSNonXPAC += o.TargetCSNonXPAC
	p.FilterMismatchCount += o.FilterMismatchCount

	for _, s := range o.MismatchSamples {
		if len(p.MismatchSamples) >= 5 {
			break
		}
		p.MismatchSamples = append(p.MismatchSamples, s)
	}
}

func logWorksProfile(t *testing.T, title string, p *worksProfile, scanElapsed time.Duration) {
	t.Helper()

	var sb strings.Builder
	rps := 0.0
	if scanElapsed > 0 {
		rps = float64(p.TotalRows) / scanElapsed.Seconds()
	}

	fmt.Fprintf(&sb, "\n============================================================\n")
	fmt.Fprintf(&sb, "%s\n", title)
	fmt.Fprintf(&sb, "Scanned: %d rows in %s (%.0f rows/sec projected)\n",
		p.TotalRows, scanElapsed.Round(time.Millisecond), rps)

	fmt.Fprintf(&sb, "\n[1. Counters & uint16 (<=65535) Check]\n")
	fmt.Fprintf(&sb, "  referenced_works_count : max=%-8d | >uint16=%d | <0=%d\n",
		p.MaxRefWorksCount, p.RefWorksOverUint16, p.RefWorksNegative)
	fmt.Fprintf(&sb, "  authors_count          : max=%-8d | >uint16=%d | ==0=%d (%.2f%%) | null=%d\n",
		p.MaxAuthorsCount, p.AuthorsOverUint16, p.AuthorsZero, pct(p.AuthorsZero, p.TotalRows), p.AuthorsNull)
	fmt.Fprintf(&sb, "  cited_by_count         : max=%-8d | >uint16=%d\n",
		p.MaxCitedByCount, p.CitedByOverUint16)

	fmt.Fprintf(&sb, "\n[2. Years & ClickHouse Date32 (1900..2299) Bounds]\n")
	if p.HasPubYear {
		fmt.Fprintf(&sb, "  publication_year       : min=%d | max=%d | null=%d | <=0=%d | <1900=%d | >2026=%d | >uint16=%d\n",
			p.MinPubYear, p.MaxPubYear, p.PubYearNull, p.PubYearZeroOrNeg, p.PubYearBefore1900, p.PubYearAfter2026, p.PubYearOverUint16)
	} else {
		fmt.Fprintf(&sb, "  publication_year       : all NULL (%d)\n", p.PubYearNull)
	}
	if p.HasPubDate {
		fmt.Fprintf(&sb, "  publication_date       : min=%s (%d) | max=%s (%d) | null=%d (%.2f%%) | <1900=%d | >2299=%d\n",
			formatParquetDate(p.MinPubDateDays), p.MinPubDateDays,
			formatParquetDate(p.MaxPubDateDays), p.MaxPubDateDays,
			p.PubDateNull, pct(p.PubDateNull, p.TotalRows), p.PubDateBefore1900, p.PubDateAfter2299)
	} else {
		fmt.Fprintf(&sb, "  publication_date       : all NULL (%d)\n", p.PubDateNull)
	}

	fmt.Fprintf(&sb, "\n[3. Nulls, Dirty Records (Task 1) & Abstract Cap]\n")
	fmt.Fprintf(&sb, "  doi null/empty         : %d (%.2f%%)\n", p.DOINullOrEmpty, pct(p.DOINullOrEmpty, p.TotalRows))
	fmt.Fprintf(&sb, "  title null/empty       : %d (%.2f%%)\n", p.TitleNullOrEmpty, pct(p.TitleNullOrEmpty, p.TotalRows))
	fmt.Fprintf(&sb, "  fwci null              : %d (%.2f%%)\n", p.FWCINull, pct(p.FWCINull, p.TotalRows))
	fmt.Fprintf(&sb, "  is_retracted == true   : %d (%.2f%%)\n", p.RetractedCount, pct(p.RetractedCount, p.TotalRows))
	fmt.Fprintf(&sb, "  primary_topic null     : %d (%.2f%%)\n", p.PrimaryTopicNull, pct(p.PrimaryTopicNull, p.TotalRows))
	fmt.Fprintf(&sb, "  abstract null/empty    : %d (%.2f%%) | raw JSON >32KiB=%d | max=%s\n",
		p.AbstractNullOrEmpty, pct(p.AbstractNullOrEmpty, p.TotalRows), p.AbstractOver32KB, formatBytes(int64(p.MaxAbstractBytes)))

	fmt.Fprintf(&sb, "\n[4. is_xpac & CS (fields/17) Pre-Filter Accuracy]\n")
	fmt.Fprintf(&sb, "  is_xpac                : true=%d (%.2f%%) | false=%d (%.2f%%) | null=%d\n",
		p.XPACTrue, pct(p.XPACTrue, p.TotalRows), p.XPACFalse, pct(p.XPACFalse, p.TotalRows), p.XPACNull)
	fmt.Fprintf(&sb, "  CS match (all rows)    : exact=%d (%.2f%%) | suffix=%d | contains=%d | mismatches=%d\n",
		p.CSExactAll, pct(p.CSExactAll, p.TotalRows), p.CSSuffixAll, p.CSContainsAll, p.FilterMismatchCount)
	if len(p.MismatchSamples) > 0 {
		fmt.Fprintf(&sb, "  mismatch samples       : %v\n", p.MismatchSamples)
	}
	fmt.Fprintf(&sb, "  TARGET CS (!is_xpac)   : %d (%.2f%% of scanned)\n",
		p.TargetCSNonXPAC, pct(p.TargetCSNonXPAC, p.TotalRows))

	t.Log(sb.String())
}

func formatParquetDate(days int32) string {
	return time.Unix(int64(days)*86400, 0).UTC().Format("2006-01-02")
}

func pct(part, total int64) float64 {
	if total == 0 {
		return 0
	}
	return 100 * float64(part) / float64(total)
}
