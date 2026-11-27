package normalizer

import (
	"math"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestParseOpenAlexIDs(t *testing.T) {
	t.Parallel()

	if got := ParseWorkID("https://openalex.org/W2741809807"); got != 2741809807 {
		t.Fatalf("ParseWorkID full url = %d, want 2741809807", got)
	}
	if got := ParseWorkID("W2741809807"); got != 2741809807 {
		t.Fatalf("ParseWorkID short = %d, want 2741809807", got)
	}
	if got := ParseWorkID("https://openalex.org/w42"); got != 42 {
		t.Fatalf("ParseWorkID lowercase prefix = %d, want 42", got)
	}
	if got := ParseAuthorID("https://openalex.org/A5023888391"); got != 5023888391 {
		t.Fatalf("ParseAuthorID = %d, want 5023888391", got)
	}
	if got := ParseInstitutionID("https://openalex.org/I2799651439"); got != 2799651439 {
		t.Fatalf("ParseInstitutionID = %d, want 2799651439", got)
	}
	if got := ParseSourceID("https://openalex.org/S4306402476"); got != 4306402476 {
		t.Fatalf("ParseSourceID = %d, want 4306402476", got)
	}
	if got := ParseTopicID("https://openalex.org/T12799"); got != 12799 {
		t.Fatalf("ParseTopicID = %d, want 12799", got)
	}
	if got := ParseSubfieldID("https://openalex.org/subfields/1710"); got != 1710 {
		t.Fatalf("ParseSubfieldID = %d, want 1710", got)
	}
	if got := ParseFieldID("https://openalex.org/fields/17"); got != 17 {
		t.Fatalf("ParseFieldID = %d, want 17", got)
	}
	if got := ParseDomainID("https://openalex.org/domains/3"); got != 3 {
		t.Fatalf("ParseDomainID = %d, want 3", got)
	}

	badWorkIDs := []string{
		"",
		"https://openalex.org/",
		"https://openalex.org/W",
		"https://openalex.org/A2741809807",
		"https://openalex.org/W123abc",
		"https://openalex.org/W99999999999999999999999999",
	}
	for _, raw := range badWorkIDs {
		if got := ParseWorkID(raw); got != 0 {
			t.Fatalf("ParseWorkID(%q) = %d, want 0", raw, got)
		}
	}
	if got := ParseTopicID("https://openalex.org/T5000000000"); got != 0 {
		t.Fatalf("ParseTopicID(uint32 overflow) = %d, want 0", got)
	}
	if got := ParseFieldID("https://openalex.org/fields/70000"); got != 0 {
		t.Fatalf("ParseFieldID(uint16 overflow) = %d, want 0", got)
	}
	if got := ParseDomainID("https://openalex.org/domains/300"); got != 0 {
		t.Fatalf("ParseDomainID(uint8 overflow) = %d, want 0", got)
	}
}

func TestDate32Parsing(t *testing.T) {
	t.Parallel()
	dateDays := func(y int, m time.Month, d int) int32 {
		return int32(time.Date(y, m, d, 0, 0, 0, 0, time.UTC).Unix() / 86400)
	}

	for _, tc := range []struct {
		days int32
		want string
	}{
		{dateDays(1, time.January, 1), "0001-01-01"},
		{dateDays(1400, time.January, 1), "1400-01-01"},
		{dateDays(1900, time.January, 1), "1900-01-01"},
		{dateDays(2299, time.December, 31), "2299-12-31"},
		{dateDays(9999, time.December, 31), "9999-12-31"},
	} {
		got, ok := ParseParquetDate32(tc.days)
		if !ok || got.Format("2006-01-02") != tc.want {
			t.Fatalf("ParseParquetDate32(%d) = (%v, %v), want %s", tc.days, got, ok, tc.want)
		}
	}
	if _, ok := ParseParquetDate32(dateDays(0, time.January, 1)); ok {
		t.Fatal("year 0 must be rejected")
	}
	if _, ok := ParseParquetDate32(dateDays(10000, time.January, 1)); ok {
		t.Fatal("year 10000 must be rejected")
	}

	// JSON string date tests.
	validStrings := []string{
		"0001-01-01", "1400-01-01", "1850-05-01", "1899-12-31", "1900-01-01",
		"2024-02-29", "2026-10-14", "2026-10-14T12:00:00Z", "2299-12-31", "2500-01-01",
	}
	for _, s := range validStrings {
		got, ok := ParseDate32String(s)
		if !ok || got.Format("2006-01-02") != s[:10] {
			t.Fatalf("ParseDate32String(%q) = (%v, %v), want valid %s", s, got, ok, s[:10])
		}
	}

	invalidStrings := []string{
		"",
		"2024",
		"0000-01-01",
		"2023-02-29",
		"2024-04-31",
		"2024-13-01",
		"2024/05/01",
		"2024-05-01garbage",
	}
	for _, s := range invalidStrings {
		if got, ok := ParseDate32String(s); ok {
			t.Fatalf("ParseDate32String(%q) = %v, want invalid", s, got)
		}
	}
}

func TestTimestampParsing(t *testing.T) {
	t.Parallel()
	ts := ParseParquetTimestampMicros(1775888348300824)
	if ts.Year() != 2026 || ts.Month() != time.April || ts.Day() != 11 {
		t.Fatalf("ParseParquetTimestampMicros = %v, want 2026-04-11", ts)
	}

	if got := ParseTimestampString("2026-10-14T06:19:08.300824Z"); got.Year() != 2026 || got.Minute() != 19 {
		t.Fatalf("ParseTimestampString(RFC3339) = %v", got)
	}
	if got := ParseTimestampString("2026-10-14T06:19:08.300824"); got.Year() != 2026 || got.Second() != 8 {
		t.Fatalf("ParseTimestampString(ISO no Z) = %v", got)
	}
	if got := ParseTimestampString("2026-10-14"); got.Year() != 2026 || got.Day() != 14 {
		t.Fatalf("ParseTimestampString(date) = %v", got)
	}
	if got := ParseTimestampString(""); !got.Equal(unixEpochUTC) {
		t.Fatalf("ParseTimestampString(empty) = %v, want %v", got, unixEpochUTC)
	}
	if got := ParseTimestampString("not-a-date"); !got.Equal(unixEpochUTC) {
		t.Fatalf("ParseTimestampString(bad) = %v, want %v", got, unixEpochUTC)
	}
}

func TestCasingAndClamp(t *testing.T) {
	t.Parallel()

	if got := NormalizeKeyword("  Deep Learning \t"); got != "deep learning" {
		t.Fatalf("NormalizeKeyword = %q, want %q", got, "deep learning")
	}
	if got := NormalizeKeyword("   "); got != "" {
		t.Fatalf("NormalizeKeyword(spaces) = %q, want empty", got)
	}
	if got := NormalizeCountryCode(" us "); got != "US" {
		t.Fatalf("NormalizeCountryCode = %q, want US", got)
	}
	if got := NormalizeCountryCode(""); got != "" {
		t.Fatalf("NormalizeCountryCode(empty) = %q, want empty", got)
	}

	if got := ClampUint16(-5); got != 0 {
		t.Fatalf("ClampUint16(-5) = %d, want 0", got)
	}
	if got := ClampUint16(42); got != 42 {
		t.Fatalf("ClampUint16(42) = %d, want 42", got)
	}
	if got := ClampUint16(100_000); got != math.MaxUint16 {
		t.Fatalf("ClampUint16(100000) = %d, want %d", got, math.MaxUint16)
	}
	if got := ClampUint32(-1); got != 0 {
		t.Fatalf("ClampUint32(-1) = %d, want 0", got)
	}
	if got := ClampUint32(150_000); got != 150_000 {
		t.Fatalf("ClampUint32(150000) = %d, want 150000", got)
	}
}

func TestReconstructAbstract(t *testing.T) {
	t.Parallel()
	realJSON := []byte(`{"Presentation": [0], "of": [1], "NaturaQui": [2], "smartphone": [3], "application,": [4], "given": [5], "at": [6, 12], "SKiM": [7], "2021": [8], "training": [9], "in": [10], "Italy": [11], "CIHEAM": [13], "Bari": [14], "institute.": [15]}`)
	want := "Presentation of NaturaQui smartphone application, given at SKiM 2021 training in Italy at CIHEAM Bari institute."
	if got := ReconstructAbstractJSON(realJSON); got != want {
		t.Fatalf("ReconstructAbstractJSON(real) =\n  %q\nwant:\n  %q", got, want)
	}

	secondJSON := []byte(`{"hello": [0], "world": [1]}`)
	if got := ReconstructAbstractJSON(secondJSON); got != "hello world" {
		t.Fatalf("ReconstructAbstractJSON(second) = %q, want %q", got, "hello world")
	}

	for _, raw := range []string{"", " ", "{}", "null", "{invalid-json"} {
		if got := ReconstructAbstractJSON([]byte(raw)); got != "" {
			t.Fatalf("ReconstructAbstractJSON(%q) = %q, want empty", raw, got)
		}
	}
	dirty := map[string][]int{
		"alpha": {-5, 0},
		"beta":  {4},
		"bomb":  {999_999_999},
	}
	if got := ReconstructAbstract(dirty); got != "alpha beta" {
		t.Fatalf("ReconstructAbstract(dirty) = %q, want %q", got, "alpha beta")
	}
	huge := make(map[string][]int, 1)
	positions := make([]int, 4000)
	for i := range positions {
		positions[i] = i
	}
	huge["алгоритм"] = positions
	capped := ReconstructAbstract(huge)
	if len(capped) > MaxAbstractBytes {
		t.Fatalf("len(capped) = %d, exceeds MaxAbstractBytes (%d)", len(capped), MaxAbstractBytes)
	}
	if len(capped) < MaxAbstractBytes-32 {
		t.Fatalf("len(capped) = %d, unexpectedly short", len(capped))
	}
	if !utf8.ValidString(capped) {
		t.Fatal("capped UTF-8 abstract contains broken multi-byte rune")
	}
	if !strings.HasPrefix(capped, "алгоритм алгоритм") {
		t.Fatalf("unexpected prefix: %q", capped[:40])
	}
}
