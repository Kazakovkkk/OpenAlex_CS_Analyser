package normalizer

import (
	"math"
	"strings"
	"time"
)

const (
	minDateYear = 1
	maxDateYear = 9999
)

var (
	unixEpochUTC = time.Unix(0, 0).UTC()
)

func ParseOpenAlexID(raw string, prefix byte) uint64 {
	if len(raw) == 0 {
		return 0
	}
	start := strings.LastIndexByte(raw, '/') + 1
	if start >= len(raw) {
		return 0
	}
	if prefix != 0 {
		b := raw[start]
		if b != prefix && b != prefix+('a'-'A') {
			return 0
		}
		start++
		if start >= len(raw) {
			return 0
		}
	}

	var val uint64
	for i := start; i < len(raw); i++ {
		c := raw[i]
		if c < '0' || c > '9' {
			return 0
		}
		d := uint64(c - '0')
		if val > (math.MaxUint64-d)/10 {
			return 0
		}
		val = val*10 + d
	}
	return val
}

func ParseWorkID(raw string) uint64        { return ParseOpenAlexID(raw, 'W') }
func ParseAuthorID(raw string) uint64      { return ParseOpenAlexID(raw, 'A') }
func ParseInstitutionID(raw string) uint64 { return ParseOpenAlexID(raw, 'I') }
func ParseSourceID(raw string) uint64      { return ParseOpenAlexID(raw, 'S') }

func ParseTopicID(raw string) uint32 {
	v := ParseOpenAlexID(raw, 'T')
	if v > math.MaxUint32 {
		return 0
	}
	return uint32(v)
}

func ParseSubfieldID(raw string) uint32 {
	v := ParseOpenAlexID(raw, 0)
	if v > math.MaxUint32 {
		return 0
	}
	return uint32(v)
}

func ParseFieldID(raw string) uint16 {
	v := ParseOpenAlexID(raw, 0)
	if v > math.MaxUint16 {
		return 0
	}
	return uint16(v)
}

func ParseDomainID(raw string) uint8 {
	v := ParseOpenAlexID(raw, 0)
	if v > math.MaxUint8 {
		return 0
	}
	return uint8(v)
}

func ParseParquetDate32(days int32) (time.Time, bool) {
	return ParseDateDays(days)
}
func ParseDateDays(days int32) (time.Time, bool) {
	t := time.Unix(int64(days)*86400, 0).UTC()
	if y := t.Year(); y < minDateYear || y > maxDateYear {
		return time.Time{}, false
	}
	return t, true
}

func ParseDate32String(s string) (time.Time, bool) {
	if len(s) < 10 {
		return time.Time{}, false
	}
	if len(s) > 10 && s[10] != 'T' && s[10] != ' ' {
		return time.Time{}, false
	}
	if s[4] != '-' || s[7] != '-' {
		return time.Time{}, false
	}

	year, okY := parseASCIIInt(s, 0, 4)
	month, okM := parseASCIIInt(s, 5, 2)
	day, okD := parseASCIIInt(s, 8, 2)
	if !okY || !okM || !okD {
		return time.Time{}, false
	}
	if year < minDateYear || year > maxDateYear || month < 1 || month > 12 || day < 1 || day > 31 {
		return time.Time{}, false
	}

	t := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
	if t.Year() != year || int(t.Month()) != month || t.Day() != day {
		return time.Time{}, false
	}
	return t, true
}

func parseASCIIInt(s string, offset, length int) (int, bool) {
	val := 0
	for i := offset; i < offset+length; i++ {
		c := s[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		val = val*10 + int(c-'0')
	}
	return val, true
}

func ParseParquetTimestampMicros(us int64) time.Time {
	return time.UnixMicro(us).UTC()
}

func ParseTimestampString(s string) time.Time {
	if t, ok := ParseUpdatedDate(s); ok {
		return t
	}
	return unixEpochUTC
}

func ParseUpdatedDate(s string) (time.Time, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, false
	}
	for _, layout := range []string{
		time.RFC3339Nano,
		"2006-01-02T15:04:05.999999999",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
		"2006-01-02",
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}

func NormalizeKeyword(s string) string {
	s = strings.TrimSpace(s)
	if len(s) == 0 {
		return ""
	}
	return strings.ToLower(s)
}

func NormalizeCountryCode(s string) string {
	s = strings.TrimSpace(s)
	if len(s) == 0 {
		return ""
	}
	return strings.ToUpper(s)
}

func ClampUint16(v int32) uint16 {
	if v <= 0 {
		return 0
	}
	if v > math.MaxUint16 {
		return math.MaxUint16
	}
	return uint16(v)
}

func ClampUint32(v int32) uint32 {
	if v <= 0 {
		return 0
	}
	return uint32(v)
}
