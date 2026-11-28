package s3

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"path"
	"slices"
	"strings"
	"time"
)

const (
	MaxManifestBytes = 64 << 20

	threshold15MB = 15 << 20
	threshold20MB = 20 << 20
)

var (
	// ErrManifestUnavailable marks a network/404/5xx failure while reading a
	// required release manifest.
	ErrManifestUnavailable = errors.New("s3: manifest unavailable")
	// ErrManifestInvalid marks a manifest whose entity, format, date, file list
	// or file keys do not satisfy the release contract.
	ErrManifestInvalid = errors.New("s3: manifest invalid")
)

type ManifestUnavailableError struct {
	Key string
	Err error
}

func (e *ManifestUnavailableError) Error() string {
	return fmt.Sprintf("s3: manifest %s is unavailable: %v", e.Key, e.Err)
}

func (e *ManifestUnavailableError) Unwrap() error { return e.Err }

func (e *ManifestUnavailableError) Is(target error) bool { return target == ErrManifestUnavailable }

type ManifestInvalidError struct {
	Key    string
	Reason string
}

func (e *ManifestInvalidError) Error() string {
	return fmt.Sprintf("s3: manifest %s is invalid: %s", e.Key, e.Reason)
}

func (e *ManifestInvalidError) Is(target error) bool { return target == ErrManifestInvalid }

type Manifest struct {
	Date          string          `json:"date"`
	Format        string          `json:"format"`
	Entity        string          `json:"entity"`
	RecordCount   int64           `json:"record_count"`
	ContentLength int64           `json:"content_length"`
	Files         []ManifestEntry `json:"files"`
}
type ManifestEntry struct {
	URL  string       `json:"url"`
	Meta ManifestMeta `json:"meta"`
}

type ManifestMeta struct {
	ContentLength int64 `json:"content_length"`
	RecordCount   int64 `json:"record_count"`
}

type SizeStats struct {
	FileCount      int
	TotalBytes     int64
	TotalRecords   int64
	MinBytes       int64
	P25Bytes       int64
	MedianBytes    int64
	P75Bytes       int64
	P95Bytes       int64
	MaxBytes       int64
	Under15MBCount int
	Under20MBCount int
}

func ParquetManifestKey(entity string) string {
	return path.Join(ParquetPrefix, entity, "manifest.json")
}

// CanonicalID is the reproducible identity of one manifest: its date, format,
// entity, record count and the sorted (url, content_length) pairs. Sorting is
// applied to a copy, so the shard assignment (which follows the manifest order)
// is never affected (round-2 answer 3.10).
func (m *Manifest) CanonicalID() string {
	if m == nil {
		return ""
	}
	files := make([]ManifestEntry, len(m.Files))
	copy(files, m.Files)
	slices.SortFunc(files, func(a, b ManifestEntry) int {
		return strings.Compare(a.URL, b.URL)
	})

	h := sha256.New()
	fmt.Fprintf(h, "%s|%s|%s|%d\n", m.Date, m.Format, m.Entity, m.RecordCount)
	for _, f := range files {
		fmt.Fprintf(h, "%s|%d\n", f.URL, f.Meta.ContentLength)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// CombineIDs folds several manifest ids (for example works + topics) into the
// single id stored in pipeline_state.
func CombineIDs(parts ...string) string {
	cleaned := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			cleaned = append(cleaned, p)
		}
	}
	slices.Sort(cleaned)
	h := sha256.New()
	for _, p := range cleaned {
		h.Write([]byte(p))
		h.Write([]byte{'\n'})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func (c *Client) FetchManifest(ctx context.Context, key string) (*Manifest, error) {
	raw, err := c.ReadAll(ctx, c.bucket, key, MaxManifestBytes)
	if err != nil {
		return nil, &ManifestUnavailableError{Key: key, Err: err}
	}
	m, err := ParseManifest(raw)
	if err != nil {
		return nil, &ManifestInvalidError{Key: key, Reason: err.Error()}
	}
	return m, nil
}

// Validate checks the release contract of one manifest: entity, Parquet format,
// a real date, a non-empty file list and unique file keys/URLs.
func (m *Manifest) Validate(entity string) error {
	if m == nil {
		return &ManifestInvalidError{Reason: "manifest is nil"}
	}
	if strings.TrimSpace(m.Entity) != entity {
		return &ManifestInvalidError{Reason: fmt.Sprintf("entity is %q, want %q", m.Entity, entity)}
	}
	if strings.TrimSpace(strings.ToLower(m.Format)) != "parquet" {
		return &ManifestInvalidError{Reason: fmt.Sprintf("format is %q, want parquet", m.Format)}
	}
	if _, err := time.Parse("2006-01-02", strings.TrimSpace(m.Date)); err != nil {
		return &ManifestInvalidError{Reason: fmt.Sprintf("date %q is not YYYY-MM-DD", m.Date)}
	}
	if m.RecordCount < 0 {
		return &ManifestInvalidError{Reason: fmt.Sprintf("record_count %d is negative", m.RecordCount)}
	}
	if len(m.Files) == 0 {
		return &ManifestInvalidError{Reason: "file list is empty"}
	}
	seenURL := make(map[string]struct{}, len(m.Files))
	seenKey := make(map[string]struct{}, len(m.Files))
	for i, f := range m.Files {
		raw := strings.TrimSpace(f.URL)
		if raw == "" {
			return &ManifestInvalidError{Reason: fmt.Sprintf("file %d has an empty url", i)}
		}
		_, key, err := ParseS3URL(raw)
		if err != nil {
			return &ManifestInvalidError{Reason: fmt.Sprintf("file %d: %v", i, err)}
		}
		if f.Meta.ContentLength < 0 {
			return &ManifestInvalidError{Reason: fmt.Sprintf("file %d has a negative content_length", i)}
		}
		if _, dup := seenURL[raw]; dup {
			return &ManifestInvalidError{Reason: fmt.Sprintf("duplicate url %q", raw)}
		}
		if _, dup := seenKey[key]; dup {
			return &ManifestInvalidError{Reason: fmt.Sprintf("duplicate object key %q", key)}
		}
		seenURL[raw] = struct{}{}
		seenKey[key] = struct{}{}
	}
	return nil
}

func ParseManifest(data []byte) (*Manifest, error) {
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("parse manifest json: %w", err)
	}
	return &m, nil
}

func (m *Manifest) SelectEntries(shardIndex, shardTotal, maxFiles int) ([]ManifestEntry, error) {
	if shardTotal < 1 {
		return nil, fmt.Errorf("invalid shard_total %d: must be >= 1", shardTotal)
	}
	if shardIndex < 0 || shardIndex >= shardTotal {
		return nil, fmt.Errorf("invalid shard_index %d for shard_total %d", shardIndex, shardTotal)
	}
	if maxFiles < 0 {
		return nil, fmt.Errorf("invalid max_files %d: must be >= 0", maxFiles)
	}

	capHint := len(m.Files)/shardTotal + 1
	if maxFiles > 0 && maxFiles < capHint {
		capHint = maxFiles
	}
	selected := make([]ManifestEntry, 0, capHint)

	for i, entry := range m.Files {
		if i%shardTotal != shardIndex {
			continue
		}
		selected = append(selected, entry)
		if maxFiles > 0 && len(selected) >= maxFiles {
			break
		}
	}
	return selected, nil
}

func ComputeSizeStats(entries []ManifestEntry) SizeStats {
	if len(entries) == 0 {
		return SizeStats{}
	}

	sizes := make([]int64, len(entries))
	stats := SizeStats{FileCount: len(entries)}

	for i, e := range entries {
		sz := e.Meta.ContentLength
		sizes[i] = sz
		stats.TotalBytes += sz
		stats.TotalRecords += e.Meta.RecordCount
		if sz < threshold15MB {
			stats.Under15MBCount++
		}
		if sz < threshold20MB {
			stats.Under20MBCount++
		}
	}

	slices.Sort(sizes)
	stats.MinBytes = sizes[0]
	stats.P25Bytes = percentile(sizes, 0.25)
	stats.MedianBytes = percentile(sizes, 0.50)
	stats.P75Bytes = percentile(sizes, 0.75)
	stats.P95Bytes = percentile(sizes, 0.95)
	stats.MaxBytes = sizes[len(sizes)-1]

	return stats
}

func percentile(sorted []int64, p float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(p*float64(len(sorted)))) - 1
	idx = max(0, min(idx, len(sorted)-1))
	return sorted[idx]
}

func PickClosestBySize(entries []ManifestEntry, targetBytes []int64) []ManifestEntry {
	if len(entries) == 0 || len(targetBytes) == 0 {
		return nil
	}

	used := make([]bool, len(entries))
	result := make([]ManifestEntry, 0, len(targetBytes))

	for _, target := range targetBytes {
		bestIdx := -1
		var bestDiff int64 = math.MaxInt64
		for i, e := range entries {
			if used[i] {
				continue
			}
			diff := e.Meta.ContentLength - target
			if diff < 0 {
				diff = -diff
			}
			if diff < bestDiff {
				bestDiff = diff
				bestIdx = i
			}
		}
		if bestIdx >= 0 {
			used[bestIdx] = true
			result = append(result, entries[bestIdx])
		}
	}
	return result
}

func ExtractPartition(raw string) string {
	for seg := range strings.SplitSeq(raw, "/") {
		if strings.HasPrefix(seg, "updated_date=") {
			return seg
		}
	}
	return ""
}
