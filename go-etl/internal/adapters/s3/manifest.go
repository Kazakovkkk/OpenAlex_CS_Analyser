package s3

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"path"
	"slices"
	"strings"
)

const (
	MaxManifestBytes = 64 << 20

	threshold15MB = 15 << 20
	threshold20MB = 20 << 20
)

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
		return nil, fmt.Errorf("fetch manifest %s: %w", key, err)
	}
	return ParseManifest(raw)
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
