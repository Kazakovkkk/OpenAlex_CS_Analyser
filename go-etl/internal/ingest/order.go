package ingest

import (
	"fmt"
	"slices"
	"strings"
	"time"

	s3 "go-etl/internal/adapters/s3"
)

func SortEntries(entries []Entry, mode OrderMode) []Entry {
	out := append([]Entry(nil), entries...)
	switch mode {
	case OrderManifest, "":
		return out
	case OrderSmallFirst:
		slices.SortStableFunc(out, bySizeAsc)
		return out
	case OrderLargestFirst:
		slices.SortStableFunc(out, bySizeDesc)
		return out
	case OrderBalanced:
		slices.SortStableFunc(out, bySizeAsc)
		k := len(out) / 10
		if k == 0 {
			return out
		}
		head := out[:len(out)-k]
		big := out[len(out)-k:]
		insertAt := len(head) / 4
		res := make([]Entry, 0, len(out))
		res = append(res, head[:insertAt]...)
		res = append(res, big...)
		res = append(res, head[insertAt:]...)
		return res
	default:
		slices.SortStableFunc(out, bySizeAsc)
		return out
	}
}

func bySizeAsc(a, b Entry) int {
	switch {
	case a.Meta.ContentLength < b.Meta.ContentLength:
		return -1
	case a.Meta.ContentLength > b.Meta.ContentLength:
		return 1
	default:
		return strings.Compare(a.URL, b.URL)
	}
}

func bySizeDesc(a, b Entry) int { return -bySizeAsc(a, b) }

// FilterByPartitionDate keeps only the files whose `updated_date=YYYY-MM-DD`
// partition is not older than from. The S3 snapshot is partitioned by update
// date, so this is the S3 equivalent of the API's from_date and makes
// "load only recent changes" possible without downloading the whole snapshot.
//
// An empty from keeps everything; entries without a parsable partition are kept
// (fail open: an unknown layout must not silently drop data).
func FilterByPartitionDate(entries []Entry, from string) ([]Entry, error) {
	if from == "" {
		return entries, nil
	}
	cutoff, err := time.Parse("2006-01-02", from)
	if err != nil {
		return nil, fmt.Errorf("ingest: invalid from date %q: %w", from, err)
	}
	kept := make([]Entry, 0, len(entries))
	for _, e := range entries {
		raw, ok := strings.CutPrefix(s3.ExtractPartition(e.URL), "updated_date=")
		if !ok {
			kept = append(kept, e)
			continue
		}
		day, err := time.Parse("2006-01-02", raw)
		if err != nil {
			kept = append(kept, e)
			continue
		}
		if !day.Before(cutoff) {
			kept = append(kept, e)
		}
	}
	return kept, nil
}