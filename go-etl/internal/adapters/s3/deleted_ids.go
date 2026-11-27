package s3

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"path"
	"strings"
	"time"

	"go-etl/internal/normalizer"
)

var DeletedIDsKey = path.Join(ParquetPrefix, "works", "deleted_ids.csv.gz")

func (c *Client) StreamDeletedIDs(ctx context.Context, fn func(id uint64, deletedAt time.Time, hasDate bool) error) (int64, error) {
	out, err := c.GetObject(ctx, c.bucket, DeletedIDsKey, disableChecksumLog)
	if err != nil {
		return 0, fmt.Errorf("deleted_ids: %w", err)
	}
	defer func() { _ = out.Body.Close() }()

	gz, err := gzip.NewReader(out.Body)
	if err != nil {
		return 0, fmt.Errorf("deleted_ids: gzip: %w", err)
	}
	defer func() { _ = gz.Close() }()

	sc := bufio.NewScanner(gz)
	sc.Buffer(make([]byte, 64<<10), 1<<20)

	var seen int64
	for sc.Scan() {
		if err := ctx.Err(); err != nil {
			return seen, err
		}
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 || line[0] == '#' {
			continue
		}
		id, at, has := ParseDeletedLine(string(line))
		if id == 0 {
			continue
		}
		seen++
		if err := fn(id, at, has); err != nil {
			return seen, err
		}
	}
	if err := sc.Err(); err != nil {
		return seen, fmt.Errorf("deleted_ids: scan: %w", err)
	}
	return seen, nil
}

// ParseDeletedLine parses one log row. It returns id 0 for a header or garbage.
func ParseDeletedLine(line string) (uint64, time.Time, bool) {
	raw, rest, _ := strings.Cut(strings.TrimSpace(line), ",")
	id := normalizer.ParseWorkID(strings.TrimSpace(raw))
	if id == 0 {
		return 0, time.Time{}, false
	}
	rest = strings.TrimSpace(rest)
	if rest == "" {
		return id, time.Time{}, false
	}
	if at, err := time.Parse("2006-01-02", rest); err == nil {
		return id, at.UTC(), true
	}
	return id, time.Time{}, false
}
