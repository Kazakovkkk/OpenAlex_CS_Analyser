package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

const deleteChunk = 5000

// DeleteWorks removes every stored version of the given work ids and verifies
// that none of them is readable any more. It returns how many ids were removed.
func DeleteWorks(ctx context.Context, conn Conn, database string, ids []uint64) (int64, error) {
	if conn == nil {
		return 0, errors.New("pipeline: nil connection for deletions")
	}
	if len(ids) == 0 {
		return 0, nil
	}
	if !identifierRE.MatchString(database) {
		return 0, fmt.Errorf("pipeline: invalid database name %q", database)
	}

	var deleted int64
	for start := 0; start < len(ids); start += deleteChunk {
		end := min(start+deleteChunk, len(ids))
		list := uintList(ids[start:end])
		table := "`" + database + "`.`works`"

		if err := conn.Exec(ctx, "DELETE FROM "+table+" WHERE work_id IN ("+list+")"); err != nil {
			return deleted, fmt.Errorf("pipeline: delete works: %w", err)
		}
		var left uint64
		if err := conn.QueryRow(ctx,
			"SELECT count() FROM "+table+" WHERE work_id IN ("+list+")").Scan(&left); err != nil {
			return deleted, fmt.Errorf("pipeline: verify deletion: %w", err)
		}
		if left != 0 {
			return deleted, fmt.Errorf("pipeline: %d row(s) of deleted works are still readable", left)
		}
		deleted += int64(end - start)
	}
	return deleted, nil
}

func uintList(ids []uint64) string {
	var b strings.Builder
	b.Grow(len(ids) * 8)
	for i, id := range ids {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.FormatUint(id, 10))
	}
	return b.String()
}
