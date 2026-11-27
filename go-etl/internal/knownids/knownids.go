// Package knownids holds the set of work_ids already stored in ClickHouse, so a
// load can tell a new record from an update of a known one without querying per batch.
package knownids

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring/v2/roaring64"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

const (
	shards     = 64
	shardMask  = shards - 1
	shardShift = 16
)

type Conn interface {
	Query(ctx context.Context, query string, args ...any) (driver.Rows, error)
}

type shard struct {
	mu sync.Mutex
	bm *roaring64.Bitmap
	_  [48]byte
}

type Index struct {
	shards [shards]shard
}

func New() *Index {
	idx := &Index{}
	for i := range idx.shards {
		idx.shards[i].bm = roaring64.New()
	}
	return idx
}

func (i *Index) shardFor(id uint64) *shard {
	return &i.shards[(id>>shardShift)&shardMask]
}

func (i *Index) Add(id uint64) {
	if id == 0 {
		return
	}
	sh := i.shardFor(id)
	sh.mu.Lock()
	sh.bm.Add(id)
	sh.mu.Unlock()
}

func (i *Index) Contains(id uint64) bool {
	if id == 0 {
		return false
	}
	sh := i.shardFor(id)
	sh.mu.Lock()
	ok := sh.bm.Contains(id)
	sh.mu.Unlock()
	return ok
}

func (i *Index) Len() uint64 {
	var total uint64
	for k := range i.shards {
		sh := &i.shards[k]
		sh.mu.Lock()
		total += sh.bm.GetCardinality()
		sh.mu.Unlock()
	}
	return total
}

func (i *Index) SizeInBytes() uint64 {
	var total uint64
	for k := range i.shards {
		sh := &i.shards[k]
		sh.mu.Lock()
		total += sh.bm.GetSizeInBytes()
		sh.mu.Unlock()
	}
	return total
}

func databaseName(db string) (string, error) {
	for k := range db {
		c := db[k]
		if c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (k > 0 && c >= '0' && c <= '9') {
			continue
		}
		return "", fmt.Errorf("knownids: invalid database name %q", db)
	}
	if db == "" {
		return "", errors.New("knownids: empty database name")
	}
	return db, nil
}

// Build streams every work_id from the target table once. If the stream breaks,
// the error is returned and the caller must not continue with a partial set.
func Build(ctx context.Context, conn Conn, database, table string) (*Index, time.Duration, error) {
	if conn == nil {
		return nil, 0, errors.New("knownids: nil connection")
	}
	db, err := databaseName(database)
	if err != nil {
		return nil, 0, err
	}
	start := time.Now()
	rows, err := conn.Query(ctx, fmt.Sprintf("SELECT work_id FROM `%s`.`%s`", db, table))
	if err != nil {
		return nil, 0, fmt.Errorf("knownids: query: %w", err)
	}
	defer rows.Close()

	idx := New()
	for rows.Next() {
		var id uint64
		if err := rows.Scan(&id); err != nil {
			return nil, 0, fmt.Errorf("knownids: scan: %w", err)
		}
		idx.Add(id)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("knownids: stream: %w", err)
	}
	return idx, time.Since(start), nil
}
