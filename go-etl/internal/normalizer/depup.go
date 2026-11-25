package normalizer

import (
	"sync"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

const (
	dedupShards     = 64
	dedupShardMask  = dedupShards - 1
	dedupShardShift = 16
)

type dedupShard struct {
	mu sync.Mutex
	bm *roaring64.Bitmap
	_  [48]byte
}

type WorkDeduplicator struct {
	shards [dedupShards]dedupShard
}

func NewWorkDeduplicator() *WorkDeduplicator {
	d := &WorkDeduplicator{}
	for i := range d.shards {
		d.shards[i].bm = roaring64.New()
	}
	return d
}

func (d *WorkDeduplicator) AddIfNew(workID uint64) bool {
	if workID == 0 {
		return true
	}
	sh := &d.shards[(workID>>dedupShardShift)&dedupShardMask]
	sh.mu.Lock()
	added := sh.bm.CheckedAdd(workID)
	sh.mu.Unlock()
	return added
}

func (d *WorkDeduplicator) Len() uint64 {
	var total uint64
	for i := range d.shards {
		sh := &d.shards[i]
		sh.mu.Lock()
		total += sh.bm.GetCardinality()
		sh.mu.Unlock()
	}
	return total
}

func (d *WorkDeduplicator) SizeInBytes() uint64 {
	var total uint64
	for i := range d.shards {
		sh := &d.shards[i]
		sh.mu.Lock()
		total += sh.bm.GetSizeInBytes()
		sh.mu.Unlock()
	}
	return total
}

func (d *WorkDeduplicator) Clear() {
	for i := range d.shards {
		sh := &d.shards[i]
		sh.mu.Lock()
		sh.bm.Clear()
		sh.mu.Unlock()
	}
}
