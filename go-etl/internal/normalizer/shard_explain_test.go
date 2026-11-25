package normalizer

import (
	"testing"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

func TestDedupShardingKeepsContainersIntact(t *testing.T) {
	t.Parallel()

	const n = 200_000
	ids := make([]uint64, n)
	x := uint64(2741809807)
	for i := range ids {
		x = x*6364136223846793005 + 1442695040888963407
		ids[i] = 2_000_000_000 + x%1_000_000_000
	}

	single := roaring64.New()
	for _, id := range ids {
		single.CheckedAdd(id)
	}

	d := NewWorkDeduplicator()
	for _, id := range ids {
		d.AddIfNew(id)
	}

	singleBytes := single.GetSizeInBytes()
	shardedBytes := d.SizeInBytes()
	if shardedBytes > singleBytes*12/10 {
		t.Fatalf("sharded footprint %d B is %.2fx the single-bitmap %d B: sharding is splitting containers",
			shardedBytes, float64(shardedBytes)/float64(singleBytes), singleBytes)
	}

	// The property itself: ids in the same 64KiB block must land in one shard.
	a := (uint64(0x0001_0000) >> dedupShardShift) & dedupShardMask
	b := (uint64(0x0001_FFFF) >> dedupShardShift) & dedupShardMask
	if a != b {
		t.Fatalf("ids of one container landed in different shards: %d != %d", a, b)
	}
}
