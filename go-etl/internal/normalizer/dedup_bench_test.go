package normalizer

import (
	"runtime"
	"testing"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

func BenchmarkDedupMapVsRoaring(b *testing.B) {
	const n = 1_000_000
	ids := make([]uint64, n)
	x := uint64(2741809807)
	for i := range ids {
		x = x*6364136223846793005 + 1442695040888963407
		ids[i] = 2_000_000_000 + x%1_000_000_000
	}

	b.Run("map", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			m := make(map[uint64]struct{}, n)
			for _, id := range ids {
				m[id] = struct{}{}
			}
			runtime.KeepAlive(m)
		}
	})

	b.Run("roaring64", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			bm := roaring64.New()
			for _, id := range ids {
				bm.CheckedAdd(id)
			}
			runtime.KeepAlive(bm)
		}
	})

	b.Run("sharded_roaring64", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			d := NewWorkDeduplicator()
			for _, id := range ids {
				d.AddIfNew(id)
			}
			runtime.KeepAlive(d)
		}
	})
}

func TestDedupSemantics(t *testing.T) {
	t.Parallel()

	d := NewWorkDeduplicator()
	if !d.AddIfNew(2741809807) {
		t.Fatal("first occurrence must be new")
	}
	if d.AddIfNew(2741809807) {
		t.Fatal("second occurrence must be a duplicate")
	}
	if !d.AddIfNew(0) || !d.AddIfNew(0) {
		t.Fatal("id=0 must always pass (dirty records are kept)")
	}
	if got := d.Len(); got != 1 {
		t.Fatalf("Len = %d, want 1", got)
	}
	if d.SizeInBytes() == 0 {
		t.Fatal("SizeInBytes must report the bitmap footprint")
	}
	d.Clear()
	if got := d.Len(); got != 0 {
		t.Fatalf("Len after Clear = %d", got)
	}
}
