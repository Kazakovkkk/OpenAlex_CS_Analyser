package knownids

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func TestIndexAddContainsLen(t *testing.T) {
	t.Parallel()
	idx := New()
	if idx.Contains(42) {
		t.Fatal("empty index contains an id")
	}
	idx.Add(42)
	idx.Add(42)
	idx.Add(1 << 40)
	idx.Add(0)

	if !idx.Contains(42) || !idx.Contains(1<<40) {
		t.Fatal("index lost an id")
	}
	if idx.Contains(43) {
		t.Fatal("index reports a foreign id")
	}
	if got := idx.Len(); got != 2 {
		t.Fatalf("Len = %d, want 2", got)
	}
	if idx.SizeInBytes() == 0 {
		t.Fatal("SizeInBytes = 0")
	}
}

func TestIndexConcurrent(t *testing.T) {
	t.Parallel()
	idx := New()
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := range 500 {
				idx.Add(uint64(w*1000 + i + 1))
			}
		}(w)
	}
	wg.Wait()

	if got := idx.Len(); got != 8*500 {
		t.Fatalf("Len = %d, want %d", got, 8*500)
	}
	for w := range 8 {
		for i := range 500 {
			if !idx.Contains(uint64(w*1000 + i + 1)) {
				t.Fatalf("missing id %d", w*1000+i+1)
			}
		}
	}
}

func TestBuildRejectsBadInput(t *testing.T) {
	t.Parallel()
	if _, _, err := Build(t.Context(), nil, "openalex", "works"); err == nil {
		t.Fatal("nil connection accepted")
	}
	if _, _, err := Build(t.Context(), fakeConn{}, "open-alex", "works"); err == nil {
		t.Fatal("invalid database accepted")
	}
}

func TestDatabaseName(t *testing.T) {
	t.Parallel()
	for _, ok := range []string{"openalex", "db_1", "Db"} {
		if _, err := databaseName(ok); err != nil {
			t.Fatalf("%q rejected: %v", ok, err)
		}
	}
	for _, bad := range []string{"", "open-alex", "db;drop", "1db"} {
		if _, err := databaseName(bad); err == nil {
			t.Fatalf("%q accepted", bad)
		}
	}
}

type fakeConn struct{}

func (fakeConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	return nil, fmt.Errorf("fakeConn: not implemented")
}
