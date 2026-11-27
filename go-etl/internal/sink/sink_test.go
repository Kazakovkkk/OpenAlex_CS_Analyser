package sink

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestWorksBatchResetAndApproxBytes(t *testing.T) {
	t.Parallel()

	b := NewWorksBatch(16)
	d := time.Date(2025, 6, 15, 0, 0, 0, 0, time.UTC)

	b.WorkIDs = append(b.WorkIDs, 1001, 1002)
	b.WorkTypes = append(b.WorkTypes, "article", "preprint")
	b.PubYears = append(b.PubYears, 2025, 2024)
	b.PubDates = append(b.PubDates, b.AllocPubDate(d), nil)
	b.FWCIs = append(b.FWCIs, b.AllocFWCI(1.5), nil)
	b.TopicIDs = append(b.TopicIDs, []uint32{10, 20}, nil)

	if b.Len() != 2 {
		t.Fatalf("Len() = %d, want 2", b.Len())
	}
	if *b.PubDates[0] != d || b.PubDates[1] != nil {
		t.Fatalf("unexpected PubDates: %v", b.PubDates)
	}
	if *b.FWCIs[0] != 1.5 || b.FWCIs[1] != nil {
		t.Fatalf("unexpected FWCIs: %v", b.FWCIs)
	}
	if got := b.ApproxBytes(); got <= 2*worksFixedBytesPerRow {
		t.Fatalf("ApproxBytes() = %d, expected > %d", got, 2*worksFixedBytesPerRow)
	}

	b.Reset()

	if b.Len() != 0 || b.ApproxBytes() != 0 {
		t.Fatalf("after Reset: Len=%d, ApproxBytes=%d, want 0", b.Len(), b.ApproxBytes())
	}
	if cap(b.WorkIDs) != 16 || cap(b.WorkTypes) != 16 || cap(b.fwciSlab) != 16 {
		t.Fatalf("Reset lost slice capacity: WorkIDs=%d WorkTypes=%d fwciSlab=%d",
			cap(b.WorkIDs), cap(b.WorkTypes), cap(b.fwciSlab))
	}
	if b.WorkTypes[:2][0] != "" || b.PubDates[:2][0] != nil || b.TopicIDs[:2][0] != nil {
		t.Fatal("Reset did not clear pointer/string references in backing arrays")
	}
}

func TestWorksBatchPoolCapGuard(t *testing.T) {
	t.Parallel()

	pool := NewWorksBatchPool(8, 16)
	b := pool.Get()
	if cap(b.WorkIDs) != 8 {
		t.Fatalf("initial cap = %d, want 8", cap(b.WorkIDs))
	}

	b.WorkIDs = append(b.WorkIDs, 1, 2, 3)
	pool.Put(b)
	if b.Len() != 0 {
		t.Fatalf("Put did not Reset batch: Len=%d", b.Len())
	}

	oversized := NewWorksBatch(32)
	oversized.WorkIDs = append(oversized.WorkIDs, 99)
	pool.Put(oversized)
	if oversized.Len() != 0 {
		t.Fatalf("Put did not Reset oversized batch: Len=%d", oversized.Len())
	}
	pool.Put(nil)
}

func TestDimensionsBatches(t *testing.T) {
	t.Parallel()

	tb := NewTopicsBatch(4)
	tb.TopicIDs = append(tb.TopicIDs, 10001)
	tb.DisplayNames = append(tb.DisplayNames, "Machine Learning")
	if tb.Len() != 1 || tb.ApproxBytes() <= topicsFixedBytesPerRow {
		t.Fatalf("unexpected TopicsBatch state: len=%d bytes=%d", tb.Len(), tb.ApproxBytes())
	}
	tb.Reset()
	if tb.Len() != 0 || tb.DisplayNames[:1][0] != "" {
		t.Fatal("TopicsBatch.Reset failed to clear state")
	}

	ib := NewInstitutionsBatch(4)
	ib.InstitutionIDs = append(ib.InstitutionIDs, 2799651439)
	ib.DisplayNames = append(ib.DisplayNames, "MIT")
	ib.LineageIDs = append(ib.LineageIDs, []uint64{2799651439})
	if ib.Len() != 1 || ib.ApproxBytes() <= institutionsFixedBytesPerRow {
		t.Fatalf("unexpected InstitutionsBatch state: len=%d bytes=%d", ib.Len(), ib.ApproxBytes())
	}
	ib.Reset()
	if ib.Len() != 0 || ib.LineageIDs[:1][0] != nil {
		t.Fatal("InstitutionsBatch.Reset failed to clear state")
	}
}

func TestNoopSinkConcurrentAndCancel(t *testing.T) {
	t.Parallel()

	s := NewNoopSink()
	ctx := context.Background()

	const (
		workers       = 8
		batchesPerWkr = 25
		rowsPerBatch  = 10
	)

	var wg sync.WaitGroup
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			b := NewWorksBatch(rowsPerBatch)
			for range batchesPerWkr {
				b.Reset()
				for i := range rowsPerBatch {
					b.WorkIDs = append(b.WorkIDs, uint64(i+1))
				}
				if err := s.WriteWorksBatch(ctx, b); err != nil {
					t.Errorf("WriteWorksBatch error: %v", err)
				}
			}
		}()
	}
	wg.Wait()

	if err := s.WriteWorksBatch(ctx, nil); err != nil {
		t.Fatalf("WriteWorksBatch(nil): %v", err)
	}
	if err := s.WriteWorksBatch(ctx, NewWorksBatch(4)); err != nil {
		t.Fatalf("WriteWorksBatch(empty): %v", err)
	}

	tb := NewTopicsBatch(2)
	tb.TopicIDs = append(tb.TopicIDs, 1, 2)
	if err := s.WriteTopicsBatch(ctx, tb); err != nil {
		t.Fatalf("WriteTopicsBatch: %v", err)
	}
	ib := NewInstitutionsBatch(2)
	ib.InstitutionIDs = append(ib.InstitutionIDs, 10, 20, 30)
	if err := s.WriteInstitutionsBatch(ctx, ib); err != nil {
		t.Fatalf("WriteInstitutionsBatch: %v", err)
	}

	st := s.Stats()
	wantBatches := uint64(workers * batchesPerWkr)
	wantRows := wantBatches * rowsPerBatch
	if st.WorksBatches != wantBatches || st.WorksRows != wantRows || st.WorksBytes == 0 {
		t.Fatalf("Works stats = (%d batches, %d rows, %d bytes), want (%d, %d, >0)",
			st.WorksBatches, st.WorksRows, st.WorksBytes, wantBatches, wantRows)
	}
	if st.TopicsBatches != 1 || st.TopicsRows != 2 {
		t.Fatalf("Topics stats = (%d, %d), want (1, 2)", st.TopicsBatches, st.TopicsRows)
	}
	if st.InstitutionsBatches != 1 || st.InstitutionsRows != 3 {
		t.Fatalf("Institutions stats = (%d, %d), want (1, 3)", st.InstitutionsBatches, st.InstitutionsRows)
	}

	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()
	if err := s.WriteWorksBatch(canceledCtx, NewWorksBatch(1)); !errors.Is(err, context.Canceled) {
		t.Fatalf("WriteWorksBatch(canceled) = %v, want context.Canceled", err)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !s.Closed() {
		t.Fatal("Closed() = false after Close()")
	}
}
