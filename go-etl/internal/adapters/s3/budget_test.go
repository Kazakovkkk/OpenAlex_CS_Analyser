package s3

import (
	"sync"
	"testing"
)

func TestRowBudgetTake(t *testing.T) {
	t.Parallel()
	b := NewRowBudget(3)
	if b.Remaining() != 3 {
		t.Fatalf("Remaining = %d, want 3", b.Remaining())
	}
	for i := range 3 {
		if !b.Take() {
			t.Fatalf("Take %d = false", i)
		}
	}
	if b.Take() {
		t.Fatal("Take beyond the budget must be false")
	}
	b.Give()
	if !b.Take() {
		t.Fatal("Give must return a reservation")
	}
	if b.Remaining() != 0 {
		t.Fatalf("Remaining = %d, want 0", b.Remaining())
	}
}

func TestRowBudgetConcurrentNeverOvershoots(t *testing.T) {
	t.Parallel()
	b := NewRowBudget(100)
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		granted int
	)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				if !b.Take() {
					return
				}
				mu.Lock()
				granted++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if granted != 100 {
		t.Fatalf("granted = %d, want exactly 100", granted)
	}
	if b.Remaining() != 0 {
		t.Fatalf("Remaining = %d, want 0", b.Remaining())
	}
}
