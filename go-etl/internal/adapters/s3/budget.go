package s3

import "sync/atomic"

// RowBudget is the job-level max_records budget shared by all workers, so the
// cap holds regardless of how many files are read at once.
type RowBudget struct {
	left atomic.Int64
}

func NewRowBudget(n int64) *RowBudget {
	b := &RowBudget{}
	if n > 0 {
		b.left.Store(n)
	}
	return b
}

// Take reserves one row; it reports false when the budget is exhausted.
func (b *RowBudget) Take() bool {
	for {
		cur := b.left.Load()
		if cur <= 0 {
			return false
		}
		if b.left.CompareAndSwap(cur, cur-1) {
			return true
		}
	}
}

func (b *RowBudget) Remaining() int64 { return b.left.Load() }

// Give returns a reservation that was not used (a filtered or duplicate row).
func (b *RowBudget) Give() { b.left.Add(1) }
