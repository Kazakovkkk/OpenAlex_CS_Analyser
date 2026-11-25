package s3

import (
	"fmt"
	"io"
	"os"
	"sort"
)

const (
	DefaultPrefetchBudget = 64 << 20

	DefaultPrefetchWorkers = 6

	minWindowSize = 16 << 10
	maxWindowSize = 8 << 20
)

var prefetchTrace = os.Getenv("S3_TRACE_PREFETCH") == "1"

type Span struct {
	Off int64
	Len int64
}

func (s Span) End() int64 { return s.Off + s.Len }

func (s Span) String() string { return fmt.Sprintf("[%d-%d)", s.Off, s.End()) }

type PrefetchGroup struct {
	Lo, Hi int64
	Spans  []Span

	TotalBytes int64
}

func (g *PrefetchGroup) spanFor(off int64) int {
	i := sort.Search(len(g.Spans), func(i int) bool { return g.Spans[i].End() > off })
	if i < len(g.Spans) && g.Spans[i].Off <= off {
		return i
	}
	return -1
}

type PrefetchPlan struct {
	Groups []PrefetchGroup

	Lo, Hi     int64
	TotalBytes int64
	MaxSpans   int
	MaxGroup   int64
}

func NewPrefetchPlan(groups []PrefetchGroup) (*PrefetchPlan, error) {
	if len(groups) == 0 {
		return nil, fmt.Errorf("prefetch plan: no groups")
	}
	p := &PrefetchPlan{Groups: make([]PrefetchGroup, 0, len(groups))}
	for gi := range groups {
		g := groups[gi]
		if len(g.Spans) == 0 {
			return nil, fmt.Errorf("prefetch plan: group %d has no spans", gi)
		}
		spans := append([]Span(nil), g.Spans...)
		sort.Slice(spans, func(a, b int) bool { return spans[a].Off < spans[b].Off })
		merged := spans[:1]
		for _, s := range spans[1:] {
			last := &merged[len(merged)-1]
			if s.Off < last.Off {
				return nil, fmt.Errorf("prefetch plan: overlapping spans %s and %s", last, s)
			}
			if s.Off <= last.End() {
				if s.End() > last.End() {
					last.Len = s.End() - last.Off
				}
				continue
			}
			merged = append(merged, s)
		}
		for _, s := range merged {
			if s.Off < 0 || s.Len <= 0 {
				return nil, fmt.Errorf("prefetch plan: invalid span %s", s)
			}
		}
		p.Groups = append(p.Groups, PrefetchGroup{
			Lo:         merged[0].Off,
			Hi:         merged[len(merged)-1].End(),
			Spans:      merged,
			TotalBytes: spansTotal(merged),
		})
	}
	sort.Slice(p.Groups, func(a, b int) bool { return p.Groups[a].Lo < p.Groups[b].Lo })
	for i := 1; i < len(p.Groups); i++ {
		if p.Groups[i].Lo < p.Groups[i-1].Hi {
			return nil, fmt.Errorf("prefetch plan: groups %d and %d overlap", i-1, i)
		}
	}
	p.Lo = p.Groups[0].Lo
	p.Hi = p.Groups[len(p.Groups)-1].Hi
	for i := range p.Groups {
		p.TotalBytes += p.Groups[i].TotalBytes
		p.MaxSpans = max(p.MaxSpans, len(p.Groups[i].Spans))
		p.MaxGroup = max(p.MaxGroup, p.Groups[i].TotalBytes)
	}
	return p, nil
}

func (p *PrefetchPlan) groupFor(off int64) int {
	i := sort.Search(len(p.Groups), func(i int) bool { return p.Groups[i].Hi > off })
	if i < len(p.Groups) && p.Groups[i].Lo <= off {
		return i
	}
	return -1
}

func spansTotal(spans []Span) int64 {
	var total int64
	for _, s := range spans {
		total += s.Len
	}
	return total
}

type spanWindow struct {
	span    Span
	buf     []byte
	dataOff int64
	pending chan struct{}
	err     error
}

func (w *spanWindow) end() int64 { return w.dataOff + int64(len(w.buf)) }

func (w *spanWindow) covers(off int64) bool { return off >= w.dataOff && off < w.end() }

func (w *spanWindow) complete() bool {
	return w.dataOff <= w.span.Off && w.end() >= w.span.End()
}

type spanFetch struct {
	idx   int
	start int64
	end   int64
}

func (r *RangeReaderAt) SetPrefetchPlan(plan *PrefetchPlan) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.plan = plan
	r.activeGroup = -1
	r.windows = nil
	if plan != nil && r.prefetchWorkers <= 0 {
		r.prefetchWorkers = DefaultPrefetchWorkers
	}
}

func (r *RangeReaderAt) sweepPendingLocked() {
	for i := range r.windows {
		w := &r.windows[i]
		if w.pending == nil {
			continue
		}
		select {
		case <-w.pending:
			w.pending = nil
		default:
		}
	}
}

func (r *RangeReaderAt) readPlanLocked(target []byte, off int64, eofAtEnd bool) (readOutcome, bool) {
	gi := r.plan.groupFor(off)
	if gi < 0 {
		return readOutcome{}, false
	}
	if gi != r.activeGroup {
		r.activateGroup(gi)
	}
	g := &r.plan.Groups[gi]
	si := g.spanFor(off)
	if si < 0 {
		return readOutcome{}, false
	}

	if prefetchTrace {
		fmt.Fprintf(os.Stderr, "[readPlan] off=%d want=%d group=%d span=%d [%s]\n",
			off, len(target), gi, si, g.Spans[si])
	}

	copied := 0
	cur := off
	for copied < len(target) {
		if cur >= g.Spans[si].End() {
			if si+1 < len(g.Spans) && cur >= g.Spans[si+1].Off {
				si++
				continue
			}
			return readDone(copied, fmt.Errorf("s3ReaderAt: read at %d is beyond chunk %s", cur, g.Spans[si])), true
		}

		w := &r.windows[si]
		if w.pending != nil {
			return readWait(w.pending), true
		}
		if w.err != nil {
			err := w.err
			w.err = nil
			return readDone(copied, err), true
		}
		if w.covers(cur) {
			rel := cur - w.dataOff
			take := min(int64(len(target)-copied), int64(len(w.buf))-rel)
			copy(target[copied:copied+int(take)], w.buf[rel:])
			copied += int(take)
			cur += take
			r.cacheHits.Add(1)
			continue
		}

		ready, err := r.fillFrontierLocked(gi, si, cur)
		if err != nil {
			return readDone(copied, err), true
		}
		if ready == nil {
			return readDone(copied, fmt.Errorf("s3ReaderAt: prefetch planned nothing for offset %d (chunk %s)", cur, g.Spans[si])), true
		}
		return readWait(ready), true
	}

	if eofAtEnd {
		return readDone(copied, io.EOF), true
	}
	return readDone(copied, nil), true
}

func (r *RangeReaderAt) activateGroup(gi int) {
	g := &r.plan.Groups[gi]
	n := len(g.Spans)
	r.activeGroup = gi

	if g.TotalBytes <= r.prefetchBudget || n == 0 {
		r.wholeGroup = true
		r.winSize = 0
	} else {
		r.wholeGroup = false
		r.winSize = min(max(r.prefetchBudget/int64(n), minWindowSize), maxWindowSize)
	}

	r.windows = make([]spanWindow, n)
	for i, sp := range g.Spans {
		r.windows[i] = spanWindow{span: sp, dataOff: sp.Off}
	}
	r.groupsLoaded.Add(1)
}

func (r *RangeReaderAt) windowSizeOf(sp Span) int64 {
	if r.wholeGroup {
		return sp.Len
	}
	return min(r.winSize, sp.Len)
}

func (r *RangeReaderAt) fillFrontierLocked(gi, target int, needOff int64) (chan struct{}, error) {
	g := &r.plan.Groups[gi]
	tsp := g.Spans[target]
	if needOff < tsp.Off {
		needOff = tsp.Off
	}
	if needOff >= tsp.End() {
		return nil, fmt.Errorf("s3ReaderAt: read at %d beyond chunk %s", needOff, tsp)
	}

	var frontier int64
	if r.wholeGroup {
		frontier = tsp.End()
	} else {
		k := (needOff - tsp.Off) / r.winSize
		frontier = min(tsp.Off+(k+1)*r.winSize, tsp.End())
	}

	fetches := make([]spanFetch, 0, len(g.Spans))
	for i := range g.Spans {
		sp := g.Spans[i]
		w := &r.windows[i]
		if w.pending != nil {
			continue
		}
		if w.complete() {
			continue
		}
		var from, to int64
		switch {
		case r.wholeGroup:
			from, to = sp.Off, sp.End()
		case i == target:
			k := (needOff - sp.Off) / r.winSize
			from = sp.Off + k*r.winSize
			to = min(from+r.winSize, sp.End())
		default:
			if w.end() >= frontier {
				continue
			}
			from = max(w.end(), sp.Off)
			to = min(from+r.winSize, sp.End())
		}
		if to <= from {
			continue
		}
		fetches = append(fetches, spanFetch{idx: i, start: from, end: to})
	}
	return r.launchFetchesLocked(fetches, target), nil
}

func (r *RangeReaderAt) launchFetchesLocked(fetches []spanFetch, targetIdx int) chan struct{} {
	if len(fetches) == 0 {
		return nil
	}
	r.prefetchBurst.Add(1)
	if prefetchTrace {
		for _, f := range fetches {
			fmt.Fprintf(os.Stderr, "[prefetch] burst=%d span=%d [%d-%d) len=%d\n",
				r.prefetchBurst.Load(), f.idx, f.start, f.end, f.end-f.start)
		}
	}

	var sem chan struct{}
	if len(fetches) > r.prefetchWorkers {
		sem = make(chan struct{}, r.prefetchWorkers)
	}
	var targetReady chan struct{}
	for _, f := range fetches {
		w := &r.windows[f.idx]
		size := f.end - f.start
		if int64(cap(w.buf)) < size {
			w.buf = make([]byte, size)
		}
		dst := w.buf[:size]
		ready := make(chan struct{})
		w.pending = ready
		w.err = nil
		if f.idx == targetIdx {
			targetReady = ready
		}
		go func(f spanFetch, w *spanWindow, dst []byte, ready chan struct{}) {
			if sem != nil {
				sem <- struct{}{}
				defer func() { <-sem }()
			}
			if _, err := r.fetchInto(f.start, dst); err != nil {
				w.err = err
			} else {
				w.dataOff = f.start
				w.buf = dst
				r.prefetchBytes.Add(uint64(len(dst)))
			}
			close(ready)
		}(f, w, dst, ready)
	}
	if targetReady == nil {
		closed := make(chan struct{})
		close(closed)
		return closed
	}
	return targetReady
}
