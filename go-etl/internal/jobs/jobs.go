package jobs

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go-etl/internal/metrics"
	"go-etl/internal/pipeline"
)

type State string

const (
	StateRunning   State = "running"
	StateCompleted State = "completed"
	StateStopped   State = "stopped"
	StateFailed    State = "failed"
)

type Kind string

const (
	KindIngest     Kind = "ingest"
	KindDimensions Kind = "dimensions"
	KindLoadTest   Kind = "loadtest"
)

func (k Kind) prefix() string {
	switch k {
	case KindIngest:
		return "ing"
	case KindDimensions:
		return "dim"
	case KindLoadTest:
		return "lt"
	default:
		return "job"
	}
}

type Snapshot struct {
	JobID string `json:"job_id"`
	Kind  string `json:"kind"`
	State State  `json:"state"`

	ScannedRecords   uint64            `json:"scanned_records"`
	MatchedRecords   uint64            `json:"matched_records"`
	WrittenRecords   uint64            `json:"written_records"`
	DedupDropped     uint64            `json:"dedup_dropped"`
	RejectedRecords  uint64            `json:"rejected_records"`
	RejectedByReason map[string]uint64 `json:"rejected_by_reason,omitempty"`

	BytesRead    uint64 `json:"bytes_read"`
	HTTPRequests uint64 `json:"http_requests"`

	ElapsedSec float64 `json:"elapsed_sec"`
	CurrentRPS float64 `json:"current_rps"`

	AvgBatchMs float64 `json:"avg_batch_ms"`
	P50BatchMs float64 `json:"p50_batch_ms"`
	P95BatchMs float64 `json:"p95_batch_ms"`
	P99BatchMs float64 `json:"p99_batch_ms"`

	CurrentItem  string `json:"current_item"`
	Error        string `json:"error"`
	ErrorCode    string `json:"error_code,omitempty"`
	RestartToken string `json:"restart_token,omitempty"`
}

type Runner func(ctx context.Context, j *Job) error

type Job struct {
	id      string
	kind    Kind
	metrics *metrics.JobMetrics

	ctx    context.Context
	cancel context.CancelFunc

	done chan struct{}

	mu         sync.Mutex
	state      State
	err        error
	errCode    string
	startedAt  time.Time
	finishedAt time.Time
}

func (j *Job) ID() string { return j.id }

func (j *Job) Kind() Kind { return j.kind }

func (j *Job) Metrics() *metrics.JobMetrics { return j.metrics }

func (j *Job) Context() context.Context { return j.ctx }

func (j *Job) Done() <-chan struct{} { return j.done }

func (j *Job) State() State {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.state
}

func (j *Job) Err() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.err
}

func (j *Job) Stop() bool {
	j.mu.Lock()
	running := j.state == StateRunning
	j.mu.Unlock()
	if !running {
		return false
	}
	j.cancel()
	return true
}

func (j *Job) Wait(ctx context.Context) error {
	select {
	case <-j.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (j *Job) Snapshot() Snapshot {
	m := j.metrics.Snapshot()

	j.mu.Lock()
	state := j.state
	errText := ""
	if j.err != nil {
		errText = j.err.Error()
	}
	errCode := j.errCode
	var restartToken string
	var changed *pipeline.SnapshotChangedError
	if errors.As(j.err, &changed) {
		restartToken = changed.Token
	}
	started := j.startedAt
	finished := j.finishedAt
	j.mu.Unlock()

	elapsed := m.ElapsedSec
	if state == StateRunning && !started.IsZero() {
		elapsed = time.Since(started).Seconds()
	} else if !started.IsZero() && !finished.IsZero() {
		elapsed = finished.Sub(started).Seconds()
	}

	return Snapshot{
		JobID:            j.id,
		Kind:             string(j.kind),
		State:            state,
		ScannedRecords:   m.ScannedRecords,
		MatchedRecords:   m.MatchedRecords,
		WrittenRecords:   m.WrittenRecords,
		DedupDropped:     m.DedupDropped,
		RejectedRecords:  m.RejectedRecords,
		RejectedByReason: m.RejectedByReason,
		BytesRead:        m.BytesRead,
		HTTPRequests:     m.HTTPRequests,
		ElapsedSec:       elapsed,
		CurrentRPS:       m.CurrentRPS,
		AvgBatchMs:       m.AvgBatchMs,
		P50BatchMs:       m.P50BatchMs,
		P95BatchMs:       m.P95BatchMs,
		P99BatchMs:       m.P99BatchMs,
		CurrentItem:      m.CurrentItem,
		Error:            errText,
		ErrorCode:        errCode,
		RestartToken:     restartToken,
	}
}

type Manager struct {
	mu             sync.Mutex
	seq            map[Kind]int
	jobs           map[string]*Job
	MetricsFactory func() *metrics.JobMetrics
}

func NewManager() *Manager {
	return &Manager{seq: make(map[Kind]int, 3), jobs: make(map[string]*Job, 8)}
}

func (m *Manager) newMetrics() *metrics.JobMetrics {
	if m.MetricsFactory != nil {
		return m.MetricsFactory()
	}
	return metrics.NewJobMetrics()
}

var ErrJobsLimit = errors.New("jobs: too many running jobs")

const MaxRunningJobs = 4

func (m *Manager) Start(parent context.Context, kind Kind, run Runner) (*Job, error) {
	if run == nil {
		return nil, errors.New("jobs: nil runner")
	}
	m.mu.Lock()
	if m.runningLocked() >= MaxRunningJobs {
		m.mu.Unlock()
		return nil, fmt.Errorf("%w (limit %d)", ErrJobsLimit, MaxRunningJobs)
	}
	m.seq[kind]++
	id := fmt.Sprintf("%s-%d", kind.prefix(), m.seq[kind])
	ctx, cancel := context.WithCancel(parent)
	j := &Job{
		id:        id,
		kind:      kind,
		metrics:   m.newMetrics(),
		ctx:       ctx,
		cancel:    cancel,
		done:      make(chan struct{}),
		state:     StateRunning,
		startedAt: time.Now(),
	}
	m.jobs[id] = j
	m.mu.Unlock()

	go func() {
		defer close(j.done)
		defer cancel()

		err := run(ctx, j)
		j.metrics.Finish()

		j.mu.Lock()
		j.finishedAt = time.Now()
		switch {
		case err != nil && !errors.Is(err, context.Canceled):
			j.state = StateFailed
			j.err = err
			j.errCode = ErrorCode(err)
		case ctx.Err() != nil:
			j.state = StateStopped
		default:
			j.state = StateCompleted
		}
		j.mu.Unlock()
	}()
	return j, nil
}

func (m *Manager) runningLocked() int {
	n := 0
	for _, j := range m.jobs {
		if j.State() == StateRunning {
			n++
		}
	}
	return n
}

func (m *Manager) Get(id string) (*Job, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[id]
	return j, ok
}

func (m *Manager) RunningOfKind(kind Kind) *Job {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, j := range m.jobs {
		if j.kind == kind && j.State() == StateRunning {
			return j
		}
	}
	return nil
}

func (m *Manager) Snapshot(id string) (Snapshot, bool) {
	j, ok := m.Get(id)
	if !ok {
		return Snapshot{}, false
	}
	return j.Snapshot(), true
}

func (m *Manager) List() []Snapshot {
	m.mu.Lock()
	jobs := make([]*Job, 0, len(m.jobs))
	for _, j := range m.jobs {
		jobs = append(jobs, j)
	}
	m.mu.Unlock()

	sortJobs(jobs)
	out := make([]Snapshot, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, j.Snapshot())
	}
	return out
}

func (m *Manager) Stop(id string) bool {
	j, ok := m.Get(id)
	if !ok {
		return false
	}
	return j.Stop()
}

func (m *Manager) StopAll(ctx context.Context) error {
	m.mu.Lock()
	jobs := make([]*Job, 0, len(m.jobs))
	for _, j := range m.jobs {
		jobs = append(jobs, j)
	}
	m.mu.Unlock()

	for _, j := range jobs {
		j.Stop()
	}
	var firstErr error
	for _, j := range jobs {
		if err := j.Wait(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (m *Manager) Running() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.runningLocked()
}

func sortJobs(jobs []*Job) {
	for i := 1; i < len(jobs); i++ {
		for k := i; k > 0; k-- {
			a, b := jobs[k-1], jobs[k]

			aStarted := a.startedAt
			bStarted := b.startedAt
			if bStarted.Before(aStarted) || (bStarted.Equal(aStarted) && b.id < a.id) {
				jobs[k-1], jobs[k] = jobs[k], jobs[k-1]
				continue
			}
			break
		}
	}
}
