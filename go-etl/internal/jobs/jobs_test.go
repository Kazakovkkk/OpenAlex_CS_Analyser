package jobs

import (
	"context"
	"errors"
	"testing"
	"time"
)

func waitState(t *testing.T, j *Job, want State) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if j.State() == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("job %s state = %s, want %s", j.ID(), j.State(), want)
}

func TestStartCompletesAndCountsMetrics(t *testing.T) {
	t.Parallel()

	m := NewManager()
	j, err := m.Start(context.Background(), KindIngest, func(ctx context.Context, j *Job) error {
		j.Metrics().AddScanned(100)
		j.Metrics().AddWritten(10, 1024)
		j.Metrics().SetCurrentItem("part_0001.parquet")
		time.Sleep(2 * time.Millisecond)
		return nil
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if j.ID() != "ing-1" {
		t.Fatalf("id = %s, want ing-1", j.ID())
	}
	if err := j.Wait(context.Background()); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	waitState(t, j, StateCompleted)

	snap := j.Snapshot()
	if snap.State != StateCompleted || snap.ScannedRecords != 100 || snap.WrittenRecords != 10 {
		t.Fatalf("snapshot = %+v", snap)
	}
	if snap.JobID != "ing-1" || snap.Kind != string(KindIngest) {
		t.Fatalf("snapshot id/kind = %s/%s", snap.JobID, snap.Kind)
	}
	if snap.ElapsedSec <= 0 {
		t.Fatalf("elapsed = %f", snap.ElapsedSec)
	}
}

func TestStartFailed(t *testing.T) {
	t.Parallel()

	m := NewManager()
	boom := errors.New("boom")
	j, err := m.Start(context.Background(), KindIngest, func(ctx context.Context, j *Job) error {
		return boom
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	_ = j.Wait(context.Background())
	waitState(t, j, StateFailed)
	if !errors.Is(j.Err(), boom) {
		t.Fatalf("err = %v, want boom", j.Err())
	}
	if snap := j.Snapshot(); snap.Error == "" {
		t.Fatal("snapshot must contain the error text")
	}
}

func TestStopBecomesStopped(t *testing.T) {
	t.Parallel()

	m := NewManager()
	started := make(chan struct{})
	j, err := m.Start(context.Background(), KindLoadTest, func(ctx context.Context, j *Job) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	<-started
	if !m.Stop(j.ID()) {
		t.Fatal("Stop returned false for a running job")
	}
	_ = j.Wait(context.Background())
	waitState(t, j, StateStopped)
	if m.Stop(j.ID()) {
		t.Fatal("second Stop must return false")
	}
	if j.ID() != "lt-1" {
		t.Fatalf("id = %s, want lt-1", j.ID())
	}
}

func TestPrefixesPerKind(t *testing.T) {
	t.Parallel()

	m := NewManager()
	for _, tc := range []struct {
		kind Kind
		want string
	}{
		{KindIngest, "ing-1"},
		{KindDimensions, "dim-1"},
		{KindLoadTest, "lt-1"},
	} {
		j, err := m.Start(context.Background(), tc.kind, func(ctx context.Context, j *Job) error { return nil })
		if err != nil {
			t.Fatalf("Start(%s): %v", tc.kind, err)
		}
		if j.ID() != tc.want {
			t.Fatalf("kind %s id = %s, want %s", tc.kind, j.ID(), tc.want)
		}
		_ = j.Wait(context.Background())
	}
}

func TestGetListAndUnknown(t *testing.T) {
	t.Parallel()

	m := NewManager()
	j, err := m.Start(context.Background(), KindIngest, func(ctx context.Context, j *Job) error { return nil })
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	_ = j.Wait(context.Background())

	if got, ok := m.Get(j.ID()); !ok || got.ID() != j.ID() {
		t.Fatal("Get failed for an existing job")
	}
	if _, ok := m.Get("ing-999"); ok {
		t.Fatal("Get must fail for an unknown job")
	}
	if _, ok := m.Snapshot("ing-999"); ok {
		t.Fatal("Snapshot must fail for an unknown job")
	}
	if len(m.List()) != 1 {
		t.Fatalf("List = %d, want 1", len(m.List()))
	}
	if m.Stop("ing-999") {
		t.Fatal("Stop must fail for an unknown job")
	}
}

func TestJobsLimit(t *testing.T) {
	t.Parallel()

	m := NewManager()
	block := make(chan struct{})
	defer close(block)

	var started []*Job
	for i := range MaxRunningJobs {
		j, err := m.Start(context.Background(), KindIngest, func(ctx context.Context, j *Job) error {
			<-block
			return nil
		})
		if err != nil {
			t.Fatalf("start %d: %v", i, err)
		}
		started = append(started, j)
	}
	if _, err := m.Start(context.Background(), KindIngest, func(ctx context.Context, j *Job) error { return nil }); !errors.Is(err, ErrJobsLimit) {
		t.Fatalf("expected ErrJobsLimit, got %v", err)
	}
	if m.Running() != MaxRunningJobs {
		t.Fatalf("running = %d, want %d", m.Running(), MaxRunningJobs)
	}
}

func TestStopAllWaitsForRunners(t *testing.T) {
	t.Parallel()

	m := NewManager()
	exited := make(chan struct{}, 2)
	for range 2 {
		_, err := m.Start(context.Background(), KindIngest, func(ctx context.Context, j *Job) error {
			<-ctx.Done()
			exited <- struct{}{}
			return nil
		})
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := m.StopAll(ctx); err != nil {
		t.Fatalf("StopAll: %v", err)
	}
	if len(exited) != 2 {
		t.Fatalf("runners exited = %d, want 2", len(exited))
	}
	if m.Running() != 0 {
		t.Fatalf("running = %d, want 0", m.Running())
	}
}

func TestNilRunner(t *testing.T) {
	t.Parallel()
	m := NewManager()
	if _, err := m.Start(context.Background(), KindIngest, nil); err == nil {
		t.Fatal("expected an error for a nil runner")
	}
}
