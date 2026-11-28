package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

const (
	LoadIdle    = "idle"
	LoadLoading = "loading"
)

// LoadPhase is the load_phase column contract: Enum8('none','resetting','full',
// 'incremental') with DEFAULT 'none'.
type LoadPhase string

const (
	PhaseNone        LoadPhase = "none"
	PhaseResetting   LoadPhase = "resetting"
	PhaseFull        LoadPhase = "full"
	PhaseIncremental LoadPhase = "incremental"
)

// ParsePhase normalises a value read from ClickHouse. Unknown values are
// treated as "none", which is the safe published state.
func ParsePhase(raw string) LoadPhase {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case string(PhaseResetting):
		return PhaseResetting
	case string(PhaseFull):
		return PhaseFull
	case string(PhaseIncremental):
		return PhaseIncremental
	default:
		return PhaseNone
	}
}

func (p LoadPhase) Valid() bool {
	switch p {
	case PhaseNone, PhaseResetting, PhaseFull, PhaseIncremental:
		return true
	}
	return false
}

var (
	ErrRevisionConflict = errors.New("pipeline: state revision changed under us")
	ErrLoadInProgress   = errors.New("pipeline: another load is in progress")
	ErrNotLoading       = errors.New("pipeline: publication without an active load")
	// ErrCorpusIncomplete means part of the release could not be loaded, so no
	// version may be published.
	ErrCorpusIncomplete = errors.New("pipeline: corpus is incomplete")
)

type State struct {
	Revision      uint64
	SnapshotDate  *time.Time
	DataUpdatedAt *time.Time
	DataVersion   uint64
	LoadState     string
	Phase         LoadPhase

	LoadingSnapshotDate *time.Time
	ManifestID          string
	InstanceID          string
	JobID               string

	AnalysisDataVersion  uint64
	AnalysisSnapshotDate *time.Time
	AnalysisCompletedAt  *time.Time
}

func (s State) Published() bool {
	return s.LoadState == LoadIdle && s.Phase == PhaseNone &&
		s.DataVersion > 0 && s.SnapshotDate != nil
}

func (s State) Loading() bool { return s.LoadState == LoadLoading }

func (s State) SnapshotYear() int {
	if s.SnapshotDate == nil {
		return 0
	}
	return s.SnapshotDate.Year()
}

// Store owns the pipeline_state transitions. All public methods run on one
// pinned management session (see Slot); the callback is safe to re-run.
type Store struct {
	session  Session
	database string
}

var identifierRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func NewStore(session Session, database string) (*Store, error) {
	if session == nil {
		return nil, errors.New("pipeline: nil management session")
	}
	if !identifierRE.MatchString(database) {
		return nil, fmt.Errorf("pipeline: invalid database name %q", database)
	}
	return &Store{session: session, database: database}, nil
}

func (s *Store) table(name string) string {
	return "`" + s.database + "`.`" + name + "`"
}

// Database is the database this store manages.
func (s *Store) Database() string { return s.database }

const stateColumns = "state_revision, snapshot_date, data_updated_at, data_version, load_state, " +
	"loading_snapshot_date, loading_manifest_id, instance_id, job_id, " +
	"analysis_data_version, analysis_snapshot_date, analysis_completed_at, load_phase"

const stateSelect = `state_revision,
	toUInt32(ifNull(toYYYYMMDD(snapshot_date), 0)),
	toInt64(ifNull(toUnixTimestamp64Micro(data_updated_at), 0)),
	data_version, load_state,
	toUInt32(ifNull(toYYYYMMDD(loading_snapshot_date), 0)),
	loading_manifest_id, instance_id, job_id,
	analysis_data_version,
	toUInt32(ifNull(toYYYYMMDD(analysis_snapshot_date), 0)),
	toInt64(ifNull(toUnixTimestamp64Micro(analysis_completed_at), 0)),
	load_phase`

func (s *Store) Read(ctx context.Context) (State, error) {
	var out State
	err := s.session.Run(ctx, func(ctx context.Context, conn Conn) error {
		st, err := s.read(ctx, conn)
		out = st
		return err
	})
	return out, err
}

// LastIdle reads the newest successfully published row from the revision
// history. A resume of a dictionary refresh needs the release id of that row.
func (s *Store) LastIdle(ctx context.Context) (State, error) {
	var out State
	err := s.session.Run(ctx, func(ctx context.Context, conn Conn) error {
		st, err := s.readWith(ctx, conn, " AND load_state = 'idle'")
		out = st
		return err
	})
	return out, err
}

func (s *Store) read(ctx context.Context, conn Conn) (State, error) {
	return s.readWith(ctx, conn, "")
}

func (s *Store) readWith(ctx context.Context, conn Conn, extra string) (State, error) {
	query := "SELECT " + stateSelect + " FROM " + s.table("pipeline_state") +
		" WHERE id = 1" + extra + " ORDER BY state_revision DESC LIMIT 1"

	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return State{}, fmt.Errorf("pipeline: read state: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var (
		st                                   State
		snapYMD, loadingYMD, analysisSnapYMD uint32
		updMicros, analysisDoneMicros        int64
		phase                                string
	)
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return State{}, fmt.Errorf("pipeline: read state: %w", err)
		}
		return State{}, sql.ErrNoRows
	}
	if err := rows.Scan(
		&st.Revision, &snapYMD, &updMicros, &st.DataVersion, &st.LoadState,
		&loadingYMD, &st.ManifestID, &st.InstanceID, &st.JobID,
		&st.AnalysisDataVersion, &analysisSnapYMD, &analysisDoneMicros, &phase,
	); err != nil {
		return State{}, fmt.Errorf("pipeline: scan state: %w", err)
	}
	if err := rows.Err(); err != nil {
		return State{}, fmt.Errorf("pipeline: read state: %w", err)
	}
	st.Phase = ParsePhase(phase)
	st.SnapshotDate = dateFromYMD(snapYMD)
	st.DataUpdatedAt = unixMicros(updMicros)
	st.LoadingSnapshotDate = dateFromYMD(loadingYMD)
	st.AnalysisSnapshotDate = dateFromYMD(analysisSnapYMD)
	st.AnalysisCompletedAt = unixMicros(analysisDoneMicros)
	return st, nil
}

type BeginParams struct {
	InstanceID   string
	JobID        string
	ManifestID   string
	SnapshotDate time.Time
	Phase        LoadPhase
}

// BeginLoad starts a new load or records a new attempt of the unfinished one.
// The management slot guarantees that only one process is inside.
func (s *Store) BeginLoad(ctx context.Context, p BeginParams) (State, error) {
	var out State
	err := s.session.Run(ctx, func(ctx context.Context, conn Conn) error {
		cur, err := s.read(ctx, conn)
		if err != nil {
			return err
		}
		resuming := cur.LoadState == LoadLoading && p.ManifestID != "" && cur.ManifestID == p.ManifestID
		if cur.LoadState == LoadLoading && !resuming {
			return fmt.Errorf("%w (another release %q is being loaded)", ErrLoadInProgress, cur.ManifestID)
		}
		if !resuming {
			if err := s.clearCheckpoints(ctx, conn); err != nil {
				return err
			}
			if !p.Phase.Valid() || p.Phase == PhaseNone || p.Phase == PhaseResetting {
				p.Phase = PhaseFull
			}
		} else {
			// Same release: keep its phase and pinned date, only the attempt,
			// instance and job change.
			p.Phase = cur.Phase
			if p.SnapshotDate.IsZero() && cur.LoadingSnapshotDate != nil {
				p.SnapshotDate = *cur.LoadingSnapshotDate
			}
		}

		next := cur
		next.Revision = cur.Revision + 1
		next.LoadState = LoadLoading
		next.Phase = p.Phase
		next.ManifestID = p.ManifestID
		next.InstanceID = p.InstanceID
		next.JobID = p.JobID
		snap := p.SnapshotDate.UTC()
		next.LoadingSnapshotDate = &snap
		if err := s.insert(ctx, conn, next); err != nil {
			return err
		}
		out, err = s.verify(ctx, conn, next)
		return err
	})
	return out, err
}

type PublishParams struct {
	InstanceID   string
	JobID        string
	ManifestID   string
	SnapshotDate time.Time

	// Bump is true when the plan verified a new release, even if no rows were
	// inserted: every new verified snapshot raises data_version by exactly one.
	Bump bool
	Now  time.Time
}

// Publish records idle/none. It is idempotent for a lost acknowledgement: when
// the expected transition is already recorded for the same release and owner,
// it reports success instead of writing again.
func (s *Store) Publish(ctx context.Context, p PublishParams) (State, error) {
	var out State
	err := s.session.Run(ctx, func(ctx context.Context, conn Conn) error {
		cur, err := s.read(ctx, conn)
		if err != nil {
			return err
		}
		if s.publishedByUs(cur, p) {
			out = cur
			return nil
		}
		if cur.LoadState != LoadLoading {
			return fmt.Errorf("%w (state=%s phase=%s)", ErrNotLoading, cur.LoadState, cur.Phase)
		}
		if cur.ManifestID != p.ManifestID || cur.InstanceID != p.InstanceID || cur.JobID != p.JobID {
			return fmt.Errorf("%w (revision %d owned by %s/%s release %q)",
				ErrRevisionConflict, cur.Revision, cur.InstanceID, cur.JobID, cur.ManifestID)
		}

		next := cur
		next.Revision = cur.Revision + 1
		next.LoadState = LoadIdle
		next.Phase = PhaseNone
		if p.Bump {
			if p.SnapshotDate.IsZero() {
				return errors.New("pipeline: publication needs a snapshot date")
			}
			snap := p.SnapshotDate.UTC()
			next.SnapshotDate = &snap
			next.DataVersion = cur.DataVersion + 1
			now := p.Now.UTC()
			if now.IsZero() {
				now = time.Now().UTC()
			}
			next.DataUpdatedAt = &now
		}
		if err := s.insert(ctx, conn, next); err != nil {
			return err
		}
		out, err = s.verify(ctx, conn, next)
		return err
	})
	return out, err
}

func (s *Store) publishedByUs(cur State, p PublishParams) bool {
	return cur.LoadState == LoadIdle && cur.Phase == PhaseNone &&
		cur.ManifestID == p.ManifestID &&
		cur.InstanceID == p.InstanceID &&
		cur.JobID == p.JobID
}

func (s *Store) verify(ctx context.Context, conn Conn, want State) (State, error) {
	got, err := s.read(ctx, conn)
	if err != nil {
		return State{}, err
	}
	if got.Revision != want.Revision || got.InstanceID != want.InstanceID || got.JobID != want.JobID {
		return got, fmt.Errorf("%w (wanted revision %d by %s/%s, found %d by %s/%s)",
			ErrRevisionConflict, want.Revision, want.InstanceID, want.JobID,
			got.Revision, got.InstanceID, got.JobID)
	}
	if got.LoadState != want.LoadState || got.Phase != want.Phase || got.ManifestID != want.ManifestID {
		return got, fmt.Errorf("%w (wanted %s/%s release %q, found %s/%s release %q)",
			ErrRevisionConflict, want.LoadState, want.Phase, want.ManifestID,
			got.LoadState, got.Phase, got.ManifestID)
	}
	return got, nil
}

func (s *Store) insert(ctx context.Context, conn Conn, st State) error {
	query := "INSERT INTO " + s.table("pipeline_state") +
		" (id, state_revision, snapshot_date, data_updated_at, data_version, load_state," +
		" loading_snapshot_date, loading_manifest_id, instance_id, job_id," +
		" analysis_data_version, analysis_snapshot_date, analysis_completed_at, load_phase)" +
		" VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	_, err := conn.ExecContext(ctx, query,
		st.Revision,
		nullableTime(st.SnapshotDate),
		nullableTime(st.DataUpdatedAt),
		st.DataVersion,
		st.LoadState,
		nullableTime(st.LoadingSnapshotDate),
		st.ManifestID,
		st.InstanceID,
		st.JobID,
		st.AnalysisDataVersion,
		nullableTime(st.AnalysisSnapshotDate),
		nullableTime(st.AnalysisCompletedAt),
		string(st.Phase),
	)
	if err != nil {
		return fmt.Errorf("pipeline: insert state revision %d: %w", st.Revision, err)
	}
	return nil
}

func nullableTime(t *time.Time) any {
	if t == nil {
		return (*time.Time)(nil)
	}
	return *t
}

func dateFromYMD(ymd uint32) *time.Time {
	if ymd == 0 {
		return nil
	}
	y, m, d := int(ymd/10000), int(ymd/100%100), int(ymd%100)
	if y < 1 || m < 1 || m > 12 || d < 1 || d > 31 {
		return nil
	}
	t := time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.UTC)
	return &t
}

func unixMicros(us int64) *time.Time {
	if us <= 0 {
		return nil
	}
	t := time.UnixMicro(us).UTC()
	return &t
}

func (s State) ManifestMatches(manifestID string) bool {
	return s.Published() && strings.TrimSpace(manifestID) != "" && s.ManifestID == manifestID
}
