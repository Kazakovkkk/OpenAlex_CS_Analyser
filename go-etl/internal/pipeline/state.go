package pipeline

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

const (
	LoadIdle    = "idle"
	LoadLoading = "loading"
)

var (
	ErrRevisionConflict = errors.New("pipeline: state revision changed under us")
	ErrLoadInProgress   = errors.New("pipeline: another load is in progress")
	ErrNotLoading       = errors.New("pipeline: publication without an active load")
)

type Conn interface {
	Exec(ctx context.Context, query string, args ...any) error
	Query(ctx context.Context, query string, args ...any) (driver.Rows, error)
	QueryRow(ctx context.Context, query string, args ...any) driver.Row
}

type State struct {
	Revision      uint64
	SnapshotDate  *time.Time
	DataUpdatedAt *time.Time
	DataVersion   uint64
	LoadState     string

	LoadingSnapshotDate *time.Time
	ManifestID          string
	InstanceID          string
	JobID               string

	AnalysisDataVersion  uint64
	AnalysisSnapshotDate *time.Time
	AnalysisCompletedAt  *time.Time
}

func (s State) Published() bool {
	return s.LoadState == LoadIdle && s.DataVersion > 0 && s.SnapshotDate != nil
}

func (s State) SnapshotYear() int {
	if s.SnapshotDate == nil {
		return 0
	}
	return s.SnapshotDate.Year()
}

type Store struct {
	conn     Conn
	database string
}

var identifierRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func NewStore(conn Conn, database string) (*Store, error) {
	if conn == nil {
		return nil, errors.New("pipeline: nil connection")
	}
	if !identifierRE.MatchString(database) {
		return nil, fmt.Errorf("pipeline: invalid database name %q", database)
	}
	return &Store{conn: conn, database: database}, nil
}

func (s *Store) table(name string) string {
	return "`" + s.database + "`.`" + name + "`"
}

// Database is the database this store manages.
func (s *Store) Database() string { return s.database }

const stateColumns = "state_revision, snapshot_date, data_updated_at, data_version, load_state, " +
	"loading_snapshot_date, loading_manifest_id, instance_id, job_id, " +
	"analysis_data_version, analysis_snapshot_date, analysis_completed_at"

const stateSelect = `state_revision,
	toUInt32(ifNull(toYYYYMMDD(snapshot_date), 0)),
	toInt64(ifNull(toUnixTimestamp64Micro(data_updated_at), 0)),
	data_version, load_state,
	toUInt32(ifNull(toYYYYMMDD(loading_snapshot_date), 0)),
	loading_manifest_id, instance_id, job_id,
	analysis_data_version,
	toUInt32(ifNull(toYYYYMMDD(analysis_snapshot_date), 0)),
	toInt64(ifNull(toUnixTimestamp64Micro(analysis_completed_at), 0))`

func (s *Store) Read(ctx context.Context) (State, error) {
	query := "SELECT " + stateSelect + " FROM " + s.table("pipeline_state") +
		" WHERE id = 1 ORDER BY state_revision DESC LIMIT 1"
	var (
		st                                   State
		snapYMD, loadingYMD, analysisSnapYMD uint32
		updMicros, analysisDoneMicros        int64
	)
	err := s.conn.QueryRow(ctx, query).Scan(
		&st.Revision, &snapYMD, &updMicros, &st.DataVersion, &st.LoadState,
		&loadingYMD, &st.ManifestID, &st.InstanceID, &st.JobID,
		&st.AnalysisDataVersion, &analysisSnapYMD, &analysisDoneMicros,
	)
	if err != nil {
		return State{}, fmt.Errorf("pipeline: read state: %w", err)
	}
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
}

func (s *Store) BeginLoad(ctx context.Context, p BeginParams) (State, error) {
	cur, err := s.Read(ctx)
	if err != nil {
		return State{}, err
	}
	if cur.LoadState == LoadLoading && cur.InstanceID != p.InstanceID {
		return cur, fmt.Errorf("%w (instance %q owns revision %d)", ErrLoadInProgress, cur.InstanceID, cur.Revision)
	}
	if err := s.ClearCheckpoints(ctx); err != nil {
		return cur, err
	}
	next := cur
	next.Revision = cur.Revision + 1
	next.LoadState = LoadLoading
	next.ManifestID = p.ManifestID
	next.InstanceID = p.InstanceID
	next.JobID = p.JobID
	snap := p.SnapshotDate.UTC()
	next.LoadingSnapshotDate = &snap
	if err := s.insert(ctx, next); err != nil {
		return cur, err
	}
	return s.verify(ctx, next)
}

type PublishParams struct {
	InstanceID   string
	JobID        string
	ManifestID   string
	SnapshotDate time.Time

	DataChanged bool
	Now         time.Time
}

func (s *Store) Publish(ctx context.Context, p PublishParams) (State, error) {
	cur, err := s.Read(ctx)
	if err != nil {
		return State{}, err
	}
	if cur.LoadState != LoadLoading {
		return cur, fmt.Errorf("%w (state=%s)", ErrNotLoading, cur.LoadState)
	}
	if cur.InstanceID != p.InstanceID || cur.JobID != p.JobID {
		// Somebody else owns the load now: our transition lost the revision.
		return cur, fmt.Errorf("%w (revision %d taken by %s/%s)",
			ErrRevisionConflict, cur.Revision, cur.InstanceID, cur.JobID)
	}
	next := cur
	next.Revision = cur.Revision + 1
	next.LoadState = LoadIdle
	next.ManifestID = p.ManifestID
	if p.DataChanged {
		snap := p.SnapshotDate.UTC()
		next.SnapshotDate = &snap
		next.DataVersion = cur.DataVersion + 1
		now := p.Now.UTC()
		next.DataUpdatedAt = &now
	}
	if err := s.insert(ctx, next); err != nil {
		return cur, err
	}
	return s.verify(ctx, next)
}

func (s *Store) verify(ctx context.Context, want State) (State, error) {
	got, err := s.Read(ctx)
	if err != nil {
		return State{}, err
	}
	if got.Revision != want.Revision || got.InstanceID != want.InstanceID || got.JobID != want.JobID {
		return got, fmt.Errorf("%w (wanted revision %d by %s/%s, found %d by %s/%s)",
			ErrRevisionConflict, want.Revision, want.InstanceID, want.JobID,
			got.Revision, got.InstanceID, got.JobID)
	}
	return got, nil
}

func (s *Store) insert(ctx context.Context, st State) error {
	query := "INSERT INTO " + s.table("pipeline_state") +
		" (id, state_revision, snapshot_date, data_updated_at, data_version, load_state," +
		" loading_snapshot_date, loading_manifest_id, instance_id, job_id," +
		" analysis_data_version, analysis_snapshot_date, analysis_completed_at)" +
		" VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	err := s.conn.Exec(ctx, query,
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
