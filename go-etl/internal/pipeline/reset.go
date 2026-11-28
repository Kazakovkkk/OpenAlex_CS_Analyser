package pipeline

import (
	"context"
	"fmt"
	"time"
)

// clearCheckpoints empties the ledger for a new load. It runs inside the
// management session: a crash after the clear but before the loading row only
// makes the next attempt reload files from the beginning.
func (s *Store) clearCheckpoints(ctx context.Context, conn Conn) error {
	if _, err := conn.ExecContext(ctx, "TRUNCATE TABLE "+s.table("etl_checkpoints")); err != nil {
		return fmt.Errorf("pipeline: clear checkpoints: %w", err)
	}
	return nil
}

type RestartParams struct {
	InstanceID   string
	JobID        string
	ManifestID   string
	SnapshotDate time.Time
}

// Restart performs a confirmed full restart exactly as the spec orders it:
//
//	loading/resetting with the new date and release hash,
//	TRUNCATE works, dim_topics, etl_checkpoints (in that order),
//	loading/full.
//
// All of it runs on one management session. If the process dies after any of
// the clears, the row stays loading/resetting and the next attempt repeats all
// three clears before moving to full.
func (s *Store) Restart(ctx context.Context, p RestartParams) (State, error) {
	var out State
	err := s.session.Run(ctx, func(ctx context.Context, conn Conn) error {
		cur, err := s.read(ctx, conn)
		if err != nil {
			return err
		}
		resuming := cur.LoadState == LoadLoading && cur.Phase == PhaseResetting &&
			cur.ManifestID == p.ManifestID

		if !resuming {
			if p.ManifestID == "" {
				return fmt.Errorf("pipeline: restart needs a release id")
			}
			snap := p.SnapshotDate.UTC()
			next := cur
			next.Revision = cur.Revision + 1
			next.LoadState = LoadLoading
			next.Phase = PhaseResetting
			next.ManifestID = p.ManifestID
			next.InstanceID = p.InstanceID
			next.JobID = p.JobID
			next.LoadingSnapshotDate = &snap
			if err := s.insert(ctx, conn, next); err != nil {
				return err
			}
			if cur, err = s.verify(ctx, conn, next); err != nil {
				return err
			}
		}

		for _, table := range []string{"works", "dim_topics", "etl_checkpoints"} {
			if _, err := conn.ExecContext(ctx, "TRUNCATE TABLE "+s.table(table)); err != nil {
				return fmt.Errorf("pipeline: truncate %s: %w", table, err)
			}
		}
		// pipeline_writer may read works and dim_topics, so verify the two data
		// tables explicitly. etl_checkpoints is write-only for this user; a
		// successful synchronous TRUNCATE is its confirmation.
		for _, table := range []string{"works", "dim_topics"} {
			if err := s.verifyEmpty(ctx, conn, table); err != nil {
				return err
			}
		}

		next := cur
		next.Revision = cur.Revision + 1
		next.LoadState = LoadLoading
		next.Phase = PhaseFull
		next.ManifestID = p.ManifestID
		next.InstanceID = p.InstanceID
		next.JobID = p.JobID
		if err := s.insert(ctx, conn, next); err != nil {
			return err
		}
		out, err = s.verify(ctx, conn, next)
		return err
	})
	return out, err
}

func (s *Store) verifyEmpty(ctx context.Context, conn Conn, table string) error {
	var n uint64
	if err := conn.QueryRowContext(ctx, "SELECT count() FROM "+s.table(table)).Scan(&n); err != nil {
		return fmt.Errorf("pipeline: verify %s is empty: %w", table, err)
	}
	if n != 0 {
		return fmt.Errorf("pipeline: %s still has %d row(s) after TRUNCATE", table, n)
	}
	return nil
}
