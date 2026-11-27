package pipeline

import (
	"context"
	"fmt"
	"time"
)

func (s *Store) ClearCheckpoints(ctx context.Context) error {
	if err := s.conn.Exec(ctx, "TRUNCATE TABLE "+s.table("etl_checkpoints")); err != nil {
		return fmt.Errorf("pipeline: clear checkpoints: %w", err)
	}
	return nil
}

// ResetCorpus drops the data and the ledger before a confirmed restart. The
// published version is left alone, so analytics keeps reading the old release
// until the reload publishes again.
func (s *Store) ResetCorpus(ctx context.Context) error {
	for _, name := range []string{"works", "dim_topics", "etl_checkpoints"} {
		if err := s.conn.Exec(ctx, "TRUNCATE TABLE "+s.table(name)); err != nil {
			return fmt.Errorf("pipeline: truncate %s: %w", name, err)
		}
	}
	return nil
}

type CheckpointStore struct {
	conn     Conn
	database string
	now      func() time.Time
}

func NewCheckpointStore(conn Conn, database string) (*CheckpointStore, error) {
	if conn == nil {
		return nil, fmt.Errorf("pipeline: nil connection for checkpoints")
	}
	if !identifierRE.MatchString(database) {
		return nil, fmt.Errorf("pipeline: invalid database name %q", database)
	}
	return &CheckpointStore{conn: conn, database: database, now: time.Now}, nil
}

func (c *CheckpointStore) table() string {
	return "`" + c.database + "`.`etl_checkpoints`"
}

func (c *CheckpointStore) Load(ctx context.Context, entity string) (map[string]struct{}, error) {
	rows, err := c.conn.Query(ctx, "SELECT object_key FROM "+c.table()+" FINAL WHERE entity = ?", entity)
	if err != nil {
		return nil, fmt.Errorf("pipeline: read checkpoints: %w", err)
	}
	defer rows.Close()

	done := make(map[string]struct{}, 256)
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, fmt.Errorf("pipeline: scan checkpoint row: %w", err)
		}
		done[key] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pipeline: iterate checkpoints: %w", err)
	}
	return done, nil
}

func (c *CheckpointStore) MarkDone(ctx context.Context, entity, objectKey string) error {
	if entity == "" || objectKey == "" {
		return fmt.Errorf("pipeline: checkpoint needs entity and object key")
	}
	query := "INSERT INTO " + c.table() + " (entity, object_key, completed_at) VALUES (?, ?, ?)"
	if err := c.conn.Exec(ctx, query, entity, objectKey, c.now().UTC()); err != nil {
		return fmt.Errorf("pipeline: mark checkpoint %s/%s: %w", entity, objectKey, err)
	}
	return nil
}

func (c *CheckpointStore) Count(ctx context.Context, entity string) (uint64, error) {
	var n uint64
	err := c.conn.QueryRow(ctx,
		"SELECT count() FROM "+c.table()+" FINAL WHERE entity = ?", entity).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("pipeline: count checkpoints: %w", err)
	}
	return n, nil
}
