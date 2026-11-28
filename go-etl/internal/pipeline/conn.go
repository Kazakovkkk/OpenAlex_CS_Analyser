package pipeline

import (
	"context"
	"database/sql"
)

// Conn is the subset of database/sql that a transition needs. Both *sql.DB and
// *sql.Conn satisfy it; management transitions must always use a *sql.Conn so
// every statement runs on the same physical session.
type Conn interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// Session runs a whole management transition on one pinned physical
// connection. The callback must re-read the state it depends on every time it
// is invoked: Session may call it again after a broken connection.
type Session interface {
	Run(ctx context.Context, fn func(ctx context.Context, conn Conn) error) error
}
