package pipeline

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	etlch "go-etl/internal/clickhouse"
)

// Slot serialises management transitions through one pinned physical
// connection. ClickHouse limits pipeline_writer to one session on the shared
// login, so the slot is the mutual exclusion between Go and Python, and between
// two Go transitions.
//
// Rules from the spec (step 1.2):
//   - open the session only for the transition and close it right after;
//   - while the slot is busy, wait with pauses but honour task cancellation;
//   - on a broken connection, take a new session and re-run the transition,
//     which re-reads the state;
//   - bad credentials and missing privileges fail immediately.
type Slot struct {
	db *sql.DB

	busyPause    time.Duration
	busyRetries  int
	brokenPause  time.Duration
	brokenRetry  int
}

// NewSlot wraps a database/sql handle whose pool is already limited to one
// connection (see clickhouse.OpenSQL).
func NewSlot(db *sql.DB) (*Slot, error) {
	if db == nil {
		return nil, errors.New("pipeline: nil management database")
	}
	return &Slot{
		db:          db,
		busyPause:   time.Second,
		busyRetries: 5,
		brokenPause: 200 * time.Millisecond,
		brokenRetry: 3,
	}, nil
}

func (s *Slot) Run(ctx context.Context, fn func(ctx context.Context, conn Conn) error) error {
	if fn == nil {
		return errors.New("pipeline: nil transition")
	}
	var busyWaits, brokenRetries int
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		conn, err := s.db.Conn(ctx)
		if err != nil {
			if etlch.KindOf(err) == etlch.KindSessionBusy {
				if busyWaits >= s.busyRetries {
					return fmt.Errorf("pipeline: management slot is busy: %w", err)
				}
				busyWaits++
				if werr := wait(ctx, s.busyPause); werr != nil {
					return werr
				}
				continue
			}
			return fmt.Errorf("pipeline: open management session: %w", err)
		}

		runErr := fn(ctx, conn)
		closeErr := conn.Close()
		if runErr == nil {
			if closeErr != nil {
				return fmt.Errorf("pipeline: close management session: %w", closeErr)
			}
			return nil
		}
		if ctx.Err() != nil {
			return runErr
		}
		if etlch.KindOf(runErr) == etlch.KindSessionBusy {
			if busyWaits >= s.busyRetries {
				return fmt.Errorf("pipeline: management slot is busy: %w", runErr)
			}
			busyWaits++
			if werr := wait(ctx, s.busyPause); werr != nil {
				return werr
			}
			continue
		}
		if isBrokenConnection(runErr) {
			if brokenRetries >= s.brokenRetry {
				return fmt.Errorf("pipeline: management session keeps breaking: %w", runErr)
			}
			brokenRetries++
			if werr := wait(ctx, s.brokenPause); werr != nil {
				return werr
			}
			continue
		}
		return runErr
	}
}

func wait(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// isBrokenConnection reports transport failures that are worth retrying on a
// fresh session. Query and privilege errors are not retried.
func isBrokenConnection(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, driver.ErrBadConn) || errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return true
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	lower := strings.ToLower(err.Error())
	for _, marker := range []string{
		"connection reset by peer",
		"broken pipe",
		"unexpected eof",
		"connection is closed",
		"server closed the connection",
		"failed to read",
	} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}
