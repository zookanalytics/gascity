package queryindex

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// Execer runs a statement on a database connection. *sql.DB and *sql.Conn
// satisfy it.
type Execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// Clock is the time source for waits. Tests substitute a fake one.
type Clock interface {
	Now() time.Time
	// Sleep waits for d or until ctx ends, and returns ctx's error if it
	// ended.
	Sleep(ctx context.Context, d time.Duration) error
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

func (realClock) Sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// QuietOptions bound the wait for a table to stop changing.
type QuietOptions struct {
	// QuietFor is how long the table's hash must hold one value.
	QuietFor time.Duration
	// Poll is the interval between hash reads.
	Poll time.Duration
	// MaxWait bounds the whole wait.
	MaxWait time.Duration
}

// WaitQuiet reads hash every opts.Poll until one value has held for
// opts.QuietFor, and reports true then. It reports false once opts.MaxWait has
// passed without such a stretch, and returns the error of a failed read or of
// ctx ending.
func WaitQuiet(ctx context.Context, clock Clock, hash func(context.Context) (string, error), opts QuietOptions) (bool, error) {
	start := clock.Now()
	current, err := hash(ctx)
	if err != nil {
		return false, err
	}
	since := start
	for {
		now := clock.Now()
		if now.Sub(since) >= opts.QuietFor {
			return true, nil
		}
		if now.Sub(start) >= opts.MaxWait {
			return false, nil
		}
		if err := clock.Sleep(ctx, opts.Poll); err != nil {
			return false, err
		}
		next, err := hash(ctx)
		if err != nil {
			return false, err
		}
		if next != current {
			current = next
			since = clock.Now()
		}
	}
}

// TableHash returns Dolt's hash of table in the working set. It changes
// whenever a write to the table commits.
func TableHash(ctx context.Context, db Querier, table string) (string, error) {
	hashes, err := queryStrings(ctx, db, "SELECT DOLT_HASHOF_TABLE(?)", table)
	if err != nil {
		return "", fmt.Errorf("hashing table %s: %w", table, err)
	}
	if len(hashes) != 1 {
		return "", fmt.Errorf("hashing table %s: got %d rows, want 1", table, len(hashes))
	}
	return hashes[0], nil
}

// Create builds ix with one CREATE INDEX statement.
func Create(ctx context.Context, db Execer, ix Index) error {
	if _, err := db.ExecContext(ctx, ix.CreateStatement()); err != nil {
		return fmt.Errorf("creating %s: %w", ix.Name, err)
	}
	return nil
}

// commitAuthor is the identity on gascity's schema commits, the same one every
// DOLT_COMMIT gascity issues carries: Dolt aborts a commit with no committer
// identity, and a fresh host may have none configured.
const commitAuthor = "gascity-builder <builder@gascity.local>"

// CommitSchema records a table's index change in the database's Dolt history
// by staging the table and committing it. Dolt keeps no history for a table it
// ignores, such as wisps, so that commit finds nothing to record, which is not
// an error.
func CommitSchema(ctx context.Context, db Execer, table, message string) error {
	if _, err := db.ExecContext(ctx, "CALL DOLT_ADD("+sqlString(table)+")"); err != nil {
		return fmt.Errorf("staging %s: %w", table, err)
	}
	if _, err := db.ExecContext(ctx, "CALL DOLT_COMMIT('-m', "+sqlString(message)+", '--author', "+sqlString(commitAuthor)+")"); err != nil {
		if isNothingToCommit(err) {
			return nil
		}
		return fmt.Errorf("committing %s: %w", table, err)
	}
	return nil
}

func isNothingToCommit(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "nothing to commit")
}

// sqlString renders s as a single-quoted SQL string literal.
func sqlString(s string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `''`).Replace(s) + "'"
}

// SQLStore returns a Store that reads through db and runs each build and each
// self-test on a handle openDedicated opens for it and the Store closes
// afterwards. Dolt sends nothing back until a CREATE INDEX finishes, so the
// dedicated handle's read deadline must outlast a build.
func SQLStore(label string, db Querier, openDedicated func(context.Context) (*sql.DB, error)) Store {
	return Store{
		Label: label,
		Inspect: func(ctx context.Context, want []Index) (Catalog, error) {
			return Inspect(ctx, db, want)
		},
		TableHash: func(ctx context.Context, table string) (string, error) {
			return TableHash(ctx, db, table)
		},
		Build: func(ctx context.Context, ix Index) (err error) {
			dedicated, err := openDedicated(ctx)
			if err != nil {
				return fmt.Errorf("opening a connection: %w", err)
			}
			defer closeInto(dedicated, &err)
			if err := Create(ctx, dedicated, ix); err != nil {
				return err
			}
			return CommitSchema(ctx, dedicated, ix.Table, "gascity: add query index "+ix.Name+" on "+ix.String())
		},
		DoltVersion: func(ctx context.Context) (string, error) {
			return DoltVersion(ctx, db)
		},
		SelfTest: func(ctx context.Context) (result SelfTestResult, err error) {
			dedicated, err := openDedicated(ctx)
			if err != nil {
				return SelfTestResult{}, fmt.Errorf("opening a connection: %w", err)
			}
			defer closeInto(dedicated, &err)
			conn, err := dedicated.Conn(ctx)
			if err != nil {
				return SelfTestResult{}, fmt.Errorf("opening a connection: %w", err)
			}
			defer closeInto(conn, &err)
			return SelfTest(ctx, conn)
		},
	}
}

// closeInto closes c and, when *err is nil, reports a failed close there.
func closeInto(c interface{ Close() error }, err *error) {
	if cerr := c.Close(); cerr != nil && *err == nil {
		*err = fmt.Errorf("closing the connection: %w", cerr)
	}
}
