package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// doltMaintenanceClient is the minimal SQL surface the multi-database
// store-maintenance ops need against the managed Dolt server. Production is
// sqlDoltMaintenanceClient (wraps *sql.DB); tests inject a fake. This mirrors
// CleanupDoltClient so the per-database iteration in managedDoltMaintenanceOps
// stays unit-testable without a live Dolt server.
type doltMaintenanceClient interface {
	// ListUserDatabases returns the managed user databases (system databases
	// such as information_schema/mysql/dolt are excluded).
	ListUserDatabases(ctx context.Context) ([]string, error)
	// GCDatabase runs CALL DOLT_GC() against the named database.
	GCDatabase(ctx context.Context, name string) error
	// CountIssues returns SELECT COUNT(*) FROM issues for the named database —
	// the post-gc readability smoke test.
	CountIssues(ctx context.Context, name string) (int, error)
	// Close releases the underlying connection pool.
	Close() error
}

// managedDoltMaintenanceOps implements supervisor.DoltOps for a managed Dolt
// server that hosts multiple databases (the fork's gc/tk/sl/lx/su beads
// ledgers). CALL DOLT_GC() compacts only the session's selected database, so
// reclaiming disk across the whole server requires selecting and compacting
// each managed database in turn — a single call would leave every database
// but the default at its peak on-disk size.
type managedDoltMaintenanceOps struct {
	client doltMaintenanceClient
}

// newManagedDoltMaintenanceOps opens a maintenance-tuned connection to the
// managed Dolt server and wraps it for the store-maintenance loop.
func newManagedDoltMaintenanceOps(host, port, user string) (*managedDoltMaintenanceOps, error) {
	db, err := openManagedDoltMaintenanceDB(host, port, user)
	if err != nil {
		return nil, err
	}
	return &managedDoltMaintenanceOps{client: &sqlDoltMaintenanceClient{db: db}}, nil
}

// ExecGC runs CALL DOLT_GC() against every managed user database. It fails
// fast, naming the database, so the maintenance loop can classify and alert on
// the specific database that could not be compacted; databases compacted
// before the failure keep their reclaimed space.
func (o *managedDoltMaintenanceOps) ExecGC(ctx context.Context) error {
	dbs, err := o.client.ListUserDatabases(ctx)
	if err != nil {
		return fmt.Errorf("list managed databases: %w", err)
	}
	if len(dbs) == 0 {
		return errors.New("no managed user databases found for DOLT_GC")
	}
	for _, name := range dbs {
		if err := o.client.GCDatabase(ctx, name); err != nil {
			return fmt.Errorf("DOLT_GC on database %q: %w", name, err)
		}
	}
	return nil
}

// SmokeCount sums SELECT COUNT(*) FROM issues across every managed user
// database. A per-database query error (e.g. a table the gc corrupted)
// propagates so the loop records a smoke-test failure; the supervisor treats a
// zero total as unhealthy. Summing tolerates an individual database that
// legitimately holds zero issues as long as some database is populated.
func (o *managedDoltMaintenanceOps) SmokeCount(ctx context.Context) (int, error) {
	dbs, err := o.client.ListUserDatabases(ctx)
	if err != nil {
		return 0, fmt.Errorf("list managed databases: %w", err)
	}
	total := 0
	for _, name := range dbs {
		n, err := o.client.CountIssues(ctx, name)
		if err != nil {
			return 0, fmt.Errorf("smoke count on database %q: %w", name, err)
		}
		total += n
	}
	return total, nil
}

// Close releases the underlying connection pool.
func (o *managedDoltMaintenanceOps) Close() error {
	return o.client.Close()
}

// sqlDoltMaintenanceClient is the production doltMaintenanceClient: it runs each
// operation against the managed Dolt server over a *sql.DB pool. Each
// database-scoped call takes a fresh connection, selects the database with USE,
// then runs the statement — CALL DOLT_GC() may reset the session, so
// connections are never reused across databases.
type sqlDoltMaintenanceClient struct {
	db *sql.DB
}

func (c *sqlDoltMaintenanceClient) ListUserDatabases(ctx context.Context) ([]string, error) {
	conn, err := c.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close() //nolint:errcheck // best-effort; pool owns lifecycle
	return managedDoltSelectUserDatabasesFromConn(ctx, conn)
}

func (c *sqlDoltMaintenanceClient) GCDatabase(ctx context.Context, name string) error {
	conn, err := c.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close() //nolint:errcheck // best-effort; pool owns lifecycle
	if err := useManagedDoltDatabase(ctx, conn, name); err != nil {
		return err
	}
	_, err = conn.ExecContext(ctx, "CALL DOLT_GC()")
	return err
}

func (c *sqlDoltMaintenanceClient) CountIssues(ctx context.Context, name string) (int, error) {
	conn, err := c.db.Conn(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Close() //nolint:errcheck // best-effort; pool owns lifecycle
	if err := useManagedDoltDatabase(ctx, conn, name); err != nil {
		return 0, err
	}
	var n int
	// The bd schema names the work-unit table "issues" in every managed
	// ledger; internal/supervisor.maintenanceSmokeTable is the same literal.
	if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM `issues`").Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

func (c *sqlDoltMaintenanceClient) Close() error {
	return c.db.Close()
}

// useManagedDoltDatabase selects name on conn, backtick-escaping the
// identifier. The names come from the server's own SHOW DATABASES catalog, but
// escaping matches the managed-Dolt convention (see PurgeDroppedDatabases).
func useManagedDoltDatabase(ctx context.Context, conn *sql.Conn, name string) error {
	safe := strings.ReplaceAll(name, "`", "``")
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("USE `%s`", safe)); err != nil { //nolint:gosec // G201: identifier-escaped
		return fmt.Errorf("USE %q: %w", name, err)
	}
	return nil
}
