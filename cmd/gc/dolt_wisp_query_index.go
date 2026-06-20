package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// wispQueryIndexStatements lists the CREATE INDEX statements applied to every
// managed Beads database by the wisp-query-index migration. Each entry is
// idempotent (IF NOT EXISTS).
//
// Background: the beads library's SearchIssuesWithCountsInTx generates SQL
// with full-table subquery aggregations for wisp_labels and wisp_dependencies
// (JSON_ARRAYAGG labels, dep/rdep/comment counts), all materialized across the
// entire table before the outer WHERE filter is applied. Two indexes are
// missing from the beads schema migrations that would significantly reduce the
// cost of these scans on a busy server:
//
//   - idx_wisp_labels_issue_id: without it, the GROUP BY issue_id scan in the
//     labels subquery does a full wisp_labels scan + sort on every bd query call.
//   - idx_wisps_status_type: composite covering the most common hot filter
//     (status='open' AND issue_type='message') so the outer WHERE can use a
//     single range scan instead of filtering two separate index rows.
//
// These belong upstream in the beads schema migrations; this gc-side guard
// applies them immediately without waiting for a beads version bump. Federated
// hooks and controller scans query the city database plus every rig database,
// so the indexes must exist on all of them — not only the canonical city DB.
var wispQueryIndexStatements = []string{
	"CREATE INDEX IF NOT EXISTS idx_wisp_labels_issue_id ON wisp_labels(issue_id)",
	"CREATE INDEX IF NOT EXISTS idx_wisps_status_type ON wisps(status, issue_type)",
}

// wispQueryIndexTables lists the tables the wisp query indexes target. A managed
// database must carry all of them to be treated as a Beads wisp store; databases
// missing them (system databases, non-Beads databases, fresh stores before
// `bd init`) are skipped cleanly by the migration.
var wispQueryIndexTables = []string{"wisp_labels", "wisps"}

// wispQueryIndexCommitStatements persist the schema change so the indexes
// survive reset/sync, matching the schemas/wisps-composite-index migration
// convention. "nothing to commit" on an idempotent re-run is treated as success.
var wispQueryIndexCommitStatements = []string{
	"CALL DOLT_ADD('.')",
	"CALL DOLT_COMMIT('-m', 'gc: add wisp-query performance indexes (gcy-0m1)', '--author', 'gascity-builder <builder@gascity.local>')",
}

const (
	// wispQueryIndexMigrationTimeout bounds the entire multi-database sweep so a
	// wedged Dolt server cannot keep the background goroutine alive forever. The
	// migration re-runs on the next controller start, so a cut sweep converges.
	wispQueryIndexMigrationTimeout = 5 * time.Minute
	// wispQueryIndexProbeTimeout bounds the cheap per-database probes
	// (enumeration, schema presence).
	wispQueryIndexProbeTimeout = 10 * time.Second
	// wispQueryIndexApplyTimeout bounds the per-database index creation and
	// commit. It is generous because CREATE INDEX on a large wisps table can run
	// longer than the cheap probes.
	wispQueryIndexApplyTimeout = 2 * time.Minute
)

// wispIndexStatus classifies the per-database outcome of the wisp-query-index
// migration.
type wispIndexStatus string

const (
	wispIndexApplied wispIndexStatus = "applied"
	wispIndexSkipped wispIndexStatus = "skipped"
	wispIndexFailed  wispIndexStatus = "error"
)

// wispIndexResult records the outcome of applying (or skipping) the wisp query
// indexes for one managed database.
type wispIndexResult struct {
	Database string
	Status   wispIndexStatus
	Err      error
}

// wispIndexClient is the minimal SQL surface the multi-database
// wisp-query-index migration needs against the managed Dolt server. Production
// is sqlWispIndexClient (wraps *sql.DB); tests inject a fake. This mirrors
// doltMaintenanceClient so the per-database iteration in
// applyWispQueryIndexesAcrossDatabases stays unit-testable without a live Dolt
// server.
type wispIndexClient interface {
	// ListUserDatabases returns the managed user databases (system databases
	// such as information_schema/mysql/dolt are excluded).
	ListUserDatabases(ctx context.Context) ([]string, error)
	// HasWispSchema reports whether the named database carries the Beads wisp
	// tables the indexes target.
	HasWispSchema(ctx context.Context, name string) (bool, error)
	// ApplyIndexes creates the wisp query indexes on the named database and
	// commits the schema change. It is idempotent.
	ApplyIndexes(ctx context.Context, name string) error
	// Close releases the underlying connection pool.
	Close() error
}

// applyWispQueryIndexesAcrossDatabases applies the wisp query indexes to every
// managed user database that carries the Beads wisp schema, returning one
// result per database examined. Databases without the wisp tables are skipped
// cleanly. A per-database error is recorded and the sweep continues so a single
// degraded database cannot block the rest; only a failure to enumerate
// databases (or a canceled context) aborts the sweep.
func applyWispQueryIndexesAcrossDatabases(ctx context.Context, client wispIndexClient) ([]wispIndexResult, error) {
	dbs, err := client.ListUserDatabases(ctx)
	if err != nil {
		return nil, fmt.Errorf("list managed databases: %w", err)
	}
	results := make([]wispIndexResult, 0, len(dbs))
	for _, name := range dbs {
		if err := ctx.Err(); err != nil {
			return results, err
		}
		has, err := client.HasWispSchema(ctx, name)
		if err != nil {
			results = append(results, wispIndexResult{Database: name, Status: wispIndexFailed, Err: err})
			continue
		}
		if !has {
			results = append(results, wispIndexResult{Database: name, Status: wispIndexSkipped})
			continue
		}
		if err := client.ApplyIndexes(ctx, name); err != nil {
			results = append(results, wispIndexResult{Database: name, Status: wispIndexFailed, Err: err})
			continue
		}
		results = append(results, wispIndexResult{Database: name, Status: wispIndexApplied})
	}
	return results, nil
}

// summarizeWispIndexResults renders a one-line applied/skipped/failed tally for
// the migration log.
func summarizeWispIndexResults(results []wispIndexResult) string {
	var applied, skipped, failed int
	for _, r := range results {
		switch r.Status {
		case wispIndexApplied:
			applied++
		case wispIndexSkipped:
			skipped++
		case wispIndexFailed:
			failed++
		}
	}
	return fmt.Sprintf("applied=%d skipped=%d failed=%d", applied, skipped, failed)
}

// applyWispQueryIndexes creates the missing wisp query performance indexes on
// every managed Beads database (city plus all rig databases). It is idempotent
// and fail-open: per-database errors are logged to stderr but never returned, so
// a degraded Dolt connection does not block the controller. The caller runs
// this on a background goroutine so a long first-time index build never stalls
// startup; the controller-lifetime context cancels it on shutdown.
func (cr *CityRuntime) applyWispQueryIndexes(ctx context.Context) {
	if !cityUsesManagedDoltBeadsLifecycle(cr.cityPath) {
		return
	}
	portFn := cr.managedDoltPort
	if portFn == nil {
		portFn = currentResolvableManagedDoltPort
	}
	port := portFn(cr.cityPath)
	if port == "" {
		return
	}

	ctx, cancel := context.WithTimeout(ctx, wispQueryIndexMigrationTimeout)
	defer cancel()

	client, err := newSQLWispIndexClient(port)
	if err != nil {
		fmt.Fprintf(cr.stderr, "%s: wisp-query-index migration: %v\n", cr.logPrefix, err) //nolint:errcheck // best-effort stderr
		return
	}
	defer client.Close() //nolint:errcheck

	results, err := applyWispQueryIndexesAcrossDatabases(ctx, client)
	if err != nil {
		fmt.Fprintf(cr.stderr, "%s: wisp-query-index migration: %v\n", cr.logPrefix, err) //nolint:errcheck // best-effort stderr
	}
	for _, r := range results {
		switch r.Status {
		case wispIndexApplied:
			fmt.Fprintf(cr.stderr, "%s: wisp-query-index: %s: applied\n", cr.logPrefix, r.Database) //nolint:errcheck // best-effort stderr
		case wispIndexFailed:
			fmt.Fprintf(cr.stderr, "%s: wisp-query-index: %s: %v\n", cr.logPrefix, r.Database, r.Err) //nolint:errcheck // best-effort stderr
		}
	}
	if len(results) > 0 {
		fmt.Fprintf(cr.stderr, "%s: wisp-query-index migration: %s\n", cr.logPrefix, summarizeWispIndexResults(results)) //nolint:errcheck // best-effort stderr
	}
}

// sqlWispIndexClient is the production wispIndexClient: it runs each operation
// against the managed Dolt server over a *sql.DB pool. Index creation can run
// longer than a fixed read deadline on a large table, so the pool leaves
// ReadTimeout unset and bounds every operation with a per-call context instead.
type sqlWispIndexClient struct {
	db *sql.DB
}

// newSQLWispIndexClient opens a connection pool to the managed Dolt server on
// the loopback interface at port. The pool is sequential (one connection): the
// migration visits databases one at a time.
func newSQLWispIndexClient(port string) (*sqlWispIndexClient, error) {
	cfg, err := managedDoltBaseConfig("127.0.0.1", port, "root")
	if err != nil {
		return nil, err
	}
	// Leave ReadTimeout unset so a legitimately slow CREATE INDEX is not severed
	// mid-build; per-operation contexts bound the work instead. WriteTimeout
	// guards a wedged socket on the send path.
	cfg.WriteTimeout = 5 * time.Second
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return &sqlWispIndexClient{db: db}, nil
}

// ListUserDatabases returns the managed user databases on the server.
func (c *sqlWispIndexClient) ListUserDatabases(ctx context.Context) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, wispQueryIndexProbeTimeout)
	defer cancel()
	conn, err := c.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close() //nolint:errcheck // best-effort; pool owns lifecycle
	return managedDoltSelectUserDatabasesFromConn(ctx, conn)
}

// HasWispSchema reports whether name carries every table in wispQueryIndexTables.
// It queries information_schema so it needs no USE and ignores extra tables.
func (c *sqlWispIndexClient) HasWispSchema(ctx context.Context, name string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, wispQueryIndexProbeTimeout)
	defer cancel()

	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(wispQueryIndexTables)), ",")
	//nolint:gosec // G201: placeholders are a fixed run of "?" — table names are bound parameters.
	query := fmt.Sprintf(
		"SELECT COUNT(DISTINCT table_name) FROM information_schema.tables WHERE table_schema = ? AND table_name IN (%s)",
		placeholders,
	)
	args := make([]any, 0, len(wispQueryIndexTables)+1)
	args = append(args, name)
	for _, table := range wispQueryIndexTables {
		args = append(args, table)
	}

	var count int
	if err := c.db.QueryRowContext(ctx, query, args...).Scan(&count); err != nil {
		return false, err
	}
	return count == len(wispQueryIndexTables), nil
}

// ApplyIndexes creates the wisp query indexes on name and commits the schema
// change. All statements run on a single connection that selects name first so
// the unqualified CREATE INDEX and the DOLT_COMMIT both target name.
func (c *sqlWispIndexClient) ApplyIndexes(ctx context.Context, name string) error {
	ctx, cancel := context.WithTimeout(ctx, wispQueryIndexApplyTimeout)
	defer cancel()

	conn, err := c.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close() //nolint:errcheck // best-effort; pool owns lifecycle
	if err := useManagedDoltDatabase(ctx, conn, name); err != nil {
		return err
	}
	for _, stmt := range wispQueryIndexStatements {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("%s: %w", stmt, err)
		}
	}
	for _, stmt := range wispQueryIndexCommitStatements {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			if strings.Contains(strings.ToLower(err.Error()), "nothing to commit") {
				break
			}
			return fmt.Errorf("%s: %w", stmt, err)
		}
	}
	return nil
}

// Close releases the underlying connection pool.
func (c *sqlWispIndexClient) Close() error {
	return c.db.Close()
}
