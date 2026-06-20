package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/doctor"
)

// wispOrphanTables are the wisp auxiliary tables owned by a wisp through their
// issue_id column. The beads schema (migration 0021_create_wisp_auxiliary)
// gives wisp_dependencies a FOREIGN KEY ... ON DELETE CASCADE back to wisps,
// but wisp_labels, wisp_events, and wisp_comments carry NO foreign key. A bare
// `DELETE FROM wisps` therefore leaves their rows behind as orphans — the leak
// that saturates a busy Dolt server (≈1M orphaned wisp_events rows observed on
// a live city). wisp_dependencies is included for completeness and defense in
// depth: if FK enforcement ever lapses on the dolt_ignore'd working-set tables,
// source-orphaned dependency rows are cleaned too.
//
// All four tables are keyed on the owning issue_id only. We never key cleanup
// on wisp_dependencies' target columns (depends_on_issue_id / depends_on_wisp_id
// / depends_on_external): a dependency's target can legitimately be an issue or
// an external reference rather than a wisp, so a target absent from `wisps` is
// not proof the row is orphaned. Only the owning issue_id absent from `wisps`
// proves the row has no live owner.
var wispOrphanTables = []string{
	"wisp_labels",
	"wisp_events",
	"wisp_comments",
	"wisp_dependencies",
}

const (
	// wispOrphanWarnThreshold is the orphan-row count above which the doctor
	// check escalates from an informational OK to an advisory Warning (and
	// becomes eligible for `gc doctor --fix`). Trivial residue below this — a
	// handful of rows from a wisp purged moments before its auxiliary rows —
	// is not worth a Dolt-wide delete sweep; the threshold keeps the check
	// quiet until the backlog is operationally significant.
	wispOrphanWarnThreshold = 1000

	// wispOrphanDeleteBatch bounds each DELETE so a multi-hundred-thousand-row
	// cleanup never runs as one statement that monopolizes the Dolt server.
	// Each batch is its own statement; partial progress persists, so an
	// interrupted cleanup is safely resumable by re-running --fix.
	wispOrphanDeleteBatch = 5000

	// wispOrphanScanTimeout bounds the read-only scan. COUNT over a saturated
	// (≈1M row) auxiliary table scans the whole table probing the wisps PK, so
	// the budget is generous; an over-budget scan reports what it counted
	// rather than hanging `gc doctor`.
	wispOrphanScanTimeout = 60 * time.Second

	// wispOrphanFixTimeout bounds the whole batched cleanup. Individual batches
	// stay small (wispOrphanDeleteBatch); this only caps the worst-case total.
	wispOrphanFixTimeout = 10 * time.Minute
)

// errWispOrphanNoManagedDolt signals that no managed Dolt server is resolvable,
// so the scan is skipped rather than failed.
var errWispOrphanNoManagedDolt = errors.New("managed Dolt server not running")

// wispOrphanCountSQL builds the COUNT statement for orphan rows in table: rows
// whose owning issue_id has no matching wisps.id. The database is selected with
// USE before this runs, so the table is referenced unqualified. wisps.id is the
// non-null primary key, so `NOT IN (SELECT id FROM wisps)` is safe from the
// SQL NULL-semantics trap.
func wispOrphanCountSQL(table string) string {
	return fmt.Sprintf("SELECT COUNT(*) FROM `%s` WHERE issue_id NOT IN (SELECT id FROM `wisps`)", table)
}

// wispOrphanDeleteSQL builds the bounded DELETE for orphan rows in table. The
// LIMIT keeps each statement bounded; callers loop until a batch deletes fewer
// than limit rows. Dolt's SQL engine supports DELETE ... LIMIT (vitess Delete
// carries a Limit; go-mysql-server wraps the delete source in a Limit node).
func wispOrphanDeleteSQL(table string, limit int) string {
	return fmt.Sprintf("DELETE FROM `%s` WHERE issue_id NOT IN (SELECT id FROM `wisps`) LIMIT %d", table, limit)
}

// isWispOrphanTable reports whether table is one of the known wisp auxiliary
// tables. Cleanup refuses any other identifier so a stray table name can never
// reach the COUNT/DELETE SQL builders.
func isWispOrphanTable(table string) bool {
	for _, t := range wispOrphanTables {
		if t == table {
			return true
		}
	}
	return false
}

// wispOrphanClient is the minimal SQL surface the wisp-orphan scan and cleanup
// need against the managed Dolt server. Production is sqlWispOrphanClient
// (wraps *sql.DB); tests inject a fake. It mirrors doltMaintenanceClient so the
// per-database iteration stays unit-testable without a live Dolt server.
type wispOrphanClient interface {
	// ListUserDatabases returns the managed user databases (system schemas
	// excluded) — the same set DOLT_GC maintenance compacts.
	ListUserDatabases(ctx context.Context) ([]string, error)
	// WispOrphanTables returns the subset of wispOrphanTables that exist in db.
	// It returns nil when db lacks the `wisps` owner table (not a bd-managed
	// bead store) so the caller skips the database entirely.
	WispOrphanTables(ctx context.Context, db string) ([]string, error)
	// CountOrphans returns the count of rows in db.table whose owning issue_id
	// is absent from db.wisps.
	CountOrphans(ctx context.Context, db, table string) (int64, error)
	// DeleteOrphans deletes up to limit orphan rows from db.table and returns
	// the number removed.
	DeleteOrphans(ctx context.Context, db, table string, limit int) (int64, error)
	// Close releases the underlying connection pool.
	Close() error
}

// wispOrphanCount is the orphan-row count for one database/table.
type wispOrphanCount struct {
	Database string
	Table    string
	Count    int64
}

// wispOrphanScanResult is the read-only scan outcome: per-table orphan counts,
// the total, how many bd stores were scanned, and any non-fatal per-database
// errors that did not abort the scan.
type wispOrphanScanResult struct {
	Counts           []wispOrphanCount
	Total            int64
	DatabasesScanned int
	Errors           []string
}

// wispOrphanRemoved records the rows removed from one database/table.
type wispOrphanRemoved struct {
	Database string
	Table    string
	Count    int64
}

// wispOrphanCleanupResult is the cleanup outcome: rows removed per
// database/table, the total, and any non-fatal errors.
type wispOrphanCleanupResult struct {
	Removed []wispOrphanRemoved
	Total   int64
	Errors  []string
}

// scanWispOrphans counts orphan rows across every managed user database that
// has wisp tables. Per-database and per-table errors are recorded but never
// abort the sweep — a partial count is more useful than no count. Databases
// without wisp tables are skipped silently (not bd-managed stores).
func scanWispOrphans(ctx context.Context, client wispOrphanClient) wispOrphanScanResult {
	var res wispOrphanScanResult
	dbs, err := client.ListUserDatabases(ctx)
	if err != nil {
		res.Errors = append(res.Errors, fmt.Sprintf("list databases: %v", err))
		return res
	}
	for _, db := range dbs {
		if ctxErr := ctx.Err(); ctxErr != nil {
			res.Errors = append(res.Errors, fmt.Sprintf("scan interrupted: %v", ctxErr))
			return res
		}
		tables, err := client.WispOrphanTables(ctx, db)
		if err != nil {
			res.Errors = append(res.Errors, fmt.Sprintf("%s: list wisp tables: %v", db, err))
			continue
		}
		if len(tables) == 0 {
			continue
		}
		res.DatabasesScanned++
		for _, table := range tables {
			n, err := client.CountOrphans(ctx, db, table)
			if err != nil {
				res.Errors = append(res.Errors, fmt.Sprintf("%s.%s: count orphans: %v", db, table, err))
				continue
			}
			if n > 0 {
				res.Counts = append(res.Counts, wispOrphanCount{Database: db, Table: table, Count: n})
				res.Total += n
			}
		}
	}
	return res
}

// cleanupWispOrphans removes orphan rows across every managed user database
// that has wisp tables, deleting in bounded batches so no single statement
// monopolizes Dolt. Each table loops until a batch removes fewer than batch
// rows (i.e. no orphans remain). Errors are recorded but never abort the sweep;
// a context deadline stops cleanly with partial progress preserved.
func cleanupWispOrphans(ctx context.Context, client wispOrphanClient, batch int) wispOrphanCleanupResult {
	var res wispOrphanCleanupResult
	if batch <= 0 {
		batch = wispOrphanDeleteBatch
	}
	dbs, err := client.ListUserDatabases(ctx)
	if err != nil {
		res.Errors = append(res.Errors, fmt.Sprintf("list databases: %v", err))
		return res
	}
	for _, db := range dbs {
		tables, err := client.WispOrphanTables(ctx, db)
		if err != nil {
			res.Errors = append(res.Errors, fmt.Sprintf("%s: list wisp tables: %v", db, err))
			continue
		}
		for _, table := range tables {
			var removed int64
			for {
				if ctxErr := ctx.Err(); ctxErr != nil {
					res.Errors = append(res.Errors, fmt.Sprintf("%s.%s: cleanup interrupted after %d rows: %v", db, table, removed, ctxErr))
					break
				}
				n, err := client.DeleteOrphans(ctx, db, table, batch)
				if err != nil {
					res.Errors = append(res.Errors, fmt.Sprintf("%s.%s: delete orphans: %v", db, table, err))
					break
				}
				removed += n
				if n < int64(batch) {
					break
				}
			}
			if removed > 0 {
				res.Removed = append(res.Removed, wispOrphanRemoved{Database: db, Table: table, Count: removed})
				res.Total += removed
			}
		}
	}
	return res
}

// wispOrphanDetailLines renders per-database/table orphan counts for the
// doctor result's verbose Details.
func wispOrphanDetailLines(counts []wispOrphanCount) []string {
	lines := make([]string, 0, len(counts))
	for _, c := range counts {
		lines = append(lines, fmt.Sprintf("%s.%s: %d orphan rows", c.Database, c.Table, c.Count))
	}
	return lines
}

// wispOrphanCheck is a doctor check that reports orphaned wisp auxiliary rows
// across the managed Dolt server and, under `gc doctor --fix`, removes them in
// bounded batches. Plain `gc doctor` only reports; deletion happens solely
// under --fix, so a status/health run never deletes anything.
type wispOrphanCheck struct {
	cityPath      string
	cfg           *config.City
	warnThreshold int64

	// newClient is the injection point for tests; production resolves the
	// managed Dolt port and opens a real SQL client.
	newClient func() (wispOrphanClient, error)

	// cleanable is set by Run when the orphan backlog exceeds the warn
	// threshold, gating CanFix. The doctor framework always calls Run before
	// CanFix, and re-runs Run after a successful Fix, so this is reset each Run.
	cleanable bool
}

// newWispOrphanCheck constructs a wispOrphanCheck for the given city.
func newWispOrphanCheck(cityPath string, cfg *config.City) *wispOrphanCheck {
	return &wispOrphanCheck{cityPath: cityPath, cfg: cfg, warnThreshold: wispOrphanWarnThreshold}
}

// Name implements doctor.Check.
func (c *wispOrphanCheck) Name() string { return "wisp-orphans" }

// WarmupEligible implements doctor.Check. The scan issues COUNT queries over
// potentially large tables, so it runs on demand via `gc doctor`, never during
// `gc start` warm-up.
func (c *wispOrphanCheck) WarmupEligible() bool { return false }

// CanFix implements doctor.Check. Fix is offered only when the backlog exceeds
// the warn threshold (set by Run); trivial residue is left alone.
func (c *wispOrphanCheck) CanFix() bool { return c.cleanable }

// Run implements doctor.Check. It scans every managed user database for
// orphaned wisp auxiliary rows and reports per-database counts, warning
// (advisory) when the total exceeds the threshold.
func (c *wispOrphanCheck) Run(_ *doctor.CheckContext) *doctor.CheckResult {
	c.cleanable = false
	r := &doctor.CheckResult{Name: c.Name(), Severity: doctor.SeverityAdvisory, Status: doctor.StatusOK}

	if c.cfg != nil && !workspaceUsesManagedBdStoreContract(c.cityPath, c.cfg.Rigs) {
		r.Message = "not using bd-backed Dolt store; wisp-orphan scan not applicable"
		return r
	}

	client, err := c.openClient()
	if err != nil {
		if errors.Is(err, errWispOrphanNoManagedDolt) {
			r.Message = "managed Dolt not running; wisp-orphan scan skipped"
			return r
		}
		r.Status = doctor.StatusWarning
		r.Message = fmt.Sprintf("wisp-orphan scan unavailable: %v", err)
		return r
	}
	defer client.Close() //nolint:errcheck // best-effort close

	ctx, cancel := context.WithTimeout(context.Background(), wispOrphanScanTimeout)
	defer cancel()
	scan := scanWispOrphans(ctx, client)
	r.Details = wispOrphanDetailLines(scan.Counts)

	switch {
	case scan.Total > c.warnThreshold:
		c.cleanable = true
		r.Status = doctor.StatusWarning
		r.Message = fmt.Sprintf("%d orphaned wisp auxiliary rows across %d database(s); a bare DELETE FROM wisps leaks label/event/comment rows", scan.Total, scan.DatabasesScanned)
		r.FixHint = "run `gc doctor --fix` to remove orphaned wisp_labels/wisp_events/wisp_comments/wisp_dependencies rows in bounded batches, then run Dolt GC maintenance to reclaim disk"
		r.Details = append(r.Details, scan.Errors...)
	case len(scan.Errors) > 0:
		r.Status = doctor.StatusWarning
		r.Message = fmt.Sprintf("wisp-orphan scan incomplete (%d orphan rows counted before errors)", scan.Total)
		r.Details = append(r.Details, scan.Errors...)
	case scan.Total > 0:
		r.Message = fmt.Sprintf("%d orphaned wisp auxiliary rows (below warn threshold %d)", scan.Total, c.warnThreshold)
	default:
		r.Message = "no orphaned wisp auxiliary rows"
	}
	return r
}

// Fix implements doctor.Check. It removes orphaned wisp auxiliary rows in
// bounded batches and prints a per-table summary plus Dolt-GC guidance. Only
// invoked by the framework when CanFix is true and Run returned non-OK.
func (c *wispOrphanCheck) Fix(ctx *doctor.CheckContext) error {
	out := io.Writer(os.Stderr)
	if ctx != nil && ctx.Output != nil {
		out = ctx.Output
	}

	client, err := c.openClient()
	if err != nil {
		return fmt.Errorf("wisp-orphans: open managed Dolt client: %w", err)
	}
	defer client.Close() //nolint:errcheck // best-effort close

	fctx, cancel := context.WithTimeout(context.Background(), wispOrphanFixTimeout)
	defer cancel()
	res := cleanupWispOrphans(fctx, client, wispOrphanDeleteBatch)

	for _, removed := range res.Removed {
		fmt.Fprintf(out, "  wisp-orphans: removed %d orphan rows from %s.%s\n", removed.Count, removed.Database, removed.Table) //nolint:errcheck // best-effort output
	}
	if res.Total > 0 {
		// Guidance (acceptance): reclaim disk with Dolt GC after large removals.
		fmt.Fprintf(out, "  wisp-orphans: removed %d orphaned wisp auxiliary rows; run Dolt GC maintenance (CALL DOLT_GC, or `gc dolt-cleanup --force`) to reclaim disk after large removals\n", res.Total) //nolint:errcheck // best-effort output
	}
	for _, e := range res.Errors {
		fmt.Fprintf(out, "  wisp-orphans: %s\n", e) //nolint:errcheck // best-effort output
	}
	if res.Total == 0 && len(res.Errors) > 0 {
		return fmt.Errorf("wisp-orphans: cleanup made no progress: %s", strings.Join(res.Errors, "; "))
	}
	return nil
}

// openClient resolves the managed Dolt port and opens a SQL client, or returns
// the injected test client. errWispOrphanNoManagedDolt is returned (and treated
// as a skip by Run) when no managed Dolt server is resolvable.
func (c *wispOrphanCheck) openClient() (wispOrphanClient, error) {
	if c.newClient != nil {
		return c.newClient()
	}
	port := currentResolvableManagedDoltPort(c.cityPath)
	if strings.TrimSpace(port) == "" {
		return nil, errWispOrphanNoManagedDolt
	}
	return newSQLWispOrphanClient("127.0.0.1", port)
}

// sqlWispOrphanClient is the production wispOrphanClient: it runs each
// operation against the managed Dolt server over a *sql.DB pool. The wisp
// auxiliary tables are dolt_ignore'd working-set tables, so deletes apply to
// the working set and persist without a DOLT_COMMIT — matching how the reaper
// and bd mutate them.
type sqlWispOrphanClient struct {
	db *sql.DB
}

// newSQLWispOrphanClient opens a maintenance-tuned connection to the managed
// Dolt server. The maintenance pool leaves per-read deadlines unset so a large
// batched cleanup is bounded by the caller's context rather than a short socket
// timeout. Caller must Close when done.
func newSQLWispOrphanClient(host, port string) (wispOrphanClient, error) {
	db, err := openManagedDoltMaintenanceDB(host, port, "root")
	if err != nil {
		return nil, fmt.Errorf("open dolt connection: %w", err)
	}
	return &sqlWispOrphanClient{db: db}, nil
}

func (c *sqlWispOrphanClient) ListUserDatabases(ctx context.Context) ([]string, error) {
	conn, err := c.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close() //nolint:errcheck // best-effort; pool owns lifecycle
	return managedDoltSelectUserDatabasesFromConn(ctx, conn)
}

func (c *sqlWispOrphanClient) WispOrphanTables(ctx context.Context, db string) ([]string, error) {
	conn, err := c.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close() //nolint:errcheck // best-effort; pool owns lifecycle

	rows, err := conn.QueryContext(ctx,
		"SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_name IN ('wisps','wisp_labels','wisp_events','wisp_comments','wisp_dependencies')",
		db)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck

	present := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		present[strings.ToLower(strings.TrimSpace(name))] = true
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if !present["wisps"] {
		// No owner table — not a bd-managed bead store. Skip it.
		return nil, nil
	}
	var out []string
	for _, table := range wispOrphanTables {
		if present[table] {
			out = append(out, table)
		}
	}
	return out, nil
}

func (c *sqlWispOrphanClient) CountOrphans(ctx context.Context, db, table string) (int64, error) {
	if !isWispOrphanTable(table) {
		return 0, fmt.Errorf("refusing to count unknown table %q", table)
	}
	conn, err := c.db.Conn(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Close() //nolint:errcheck // best-effort; pool owns lifecycle
	if err := useManagedDoltDatabase(ctx, conn, db); err != nil {
		return 0, err
	}
	var n int64
	if err := conn.QueryRowContext(ctx, wispOrphanCountSQL(table)).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

func (c *sqlWispOrphanClient) DeleteOrphans(ctx context.Context, db, table string, limit int) (int64, error) {
	if !isWispOrphanTable(table) {
		return 0, fmt.Errorf("refusing to delete from unknown table %q", table)
	}
	if limit <= 0 {
		limit = wispOrphanDeleteBatch
	}
	conn, err := c.db.Conn(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Close() //nolint:errcheck // best-effort; pool owns lifecycle
	if err := useManagedDoltDatabase(ctx, conn, db); err != nil {
		return 0, err
	}
	result, err := conn.ExecContext(ctx, wispOrphanDeleteSQL(table, limit))
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (c *sqlWispOrphanClient) Close() error { return c.db.Close() }
