//go:build integration || dolt_integration

package gastown_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestReaperWorkflowRootCleanupRealDoltSemantics(t *testing.T) {
	doltPath, err := exec.LookPath("dolt")
	if err != nil {
		t.Skipf("dolt not found: %v", err)
	}

	cityDir := t.TempDir()
	dataDir := filepath.Join(t.TempDir(), "dolt")
	for _, db := range []string{"citydb", "rigdb"} {
		dbDir := filepath.Join(dataDir, db)
		if err := os.MkdirAll(dbDir, 0o755); err != nil {
			t.Fatalf("MkdirAll(%s): %v", dbDir, err)
		}
		runDoltForMaintenanceTest(t, doltPath, dbDir, "init", "--name", "Gas City", "--email", "test@example.com")
		runDoltSQLForMaintenanceTest(t, doltPath, dbDir, maintenanceReaperSchemaSQL())
	}

	runDoltSQLForMaintenanceTest(t, doltPath, filepath.Join(dataDir, "citydb"), maintenanceReaperCitySeedSQL())
	runDoltForMaintenanceTest(t, doltPath, filepath.Join(dataDir, "citydb"), "add", ".")
	runDoltForMaintenanceTest(t, doltPath, filepath.Join(dataDir, "citydb"), "commit", "-m", "seed city workflow roots")

	runDoltSQLForMaintenanceTest(t, doltPath, filepath.Join(dataDir, "rigdb"), maintenanceReaperRigSeedSQL())
	runDoltForMaintenanceTest(t, doltPath, filepath.Join(dataDir, "rigdb"), "add", ".")
	runDoltForMaintenanceTest(t, doltPath, filepath.Join(dataDir, "rigdb"), "commit", "-m", "seed rig workflow roots")

	port := startDoltServerForMaintenanceTest(t, doltPath, dataDir)
	waitForDoltServerForMaintenanceTest(t, doltPath, port, "citydb")
	writeCityBeadsMetadata(t, cityDir, "citydb")
	rigDir := filepath.Join(cityDir, "rigs", "rig-with-db-alias")
	writeCityBeadsMetadata(t, rigDir, "rigdb")
	writeSiteRigBinding(t, cityDir, "rig-with-db-alias", rigDir)

	binDir := t.TempDir()
	bdLog := filepath.Join(t.TempDir(), "bd.log")
	if err := os.Symlink(doltPath, filepath.Join(binDir, "dolt")); err != nil {
		t.Fatalf("Symlink(dolt): %v", err)
	}
	writeExecutable(t, filepath.Join(binDir, "bd"), `#!/bin/sh
set -e
printf '%s\n' "$*" >> "$BD_CALL_LOG"
case "$1" in
  prune)
    printf '{"pruned_count":0}\n'
    ;;
  close)
    issue_id="$2"
    DOLT_CLI_PASSWORD="${GC_DOLT_PASSWORD:-}" dolt --host "$GC_DOLT_HOST" --port "$GC_DOLT_PORT" --user "$GC_DOLT_USER" --no-tls --use-db citydb sql \
      -q "UPDATE issues SET status='closed', closed_at=NOW() WHERE id='${issue_id}'; CALL DOLT_COMMIT('-Am', 'test bd close')"
    ;;
esac
exit 0
`)
	writeExecutable(t, filepath.Join(binDir, "gc"), `#!/bin/sh
case "$1 $2" in
  "session prune")
    printf '{"count":0}\n'
    ;;
esac
exit 0
`)

	env := map[string]string{
		"BD_CALL_LOG":      bdLog,
		"GC_CITY":          cityDir,
		"GC_CITY_PATH":     cityDir,
		"GC_DOLT_HOST":     "127.0.0.1",
		"GC_DOLT_PORT":     fmt.Sprintf("%d", port),
		"GC_DOLT_USER":     "root",
		"GC_DOLT_PASSWORD": "",
		"PATH":             binDir + string(os.PathListSeparator) + os.Getenv("PATH"),
	}
	runScript(t, coreScriptPath("reaper.sh"), env)

	bdData, err := os.ReadFile(bdLog)
	if err != nil {
		t.Fatalf("ReadFile(bd log): %v", err)
	}
	if !strings.Contains(string(bdData), "close issue-close --reason stale inactive workflow root auto-closed by reaper") {
		t.Fatalf("reaper did not close city workflow issue root through bd close:\n%s", bdData)
	}

	cityWispStatuses := queryMaintenanceStatusByID(t, doltPath, port, "citydb", "wisps")
	requireMaintenanceStatuses(t, cityWispStatuses, map[string]string{
		"wisp-close":               "closed",
		"wisp-city-store-root":     "closed",
		"wisp-cross-store-root":    "open",
		"wisp-held":                "blocked",
		"wisp-non-root-workflow":   "open",
		"wisp-recent-root":         "open",
		"wisp-nested-root":         "open",
		"wisp-subroot":             "closed",
		"wisp-live-grandchild":     "in_progress",
		"wisp-recent-closed-child": "closed",
	})

	cityIssueStatuses := queryMaintenanceStatusByID(t, doltPath, port, "citydb", "issues")
	requireMaintenanceStatuses(t, cityIssueStatuses, map[string]string{
		"issue-city-store-root":   "closed",
		"issue-close":             "closed",
		"issue-cross-store-root":  "open",
		"issue-held":              "blocked",
		"issue-dep-root":          "open",
		"issue-dep-live":          "in_progress",
		"issue-non-root-workflow": "open",
	})

	rigWispStatuses := queryMaintenanceStatusByID(t, doltPath, port, "rigdb", "wisps")
	requireMaintenanceStatuses(t, rigWispStatuses, map[string]string{
		"rig-wisp-close":            "closed",
		"rig-wisp-store-root":       "closed",
		"rig-wisp-other-store-root": "open",
	})

	rigIssueStatuses := queryMaintenanceStatusByID(t, doltPath, port, "rigdb", "issues")
	requireMaintenanceStatuses(t, rigIssueStatuses, map[string]string{
		"rig-issue-preserve": "open",
	})
}

func maintenanceReaperSchemaSQL() string {
	return `
CREATE TABLE wisps (
  id VARCHAR(64) PRIMARY KEY,
  title VARCHAR(255),
  status VARCHAR(32),
  issue_type VARCHAR(32),
  priority BIGINT,
  created_at DATETIME(6),
  updated_at DATETIME(6),
  closed_at DATETIME(6),
  assignee VARCHAR(255),
  description LONGTEXT,
  metadata JSON
);
CREATE TABLE issues (
  id VARCHAR(64) PRIMARY KEY,
  title VARCHAR(255),
  status VARCHAR(32),
  issue_type VARCHAR(32),
  priority BIGINT,
  created_at DATETIME(6),
  updated_at DATETIME(6),
  closed_at DATETIME(6),
  assignee VARCHAR(255),
  description LONGTEXT,
  metadata JSON
);
CREATE TABLE dependencies (
  issue_id VARCHAR(64),
  depends_on_issue_id VARCHAR(64),
  depends_on_wisp_id VARCHAR(64),
  depends_on_external VARCHAR(64),
  type VARCHAR(32)
);
CREATE TABLE wisp_dependencies (
  issue_id VARCHAR(64),
  depends_on_issue_id VARCHAR(64),
  depends_on_wisp_id VARCHAR(64),
  depends_on_external VARCHAR(64),
  type VARCHAR(32)
);
CREATE TABLE labels (
  issue_id VARCHAR(64),
  label VARCHAR(255)
);
CREATE TABLE wisp_labels (
  issue_id VARCHAR(64),
  label VARCHAR(255)
);
CREATE TABLE wisp_events (
  id VARCHAR(64) PRIMARY KEY,
  issue_id VARCHAR(64),
  event_type VARCHAR(64),
  created_at DATETIME(6)
);
CREATE TABLE wisp_comments (
  id VARCHAR(64) PRIMARY KEY,
  issue_id VARCHAR(64),
  author VARCHAR(255),
  text LONGTEXT,
  created_at DATETIME(6)
);
`
}

func maintenanceReaperCitySeedSQL() string {
	return `
INSERT INTO wisps (id, title, status, issue_type, priority, created_at, updated_at, assignee, metadata) VALUES
  ('wisp-close', 'closeable root', 'open', 'task', 2, '2026-01-01 00:00:00', '2026-01-01 00:00:00', '', '{"gc.kind":"workflow"}'),
  ('wisp-city-store-root', 'closeable city-store root', 'open', 'task', 2, '2026-01-01 00:00:00', '2026-01-01 00:00:00', '', '{"gc.kind":"workflow","gc.root_store_ref":"city:test-city"}'),
  ('wisp-cross-store-root', 'cross-store root preserved', 'open', 'task', 2, '2026-01-01 00:00:00', '2026-01-01 00:00:00', '', '{"gc.kind":"workflow","gc.root_store_ref":"rig:other"}'),
  ('wisp-held', 'held root', 'blocked', 'task', 2, '2026-01-01 00:00:00', '2026-01-01 00:00:00', '', '{"gc.kind":"workflow"}'),
  ('wisp-non-root-workflow', 'non-root topology bead preserved', 'open', 'task', 2, '2026-01-01 00:00:00', '2026-01-01 00:00:00', '', '{"gc.kind":"workflow","gc.root_bead_id":"wisp-nested-root"}'),
  ('wisp-recent-root', 'recent descendant root', 'open', 'task', 2, '2026-01-01 00:00:00', '2026-01-01 00:00:00', '', '{"gc.kind":"workflow"}'),
  ('wisp-recent-closed-child', 'recent closed child', 'closed', 'task', 2, '2026-01-01 00:00:00', NOW(), '', '{"gc.root_bead_id":"wisp-recent-root"}'),
  ('wisp-nested-root', 'nested root', 'open', 'task', 2, '2026-01-01 00:00:00', '2026-01-01 00:00:00', '', '{"gc.kind":"workflow"}'),
  ('wisp-subroot', 'nested subroot', 'closed', 'task', 2, '2026-01-01 00:00:00', '2026-01-01 00:00:00', '', '{"gc.root_bead_id":"wisp-nested-root"}'),
  ('wisp-live-grandchild', 'live nested child', 'in_progress', 'task', 2, '2026-01-01 00:00:00', '2026-01-01 00:00:00', '', '{}');
INSERT INTO wisp_dependencies (issue_id, depends_on_wisp_id, type) VALUES
  ('wisp-subroot', 'wisp-nested-root', 'tracks'),
  ('wisp-live-grandchild', 'wisp-subroot', 'tracks');
INSERT INTO issues (id, title, status, issue_type, priority, created_at, updated_at, assignee, metadata) VALUES
  ('issue-close', 'closeable city issue root', 'open', 'task', 2, '2026-01-01 00:00:00', '2026-01-01 00:00:00', '', '{"gc.kind":"workflow"}'),
  ('issue-city-store-root', 'closeable city-store issue root', 'open', 'task', 2, '2026-01-01 00:00:00', '2026-01-01 00:00:00', '', '{"gc.kind":"workflow","gc.root_store_ref":"city:test-city"}'),
  ('issue-cross-store-root', 'cross-store issue root preserved', 'open', 'task', 1, '2026-01-01 00:00:00', '2026-01-01 00:00:00', '', '{"gc.kind":"workflow","gc.root_store_ref":"rig:other"}'),
  ('issue-held', 'held city issue root', 'blocked', 'task', 2, '2026-01-01 00:00:00', '2026-01-01 00:00:00', '', '{"gc.kind":"workflow"}'),
  ('issue-non-root-workflow', 'non-root issue topology bead preserved', 'open', 'task', 1, '2026-01-01 00:00:00', '2026-01-01 00:00:00', '', '{"gc.kind":"workflow","gc.root_bead_id":"issue-dep-root"}'),
  ('issue-dep-root', 'dependency-protected issue root', 'open', 'task', 2, '2026-01-01 00:00:00', '2026-01-01 00:00:00', '', '{"gc.kind":"workflow"}'),
  ('issue-dep-live', 'live issue dependency child', 'in_progress', 'task', 2, '2026-01-01 00:00:00', '2026-01-01 00:00:00', '', '{}');
INSERT INTO dependencies (issue_id, depends_on_issue_id, type) VALUES
  ('issue-dep-live', 'issue-dep-root', 'blocks');
`
}

func maintenanceReaperRigSeedSQL() string {
	return `
INSERT INTO wisps (id, title, status, issue_type, priority, created_at, updated_at, assignee, metadata) VALUES
  ('rig-wisp-close', 'closeable non-city wisp root', 'open', 'task', 2, '2026-01-01 00:00:00', '2026-01-01 00:00:00', '', '{"gc.kind":"workflow"}'),
  ('rig-wisp-store-root', 'closeable rig-store wisp root', 'open', 'task', 2, '2026-01-01 00:00:00', '2026-01-01 00:00:00', '', '{"gc.kind":"workflow","gc.root_store_ref":"rig:rig-with-db-alias"}'),
  ('rig-wisp-other-store-root', 'other rig-store root preserved', 'open', 'task', 2, '2026-01-01 00:00:00', '2026-01-01 00:00:00', '', '{"gc.kind":"workflow","gc.root_store_ref":"rig:other"}');
INSERT INTO issues (id, title, status, issue_type, priority, created_at, updated_at, assignee, metadata) VALUES
  ('rig-issue-preserve', 'non-city issue root', 'open', 'task', 2, '2026-01-01 00:00:00', '2026-01-01 00:00:00', '', '{"gc.kind":"workflow"}');
`
}

func runDoltForMaintenanceTest(t *testing.T, doltPath, dir string, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, doltPath, args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("dolt %s failed in %s: %v\n%s", strings.Join(args, " "), dir, err, out)
	}
	return string(out)
}

func runDoltSQLForMaintenanceTest(t *testing.T, doltPath, dir, query string) string {
	t.Helper()
	return runDoltForMaintenanceTest(t, doltPath, dir, "sql", "-q", query)
}

func startDoltServerForMaintenanceTest(t *testing.T, doltPath, dataDir string) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatalf("Close listener: %v", err)
	}

	logPath := filepath.Join(dataDir, "sql-server.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("Create(%s): %v", logPath, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, doltPath, "sql-server",
		"-H", "127.0.0.1",
		"-P", fmt.Sprintf("%d", port),
		"--data-dir", dataDir,
		"--loglevel", "warning",
	)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		t.Fatalf("Start dolt sql-server: %v", err)
	}

	waitCh := make(chan error, 1)
	go func() {
		waitCh <- cmd.Wait()
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-waitCh:
		case <-time.After(10 * time.Second):
			_ = cmd.Process.Kill()
			<-waitCh
		}
		_ = logFile.Close()
	})
	return port
}

func waitForDoltServerForMaintenanceTest(t *testing.T, doltPath string, port int, db string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	var lastOut []byte
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		cmd := exec.CommandContext(ctx, doltPath,
			"--host", "127.0.0.1",
			"--port", fmt.Sprintf("%d", port),
			"--user", "root",
			"--no-tls",
			"--use-db", db,
			"sql", "-q", "SELECT 1",
		)
		cmd.Env = append(os.Environ(), "DOLT_CLI_PASSWORD=")
		lastOut, lastErr = cmd.CombinedOutput()
		cancel()
		if lastErr == nil {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("dolt sql-server did not become ready on port %d: %v\n%s", port, lastErr, lastOut)
}

func queryMaintenanceStatusByID(t *testing.T, doltPath string, port int, db string, table string) map[string]string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, doltPath,
		"--host", "127.0.0.1",
		"--port", fmt.Sprintf("%d", port),
		"--user", "root",
		"--no-tls",
		"--use-db", db,
		"sql", "-r", "csv", "-q", fmt.Sprintf("SELECT id,status FROM %s ORDER BY id", table),
	)
	cmd.Env = append(os.Environ(), "DOLT_CLI_PASSWORD=")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("query %s.%s statuses: %v\n%s", db, table, err, out)
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) == 0 || strings.TrimSpace(lines[0]) != "id,status" {
		t.Fatalf("unexpected status output for %s.%s:\n%s", db, table, out)
	}
	statuses := make(map[string]string)
	for _, line := range lines[1:] {
		fields := strings.Split(line, ",")
		if len(fields) != 2 {
			t.Fatalf("unexpected status row for %s.%s: %q\nfull output:\n%s", db, table, line, out)
		}
		statuses[fields[0]] = fields[1]
	}
	return statuses
}

func requireMaintenanceStatuses(t *testing.T, got map[string]string, want map[string]string) {
	t.Helper()
	for id, wantStatus := range want {
		if got[id] != wantStatus {
			t.Fatalf("status[%s] = %q, want %q\nall statuses: %#v", id, got[id], wantStatus, got)
		}
	}
}

// TestReaperPurgeCascadesAuxiliaryWispTables proves the closed-wisp purge
// (reaper.sh Step 3) removes the auxiliary rows owned by a purged wisp —
// wisp_labels, wisp_events, wisp_comments, and wisp_dependencies — rather than
// stranding them as orphans. A bare `DELETE FROM wisps` leaves those child
// rows behind; on the production city that accumulated into ~571k orphaned
// wisp_labels and ~1.01M orphaned wisp_events rows and saturated Dolt
// (gc-4zo0v). The test also confirms purge protection (a closed parent with a
// live child is preserved) and that live / recently-closed wisps keep their
// auxiliary rows.
func TestReaperPurgeCascadesAuxiliaryWispTables(t *testing.T) {
	doltPath, err := exec.LookPath("dolt")
	if err != nil {
		t.Skipf("dolt not found: %v", err)
	}

	cityDir := t.TempDir()
	dataDir := filepath.Join(t.TempDir(), "dolt")
	dbDir := filepath.Join(dataDir, "citydb")
	if err := os.MkdirAll(dbDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(%s): %v", dbDir, err)
	}
	runDoltForMaintenanceTest(t, doltPath, dbDir, "init", "--name", "Gas City", "--email", "test@example.com")
	runDoltSQLForMaintenanceTest(t, doltPath, dbDir, maintenanceReaperSchemaSQL())
	runDoltSQLForMaintenanceTest(t, doltPath, dbDir, maintenanceReaperPurgeSeedSQL())
	runDoltForMaintenanceTest(t, doltPath, dbDir, "add", ".")
	runDoltForMaintenanceTest(t, doltPath, dbDir, "commit", "-m", "seed purge cascade fixtures")

	port := startDoltServerForMaintenanceTest(t, doltPath, dataDir)
	waitForDoltServerForMaintenanceTest(t, doltPath, port, "citydb")
	writeCityBeadsMetadata(t, cityDir, "citydb")

	binDir := t.TempDir()
	bdLog := filepath.Join(t.TempDir(), "bd.log")
	if err := os.Symlink(doltPath, filepath.Join(binDir, "dolt")); err != nil {
		t.Fatalf("Symlink(dolt): %v", err)
	}
	writeExecutable(t, filepath.Join(binDir, "bd"), `#!/bin/sh
set -e
printf '%s\n' "$*" >> "$BD_CALL_LOG"
case "$1" in
  prune)
    printf '{"pruned_count":0}\n'
    ;;
esac
exit 0
`)
	writeExecutable(t, filepath.Join(binDir, "gc"), `#!/bin/sh
case "$1 $2" in
  "session prune")
    printf '{"count":0}\n'
    ;;
esac
exit 0
`)

	env := map[string]string{
		"BD_CALL_LOG":      bdLog,
		"GC_CITY":          cityDir,
		"GC_CITY_PATH":     cityDir,
		"GC_DOLT_HOST":     "127.0.0.1",
		"GC_DOLT_PORT":     fmt.Sprintf("%d", port),
		"GC_DOLT_USER":     "root",
		"GC_DOLT_PASSWORD": "",
		"PATH":             binDir + string(os.PathListSeparator) + os.Getenv("PATH"),
	}
	runScript(t, coreScriptPath("reaper.sh"), env)

	// The old, unprotected closed wisp is gone; protected / live /
	// recently-closed wisps survive.
	wispStatuses := queryMaintenanceStatusByID(t, doltPath, port, "citydb", "wisps")
	if status, ok := wispStatuses["purge-target"]; ok {
		t.Fatalf("purge-target should have been purged, still present with status %q\nall: %#v", status, wispStatuses)
	}
	requireMaintenanceStatuses(t, wispStatuses, map[string]string{
		"protected-parent": "closed",
		"live-wisp":        "in_progress",
		"purge-recent":     "closed",
		"closed-child":     "closed",
		"live-child":       "in_progress",
	})

	// Auxiliary tables keyed by issue_id: the purged wisp's rows are gone, every
	// surviving wisp keeps its rows, and no orphan (row whose issue_id no longer
	// resolves to a wisp) remains. The orphan check is the direct regression
	// assertion — it is non-zero under the old `DELETE FROM wisps`-only purge.
	for _, aux := range []string{"wisp_labels", "wisp_events", "wisp_comments"} {
		if got := queryMaintenanceScalarInt(t, doltPath, port, "citydb",
			fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE issue_id='purge-target'", aux)); got != 0 {
			t.Fatalf("%s: purge-target rows should be cascaded, found %d", aux, got)
		}
		if got := queryMaintenanceScalarInt(t, doltPath, port, "citydb",
			fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE issue_id='live-wisp'", aux)); got != 1 {
			t.Fatalf("%s: live-wisp row should be preserved, found %d", aux, got)
		}
		if got := queryMaintenanceScalarInt(t, doltPath, port, "citydb",
			fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE issue_id='protected-parent'", aux)); got != 1 {
			t.Fatalf("%s: protected-parent row should be preserved, found %d", aux, got)
		}
		if got := queryMaintenanceScalarInt(t, doltPath, port, "citydb",
			fmt.Sprintf("SELECT COUNT(*) FROM %s a LEFT JOIN wisps w ON a.issue_id = w.id WHERE w.id IS NULL", aux)); got != 0 {
			t.Fatalf("%s: expected zero orphaned rows after purge, found %d", aux, got)
		}
	}

	// wisp_dependencies: the purged wisp's owned edge (issue_id) and the inbound
	// edge that referenced it (depends_on_wisp_id) are both removed so no edge
	// dangles; the live child's protection edge is untouched.
	if got := queryMaintenanceScalarInt(t, doltPath, port, "citydb",
		"SELECT COUNT(*) FROM wisp_dependencies WHERE issue_id='purge-target'"); got != 0 {
		t.Fatalf("wisp_dependencies: owned edges of purge-target should be cascaded, found %d", got)
	}
	if got := queryMaintenanceScalarInt(t, doltPath, port, "citydb",
		"SELECT COUNT(*) FROM wisp_dependencies WHERE depends_on_wisp_id='purge-target'"); got != 0 {
		t.Fatalf("wisp_dependencies: inbound edges to purge-target should be cascaded, found %d", got)
	}
	if got := queryMaintenanceScalarInt(t, doltPath, port, "citydb",
		"SELECT COUNT(*) FROM wisp_dependencies WHERE issue_id='live-child' AND depends_on_wisp_id='protected-parent'"); got != 1 {
		t.Fatalf("wisp_dependencies: live-child protection edge should be preserved, found %d", got)
	}
}

func maintenanceReaperPurgeSeedSQL() string {
	return `
INSERT INTO wisps (id, title, status, issue_type, priority, created_at, updated_at, closed_at, assignee, metadata) VALUES
  ('purge-target', 'old closed purgeable wisp', 'closed', 'task', 2, '2026-01-01 00:00:00', '2026-01-01 00:00:00', '2026-01-01 00:00:00', '', '{}'),
  ('purge-recent', 'recently closed wisp, too new to purge', 'closed', 'task', 2, '2026-01-01 00:00:00', NOW(), NOW(), '', '{}'),
  ('live-wisp', 'live wisp with auxiliary rows', 'in_progress', 'task', 2, '2026-01-01 00:00:00', '2026-01-01 00:00:00', NULL, '', '{}'),
  ('protected-parent', 'old closed parent protected by a live child', 'closed', 'task', 2, '2026-01-01 00:00:00', '2026-01-01 00:00:00', '2026-01-01 00:00:00', '', '{}'),
  ('live-child', 'live child that blocks parent purge', 'in_progress', 'task', 2, '2026-01-01 00:00:00', '2026-01-01 00:00:00', NULL, '', '{}'),
  ('closed-child', 'recently closed child with an inbound edge to purge-target', 'closed', 'task', 2, '2026-01-01 00:00:00', NOW(), NOW(), '', '{}');
INSERT INTO wisp_dependencies (issue_id, depends_on_issue_id, depends_on_wisp_id, depends_on_external, type) VALUES
  ('live-child', NULL, 'protected-parent', NULL, 'blocks'),
  ('closed-child', NULL, 'purge-target', NULL, 'parent-child'),
  ('purge-target', NULL, NULL, 'ext-ref', 'tracks');
INSERT INTO wisp_labels (issue_id, label) VALUES
  ('purge-target', 'order-tracking'),
  ('purge-recent', 'order-tracking'),
  ('live-wisp', 'order-tracking'),
  ('protected-parent', 'order-tracking'),
  ('live-child', 'order-tracking'),
  ('closed-child', 'order-tracking');
INSERT INTO wisp_events (id, issue_id, event_type, created_at) VALUES
  ('ev-purge-target', 'purge-target', 'created', '2026-01-01 00:00:00'),
  ('ev-purge-recent', 'purge-recent', 'created', '2026-01-01 00:00:00'),
  ('ev-live-wisp', 'live-wisp', 'created', '2026-01-01 00:00:00'),
  ('ev-protected-parent', 'protected-parent', 'created', '2026-01-01 00:00:00'),
  ('ev-live-child', 'live-child', 'created', '2026-01-01 00:00:00'),
  ('ev-closed-child', 'closed-child', 'created', '2026-01-01 00:00:00');
INSERT INTO wisp_comments (id, issue_id, author, text, created_at) VALUES
  ('cm-purge-target', 'purge-target', 'tester', 'note', '2026-01-01 00:00:00'),
  ('cm-purge-recent', 'purge-recent', 'tester', 'note', '2026-01-01 00:00:00'),
  ('cm-live-wisp', 'live-wisp', 'tester', 'note', '2026-01-01 00:00:00'),
  ('cm-protected-parent', 'protected-parent', 'tester', 'note', '2026-01-01 00:00:00'),
  ('cm-live-child', 'live-child', 'tester', 'note', '2026-01-01 00:00:00'),
  ('cm-closed-child', 'closed-child', 'tester', 'note', '2026-01-01 00:00:00');
`
}

func queryMaintenanceScalarInt(t *testing.T, doltPath string, port int, db string, query string) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, doltPath,
		"--host", "127.0.0.1",
		"--port", fmt.Sprintf("%d", port),
		"--user", "root",
		"--no-tls",
		"--use-db", db,
		"sql", "-r", "csv", "-q", query,
	)
	cmd.Env = append(os.Environ(), "DOLT_CLI_PASSWORD=")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("scalar query %q on %s: %v\n%s", query, db, err, out)
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) < 2 {
		t.Fatalf("unexpected scalar output for %q on %s:\n%s", query, db, out)
	}
	value := strings.TrimSpace(lines[len(lines)-1])
	n, convErr := strconv.Atoi(value)
	if convErr != nil {
		t.Fatalf("non-integer scalar %q for %q on %s:\n%s", value, query, db, out)
	}
	return n
}
