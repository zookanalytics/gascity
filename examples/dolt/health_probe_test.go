// Probe-representativeness tests for the health command (gc-le2h7o).
//
// Regression context: during the 2026-06-01→06-03 data-plane wedge, the
// connection pool was saturated by 30+ minute table scans. Every fresh bd
// client was rejected with "max waiting connections reached. Client
// rejected", yet `gc dolt health` reported reachable:true because its
// single SELECT 1 probe occasionally squeezed through the wait queue and,
// being trivial, completed instantly once connected. These tests pin the
// fixed contract: the verdict requires a majority of connect-fresh
// representative-query attempts, pool rejections surface as first-class
// saturation signals, and a single lucky squeeze-through no longer reports
// healthy.
package dolt_test

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// healthProbeReport mirrors the health --json schema fields exercised by
// the probe tests. Additive fields elsewhere in the payload are ignored.
type healthProbeReport struct {
	Server struct {
		Running   bool `json:"running"`
		Reachable bool `json:"reachable"`
		Degraded  bool `json:"degraded"`
		LatencyMS int  `json:"latency_ms"`
		Probe     struct {
			Attempts  int    `json:"attempts"`
			Successes int    `json:"successes"`
			Rejected  int    `json:"rejected"`
			Timeouts  int    `json:"timeouts"`
			Errors    int    `json:"errors"`
			Database  string `json:"database"`
			LastError string `json:"last_error"`
		} `json:"probe"`
		Pool struct {
			ActiveConnections int  `json:"active_connections"`
			MaxConnections    int  `json:"max_connections"`
			Saturated         bool `json:"saturated"`
			ProbeOK           bool `json:"probe_ok"`
		} `json:"pool"`
	} `json:"server"`
	Databases []struct {
		Name    string `json:"name"`
		Commits int    `json:"commits"`
		ProbeOK bool   `json:"probe_ok"`
	} `json:"databases"`
}

// runHealthJSON executes the health script with a fake dolt binary on
// PATH and returns the decoded JSON report. The TCP listener keeps
// server_running=true so the probe path executes; binDir is prepended to
// PATH so the stub `dolt` intercepts every SQL probe.
func runHealthJSON(t *testing.T, binDir, dataDir string, port int) healthProbeReport {
	t.Helper()
	root := repoRoot(t)
	cityPath := t.TempDir()
	if err := os.MkdirAll(filepath.Join(cityPath, ".beads"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	cmd := exec.Command("sh", filepath.Join(root, healthScript), "--json")
	cmd.Env = append(filteredEnv("PATH"),
		"PATH="+binDir+":"+os.Getenv("PATH"),
		"GC_CITY_PATH="+cityPath,
		"GC_PACK_DIR="+root,
		"GC_DOLT_DATA_DIR="+dataDir,
		"GC_DOLT_HOST=127.0.0.1",
		"GC_DOLT_PORT="+strconv.Itoa(port),
		"GC_DOLT_USER=root",
		"GC_DOLT_PASSWORD=",
		"GC_HEALTH_SKIP_ZOMBIE_SCAN=1",
	)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("health --json exited non-zero: %v\n%s", err, out)
	}
	var report healthProbeReport
	if err := json.Unmarshal(out, &report); err != nil {
		t.Fatalf("decode health JSON: %v\n%s", err, out)
	}
	return report
}

// newUserDBDataDir creates a data dir holding a single user database so
// the probe selects it as the representative-query target.
func newUserDBDataDir(t *testing.T, db string) string {
	t.Helper()
	dataDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dataDir, db, ".dolt"), 0o755); err != nil {
		t.Fatalf("mkdir user db: %v", err)
	}
	return dataDir
}

const poolRejectionMessage = "max waiting connections reached. Client rejected"

// writeRejectingDolt installs a stub dolt that fails every SQL call with
// the wait-queue rejection error real bd clients saw during the wedge.
func writeRejectingDolt(t *testing.T, binDir string) {
	t.Helper()
	writeExecutable(t, filepath.Join(binDir, "dolt"), fmt.Sprintf(`#!/usr/bin/env bash
echo %q >&2
exit 1
`, poolRejectionMessage))
}

func TestHealthScriptPoolRejectionReportsUnreachable(t *testing.T) {
	port, cleanup := startReachableTCPListener(t)
	defer cleanup()

	binDir := t.TempDir()
	writeRejectingDolt(t, binDir)

	report := runHealthJSON(t, binDir, newUserDBDataDir(t, "beads"), port)

	if !report.Server.Running {
		t.Errorf("server.running = false; TCP listener is up, want true")
	}
	if report.Server.Reachable {
		t.Errorf("server.reachable = true with a consistently-rejecting pool; want false")
	}
	if got := report.Server.Probe.Attempts; got < 3 {
		t.Errorf("probe.attempts = %d; want >= 3 (sustained evidence)", got)
	}
	if got := report.Server.Probe.Rejected; got != report.Server.Probe.Attempts {
		t.Errorf("probe.rejected = %d; want %d (every attempt rejected)", got, report.Server.Probe.Attempts)
	}
	if !report.Server.Pool.Saturated {
		t.Errorf("pool.saturated = false; rejection errors must surface as saturation")
	}
	if !strings.Contains(report.Server.Probe.LastError, "max waiting connections") {
		t.Errorf("probe.last_error = %q; want the rejection message preserved", report.Server.Probe.LastError)
	}
}

// TestHealthScriptSingleSqueezeThroughStaysUnreachable is the direct
// regression test for the 43h false-healthy: one probe squeezing through
// the wait queue while every other fresh client is rejected must NOT
// flip the verdict to healthy.
func TestHealthScriptSingleSqueezeThroughStaysUnreachable(t *testing.T) {
	port, cleanup := startReachableTCPListener(t)
	defer cleanup()

	binDir := t.TempDir()
	counter := filepath.Join(binDir, "attempts")
	writeExecutable(t, filepath.Join(binDir, "dolt"), fmt.Sprintf(`#!/usr/bin/env bash
case "$*" in
  *PROCESSLIST*)
    echo %[1]q >&2
    exit 1
    ;;
esac
n=$(cat %[2]s 2>/dev/null || echo 0)
n=$((n + 1))
printf '%%s' "$n" > %[2]s
if [ "$n" -eq 2 ]; then
  printf 'COUNT(*)\n42\n'
  exit 0
fi
echo %[1]q >&2
exit 1
`, poolRejectionMessage, shellQuote(counter)))

	report := runHealthJSON(t, binDir, newUserDBDataDir(t, "beads"), port)

	if report.Server.Reachable {
		t.Errorf("server.reachable = true on 1/%d squeeze-through; majority must be required",
			report.Server.Probe.Attempts)
	}
	if got := report.Server.Probe.Successes; got != 1 {
		t.Errorf("probe.successes = %d; fake succeeds exactly once, want 1", got)
	}
	if !report.Server.Pool.Saturated {
		t.Errorf("pool.saturated = false; majority-rejected probes must mark the pool saturated")
	}
}

func TestHealthScriptTransientBlipStaysReachableButDegraded(t *testing.T) {
	port, cleanup := startReachableTCPListener(t)
	defer cleanup()

	binDir := t.TempDir()
	counter := filepath.Join(binDir, "attempts")
	// First probe attempt fails with a generic error; everything after
	// (remaining probes, pool stats) succeeds.
	writeExecutable(t, filepath.Join(binDir, "dolt"), fmt.Sprintf(`#!/usr/bin/env bash
case "$*" in
  *PROCESSLIST*)
    printf 'active,max_conn\n7,1000\n'
    exit 0
    ;;
esac
n=$(cat %[1]s 2>/dev/null || echo 0)
n=$((n + 1))
printf '%%s' "$n" > %[1]s
if [ "$n" -eq 1 ]; then
  echo "connection reset by peer" >&2
  exit 1
fi
printf 'COUNT(*)\n42\n'
exit 0
`, shellQuote(counter)))

	report := runHealthJSON(t, binDir, newUserDBDataDir(t, "beads"), port)

	if !report.Server.Reachable {
		t.Errorf("server.reachable = false after one transient blip; majority succeeded, want true")
	}
	if !report.Server.Degraded {
		t.Errorf("server.degraded = false; a failed attempt on a reachable server must surface as degraded")
	}
	if got := report.Server.Probe.Errors; got != 1 {
		t.Errorf("probe.errors = %d; want 1", got)
	}
	if report.Server.Pool.Saturated {
		t.Errorf("pool.saturated = true; generic blip with healthy pool stats must not mark saturation")
	}
}

func TestHealthScriptHealthyProbeReportsPoolStats(t *testing.T) {
	port, cleanup := startReachableTCPListener(t)
	defer cleanup()

	binDir := t.TempDir()
	writeExecutable(t, filepath.Join(binDir, "dolt"), `#!/usr/bin/env bash
case "$*" in
  *PROCESSLIST*)
    printf 'active,max_conn\n7,1000\n'
    exit 0
    ;;
esac
printf 'COUNT(*)\n42\n'
exit 0
`)

	report := runHealthJSON(t, binDir, newUserDBDataDir(t, "beads"), port)

	if !report.Server.Reachable {
		t.Errorf("server.reachable = false on an all-success probe; want true")
	}
	if report.Server.Degraded {
		t.Errorf("server.degraded = true on a healthy server; want false")
	}
	if got := report.Server.Probe.Successes; got != report.Server.Probe.Attempts {
		t.Errorf("probe.successes = %d; want all %d attempts", got, report.Server.Probe.Attempts)
	}
	if got := report.Server.Pool.ActiveConnections; got != 7 {
		t.Errorf("pool.active_connections = %d; want 7", got)
	}
	if got := report.Server.Pool.MaxConnections; got != 1000 {
		t.Errorf("pool.max_connections = %d; want 1000", got)
	}
	if report.Server.Pool.Saturated {
		t.Errorf("pool.saturated = true at 7/1000 connections; want false")
	}
	if !report.Server.Pool.ProbeOK {
		t.Errorf("pool.probe_ok = false; pool stats query succeeded, want true")
	}
}

// TestHealthScriptPoolSaturationByCountIsDegraded covers the wedge shape
// where probes still squeeze through but the pool is nearly exhausted:
// active connections at >=90%% of max must mark saturation and degrade
// the verdict even though every probe attempt succeeded.
func TestHealthScriptPoolSaturationByCountIsDegraded(t *testing.T) {
	port, cleanup := startReachableTCPListener(t)
	defer cleanup()

	binDir := t.TempDir()
	writeExecutable(t, filepath.Join(binDir, "dolt"), `#!/usr/bin/env bash
case "$*" in
  *PROCESSLIST*)
    printf 'active,max_conn\n950,1000\n'
    exit 0
    ;;
esac
printf 'COUNT(*)\n42\n'
exit 0
`)

	report := runHealthJSON(t, binDir, newUserDBDataDir(t, "beads"), port)

	if !report.Server.Reachable {
		t.Errorf("server.reachable = false; probes all succeeded, want true")
	}
	if !report.Server.Pool.Saturated {
		t.Errorf("pool.saturated = false at 950/1000 connections; want true")
	}
	if !report.Server.Degraded {
		t.Errorf("server.degraded = false with a saturated pool; want true")
	}
}

// TestHealthScriptProbeQueriesRepresentativeDatabase asserts the verdict
// probe exercises the same path real bd clients use: a fresh client
// invocation per attempt running a storage-touching query (dolt_log)
// against an actual user database — not a bare SELECT 1 that proves only
// that the SQL engine can echo a constant.
func TestHealthScriptProbeQueriesRepresentativeDatabase(t *testing.T) {
	port, cleanup := startReachableTCPListener(t)
	defer cleanup()

	binDir := t.TempDir()
	argvLog := filepath.Join(binDir, "argv.log")
	writeExecutable(t, filepath.Join(binDir, "dolt"), fmt.Sprintf(`#!/usr/bin/env bash
printf '%%s\n' "$*" >> %s
case "$*" in
  *PROCESSLIST*)
    printf 'active,max_conn\n7,1000\n'
    exit 0
    ;;
esac
printf 'COUNT(*)\n42\n'
exit 0
`, shellQuote(argvLog)))

	report := runHealthJSON(t, binDir, newUserDBDataDir(t, "beads"), port)

	if got := report.Server.Probe.Database; got != "beads" {
		t.Errorf("probe.database = %q; want %q", got, "beads")
	}

	data, err := os.ReadFile(argvLog)
	if err != nil {
		t.Fatalf("read argv log: %v", err)
	}
	var probeCalls int
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if strings.Contains(line, "dolt_log") && strings.Contains(line, "beads") {
			probeCalls++
		}
	}
	// 3 verdict attempts + 1 inventory count for the db; require at
	// least the 3 connect-fresh verdict attempts.
	if probeCalls < 3 {
		t.Errorf("representative probe invocations = %d; want >= 3 connect-fresh attempts querying dolt_log on %q\nargv log:\n%s",
			probeCalls, "beads", data)
	}
}

// TestHealthScriptFreshCityFallsBackToSelectOne keeps the probe working
// before any user database exists: with nothing on disk to query, the
// probe must still attempt connectivity rather than skipping or failing.
func TestHealthScriptFreshCityFallsBackToSelectOne(t *testing.T) {
	port, cleanup := startReachableTCPListener(t)
	defer cleanup()

	binDir := t.TempDir()
	writeExecutable(t, filepath.Join(binDir, "dolt"), `#!/usr/bin/env bash
case "$*" in
  *PROCESSLIST*)
    printf 'active,max_conn\n1,1000\n'
    exit 0
    ;;
esac
printf '1\n1\n'
exit 0
`)

	report := runHealthJSON(t, binDir, t.TempDir(), port)

	if !report.Server.Reachable {
		t.Errorf("server.reachable = false on a fresh city with healthy server; want true")
	}
	if got := report.Server.Probe.Database; got != "" {
		t.Errorf("probe.database = %q on a fresh city; want empty", got)
	}
	if got := report.Server.Probe.Successes; got == 0 {
		t.Errorf("probe.successes = 0; SELECT 1 fallback should have run")
	}
}

// TestHealthScriptDatabaseProbeFailureIsNotZeroCommits pins the
// inventory-loop fix: a per-database probe failure must be reported as
// probe_ok=false (and degrade the verdict), not masked as a healthy-looking
// "0 commits" row — during the wedge that masking made saturated databases
// indistinguishable from empty ones.
func TestHealthScriptDatabaseProbeFailureIsNotZeroCommits(t *testing.T) {
	port, cleanup := startReachableTCPListener(t)
	defer cleanup()

	binDir := t.TempDir()
	// Verdict probes against db "aaa" succeed; the inventory count for
	// db "zzz" fails. Alphabetical enumeration makes "aaa" the
	// representative db, so the zzz failure exercises only the
	// inventory path.
	writeExecutable(t, filepath.Join(binDir, "dolt"), `#!/usr/bin/env bash
case "$*" in
  *PROCESSLIST*)
    printf 'active,max_conn\n7,1000\n'
    exit 0
    ;;
  *zzz*)
    echo "max waiting connections reached. Client rejected" >&2
    exit 1
    ;;
esac
printf 'COUNT(*)\n42\n'
exit 0
`)

	dataDir := newUserDBDataDir(t, "aaa")
	if err := os.MkdirAll(filepath.Join(dataDir, "zzz", ".dolt"), 0o755); err != nil {
		t.Fatalf("mkdir second db: %v", err)
	}

	report := runHealthJSON(t, binDir, dataDir, port)

	if !report.Server.Reachable {
		t.Fatalf("server.reachable = false; verdict probes succeed, want true")
	}
	var sawFailed, sawOK bool
	for _, db := range report.Databases {
		switch db.Name {
		case "zzz":
			if db.ProbeOK {
				t.Errorf("databases[zzz].probe_ok = true; count query failed, want false")
			}
			sawFailed = true
		case "aaa":
			if !db.ProbeOK {
				t.Errorf("databases[aaa].probe_ok = false; count query succeeded, want true")
			}
			if db.Commits != 42 {
				t.Errorf("databases[aaa].commits = %d; want 42", db.Commits)
			}
			sawOK = true
		}
	}
	if !sawFailed || !sawOK {
		t.Fatalf("databases missing expected entries (sawFailed=%v sawOK=%v): %+v", sawFailed, sawOK, report.Databases)
	}
	if !report.Server.Degraded {
		t.Errorf("server.degraded = false with a failing database probe; want true")
	}
}

// TestHealthScriptHumanModeSurfacesProbeAndPool asserts the operator-facing
// text output carries the new probe/pool detail and that the exit code
// fails on a rejecting pool.
func TestHealthScriptHumanModeSurfacesProbeAndPool(t *testing.T) {
	port, cleanup := startReachableTCPListener(t)
	defer cleanup()

	binDir := t.TempDir()
	writeRejectingDolt(t, binDir)

	root := repoRoot(t)
	cityPath := t.TempDir()
	if err := os.MkdirAll(filepath.Join(cityPath, ".beads"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	cmd := exec.Command("sh", filepath.Join(root, healthScript))
	cmd.Env = append(filteredEnv("PATH"),
		"PATH="+binDir+":"+os.Getenv("PATH"),
		"GC_CITY_PATH="+cityPath,
		"GC_PACK_DIR="+root,
		"GC_DOLT_DATA_DIR="+newUserDBDataDir(t, "beads"),
		"GC_DOLT_HOST=127.0.0.1",
		"GC_DOLT_PORT="+strconv.Itoa(port),
		"GC_DOLT_USER=root",
		"GC_DOLT_PASSWORD=",
		"GC_HEALTH_SKIP_ZOMBIE_SCAN=1",
	)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("human-mode health exited 0 against a rejecting pool; want non-zero\n%s", out)
	}
	text := string(out)
	if !strings.Contains(text, "Probe:") {
		t.Errorf("human output missing Probe: line:\n%s", text)
	}
	if !strings.Contains(text, "rejected=") {
		t.Errorf("human output missing rejection classification:\n%s", text)
	}
}
