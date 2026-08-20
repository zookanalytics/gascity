package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// waitStatusExited encodes a normal exit with the given status, and
// waitStatusSignaled a death by signal, in the POSIX wait(2) layout Go's
// syscall.WaitStatus wraps: the low 7 bits carry the terminating signal and
// the next 8 the exit code. Building them directly is what lets the
// disposition rule be exercised over every status the kernel can report
// without spawning a process per case.
func waitStatusExited(code int) syscall.WaitStatus {
	return syscall.WaitStatus(code << 8)
}

func waitStatusSignaled(sig syscall.Signal) syscall.WaitStatus {
	return syscall.WaitStatus(sig)
}

func TestClassifyManagedDoltWaitStatus(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX wait status required")
	}
	tests := []struct {
		name   string
		status syscall.WaitStatus
		want   managedDoltChildExit
	}{
		{name: "exit 0 is a clean shutdown", status: waitStatusExited(0), want: managedDoltChildExitClean},
		{name: "the ENOSPC journal panic's exit 2 is a crash", status: waitStatusExited(2), want: managedDoltChildExitCrashed},
		{name: "any other non-zero exit is a crash", status: waitStatusExited(137), want: managedDoltChildExitCrashed},
		// `gc dolt stop` sends SIGTERM and escalates to SIGKILL; both mean
		// an external actor owns the shutdown.
		{name: "SIGTERM death is external", status: waitStatusSignaled(syscall.SIGTERM), want: managedDoltChildExitSignaled},
		{name: "SIGKILL death is external", status: waitStatusSignaled(syscall.SIGKILL), want: managedDoltChildExitSignaled},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Guard the fixtures: a mis-encoded status would make every
			// assertion below vacuous.
			if tc.want == managedDoltChildExitSignaled && !tc.status.Signaled() {
				t.Fatalf("fixture %#x does not encode a signal death", uint32(tc.status))
			}
			if tc.want != managedDoltChildExitSignaled && !tc.status.Exited() {
				t.Fatalf("fixture %#x does not encode a normal exit", uint32(tc.status))
			}
			if got := classifyManagedDoltWaitStatus(tc.status); got != tc.want {
				t.Fatalf("classifyManagedDoltWaitStatus = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestClassifyManagedDoltChildExit(t *testing.T) {
	if got := classifyManagedDoltChildExit(nil); got != managedDoltChildExitClean {
		t.Errorf("nil error = %v, want clean", got)
	}
	// A wait failure with no exit status attached — the shape a reaped or
	// unwaitable child produces. Absence of evidence is not a crash.
	if got := classifyManagedDoltChildExit(errors.New("waitid: no child processes")); got != managedDoltChildExitUnknown {
		t.Errorf("bare error = %v, want unknown", got)
	}
	if got := classifyManagedDoltChildExit(fmt.Errorf("wrapped: %w", errors.New("boom"))); got != managedDoltChildExitUnknown {
		t.Errorf("wrapped bare error = %v, want unknown", got)
	}
}

func TestManagedDoltRestartDelayBacksOffAndCaps(t *testing.T) {
	base := 2 * time.Second
	maxDelay := 30 * time.Second
	want := []time.Duration{
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
		16 * time.Second,
		30 * time.Second,
		30 * time.Second,
	}
	for i, expected := range want {
		attempt := i + 1
		if got := managedDoltRestartDelay(attempt, base, maxDelay); got != expected {
			t.Errorf("managedDoltRestartDelay(%d) = %s, want %s", attempt, got, expected)
		}
	}
	// A non-positive attempt is treated as the first one rather than
	// producing a zero delay that would spin.
	if got := managedDoltRestartDelay(0, base, maxDelay); got != base {
		t.Errorf("managedDoltRestartDelay(0) = %s, want %s", got, base)
	}
}

func TestPruneManagedDoltRestartHistoryDropsEntriesOutsideWindow(t *testing.T) {
	now := time.Date(2026, 8, 19, 9, 8, 0, 0, time.UTC)
	window := 10 * time.Minute
	history := []time.Time{
		now.Add(-30 * time.Minute), // outside
		now.Add(-11 * time.Minute), // outside
		now.Add(-9 * time.Minute),  // inside
		now.Add(-time.Second),      // inside
	}
	pruned := pruneManagedDoltRestartHistory(history, now, window)
	if len(pruned) != 2 {
		t.Fatalf("pruned %d entries, want 2: %v", len(pruned), pruned)
	}
	// A rolling window is what makes the budget survive a healthy city that
	// crashes once a day: an old restart must not consume today's budget.
	if !pruned[0].Equal(now.Add(-9*time.Minute)) || !pruned[1].Equal(now.Add(-time.Second)) {
		t.Fatalf("pruned kept the wrong entries: %v", pruned)
	}
	if len(history) != 4 {
		t.Fatalf("pruneManagedDoltRestartHistory mutated its input: %v", history)
	}
}

func TestManagedDoltRestartBudgetHonorsEnvOverride(t *testing.T) {
	t.Setenv(managedDoltRestartBudgetEnv, "")
	if got := managedDoltRestartBudget(); got != managedDoltRestartDefaultBudget {
		t.Errorf("default budget = %d, want %d", got, managedDoltRestartDefaultBudget)
	}
	t.Setenv(managedDoltRestartBudgetEnv, "2")
	if got := managedDoltRestartBudget(); got != 2 {
		t.Errorf("override budget = %d, want 2", got)
	}
	// A zero budget is a legitimate opt-out: it restores the pre-fix
	// behavior for an operator running their own supervisor.
	t.Setenv(managedDoltRestartBudgetEnv, "0")
	if got := managedDoltRestartBudget(); got != 0 {
		t.Errorf("zero budget = %d, want 0", got)
	}
	t.Setenv(managedDoltRestartBudgetEnv, "nonsense")
	if got := managedDoltRestartBudget(); got != managedDoltRestartDefaultBudget {
		t.Errorf("unparsable budget = %d, want the default %d", got, managedDoltRestartDefaultBudget)
	}
	t.Setenv(managedDoltRestartBudgetEnv, "-3")
	if got := managedDoltRestartBudget(); got != managedDoltRestartDefaultBudget {
		t.Errorf("negative budget = %d, want the default %d", got, managedDoltRestartDefaultBudget)
	}
}

func TestManagedDoltRestartBaseDelayHonorsEnvOverride(t *testing.T) {
	t.Setenv(managedDoltRestartDelayEnv, "")
	if got := managedDoltRestartBaseDelay(); got != managedDoltRestartDefaultBaseDelay {
		t.Errorf("default base delay = %s, want %s", got, managedDoltRestartDefaultBaseDelay)
	}
	t.Setenv(managedDoltRestartDelayEnv, "25")
	if got := managedDoltRestartBaseDelay(); got != 25*time.Millisecond {
		t.Errorf("override base delay = %s, want 25ms", got)
	}
	t.Setenv(managedDoltRestartDelayEnv, "0")
	if got := managedDoltRestartBaseDelay(); got != managedDoltRestartDefaultBaseDelay {
		t.Errorf("zero base delay = %s, want the default %s", got, managedDoltRestartDefaultBaseDelay)
	}
}

func TestRefreshManagedDoltRuntimePIDRecord(t *testing.T) {
	dir := t.TempDir()
	stateFile := filepath.Join(dir, "dolt-state.json")
	pidFile := filepath.Join(dir, "dolt.pid")
	cityPath := filepath.Join(dir, "city")
	if err := os.MkdirAll(cityPath, 0o755); err != nil {
		t.Fatalf("make city dir: %v", err)
	}
	t.Setenv("GC_PACK_STATE_DIR", dir)
	t.Setenv("GC_DOLT_STATE_FILE", stateFile)
	t.Setenv("GC_DOLT_PID_FILE", pidFile)

	original := doltRuntimeState{Running: true, PID: 5893, Port: 3307, DataDir: filepath.Join(cityPath, ".beads", "dolt"), StartedAt: "2026-08-19T09:00:00Z"}
	if err := writeDoltRuntimeStateFile(stateFile, original); err != nil {
		t.Fatalf("seed state: %v", err)
	}
	if err := os.WriteFile(pidFile, []byte("5893\n"), 0o644); err != nil {
		t.Fatalf("seed pid file: %v", err)
	}

	if err := refreshManagedDoltRuntimePIDRecord(cityPath, 5893, 6042); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	got, err := readDoltRuntimeStateFile(stateFile)
	if err != nil {
		t.Fatalf("read state: %v", err)
	}
	if got.PID != 6042 {
		t.Errorf("state pid = %d, want the restarted pid 6042", got.PID)
	}
	if !got.Running {
		t.Error("state running = false, want true after a successful restart")
	}
	// Port and data dir belong to the parent that wrote the record; the
	// restart changes only which process serves them.
	if got.Port != original.Port || got.DataDir != original.DataDir {
		t.Errorf("refresh clobbered port/data dir: %+v", got)
	}
	if got.StartedAt == original.StartedAt {
		t.Error("started_at not advanced; a restarted server has a new start time")
	}
	pidData, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatalf("read pid file: %v", err)
	}
	if strings.TrimSpace(string(pidData)) != "6042" {
		t.Errorf("pid file = %q, want 6042", strings.TrimSpace(string(pidData)))
	}
}

func TestRefreshManagedDoltRuntimePIDRecordLeavesAnotherOwnersRecordAlone(t *testing.T) {
	dir := t.TempDir()
	stateFile := filepath.Join(dir, "dolt-state.json")
	pidFile := filepath.Join(dir, "dolt.pid")
	cityPath := filepath.Join(dir, "city")
	if err := os.MkdirAll(cityPath, 0o755); err != nil {
		t.Fatalf("make city dir: %v", err)
	}
	t.Setenv("GC_PACK_STATE_DIR", dir)
	t.Setenv("GC_DOLT_STATE_FILE", stateFile)
	t.Setenv("GC_DOLT_PID_FILE", pidFile)

	// Another gc started a fresh server and owns the record now. Our
	// restart must not overwrite it — that would hide a live server.
	foreign := doltRuntimeState{Running: true, PID: 7777, Port: 3307, DataDir: filepath.Join(cityPath, ".beads", "dolt"), StartedAt: "2026-08-19T16:07:15Z"}
	if err := writeDoltRuntimeStateFile(stateFile, foreign); err != nil {
		t.Fatalf("seed state: %v", err)
	}

	if err := refreshManagedDoltRuntimePIDRecord(cityPath, 5893, 6042); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	got, err := readDoltRuntimeStateFile(stateFile)
	if err != nil {
		t.Fatalf("read state: %v", err)
	}
	if got.PID != 7777 {
		t.Errorf("state pid = %d, want the foreign owner's 7777 left intact", got.PID)
	}
	if _, err := os.Stat(pidFile); !os.IsNotExist(err) {
		t.Errorf("pid file written for a record we do not own (err=%v)", err)
	}
}

// writeCrashingFakeDoltSQLServer writes a fake `dolt` whose sql-server
// subcommand crashes (exit 2, the ENOSPC panic's status) on its first
// crashTimes invocations and then stays up. Every invocation appends its own
// PID to <stateDir>/pids so a test can count restarts independently of the
// watchdog's log format.
func writeCrashingFakeDoltSQLServer(t *testing.T, stateDir string, crashTimes int) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("shell fake requires POSIX sh")
	}
	dir := t.TempDir()
	script := filepath.Join(dir, "dolt")
	content := "#!/bin/sh\n" +
		"if [ \"$1\" != \"sql-server\" ]; then\n" +
		"  echo \"unexpected dolt args: $*\" >&2\n" +
		"  exit 2\n" +
		"fi\n" +
		"d=" + shellQuotePOSIXPath(stateDir) + "\n" +
		"n=$(cat \"$d/count\" 2>/dev/null || echo 0)\n" +
		"n=$((n + 1))\n" +
		"printf '%s\\n' \"$n\" > \"$d/count\"\n" +
		"printf '%s\\n' \"$$\" >> \"$d/pids\"\n" +
		"if [ \"$n\" -le " + strconv.Itoa(crashTimes) + " ]; then\n" +
		"  echo \"fake dolt: fatal error writing to journal (run $n)\" >&2\n" +
		"  exit 2\n" +
		"fi\n" +
		"exec sleep 60\n"
	if err := os.WriteFile(script, []byte(content), 0o755); err != nil {
		t.Fatalf("write crashing fake dolt: %v", err)
	}
	return dir
}

// readFakeDoltPIDs returns the PIDs recorded by writeCrashingFakeDoltSQLServer,
// one per sql-server invocation, oldest first.
func readFakeDoltPIDs(t *testing.T, stateDir string) []int {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(stateDir, "pids"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatalf("read fake dolt pids: %v", err)
	}
	var pids []int
	for _, line := range strings.Fields(string(data)) {
		pid, err := strconv.Atoi(strings.TrimSpace(line))
		if err != nil {
			t.Fatalf("parse fake dolt pid %q: %v", line, err)
		}
		pids = append(pids, pid)
	}
	return pids
}

// waitForFakeDoltStarts blocks until the fake dolt has been invoked at least
// want times, or the timeout passes, and returns whatever it saw.
func waitForFakeDoltStarts(t *testing.T, stateDir string, want int, timeout time.Duration) []int {
	t.Helper()
	waitForManagedDoltScopeCondition(timeout, func() bool {
		return len(readFakeDoltPIDs(t, stateDir)) >= want
	})
	return readFakeDoltPIDs(t, stateDir)
}

// waitForManagedDoltPIDExit reports whether pid is gone within the timeout.
func waitForManagedDoltPIDExit(t *testing.T, pid int, timeout time.Duration) bool {
	t.Helper()
	return waitForManagedDoltScopeCondition(timeout, func() bool { return !pidAlive(pid) })
}

// startScopeWatchdogHelper spawns the shared scope-watchdog helper process
// with the restart knobs wired, and returns the first dolt PID, the watchdog
// PID it reported, and the path to the watchdog's log.
func startScopeWatchdogHelper(t *testing.T, dir, fakeDoltDir string, extraEnv ...string) (int, int, string) {
	t.Helper()
	statePath := filepath.Join(dir, "state")
	configPath := filepath.Join(dir, "dolt-config.yaml")
	logPath := filepath.Join(dir, "dolt.log")
	if err := os.WriteFile(configPath, []byte("log_level: debug\n"), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	runManagedDoltScopeWatchdogHelper(t, append(sanitizedBaseEnv(
		"GC_TEST_MANAGED_DOLT_HELPER=scope-watchdog",
		"GC_TEST_MANAGED_DOLT_HELPER_STATE="+statePath,
		"GC_TEST_MANAGED_DOLT_HELPER_CONFIG="+configPath,
		"GC_TEST_MANAGED_DOLT_HELPER_LOG="+logPath,
		"GC_TEST_MANAGED_DOLT_HELPER_FAKE_DOLT_DIR="+fakeDoltDir,
		"GC_TEST_MANAGED_DOLT_HELPER_SCOPE_WD_INTERVAL_MS=50",
	), extraEnv...))
	doltPID, watchdogPID := readManagedDoltTestState(t, statePath)
	return doltPID, watchdogPID, logPath
}

// TestManagedDoltScopeWatchdogRestartsCrashedServer is the gc-zl5ta
// regression: a mid-lifetime crash used to leave the scope with a dead
// server and an exited watchdog until some later gc invocation noticed. On
// 2026-08-19 that turned a transient ENOSPC panic into a 6h58m city-wide
// data-plane outage.
func TestManagedDoltScopeWatchdogRestartsCrashedServer(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX process semantics required")
	}
	dir := t.TempDir()
	fakeState := filepath.Join(dir, "fake")
	if err := os.MkdirAll(fakeState, 0o755); err != nil {
		t.Fatalf("make fake state dir: %v", err)
	}
	fakeDoltDir := writeCrashingFakeDoltSQLServer(t, fakeState, 1)
	_, watchdogPID, logPath := startScopeWatchdogHelper(t, dir, fakeDoltDir,
		"GC_TEST_MANAGED_DOLT_HELPER_RESTART_DELAY_MS=25",
		"GC_TEST_MANAGED_DOLT_HELPER_RESTART_BUDGET=5",
	)

	pids := waitForFakeDoltStarts(t, fakeState, 2, 15*time.Second)
	t.Cleanup(func() {
		for _, pid := range pids {
			cleanupManagedDoltTestPID(t, pid)
		}
		cleanupManagedDoltTestPID(t, watchdogPID)
	})
	if len(pids) < 2 {
		logData, _ := os.ReadFile(logPath)
		t.Fatalf("fake dolt started %d times, want a restart after the crash; watchdog log:\n%s", len(pids), logData)
	}
	if !pidAlive(pids[1]) {
		t.Fatalf("restarted fake dolt pid %d is not alive", pids[1])
	}
	if !pidAlive(watchdogPID) {
		logData, _ := os.ReadFile(logPath)
		t.Fatalf("watchdog pid %d exited instead of supervising the restarted server; log:\n%s", watchdogPID, logData)
	}
	logData, _ := os.ReadFile(logPath)
	if !strings.Contains(string(logData), "restarted dolt sql-server") {
		t.Errorf("watchdog log has no restart record; log:\n%s", logData)
	}
}

// TestManagedDoltScopeWatchdogDoesNotRestartSignaledServer guards the
// stop path. `gc dolt stop` signals the dolt PID directly and never touches
// the watchdog, so a supervisor that restarts a signaled child makes the
// managed server unstoppable.
func TestManagedDoltScopeWatchdogDoesNotRestartSignaledServer(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX process semantics required")
	}
	dir := t.TempDir()
	fakeState := filepath.Join(dir, "fake")
	if err := os.MkdirAll(fakeState, 0o755); err != nil {
		t.Fatalf("make fake state dir: %v", err)
	}
	fakeDoltDir := writeCrashingFakeDoltSQLServer(t, fakeState, 0)
	doltPID, watchdogPID, logPath := startScopeWatchdogHelper(t, dir, fakeDoltDir,
		"GC_TEST_MANAGED_DOLT_HELPER_RESTART_DELAY_MS=25",
		"GC_TEST_MANAGED_DOLT_HELPER_RESTART_BUDGET=5",
	)
	t.Cleanup(func() {
		for _, pid := range readFakeDoltPIDs(t, fakeState) {
			cleanupManagedDoltTestPID(t, pid)
		}
		cleanupManagedDoltTestPID(t, watchdogPID)
	})

	if err := syscall.Kill(doltPID, syscall.SIGKILL); err != nil {
		t.Fatalf("kill fake dolt: %v", err)
	}
	if !waitForManagedDoltPIDExit(t, watchdogPID, 15*time.Second) {
		logData, _ := os.ReadFile(logPath)
		t.Fatalf("watchdog pid %d survived an externally signaled server; log:\n%s", watchdogPID, logData)
	}
	// Give a would-be restart every chance to show up before asserting.
	pids := waitForFakeDoltStarts(t, fakeState, 2, 2*time.Second)
	if len(pids) != 1 {
		logData, _ := os.ReadFile(logPath)
		t.Fatalf("fake dolt started %d times, want exactly 1 (no restart after an external kill); log:\n%s", len(pids), logData)
	}
	logData, _ := os.ReadFile(logPath)
	if !strings.Contains(string(logData), "terminated by a signal") {
		t.Errorf("watchdog log does not record the external termination; log:\n%s", logData)
	}
}

// TestManagedDoltScopeWatchdogGivesUpWhenRestartBudgetExhausted proves the
// restart loop is bounded. An unbounded one trades a dead server for a
// crash loop — the failure mode gc-x1a87 hit at 83439 restarts.
func TestManagedDoltScopeWatchdogGivesUpWhenRestartBudgetExhausted(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX process semantics required")
	}
	dir := t.TempDir()
	fakeState := filepath.Join(dir, "fake")
	if err := os.MkdirAll(fakeState, 0o755); err != nil {
		t.Fatalf("make fake state dir: %v", err)
	}
	fakeDoltDir := writeCrashingFakeDoltSQLServer(t, fakeState, 1000)
	_, watchdogPID, logPath := startScopeWatchdogHelper(t, dir, fakeDoltDir,
		"GC_TEST_MANAGED_DOLT_HELPER_RESTART_DELAY_MS=10",
		"GC_TEST_MANAGED_DOLT_HELPER_RESTART_BUDGET=2",
	)
	t.Cleanup(func() { cleanupManagedDoltTestPID(t, watchdogPID) })

	if !waitForManagedDoltPIDExit(t, watchdogPID, 20*time.Second) {
		logData, _ := os.ReadFile(logPath)
		t.Fatalf("watchdog pid %d never gave up on a server that crashes every time; log:\n%s", watchdogPID, logData)
	}
	pids := readFakeDoltPIDs(t, fakeState)
	if len(pids) != 3 {
		logData, _ := os.ReadFile(logPath)
		t.Fatalf("fake dolt started %d times, want 3 (initial + budget of 2); log:\n%s", len(pids), logData)
	}
	logData, _ := os.ReadFile(logPath)
	if !strings.Contains(string(logData), managedDoltRestartGaveUpMarker) {
		t.Errorf("watchdog log is missing the %q alarm marker; log:\n%s", managedDoltRestartGaveUpMarker, logData)
	}
}

// TestManagedDoltRestartGaveUpMarkerIsGreppable pins the alarm string. It is
// the one piece of evidence that survives outside the data plane, so an
// operator grep can key on it and a rename has to be deliberate.
func TestManagedDoltRestartGaveUpMarkerIsGreppable(t *testing.T) {
	if !strings.Contains(managedDoltRestartGaveUpMarker, "dolt") {
		t.Errorf("marker %q does not mention dolt", managedDoltRestartGaveUpMarker)
	}
	if strings.TrimSpace(managedDoltRestartGaveUpMarker) != managedDoltRestartGaveUpMarker {
		t.Errorf("marker %q has surrounding whitespace", managedDoltRestartGaveUpMarker)
	}
}

// TestManagedDoltScopeWatchdogRestartBudgetZeroKeepsPreFixBehavior covers the
// documented opt-out for an operator running their own supervisor: budget 0
// restores exactly what the watchdog did before crash recovery existed — log
// the crash, exit, leave the scope without a server.
//
// It is also the control for the restart tests above. The assertions here and
// the ones in TestManagedDoltScopeWatchdogRestartsCrashedServer are exact
// opposites over the same fake dolt, so a change that quietly stopped
// restarting could not satisfy both.
func TestManagedDoltScopeWatchdogRestartBudgetZeroKeepsPreFixBehavior(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX process semantics required")
	}
	dir := t.TempDir()
	fakeState := filepath.Join(dir, "fake")
	if err := os.MkdirAll(fakeState, 0o755); err != nil {
		t.Fatalf("make fake state dir: %v", err)
	}
	fakeDoltDir := writeCrashingFakeDoltSQLServer(t, fakeState, 1)
	_, watchdogPID, logPath := startScopeWatchdogHelper(t, dir, fakeDoltDir,
		"GC_TEST_MANAGED_DOLT_HELPER_RESTART_DELAY_MS=25",
		"GC_TEST_MANAGED_DOLT_HELPER_RESTART_BUDGET=0",
	)
	t.Cleanup(func() {
		for _, pid := range readFakeDoltPIDs(t, fakeState) {
			cleanupManagedDoltTestPID(t, pid)
		}
		cleanupManagedDoltTestPID(t, watchdogPID)
	})

	if !waitForManagedDoltPIDExit(t, watchdogPID, 15*time.Second) {
		logData, _ := os.ReadFile(logPath)
		t.Fatalf("watchdog pid %d survived a crash with recovery opted out; log:\n%s", watchdogPID, logData)
	}
	pids := waitForFakeDoltStarts(t, fakeState, 2, 2*time.Second)
	if len(pids) != 1 {
		logData, _ := os.ReadFile(logPath)
		t.Fatalf("fake dolt started %d times with budget 0, want exactly 1; log:\n%s", len(pids), logData)
	}
	logData, _ := os.ReadFile(logPath)
	if !strings.Contains(string(logData), managedDoltRestartGaveUpMarker) {
		t.Errorf("opting out still has to alarm rather than exit silently; log:\n%s", logData)
	}
}

func TestManagedDoltRuntimeRecordSaysStopped(t *testing.T) {
	dir := t.TempDir()
	stateFile := filepath.Join(dir, "dolt-state.json")
	cityPath := filepath.Join(dir, "city")
	if err := os.MkdirAll(cityPath, 0o755); err != nil {
		t.Fatalf("make city dir: %v", err)
	}
	t.Setenv("GC_PACK_STATE_DIR", dir)
	t.Setenv("GC_DOLT_STATE_FILE", stateFile)

	// No city, no record: neither proves an operator took the scope down, so
	// neither may veto crash recovery.
	if managedDoltRuntimeRecordSaysStopped("") {
		t.Error("an empty city path must not read as a deliberate stop")
	}
	if managedDoltRuntimeRecordSaysStopped(cityPath) {
		t.Error("a missing record must not read as a deliberate stop")
	}
	if err := os.WriteFile(stateFile, []byte("{not json"), 0o644); err != nil {
		t.Fatalf("write unreadable state: %v", err)
	}
	if managedDoltRuntimeRecordSaysStopped(cityPath) {
		t.Error("an unreadable record must not read as a deliberate stop")
	}

	running := doltRuntimeState{Running: true, PID: 5893, Port: 3307, DataDir: filepath.Join(cityPath, ".beads", "dolt")}
	if err := writeDoltRuntimeStateFile(stateFile, running); err != nil {
		t.Fatalf("write running state: %v", err)
	}
	if managedDoltRuntimeRecordSaysStopped(cityPath) {
		t.Error("a record claiming a running server must not block a restart")
	}

	// This is what `gc dolt stop` leaves behind.
	stopped := doltRuntimeState{Running: false, PID: 0, Port: 3307, DataDir: running.DataDir}
	if err := writeDoltRuntimeStateFile(stateFile, stopped); err != nil {
		t.Fatalf("write stopped state: %v", err)
	}
	if !managedDoltRuntimeRecordSaysStopped(cityPath) {
		t.Error("a stopped record must block a restart")
	}
}
