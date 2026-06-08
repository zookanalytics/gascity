package dolt_test

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// writeDoctorProbeDolt installs a `dolt` shim that satisfies the doctor's
// read-only probes: active_branch() (server reachable), PROCESSLIST count
// (a single, well-under-threshold connection), and SHOW DATABASES. userDBs
// are extra database names returned by SHOW DATABASES (used to inject orphan
// databases such as "testdb_x"). The probe always returns instantly, so
// measured latency is a handful of milliseconds — well under the default
// 1000ms threshold unless the test forces a warn via GC_DOCTOR_LATENCY_WARN_S.
func writeDoctorProbeDolt(t *testing.T, binDir string, userDBs ...string) {
	t.Helper()
	showDBs := "Database"
	for _, db := range userDBs {
		showDBs += "\\n" + db
	}
	script := fmt.Sprintf(`#!/usr/bin/env bash
set -euo pipefail
case "$*" in
  *"SELECT active_branch()"*)
    printf 'active_branch()\nmain\n'
    exit 0
    ;;
  *"COUNT(*) FROM information_schema.PROCESSLIST"*)
    printf 'COUNT(*)\n1\n'
    exit 0
    ;;
  *"SHOW DATABASES"*)
    printf '%s\n'
    exit 0
    ;;
esac
exit 0
`, showDBs)
	writeExecutable(t, filepath.Join(binDir, "dolt"), script)
}

// countAdvisories returns how many MEDIUM Dolt health advisory mails the
// fake gc recorded in its log.
func countAdvisories(t *testing.T, gcLogPath string) int {
	t.Helper()
	data, err := os.ReadFile(gcLogPath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0
		}
		t.Fatalf("read gc log: %v", err)
	}
	return strings.Count(string(data), "Dolt health advisory [MEDIUM]")
}

// backdateAdvisoryState rewrites the recorded last-sent epoch in the doctor's
// advisory-coalescing state file to the distant past, simulating a cooldown
// window that has fully elapsed without coupling the test to the signature
// token scheme.
func backdateAdvisoryState(t *testing.T, statePath string) {
	t.Helper()
	data, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatalf("read advisory state %s: %v", statePath, err)
	}
	re := regexp.MustCompile(`"last_sent_epoch_s":[0-9]+`)
	if !re.Match(data) {
		t.Fatalf("advisory state missing last_sent_epoch_s, got:\n%s", data)
	}
	updated := re.ReplaceAll(data, []byte(`"last_sent_epoch_s":1`))
	if err := os.WriteFile(statePath, updated, 0o644); err != nil {
		t.Fatalf("rewrite advisory state: %v", err)
	}
}

// TestDoctorScriptCoalescesRepeatedAdvisories is the core self-DoS regression:
// a borderline condition that persists across health-check ticks must produce
// at most one advisory mail per cooldown window, not one per tick. Each mail
// is a Dolt write, so the un-coalesced per-tick flood piled write load onto
// the data plane precisely when it was already latency-stressed.
func TestDoctorScriptCoalescesRepeatedAdvisories(t *testing.T) {
	cityPath := t.TempDir()
	dataDir := filepath.Join(cityPath, "dolt-data")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatalf("mkdir data dir: %v", err)
	}

	binDir := t.TempDir()
	gcLogPath := writeDogFakeGC(t, binDir)
	writeDoctorProbeDolt(t, binDir)

	statePath := filepath.Join(cityPath, "advisory-state.json")
	env := []string{
		"GC_DOCTOR_LATENCY_WARN_S=0", // force the latency WARN on every tick
		"GC_DOCTOR_ADVISORY_COOLDOWN_S=3600",
		"GC_DOCTOR_ADVISORY_STATE_FILE=" + statePath,
	}

	// Five consecutive ticks with the identical borderline condition.
	for i := 0; i < 5; i++ {
		runDogScript(t, "mol-dog-doctor.sh", binDir, cityPath, dataDir, env...)
	}

	if got := countAdvisories(t, gcLogPath); got != 1 {
		log, _ := os.ReadFile(gcLogPath)
		t.Fatalf("expected exactly 1 advisory across 5 identical ticks, got %d\nlog:\n%s", got, log)
	}
}

// TestDoctorScriptReAdvisesAfterCooldown verifies the cooldown is a
// re-notify window, not a permanent mute: once it elapses, a still-present
// condition is re-announced so the operator learns the problem persists.
func TestDoctorScriptReAdvisesAfterCooldown(t *testing.T) {
	cityPath := t.TempDir()
	dataDir := filepath.Join(cityPath, "dolt-data")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatalf("mkdir data dir: %v", err)
	}

	binDir := t.TempDir()
	gcLogPath := writeDogFakeGC(t, binDir)
	writeDoctorProbeDolt(t, binDir)

	statePath := filepath.Join(cityPath, "advisory-state.json")
	env := []string{
		"GC_DOCTOR_LATENCY_WARN_S=0",
		"GC_DOCTOR_ADVISORY_COOLDOWN_S=3600",
		"GC_DOCTOR_ADVISORY_STATE_FILE=" + statePath,
	}

	runDogScript(t, "mol-dog-doctor.sh", binDir, cityPath, dataDir, env...)
	if got := countAdvisories(t, gcLogPath); got != 1 {
		t.Fatalf("expected 1 advisory after first tick, got %d", got)
	}

	// Within the cooldown the identical condition is suppressed.
	runDogScript(t, "mol-dog-doctor.sh", binDir, cityPath, dataDir, env...)
	if got := countAdvisories(t, gcLogPath); got != 1 {
		t.Fatalf("expected advisory suppressed inside cooldown, got %d", got)
	}

	// Age the last-sent timestamp past the cooldown; the next tick re-alerts.
	backdateAdvisoryState(t, statePath)
	runDogScript(t, "mol-dog-doctor.sh", binDir, cityPath, dataDir, env...)
	if got := countAdvisories(t, gcLogPath); got != 2 {
		log, _ := os.ReadFile(gcLogPath)
		t.Fatalf("expected re-advisory after cooldown elapsed, got %d\nlog:\n%s", got, log)
	}
}

// TestDoctorScriptReAdvisesWhenConditionChanges verifies the cooldown does not
// mask a genuinely new problem: when the set of active warnings changes (a new
// failure category appears), the advisory fires immediately even inside the
// cooldown window — edge-trigger on transition, not blind rate-limiting.
func TestDoctorScriptReAdvisesWhenConditionChanges(t *testing.T) {
	cityPath := t.TempDir()
	dataDir := filepath.Join(cityPath, "dolt-data")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatalf("mkdir data dir: %v", err)
	}

	binDir := t.TempDir()
	gcLogPath := writeDogFakeGC(t, binDir)

	statePath := filepath.Join(cityPath, "advisory-state.json")
	env := []string{
		"GC_DOCTOR_LATENCY_WARN_S=0",
		"GC_DOCTOR_ADVISORY_COOLDOWN_S=3600",
		"GC_DOCTOR_ADVISORY_STATE_FILE=" + statePath,
	}

	// Tick 1: latency warn only.
	writeDoctorProbeDolt(t, binDir)
	runDogScript(t, "mol-dog-doctor.sh", binDir, cityPath, dataDir, env...)
	if got := countAdvisories(t, gcLogPath); got != 1 {
		t.Fatalf("expected 1 advisory after first tick, got %d", got)
	}

	// Tick 2: latency warn AND a new orphan-database warn — different
	// condition signature, so it must re-alert despite the cooldown.
	writeDoctorProbeDolt(t, binDir, "testdb_orphan")
	runDogScript(t, "mol-dog-doctor.sh", binDir, cityPath, dataDir, env...)
	if got := countAdvisories(t, gcLogPath); got != 2 {
		log, _ := os.ReadFile(gcLogPath)
		t.Fatalf("expected re-advisory when warning set changed, got %d\nlog:\n%s", got, log)
	}
}

// TestDoctorScriptAnnouncesRecoveryOnce verifies hysteresis on the falling
// edge: when a previously-warning condition clears, the operator gets exactly
// one "cleared" advisory, and subsequent healthy ticks stay silent.
func TestDoctorScriptAnnouncesRecoveryOnce(t *testing.T) {
	cityPath := t.TempDir()
	dataDir := filepath.Join(cityPath, "dolt-data")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatalf("mkdir data dir: %v", err)
	}

	binDir := t.TempDir()
	gcLogPath := writeDogFakeGC(t, binDir)
	writeDoctorProbeDolt(t, binDir)

	statePath := filepath.Join(cityPath, "advisory-state.json")
	warnEnv := []string{
		"GC_DOCTOR_LATENCY_WARN_S=0",
		"GC_DOCTOR_ADVISORY_COOLDOWN_S=3600",
		"GC_DOCTOR_ADVISORY_STATE_FILE=" + statePath,
	}
	healthyEnv := []string{
		// No forced latency warn: the instant probe stays well under the
		// default threshold, so the condition is healthy.
		"GC_DOCTOR_ADVISORY_COOLDOWN_S=3600",
		"GC_DOCTOR_ADVISORY_STATE_FILE=" + statePath,
	}

	// Warn, then recover, then stay healthy.
	runDogScript(t, "mol-dog-doctor.sh", binDir, cityPath, dataDir, warnEnv...)
	runDogScript(t, "mol-dog-doctor.sh", binDir, cityPath, dataDir, healthyEnv...)
	runDogScript(t, "mol-dog-doctor.sh", binDir, cityPath, dataDir, healthyEnv...)

	data, err := os.ReadFile(gcLogPath)
	if err != nil {
		t.Fatalf("read gc log: %v", err)
	}
	gcLog := string(data)
	if got := strings.Count(gcLog, "Dolt health advisory [MEDIUM]"); got != 1 {
		t.Fatalf("expected exactly 1 MEDIUM advisory, got %d\nlog:\n%s", got, gcLog)
	}
	if got := strings.Count(gcLog, "Dolt health advisory cleared"); got != 1 {
		t.Fatalf("expected exactly 1 cleared advisory across recovery + healthy tick, got %d\nlog:\n%s", got, gcLog)
	}
	if !strings.Contains(gcLog, "--from controller") {
		t.Fatalf("cleared advisory must also pass --from controller, log:\n%s", gcLog)
	}
}

// TestDoctorScriptSubSecondLatencyDoesNotWarn locks the #2845 millisecond
// measurement against regression: a fast (sub-second) probe must not trip the
// 1s latency boundary, so a healthy server produces no advisory mail at all.
// The pre-#2845 whole-second timing quantized a sub-second probe to 0s or 1s
// depending on whether it straddled a wall-clock tick, producing false
// latency WARNs (and MEDIUM advisory mail) at the 1s threshold.
func TestDoctorScriptSubSecondLatencyDoesNotWarn(t *testing.T) {
	cityPath := t.TempDir()
	dataDir := filepath.Join(cityPath, "dolt-data")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatalf("mkdir data dir: %v", err)
	}

	binDir := t.TempDir()
	gcLogPath := writeDogFakeGC(t, binDir)
	writeDoctorProbeDolt(t, binDir)

	// Default 1s/1000ms threshold (no GC_DOCTOR_LATENCY_WARN_S override).
	out := runDogScript(t, "mol-dog-doctor.sh", binDir, cityPath, dataDir,
		"GC_DOCTOR_ADVISORY_STATE_FILE="+filepath.Join(cityPath, "advisory-state.json"))
	if !strings.Contains(out, "server: ok") {
		t.Fatalf("doctor should report server ok, output:\n%s", out)
	}
	if got := countAdvisories(t, gcLogPath); got != 0 {
		log, _ := os.ReadFile(gcLogPath)
		t.Fatalf("sub-second latency must not trip the 1s boundary; expected 0 advisories, got %d\nlog:\n%s", got, log)
	}
}
