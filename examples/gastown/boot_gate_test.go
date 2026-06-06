package gastown_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// bootGateScript is the path to the boot-gate exec-order script under test.
func bootGateScript() string {
	return filepath.Join(exampleDir(), "packs", "gastown", "assets", "scripts", "boot-gate.sh")
}

// rfc3339Ago renders a whole-second RFC3339 UTC timestamp `d` in the past,
// matching the format bead stores emit for updated_at/created_at.
func rfc3339Ago(d time.Duration) string {
	return time.Now().UTC().Add(-d).Format("2006-01-02T15:04:05Z")
}

// writeBootGateGCStub installs a fake `gc` on PATH that logs every call to
// $GC_CALL_LOG and answers the two `bd list` shapes the gate issues plus
// `bd create`. wispsJSON is returned for the in-progress wisp discovery
// query; existingJSON is returned for the idempotency query (boot's
// outstanding gate beads). Both default to an empty JSON array.
func writeBootGateGCStub(t *testing.T, binDir, wispsJSON, existingJSON string) {
	t.Helper()
	if strings.TrimSpace(wispsJSON) == "" {
		wispsJSON = "[]"
	}
	if strings.TrimSpace(existingJSON) == "" {
		existingJSON = "[]"
	}
	body := fmt.Sprintf(`#!/bin/sh
printf '%%s\n' "$*" >> "$GC_CALL_LOG"
if [ "$1" = "bd" ] && [ "$2" = "list" ]; then
  case "$*" in
    *"--type=wisp"*)
      cat <<'WISPS'
%s
WISPS
      exit 0 ;;
    *"--label=boot-gate"*)
      cat <<'EXISTING'
%s
EXISTING
      exit 0 ;;
    *)
      printf '[]\n' ; exit 0 ;;
  esac
fi
if [ "$1" = "bd" ] && [ "$2" = "create" ]; then
  printf '[{"id":"bg-created"}]\n' ; exit 0
fi
exit 0
`, wispsJSON, existingJSON)
	writeExecutable(t, filepath.Join(binDir, "gc"), body)
}

// runBootGate executes the gate script with the given env overrides and
// returns combined output plus the logged gc calls.
func runBootGate(t *testing.T, env map[string]string) (string, string) {
	t.Helper()
	gcLog := env["GC_CALL_LOG"]
	out, err := runScriptResult(t, bootGateScript(), env)
	if err != nil {
		t.Fatalf("boot-gate.sh failed: %v\n%s", err, out)
	}
	logData, _ := os.ReadFile(gcLog)
	return string(out), string(logData)
}

func TestBootGateFreshWispCreatesNoBead(t *testing.T) {
	binDir := t.TempDir()
	gcLog := filepath.Join(t.TempDir(), "gc.log")
	wisps := fmt.Sprintf(`[{"id":"w-1","issue_type":"wisp","status":"in_progress","assignee":"deacon","updated_at":%q}]`,
		rfc3339Ago(60*time.Second))
	writeBootGateGCStub(t, binDir, wisps, "")

	_, log := runBootGate(t, map[string]string{
		"GC_CALL_LOG":            gcLog,
		"GC_BOOT_GATE_STALENESS": "600",
		"PATH":                   binDir + string(os.PathListSeparator) + os.Getenv("PATH"),
	})

	if strings.Contains(log, "bd create") {
		t.Fatalf("fresh wisp must not create a wake bead; gc calls:\n%s", log)
	}
}

func TestBootGateStaleWispWakesBoot(t *testing.T) {
	binDir := t.TempDir()
	gcLog := filepath.Join(t.TempDir(), "gc.log")
	wisps := fmt.Sprintf(`[{"id":"w-1","issue_type":"wisp","status":"in_progress","assignee":"deacon","updated_at":%q}]`,
		rfc3339Ago(20*time.Minute))
	writeBootGateGCStub(t, binDir, wisps, "")

	out, log := runBootGate(t, map[string]string{
		"GC_CALL_LOG":            gcLog,
		"GC_BOOT_GATE_STALENESS": "600",
		"PATH":                   binDir + string(os.PathListSeparator) + os.Getenv("PATH"),
	})

	if !strings.Contains(log, "bd create") {
		t.Fatalf("stale wisp must create a wake bead; gc calls:\n%s", log)
	}
	// Named on_demand sessions wake on assignee match, not gc.routed_to.
	if !strings.Contains(log, "--assignee=boot") {
		t.Fatalf("wake bead must be assigned to boot; gc calls:\n%s", log)
	}
	if !strings.Contains(log, "--labels=boot-gate") {
		t.Fatalf("wake bead must carry the boot-gate label; gc calls:\n%s", log)
	}
	if !strings.Contains(out, "boot") {
		t.Fatalf("expected a wake summary on stdout, got:\n%s", out)
	}
}

func TestBootGateDerivesPrefixedBootIdentity(t *testing.T) {
	binDir := t.TempDir()
	gcLog := filepath.Join(t.TempDir(), "gc.log")
	// A bound import gives the deacon a "gastown." binding prefix; boot must
	// inherit the same prefix so the controller's named-demand match fires.
	wisps := fmt.Sprintf(`[{"id":"w-1","issue_type":"wisp","status":"in_progress","assignee":"gastown.deacon","updated_at":%q}]`,
		rfc3339Ago(20*time.Minute))
	writeBootGateGCStub(t, binDir, wisps, "")

	_, log := runBootGate(t, map[string]string{
		"GC_CALL_LOG":            gcLog,
		"GC_BOOT_GATE_STALENESS": "600",
		"PATH":                   binDir + string(os.PathListSeparator) + os.Getenv("PATH"),
	})

	if !strings.Contains(log, "--assignee=gastown.boot") {
		t.Fatalf("prefixed deacon must derive gastown.boot; gc calls:\n%s", log)
	}
}

func TestBootGateSkipsWhenBootBeadAlreadyOpen(t *testing.T) {
	binDir := t.TempDir()
	gcLog := filepath.Join(t.TempDir(), "gc.log")
	wisps := fmt.Sprintf(`[{"id":"w-1","issue_type":"wisp","status":"in_progress","assignee":"deacon","updated_at":%q}]`,
		rfc3339Ago(20*time.Minute))
	existing := `[{"id":"bg-old","status":"open","assignee":"boot"}]`
	writeBootGateGCStub(t, binDir, wisps, existing)

	_, log := runBootGate(t, map[string]string{
		"GC_CALL_LOG":            gcLog,
		"GC_BOOT_GATE_STALENESS": "600",
		"PATH":                   binDir + string(os.PathListSeparator) + os.Getenv("PATH"),
	})

	if strings.Contains(log, "bd create") {
		t.Fatalf("must not pile on a second wake bead; gc calls:\n%s", log)
	}
}

func TestBootGateNoInProgressWispIsNoop(t *testing.T) {
	binDir := t.TempDir()
	gcLog := filepath.Join(t.TempDir(), "gc.log")
	writeBootGateGCStub(t, binDir, "[]", "")

	_, log := runBootGate(t, map[string]string{
		"GC_CALL_LOG":            gcLog,
		"GC_BOOT_GATE_STALENESS": "600",
		"PATH":                   binDir + string(os.PathListSeparator) + os.Getenv("PATH"),
	})

	if strings.Contains(log, "bd create") {
		t.Fatalf("no deacon wisp must be a no-op (liveness is the controller's job); gc calls:\n%s", log)
	}
}

func TestBootGateHonorsIdentityOverrides(t *testing.T) {
	binDir := t.TempDir()
	gcLog := filepath.Join(t.TempDir(), "gc.log")
	// Exact-match the deacon by env, and force a non-derived boot identity.
	wisps := fmt.Sprintf(`[{"id":"w-1","issue_type":"wisp","status":"in_progress","assignee":"town.deacon","updated_at":%q}]`,
		rfc3339Ago(5*time.Minute))
	writeBootGateGCStub(t, binDir, wisps, "")

	_, log := runBootGate(t, map[string]string{
		"GC_CALL_LOG":            gcLog,
		"GC_BOOT_GATE_STALENESS": "120",
		"GC_BOOT_GATE_DEACON":    "town.deacon",
		"GC_BOOT_GATE_BOOT":      "town.boot",
		"PATH":                   binDir + string(os.PathListSeparator) + os.Getenv("PATH"),
	})

	if !strings.Contains(log, "--assignee=town.boot") {
		t.Fatalf("identity overrides must be honored; gc calls:\n%s", log)
	}
}
