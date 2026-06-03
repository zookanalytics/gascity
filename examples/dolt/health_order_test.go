package dolt_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestDoltHealthOrderIsDiagnosticOnly(t *testing.T) {
	root := repoRoot(t)
	orderPath := filepath.Join(root, "orders", "dolt-health.toml")
	data, err := os.ReadFile(orderPath)
	if err != nil {
		t.Fatalf("read dolt-health order: %v", err)
	}

	text := string(data)
	if !strings.Contains(text, `exec = "gc dolt health --json | gc dolt health-check"`) {
		t.Fatalf("dolt-health order should run bounded health JSON, got:\n%s", text)
	}
	for _, forbidden := range []string{"gc dolt start", "gc dolt status"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("dolt-health order must not call %q directly:\n%s", forbidden, text)
		}
	}
}

func TestDoltHealthCheckFailsUnreachableReportWithUsefulMessage(t *testing.T) {
	root := repoRoot(t)
	script := filepath.Join(root, "commands", "health-check", "run.sh")
	input := `{
  "server": {
    "running": true,
    "reachable": false,
    "pid": 123,
    "port": 3311,
    "latency_ms": 0
  }
}`

	cmd := exec.Command("sh", script)
	cmd.Stdin = strings.NewReader(input)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("health-check unexpectedly succeeded:\n%s", out)
	}
	for _, want := range []string{"Dolt server unreachable", "running=true", "pid=123", "port=3311"} {
		if !strings.Contains(string(out), want) {
			t.Fatalf("health-check output missing %q:\n%s", want, out)
		}
	}
}

func TestDoltHealthCheckPassesReachableReport(t *testing.T) {
	root := repoRoot(t)
	script := filepath.Join(root, "commands", "health-check", "run.sh")
	input := `{
  "server": {
    "running": true,
    "reachable": true,
    "pid": 123,
    "port": 3311,
    "latency_ms": 12
  }
}`

	cmd := exec.Command("sh", script)
	cmd.Stdin = strings.NewReader(input)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("health-check failed: %v\n%s", err, out)
	}
}

// TestDoltHealthCheckFailsRejectingPoolWithProbeDetail asserts the order
// outcome message carries the probe classification and pool counts, so an
// `order.failed` event from a saturated pool reads as "pool wedge", not a
// bare "unreachable".
func TestDoltHealthCheckFailsRejectingPoolWithProbeDetail(t *testing.T) {
	root := repoRoot(t)
	script := filepath.Join(root, "commands", "health-check", "run.sh")
	input := `{
  "server": {
    "running": true,
    "reachable": false,
    "degraded": false,
    "pid": 123,
    "port": 3311,
    "latency_ms": 0,
    "probe": {
      "attempts": 3,
      "successes": 0,
      "rejected": 3,
      "timeouts": 0,
      "errors": 0,
      "database": "beads",
      "last_error": "max waiting connections reached. Client rejected"
    },
    "pool": {
      "active_connections": 998,
      "max_connections": 1000,
      "saturated": true,
      "probe_ok": false
    }
  }
}`

	cmd := exec.Command("sh", script)
	cmd.Stdin = strings.NewReader(input)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("health-check unexpectedly succeeded against a rejecting pool:\n%s", out)
	}
	for _, want := range []string{"Dolt server unreachable", "rejected=3/3", "saturated"} {
		if !strings.Contains(string(out), want) {
			t.Fatalf("health-check output missing %q:\n%s", want, out)
		}
	}
}

// TestDoltHealthCheckPassesDegradedReportButNotes asserts a reachable-but-
// degraded report does not fail the order (anti-flap: degradation is a
// warning surfaced in-band), while still emitting a diagnostic note.
func TestDoltHealthCheckPassesDegradedReportButNotes(t *testing.T) {
	root := repoRoot(t)
	script := filepath.Join(root, "commands", "health-check", "run.sh")
	input := `{
  "server": {
    "running": true,
    "reachable": true,
    "degraded": true,
    "pid": 123,
    "port": 3311,
    "latency_ms": 4200,
    "probe": {
      "attempts": 3,
      "successes": 2,
      "rejected": 1,
      "timeouts": 0,
      "errors": 0,
      "database": "beads",
      "last_error": "max waiting connections reached. Client rejected"
    },
    "pool": {
      "active_connections": 920,
      "max_connections": 1000,
      "saturated": true,
      "probe_ok": true
    }
  }
}`

	cmd := exec.Command("sh", script)
	cmd.Stdin = strings.NewReader(input)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("health-check failed on a degraded-but-reachable report: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "degraded") {
		t.Fatalf("health-check should note degradation for diagnostics:\n%s", out)
	}
}
