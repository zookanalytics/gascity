package gastown

import (
	"io/fs"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/orders"
)

// readOrder parses an order TOML from the embedded pack FS and restores the
// Name the scanner would normally derive from the filename (Parse leaves it
// blank because Name is not a TOML field).
func readOrder(t *testing.T, file string) orders.Order {
	t.Helper()
	data, err := fs.ReadFile(PackFS, "orders/"+file)
	if err != nil {
		t.Fatalf("reading orders/%s: %v", file, err)
	}
	o, err := orders.Parse(data)
	if err != nil {
		t.Fatalf("parsing orders/%s: %v", file, err)
	}
	o.Name = strings.TrimSuffix(file, ".toml")
	return o
}

// TestGastownOrdersValidate asserts every embedded order TOML parses and passes
// structural validation, so a malformed order can never ship in the gastown
// pack bundled into the gc binary.
func TestGastownOrdersValidate(t *testing.T) {
	entries, err := fs.ReadDir(PackFS, "orders")
	if err != nil {
		t.Fatalf("reading orders dir: %v", err)
	}
	saw := false
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".toml") {
			continue
		}
		saw = true
		o := readOrder(t, e.Name())
		if err := orders.Validate(o); err != nil {
			t.Errorf("order %s failed validation: %v", e.Name(), err)
		}
	}
	if !saw {
		t.Fatal("no order TOML files found in embedded pack")
	}
}

// TestBootGateOrderIsExecCooldown pins the boot-gate order's shape: it must be
// a no-LLM exec order on a cooldown trigger that dispatches the embedded
// boot-gate.sh script and never routes to an agent pool. This is the mechanism
// that replaces boot's mode="always" per-tick revival churn — if the order
// regresses to a formula/pool dispatch it would reintroduce an LLM cold-start
// on every cooldown.
func TestBootGateOrderIsExecCooldown(t *testing.T) {
	o := readOrder(t, "boot-gate.toml")
	if err := orders.Validate(o); err != nil {
		t.Fatalf("boot-gate order failed validation: %v", err)
	}
	if !o.IsExec() {
		t.Errorf("boot-gate must dispatch via exec, got formula %q", o.Formula)
	}
	if o.Pool != "" {
		t.Errorf("boot-gate is no-LLM and must not set a pool, got %q", o.Pool)
	}
	if o.Trigger != "cooldown" {
		t.Errorf("boot-gate trigger = %q, want cooldown", o.Trigger)
	}
	if o.Scope != "city" {
		t.Errorf("boot-gate scope = %q, want city (boot and deacon are city-scoped)", o.Scope)
	}
	const wantSuffix = "assets/scripts/boot-gate.sh"
	if !strings.HasSuffix(o.Exec, wantSuffix) {
		t.Errorf("boot-gate exec = %q, want suffix %q", o.Exec, wantSuffix)
	}
	if _, err := fs.ReadFile(PackFS, wantSuffix); err != nil {
		t.Errorf("boot-gate script not embedded at %s: %v", wantSuffix, err)
	}
}
