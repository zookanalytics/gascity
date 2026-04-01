//go:build acceptance_a

// Formula and events acceptance tests.
//
// These exercise gc formula (list, show) and gc events / gc event emit
// as a black box. Formula tests use a gastown city which has formulas
// from its packs. Event tests verify emit+query round-trip against the
// file-backed event log.
package acceptance_test

import (
	"path/filepath"
	"strings"
	"testing"

	helpers "github.com/gastownhall/gascity/test/acceptance/helpers"
)

// --- gc formula list ---

// TestFormulaList_GastownCity_ShowsFormulas verifies that gc formula list
// on a gastown city discovers and lists formulas from packs.
func TestFormulaList_GastownCity_ShowsFormulas(t *testing.T) {
	c := helpers.NewCity(t, testEnv)
	c.InitFrom(filepath.Join(helpers.ExamplesDir(), "gastown"))

	out, err := c.GC("formula", "list")
	if err != nil {
		t.Fatalf("gc formula list failed: %v\n%s", err, out)
	}

	// Gastown ships many formulas — verify at least one is discovered.
	if !strings.Contains(out, "mol-") {
		t.Errorf("expected gastown formulas (mol-*) in output, got:\n%s", out)
	}
}

// TestFormulaList_TutorialCity_ListsAvailableFormulas verifies that
// gc formula list on a tutorial city works without error.
func TestFormulaList_TutorialCity_ListsAvailableFormulas(t *testing.T) {
	c := helpers.NewCity(t, testEnv)
	c.Init("claude")

	out, err := c.GC("formula", "list")
	if err != nil {
		t.Fatalf("gc formula list failed: %v\n%s", err, out)
	}
	// Tutorial city may have system formulas. The command should not crash.
	_ = out
}

// --- gc formula show ---

// TestFormulaShow_GastownFormula_DisplaysSteps verifies that gc formula show
// compiles and displays a gastown formula with its steps.
func TestFormulaShow_GastownFormula_DisplaysSteps(t *testing.T) {
	c := helpers.NewCity(t, testEnv)
	c.InitFrom(filepath.Join(helpers.ExamplesDir(), "gastown"))

	// List formulas first to get a real name.
	listOut, err := c.GC("formula", "list")
	if err != nil {
		t.Fatalf("gc formula list failed: %v\n%s", err, listOut)
	}

	// Pick the first formula.
	lines := strings.Split(strings.TrimSpace(listOut), "\n")
	if len(lines) == 0 || lines[0] == "" || strings.Contains(lines[0], "No formula") {
		t.Skip("no formulas available to show")
	}
	formulaName := strings.TrimSpace(lines[0])

	out, err := c.GC("formula", "show", formulaName)
	if err != nil {
		t.Fatalf("gc formula show %s failed: %v\n%s", formulaName, err, out)
	}

	if !strings.Contains(out, "Formula:") {
		t.Errorf("expected 'Formula:' header in output, got:\n%s", out)
	}
	if !strings.Contains(out, "Steps") {
		t.Errorf("expected 'Steps' section in output, got:\n%s", out)
	}
}

// TestFormulaShow_NonexistentFormula_ReturnsError verifies that showing
// a formula that doesn't exist returns an error.
func TestFormulaShow_NonexistentFormula_ReturnsError(t *testing.T) {
	c := helpers.NewCity(t, testEnv)
	c.Init("claude")

	_, err := c.GC("formula", "show", "mol-nonexistent-formula-xyz")
	if err == nil {
		t.Fatal("expected error for nonexistent formula, got success")
	}
}

// --- gc event emit + gc events round-trip ---

// TestEventEmit_ThenEventsList_ShowsEvent verifies the event emit → events
// list round-trip: emit a custom event, then query the log and find it.
func TestEventEmit_ThenEventsList_ShowsEvent(t *testing.T) {
	c := helpers.NewCity(t, testEnv)
	c.Init("claude")

	// Emit a custom event.
	out, err := c.GC("event", "emit", "test.acceptance",
		"--subject", "test-bead-123",
		"--message", "acceptance test event",
		"--actor", "quinn")
	if err != nil {
		t.Fatalf("gc event emit failed: %v\n%s", err, out)
	}

	// Query the event log.
	out, err = c.GC("events")
	if err != nil {
		t.Fatalf("gc events failed: %v\n%s", err, out)
	}
	if !strings.Contains(out, "test.acceptance") {
		t.Errorf("event log should contain emitted event type 'test.acceptance', got:\n%s", out)
	}
}

// TestEventEmit_AlwaysExitsZero verifies that gc event emit never fails
// (best-effort design for use in bead hooks).
func TestEventEmit_AlwaysExitsZero(t *testing.T) {
	c := helpers.NewCity(t, testEnv)
	c.Init("claude")

	out, err := c.GC("event", "emit", "test.bestEffort")
	if err != nil {
		t.Fatalf("gc event emit should always exit 0: %v\n%s", err, out)
	}
}

// TestEvents_TypeFilter_FiltersResults verifies that gc events --type
// filters the output to matching event types.
func TestEvents_TypeFilter_FiltersResults(t *testing.T) {
	c := helpers.NewCity(t, testEnv)
	c.Init("claude")

	// Emit two different event types.
	c.GC("event", "emit", "test.alpha", "--message", "alpha event")
	c.GC("event", "emit", "test.beta", "--message", "beta event")

	// Filter to just alpha.
	out, err := c.GC("events", "--type", "test.alpha")
	if err != nil {
		t.Fatalf("gc events --type filter failed: %v\n%s", err, out)
	}
	if !strings.Contains(out, "test.alpha") {
		t.Errorf("filtered output should contain test.alpha, got:\n%s", out)
	}
	if strings.Contains(out, "test.beta") {
		t.Errorf("filtered output should NOT contain test.beta, got:\n%s", out)
	}
}

// TestEvents_Seq_PrintsNumber verifies that gc events --seq prints
// the current sequence number.
func TestEvents_Seq_PrintsNumber(t *testing.T) {
	c := helpers.NewCity(t, testEnv)
	c.Init("claude")

	out, err := c.GC("events", "--seq")
	if err != nil {
		t.Fatalf("gc events --seq failed: %v\n%s", err, out)
	}
	trimmed := strings.TrimSpace(out)
	if trimmed == "" {
		t.Error("gc events --seq output is empty")
	}
}

// TestEvents_JSONFormat_OutputsParseable verifies that gc events --json
// produces JSON output.
func TestEvents_JSONFormat_OutputsParseable(t *testing.T) {
	c := helpers.NewCity(t, testEnv)
	c.Init("claude")

	// Emit an event so there's something to show.
	c.GC("event", "emit", "test.json", "--message", "json test")

	out, err := c.GC("events", "--json")
	if err != nil {
		t.Fatalf("gc events --json failed: %v\n%s", err, out)
	}
	// JSON output should start with [ (array) or contain {.
	trimmed := strings.TrimSpace(out)
	if trimmed != "" && !strings.Contains(trimmed, "{") {
		t.Errorf("expected JSON-like output, got:\n%s", out)
	}
}

// --- gc event (bare command) ---

// TestEvent_NoSubcommand_ReturnsError verifies that bare gc event prints
// a helpful error about missing subcommand.
func TestEvent_NoSubcommand_ReturnsError(t *testing.T) {
	c := helpers.NewCity(t, testEnv)
	c.Init("claude")

	out, err := c.GC("event")
	if err == nil {
		t.Fatal("expected error for bare 'gc event', got success")
	}
	if !strings.Contains(out, "missing subcommand") {
		t.Errorf("expected 'missing subcommand' message, got:\n%s", out)
	}
}
