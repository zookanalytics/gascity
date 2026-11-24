package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/config"
)

// This file expresses the ga-x9kptu / ga-5736js acceptance criteria for the
// shell-generated control-ready query (workflowServeControlReadyQueryForBeads):
// routed_ready() (route-scoped, unassigned) must exclude beads carrying an
// active hold, whatever its value names, while assignee_ready() (Tier 1/2)
// stays hold-transparent. routed_ready() is a single shared function invoked
// once for the primary target and once each for the legacy and bare-route
// aliases, so fixing it once must apply uniformly across all three.
//
// TestWorkflowServeControlReadyQueryShellFallbackUnreachable closes the
// reachability question this bead's acceptance criteria raised: whether
// routed_ready()/assignee_ready() can ever actually run as a subprocess via
// nextWorkflowServeBeads's raw shellWorkQueryWithEnv fallback, or whether
// tryControlReadyScan always intercepts first. It proves the
// latter, so the shell-level tests here are defense-in-depth on
// currently-dead code, not coverage of a live production path -- the real
// enforcement for this query shape runs through evaluateControlReady /
// filterReadyByRoute (see dispatch_control_ready_hold_label_test.go).

func TestWorkflowServeControlReadyQueryRoutedReadyDropsActiveHolds(t *testing.T) {
	query := workflowServeControlReadyQuery(config.Agent{Name: config.ControlDispatcherAgentName, Dir: "gascity"})
	if count := strings.Count(query, `emit_routed_ready bd --readonly --sandbox ready`); count != 2 {
		t.Errorf("routed_ready's bd-ready calls go through emit_routed_ready %d times, want 2: %s", count, query)
	}
	if strings.Contains(query, `--exclude-label "hold:`) {
		t.Errorf("workflowServeControlReadyQuery() names a hold label in an --exclude-label flag; holds are dropped by the active hold predicate: %s", query)
	}
}

// TestWorkflowServeControlReadyQueryShellServesNoHeldRow runs the generated
// shell against a fake bd whose routed reads answer a full scan of held rows
// ahead of an unheld one, and whose assignee reads answer a held assignment:
// the routed groups lose every held row and keep the unheld one, the assignee
// group stays hold-transparent.
func TestWorkflowServeControlReadyQueryShellServesNoHeldRow(t *testing.T) {
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq not available; the control-ready shell requires it")
	}
	var routed []map[string]any
	for i := 0; i < workflowServeScanLimit; i++ {
		routed = append(routed, map[string]any{"id": "held-" + string(rune('a'+i)), "labels": []string{"hold:po"}})
	}
	routed = append(routed, map[string]any{"id": "unheld"}, map[string]any{"id": "cleared", "labels": []string{"hold:cleared"}})
	routedJSON, err := json.Marshal(routed)
	if err != nil {
		t.Fatal(err)
	}
	routedPath := filepath.Join(t.TempDir(), "routed.json")
	if err := os.WriteFile(routedPath, routedJSON, 0o644); err != nil {
		t.Fatal(err)
	}
	query := workflowServeControlReadyQuery(config.Agent{Name: config.ControlDispatcherAgentName, Dir: "gascity"})
	out := runWorkflowServeShellQueryForTest(t, query, map[string]string{
		"GC_ALIAS": "gascity/" + config.ControlDispatcherAgentName,
	}, `#!/bin/sh
case " $* " in
  *" --assignee="*) printf '[{"id":"assigned-held","labels":["hold:po"]}]' ;;
  *"gc.routed_to="*) cat "`+routedPath+`" ;;
  *) printf '[]' ;;
esac
`)
	var rows []struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal([]byte(out), &rows); err != nil {
		t.Fatalf("control-ready shell printed %q: %v", out, err)
	}
	var ids []string
	for _, row := range rows {
		ids = append(ids, row.ID)
	}
	if want := []string{"assigned-held", "unheld", "cleared"}; !stringSlicesEqual(ids, want) {
		t.Fatalf("control-ready shell ids = %v, want %v: held routed rows dropped before the scan limit, the assignee group hold-transparent", ids, want)
	}
}

func TestWorkflowServeControlReadyQueryAssigneeReadyIsHoldTransparent(t *testing.T) {
	query := workflowServeControlReadyQuery(config.Agent{Name: config.ControlDispatcherAgentName, Dir: "gascity"})
	start := strings.Index(query, `assignee_ready() { `)
	if start < 0 {
		t.Fatalf("workflowServeControlReadyQuery() missing assignee_ready() definition: %s", query)
	}
	relEnd := strings.Index(query[start:], `; }; `)
	if relEnd < 0 {
		t.Fatalf("workflowServeControlReadyQuery() could not locate end of assignee_ready() body: %s", query)
	}
	body := query[start : start+relEnd]
	if strings.Contains(body, "--exclude-label") {
		t.Errorf("assignee_ready() body = %q, must stay hold-transparent (Tier 1/2 assignee-scoped)", body)
	}
}

func TestWorkflowServeControlReadyQueryRoutedReadyAppliesToAllRouteAliases(t *testing.T) {
	query := workflowServeControlReadyQuery(config.Agent{Name: config.ControlDispatcherAgentName, Dir: "gascity"})
	if count := strings.Count(query, `routed_ready "`); count != 3 {
		t.Errorf("workflowServeControlReadyQuery() calls routed_ready %d times, want 3 (target, legacy, bare route): %s", count, query)
	}
}

// TestWorkflowServeControlReadyQueryShellFallbackUnreachable proves
// nextWorkflowServeBeads's raw shell fallback (shellWorkQueryWithEnv) can
// never execute a control-ready-shaped query. tryControlReadyScan
// returns handled=false only when parseControlReadyQuery fails to recognize
// the query (dispatch_control_ready.go), which happens only when its parsed
// target is empty. workflowServeControlReadyQueryForBeads guarantees a
// non-empty GC_CONTROL_TARGET unconditionally, falling back to
// config.ControlDispatcherAgentName when agentCfg.QualifiedName() is blank
// (dispatch_runtime.go) -- so this test uses the zero-value Agent, the most
// adversarial input available, to confirm even that never yields an
// unrecognized query. If a future change ever lets target come back empty,
// this test fails first, flagging that routed_ready()/assignee_ready()'s
// hold-label handling has become load-bearing and needs a real fix, not
// just the string-level assertions above.
func TestWorkflowServeControlReadyQueryShellFallbackUnreachable(t *testing.T) {
	query := workflowServeControlReadyQuery(config.Agent{})
	parsed, ok := parseControlReadyQuery(query)
	if !ok || parsed.target == "" {
		t.Fatalf("parseControlReadyQuery(%q) = %+v, ok=%v; want ok=true with non-empty target -- shell fallback would become reachable", query, parsed, ok)
	}
}
