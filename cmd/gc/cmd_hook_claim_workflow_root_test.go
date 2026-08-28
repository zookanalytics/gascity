package main

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
)

// A workflow root carries the run's vars and nothing executable, yet stays a
// ready, routed, unassigned row for the whole run, so any worker that frees up
// mid-run can be handed one. These tests pin the rule
// beadmeta.WorkflowTopologyKinds states in words — routing never lands on
// these, agents must never claim them — at the readers that enforce it: the
// hook's candidate filter, the fresh-claim gate, and the demand predicate.

const workflowRootTestIdentity = "gastown__polecat-az-wisp-ak1hw"

// topologyWorkQueryRow renders one ready, routed, unassigned work-query row
// carrying the given gc.kind.
func topologyWorkQueryRow(id, kind string) string {
	return `{"id":"` + id + `","status":"open","issue_type":"task","assignee":"","metadata":{"gc.kind":"` +
		kind + `","gc.routed_to":"` + workflowRootTestIdentity + `"}}`
}

func workflowRootTestClaimOptions() hookClaimOptions {
	return hookClaimOptions{
		Assignee:           workflowRootTestIdentity,
		IdentityCandidates: hookClaimIdentityCandidates(workflowRootTestIdentity),
		RouteTargets:       hookClaimRouteTargets(workflowRootTestIdentity),
		JSON:               true,
	}
}

// TestHookClaimDoesNotServeWorkflowTopologyBead holds that a ready, routed,
// unassigned topology bead must drain, not claim. The cases are generated from
// the kind set, so a kind added to WorkflowTopologyKinds cannot ship without
// this coverage.
func TestHookClaimDoesNotServeWorkflowTopologyBead(t *testing.T) {
	for _, kind := range beadmeta.WorkflowTopologyKinds {
		t.Run(kind, func(t *testing.T) {
			const rootID = "gc-qubnz"
			runner := func(string, string) (string, error) {
				return `[` + topologyWorkQueryRow(rootID, kind) + `]`, nil
			}
			ops := hookClaimOps{
				Runner: runner,
				Claim: func(_ context.Context, _ string, _ []string, id, _ string) (beads.Bead, bool, error) {
					t.Fatalf("store.Claim called for %s bead %q; workflow topology must never reach the claim mutation", kind, id)
					return beads.Bead{}, false, nil
				},
			}
			var stdout, stderr bytes.Buffer
			doHookClaim("bd ready --json", "/tmp/work", workflowRootTestClaimOptions(), ops, &stdout, &stderr)

			var result hookClaimJSONResult
			if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
				t.Fatalf("stdout is not JSON: %v\nraw: %s", err, stdout.String())
			}
			if result.Action == "work" {
				t.Fatalf("hook served %s bead %q as action=work (reason=%q); a topology bead carries no executable body",
					kind, result.BeadID, result.Reason)
			}
			if result.Action != "drain" || result.Reason != hookClaimReasonNoWork {
				t.Fatalf("want action=drain reason=%s for a hook holding only a %s bead, got action=%q reason=%q",
					hookClaimReasonNoWork, kind, result.Action, result.Reason)
			}
		})
	}
}

// TestHookClaimSkipsWorkflowRootAndClaimsFrontierStep covers the batch a live
// run presents: the root and its own frontier step are ready together for the
// whole run, and the worker must take the step.
func TestHookClaimSkipsWorkflowRootAndClaimsFrontierStep(t *testing.T) {
	const (
		rootID = "gc-qubnz"
		stepID = "gc-h82h5"
	)
	runner := func(string, string) (string, error) {
		return `[
			` + topologyWorkQueryRow(rootID, beadmeta.KindWorkflow) + `,
			{"id":"` + stepID + `","status":"open","issue_type":"task","assignee":"","metadata":{"gc.routed_to":"` + workflowRootTestIdentity + `","gc.step_ref":"mol-polecat-work.load-context"}}
		]`, nil
	}
	claimedID := ""
	ops := hookClaimOps{
		Runner: runner,
		Claim: func(_ context.Context, _ string, _ []string, id, assignee string) (beads.Bead, bool, error) {
			if id == rootID {
				t.Fatalf("store.Claim called for workflow root %q; want the frontier step %q", id, stepID)
			}
			claimedID = id
			return beads.Bead{ID: id, Status: "in_progress", Assignee: assignee, Type: "task"}, true, nil
		},
	}
	var stdout, stderr bytes.Buffer
	doHookClaim("bd ready --json", "/tmp/work", workflowRootTestClaimOptions(), ops, &stdout, &stderr)

	var result hookClaimJSONResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("stdout is not JSON: %v\nraw: %s", err, stdout.String())
	}
	if result.BeadID != stepID || result.Action != "work" {
		t.Fatalf("want frontier step %q served as work, got action=%q reason=%q bead=%q",
			stepID, result.Action, result.Reason, result.BeadID)
	}
	if claimedID != stepID {
		t.Fatalf("store.Claim claimed %q, want %q", claimedID, stepID)
	}
}

// TestFilterUnreadyHookCandidatesDropsWorkflowTopologyKinds pins the shared
// defensive filter directly — it is the single seam every hook path runs
// through (doHook display, cross-store federation, and the claim), so plain
// `gc hook` cannot display as work what --claim refuses to hand over.
func TestFilterUnreadyHookCandidatesDropsWorkflowTopologyKinds(t *testing.T) {
	in := "[\n"
	for _, kind := range beadmeta.WorkflowTopologyKinds {
		in += topologyWorkQueryRow("topology-"+kind, kind) + ",\n"
	}
	in += topologyWorkQueryRow("step", "") + "\n]"

	got := filterUnreadyHookCandidates(in, time.Now())

	var rows []map[string]any
	if err := json.Unmarshal([]byte(got), &rows); err != nil {
		t.Fatalf("filtered output is not a JSON array: %v (output %q)", err, got)
	}
	if len(rows) != 1 || rows[0]["id"] != "step" {
		t.Fatalf("filterUnreadyHookCandidates kept %d rows, want only the step row; topology kinds %v must be dropped: %s",
			len(rows), beadmeta.WorkflowTopologyKinds, got)
	}
}

// TestFilterUnreadyHookCandidatesKeepsExecutableRoots is the load-bearing
// negative control. A vapor/root-only wisp root IS the work — the compiler
// stamps gc.kind=wisp precisely because there are no child steps to route to —
// and a legacy row carries no gc.kind at all. A filter that keyed on
// "is a root" rather than on the topology kind set would strand both, which is
// the whole scale-from-zero path.
func TestFilterUnreadyHookCandidatesKeepsExecutableRoots(t *testing.T) {
	in := `[
		` + topologyWorkQueryRow("wisp-root", beadmeta.KindWisp) + `,
		{"id":"legacy","status":"open","issue_type":"task","metadata":{"gc.routed_to":"` + workflowRootTestIdentity + `"}},
		{"id":"nometa","status":"open","issue_type":"task"}
	]`
	got := filterUnreadyHookCandidates(in, time.Now())

	var rows []map[string]any
	if err := json.Unmarshal([]byte(got), &rows); err != nil {
		t.Fatalf("filtered output is not a JSON array: %v (output %q)", err, got)
	}
	want := []string{"wisp-root", "legacy", "nometa"}
	if len(rows) != len(want) {
		t.Fatalf("filterUnreadyHookCandidates kept %d rows, want %d; only %v are topology kinds: %s",
			len(rows), len(want), beadmeta.WorkflowTopologyKinds, got)
	}
	for i, id := range want {
		if rows[i]["id"] != id {
			t.Fatalf("row %d id = %v, want %q: %s", i, rows[i]["id"], id, got)
		}
	}
}

// TestHookCandidateClaimableRefusesWorkflowTopologyKinds pins the gate that has
// the last word before the claim CAS, so a candidate that reaches the claim
// path without passing through the JSON filter is still refused.
func TestHookCandidateClaimableRefusesWorkflowTopologyKinds(t *testing.T) {
	routeTargets := hookClaimRouteTargets(workflowRootTestIdentity)
	for _, kind := range beadmeta.WorkflowTopologyKinds {
		candidate := beads.Bead{
			ID: "topology-" + kind, Status: "open", Type: "task",
			Metadata: map[string]string{
				beadmeta.KindMetadataKey:     kind,
				beadmeta.RoutedToMetadataKey: workflowRootTestIdentity,
			},
		}
		if hookCandidateClaimable(candidate, routeTargets) {
			t.Errorf("hookCandidateClaimable(%s) = true, want false", kind)
		}
	}
	step := beads.Bead{
		ID: "step", Status: "open", Type: "task",
		Metadata: map[string]string{beadmeta.RoutedToMetadataKey: workflowRootTestIdentity},
	}
	if !hookCandidateClaimable(step, routeTargets) {
		t.Error("hookCandidateClaimable(routed step) = false, want true")
	}
}
