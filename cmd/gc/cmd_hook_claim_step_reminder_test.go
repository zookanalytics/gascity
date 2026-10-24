package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
)

// heldStep is the step bead a fresh-woken pool session holds. Its description
// is the instruction the session must continue.
var heldStep = beads.Bead{
	ID:          "own-1",
	Title:       "Review the branch",
	Description: "Read the diff against the base branch, then record one verdict through signoff.sh.",
	Status:      "in_progress",
	Assignee:    "worker-1",
	Metadata:    map[string]string{"gc.routed_to": "worker"},
}

// heldTiers are the work tiers that hand a session a claim it did not mint on
// this call: its own in_progress bead re-served (existing_assignment), and a
// bead assigned to it and promoted now (ready_assignment).
var heldTiers = []string{"existing_assignment", "ready_assignment"}

// stepReminderClaimOps serves output as the work query and fakes every store
// seam, so a test sees exactly the result the claim writes and nothing reaches
// a real store. Claim and the canonical readback both answer with canonical,
// standing in for the full bead the production store returns.
func stepReminderClaimOps(output string, canonical beads.Bead) hookClaimOps {
	return hookClaimOps{
		Runner: func(string, string) (string, error) { return output, nil },
		Claim: func(_ context.Context, _ string, _ []string, beadID, assignee string) (beads.Bead, bool, error) {
			claimed := canonical
			claimed.ID = beadID
			claimed.Status = "in_progress"
			claimed.Assignee = assignee
			return claimed, true, nil
		},
		ReadWorkMeta: func(context.Context, string, []string, string, string) (beads.Bead, error) {
			return canonical, nil
		},
		ListContinuation: func(context.Context, string, []string, string, string) ([]beads.Bead, error) {
			return nil, nil
		},
		EmitExecutionStepStarted: func(beads.Bead, string, []string, string) {},
		PublishRunMap:            func(string, string, ...string) error { return nil },
		StampWorkMeta:            func(context.Context, string, []string, string, string, map[string]string) error { return nil },
		StampSessionClaim:        func(string, string) error { return nil },
		ResolveWorkBranch:        func(hookClaimWorkTree) string { return "" },
		ResolveSessionWorkDir:    func(string) string { return "" },
	}
}

func stepReminderClaimOpts(jsonOut bool) hookClaimOptions {
	return hookClaimOptions{
		Assignee:           "worker-1",
		IdentityCandidates: []string{"worker-1"},
		RouteTargets:       []string{"worker"},
		JSON:               jsonOut,
	}
}

// heldStepRow renders bead as the one-row work-query output `bd list --json`
// serves, description included.
func heldStepRow(t *testing.T, bead beads.Bead) string {
	t.Helper()
	row, err := json.Marshal([]beads.Bead{bead})
	if err != nil {
		t.Fatalf("marshal work-query row: %v", err)
	}
	return string(row)
}

// tierRow renders bead as the work-query row that reaches tier: in_progress
// and assigned for existing_assignment, open and assigned for
// ready_assignment, open and unassigned for claimed.
func tierRow(t *testing.T, tier string, bead beads.Bead) string {
	t.Helper()
	switch tier {
	case "existing_assignment":
		bead.Status, bead.Assignee = "in_progress", "worker-1"
	case "ready_assignment":
		bead.Status, bead.Assignee = "open", "worker-1"
	case "claimed":
		bead.Status, bead.Assignee = "open", ""
	default:
		t.Fatalf("no work-query row for tier %q", tier)
	}
	return heldStepRow(t, bead)
}

// bareTierRow is tierRow for a row that carries no title or description, so a
// step the result carries can only have come from the canonical bead.
func bareTierRow(t *testing.T, tier string) string {
	t.Helper()
	return tierRow(t, tier, beads.Bead{ID: heldStep.ID, Metadata: map[string]string{"gc.routed_to": "worker"}})
}

// resultGolden is the literal JSON line a work result carries when it holds no
// step, so a change to the result struct cannot move it.
func resultGolden(reason string) string {
	return fmt.Sprintf(`{"schema_version":"1","ok":true,"command":"hook","action":"work","reason":%q,"bead_id":"own-1","assignee":"worker-1","route":"worker"}`, reason) + "\n"
}

// runStepReminderClaim runs one claim and returns its stdout, failing the test
// unless the claim succeeded.
func runStepReminderClaim(t *testing.T, jsonOut bool, ops hookClaimOps) string {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := doHookClaim("query", "/rig", stepReminderClaimOpts(jsonOut), ops, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("code = %d, want 0; stderr=%s", code, stderr.String())
	}
	return stdout.String()
}

// TestHookClaimHeldTiersCarryTheClaimedStep pins the reply a pool session reads
// when it holds a claim it has no memory of making. On both held tiers the
// result carries the step, rendered the way gc nudge and gc prime --hook render
// it. The row carries no step text, so the reminder comes from the canonical
// bead: the adoption readback on existing_assignment, the claim readback on
// ready_assignment.
func TestHookClaimHeldTiersCarryTheClaimedStep(t *testing.T) {
	for _, tier := range heldTiers {
		t.Run(tier, func(t *testing.T) {
			stdout := runStepReminderClaim(t, true, stepReminderClaimOps(bareTierRow(t, tier), heldStep))
			result := decodeTurnBoundResult(t, stdout)
			if result.Reason != tier || result.BeadID != heldStep.ID {
				t.Fatalf("result = %+v, want %s for %s", result, tier, heldStep.ID)
			}
			for _, want := range []string{heldStep.Title, heldStep.Description} {
				if !strings.Contains(result.StepReminder, want) {
					t.Errorf("step_reminder = %q, want it to carry %q", result.StepReminder, want)
				}
			}
			if want := formatWispStepReminder(&heldStep); result.StepReminder != want {
				t.Errorf("step_reminder = %q, want the rendering gc nudge and gc prime --hook deliver, %q", result.StepReminder, want)
			}
			validateJSONResultSchema(t, []string{"hook"}, []byte(stdout))
		})
	}
}

// TestHookClaimFreshClaimCarriesNoStep pins the claimed tier: the session asked
// for work on this call and the claim it minted is new to it, so the result is
// the identifiers alone, however much text the bead carries.
func TestHookClaimFreshClaimCarriesNoStep(t *testing.T) {
	stdout := runStepReminderClaim(t, true, stepReminderClaimOps(tierRow(t, "claimed", heldStep), heldStep))
	if want := resultGolden("claimed"); stdout != want {
		t.Fatalf("stdout =\n%q\nwant\n%q", stdout, want)
	}
}

// TestHookClaimPlainOutputIsTheBeadIDAlone pins the non-JSON form a caller
// captures as an id (`id=$(gc hook --claim)`): the bead id and a newline, on
// every tier, even when the bead carries a step.
func TestHookClaimPlainOutputIsTheBeadIDAlone(t *testing.T) {
	for _, tier := range append([]string{"claimed"}, heldTiers...) {
		t.Run(tier, func(t *testing.T) {
			stdout := runStepReminderClaim(t, false, stepReminderClaimOps(tierRow(t, tier, heldStep), heldStep))
			if want := heldStep.ID + "\n"; stdout != want {
				t.Fatalf("stdout =\n%q\nwant the bead id alone\n%q", stdout, want)
			}
		})
	}
}

// TestHookClaimTitleOnlyStepCarriesItsTitle pins a step whose whole
// instruction is its title: the title is still an instruction to continue, so
// the result carries it.
func TestHookClaimTitleOnlyStepCarriesItsTitle(t *testing.T) {
	titleOnly := heldStep
	titleOnly.Description = " \n\t "
	for _, tier := range heldTiers {
		t.Run(tier, func(t *testing.T) {
			stdout := runStepReminderClaim(t, true, stepReminderClaimOps(bareTierRow(t, tier), titleOnly))
			result := decodeTurnBoundResult(t, stdout)
			if want := formatWispStepReminder(&titleOnly); result.StepReminder != want {
				t.Fatalf("step_reminder = %q, want the title-only step %q", result.StepReminder, want)
			}
			if !strings.Contains(result.StepReminder, titleOnly.Title) {
				t.Errorf("step_reminder = %q, want it to carry the title %q", result.StepReminder, titleOnly.Title)
			}
		})
	}
}

// TestHookClaimResultWithNoStepOfItsOwnIsByteIdentical pins the held tiers'
// results that carry no step: a bead with no title or description has no
// instruction to carry, and a bead whose work runs as other beads is not the
// step to continue. A molecule root's step is a child, which gc nudge and gc
// prime --hook resolve to; a source bead bridged by molecule_id leads them to
// its molecule's step; a graph.v2 workflow root's work runs as the
// continuation siblings the session claims next. Each result is the
// identifiers alone, and the claim still succeeds.
func TestHookClaimResultWithNoStepOfItsOwnIsByteIdentical(t *testing.T) {
	withMeta := func(key, value string) beads.Bead {
		b := heldStep
		b.Metadata = map[string]string{"gc.routed_to": "worker", key: value}
		return b
	}
	blank := heldStep
	blank.Title, blank.Description = "", " \n\t "
	moleculeRoot := heldStep
	moleculeRoot.Type = "molecule"
	wispRoot := heldStep
	wispRoot.Type = "wisp"
	for _, tc := range []struct {
		name string
		bead beads.Bead
	}{
		{name: "no title or description", bead: blank},
		{name: "molecule root", bead: moleculeRoot},
		{name: "wisp root", bead: wispRoot},
		{name: "workflow root", bead: withMeta(beadmeta.KindMetadataKey, beadmeta.KindWorkflow)},
		{name: "bridged source", bead: withMeta(beadmeta.MoleculeIDMetadataKey, "mol-1")},
	} {
		for _, tier := range heldTiers {
			t.Run(tc.name+"/"+tier, func(t *testing.T) {
				stdout := runStepReminderClaim(t, true, stepReminderClaimOps(tierRow(t, tier, tc.bead), tc.bead))
				if want := resultGolden(tier); stdout != want {
					t.Fatalf("stdout =\n%q\nwant byte-identical\n%q", stdout, want)
				}
			})
		}
	}
}

// TestHookClaimHeldStepRendersTheCanonicalBead pins where existing_assignment
// reads the step. The work-query row can come from a caching store that lags
// an edit made after the claim, and adoption already reads the canonical bead
// to certify ownership, so the step renders from that readback.
func TestHookClaimHeldStepRendersTheCanonicalBead(t *testing.T) {
	stale := heldStep
	stale.Title = "Review the branch (superseded)"
	stale.Description = "Superseded instruction: rebase the branch."
	stdout := runStepReminderClaim(t, true, stepReminderClaimOps(tierRow(t, "existing_assignment", stale), heldStep))
	result := decodeTurnBoundResult(t, stdout)
	if want := formatWispStepReminder(&heldStep); result.StepReminder != want {
		t.Fatalf("step_reminder = %q, want the canonical step %q", result.StepReminder, want)
	}
	if strings.Contains(result.StepReminder, stale.Description) {
		t.Errorf("step_reminder = %q carries the stale row's description", result.StepReminder)
	}
}

// TestHookClaimUnverifiedAdoptionRendersTheRow pins the held claim whose
// canonical readback fails. Adoption then proceeds on the work-query row alone,
// and the step renders from that same row.
func TestHookClaimUnverifiedAdoptionRendersTheRow(t *testing.T) {
	ops := stepReminderClaimOps(tierRow(t, "existing_assignment", heldStep), heldStep)
	ops.ReadWorkMeta = func(context.Context, string, []string, string, string) (beads.Bead, error) {
		return beads.Bead{}, errors.New("store unavailable")
	}
	stdout := runStepReminderClaim(t, true, ops)
	result := decodeTurnBoundResult(t, stdout)
	if result.Reason != "existing_assignment" {
		t.Fatalf("result = %+v, want existing_assignment", result)
	}
	if want := formatWispStepReminder(&heldStep); result.StepReminder != want {
		t.Fatalf("step_reminder = %q, want the row's step %q", result.StepReminder, want)
	}
}

// TestHookClaimStepNamesTheResultBeadID pins the id a step is rendered under.
// A canonical readback that omits the id still yields a result whose bead_id
// is the candidate's, and the step names that same id.
func TestHookClaimStepNamesTheResultBeadID(t *testing.T) {
	noID := heldStep
	noID.ID = ""
	for _, tier := range heldTiers {
		t.Run(tier, func(t *testing.T) {
			ops := stepReminderClaimOps(bareTierRow(t, tier), noID)
			ops.Claim = func(_ context.Context, _ string, _ []string, _, assignee string) (beads.Bead, bool, error) {
				claimed := noID
				claimed.Status = "in_progress"
				claimed.Assignee = assignee
				return claimed, true, nil
			}
			stdout := runStepReminderClaim(t, true, ops)
			result := decodeTurnBoundResult(t, stdout)
			if result.BeadID != heldStep.ID {
				t.Fatalf("bead_id = %q, want %q", result.BeadID, heldStep.ID)
			}
			if want := formatWispStepReminder(&heldStep); result.StepReminder != want {
				t.Fatalf("step_reminder = %q, want the step under bead_id %q: %q", result.StepReminder, heldStep.ID, want)
			}
		})
	}
}
