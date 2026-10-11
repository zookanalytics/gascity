package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

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

// stepReminderClaimOps serves output as the work query and fakes every store
// seam, so a test sees exactly the result the claim writes and nothing reaches
// a real store. Claim answers with canonical, standing in for the full bead the
// production store's post-claim readback returns.
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
// serves for an in_progress claim, description included.
func heldStepRow(t *testing.T, bead beads.Bead) string {
	t.Helper()
	row, err := json.Marshal([]beads.Bead{bead})
	if err != nil {
		t.Fatalf("marshal work-query row: %v", err)
	}
	return string(row)
}

// TestHookClaimWorkResultCarriesTheClaimedStep pins the reply a pool session
// reads when it has no memory of the claim. Re-served its own in_progress step
// (existing_assignment), it must learn what the step is from the result itself.
// A ready assignment is promoted without the session asking for that bead, the
// same shape; a fresh claim carries the step too, so every work result reads
// alike. The step comes off the work-query row on the held tier and off the
// claim readback on the other two, so each tier is its own case.
func TestHookClaimWorkResultCarriesTheClaimedStep(t *testing.T) {
	for _, tc := range []struct {
		reason string
		row    string
	}{
		{reason: "existing_assignment", row: heldStepRow(t, heldStep)},
		{reason: "ready_assignment", row: `[{"id":"own-1","status":"open","assignee":"worker-1","metadata":{"gc.routed_to":"worker"}}]`},
		{reason: "claimed", row: `[{"id":"own-1","status":"open","metadata":{"gc.routed_to":"worker"}}]`},
	} {
		t.Run(tc.reason, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := doHookClaim("query", "/rig", stepReminderClaimOpts(true), stepReminderClaimOps(tc.row, heldStep), &stdout, &stderr)
			if code != 0 {
				t.Fatalf("code = %d, want 0; stderr=%s", code, stderr.String())
			}
			result := decodeTurnBoundResult(t, stdout.String())
			if result.Reason != tc.reason || result.BeadID != heldStep.ID {
				t.Fatalf("result = %+v, want %s for %s", result, tc.reason, heldStep.ID)
			}
			for _, want := range []string{heldStep.Title, heldStep.Description} {
				if !strings.Contains(result.StepReminder, want) {
					t.Errorf("step_reminder = %q, want it to carry %q", result.StepReminder, want)
				}
			}
			if want := formatWispStepReminder(&heldStep); result.StepReminder != want {
				t.Errorf("step_reminder = %q, want the rendering gc nudge and gc prime --hook deliver, %q", result.StepReminder, want)
			}
			validateJSONResultSchema(t, []string{"hook"}, stdout.Bytes())
		})
	}
}

// TestHookClaimUnresolvableStepLeavesResultByteIdentical pins the best-effort
// contract: a bead with no instruction to carry yields exactly the result the
// claim wrote before the field existed, and the claim still succeeds. The
// goldens are literal bytes rather than a re-encoding of the struct, so a
// change to the struct cannot move them.
func TestHookClaimUnresolvableStepLeavesResultByteIdentical(t *testing.T) {
	const jsonGolden = `{"schema_version":"1","ok":true,"command":"hook","action":"work","reason":"existing_assignment","bead_id":"own-1","assignee":"worker-1","route":"worker"}` + "\n"
	for _, tc := range []struct {
		name        string
		description string
	}{
		{name: "empty description", description: ""},
		{name: "blank description", description: " \n\t "},
	} {
		bead := heldStep
		bead.Description = tc.description
		for _, mode := range []struct {
			name   string
			json   bool
			golden string
		}{
			{name: "json", json: true, golden: jsonGolden},
			{name: "plain", json: false, golden: "own-1\n"},
		} {
			t.Run(tc.name+"/"+mode.name, func(t *testing.T) {
				var stdout, stderr bytes.Buffer
				code := doHookClaim("query", "/rig", stepReminderClaimOpts(mode.json), stepReminderClaimOps(heldStepRow(t, bead), bead), &stdout, &stderr)
				if code != 0 {
					t.Fatalf("code = %d, want 0: an unresolvable step must never fail the claim; stderr=%s", code, stderr.String())
				}
				if got := stdout.String(); got != mode.golden {
					t.Fatalf("stdout =\n%q\nwant byte-identical\n%q", got, mode.golden)
				}
			})
		}
	}
}

// TestHookClaimPlainOutputAppendsTheStepAfterTheBeadID pins the non-JSON form:
// the first line stays the bead id alone, and the step follows it.
func TestHookClaimPlainOutputAppendsTheStepAfterTheBeadID(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := doHookClaim("query", "/rig", stepReminderClaimOpts(false), stepReminderClaimOps(heldStepRow(t, heldStep), heldStep), &stdout, &stderr)
	if code != 0 {
		t.Fatalf("code = %d, want 0; stderr=%s", code, stderr.String())
	}
	if want := heldStep.ID + "\n" + formatWispStepReminder(&heldStep); stdout.String() != want {
		t.Fatalf("stdout =\n%q\nwant the bead id line, then the step:\n%q", stdout.String(), want)
	}
}
