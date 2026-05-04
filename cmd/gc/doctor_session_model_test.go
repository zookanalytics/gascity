package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/session"
)

func TestLooksLikeSessionBeadIDRejectsSessionNames(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want bool
	}{
		{"plain bead id", "gc-119r", true},
		{"hierarchical bead id", "gc-119r.child", true},
		{"bd-prefixed bead id", "bd-abc12", true},
		{"mc-prefixed bead id", "mc-abc12", true},
		{"rig-qualified session name", "gc-toolkit/gastown.witness", false},
		{"role-qualified session name", "mayor/refinery", false},
		{"plain rig-prefixed session", "gc-foo/bar", false},
		{"unrelated prefix", "agent-diagnostics-h1", false},
		{"empty string", "", false},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			if got := looksLikeSessionBeadID(tt.in); got != tt.want {
				t.Errorf("looksLikeSessionBeadID(%q) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

// TestPhase0DoctorDoesNotFalsePositiveOnRigQualifiedSessionName ensures that
// when an assignee is a rig-qualified session name (e.g.
// "gc-toolkit/gastown.witness") whose live session bead exists in the store,
// the session-model check does not emit a "missing-bead-owner" finding for it.
//
// Reproduces gc-119r: looksLikeSessionBeadID previously returned true for
// any string starting with "gc-", "bd-", or "mc-", causing rig names with
// those prefixes to be misclassified as session bead IDs.
func TestPhase0DoctorDoesNotFalsePositiveOnRigQualifiedSessionName(t *testing.T) {
	cityPath, store := newPhase0DoctorCity(t)

	witness, err := store.Create(beads.Bead{
		Type:   session.BeadType,
		Labels: []string{session.LabelSession},
		Metadata: map[string]string{
			"session_name": "gc-toolkit/gastown.witness",
			"alias":        "gc-toolkit/gastown.witness",
			"template":     "worker",
		},
	})
	if err != nil {
		t.Fatalf("create witness session bead: %v", err)
	}
	if _, err := store.Create(beads.Bead{
		Type:     "task",
		Status:   "open",
		Title:    "wisp routed to live witness",
		Assignee: "gc-toolkit/gastown.witness",
	}); err != nil {
		t.Fatalf("create wisp bead: %v", err)
	}

	t.Setenv("GC_CITY", cityPath)
	var stdout, stderr bytes.Buffer
	_ = doDoctor(false, true, &stdout, &stderr)

	out := stdout.String() + stderr.String()
	if strings.Contains(out, "missing-bead-owner") {
		t.Fatalf("doctor falsely reported missing-bead-owner for live session %s:\n%s", witness.ID, out)
	}
}
