package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/session"
)

func TestLooksLikeSessionBeadIDRejectsSessionNames(t *testing.T) {
	defaultPrefixes := map[string]bool{"gc": true, "bd": true, "mc": true, "lx": true}
	cases := []struct {
		name     string
		in       string
		prefixes map[string]bool
		want     bool
	}{
		{"plain bead id with known prefix", "gc-119r", defaultPrefixes, true},
		{"workspace prefix bead id", "lx-v2yp1", defaultPrefixes, true},
		{"bd-prefixed bead id with known prefix", "bd-abc12", defaultPrefixes, true},
		{"mc-prefixed bead id with known prefix", "mc-abc12", defaultPrefixes, true},
		{"prefix not in known set", "lx-v2yp1", map[string]bool{"gc": true}, false},
		{"unknown prefix entirely", "qq-abc12", defaultPrefixes, false},
		{"rig-qualified session name", "gc-toolkit/gastown.witness", defaultPrefixes, false},
		{"role-qualified session name", "mayor/refinery", defaultPrefixes, false},
		{"plain rig-prefixed session", "gc-foo/bar", defaultPrefixes, false},
		{"dot-separated rig-qualified", "gc-toolkit.mechanik", defaultPrefixes, false},
		{"double-hyphen alias", "gascity--control-dispatcher", defaultPrefixes, false},
		{"bare alias", "mechanik", defaultPrefixes, false},
		{"hyphen alias", "control-dispatcher", defaultPrefixes, false},
		{"unrelated prefix", "agent-diagnostics-h1", defaultPrefixes, false},
		{"empty string", "", defaultPrefixes, false},
		{"empty prefix set", "gc-119r", map[string]bool{}, false},
		{"nil prefix set", "gc-119r", nil, false},
		{"empty string entry in set is skipped", "gc-119r", map[string]bool{"": true}, false},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			if got := looksLikeSessionBeadID(tt.in, tt.prefixes); got != tt.want {
				t.Errorf("looksLikeSessionBeadID(%q, %v) = %v, want %v", tt.in, tt.prefixes, got, tt.want)
			}
		})
	}
}

func TestKnownBeadPrefixesIncludesHQAndRigPrefixes(t *testing.T) {
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city", Prefix: "tc"},
		Rigs: []config.Rig{
			{Name: "gc-toolkit", Prefix: "tk"}, // explicit prefix wins
			{Name: "gascity"},                  // derived: "ga"
		},
	}
	got := knownBeadPrefixes(cfg)
	wantKeys := []string{"tc", "tk", "ga"}
	for _, k := range wantKeys {
		if !got[k] {
			t.Errorf("knownBeadPrefixes() missing %q (got %v)", k, got)
		}
	}
	if got["gc-toolkit"] {
		t.Errorf("knownBeadPrefixes() should not contain rig name %q (got %v)", "gc-toolkit", got)
	}
}

func TestKnownBeadPrefixesNilConfig(t *testing.T) {
	got := knownBeadPrefixes(nil)
	if len(got) != 0 {
		t.Errorf("knownBeadPrefixes(nil) = %v, want empty", got)
	}
}

// TestPhase0DoctorDoesNotFalsePositiveOnRigQualifiedSessionName ensures that
// when an assignee is a rig-qualified session name (e.g.
// "gc-toolkit/gastown.witness" or "gc-toolkit.mechanik") whose live session
// bead exists in the store, the session-model check does not emit a
// "missing-bead-owner" finding for it.
//
// Reproduces gc-119r / gc-8ylt3: looksLikeSessionBeadID previously returned
// true for any string starting with a hardcoded prefix ("gc-", "bd-", "mc-"),
// causing rig names with those prefixes to be misclassified as session bead
// IDs. The fix in gc-119r added a "/" reject; gc-8ylt3 generalizes this to a
// closed-set classifier that also rejects "." separators.
func TestPhase0DoctorDoesNotFalsePositiveOnRigQualifiedSessionName(t *testing.T) {
	cases := []struct {
		name        string
		sessionName string
	}{
		{"slash separator", "gc-toolkit/gastown.witness"},
		{"dot separator", "gc-toolkit.mechanik"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			cityPath, store := newPhase0DoctorCity(t)

			witness, err := store.Create(beads.Bead{
				Type:   session.BeadType,
				Labels: []string{session.LabelSession},
				Metadata: map[string]string{
					"session_name": tt.sessionName,
					"alias":        tt.sessionName,
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
				Assignee: tt.sessionName,
			}); err != nil {
				t.Fatalf("create wisp bead: %v", err)
			}

			t.Setenv("GC_CITY", cityPath)
			var stdout, stderr bytes.Buffer
			_ = doDoctor(false, true, false, &stdout, &stderr)

			out := stdout.String() + stderr.String()
			if strings.Contains(out, "missing-bead-owner") {
				t.Fatalf("doctor falsely reported missing-bead-owner for live session %s:\n%s", witness.ID, out)
			}
		})
	}
}
