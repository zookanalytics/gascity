package main

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/beads"
)

func TestCloseActorForOwnClaim(t *testing.T) {
	session := map[string]string{
		"GC_SESSION_ID":   "ci-wisp-fe8",
		"GC_SESSION_NAME": "rig--gc__review-synthesizer-1-pool",
		"BEADS_ACTOR":     "rig--gc__review-synthesizer-1-pool",
	}
	getenv := func(env map[string]string) func(string) string { return func(k string) string { return env[k] } }
	held := func(assignee string) beads.Bead { return beads.Bead{ID: "ci-1", Assignee: assignee} }

	for _, tc := range []struct {
		name    string
		args    []string
		targets map[string]beads.Bead
		env     map[string]string
		// effective is the BEADS_ACTOR the bd child would run under; nil
		// means the same value as the process env.
		effective *string
		want      string
	}{
		{
			"own claim under the session bead id closes as that id",
			[]string{"close", "ci-1", "--reason", "done"},
			map[string]beads.Bead{"ci-1": held("ci-wisp-fe8")},
			session, nil, "ci-wisp-fe8",
		},
		{
			"update to closed is a close too",
			[]string{"update", "ci-1", "--status", "closed"},
			map[string]beads.Bead{"ci-1": held("ci-wisp-fe8")},
			session, nil, "ci-wisp-fe8",
		},
		{
			"a bead held by another session keeps the session's own actor",
			[]string{"close", "ci-1"},
			map[string]beads.Bead{"ci-1": held("ci-wisp-other")},
			session, nil, "",
		},
		{
			"already the actor needs no change",
			[]string{"close", "ci-1"},
			map[string]beads.Bead{"ci-1": held("rig--gc__review-synthesizer-1-pool")},
			session, nil, "",
		},
		{
			"unassigned bead needs no change",
			[]string{"close", "ci-1"},
			map[string]beads.Bead{"ci-1": held("")},
			session, nil, "",
		},
		{
			"outside a session nothing changes",
			[]string{"close", "ci-1"},
			map[string]beads.Bead{"ci-1": held("ci-wisp-fe8")},
			map[string]string{"BEADS_ACTOR": "human"},
			nil, "",
		},
		{
			"an unread target leaves bd's check to decide",
			[]string{"close", "ci-1"},
			map[string]beads.Bead{},
			session, nil, "",
		},
		{
			"targets held by two identities are not merged",
			[]string{"close", "ci-1", "ci-2"},
			map[string]beads.Bead{"ci-1": held("ci-wisp-fe8"), "ci-2": {ID: "ci-2", Assignee: "rig--gc__review-synthesizer-1-pool"}},
			session, nil, "",
		},
		{
			"a metadata update is not a close",
			[]string{"update", "ci-1", "--set-metadata", "gc.outcome=pass"},
			map[string]beads.Bead{"ci-1": held("ci-wisp-fe8")},
			session, nil, "",
		},
		{
			// #6324: for tmux_alias pools and legacy rows the session name is
			// a shared chair, so a successor must not close its
			// predecessor's chair-named claim without --force.
			"a claim held under the session name is not the session's own",
			[]string{"close", "ci-1"},
			map[string]beads.Bead{"ci-1": held("rig--gc__review-synthesizer-1-pool")},
			session, strPtr("city-default-actor"), "",
		},
		{
			"a claim held under the alias is not the session's own",
			[]string{"close", "ci-1"},
			map[string]beads.Bead{"ci-1": held("reviewer")},
			map[string]string{"GC_SESSION_ID": "ci-wisp-fe8", "GC_ALIAS": "reviewer", "BEADS_ACTOR": "rig--gc__review-synthesizer-1-pool"},
			nil, "",
		},
		{
			"an effective actor already matching the assignee needs no change",
			[]string{"close", "ci-1"},
			map[string]beads.Bead{"ci-1": held("ci-wisp-fe8")},
			session, strPtr("ci-wisp-fe8"), "",
		},
		{
			// gc-ox80c: bd's heartbeat is owner-only too, so the holder of a
			// claim gc hook --claim stamped with the session bead id refreshes
			// the lease under that id.
			"own claim under the session bead id heartbeats as that id",
			[]string{"heartbeat", "ci-1"},
			map[string]beads.Bead{"ci-1": held("ci-wisp-fe8")},
			session, nil, "ci-wisp-fe8",
		},
		{
			"a heartbeat of a bead held by another session keeps the session's own actor",
			[]string{"heartbeat", "ci-1"},
			map[string]beads.Bead{"ci-1": held("ci-wisp-other")},
			session, nil, "",
		},
		{
			// #6324 applies to heartbeat as well: a session-name-only match is
			// a shared chair, not this session's own identity.
			"a heartbeat of a claim held under the session name is not the session's own",
			[]string{"heartbeat", "ci-1"},
			map[string]beads.Bead{"ci-1": held("rig--gc__review-synthesizer-1-pool")},
			session, strPtr("city-default-actor"), "",
		},
		{
			"a heartbeat of a claim held under the alias is not the session's own",
			[]string{"heartbeat", "ci-1"},
			map[string]beads.Bead{"ci-1": held("reviewer")},
			map[string]string{"GC_SESSION_ID": "ci-wisp-fe8", "GC_ALIAS": "reviewer", "BEADS_ACTOR": "rig--gc__review-synthesizer-1-pool"},
			nil, "",
		},
		{
			"an unread heartbeat target leaves bd's check to decide",
			[]string{"heartbeat", "ci-1"},
			map[string]beads.Bead{},
			session, nil, "",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			effective := tc.env["BEADS_ACTOR"]
			if tc.effective != nil {
				effective = *tc.effective
			}
			if got := closeActorForOwnClaim(tc.args, tc.targets, getenv(tc.env), effective); got != tc.want {
				t.Fatalf("closeActorForOwnClaim(%v) = %q, want %q", tc.args, got, tc.want)
			}
		})
	}
}

func TestWithEnvValueReplacesEveryPriorEntry(t *testing.T) {
	got := withEnvValue([]string{"A=1", "BEADS_ACTOR=old", "B=2", "BEADS_ACTOR=older"}, "BEADS_ACTOR", "new")
	if want := []string{"A=1", "B=2", "BEADS_ACTOR=new"}; !slices.Equal(got, want) {
		t.Fatalf("withEnvValue = %v, want %v", got, want)
	}
}

// TestOwnClaimCloseEnvRewritesTheChildActor pins the doBd wiring: the bd
// child's BEADS_ACTOR, not the process env's, is what the own-claim rewrite
// compares and replaces.
func TestOwnClaimCloseEnvRewritesTheChildActor(t *testing.T) {
	session := map[string]string{"GC_SESSION_ID": "ci-wisp-fe8", "BEADS_ACTOR": "process-actor"}
	getenv := func(k string) string { return session[k] }
	targets := map[string]beads.Bead{"ci-1": {ID: "ci-1", Assignee: "ci-wisp-fe8"}}
	childEnv := []string{"PATH=/bin", "BEADS_ACTOR=child-actor", "BEADS_ACTOR=child-actor-dup"}

	got := ownClaimCloseEnv(childEnv, []string{"close", "ci-1", "--reason", "done"}, targets, getenv)
	if want := []string{"PATH=/bin", "BEADS_ACTOR=ci-wisp-fe8"}; !slices.Equal(got, want) {
		t.Fatalf("ownClaimCloseEnv() = %v, want %v", got, want)
	}

	other := map[string]beads.Bead{"ci-1": {ID: "ci-1", Assignee: "ci-wisp-other"}}
	if got := ownClaimCloseEnv(childEnv, []string{"close", "ci-1"}, other, getenv); !slices.Equal(got, childEnv) {
		t.Fatalf("ownClaimCloseEnv(another session's claim) = %v, want the child env unchanged", got)
	}
}

// TestOwnClaimActorTargets pins which bd invocations the own-claim actor
// rewrite applies to: the close forms, and the lone-id heartbeat that
// rewriteBdHeartbeatArgs guarantees; nothing else.
func TestOwnClaimActorTargets(t *testing.T) {
	for _, tc := range []struct {
		name    string
		args    []string
		wantIDs []string
		wantOK  bool
	}{
		{"close", []string{"close", "ci-1", "--reason", "done"}, []string{"ci-1"}, true},
		{"update to closed", []string{"update", "ci-1", "--status", "closed"}, []string{"ci-1"}, true},
		{"lone-id heartbeat", []string{"heartbeat", "ci-1"}, []string{"ci-1"}, true},
		{"heartbeat without an id", []string{"heartbeat"}, nil, false},
		{"heartbeat with a blank id", []string{"heartbeat", "  "}, nil, false},
		{"heartbeat with extra args", []string{"heartbeat", "ci-1", "ci-2"}, nil, false},
		{"metadata update", []string{"update", "ci-1", "--set-metadata", "gc.outcome=pass"}, nil, false},
		{"show", []string{"show", "ci-1"}, nil, false},
		{"empty", nil, nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ids, ok := ownClaimActorTargets(tc.args)
			if ok != tc.wantOK || !slices.Equal(ids, tc.wantIDs) {
				t.Fatalf("ownClaimActorTargets(%v) = (%v, %v), want (%v, %v)", tc.args, ids, ok, tc.wantIDs, tc.wantOK)
			}
		})
	}
}

// TestGcBdSessionHeartbeatReadsItsClaimWhereBdResolvesIDsExactly pins the
// claim-identity heartbeat (gc-ox80c) on a scope whose bd resolves prefixed
// ids exactly. The exact-ID write guard skips its read there
// (bdWriteGuardNeedsRead), but the heartbeat needs the bead's assignee to act
// under the claim identity, so a session's heartbeat still reads it once.
// Outside a session, or when the child already acts as the session's id,
// nothing consumes the bead and the read stays skipped.
func TestGcBdSessionHeartbeatReadsItsClaimWhereBdResolvesIDsExactly(t *testing.T) {
	run := func(t *testing.T, sessionID, ambientActor string) (reads int, actor string) {
		t.Helper()
		_, cityDir := bdSQLRefusalCityDir(t, "")
		setCwd(t, cityDir)
		writeBoundBackendMetadata(t, cityDir, "http")
		binDir := t.TempDir()
		logPath := filepath.Join(t.TempDir(), "bd-calls.log")
		script := "#!/bin/sh\nprintf '%s\\n' \"$*\" >> " + logPath + "\n" +
			"case \"$1\" in\n" +
			"  show) printf '%s' '[{\"id\":\"demo-abc\",\"assignee\":\"lx-sess\",\"status\":\"in_progress\",\"issue_type\":\"task\"}]' ;;\n" +
			"  heartbeat) printf 'actor=%s\\n' \"${BEADS_ACTOR:-}\" >> " + logPath + " ;;\n" +
			"esac\nexit 0\n"
		if err := os.WriteFile(filepath.Join(binDir, "bd"), []byte(script), 0o755); err != nil {
			t.Fatal(err)
		}
		t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
		t.Setenv("GC_SESSION_ID", sessionID)
		t.Setenv("GC_SESSION_NAME", "gc-pool-name")
		t.Setenv("GC_ALIAS", "")
		t.Setenv("BEADS_ACTOR", ambientActor)

		var stdout, stderr bytes.Buffer
		if code := doBd([]string{"heartbeat", "demo-abc"}, &stdout, &stderr); code != 0 {
			t.Fatalf("doBd(heartbeat) = %d, want 0; stderr=%q", code, stderr.String())
		}
		data, err := os.ReadFile(logPath)
		if err != nil {
			t.Fatal(err)
		}
		log := string(data)
		if _, after, ok := strings.Cut(log, "actor="); ok {
			actor = strings.TrimSpace(after)
		}
		return strings.Count(log, "show --json demo-abc"), actor
	}

	t.Run("a session's heartbeat reads its claim and refreshes as the claim id", func(t *testing.T) {
		reads, actor := run(t, "lx-sess", "gc-pool-name")
		if reads != 1 {
			t.Fatalf("guard reads = %d, want 1", reads)
		}
		if actor != "lx-sess" {
			t.Fatalf("heartbeat ran as BEADS_ACTOR=%q, want the owning session id %q", actor, "lx-sess")
		}
	})
	t.Run("a session already acting as its claim id skips the read", func(t *testing.T) {
		reads, actor := run(t, "lx-sess", "lx-sess")
		if reads != 0 {
			t.Fatalf("guard reads = %d, want 0: the actor needs no rewrite", reads)
		}
		if actor != "lx-sess" {
			t.Fatalf("heartbeat ran as BEADS_ACTOR=%q, want %q", actor, "lx-sess")
		}
	})
	t.Run("outside a session the read stays skipped", func(t *testing.T) {
		reads, actor := run(t, "", "gc-pool-name")
		if reads != 0 {
			t.Fatalf("guard reads = %d, want 0", reads)
		}
		if actor != "gc-pool-name" {
			t.Fatalf("heartbeat ran as BEADS_ACTOR=%q, want the ambient actor %q", actor, "gc-pool-name")
		}
	})
}
