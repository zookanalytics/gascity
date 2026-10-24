package session

import (
	"context"
	"maps"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/beads/contract"
	"github.com/gastownhall/gascity/internal/runtime"
)

// TestWorkerDirFromInfoMatchesContract is the reprojection oracle for the
// Info.WorkerDir field-add: WorkerDirFromInfo(infoFromPersistedBead(b)) must be
// byte-identical to contract.WorkerDirFromMetadata(b.Metadata) across the
// canonical/legacy/both/neither/whitespace corpus. It is load-bearing: it fails
// if WorkerDir stops mirroring the canonical worker_dir key, if the legacy
// fallback drops Info.WorkDir, or if the TrimSpace normalization diverges (mutate
// any of those and a fixture row breaks).
func TestWorkerDirFromInfoMatchesContract(t *testing.T) {
	cases := []struct {
		name string
		meta map[string]string
		want string
	}{
		{"canonical-only", map[string]string{beadmeta.WorkerDirMetadataKey: "/w/canon"}, "/w/canon"},
		{"legacy-only", map[string]string{"work_dir": "/w/legacy"}, "/w/legacy"},
		{"both-canonical-wins", map[string]string{beadmeta.WorkerDirMetadataKey: "/w/canon", "work_dir": "/w/legacy"}, "/w/canon"},
		{"neither", map[string]string{}, ""},
		{"canonical-whitespace-falls-back", map[string]string{beadmeta.WorkerDirMetadataKey: "   ", "work_dir": "/w/legacy"}, "/w/legacy"},
		{"canonical-trimmed", map[string]string{beadmeta.WorkerDirMetadataKey: "  /w/canon  "}, "/w/canon"},
		{"legacy-trimmed", map[string]string{"work_dir": "  /w/legacy  "}, "/w/legacy"},
		{"both-whitespace", map[string]string{beadmeta.WorkerDirMetadataKey: "  ", "work_dir": "  "}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := beads.Bead{ID: "s-1", Type: BeadType, Status: "open", Labels: []string{LabelSession}, Metadata: tc.meta}
			info := infoFromPersistedBead(b)
			got := WorkerDirFromInfo(info)
			wantContract := contract.WorkerDirFromMetadata(b.Metadata)
			if got != wantContract {
				t.Fatalf("WorkerDirFromInfo diverged from contract.WorkerDirFromMetadata: got=%q contract=%q", got, wantContract)
			}
			if got != tc.want {
				t.Fatalf("WorkerDirFromInfo(%v) = %q, want %q", tc.meta, got, tc.want)
			}
		})
	}
}

// Kills a start record that rewrites an unchanged launch dir, or that a start
// in the session's own work dir leaves standing: the patch names a new task
// work_dir, clears a recorded one for a start in the session's own work dir,
// and is empty when the row already records the start's choice.
func TestLaunchTaskWorkDirPatch(t *testing.T) {
	for _, tc := range []struct {
		name     string
		recorded string
		dir      string
		want     MetadataPatch
	}{
		{"a task work_dir on a row that records none", "", "/wt/fe-1", MetadataPatch{LaunchTaskWorkDirMetadataKey: "/wt/fe-1"}},
		{"another task work_dir", "/wt/fe-1", "/wt/fe-2", MetadataPatch{LaunchTaskWorkDirMetadataKey: "/wt/fe-2"}},
		{"the own work dir after a task work_dir", "/wt/fe-1", "", MetadataPatch{LaunchTaskWorkDirMetadataKey: ""}},
		{"the task work_dir the row records", "/wt/fe-1", "/wt/fe-1", MetadataPatch{}},
		{"the own work dir on a row that records none", "", "", MetadataPatch{}},
		{"the recorded dir with surrounding whitespace", " /wt/fe-1 ", "/wt/fe-1", MetadataPatch{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := LaunchTaskWorkDirPatch(Info{LaunchTaskWorkDir: tc.recorded}, tc.dir); !maps.Equal(got, tc.want) {
				t.Fatalf("LaunchTaskWorkDirPatch(%q, %q) = %v, want %v", tc.recorded, tc.dir, got, tc.want)
			}
		})
	}
}

func TestCommitStartedPatchRecordsTheLaunchTaskWorkDir(t *testing.T) {
	now := time.Now()
	launch := MetadataPatch{LaunchTaskWorkDirMetadataKey: "/wt/fe-1"}
	if got := CommitStartedPatch(CommitStartedPatchInput{Now: now, LaunchTaskWorkDir: launch})[LaunchTaskWorkDirMetadataKey]; got != "/wt/fe-1" {
		t.Fatalf("launch_task_work_dir = %q, want the task work_dir the start launched in", got)
	}
	if patch := CommitStartedPatch(CommitStartedPatchInput{Now: now}); len(patch) > 0 {
		if _, ok := patch[LaunchTaskWorkDirMetadataKey]; ok {
			t.Fatalf("a commit with no launch record writes launch_task_work_dir: %v", patch)
		}
	}
}

// Kills a resume that leaves a task work_dir recorded on a session it starts
// in its own work dir. The Manager launches in the session's own work dir,
// never a task work_dir, so a resume it starts clears the record, and a resume
// on a row that records none writes none.
func TestResumeClearsTheLaunchTaskWorkDir(t *testing.T) {
	store := beads.NewMemStore()
	sp := runtime.NewFake()
	mgr := newTestManager(t, store, sp)
	info, err := mgr.CreateSession(context.Background(), CreateOptions{Template: "helper", Command: "codex", WorkDir: "/tmp", Provider: "codex", ExtraMeta: map[string]string{"session_origin": "manual"}})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	recorded := func() (string, bool) {
		t.Helper()
		b, err := store.Get(info.ID)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		dir, ok := b.Metadata[LaunchTaskWorkDirMetadataKey]
		return dir, ok
	}
	resume := func() {
		t.Helper()
		if err := sp.Stop(info.SessionName); err != nil {
			t.Fatalf("Stop: %v", err)
		}
		if err := mgr.Start(context.Background(), testActor(mgr, ActorOperator), info.ID, "codex", runtime.Config{WorkDir: "/tmp"}); err != nil {
			t.Fatalf("Start(resume): %v", err)
		}
	}

	resume()
	if dir, ok := recorded(); ok {
		t.Fatalf("a resume on a row with no launch record wrote launch_task_work_dir = %q", dir)
	}
	if err := store.SetMetadata(info.ID, LaunchTaskWorkDirMetadataKey, "/wt/fe-1"); err != nil {
		t.Fatalf("SetMetadata: %v", err)
	}
	resume()
	if dir, _ := recorded(); dir != "" {
		t.Fatalf("launch_task_work_dir after a resume in the session's own work dir = %q, want cleared", dir)
	}
}
