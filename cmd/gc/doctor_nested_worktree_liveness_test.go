package main

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/pathutil"
)

// stubLiveScan installs a fixed process-table scan result for one test.
func stubLiveScan(t *testing.T, state liveWorktreeState) {
	t.Helper()
	prev := collectLiveWorktreeStateFn
	collectLiveWorktreeStateFn = func() liveWorktreeState { return state }
	t.Cleanup(func() { collectLiveWorktreeStateFn = prev })
}

// memStoreFactory returns a store factory backed by an empty MemStore, which
// yields a snapshot with no open sessions.
func memStoreFactory() func(string) (beads.Store, error) {
	return func(string) (beads.Store, error) { return beads.NewMemStore(), nil }
}

// TestDoctorNestedWorktreeLiveness_ReportsLiveProcessCWD pins the wiring half
// of the gate: the doctor's probe sees a process running in a candidate
// worktree and names the signal, which is what makes the prune check reject
// it.
func TestDoctorNestedWorktreeLiveness_ReportsLiveProcessCWD(t *testing.T) {
	wt := t.TempDir()
	nested := filepath.Join(wt, "sub")
	stubLiveScan(t, liveWorktreeState{
		cwds:    []string{pathutil.NormalizePathForCompare(nested)},
		scanned: true,
		source:  liveScanSourceProc,
	})

	lookup, err := doctorNestedWorktreeLiveness(t.TempDir(), true, memStoreFactory())()
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	live, reason := lookup(wt)
	if !live {
		t.Fatalf("lookup(%s) = false, want true: a process cwd sits beneath it", wt)
	}
	if !strings.Contains(reason, "live process cwd") {
		t.Errorf("reason = %q, want it to name the process-cwd signal", reason)
	}

	if live, _ := lookup(t.TempDir()); live {
		t.Error("an unrelated directory with no live signal was reported live")
	}
}

// TestDoctorNestedWorktreeLiveness_UnscannedProcessTableIsError pins the
// fail-closed contract: an indeterminate scan must surface as an error, never
// as an empty live set, which the prune check would read as "nothing is
// running" and prune everything.
func TestDoctorNestedWorktreeLiveness_UnscannedProcessTableIsError(t *testing.T) {
	stubLiveScan(t, liveWorktreeState{scanned: false})

	lookup, err := doctorNestedWorktreeLiveness(t.TempDir(), true, memStoreFactory())()
	if err == nil {
		t.Fatal("probe returned no error for an unscannable process table")
	}
	if lookup != nil {
		t.Error("probe returned a lookup alongside an error; callers would use it")
	}
	if !strings.Contains(err.Error(), "process table") {
		t.Errorf("error = %v, want it to name the failed mechanism", err)
	}
}

// TestDoctorNestedWorktreeLiveness_StorePreflightFailureIsError pins that the
// session half of the gate failing is also fail-closed. The process scan alone
// cannot see a session whose process this scan misses, so a half-answer is not
// enough to authorize a removal.
func TestDoctorNestedWorktreeLiveness_StorePreflightFailureIsError(t *testing.T) {
	stubLiveScan(t, liveWorktreeState{scanned: true, source: liveScanSourceProc})

	_, err := doctorNestedWorktreeLiveness(t.TempDir(), false, memStoreFactory())()
	if err == nil {
		t.Fatal("probe returned no error when the bead-store preflight had already failed")
	}
	if !strings.Contains(err.Error(), "preflight") {
		t.Errorf("error = %v, want it to cite the failed preflight", err)
	}
}

// TestDoctorNestedWorktreeLiveness_StoreOpenFailureIsError covers the store
// passing preflight but failing to open at probe time.
func TestDoctorNestedWorktreeLiveness_StoreOpenFailureIsError(t *testing.T) {
	stubLiveScan(t, liveWorktreeState{scanned: true, source: liveScanSourceProc})

	newStore := func(string) (beads.Store, error) { return nil, errors.New("store is wedged") }
	_, err := doctorNestedWorktreeLiveness(t.TempDir(), true, newStore)()
	if err == nil {
		t.Fatal("probe returned no error when the bead store could not be opened")
	}
	if !strings.Contains(err.Error(), "store is wedged") {
		t.Errorf("error = %v, want it to wrap the open failure", err)
	}
}

// sessionBeadWithWorkerDir builds an open session bead whose recorded agent
// working directory is dir.
func sessionBeadWithWorkerDir(dir string) beads.Bead {
	return beads.Bead{
		Title:  "session-with-workdir",
		Type:   sessionBeadType,
		Status: "open",
		Labels: []string{sessionBeadLabel},
		Metadata: map[string]string{
			"session_name":                "s-live",
			beadmeta.WorkerDirMetadataKey: dir,
		},
	}
}

// TestDoctorNestedWorktreeLiveness_ReportsSessionWorkDir pins the second
// signal: a session whose recorded working directory is the candidate
// protects it even when no process cwd matches.
func TestDoctorNestedWorktreeLiveness_ReportsSessionWorkDir(t *testing.T) {
	wt := t.TempDir()
	stubLiveScan(t, liveWorktreeState{scanned: true, source: liveScanSourceProc})

	mem := beads.NewMemStore()
	if _, err := mem.Create(sessionBeadWithWorkerDir(wt)); err != nil {
		t.Fatalf("create session bead: %v", err)
	}
	newStore := func(string) (beads.Store, error) { return mem, nil }

	lookup, err := doctorNestedWorktreeLiveness(t.TempDir(), true, newStore)()
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	live, reason := lookup(wt)
	if !live {
		t.Fatalf("lookup(%s) = false, want true: an open session records it as its working dir", wt)
	}
	if !strings.Contains(reason, "active session dir") {
		t.Errorf("reason = %q, want it to name the session signal", reason)
	}
}
