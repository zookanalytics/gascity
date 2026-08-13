package dispatch

import (
	"errors"
	"fmt"
	"testing"

	"github.com/gastownhall/gascity/internal/beads"
)

// bdBlockedCloseStore mirrors the bead store's refusal to close a bead that
// still carries an open blocking dependency. graph.v2 gives every workflow
// root a blocks edge onto its own workflow-finalize step bead
// (formula.addWorkflowRootDeps), and the finalize path closes the root while
// that step bead is still open — so once the store began enforcing this rule,
// every workflow-finalize in the city quarantined with
// "cannot close blocked issue" (gc-892g5). MemStore does not enforce the rule,
// so the regression is only reproducible behind this wrapper.
type bdBlockedCloseStore struct {
	beads.Store
}

// openBlockers returns the IDs of open beads that block id via a
// ready-blocking dependency type.
func (s *bdBlockedCloseStore) openBlockers(id string) ([]string, error) {
	deps, err := s.DepList(id, "down")
	if err != nil {
		return nil, err
	}
	var open []string
	for _, dep := range deps {
		if !beads.IsReadyBlockingDependencyType(dep.Type) {
			continue
		}
		blocker, err := s.Get(dep.DependsOnID)
		if err != nil {
			if errors.Is(err, beads.ErrNotFound) {
				continue
			}
			return nil, err
		}
		if blocker.Status != "closed" {
			open = append(open, blocker.ID)
		}
	}
	return open, nil
}

// rejectBlockedClose reproduces the store's verbatim refusal text so tests
// assert against the same signature operators see in gc.last_finalize_error.
func (s *bdBlockedCloseStore) rejectBlockedClose(id string) error {
	open, err := s.openBlockers(id)
	if err != nil {
		return err
	}
	if len(open) == 0 {
		return nil
	}
	return fmt.Errorf("cannot close blocked issue: %s is blocked by %v", id, open)
}

func (s *bdBlockedCloseStore) Close(id string) error {
	if err := s.rejectBlockedClose(id); err != nil {
		return err
	}
	return s.Store.Close(id)
}

func (s *bdBlockedCloseStore) Update(id string, opts beads.UpdateOpts) error {
	if opts.Status != nil && *opts.Status == "closed" {
		if err := s.rejectBlockedClose(id); err != nil {
			return err
		}
	}
	return s.Store.Update(id, opts)
}

// newFinalizeBlockerFixture builds the minimal graph.v2 shape that reproduces
// gc-892g5: a workflow root, a closed work step, and a workflow-finalize step
// bead carrying the blocks edge onto the root.
func newFinalizeBlockerFixture(t *testing.T) (beads.Store, beads.Bead, beads.Bead) {
	t.Helper()

	store := &bdBlockedCloseStore{Store: beads.NewMemStore()}
	root := mustCreateWorkflowBead(t, store, beads.Bead{
		Title: "workflow",
		Type:  "task",
		Metadata: map[string]string{
			"gc.kind":             "workflow",
			"gc.formula_contract": "graph.v2",
		},
	})
	step := mustCreateWorkflowBead(t, store, beads.Bead{
		Title:  "work step",
		Type:   "task",
		Status: "closed",
		Metadata: map[string]string{
			"gc.outcome": "pass",
		},
	})
	finalizer := mustCreateWorkflowBead(t, store, beads.Bead{
		Title: "Finalize workflow",
		Type:  "task",
		Metadata: map[string]string{
			"gc.kind":         "workflow-finalize",
			"gc.root_bead_id": root.ID,
		},
	})

	mustDepAdd(t, store, finalizer.ID, step.ID, "blocks")
	mustDepAdd(t, store, root.ID, finalizer.ID, "blocks")

	return store, root, finalizer
}

// mustRootDependsOn reports whether root still carries a dependency edge onto
// dependsOnID.
func mustRootDependsOn(t *testing.T, store beads.Store, rootID, dependsOnID string) bool {
	t.Helper()
	deps, err := store.DepList(rootID, "down")
	if err != nil {
		t.Fatalf("dep list %s: %v", rootID, err)
	}
	for _, dep := range deps {
		if dep.DependsOnID == dependsOnID {
			return true
		}
	}
	return false
}

// TestProcessWorkflowFinalizeDetachesOwnRootBlockerBeforeClosingRoot is the
// regression test for gc-892g5: workflow-finalize must detach its own blocks
// edge onto the workflow root before closing that root, or every graph.v2
// workflow quarantines its finalizer and strands the root open.
func TestProcessWorkflowFinalizeDetachesOwnRootBlockerBeforeClosingRoot(t *testing.T) {
	t.Parallel()

	store, root, finalizer := newFinalizeBlockerFixture(t)

	result, err := ProcessControl(store, finalizer, ProcessOptions{})
	if err != nil {
		t.Fatalf("ProcessControl(workflow-finalize): %v", err)
	}
	if !result.Processed || result.Action != "workflow-pass" {
		t.Fatalf("workflow result = %+v, want processed workflow-pass", result)
	}

	rootAfter, err := store.Get(root.ID)
	if err != nil {
		t.Fatalf("get workflow root: %v", err)
	}
	if rootAfter.Status != "closed" {
		t.Fatalf("workflow root status = %q, want closed", rootAfter.Status)
	}
	if got := rootAfter.Metadata["gc.outcome"]; got != "pass" {
		t.Fatalf("workflow root outcome = %q, want pass", got)
	}

	finalizerAfter, err := store.Get(finalizer.ID)
	if err != nil {
		t.Fatalf("get finalizer: %v", err)
	}
	if finalizerAfter.Status != "closed" {
		t.Fatalf("finalizer status = %q, want closed", finalizerAfter.Status)
	}
	if got := finalizerAfter.Metadata["gc.final_disposition"]; got != "" {
		t.Fatalf("finalizer disposition = %q, want unset (not quarantined)", got)
	}

	if mustRootDependsOn(t, store, root.ID, finalizer.ID) {
		t.Fatalf("root %s still blocked by finalizer %s after finalize", root.ID, finalizer.ID)
	}
}

// TestProcessWorkflowFinalizeDetachIsIdempotent covers the retry path: the
// finalize bead stays open until the root is durably closed, so a controller
// crash between the detach and the root close replays the detach. It must not
// fail on an edge that is already gone.
func TestProcessWorkflowFinalizeDetachIsIdempotent(t *testing.T) {
	t.Parallel()

	store, root, finalizer := newFinalizeBlockerFixture(t)
	if err := store.DepRemove(root.ID, finalizer.ID); err != nil {
		t.Fatalf("pre-remove root blocker: %v", err)
	}

	result, err := ProcessControl(store, finalizer, ProcessOptions{})
	if err != nil {
		t.Fatalf("ProcessControl(workflow-finalize): %v", err)
	}
	if !result.Processed || result.Action != "workflow-pass" {
		t.Fatalf("workflow result = %+v, want processed workflow-pass", result)
	}

	rootAfter, err := store.Get(root.ID)
	if err != nil {
		t.Fatalf("get workflow root: %v", err)
	}
	if rootAfter.Status != "closed" {
		t.Fatalf("workflow root status = %q, want closed", rootAfter.Status)
	}
}

// TestProcessWorkflowFinalizeLeavesUnrelatedRootDepsAttached keeps the detach
// scoped to the finalizer's own edge. Other edges on the root are part of the
// workflow's recorded shape and are not the finalizer's to remove.
func TestProcessWorkflowFinalizeLeavesUnrelatedRootDepsAttached(t *testing.T) {
	t.Parallel()

	store, root, finalizer := newFinalizeBlockerFixture(t)
	sibling := mustCreateWorkflowBead(t, store, beads.Bead{
		Title:  "already-closed sibling blocker",
		Type:   "task",
		Status: "closed",
		Metadata: map[string]string{
			"gc.outcome": "pass",
		},
	})
	mustDepAdd(t, store, root.ID, sibling.ID, "blocks")

	if _, err := ProcessControl(store, finalizer, ProcessOptions{}); err != nil {
		t.Fatalf("ProcessControl(workflow-finalize): %v", err)
	}

	if mustRootDependsOn(t, store, root.ID, finalizer.ID) {
		t.Fatalf("root %s still blocked by finalizer %s after finalize", root.ID, finalizer.ID)
	}
	if !mustRootDependsOn(t, store, root.ID, sibling.ID) {
		t.Fatalf("finalize removed unrelated root dep onto %s", sibling.ID)
	}
}

// TestProcessWorkflowFinalizeOrphanedRootSurvivesBlockerDetach keeps the
// missing-root path intact: a root that no longer exists must still close the
// finalizer with gc.outcome=missing-root rather than erroring out of the
// detach.
func TestProcessWorkflowFinalizeOrphanedRootSurvivesBlockerDetach(t *testing.T) {
	t.Parallel()

	store := &bdBlockedCloseStore{Store: beads.NewMemStore()}
	finalizer := mustCreateWorkflowBead(t, store, beads.Bead{
		Title: "Finalize workflow",
		Type:  "task",
		Metadata: map[string]string{
			"gc.kind":         "workflow-finalize",
			"gc.root_bead_id": "gc-missing-root",
		},
	})

	result, err := ProcessControl(store, finalizer, ProcessOptions{})
	if err != nil {
		t.Fatalf("ProcessControl(workflow-finalize): %v", err)
	}
	if !result.Processed || result.Action != "workflow-missing_root" {
		t.Fatalf("workflow result = %+v, want processed workflow-missing_root", result)
	}

	finalizerAfter, err := store.Get(finalizer.ID)
	if err != nil {
		t.Fatalf("get finalizer: %v", err)
	}
	if finalizerAfter.Status != "closed" {
		t.Fatalf("finalizer status = %q, want closed", finalizerAfter.Status)
	}
}
