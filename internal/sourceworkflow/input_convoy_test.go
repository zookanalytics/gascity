package sourceworkflow

import (
	"testing"

	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
	convoycore "github.com/gastownhall/gascity/internal/convoy"
)

func graphRoot(t *testing.T, store beads.Store, title, inputConvoyID, rootKey string) beads.Bead {
	t.Helper()
	root, err := store.Create(beads.Bead{
		Title: title,
		Type:  "task",
		Metadata: map[string]string{
			beadmeta.KindMetadataKey:            beadmeta.KindWorkflow,
			beadmeta.FormulaContractMetadataKey: beadmeta.FormulaContractGraphV2,
			beadmeta.InputConvoyIDMetadataKey:   inputConvoyID,
			beadmeta.Graphv2RootKeyMetadataKey:  rootKey,
		},
	})
	if err != nil {
		t.Fatalf("create %s: %v", title, err)
	}
	return root
}

// ListLiveInputConvoyRoots reports the live graph.v2 roots already poured over
// an input convoy, skipping closed roots, non-workflow beads, legacy
// (non-graph.v2) roots, and the caller's own root key.
func TestListLiveInputConvoyRoots(t *testing.T) {
	store := beads.NewMemStore()
	convoy, err := store.Create(beads.Bead{Title: "input convoy", Type: "convoy", Status: "open"})
	if err != nil {
		t.Fatalf("create convoy: %v", err)
	}

	live := graphRoot(t, store, "live root", convoy.ID, "key-live")
	closed := graphRoot(t, store, "closed root", convoy.ID, "key-closed")
	if err := store.Close(closed.ID); err != nil {
		t.Fatalf("close root: %v", err)
	}
	graphRoot(t, store, "own root", convoy.ID, "key-mine")
	graphRoot(t, store, "other convoy", "other-convoy", "key-other")
	if _, err := store.Create(beads.Bead{
		Title:  "legacy root",
		Type:   "task",
		Status: "open",
		Metadata: map[string]string{
			beadmeta.KindMetadataKey:          beadmeta.KindWorkflow,
			beadmeta.InputConvoyIDMetadataKey: convoy.ID,
		},
	}); err != nil {
		t.Fatalf("create legacy root: %v", err)
	}
	if _, err := store.Create(beads.Bead{
		Title:    "plain member",
		Type:     "task",
		Status:   "open",
		Metadata: map[string]string{beadmeta.InputConvoyIDMetadataKey: convoy.ID},
	}); err != nil {
		t.Fatalf("create plain member: %v", err)
	}

	roots, err := ListLiveInputConvoyRoots(store, convoy.ID, "key-mine")
	if err != nil {
		t.Fatalf("ListLiveInputConvoyRoots: %v", err)
	}
	if len(roots) != 1 || roots[0].ID != live.ID {
		t.Fatalf("roots = %v, want only %s", BlockingWorkflowIDs(roots), live.ID)
	}
}

// An empty convoy ID or nil store is not an error — the caller has no convoy to
// gate on, so there is nothing to conflict with.
func TestListLiveInputConvoyRootsEmptyInputs(t *testing.T) {
	roots, err := ListLiveInputConvoyRoots(beads.NewMemStore(), "  ", "")
	if err != nil || len(roots) != 0 {
		t.Fatalf("blank convoy: (%v, %v), want (nil, nil)", BlockingWorkflowIDs(roots), err)
	}
	roots, err = ListLiveInputConvoyRoots(nil, "convoy-1", "")
	if err != nil || len(roots) != 0 {
		t.Fatalf("nil store: (%v, %v), want (nil, nil)", BlockingWorkflowIDs(roots), err)
	}
}

// The reverse walk is the only durable link from a work bead back to a
// convoy-first graph.v2 workflow: the root clears gc.source_bead_id and points
// only at its input convoy, which tracks the bead. Each pour mints a FRESH
// synthetic input convoy, so gating on a convoy ID alone cannot see a
// predecessor — the walk must start from the bead.
func TestListLiveInputConvoyRootsForItem(t *testing.T) {
	store := beads.NewMemStore()
	work, err := store.Create(beads.Bead{Title: "work", Type: "task", Status: "open"})
	if err != nil {
		t.Fatalf("create work: %v", err)
	}
	first, err := store.Create(beads.Bead{Title: "input convoy 1", Type: "convoy", Status: "open"})
	if err != nil {
		t.Fatalf("create convoy 1: %v", err)
	}
	second, err := store.Create(beads.Bead{Title: "input convoy 2", Type: "convoy", Status: "open"})
	if err != nil {
		t.Fatalf("create convoy 2: %v", err)
	}
	for _, convoy := range []beads.Bead{first, second} {
		if err := convoycore.TrackItem(store, convoy.ID, work.ID); err != nil {
			t.Fatalf("TrackItem(%s): %v", convoy.ID, err)
		}
	}
	predecessor := graphRoot(t, store, "predecessor", first.ID, "key-first")
	graphRoot(t, store, "mine", second.ID, "key-mine")

	roots, err := ListLiveInputConvoyRootsForItem(store, store, work.ID, "key-mine")
	if err != nil {
		t.Fatalf("ListLiveInputConvoyRootsForItem: %v", err)
	}
	if len(roots) != 1 || roots[0].ID != predecessor.ID {
		t.Fatalf("roots = %v, want only %s", BlockingWorkflowIDs(roots), predecessor.ID)
	}

	untracked, err := store.Create(beads.Bead{Title: "untracked", Type: "task", Status: "open"})
	if err != nil {
		t.Fatalf("create untracked: %v", err)
	}
	roots, err = ListLiveInputConvoyRootsForItem(store, store, untracked.ID, "")
	if err != nil || len(roots) != 0 {
		t.Fatalf("untracked bead: (%v, %v), want (nil, nil)", BlockingWorkflowIDs(roots), err)
	}
}
