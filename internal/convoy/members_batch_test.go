package convoy

import (
	"reflect"
	"testing"

	"github.com/gastownhall/gascity/internal/beads"
)

// TestMembersBatchMatchesPerConvoyMembers is the correctness contract for the
// batched read: for every convoy shape the list path can hold — tracked
// members, legacy parent-child children, a mix of the two, dangling tracks, a
// convoy with no members — MembersBatch must return exactly what a per-convoy
// Members call returns. It is the guard that the batching changes throughput,
// not output.
func TestMembersBatchMatchesPerConvoyMembers(t *testing.T) {
	store := beads.NewMemStore()

	// Convoy A: two tracked members, one open, one closed.
	convoyA, _ := store.Create(beads.Bead{Title: "convoy A", Type: "convoy"})
	a1, _ := store.Create(beads.Bead{Title: "a1", Type: "task", Status: "open"})
	a2, _ := store.Create(beads.Bead{Title: "a2", Type: "task", Status: "open"})
	if err := store.Close(a2.ID); err != nil {
		t.Fatalf("close a2: %v", err)
	}
	trackOrFatal(t, store, convoyA.ID, a1.ID)
	trackOrFatal(t, store, convoyA.ID, a2.ID)

	// Convoy B: legacy parent-child children (ParentID), one open, one closed.
	convoyB, _ := store.Create(beads.Bead{Title: "convoy B", Type: "convoy"})
	b1, _ := store.Create(beads.Bead{Title: "b1", Type: "task", Status: "open", ParentID: convoyB.ID})
	b2, _ := store.Create(beads.Bead{Title: "b2", Type: "task", Status: "open", ParentID: convoyB.ID})
	if err := store.Close(b2.ID); err != nil {
		t.Fatalf("close b2: %v", err)
	}
	_ = b1

	// Convoy C: mix of a legacy child and a tracked member, plus a dangling
	// tracks edge to an id no bead backs.
	convoyC, _ := store.Create(beads.Bead{Title: "convoy C", Type: "convoy"})
	c1, _ := store.Create(beads.Bead{Title: "c1", Type: "task", Status: "open", ParentID: convoyC.ID})
	c2, _ := store.Create(beads.Bead{Title: "c2", Type: "task", Status: "open"})
	trackOrFatal(t, store, convoyC.ID, c2.ID)
	if err := store.DepAdd(convoyC.ID, "gc-nonexistent", TrackingDepType); err != nil {
		t.Fatalf("add dangling dep: %v", err)
	}
	_ = c1

	// Convoy D: no members at all.
	convoyD, _ := store.Create(beads.Bead{Title: "convoy D", Type: "convoy"})

	ids := []string{convoyA.ID, convoyB.ID, convoyC.ID, convoyD.ID}

	for _, includeClosed := range []bool{true, false} {
		batch, err := MembersBatch(store, ids, includeClosed)
		if err != nil {
			t.Fatalf("MembersBatch(includeClosed=%v): %v", includeClosed, err)
		}
		for _, id := range ids {
			want, err := Members(store, id, includeClosed)
			if err != nil {
				t.Fatalf("Members(%s, includeClosed=%v): %v", id, includeClosed, err)
			}
			got := batch[id]
			if !reflect.DeepEqual(got, want) {
				t.Errorf("includeClosed=%v convoy %s:\n batch = %+v\n want  = %+v", includeClosed, id, got, want)
			}
		}
	}
}

// TestMembersBatchEmptyInputs pins the degenerate cases: no convoy ids returns
// an empty (non-nil) map, and a nil store is the same caller error Members
// reports rather than a panic.
func TestMembersBatchEmptyInputs(t *testing.T) {
	store := beads.NewMemStore()
	got, err := MembersBatch(store, nil, true)
	if err != nil {
		t.Fatalf("MembersBatch(nil ids): %v", err)
	}
	if got == nil || len(got) != 0 {
		t.Errorf("MembersBatch(nil ids) = %+v, want empty non-nil map", got)
	}

	var typedNil *beads.MemStore
	if _, err := MembersBatch(typedNil, []string{"gc-1"}, true); err == nil {
		t.Error("MembersBatch(nil store) = nil error, want ErrNoConvoyClass")
	}
}

func trackOrFatal(t *testing.T, store beads.Store, convoyID, itemID string) {
	t.Helper()
	if err := TrackItem(store, convoyID, itemID, store); err != nil {
		t.Fatalf("TrackItem %s -> %s: %v", convoyID, itemID, err)
	}
}
