package convoy

import (
	"reflect"
	"sort"
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
	ids := seedMembershipShapes(t, store)

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

// TestMembersInMatchesPerMemberGet is the correctness contract for the keyed
// member read: for every convoy shape, MembersIn returns exactly the members
// that resolving each tracked member alone with a Get returns.
func TestMembersInMatchesPerMemberGet(t *testing.T) {
	store := beads.NewMemStore()
	ids := seedMembershipShapes(t, store)
	classes := MemberClasses{Convoy: store}

	for _, includeClosed := range []bool{true, false} {
		for _, id := range ids {
			want := membersByGet(t, classes, id, includeClosed)
			got, err := MembersIn(classes, id, includeClosed)
			if err != nil {
				t.Fatalf("MembersIn(%s, includeClosed=%v): %v", id, includeClosed, err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("includeClosed=%v convoy %s:\n keyed   = %+v\n per-Get = %+v", includeClosed, id, got, want)
			}
		}
	}
}

// TestMembersReadTrackedMembersWithOneKeyedRead pins the read shape of both
// member readers on one class store: the tracked members are resolved by a
// single List keyed by their ids, over every status and both tiers, and never
// by a Get per member.
func TestMembersReadTrackedMembersWithOneKeyedRead(t *testing.T) {
	backing := beads.NewMemStore()
	ids := seedMembershipShapes(t, backing)
	var tracked []string
	for _, id := range ids {
		deps, err := backing.DepList(id, "down")
		if err != nil {
			t.Fatalf("DepList(%s): %v", id, err)
		}
		tracked = append(tracked, trackedIDs(deps)...)
	}
	sort.Strings(tracked)

	readers := map[string]func(beads.Store) error{
		"MembersIn": func(store beads.Store) error {
			for _, id := range ids {
				if _, err := MembersIn(MemberClasses{Convoy: store}, id, true); err != nil {
					return err
				}
			}
			return nil
		},
		"MembersBatch": func(store beads.Store) error {
			_, err := MembersBatch(store, ids, true)
			return err
		},
	}
	for name, read := range readers {
		t.Run(name, func(t *testing.T) {
			spy := &readSpyStore{Store: backing}
			if err := read(spy); err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			if len(spy.gets) != 0 {
				t.Fatalf("%s read members with Get %v; want keyed Lists", name, spy.gets)
			}
			var got []string
			for _, q := range spy.keyedLists() {
				if !q.IncludeClosed || q.TierMode != beads.TierBoth {
					t.Fatalf("%s keyed List = %+v; want every status and both tiers", name, q)
				}
				got = append(got, q.IDs...)
			}
			sort.Strings(got)
			if !reflect.DeepEqual(got, tracked) {
				t.Fatalf("%s keyed ids = %v, want each tracked member once (%v)", name, got, tracked)
			}
			if name == "MembersBatch" && len(spy.keyedLists()) != 1 {
				t.Fatalf("MembersBatch issued %d keyed Lists, want one for every convoy", len(spy.keyedLists()))
			}
		})
	}
}

// seedMembershipShapes seeds every convoy shape a member read must handle:
// tracked members, legacy parent-child children, a mix of the two, a dangling
// tracks edge, a convoy with no members, and an ephemeral tracked member. It
// returns the convoy ids.
func seedMembershipShapes(t *testing.T, store *beads.MemStore) []string {
	t.Helper()

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

	// Convoy E: an ephemeral (wisp-tier) tracked member. Members resolves each
	// member through a tier-blind Get, so the batched keyed member read must
	// span both tiers to return the real bead here rather than the dangling
	// placeholder a tier-filtered read would leave.
	convoyE, _ := store.Create(beads.Bead{Title: "convoy E", Type: "convoy"})
	e1, _ := store.Create(beads.Bead{Title: "e1", Type: "task", Status: "open", Ephemeral: true})
	trackOrFatal(t, store, convoyE.ID, e1.ID)

	return []string{convoyA.ID, convoyB.ID, convoyC.ID, convoyD.ID, convoyE.ID}
}

// membersByGet resolves a convoy's members the way MembersIn's keyed read must
// reproduce: each tracked member resolved alone with resolveMember.
func membersByGet(t *testing.T, classes MemberClasses, convoyID string, includeClosed bool) []beads.Bead {
	t.Helper()
	legacy, err := classes.Convoy.List(beads.ListQuery{ParentID: convoyID, IncludeClosed: includeClosed, Sort: beads.SortCreatedAsc})
	if err != nil {
		t.Fatalf("listing legacy children of %s: %v", convoyID, err)
	}
	deps, err := classes.Convoy.DepList(convoyID, "down")
	if err != nil {
		t.Fatalf("DepList(%s): %v", convoyID, err)
	}
	members, err := assembleMembers(legacy, deps, includeClosed, func(id string) (beads.Bead, error) {
		item, _, err := classes.resolveMember(id)
		return item, err
	})
	if err != nil {
		t.Fatalf("resolving members of %s one at a time: %v", convoyID, err)
	}
	return members
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
