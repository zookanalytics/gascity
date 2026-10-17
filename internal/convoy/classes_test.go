package convoy

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/beads"
)

// unreadableStore stands in for a named class whose backend is unavailable:
// every Get fails with an I/O-shaped error, which is emphatically NOT
// beads.ErrNotFound. It is the only way to tell the two directions of the
// partial-result rule apart — a store that is silent because nobody asked it,
// and a store that is silent because it broke.
type unreadableStore struct {
	beads.Store
	err   error
	reads int
}

func (s *unreadableStore) Get(string) (beads.Bead, error) {
	s.reads++
	return beads.Bead{}, s.err
}

// corruptRowStore serves the ids in corrupt the way the native store serves a
// row whose metadata it cannot project: Get fails with beads.ErrMetadataParse.
type corruptRowStore struct {
	beads.Store
	corrupt map[string]bool
}

func (s *corruptRowStore) Get(id string) (beads.Bead, error) {
	if s.corrupt[id] {
		return beads.Bead{}, fmt.Errorf("parsing metadata for bead %q: %w", id, beads.ErrMetadataParse)
	}
	return s.Store.Get(id)
}

// batchingStore offers the exact batch read a class store shares across
// members, and records each batch read and Get it answers. Its batch read
// answers every requested id its Get answers and leaves the rest unresolved, as
// the native store leaves a row it cannot project; batchErr fails every batch
// read instead. A non-empty prefix is the id prefix the store mints.
type batchingStore struct {
	beads.Store
	prefix   string
	batchErr error
	batches  [][]string
	gets     []string
}

func (s *batchingStore) IDPrefix() string { return s.prefix }

func (s *batchingStore) Get(id string) (beads.Bead, error) {
	s.gets = append(s.gets, id)
	return s.Store.Get(id)
}

func (s *batchingStore) GetExactBatch(ids []string) (map[string]beads.Bead, []string, error) {
	s.batches = append(s.batches, append([]string(nil), ids...))
	if s.batchErr != nil {
		return nil, nil, s.batchErr
	}
	found := make(map[string]beads.Bead, len(ids))
	var unresolved []string
	for _, id := range ids {
		b, err := s.Store.Get(id)
		if err != nil {
			unresolved = append(unresolved, id)
			continue
		}
		found[id] = b
	}
	return found, unresolved, nil
}

var _ beads.ExactBatchGetter = (*batchingStore)(nil)

// prefixStore reports an id prefix, standing in for a class store that mints
// its own id namespace (the recorded-ownership fast path).
type prefixStore struct {
	beads.Store
	prefix string
}

func (p *prefixStore) IDPrefix() string { return p.prefix }

// seedCrossClassConvoy builds the mixed-class shape this whole slice is about:
// a convoy owned by one class store that tracks a member bead physically owned
// by another. The tracks edge lives with the convoy; the member does not.
func seedCrossClassConvoy(t *testing.T) (convoyStore *beads.MemStore, memberStore *beads.MemStore, convoyID, memberID string) {
	t.Helper()
	// Ids are prefix-disjoint across class stores in the real topology, so the
	// member is seeded under a rig work prefix rather than the MemStore default.
	convoyStore = beads.NewMemStore()
	memberStore = beads.NewMemStoreFrom(1, []beads.Bead{{ID: "fe-1", Title: "work item", Type: "task", Status: "open"}}, nil)

	convoy, err := convoyStore.Create(beads.Bead{Title: "drain unit", Type: "convoy"})
	if err != nil {
		t.Fatalf("seed convoy: %v", err)
	}
	if err := convoyStore.DepAdd(convoy.ID, "fe-1", TrackingDepType); err != nil {
		t.Fatalf("seed tracks edge: %v", err)
	}
	return convoyStore, memberStore, convoy.ID, "fe-1"
}

// TestMembersInUnnamedClassContributesEmpty pins direction one of the
// partial-result rule: a class the caller did not name is never read, so the
// member it owns comes back as an unresolved placeholder and the lookup
// SUCCEEDS. The observable effect is read back through the placeholder
// predicate, not through the fixture that seeded the member.
func TestMembersInUnnamedClassContributesEmpty(t *testing.T) {
	convoyStore, memberStore, convoyID, memberID := seedCrossClassConvoy(t)

	members, err := MembersIn(MemberClasses{Convoy: convoyStore}, convoyID, true)
	if err != nil {
		t.Fatalf("MembersIn over the convoy class alone: %v", err)
	}
	if len(members) != 1 {
		t.Fatalf("members = %d (%+v), want the single tracked edge", len(members), members)
	}
	if members[0].ID != memberID {
		t.Fatalf("member id = %q, want %q", members[0].ID, memberID)
	}
	if !IsUnresolvedTrackedItem(members[0]) {
		t.Fatalf("member = %+v, want an unresolved placeholder: the class owning it was never named", members[0])
	}

	// The member really is materializable — it is only invisible because this
	// lookup did not span its class. Read it back through the owning store,
	// which MembersIn never touched.
	owned, err := memberStore.Get(memberID)
	if err != nil {
		t.Fatalf("member store Get: %v", err)
	}
	if owned.Title != "work item" {
		t.Fatalf("seeded member title = %q, want %q", owned.Title, "work item")
	}
}

// TestMembersInNamedClassMaterializesMember is the control for the test above:
// name the member's class and the same convoy yields the real bead. Without
// this case the placeholder assertion could not tell "not named" from "not
// resolvable at all".
func TestMembersInNamedClassMaterializesMember(t *testing.T) {
	convoyStore, memberStore, convoyID, memberID := seedCrossClassConvoy(t)

	members, err := MembersIn(MemberClasses{
		Convoy: convoyStore,
		Work:   []beads.Store{memberStore},
	}, convoyID, true)
	if err != nil {
		t.Fatalf("MembersIn spanning the work class: %v", err)
	}
	if len(members) != 1 {
		t.Fatalf("members = %d (%+v), want 1", len(members), members)
	}
	if IsUnresolvedTrackedItem(members[0]) {
		t.Fatalf("member %+v is a placeholder, want the materialized bead", members[0])
	}
	if members[0].ID != memberID || members[0].Title != "work item" {
		t.Fatalf("member = %+v, want the seeded work item %q", members[0], memberID)
	}
}

// TestMembersInNamedClassFailureStaysAnError pins direction two: a class the
// caller DID name is a participant, so its read failure is returned with the
// class as provenance instead of being flattened into a placeholder. Compare
// with TestMembersInUnnamedClassContributesEmpty: identical fixture, identical
// convoy, opposite outcome — which is the whole point of naming.
func TestMembersInNamedClassFailureStaysAnError(t *testing.T) {
	convoyStore, _, convoyID, memberID := seedCrossClassConvoy(t)
	broken := &unreadableStore{err: fmt.Errorf("dial work store: connection refused")}

	members, err := MembersIn(MemberClasses{
		Convoy: convoyStore,
		Work:   []beads.Store{broken},
	}, convoyID, true)
	if err == nil {
		t.Fatalf("MembersIn = %+v, nil; want the participating class's read failure", members)
	}
	if errors.Is(err, beads.ErrNotFound) {
		t.Fatalf("MembersIn err = %v, want a hard failure, not a not-found", err)
	}
	if broken.reads == 0 {
		t.Fatal("the named work class was never read; naming a class must make it a participant")
	}
	for _, want := range []string{memberID, "work", "connection refused"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("MembersIn err = %q, want it to name %q", err, want)
		}
	}
}

// TestTrackItemInRefusesCrossClassEdgeBeforeWriting pins the write side: a
// membership edge cannot span stores, so naming a member class that really owns
// the item fails with ErrMemberNotCoResident and writes NOTHING. The absence of
// the write is read back through the convoy store's dependency list, which
// TrackItemIn never writes through on this path.
func TestTrackItemInRefusesCrossClassEdgeBeforeWriting(t *testing.T) {
	convoyStore := beads.NewMemStore()
	memberStore := beads.NewMemStoreFrom(1, []beads.Bead{{ID: "fe-1", Title: "work item", Type: "task", Status: "open"}}, nil)
	convoy, err := convoyStore.Create(beads.Bead{Title: "graph convoy", Type: "convoy"})
	if err != nil {
		t.Fatalf("seed convoy: %v", err)
	}

	err = TrackItemIn(MemberClasses{
		Convoy: convoyStore,
		Work:   []beads.Store{memberStore},
	}, convoy.ID, "fe-1")
	if !errors.Is(err, ErrMemberNotCoResident) {
		t.Fatalf("TrackItemIn err = %v, want ErrMemberNotCoResident", err)
	}

	deps, err := convoyStore.DepList(convoy.ID, "down")
	if err != nil {
		t.Fatalf("DepList: %v", err)
	}
	if len(deps) != 0 {
		t.Fatalf("convoy deps = %+v, want none: a refused cross-class track must write nothing", deps)
	}
}

// TestTrackItemInWritesSameClassEdge is the control: when the named classes
// agree that the convoy's own store owns the member, the edge is written.
func TestTrackItemInWritesSameClassEdge(t *testing.T) {
	store := beads.NewMemStore()
	convoy, _ := store.Create(beads.Bead{Title: "convoy", Type: "convoy"})
	member, _ := store.Create(beads.Bead{Title: "item"})

	if err := TrackItemIn(MemberClasses{Convoy: store, Work: []beads.Store{store}}, convoy.ID, member.ID); err != nil {
		t.Fatalf("TrackItemIn: %v", err)
	}
	requireTracksDep(t, store, convoy.ID, member.ID)
}

// TestResolveMemberDuplicateResidenceIsTypedError pins that two residences are
// a typed error rather than a first-match winner. Naming a class is a claim
// about ownership; two claims mean the caller cannot know the owner, and no
// membership decision may be made on a coin flip.
func TestResolveMemberDuplicateResidenceIsTypedError(t *testing.T) {
	first := beads.NewMemStore()
	bead, err := first.Create(beads.Bead{Title: "item"})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	// The same id resident in a second named class — a migration copy left
	// behind, which is exactly the condition a first-match probe would hide.
	second := beads.NewMemStoreFrom(1, []beads.Bead{{ID: bead.ID, Title: "stale copy", Type: "task", Status: "open"}}, nil)
	if _, err := second.Get(bead.ID); err != nil {
		t.Fatalf("seed duplicate: %v", err)
	}

	classes := MemberClasses{Convoy: first, Graph: second}
	_, _, err = classes.resolveMember(bead.ID)
	if !errors.Is(err, ErrDuplicateResidence) {
		t.Fatalf("resolveMember err = %v, want ErrDuplicateResidence", err)
	}
	var dup *DuplicateResidenceError
	if !errors.As(err, &dup) {
		t.Fatalf("resolveMember err = %v, want a *DuplicateResidenceError", err)
	}
	if dup.ID != bead.ID {
		t.Fatalf("duplicate id = %q, want %q", dup.ID, bead.ID)
	}
	if len(dup.Classes) != 2 {
		t.Fatalf("duplicate classes = %v, want both claimants", dup.Classes)
	}

	// And the same duplicate refuses the mutation, before it is attempted.
	convoy, _ := first.Create(beads.Bead{Title: "convoy", Type: "convoy"})
	if err := TrackItemIn(classes, convoy.ID, bead.ID); !errors.Is(err, ErrDuplicateResidence) {
		t.Fatalf("TrackItemIn err = %v, want ErrDuplicateResidence", err)
	}
	deps, _ := first.DepList(convoy.ID, "down")
	if len(deps) != 0 {
		t.Fatalf("convoy deps = %+v, want none", deps)
	}
}

// TestCandidatesNamesEachHandleOnce pins that naming the same physical handle
// for two classes is one candidate, not a duplicate residence. Today every
// class binds to the same store, so this is the case that must stay boring.
func TestCandidatesNamesEachHandleOnce(t *testing.T) {
	store := beads.NewMemStore()
	bead, _ := store.Create(beads.Bead{Title: "item"})

	classes := MemberClasses{Convoy: store, Work: []beads.Store{store}, Graph: store}
	if got := len(classes.candidates()); got != 1 {
		t.Fatalf("candidates = %d, want 1: the same handle named three times is one store", got)
	}
	if _, _, err := classes.resolveMember(bead.ID); err != nil {
		t.Fatalf("resolveMember over one shared handle: %v", err)
	}
}

// TestCandidatesSkipsUnnamedAndTypedNilClasses pins that an unnamed class is
// dropped before it can be probed — including a typed nil handle, which is a
// NON-nil interface value and would panic on first use.
func TestCandidatesSkipsUnnamedAndTypedNilClasses(t *testing.T) {
	store := beads.NewMemStore()
	var typedNil *beads.MemStore

	classes := MemberClasses{Convoy: store, Work: []beads.Store{typedNil, nil}, Graph: typedNil}
	cands := classes.candidates()
	if len(cands) != 1 {
		t.Fatalf("candidates = %d (%+v), want only the named convoy handle", len(cands), cands)
	}
	if _, _, err := classes.resolveMember("gc-absent"); !errors.Is(err, beads.ErrNotFound) {
		t.Fatalf("resolveMember err = %v, want ErrNotFound", err)
	}
}

// TestResolveMemberPrefixOwnerIsRecordedOwnership pins the recorded-ownership
// fast path: a class store that mints the id's prefix owns it outright, so the
// other named classes are not probed at all.
func TestResolveMemberPrefixOwnerIsRecordedOwnership(t *testing.T) {
	const graphID = "gcg-1"
	graphBacking := beads.NewMemStoreFrom(1, []beads.Bead{{ID: graphID, Title: "graph node", Type: "task", Status: "open"}}, nil)
	graph := &prefixStore{Store: graphBacking, prefix: "gcg"}
	work := &unreadableStore{err: fmt.Errorf("dial work store: connection refused")}

	classes := MemberClasses{Convoy: graph, Work: []beads.Store{work}}
	got, owner, err := classes.resolveMember(graphID)
	if err != nil {
		t.Fatalf("resolveMember: %v", err)
	}
	if got.ID != graphID {
		t.Fatalf("resolved %q, want %q", got.ID, graphID)
	}
	if owner.class != "convoy" {
		t.Fatalf("owner class = %q, want the prefix owner", owner.class)
	}
	if work.reads != 0 {
		t.Fatalf("work class read %d times, want 0: recorded ownership needs no probe", work.reads)
	}
}

// TestConvoyClassHandleIsRequired pins that an operation with no handle for the
// convoy's own class fails with a diagnosis instead of dereferencing a nil (or
// worse, a typed nil, which is a non-nil interface value).
func TestConvoyClassHandleIsRequired(t *testing.T) {
	var typedNil *beads.MemStore
	for name, classes := range map[string]MemberClasses{
		"unnamed":   {Work: []beads.Store{beads.NewMemStore()}},
		"typed nil": {Convoy: typedNil},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := MembersIn(classes, "gc-1", true); !errors.Is(err, ErrNoConvoyClass) {
				t.Fatalf("MembersIn err = %v, want ErrNoConvoyClass", err)
			}
			if err := TrackItemIn(classes, "gc-1", "gc-2"); !errors.Is(err, ErrNoConvoyClass) {
				t.Fatalf("TrackItemIn err = %v, want ErrNoConvoyClass", err)
			}
		})
	}
}

// TestMembersInParticipantFailureOutranksAFoundOwnerInEitherOrder pins the
// rule that makes the partial-result semantics safe under multi-scope Work: a
// named class that cannot be read is an error EVEN WHEN another named class
// already produced the bead, and the answer does not depend on probe order.
//
// The reason is uniqueness. Two residences are a typed error precisely because
// nobody may pick a winner between them, and an unreadable participant means
// the candidate set cannot be proven to hold exactly one owner. Returning the
// bead the readable class happened to hold would be first-match ownership with
// extra steps — and it would flip behavior depending on which rig scope
// happened to be listed first, which is the least debuggable failure mode this
// design has.
func TestMembersInParticipantFailureOutranksAFoundOwnerInEitherOrder(t *testing.T) {
	orders := []struct {
		name  string
		build func(owner, broken beads.Store) []beads.Store
	}{
		{
			name:  "broken scope probed first",
			build: func(owner, broken beads.Store) []beads.Store { return []beads.Store{broken, owner} },
		},
		{
			name:  "owning scope probed first",
			build: func(owner, broken beads.Store) []beads.Store { return []beads.Store{owner, broken} },
		},
	}
	for _, order := range orders {
		t.Run(order.name, func(t *testing.T) {
			convoyStore, memberStore, convoyID, memberID := seedCrossClassConvoy(t)
			broken := &unreadableStore{err: fmt.Errorf("dial rig work store: connection refused")}

			members, err := MembersIn(MemberClasses{
				Convoy: convoyStore,
				Work:   order.build(memberStore, broken),
			}, convoyID, true)
			if err == nil {
				t.Fatalf("MembersIn = %+v, nil; a readable class answered while a named class was unreadable", members)
			}
			if errors.Is(err, beads.ErrNotFound) {
				t.Fatalf("MembersIn err = %v, want a hard failure, not a not-found", err)
			}
			if broken.reads == 0 {
				t.Fatal("the broken work scope was never read; naming a class must make it a participant")
			}
			if !strings.Contains(err.Error(), "connection refused") {
				t.Fatalf("MembersIn err = %q, want it to carry the unreadable class's own failure", err)
			}
			// The member really is resolvable through the readable scope, so
			// the failure above is the rule and not a broken fixture.
			resolvable, getErr := memberStore.Get(memberID)
			if getErr != nil || resolvable.ID != memberID {
				t.Fatalf("the fixture member is not readable through the owning scope (%v); this test would prove nothing", getErr)
			}
		})
	}
}

// residenceTopology seeds every residence shape the by-id rule distinguishes,
// spread over named class stores. The convoy and graph stores mint their own
// prefixes; the Work scope mints none, as a policy-wrapped store does not. It
// returns the classes twice over the same stores, once offering the exact batch
// read and once answering Get alone, and the ids to resolve.
func residenceTopology(t *testing.T, broken *unreadableStore, brokenFirst bool) (batched, getOnly MemberClasses, ids []string) {
	t.Helper()
	convoy := &corruptRowStore{Store: beads.NewMemStoreFrom(1, []beads.Bead{
		{ID: "co-1", Title: "convoy-class item", Type: "task", Status: "open"},
		{ID: "co-2", Title: "closed convoy-class item", Type: "task", Status: "closed"},
		{ID: "co-bad", Title: "unprojectable item", Type: "task", Status: "open"},
	}, nil), corrupt: map[string]bool{"co-bad": true}}
	work := &corruptRowStore{Store: beads.NewMemStoreFrom(1, []beads.Bead{
		// A copy its prefix owner no longer holds, left by a migration.
		{ID: "co-9", Title: "migrated item", Type: "task", Status: "open"},
		// A stale copy of a bead its prefix owner holds but cannot project.
		{ID: "co-bad", Title: "stale copy", Type: "task", Status: "closed"},
		{ID: "wk-1", Title: "work item", Type: "task", Status: "in_progress"},
		{ID: "wk-dup", Title: "work copy", Type: "task", Status: "open"},
	}, nil)}
	graph := &corruptRowStore{Store: beads.NewMemStoreFrom(1, []beads.Bead{
		{ID: "gr-1", Title: "wisp step", Type: "task", Status: "open", Ephemeral: true},
		{ID: "wk-dup", Title: "graph copy", Type: "task", Status: "open"},
	}, nil)}
	build := func(wrap func(store beads.Store, prefix string) beads.Store) MemberClasses {
		scopes := []beads.Store{wrap(work, "")}
		if broken != nil {
			if brokenFirst {
				scopes = []beads.Store{broken, scopes[0]}
			} else {
				scopes = append(scopes, broken)
			}
		}
		return MemberClasses{Convoy: wrap(convoy, "co"), Work: scopes, Graph: wrap(graph, "gr")}
	}
	batched = build(func(store beads.Store, prefix string) beads.Store {
		return &batchingStore{Store: store, prefix: prefix}
	})
	getOnly = build(func(store beads.Store, prefix string) beads.Store {
		return &prefixStore{Store: store, prefix: prefix}
	})
	ids = []string{
		"co-1",   // prefix owner holds it
		"co-2",   // prefix owner holds it, closed
		"co-9",   // prefix owner reports it absent; one other class holds it
		"co-404", // prefix owner reports it absent; no class holds it
		"co-bad", // prefix owner cannot project it; another class holds a stale copy
		"gr-1",   // prefix owner holds it, ephemeral
		"wk-1",   // no prefix owner; one class holds it
		"wk-dup", // no prefix owner; two classes hold it
		"wk-404", // no prefix owner; no class holds it
		"wk-1",   // repeated
	}
	return batched, getOnly, ids
}

// TestResolveMembersAnswersEachIDAsGetAloneDoes is the contract for the shared
// member read: whatever the residence shape, and whether or not a named class
// is unreadable, resolving ids together through the stores' batch read gives
// every id exactly the bead, owner and error the rule gives it from Get alone.
func TestResolveMembersAnswersEachIDAsGetAloneDoes(t *testing.T) {
	cases := []struct {
		name        string
		broken      bool
		brokenFirst bool
	}{
		{name: "every class readable"},
		{name: "unreadable scope probed first", broken: true, brokenFirst: true},
		{name: "unreadable scope probed last", broken: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var broken *unreadableStore
			if tc.broken {
				broken = &unreadableStore{err: fmt.Errorf("dial rig work store: connection refused")}
			}
			batched, getOnly, ids := residenceTopology(t, broken, tc.brokenFirst)
			resolve := batched.resolveMembers(ids)
			alone := getOnly.resolveMembers(ids)
			var outcomes []string
			for _, id := range ids {
				got, gotOwner, gotErr := resolve(id)
				want, wantOwner, wantErr := alone(id)
				if fmt.Sprint(gotErr) != fmt.Sprint(wantErr) {
					t.Fatalf("%s: err = %v, want %v", id, gotErr, wantErr)
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("%s: bead = %+v, want %+v", id, got, want)
				}
				if gotOwner.class != wantOwner.class {
					t.Fatalf("%s: owner = %q, want %q", id, gotOwner.class, wantOwner.class)
				}
				var dup *DuplicateResidenceError
				switch {
				case gotErr == nil:
					outcomes = append(outcomes, id+"=bead")
				case errors.As(gotErr, &dup):
					outcomes = append(outcomes, id+"=duplicate")
				case errors.Is(gotErr, beads.ErrNotFound):
					outcomes = append(outcomes, id+"=absent")
				case errors.Is(gotErr, beads.ErrMetadataParse):
					outcomes = append(outcomes, id+"=unprojectable")
				default:
					outcomes = append(outcomes, id+"=read-failure")
				}
			}
			// The fixture must reach every outcome the rule can give, or the
			// comparison above proves less than it claims. An unprojectable
			// row in its prefix owner settles its id before any other class is
			// read, so neither the stale copy nor the unreadable scope reaches
			// it.
			wantOutcomes := "co-1=bead co-2=bead co-9=bead co-404=absent co-bad=unprojectable gr-1=bead wk-1=bead wk-dup=duplicate wk-404=absent wk-1=bead"
			if tc.broken {
				wantOutcomes = "co-1=bead co-2=bead co-9=read-failure co-404=read-failure co-bad=unprojectable gr-1=bead wk-1=read-failure wk-dup=read-failure wk-404=read-failure wk-1=read-failure"
			}
			if got := strings.Join(outcomes, " "); got != wantOutcomes {
				t.Fatalf("outcomes = %s\nwant       %s", got, wantOutcomes)
			}
		})
	}
}

// TestResolveMembersBatchesEachClassOnce pins the read shape of the shared
// member read: each class store answers one batch read, holding the ids it
// mints and the ids no store mints, and is asked with Get only for an id its
// batch did not return.
func TestResolveMembersBatchesEachClassOnce(t *testing.T) {
	batched, _, ids := residenceTopology(t, nil, false)
	resolve := batched.resolveMembers(ids)
	for _, id := range ids {
		_, _, _ = resolve(id)
	}
	stores := map[string]*batchingStore{
		"convoy": batched.Convoy.(*batchingStore),
		"work":   batched.Work[0].(*batchingStore),
		"graph":  batched.Graph.(*batchingStore),
	}
	wantBatches := map[string]string{
		"convoy": "co-1 co-2 co-9 co-404 co-bad wk-1 wk-dup wk-404",
		"work":   "wk-1 wk-dup wk-404",
		"graph":  "gr-1 wk-1 wk-dup wk-404",
	}
	for name, store := range stores {
		if len(store.batches) != 1 {
			t.Fatalf("%s class answered %d batch reads (%v), want one", name, len(store.batches), store.batches)
		}
		if got := strings.Join(store.batches[0], " "); got != wantBatches[name] {
			t.Errorf("%s class batch = %q, want %q", name, got, wantBatches[name])
		}
		answered := map[string]bool{}
		for _, id := range store.batches[0] {
			if _, err := store.Store.Get(id); err == nil {
				answered[id] = true
			}
		}
		seen := map[string]bool{}
		for _, id := range store.gets {
			if answered[id] {
				t.Errorf("%s class answered Get(%s), which its batch read already returned", name, id)
			}
			if seen[id] {
				t.Errorf("%s class answered Get(%s) twice; each id resolves once", name, id)
			}
			seen[id] = true
		}
	}
}

// TestResolveMembersPrefixOwnerNeedsNoProbe is the batched form of the
// recorded-ownership fast path: ids their prefix owner holds are never looked
// up in another class, so an unreadable class that no id needs is never read.
func TestResolveMembersPrefixOwnerNeedsNoProbe(t *testing.T) {
	graphBacking := beads.NewMemStoreFrom(1, []beads.Bead{
		{ID: "gcg-1", Title: "graph node", Type: "task", Status: "open"},
		{ID: "gcg-2", Title: "graph node", Type: "task", Status: "closed"},
	}, nil)
	graph := &batchingStore{Store: graphBacking, prefix: "gcg"}
	work := &unreadableStore{err: fmt.Errorf("dial work store: connection refused")}

	resolve := MemberClasses{Convoy: graph, Work: []beads.Store{work}}.resolveMembers([]string{"gcg-1", "gcg-2"})
	for _, id := range []string{"gcg-1", "gcg-2"} {
		got, _, err := resolve(id)
		if err != nil {
			t.Fatalf("resolve(%s): %v", id, err)
		}
		if got.ID != id {
			t.Fatalf("resolved %q, want %q", got.ID, id)
		}
	}
	if work.reads != 0 {
		t.Fatalf("work class read %d times, want 0: recorded ownership needs no probe", work.reads)
	}
	if len(graph.gets) != 0 {
		t.Fatalf("graph class answered Get for %v; its batch read returned both", graph.gets)
	}
}

// TestMembersInUnprojectableMemberStaysAPlaceholder pins what an unprojectable
// member resolves to when its prefix owner cannot project it: the unresolved
// placeholder, as a Get of it alone gives, whatever another named class holds.
// The batch read leaves such a row unresolved rather than reporting it absent,
// so neither an unreadable class nor a stale copy elsewhere is consulted.
func TestMembersInUnprojectableMemberStaysAPlaceholder(t *testing.T) {
	seed := func(t *testing.T) (*batchingStore, string) {
		t.Helper()
		backing := beads.NewMemStoreFrom(1, []beads.Bead{
			{ID: "co-1", Title: "readable item", Type: "task", Status: "open"},
			{ID: "co-bad", Title: "unprojectable item", Type: "task", Status: "open"},
		}, nil)
		convoy, err := backing.Create(beads.Bead{Title: "convoy", Type: "convoy"})
		if err != nil {
			t.Fatalf("seed convoy: %v", err)
		}
		for _, id := range []string{"co-1", "co-bad"} {
			if err := backing.DepAdd(convoy.ID, id, TrackingDepType); err != nil {
				t.Fatalf("seed tracks edge to %s: %v", id, err)
			}
		}
		return &batchingStore{Store: &corruptRowStore{Store: backing, corrupt: map[string]bool{"co-bad": true}}, prefix: "co"}, convoy.ID
	}
	check := func(t *testing.T, members []beads.Bead) {
		t.Helper()
		got := map[string]beads.Bead{}
		for _, m := range members {
			got[m.ID] = m
		}
		if b := got["co-1"]; b.Title != "readable item" {
			t.Fatalf("readable member = %+v, want the bead its owner holds", b)
		}
		if b := got["co-bad"]; !IsUnresolvedTrackedItem(b) {
			t.Fatalf("unprojectable member = %+v, want the unresolved placeholder", b)
		}
	}

	t.Run("another named class is unreadable", func(t *testing.T) {
		convoyStore, convoyID := seed(t)
		broken := &unreadableStore{err: fmt.Errorf("dial work store: connection refused")}
		members, err := MembersIn(MemberClasses{Convoy: convoyStore, Work: []beads.Store{broken}}, convoyID, true)
		if err != nil {
			t.Fatalf("MembersIn: %v; the unprojectable member's owner answers it before the unreadable class is read", err)
		}
		check(t, members)
		if broken.reads != 0 {
			t.Fatalf("unreadable work class read %d times, want 0", broken.reads)
		}
	})

	t.Run("another named class holds a stale copy", func(t *testing.T) {
		convoyStore, convoyID := seed(t)
		stale := &batchingStore{Store: beads.NewMemStoreFrom(1, []beads.Bead{
			{ID: "co-bad", Title: "stale copy", Type: "task", Status: "closed"},
		}, nil)}
		members, err := MembersIn(MemberClasses{Convoy: convoyStore, Work: []beads.Store{stale}}, convoyID, true)
		if err != nil {
			t.Fatalf("MembersIn: %v", err)
		}
		check(t, members)
		if len(stale.gets) != 0 || len(stale.batches) != 0 {
			t.Fatalf("the work class was read (gets %v, batches %v); the prefix owner answers its own ids", stale.gets, stale.batches)
		}
	})
}

// TestMembersInFailedBatchReadFallsBackToGet pins that a batch read that fails
// costs no member its answer: each id it covered is read with Get, so a present
// member resolves and a deleted one stays the unresolved placeholder.
func TestMembersInFailedBatchReadFallsBackToGet(t *testing.T) {
	backing := beads.NewMemStore()
	convoy, _ := backing.Create(beads.Bead{Title: "convoy", Type: "convoy"})
	kept, _ := backing.Create(beads.Bead{Title: "kept"})
	trackOrFatal(t, backing, convoy.ID, kept.ID)
	if err := backing.DepAdd(convoy.ID, "gc-deleted", TrackingDepType); err != nil {
		t.Fatalf("seed tracks edge to a deleted member: %v", err)
	}
	store := &batchingStore{Store: backing, batchErr: errors.New("bd list: skipped 1 corrupt bead")}

	members, err := MembersIn(MemberClasses{Convoy: store}, convoy.ID, true)
	if err != nil {
		t.Fatalf("MembersIn: %v", err)
	}
	got := map[string]beads.Bead{}
	for _, m := range members {
		got[m.ID] = m
	}
	if b := got[kept.ID]; b.Title != "kept" {
		t.Errorf("present member = %+v, want the bead Get returns", b)
	}
	if b := got["gc-deleted"]; !IsUnresolvedTrackedItem(b) {
		t.Errorf("deleted member = %+v, want the unresolved placeholder", b)
	}
	sort.Strings(store.gets)
	want := []string{"gc-deleted", kept.ID}
	sort.Strings(want)
	if !reflect.DeepEqual(store.gets, want) {
		t.Errorf("Get calls = %v, want one per member the failed batch covered (%v)", store.gets, want)
	}
}

// TestResolveMembersResolvesEachIDOnce pins that an id asked about again, as a
// member several convoys track is, is answered without reading it again.
func TestResolveMembersResolvesEachIDOnce(t *testing.T) {
	store := &batchingStore{Store: beads.NewMemStore()}
	resolve := MemberClasses{Convoy: store}.resolveMembers([]string{"gc-gone", "gc-gone"})
	for range 3 {
		if _, _, err := resolve("gc-gone"); !errors.Is(err, beads.ErrNotFound) {
			t.Fatalf("resolve(gc-gone) err = %v, want ErrNotFound", err)
		}
	}
	if len(store.gets) != 1 || len(store.batches) != 0 {
		t.Fatalf("reads = gets %v, batches %v; want one Get, and no batch for one id", store.gets, store.batches)
	}
}
