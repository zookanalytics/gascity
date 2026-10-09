package convoy

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/coordclass"
	"github.com/gastownhall/gascity/internal/storeref"
)

// MemberClasses names the class handles a single convoy operation spans.
//
// Convoy membership is the one deliberately mixed-class relation in the bead
// substrate: the convoy bead and its `tracks` edges are owned by Work — every
// convoy is a work bead, the synthetic formula/drain ones included — while the
// members themselves can be owned by another class. Per the class-ownership
// table only Work and Graph ever own convoy members, so those are the only
// classes an operation can name here; Sessions, Messaging, Orders and Nudges are
// non-participating by construction rather than by a runtime skip.
//
// Naming is the whole contract, in both directions:
//
//   - A class the caller does not name does not participate. It is never read,
//     contributes an EMPTY result, and can never turn a lookup into an error.
//   - A class the caller does name participates. Its read failures are returned
//     with the class as provenance and are never flattened into "member not
//     found", because an unreachable owner and a deleted bead must not look
//     alike.
//
// This is explicit composition, not federation: there is no registry, no
// probing for a provider, and no first-match ownership. Candidate handles come
// from the caller, ownership is proven before any mutation, and a bead owned by
// two named handles is a typed error rather than a silent winner.
type MemberClasses struct {
	// Convoy is the handle for the class that owns the convoy bead itself and
	// its membership edges. It is required: every convoy operation reads or
	// writes the convoy's own edges through it, and it is also the first
	// candidate probed for members (a same-class convoy owns its members).
	Convoy beads.Store

	// Work names the Work-class scopes whose beads can be members. Work is a
	// topology, not one store — the HQ workspace plus each rig workspace — so
	// it is a slice; a caller that spans only the HQ scope names one handle.
	// Nil or empty means this operation does not span the Work class.
	Work []beads.Store

	// Graph names the Graph-class handle when graph-class beads (formula steps,
	// control beads, wisp roots) can be members. It is never the class that owns
	// the convoy itself — synthetic convoys are work beads like every other
	// convoy. Nil means this operation does not span the Graph class.
	Graph beads.Store
}

// ErrDuplicateResidence reports that one bead id was found in more than one
// named class handle. A bead has exactly one owner; two residences mean the
// candidate set is wrong or a migration left a copy behind, and neither a read
// nor a mutation may pick a winner. Match it with errors.Is.
var ErrDuplicateResidence = errors.New("duplicate residence")

// DuplicateResidenceError is the typed form of ErrDuplicateResidence, carrying
// the id and the names of every class that claimed it.
type DuplicateResidenceError struct {
	ID      string
	Classes []string
}

// Error implements error.
func (e *DuplicateResidenceError) Error() string {
	return fmt.Sprintf("bead %s is resident in more than one named class store (%s)", e.ID, strings.Join(e.Classes, ", "))
}

// Unwrap makes errors.Is(err, ErrDuplicateResidence) true.
func (e *DuplicateResidenceError) Unwrap() error { return ErrDuplicateResidence }

// ErrMemberNotCoResident reports that a membership edge cannot be written
// because the convoy and the member are owned by different class handles. A
// `tracks` dependency row lives in one store's dep table and can only reference
// an id that store can resolve, so a cross-class edge is refused before any
// write rather than left to fail inside the backend with "no issue found" —
// an error that names neither the convoy nor the class that could not see it,
// and that arrives after the caller has already decided the edge exists.
// Match it with errors.Is.
var ErrMemberNotCoResident = errors.New("convoy member is not co-resident with the convoy")

// ErrNoConvoyClass reports that an operation was given no handle for the class
// that owns the convoy. Every convoy operation reads or writes that class, so
// this is a caller error rather than an empty result. It also catches a typed
// nil handle, which is a NON-nil interface value and would otherwise panic on
// first use rather than fail with a diagnosis.
var ErrNoConvoyClass = errors.New("no handle named for the convoy's own class")

// requireConvoyHandle reports whether the operation can proceed at all.
func (m MemberClasses) requireConvoyHandle() error {
	if isNilStore(m.Convoy) {
		return ErrNoConvoyClass
	}
	return nil
}

// classHandle is one named candidate: a class name for provenance and the
// handle the caller supplied for it.
type classHandle struct {
	class string
	store beads.Store
}

// candidates returns the named class handles in deterministic probe order: the
// convoy's own class first (a same-class member is the common case), then each
// Work scope, then Graph. Unnamed classes and repeats of a handle already in
// the set are omitted, so naming the same physical store twice is one candidate
// and can never look like duplicate residence.
func (m MemberClasses) candidates() []classHandle {
	out := make([]classHandle, 0, 2+len(m.Work))
	add := func(class string, store beads.Store) {
		if isNilStore(store) {
			return
		}
		for _, existing := range out {
			if sameHandle(existing.store, store) {
				return
			}
		}
		out = append(out, classHandle{class: class, store: store})
	}
	add("convoy", m.Convoy)
	for i, w := range m.Work {
		class := coordclass.ClassWork.String()
		if len(m.Work) > 1 {
			class = fmt.Sprintf("%s[%d]", class, i)
		}
		add(class, w)
	}
	add(coordclass.ClassGraph.String(), m.Graph)
	return out
}

// resolveMember returns the bead and the class handle that owns it, applying
// the by-id residence rule: an exact prefix owner is authoritative on its own;
// otherwise every named class is probed and exactly one owner must answer.
// Zero owners return beads.ErrNotFound; more than one returns a
// *DuplicateResidenceError; a read failure from a named (participating) class
// is returned with that class as provenance even when another class holds the
// bead, because uniqueness cannot be proven while a participant is unreadable.
func (m MemberClasses) resolveMember(id string) (beads.Bead, classHandle, error) {
	return m.resolveMembers([]string{id})(id)
}

// resolveMembers returns a function that resolves an id under the by-id
// residence rule resolveMember documents, with reads shared across ids. Every
// lookup the rule makes is the answer Store.Get gives for that id in that
// class, so each id resolves exactly as it resolves alone, errors included.
//
// The reads are shared through beads.ExactBatchGetter. Before any id is
// resolved, each class store that offers that read is asked once for every id
// the rule reads from it first: the ids whose prefix it mints, and the ids no
// named store mints. A bead that read returns is the answer Get gives. Every
// other lookup goes to Get, because a bead the batch did not return may be
// absent, unprojectable, or reachable only through Get's own fallbacks, and
// only Get tells those apart. A class store without the batch read, one asked
// about a single id, and one whose batch read fails leave all their lookups to
// Get, which reports any failure of its own. Each id is resolved once; asking
// again returns the first answer.
func (m MemberClasses) resolveMembers(ids []string) func(id string) (beads.Bead, classHandle, error) {
	cands := m.candidates()
	if len(cands) == 0 {
		return func(id string) (beads.Bead, classHandle, error) {
			return beads.Bead{}, classHandle{}, fmt.Errorf("resolving %s: no class handle named: %w", id, beads.ErrNotFound)
		}
	}
	stores := make([]beads.Store, 0, len(cands))
	for _, c := range cands {
		stores = append(stores, c.store)
	}
	// ownerOf returns the index in cands of the store that mints id's prefix,
	// or -1 when no named store mints it.
	ownerOf := func(id string) int {
		owner := storeref.PrefixOwner(id, stores)
		if owner == nil {
			return -1
		}
		for i, c := range cands {
			if sameHandle(c.store, owner) {
				return i
			}
		}
		return -1
	}

	owners := make(map[string]int, len(ids))
	want := make([][]string, len(cands))
	for _, id := range ids {
		if _, ok := owners[id]; ok {
			continue
		}
		o := ownerOf(id)
		owners[id] = o
		for i := range cands {
			if o < 0 || o == i {
				want[i] = append(want[i], id)
			}
		}
	}
	batched := make([]map[string]beads.Bead, len(cands))
	for i, c := range cands {
		batched[i] = exactBatch(c.store, want[i])
	}
	get := func(i int, id string) (beads.Bead, error) {
		if b, ok := batched[i][id]; ok {
			return b, nil
		}
		return cands[i].store.Get(id)
	}

	type answer struct {
		bead  beads.Bead
		owner classHandle
		err   error
	}
	answers := make(map[string]answer, len(owners))
	return func(id string) (beads.Bead, classHandle, error) {
		a, ok := answers[id]
		if !ok {
			o, known := owners[id]
			if !known {
				o = ownerOf(id)
			}
			a.bead, a.owner, a.err = residence(id, cands, o, get)
			answers[id] = a
		}
		return a.bead, a.owner, a.err
	}
}

// residence applies the by-id residence rule to id over cands. prefixOwner is
// the index of the store that mints id's prefix, or -1, and get(i, id) answers
// cands[i]'s lookup of id the way Store.Get does.
func residence(id string, cands []classHandle, prefixOwner int, get func(int, string) (beads.Bead, error)) (beads.Bead, classHandle, error) {
	// Recorded ownership first: a store that mints id's prefix owns it, so no
	// probe of the other classes is needed and no second residence is possible.
	if prefixOwner >= 0 {
		c := cands[prefixOwner]
		got, err := get(prefixOwner, id)
		if err == nil {
			return got, c, nil
		}
		if !errors.Is(err, beads.ErrNotFound) {
			return beads.Bead{}, classHandle{}, classReadError(c, id, err)
		}
		// A prefix owner that reports the bead absent means a partial
		// migration or a foreign id shape; fall through to the full
		// named-class probe rather than reducing correctness.
	}

	var (
		found      beads.Bead
		owner      classHandle
		haveOwner  bool
		duplicates []string
	)
	for i, c := range cands {
		if i == prefixOwner {
			// The prefix owner already reported the bead absent.
			continue
		}
		got, err := get(i, id)
		if err != nil {
			if errors.Is(err, beads.ErrNotFound) {
				continue
			}
			return beads.Bead{}, classHandle{}, classReadError(c, id, err)
		}
		if haveOwner {
			if len(duplicates) == 0 {
				duplicates = append(duplicates, owner.class)
			}
			duplicates = append(duplicates, c.class)
			continue
		}
		found, owner, haveOwner = got, c, true
	}
	if len(duplicates) > 0 {
		return beads.Bead{}, classHandle{}, &DuplicateResidenceError{ID: id, Classes: duplicates}
	}
	if !haveOwner {
		return beads.Bead{}, classHandle{}, fmt.Errorf("resolving %s across %s: %w", id, strings.Join(classNames(cands), ", "), beads.ErrNotFound)
	}
	return found, owner, nil
}

// exactBatch reads ids from store with its exact batch read and returns the
// beads that read answered. It reads nothing for a single id, whose Get costs
// the same read, or from a store without the read, and answers nothing when the
// read fails: the caller reads each of those ids with Get.
func exactBatch(store beads.Store, ids []string) map[string]beads.Bead {
	if len(ids) < 2 {
		return nil
	}
	getter, ok := store.(beads.ExactBatchGetter)
	if !ok {
		return nil
	}
	found, _, err := getter.GetExactBatch(ids)
	if err != nil {
		return nil
	}
	return found
}

// classReadError wraps a participating class's read failure with the class as
// provenance. beads.ErrMetadataParse keeps its identity so membership readers
// can still keep an unprojectable edge visible.
func classReadError(c classHandle, id string, err error) error {
	return fmt.Errorf("reading %s from the %s class store: %w", id, c.class, err)
}

func classNames(cands []classHandle) []string {
	names := make([]string, 0, len(cands))
	for _, c := range cands {
		names = append(names, c.class)
	}
	return names
}

// isNilStore reports whether a handle is unnamed. A typed nil pointer in a
// beads.Store interface is a NON-nil interface value that panics on first use,
// so the nil check has to look through the interface.
func isNilStore(s beads.Store) bool {
	if s == nil {
		return true
	}
	v := reflect.ValueOf(s)
	switch v.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan, reflect.Interface:
		return v.IsNil()
	default:
		return false
	}
}

// SameStore reports whether two store handles are the same physical store. It
// is the exported form of the handle-identity check convoy membership uses to
// prove a bead's residence, for callers that must group work by its owning store
// before a per-store batch read — the ids MembersBatch takes are unique only
// within a store, so grouping is by store, never by a global id map.
func SameStore(a, b beads.Store) bool {
	return sameHandle(a, b)
}

// sameHandle reports whether two named handles are the same physical store.
// Interface comparison panics when both hold the same non-comparable dynamic
// type, so comparability is checked first; two handles that cannot be compared
// are treated as distinct, which is the conservative answer (they are probed
// separately and a genuine double-residence is reported rather than hidden).
func sameHandle(a, b beads.Store) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	av, bv := reflect.ValueOf(a), reflect.ValueOf(b)
	if av.Type() != bv.Type() || !av.Comparable() || !bv.Comparable() {
		return false
	}
	return a == b
}
