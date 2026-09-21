package convoy

import (
	"errors"
	"fmt"
	"sort"

	"github.com/gastownhall/gascity/internal/beads"
)

// TrackingDepType is the dependency type used for convoy membership edges.
const TrackingDepType = "tracks"

const trackedStatusUnknown = "unknown"

// IsTerminalStatus reports whether a tracked item should count as complete for
// convoy progress and auto-close decisions.
func IsTerminalStatus(status string) bool {
	return status == "closed" || status == "tombstone"
}

// TrackItem records that convoyID tracks itemID, spanning only the one class
// handle store.
//
// This is the residual unnamed call shape kept for the consumer packages that
// have not yet been converted to name their classes (internal/api,
// internal/dispatch, internal/graphv2, internal/sling and the non-convoy
// cmd/gc commands). Its memberStores tail is interpreted as Work-class scopes —
// the meaning its one non-empty caller already documents
// (dispatch.ProcessOptions.MemberStores, "the work-class store tail"). New code
// names its classes and calls TrackItemIn.
func TrackItem(store beads.Store, convoyID, itemID string, memberStores ...beads.Store) error {
	return TrackItemIn(MemberClasses{Convoy: store, Work: memberStores}, convoyID, itemID)
}

// TrackItemIn records that convoyID tracks itemID without changing itemID's
// parent-child relationship.
//
// The convoy bead and the tracks dependency edge live in classes.Convoy; the
// item is resolved across every class the caller named. Ownership is proven
// before anything is written: a member owned by a class other than the convoy's
// own fails with ErrMemberNotCoResident and writes nothing, because a dep row
// cannot reference an id its own store cannot resolve. Two residences are a
// *DuplicateResidenceError, never a first-match winner.
func TrackItemIn(classes MemberClasses, convoyID, itemID string) error {
	if err := classes.requireConvoyHandle(); err != nil {
		return fmt.Errorf("tracking %s in convoy %s: %w", itemID, convoyID, err)
	}
	_, owner, err := classes.resolveMember(itemID)
	if err != nil {
		return fmt.Errorf("getting tracked item %s: %w", itemID, err)
	}
	if !sameHandle(owner.store, classes.Convoy) {
		return fmt.Errorf("tracking %s in convoy %s: member is owned by the %s class store: %w",
			itemID, convoyID, owner.class, ErrMemberNotCoResident)
	}
	if err := classes.Convoy.DepAdd(convoyID, itemID, TrackingDepType); err != nil {
		return fmt.Errorf("adding %s dependency %s -> %s: %w", TrackingDepType, convoyID, itemID, err)
	}
	return nil
}

// UntrackItem removes a convoy membership edge from convoyID to itemID.
func UntrackItem(store beads.Store, convoyID, itemID string) error {
	deps, err := store.DepList(convoyID, "down")
	if err != nil {
		return fmt.Errorf("listing convoy %s dependencies: %w", convoyID, err)
	}
	hasTrack := false
	var mixedTypes []string
	for _, dep := range deps {
		if dep.IssueID != convoyID || dep.DependsOnID != itemID {
			continue
		}
		if dep.Type == TrackingDepType {
			hasTrack = true
			continue
		}
		mixedTypes = append(mixedTypes, dep.Type)
	}
	if !hasTrack {
		return nil
	}
	if len(mixedTypes) > 0 {
		return fmt.Errorf("not removing ambiguous %s dependency %s -> %s with other dependency types: %v", TrackingDepType, convoyID, itemID, mixedTypes)
	}
	if err := store.DepRemove(convoyID, itemID); err != nil {
		return fmt.Errorf("removing %s dependency %s -> %s: %w", TrackingDepType, convoyID, itemID, err)
	}
	return nil
}

// Members returns the beads tracked by a convoy, spanning only the one class
// handle store.
//
// This is the residual unnamed call shape kept for the consumer packages that
// have not yet been converted to name their classes; see TrackItem for the
// inventory and for how the memberStores tail is interpreted. New code names
// its classes and calls MembersIn.
func Members(store beads.Store, convoyID string, includeClosed bool, memberStores ...beads.Store) ([]beads.Bead, error) {
	return MembersIn(MemberClasses{Convoy: store, Work: memberStores}, convoyID, includeClosed)
}

// MembersIn returns beads tracked by a convoy. It supports both the current
// tracks dependency relation and legacy parent-child convoy membership.
// Unresolved tracks dependencies are returned with unknown status so completion
// paths never mistake missing dependency details for completed work.
//
// The convoy's own membership (legacy parent-child List and the tracks DepList)
// is read from classes.Convoy, which owns those edges. Each tracked member bead
// is resolved across the classes the caller named, and the partial-result rule
// decides what an absent member means:
//
//   - A class the caller did not name contributes nothing. A member owned by
//     an unnamed class is reported as an unresolved placeholder — an empty
//     result for a lookup that never spanned it — and the read succeeds.
//   - A class the caller did name contributes its failures. If a named class
//     cannot be read, the error is returned with that class as provenance
//     instead of degrading the member to a placeholder, so an unreachable
//     store never looks like a deleted bead.
func MembersIn(classes MemberClasses, convoyID string, includeClosed bool) ([]beads.Bead, error) {
	if err := classes.requireConvoyHandle(); err != nil {
		return nil, fmt.Errorf("listing members of convoy %s: %w", convoyID, err)
	}
	store := classes.Convoy
	legacyChildren, err := store.List(beads.ListQuery{
		ParentID:      convoyID,
		IncludeClosed: includeClosed,
		Sort:          beads.SortCreatedAsc,
	})
	if err != nil {
		return nil, fmt.Errorf("listing legacy convoy children of %s: %w", convoyID, err)
	}
	deps, err := store.DepList(convoyID, "down")
	if err != nil {
		return nil, fmt.Errorf("listing convoy %s dependencies: %w", convoyID, err)
	}
	return assembleMembers(legacyChildren, deps, includeClosed, func(id string) (beads.Bead, error) {
		item, _, err := classes.resolveMember(id)
		return item, err
	})
}

// assembleMembers folds a convoy's legacy parent-child rows and its down-edges
// into one ordered, de-duplicated member list, resolving each tracks edge
// through resolve. It is the shared core of MembersIn, which resolves each
// member with a per-member Get, and MembersBatch, which resolves from a
// pre-fetched pool; sharing it keeps the two byte-for-byte identical in their
// member lists and confines the difference to the read strategy. A tracks edge
// whose target cannot be resolved to a bead — absent, or present but
// unprojectable — stays visible as an unresolved placeholder rather than
// dropping out, because convoy membership is an edge inventory.
func assembleMembers(legacyChildren []beads.Bead, deps []beads.Dep, includeClosed bool, resolve func(id string) (beads.Bead, error)) ([]beads.Bead, error) {
	seen := make(map[string]bool, len(legacyChildren))
	members := make([]beads.Bead, 0, len(legacyChildren))
	add := func(b beads.Bead) {
		if seen[b.ID] {
			return
		}
		if !includeClosed && IsTerminalStatus(b.Status) {
			return
		}
		seen[b.ID] = true
		members = append(members, b)
	}
	for _, child := range legacyChildren {
		add(child)
	}
	for _, dep := range deps {
		if dep.Type != TrackingDepType {
			continue
		}
		item, err := resolve(dep.DependsOnID)
		if err != nil {
			if errors.Is(err, beads.ErrNotFound) || errors.Is(err, beads.ErrMetadataParse) {
				add(unresolvedTrackedItem(dep.DependsOnID))
				continue
			}
			return nil, fmt.Errorf("getting tracked item %s: %w", dep.DependsOnID, err)
		}
		add(item)
	}
	sortMembers(members)
	return members, nil
}

// MembersBatch returns each listed convoy's members, resolved from the single
// class store that owns them, in a bounded number of store queries rather than
// one Members call per convoy. For every id it returns exactly the slice
// Members(store, id, includeClosed) returns, so it is a drop-in for read paths
// that list many convoys at once (convoy list, convoy progress) and changes
// their throughput, not their output. A convoy id with no members maps to an
// empty (non-nil) slice.
//
// It spans one class, matching the single-store Members shape every convoy-list
// and convoy-check caller already uses; a caller that must resolve members
// across Work or Graph classes keeps calling MembersIn per convoy, because
// batching that cross-class residence probe is a separate concern.
//
// The reads are: every convoy's tracks edges (one DepListBatch, or one DepList
// per convoy when the store cannot batch them), every convoy's legacy
// parent-child rows (one List), and every tracked member bead (one List keyed
// by id). A tracks target absent from that member read resolves to the same
// unresolved placeholder Members produces for a Get that returns ErrNotFound.
func MembersBatch(store beads.Store, convoyIDs []string, includeClosed bool) (map[string][]beads.Bead, error) {
	if isNilStore(store) {
		return nil, fmt.Errorf("listing convoy members: %w", ErrNoConvoyClass)
	}
	out := make(map[string][]beads.Bead, len(convoyIDs))
	if len(convoyIDs) == 0 {
		return out, nil
	}

	// Legacy parent-child children for every convoy in one read, not one read
	// per convoy: a per-convoy ParentID read costs a store round trip each, and
	// on the served-Dolt backend that is ~0.5s apiece — the per-convoy cost this
	// batch exists to remove. ParentIDs is a pushdown-only hint the in-memory
	// query filter does not enforce (unlike ParentID and IDs), so the read is
	// scan-authorized and the parent match is made here against the convoy set.
	convoySet := make(map[string]bool, len(convoyIDs))
	for _, id := range convoyIDs {
		convoySet[id] = true
	}
	legacyRows, err := store.List(beads.ListQuery{
		ParentIDs:     convoyIDs,
		IncludeClosed: includeClosed,
		Sort:          beads.SortCreatedAsc,
		AllowScan:     true,
	})
	if err != nil {
		return nil, fmt.Errorf("listing legacy convoy children: %w", err)
	}
	legacyByParent := make(map[string][]beads.Bead, len(convoyIDs))
	for _, row := range legacyRows {
		if convoySet[row.ParentID] {
			legacyByParent[row.ParentID] = append(legacyByParent[row.ParentID], row)
		}
	}

	depsByConvoy, err := downEdgesBatch(store, convoyIDs)
	if err != nil {
		return nil, err
	}

	// Every distinct tracked member id, resolved in one keyed read.
	var memberIDs []string
	memberSet := make(map[string]bool)
	for _, deps := range depsByConvoy {
		for _, dep := range deps {
			if dep.Type == TrackingDepType && !memberSet[dep.DependsOnID] {
				memberSet[dep.DependsOnID] = true
				memberIDs = append(memberIDs, dep.DependsOnID)
			}
		}
	}
	pool := make(map[string]beads.Bead, len(memberIDs))
	if len(memberIDs) > 0 {
		// TierBoth so the keyed read resolves ephemeral tracked members too:
		// MembersIn resolves each member through a tier-blind Get, and a
		// zero-value (TierIssues) query drops wisp-tier rows, which would leave
		// an ephemeral member as a dangling placeholder here but a real bead
		// there.
		resolved, err := store.List(beads.ListQuery{IDs: memberIDs, IncludeClosed: true, TierMode: beads.TierBoth})
		if err != nil {
			return nil, fmt.Errorf("resolving convoy members: %w", err)
		}
		for _, b := range resolved {
			if memberSet[b.ID] {
				pool[b.ID] = b
			}
		}
	}
	resolve := func(id string) (beads.Bead, error) {
		if b, ok := pool[id]; ok {
			return b, nil
		}
		return beads.Bead{}, beads.ErrNotFound
	}

	for _, id := range convoyIDs {
		members, err := assembleMembers(legacyByParent[id], depsByConvoy[id], includeClosed, resolve)
		if err != nil {
			return nil, err
		}
		out[id] = members
	}
	return out, nil
}

// downEdgesBatch returns the down-edges of every convoy, one DepListBatch when
// the store answers that capability and one DepList per convoy otherwise —
// including when a wrapper advertises the capability but its backing store
// cannot answer it (ErrDepListBatchUnsupported).
func downEdgesBatch(store beads.Store, convoyIDs []string) (map[string][]beads.Dep, error) {
	if batcher, ok := beads.DepListBatchFor(store); ok {
		byConvoy, err := batcher.DepListBatch(convoyIDs)
		if err == nil {
			return byConvoy, nil
		}
		if !errors.Is(err, beads.ErrDepListBatchUnsupported) {
			return nil, fmt.Errorf("batch-listing convoy dependencies: %w", err)
		}
	}
	byConvoy := make(map[string][]beads.Dep, len(convoyIDs))
	for _, id := range convoyIDs {
		deps, err := store.DepList(id, "down")
		if err != nil {
			return nil, fmt.Errorf("listing convoy %s dependencies: %w", id, err)
		}
		byConvoy[id] = deps
	}
	return byConvoy, nil
}

func unresolvedTrackedItem(id string) beads.Bead {
	return beads.Bead{
		ID:     id,
		Title:  id,
		Type:   "task",
		Status: trackedStatusUnknown,
	}
}

// IsUnresolvedTrackedItem reports whether b is a synthetic placeholder for a
// dangling tracks dependency whose target bead is unavailable.
func IsUnresolvedTrackedItem(b beads.Bead) bool {
	return b.Status == trackedStatusUnknown && b.Type == "task" && b.Title == b.ID
}

// HasTrack reports whether convoyID has a tracks dependency to itemID.
//
// This spans exactly one class: membership edges are owned by the convoy's own
// class store, and the edge alone answers the question without materializing
// the member bead. It therefore takes no member classes — a caller cannot name
// a class here and be misled into thinking it was consulted.
func HasTrack(store beads.Store, convoyID, itemID string) (bool, error) {
	deps, err := store.DepList(convoyID, "down")
	if err != nil {
		return false, fmt.Errorf("listing convoy %s dependencies: %w", convoyID, err)
	}
	for _, dep := range deps {
		if dep.Type == TrackingDepType && dep.IssueID == convoyID && dep.DependsOnID == itemID {
			return true, nil
		}
	}
	return false, nil
}

// TrackingConvoysForItem returns convoy beads that track itemID via a tracks
// dependency. Dangling dependency sources are ignored.
//
// This spans exactly one class: the tracking edges and the convoy beads they
// point at are both owned by the convoy's class store, so that handle answers
// the whole lookup. It takes no member classes for the same reason HasTrack
// does not — the member's own store holds no part of this answer, and a
// parameter that is accepted but never read would let a caller believe a class
// participated when it did not.
func TrackingConvoysForItem(store beads.Store, itemID string) ([]beads.Bead, error) {
	deps, err := store.DepList(itemID, "up")
	if err != nil {
		return nil, fmt.Errorf("listing dependents of item %s: %w", itemID, err)
	}

	seen := make(map[string]bool, len(deps))
	convoys := make([]beads.Bead, 0, len(deps))
	for _, dep := range deps {
		if dep.Type != TrackingDepType || seen[dep.IssueID] {
			continue
		}
		b, err := store.Get(dep.IssueID)
		if err != nil {
			if errors.Is(err, beads.ErrNotFound) {
				continue
			}
			return nil, fmt.Errorf("getting tracking convoy %s: %w", dep.IssueID, err)
		}
		if b.Type != "convoy" {
			continue
		}
		seen[b.ID] = true
		convoys = append(convoys, b)
	}
	sortMembers(convoys)
	return convoys, nil
}

func sortMembers(items []beads.Bead) {
	sort.SliceStable(items, func(i, j int) bool {
		left := items[i]
		right := items[j]
		if left.CreatedAt.Equal(right.CreatedAt) {
			return left.ID < right.ID
		}
		return left.CreatedAt.Before(right.CreatedAt)
	})
}
