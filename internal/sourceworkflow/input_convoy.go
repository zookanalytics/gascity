package sourceworkflow

import (
	"fmt"
	"slices"
	"strings"

	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
)

// ListLiveInputConvoyRoots returns the live graph.v2 workflow roots already
// poured over inputConvoyID, sorted by ID. It is the convoy-first half of the
// "one live graph workflow per unit of work" invariant: a convoy-first pour
// clears gc.source_bead_id, so ListLiveRoots (which indexes on that key) cannot
// see it and the singleton check has to key on gc.input_convoy_id instead.
//
// allowedRootKey, when set, excludes the root carrying that gc.graphv2_root_key
// — the caller's OWN root, either an idempotent re-pour of the same
// (convoy, formula, vars) or the root a --force replacement is about to reuse.
// Without that exclusion a caller would conflict with itself.
//
// Both storage tiers are queried: a graph.v2 root materialized as a wisp lives
// in the ephemeral tier, and a single-tier read would miss it and report "no
// live workflow" for a workflow that is very much live.
func ListLiveInputConvoyRoots(store beads.Store, inputConvoyID, allowedRootKey string) ([]beads.Bead, error) {
	inputConvoyID = strings.TrimSpace(inputConvoyID)
	if store == nil || inputConvoyID == "" {
		return nil, nil
	}
	matches, err := store.ListByMetadata(
		map[string]string{beadmeta.InputConvoyIDMetadataKey: inputConvoyID},
		0, beads.WithBothTiers,
	)
	if err != nil {
		return nil, fmt.Errorf("checking live graph roots for input convoy %s: %w", inputConvoyID, err)
	}
	return filterLiveGraphV2Roots(matches, allowedRootKey), nil
}

// ListLiveInputConvoyRootsForConvoys returns the live graph.v2 workflow roots
// poured over any of convoys, deduplicated and sorted by ID.
//
// It is the root half of the reverse walk (root -> gc.input_convoy_id ->
// tracks -> item), the only durable link from a work bead back to a
// convoy-first workflow driving it: such a root carries no
// gc.source_bead_id, and the bead carries no gc.molecule_id. Keying on one
// convoy ID is not enough, because a pour over a bare bead mints a FRESH
// synthetic input convoy every time (graphv2.CreateSingleItemInputConvoy), so
// the caller passes every convoy that tracks the item
// (convoy.TrackingConvoysForItem, read from the item's store). This package
// takes the convoys rather than walking them itself: internal/convoy reaches
// internal/orders through internal/config, and internal/orders' tests import
// this package, so importing internal/convoy here closes an import cycle.
//
// allowedRootKey behaves as in ListLiveInputConvoyRoots.
func ListLiveInputConvoyRootsForConvoys(rootStore beads.Store, convoys []beads.Bead, allowedRootKey string) ([]beads.Bead, error) {
	var roots []beads.Bead
	seen := make(map[string]bool, len(convoys))
	for _, convoy := range convoys {
		matches, err := ListLiveInputConvoyRoots(rootStore, convoy.ID, allowedRootKey)
		if err != nil {
			return nil, err
		}
		for _, root := range matches {
			if seen[root.ID] {
				continue
			}
			seen[root.ID] = true
			roots = append(roots, root)
		}
	}
	slices.SortFunc(roots, func(a, b beads.Bead) int {
		return strings.Compare(a.ID, b.ID)
	})
	return roots, nil
}

// filterLiveGraphV2Roots keeps the non-terminal graph.v2 workflow roots in
// beadsList, dropping the one carrying allowedRootKey, and sorts by ID.
//
// The graph.v2 restriction is deliberate: only a graph.v2 root links back to
// its work through gc.input_convoy_id alone. A legacy gc.kind=workflow root
// still carries gc.source_bead_id and is found by ListLiveRoots, so matching it
// here would double-report it.
func filterLiveGraphV2Roots(beadsList []beads.Bead, allowedRootKey string) []beads.Bead {
	allowedRootKey = strings.TrimSpace(allowedRootKey)
	roots := make([]beads.Bead, 0, len(beadsList))
	for _, root := range beadsList {
		if root.Status == "closed" || root.Status == "tombstone" || !IsWorkflowRoot(root) {
			continue
		}
		if strings.TrimSpace(root.Metadata[beadmeta.FormulaContractMetadataKey]) != beadmeta.FormulaContractGraphV2 {
			continue
		}
		if allowedRootKey != "" && strings.TrimSpace(root.Metadata[beadmeta.Graphv2RootKeyMetadataKey]) == allowedRootKey {
			continue
		}
		roots = append(roots, root)
	}
	slices.SortFunc(roots, func(a, b beads.Bead) int {
		return strings.Compare(a.ID, b.ID)
	})
	return roots
}
