package runproj

import (
	"slices"
	"strings"

	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
)

// CanonicalRunStatus is the closed lifecycle vocabulary shared by the typed
// run API and dashboard-local aggregate projections.
type CanonicalRunStatus string

// Canonical run lifecycle states shared by API and dashboard projections.
const (
	CanonicalRunStatusPending   CanonicalRunStatus = "pending"
	CanonicalRunStatusActive    CanonicalRunStatus = "active"
	CanonicalRunStatusWaiting   CanonicalRunStatus = "waiting"
	CanonicalRunStatusCanceling CanonicalRunStatus = "canceling"
	CanonicalRunStatusCompleted CanonicalRunStatus = "completed"
	CanonicalRunStatusFailed    CanonicalRunStatus = "failed"
	CanonicalRunStatusCanceled  CanonicalRunStatus = "canceled"
	CanonicalRunStatusSkipped   CanonicalRunStatus = "skipped"
)

// CanonicalRunStatusCounts is a complete census of CanonicalRunStatus values.
type CanonicalRunStatusCounts struct {
	Pending   int `json:"pending"`
	Active    int `json:"active"`
	Waiting   int `json:"waiting"`
	Canceling int `json:"canceling"`
	Completed int `json:"completed"`
	Failed    int `json:"failed"`
	Canceled  int `json:"canceled"`
	Skipped   int `json:"skipped"`
}

// CanonicalRunCensus is one immutable snapshot published by an incremental run
// projector. Ready distinguishes a real all-zero census from a cold projection
// that has not completed its first replay.
type CanonicalRunCensus struct {
	Ready          bool
	StatusCounts   CanonicalRunStatusCounts
	Partial        bool
	PartialReasons []string
}

// CanonicalRunStatusForLane derives one run's canonical lifecycle state. A
// terminal root is authoritative even when a lingering member keeps the lane
// in an active phase. Without a root, the lane phase and started-member count
// still provide the defensive best-effort state.
func CanonicalRunStatusForLane(lane RunLane, root *beads.Bead, startedCount int) CanonicalRunStatus {
	if root != nil && strings.TrimSpace(root.Status) == "closed" {
		switch strings.TrimSpace(root.Metadata[beadmeta.OutcomeMetadataKey]) {
		case beadmeta.OutcomeFail:
			return CanonicalRunStatusFailed
		case beadmeta.OutcomeSkipped:
			return CanonicalRunStatusSkipped
		case beadmeta.OutcomeCanceled:
			return CanonicalRunStatusCanceled
		default:
			return CanonicalRunStatusCompleted
		}
	}
	if root != nil && strings.TrimSpace(root.Metadata[beadmeta.CancelRequestedMetadataKey]) != "" {
		return CanonicalRunStatusCanceling
	}
	if lane.Phase == "blocked" {
		return CanonicalRunStatusWaiting
	}
	if startedCount == 0 {
		return CanonicalRunStatusPending
	}
	return CanonicalRunStatusActive
}

// CountCanonicalRunStatuses classifies every supplied run lane against the
// same folded bead snapshot. The caller supplies the uncapped lane census from
// BuildRunSummaryWithAllLanes so row limits never truncate the counts.
func CountCanonicalRunStatuses(beadList []beads.Bead, lanes []RunLane) CanonicalRunStatusCounts {
	byID := make(map[string]beads.Bead, len(beadList))
	for i := range beadList {
		byID[beadList[i].ID] = beadList[i]
	}
	startedByRun := canonicalStartedMembersByRun(beadList, lanes)

	var counts CanonicalRunStatusCounts
	for i := range lanes {
		lane := lanes[i]
		root, found := byID[lane.ID]
		var rootPtr *beads.Bead
		if found {
			rootPtr = &root
		}
		counts.add(CanonicalRunStatusForLane(lane, rootPtr, startedByRun[lane.ID]))
	}
	return counts
}

// add counts one run in status.
func (c *CanonicalRunStatusCounts) add(status CanonicalRunStatus) {
	switch status {
	case CanonicalRunStatusPending:
		c.Pending++
	case CanonicalRunStatusActive:
		c.Active++
	case CanonicalRunStatusWaiting:
		c.Waiting++
	case CanonicalRunStatusCanceling:
		c.Canceling++
	case CanonicalRunStatusCompleted:
		c.Completed++
	case CanonicalRunStatusFailed:
		c.Failed++
	case CanonicalRunStatusCanceled:
		c.Canceled++
	case CanonicalRunStatusSkipped:
		c.Skipped++
	}
}

func canonicalStartedMembersByRun(beadList []beads.Bead, lanes []RunLane) map[string]int {
	roots := make(map[string]struct{}, len(lanes))
	counts := make(map[string]int, len(lanes))
	for i := range lanes {
		roots[lanes[i].ID] = struct{}{}
		counts[lanes[i].ID] = 0
	}
	for i := range beadList {
		for _, rootID := range startedRunRefs(beadList[i]) {
			if _, ok := roots[rootID]; ok {
				counts[rootID]++
			}
		}
	}
	return counts
}

// startedRunRefs returns the distinct run ids a started bead counts toward:
// its parent, its gc.root_bead_id, its molecule id (each possibly empty) and
// every non-empty dotted prefix of its own id, never the bead itself. A bead
// that has not started (neither in_progress nor closed) counts toward none.
func startedRunRefs(bead beads.Bead) []string {
	status := strings.TrimSpace(bead.Status)
	if status != "in_progress" && status != "closed" {
		return nil
	}
	var refs []string
	add := func(rootID string) {
		if rootID == bead.ID || slices.Contains(refs, rootID) {
			return
		}
		refs = append(refs, rootID)
	}
	add(bead.ParentID)
	add(bead.Metadata[beadmeta.RootBeadIDMetadataKey])
	add(strings.TrimSpace(bead.Metadata[beadmeta.MoleculeIDMetadataKey]))
	for offset, char := range bead.ID {
		if char == '.' && offset > 0 {
			add(bead.ID[:offset])
		}
	}
	return refs
}
