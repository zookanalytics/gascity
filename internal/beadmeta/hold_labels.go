package beadmeta

import "strings"

// HoldLabelPrefix opens a hold label, hold:<value>: the label bd keeps for a
// bead's "hold" state dimension (`bd set-state <id> hold=<value>` swaps it in
// and files an audit event). The value names the actor or condition the bead
// waits on, and its vocabulary belongs to the city or pack that sets it. Go
// code never reads the value as an agent, a role or a route.
const HoldLabelPrefix = "hold:"

// HoldClearedValue is the value `bd set-state <id> hold=cleared` leaves behind
// when it releases a hold. A hold:cleared label is inactive: it says the bead
// is no longer held, not who acts next. `bd set-state` replaces one hold label,
// the first it finds, and `bd label add` adds one beside any other, so a bead
// can carry hold:cleared beside a hold label that is active.
const HoldClearedValue = "cleared"

// ActiveHoldValue reports the value of label when label is an active hold.
//
// The label is trimmed and must begin with HoldLabelPrefix, compared
// case-insensitively. Its value is the rest of the label, trimmed, and the hold
// is active when that value is nonempty and is not HoldClearedValue, compared
// case-insensitively. Every other label is not a hold: hold:cleared, a hold:
// label whose value is empty or only whitespace, "hold" with no colon, and a
// label that only contains the word, such as mpr-human-hold or on-hold.
func ActiveHoldValue(label string) (string, bool) {
	label = strings.TrimSpace(label)
	if len(label) < len(HoldLabelPrefix) || !strings.EqualFold(label[:len(HoldLabelPrefix)], HoldLabelPrefix) {
		return "", false
	}
	value := strings.TrimSpace(label[len(HoldLabelPrefix):])
	if value == "" || strings.EqualFold(value, HoldClearedValue) {
		return "", false
	}
	return value, true
}

// HasActiveHold reports whether labels carry an active hold (ActiveHoldValue).
// A held bead's required next actor or condition is, by construction, not the
// worker looking at it, whatever the hold's value names. Two different
// questions consume this predicate, and they answer oppositely:
//
//   - "Is this bead WORK for whoever is asking?" Never, while it is held.
//     Route-scoped, unassigned automatic dispatch (the pool-demand tier, its
//     count form and the control dispatcher's routed tiers) excludes held rows,
//     and so does every path that serves a bead to an agent as work: the
//     crash-recovery and assigned-ready tiers of the work query, the
//     `gc hook --claim` result, the continuation-group siblings a claim
//     pre-assigns, and the controller's wake readiness, keep-awake probe and
//     drain-ack claimability gates over assigned OPEN rows. A held bead served
//     as work cannot be advanced, is never released, and comes back on every
//     tick. A bd --exclude-label flag matches one label exactly and cannot
//     name every value, so a generated read drops held rows with jq before it
//     cuts its answer to a window, and a held row never takes an unheld row's
//     place in that window.
//
//   - "Does a session still need to EXIST for this bead?" A hold is irrelevant:
//     the assignment is a real ownership fact either way. Pool accounting counts
//     a held OPEN assigned row toward its template's seats, held in_progress
//     work keeps its owner's session in demand and awake, and orphan release
//     still sees the assignment, so a parked bead's owner stays visible to the
//     pool and to crash recovery.
//
// The short rule: filter on holds when deciding what to DO, never when deciding
// who EXISTS. Restoring a lost route is a "what to DO" decision too: a pack that
// parks work on a hold may clear its gc.routed_to, so the controller's
// route-restore lanes leave a held bead's empty route alone until the hold
// lifts.
func HasActiveHold(labels []string) bool {
	for _, label := range labels {
		if _, ok := ActiveHoldValue(label); ok {
			return true
		}
	}
	return false
}

// HumanLabel is bd's native label for a bead waiting on a person's decision
// (`bd human list` enumerates the beads that carry it). It is not a hold label
// and HasActiveHold does not match it, so beads already assigned to an agent
// are not parked by it. Route-scoped, unassigned dispatch excludes it all the
// same, for the reason it excludes a hold: a worker cannot advance a bead that
// is waiting on a person, and serving it back on every tick burns a turn each
// time (ga-7yfnyv). The assignee-scoped tiers stay transparent to it. The
// dead-assignee sweep (releaseOrphanedPoolAssignments) skips it too: released,
// it would be served to no one, and its holder may be a person, whom no session
// answers to.
const HumanLabel = "human"

// HasHumanLabel reports whether labels carry HumanLabel. It compares the way
// the route-scoped serve filter does (bd's --exclude-label, mirrored by the
// controller's demand predicate): trimmed and exact, so a "Human" bead is
// served as ordinary work and is not a person's bead here either.
func HasHumanLabel(labels []string) bool {
	for _, label := range labels {
		if strings.TrimSpace(label) == HumanLabel {
			return true
		}
	}
	return false
}
