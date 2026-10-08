package session

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/gastownhall/gascity/internal/beads"
)

// ErrSessionNameClaimLive reports that a session name cannot be released
// because a live session bead still owns it.
var ErrSessionNameClaimLive = errors.New("session name is held by a live session")

// ReleasedNameClaim records the runtime identifiers cleared from one closed
// session bead, so callers can report exactly what was freed.
type ReleasedNameClaim struct {
	// BeadID is the closed session bead that held the reservation.
	BeadID string
	// SessionName is the runtime session name the bead reserved, if any.
	SessionName string
	// Alias is the human-facing alias the bead reserved, if any.
	Alias string
}

// ReleaseSessionNameClaim frees the runtime identifiers reserved by CLOSED
// session beads matching target, which may be either a runtime session name or
// an alias. It returns one entry per bead released.
//
// A closed session bead retains session_name, alias, and its canonical-identity
// record, and name resolution consults those records. Where the automatic
// releases in ensureSessionNameAvailableForSelfAndOwner do not apply — an
// ad-hoc explicit name, or a holder whose identity signals do not resolve to
// the claimant — the reservation outlives every runtime it described while no
// lifecycle command will touch it: nudge, wake, kill, and prune all reject a
// closed bead, and prune does not accept the closed state at all. This is the
// operator lever for that state (gc-5fdrr), replacing hand metadata surgery.
//
// agent_name is deliberately preserved: it is historical attribution, not a
// runtime reservation, and nothing resolves a name through it on a closed bead.
//
// A live holder is refused rather than released. Clearing a running session's
// identifiers would let a second session claim the same name, so the remedy
// there is to stop that session first. The refusal is all-or-nothing: no bead
// is modified when any holder is live, so a partial release cannot leave the
// name half-owned.
func ReleaseSessionNameClaim(store beads.Store, target string) ([]ReleasedNameClaim, error) {
	target = strings.TrimSpace(target)
	if target == "" {
		return nil, fmt.Errorf("releasing session name claim: target must not be empty")
	}
	if store == nil {
		return nil, nil
	}

	candidates, err := ExactMetadataSessionCandidates(store, true,
		map[string]string{"session_name": target},
		map[string]string{"alias": target},
	)
	if err != nil {
		return nil, fmt.Errorf("listing sessions holding %q: %w", target, err)
	}

	holders := make([]beads.Bead, 0, len(candidates))
	for _, b := range candidates {
		if !IsSessionBeadOrRepairable(b) {
			continue
		}
		if strings.TrimSpace(b.Metadata["session_name"]) != target &&
			strings.TrimSpace(b.Metadata["alias"]) != target {
			continue
		}
		if b.Status != "closed" {
			return nil, fmt.Errorf("%w: %q is held by %s (status %s) — stop that session first",
				ErrSessionNameClaimLive, target, b.ID, b.Status)
		}
		holders = append(holders, b)
	}
	// Candidate order follows the per-filter query order, so sort by bead ID
	// for a stable report across the session_name and alias lookups.
	sort.Slice(holders, func(i, j int) bool { return holders[i].ID < holders[j].ID })

	released := make([]ReleasedNameClaim, 0, len(holders))
	for _, b := range holders {
		meta := map[string]string{"session_name": "", "alias": ""}
		freeCanonicalIdentityMetadata(meta)
		if err := store.Update(b.ID, beads.UpdateOpts{Metadata: meta}); err != nil {
			return released, fmt.Errorf("releasing session name claim on %s: %w", b.ID, err)
		}
		released = append(released, ReleasedNameClaim{
			BeadID:      b.ID,
			SessionName: strings.TrimSpace(b.Metadata["session_name"]),
			Alias:       strings.TrimSpace(b.Metadata["alias"]),
		})
	}
	return released, nil
}
