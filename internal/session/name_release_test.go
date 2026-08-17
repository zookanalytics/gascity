package session

import (
	"errors"
	"testing"

	"github.com/gastownhall/gascity/internal/beads"
)

// closedNameHolder stores a CLOSED session bead holding the given runtime
// identifiers, in the shape the live outage left behind: session_name, alias,
// and the canonical-identity record all retained past close.
func closedNameHolder(t *testing.T, store beads.Store, sessionName, identity string) string {
	t.Helper()
	bead, err := store.Create(beads.Bead{
		Type:   BeadType,
		Labels: []string{LabelSession},
		Metadata: map[string]string{
			"alias":                       identity,
			"agent_name":                  identity,
			CanonicalInstanceNameMetadata: identity,
			CanonicalPoolSlotMetadata:     "2",
		},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := store.Update(bead.ID, beads.UpdateOpts{Metadata: map[string]string{"session_name": sessionName}}); err != nil {
		t.Fatalf("Update(session_name): %v", err)
	}
	if err := store.Close(bead.ID); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return bead.ID
}

// TestReleaseSessionNameClaim_ClearsClosedHolderReservations is the operator
// lever the outage lacked: a CLOSED session bead reserving a runtime name has
// its reservations cleared by name, without metadata surgery.
func TestReleaseSessionNameClaim_ClearsClosedHolderReservations(t *testing.T) {
	store := beads.NewMemStore()
	const (
		sessionName = "shutupandlisten--gc-toolkit__refinery"
		identity    = "shutupandlisten/gc-toolkit.refinery"
	)
	id := closedNameHolder(t, store, sessionName, identity)

	released, err := ReleaseSessionNameClaim(store, sessionName)
	if err != nil {
		t.Fatalf("ReleaseSessionNameClaim: %v", err)
	}
	if len(released) != 1 || released[0].BeadID != id {
		t.Fatalf("released = %+v, want one entry for %s", released, id)
	}
	if released[0].SessionName != sessionName || released[0].Alias != identity {
		t.Fatalf("released[0] = %+v, want session_name %q and alias %q", released[0], sessionName, identity)
	}

	got, err := store.Get(id)
	if err != nil {
		t.Fatalf("store.Get: %v", err)
	}
	for _, key := range []string{"session_name", "alias", CanonicalInstanceNameMetadata, CanonicalPoolSlotMetadata} {
		if v := got.Metadata[key]; v != "" {
			t.Fatalf("metadata[%q] = %q, want empty after release", key, v)
		}
	}
	// agent_name is history, not a runtime reservation — it survives.
	if got.Metadata["agent_name"] != identity {
		t.Fatalf("agent_name = %q, want %q preserved as history", got.Metadata["agent_name"], identity)
	}

	// The released name is available again.
	if err := ensureSessionNameAvailable(store, sessionName); err != nil {
		t.Fatalf("ensureSessionNameAvailable after release = %v, want nil", err)
	}
}

// TestReleaseSessionNameClaim_ResolvesByAlias confirms the lever accepts the
// human-facing alias as well as the sanitized runtime session name — the
// operator hitting this deadlock knows the alias, not the tmux-safe form.
func TestReleaseSessionNameClaim_ResolvesByAlias(t *testing.T) {
	store := beads.NewMemStore()
	const (
		sessionName = "shutupandlisten--gc-toolkit__refinery"
		identity    = "shutupandlisten/gc-toolkit.refinery"
	)
	id := closedNameHolder(t, store, sessionName, identity)

	released, err := ReleaseSessionNameClaim(store, identity)
	if err != nil {
		t.Fatalf("ReleaseSessionNameClaim(alias): %v", err)
	}
	if len(released) != 1 || released[0].BeadID != id {
		t.Fatalf("released = %+v, want one entry for %s", released, id)
	}
}

// TestReleaseSessionNameClaim_RefusesLiveHolder is the safety gate: releasing a
// name a running session still owns would let a second session claim it. The
// operator stops that session instead.
func TestReleaseSessionNameClaim_RefusesLiveHolder(t *testing.T) {
	store := beads.NewMemStore()
	const sessionName = "shutupandlisten--gc-toolkit__refinery"
	live, err := store.Create(beads.Bead{
		Type:     BeadType,
		Labels:   []string{LabelSession},
		Metadata: map[string]string{"session_name": sessionName},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	released, err := ReleaseSessionNameClaim(store, sessionName)
	if !errors.Is(err, ErrSessionNameClaimLive) {
		t.Fatalf("ReleaseSessionNameClaim(live holder) = %v, want %v", err, ErrSessionNameClaimLive)
	}
	if len(released) != 0 {
		t.Fatalf("released = %+v, want none when a live holder blocks", released)
	}
	if got, getErr := store.Get(live.ID); getErr != nil {
		t.Fatalf("store.Get: %v", getErr)
	} else if got.Metadata["session_name"] != sessionName {
		t.Fatalf("session_name = %q, want %q left intact", got.Metadata["session_name"], sessionName)
	}
}

// TestReleaseSessionNameClaim_NoHolder reports a clean no-op rather than an
// error, so the lever is safe to run speculatively while diagnosing.
func TestReleaseSessionNameClaim_NoHolder(t *testing.T) {
	store := beads.NewMemStore()

	released, err := ReleaseSessionNameClaim(store, "nobody-holds-this")
	if err != nil {
		t.Fatalf("ReleaseSessionNameClaim(no holder) = %v, want nil", err)
	}
	if len(released) != 0 {
		t.Fatalf("released = %+v, want none", released)
	}
}

// TestReleaseSessionNameClaim_RejectsEmptyTarget guards against an empty
// target matching every bead with an unset identifier.
func TestReleaseSessionNameClaim_RejectsEmptyTarget(t *testing.T) {
	store := beads.NewMemStore()

	if _, err := ReleaseSessionNameClaim(store, "  "); err == nil {
		t.Fatal("ReleaseSessionNameClaim(empty target) = nil, want error")
	}
}
