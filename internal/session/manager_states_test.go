package session

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/runtime"
)

func createTestSession(t *testing.T, m *Manager, template string) string {
	t.Helper()
	sp := m.sp.(*runtime.Fake)
	_ = sp // ensure fake provider available

	b, err := m.store.Create(beads.Bead{
		Title: template,
		Type:  BeadType,
		Labels: []string{
			LabelSession,
			"template:" + template,
		},
		Metadata: map[string]string{
			"template":     template,
			"state":        string(StateActive),
			"session_name": "s-test-" + template,
		},
	})
	if err != nil {
		t.Fatalf("creating test bead: %v", err)
	}
	return b.ID
}

func getState(t *testing.T, m *Manager, id string) State {
	t.Helper()
	b, err := m.store.Get(id)
	if err != nil {
		t.Fatalf("getting bead: %v", err)
	}
	return State(b.Metadata["state"])
}

func TestConformance_CreatingState(t *testing.T) {
	store := beads.NewMemStore()
	sp := runtime.NewFake()
	m := NewManagerWithOptions(store, sp)

	// Create a bead in creating state.
	b, err := store.Create(beads.Bead{
		Title:  "worker",
		Type:   BeadType,
		Labels: []string{LabelSession},
		Metadata: map[string]string{
			"template":             "worker",
			"state":                string(StateCreating),
			"pending_create_claim": "true",
			"sleep_reason":         "idle-timeout",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Confirm creation transitions to active.
	if err := m.ConfirmCreation(b.ID); err != nil {
		t.Fatal(err)
	}
	if s := getState(t, m, b.ID); s != StateActive {
		t.Errorf("state = %q, want %q", s, StateActive)
	}
	// Check state_reason.
	got, _ := store.Get(b.ID)
	if got.Metadata["state_reason"] != "creation_complete" {
		t.Errorf("state_reason = %q, want creation_complete", got.Metadata["state_reason"])
	}
	if got.Metadata["pending_create_claim"] != "" {
		t.Errorf("pending_create_claim = %q, want cleared", got.Metadata["pending_create_claim"])
	}
	if got.Metadata["sleep_reason"] != "" {
		t.Errorf("sleep_reason = %q, want cleared", got.Metadata["sleep_reason"])
	}
}

func TestConformance_DrainState(t *testing.T) {
	store := beads.NewMemStore()
	sp := runtime.NewFake()
	m := NewManagerWithOptions(store, sp)

	id := createTestSession(t, m, "worker")

	// Begin drain.
	if err := m.BeginDrain(id, "config-drift"); err != nil {
		t.Fatal(err)
	}
	if s := getState(t, m, id); s != StateDraining {
		t.Errorf("state = %q, want %q", s, StateDraining)
	}
	b, _ := store.Get(id)
	if b.Metadata["state_reason"] != "config-drift" {
		t.Errorf("state_reason = %q, want config-drift", b.Metadata["state_reason"])
	}
	if b.Metadata["drain_at"] == "" {
		t.Error("drain_at should be set")
	}

	// Archive after drain.
	if err := m.Archive(id, "drain_complete"); err != nil {
		t.Fatal(err)
	}
	if s := getState(t, m, id); s != StateArchived {
		t.Errorf("state = %q, want %q", s, StateArchived)
	}
	b, _ = store.Get(id)
	if b.Metadata["archived_at"] == "" {
		t.Error("archived_at should be set")
	}
	if b.Metadata["pending_create_claim"] != "" {
		t.Errorf("pending_create_claim = %q, want cleared", b.Metadata["pending_create_claim"])
	}
	if b.Metadata["continuity_eligible"] != "false" {
		t.Errorf("continuity_eligible = %q, want false", b.Metadata["continuity_eligible"])
	}
}

func TestConformance_QuarantineState(t *testing.T) {
	store := beads.NewMemStore()
	sp := runtime.NewFake()
	m := NewManagerWithOptions(store, sp)

	id := createTestSession(t, m, "worker")
	if err := store.SetMetadata(id, "last_woke_at", time.Now().UTC().Format(time.RFC3339)); err != nil {
		t.Fatal(err)
	}

	until := time.Now().Add(5 * time.Minute)
	if err := m.Quarantine(id, until, 3); err != nil {
		t.Fatal(err)
	}
	if s := getState(t, m, id); s != StateQuarantined {
		t.Errorf("state = %q, want %q", s, StateQuarantined)
	}
	b, _ := store.Get(id)
	if b.Metadata["quarantine_cycle"] != "3" {
		t.Errorf("quarantine_cycle = %q, want 3", b.Metadata["quarantine_cycle"])
	}
	if b.Metadata["quarantined_until"] == "" {
		t.Error("quarantined_until should be set")
	}
	if b.Metadata["last_woke_at"] != "" {
		t.Errorf("last_woke_at = %q, want cleared", b.Metadata["last_woke_at"])
	}
}

func TestConformance_ArchivedReactivation(t *testing.T) {
	store := beads.NewMemStore()
	sp := runtime.NewFake()
	m := NewManagerWithOptions(store, sp)

	id := createTestSession(t, m, "worker")

	// Archive first.
	if err := m.Archive(id, "scale-down"); err != nil {
		t.Fatal(err)
	}
	if s := getState(t, m, id); s != StateArchived {
		t.Fatalf("state = %q, want %q", s, StateArchived)
	}

	if err := store.SetMetadata(id, "pending_create_claim", "true"); err != nil {
		t.Fatal(err)
	}
	if err := store.SetMetadata(id, "continuity_eligible", "false"); err != nil {
		t.Fatal(err)
	}

	// Reactivate.
	if err := m.Reactivate(id); err != nil {
		t.Fatal(err)
	}
	if s := getState(t, m, id); s != StateAsleep {
		t.Errorf("state = %q, want %q after reactivation", s, StateAsleep)
	}
	b, _ := store.Get(id)
	if b.Metadata["state_reason"] != "reactivated" {
		t.Errorf("state_reason = %q, want reactivated", b.Metadata["state_reason"])
	}
	if b.Metadata["pending_create_claim"] != "" {
		t.Errorf("pending_create_claim = %q, want cleared", b.Metadata["pending_create_claim"])
	}
	if b.Metadata["continuity_eligible"] != "false" {
		t.Errorf("continuity_eligible = %q, want preserved false", b.Metadata["continuity_eligible"])
	}
	if b.Metadata["archived_at"] != "" {
		t.Error("archived_at should be cleared on reactivation")
	}
}

func TestConformance_IllegalTransitionDraining(t *testing.T) {
	// Fix 3j: manager mutations now validate against the state machine.
	// Drain puts a session in Draining; Suspend from Draining is illegal.
	store := beads.NewMemStore()
	sp := runtime.NewFake()
	m := NewManagerWithOptions(store, sp)

	id := createTestSession(t, m, "worker")

	if err := m.BeginDrain(id, "shutdown"); err != nil {
		t.Fatalf("BeginDrain: %v", err)
	}

	err := m.Suspend(id)
	if err == nil {
		t.Fatal("Suspend from Draining should return ErrIllegalTransition")
	}
	if !errors.Is(err, ErrIllegalTransition) {
		t.Errorf("err = %v, want wrapping ErrIllegalTransition", err)
	}
	var ite *IllegalTransitionError
	if !errors.As(err, &ite) {
		t.Fatalf("err should unwrap to *IllegalTransitionError; got %T", err)
	}
	if ite.From != StateDraining {
		t.Errorf("ite.From = %q, want %q", ite.From, StateDraining)
	}
	if ite.Command != CmdSuspend {
		t.Errorf("ite.Command = %q, want %q", ite.Command, CmdSuspend)
	}
}

func TestConformance_SuspendFailedCreateTearsDownRuntime(t *testing.T) {
	// #2597: `gc stop` issues suspend on every session bead, including
	// failed-create ones (it does not pre-filter by state). failed-create is a
	// create-rollback terminal state with no live turn to suspend, but it may
	// have leaked a runtime process. Under a backing-store outage the reconciler
	// cannot reap these (its close path requires a reachable store), so suspend
	// is the only thing that can tear the leaked process down. Suspend must
	// therefore succeed and stop the runtime rather than reject the command
	// with an illegal-transition error that blocks `gc stop` city-wide.
	store := beads.NewMemStore()
	sp := runtime.NewFake()
	m := NewManagerWithOptions(store, sp)

	id := createTestSession(t, m, "dog")
	b, err := store.Get(id)
	if err != nil {
		t.Fatalf("get bead: %v", err)
	}
	sessName := b.Metadata["session_name"]

	// Seed a leaked runtime process and the failed-create landing state.
	if err := sp.Start(context.Background(), sessName, runtime.Config{}); err != nil {
		t.Fatalf("seeding runtime: %v", err)
	}
	if err := store.SetMetadata(id, "state", string(StateFailedCreate)); err != nil {
		t.Fatalf("set failed-create state: %v", err)
	}

	// Suspend(failed-create) must succeed so `gc stop` is not blocked
	// city-wide. The pre-fix regression returned a wrapped ErrIllegalTransition;
	// either symptom (any non-nil) trips this assertion and pinpoints the
	// regression by quoting the returned error.
	if err := m.Suspend(id); err != nil {
		t.Fatalf("Suspend(failed-create) = %v, want nil (must not block gc stop)", err)
	}
	if sp.CountCalls("Stop", sessName) == 0 {
		t.Errorf("Suspend(failed-create) did not tear down the leaked runtime session %q", sessName)
	}
	if sp.IsRunning(sessName) {
		t.Errorf("runtime session %q still running after Suspend(failed-create)", sessName)
	}
}

func TestConformance_SuspendRejectsMidCreate(t *testing.T) {
	// Suspend promises the caller a durably paused session, and a mid-create
	// bead cannot honor that promise. start-pending means the controller has
	// reserved an identity and still intends to start it, and the reconciler
	// reads raw start-pending — and pending_create_claim — as a start request
	// (sessionStartRequestedInfo in cmd/gc/session_reconcile.go), so a bead left
	// in that state is re-launched on the next tick. Reporting success after
	// only tearing the runtime down would hand POST /v0/session/{id}/suspend a
	// 200 for a session that is still queued to start.
	//
	// Tearing a mid-create runtime down is a real need — force shutdown's late
	// sweep depends on it (gc-04375) — but it is a teardown, not a suspension,
	// and it belongs to Kill. See TestConformance_KillTearsDownMidCreateRuntime
	// for that half of the contract, and stopTargetThroughWorkerBoundary in
	// cmd/gc/session_lifecycle_parallel.go for the stop path that routes to it.
	for _, state := range []State{StateStartPending, StateCreating} {
		t.Run(string(state), func(t *testing.T) {
			store := beads.NewMemStore()
			sp := runtime.NewFake()
			m := NewManagerWithOptions(store, sp)

			id := createTestSession(t, m, "dog")
			b, err := store.Get(id)
			if err != nil {
				t.Fatalf("get bead: %v", err)
			}
			sessName := b.Metadata["session_name"]

			// A live runtime plus a bead that has not reached
			// creation_complete: the provider start landed, the commit that
			// marks the bead active has not.
			if err := sp.Start(context.Background(), sessName, runtime.Config{}); err != nil {
				t.Fatalf("seeding runtime: %v", err)
			}
			if err := store.SetMetadata(id, "state", string(state)); err != nil {
				t.Fatalf("set %s state: %v", state, err)
			}
			if err := store.SetMetadata(id, "pending_create_claim", "true"); err != nil {
				t.Fatalf("set pending_create_claim: %v", err)
			}

			err = m.Suspend(id)
			if err == nil {
				t.Fatalf("Suspend(%s) = nil, want a conflict: the bead is still queued to start", state)
			}
			if !errors.Is(err, ErrIllegalTransition) {
				t.Errorf("Suspend(%s) = %v, want it to wrap ErrIllegalTransition", state, err)
			}
			var ite *IllegalTransitionError
			if !errors.As(err, &ite) {
				t.Fatalf("Suspend(%s) should unwrap to *IllegalTransitionError; got %T", state, err)
			}
			if ite.From != state {
				t.Errorf("ite.From = %q, want %q", ite.From, state)
			}
			if ite.Command != CmdSuspend {
				t.Errorf("ite.Command = %q, want %q", ite.Command, CmdSuspend)
			}

			// A rejected transition must leave no trace. Half-applying it --
			// tearing the runtime down but leaving the bead queued to start --
			// is the exact shape this rejection exists to prevent.
			if got := sp.CountCalls("Stop", sessName); got != 0 {
				t.Errorf("rejected Suspend(%s) called Stop %d time(s) on %q, want 0", state, got, sessName)
			}
			if got := getState(t, m, id); got != state {
				t.Errorf("state = %q after the rejected Suspend(%s), want it left at %q", got, state, state)
			}
			after, err := store.Get(id)
			if err != nil {
				t.Fatalf("get bead: %v", err)
			}
			if got := after.Metadata["pending_create_claim"]; got != "true" {
				t.Errorf("pending_create_claim = %q after the rejected Suspend(%s), want it left at \"true\" for the reconciler", got, state)
			}
			if got, ok := after.Metadata["suspended_at"]; ok && got != "" {
				t.Errorf("rejected Suspend(%s) wrote suspended_at = %q", state, got)
			}
		})
	}
}

func TestConformance_KillTearsDownMidCreateRuntime(t *testing.T) {
	// The teardown half of the mid-create contract, and the lever the stop path
	// routes these states to once Suspend refuses them
	// (stopTargetThroughWorkerBoundary in
	// cmd/gc/session_lifecycle_parallel.go). The runtime dies; the persisted
	// lifecycle does not move. The bead is deliberately left mid-create: an
	// in-flight create may still be running, and the reconciler owns reaping a
	// create that never completed, so recording a suspension here would claim a
	// lifecycle the session never had.
	//
	// Force shutdown's late async sweep is what needs this. It re-lists the
	// fleet after abandoning the async-start wait, specifically to catch
	// sessions created too late for the first stop pass — and those are
	// exactly the ones whose create commit has not landed. Whether the sweep
	// stopped them or leaked them used to come down to whether the async commit
	// won the race to flip the bead to active, which is what made
	// TestCityRuntimeForceShutdownTearsDownAfterLateAsyncSweep fail under load
	// (gc-04375).
	for _, state := range []State{StateStartPending, StateCreating} {
		t.Run(string(state), func(t *testing.T) {
			store := beads.NewMemStore()
			sp := runtime.NewFake()
			m := NewManagerWithOptions(store, sp)

			id := createTestSession(t, m, "dog")
			b, err := store.Get(id)
			if err != nil {
				t.Fatalf("get bead: %v", err)
			}
			sessName := b.Metadata["session_name"]

			if err := sp.Start(context.Background(), sessName, runtime.Config{}); err != nil {
				t.Fatalf("seeding runtime: %v", err)
			}
			if err := store.SetMetadata(id, "state", string(state)); err != nil {
				t.Fatalf("set %s state: %v", state, err)
			}
			if err := store.SetMetadata(id, "pending_create_claim", "true"); err != nil {
				t.Fatalf("set pending_create_claim: %v", err)
			}

			if err := m.Kill(id); err != nil {
				t.Fatalf("Kill(%s) = %v, want nil (must not leave the runtime behind)", state, err)
			}
			if sp.CountCalls("Stop", sessName) == 0 {
				t.Errorf("Kill(%s) did not tear down runtime session %q", state, sessName)
			}
			if sp.IsRunning(sessName) {
				t.Errorf("runtime session %q still running after Kill(%s)", sessName, state)
			}
			if got := getState(t, m, id); got != state {
				t.Errorf("state = %q after Kill(%s), want it left at %q for the reconciler", got, state, state)
			}
			after, err := store.Get(id)
			if err != nil {
				t.Fatalf("get bead: %v", err)
			}
			if got := after.Metadata["pending_create_claim"]; got != "true" {
				t.Errorf("pending_create_claim = %q after Kill(%s), want it left at \"true\" for the reconciler", got, state)
			}
		})
	}
}

func TestConformance_KillMidCreateWithNoRuntimeSucceeds(t *testing.T) {
	// start-pending routinely has no runtime at all: it means the controller
	// reserved an identity and intends to start it, with no provider Start call
	// in flight (see StateStartPending). Killing such a bead must be a quiet
	// no-op, because `gc stop` reaches every session bead with no state
	// pre-filter — an error here would report a stop failure for every session
	// that had not yet reached its provider start.
	store := beads.NewMemStore()
	sp := runtime.NewFake()
	m := NewManagerWithOptions(store, sp)

	id := createTestSession(t, m, "dog")
	b, err := store.Get(id)
	if err != nil {
		t.Fatalf("get bead: %v", err)
	}
	sessName := b.Metadata["session_name"]

	// No Start, and clear anything the helper seeded: the provider start was
	// never issued, which is the common shape of a start-pending bead.
	if err := sp.Stop(sessName); err != nil {
		t.Fatalf("clearing any seeded runtime: %v", err)
	}
	if sp.IsRunning(sessName) {
		t.Fatalf("runtime session %q unexpectedly running before Kill", sessName)
	}
	if err := store.SetMetadata(id, "state", string(StateStartPending)); err != nil {
		t.Fatalf("set start-pending state: %v", err)
	}

	if err := m.Kill(id); err != nil {
		t.Fatalf("Kill(start-pending) = %v for a session that was never running, want nil", err)
	}
	if got := getState(t, m, id); got != StateStartPending {
		t.Errorf("state = %q after Kill(start-pending), want it left at %q", got, StateStartPending)
	}
}

func TestConformance_QuarantineReactivation(t *testing.T) {
	store := beads.NewMemStore()
	sp := runtime.NewFake()
	m := NewManagerWithOptions(store, sp)

	id := createTestSession(t, m, "crasher")

	// Quarantine the session.
	until := time.Now().Add(5 * time.Minute)
	if err := m.Quarantine(id, until, 3); err != nil {
		t.Fatal(err)
	}

	// Reactivate.
	if err := m.Reactivate(id); err != nil {
		t.Fatal(err)
	}
	if s := getState(t, m, id); s != StateAsleep {
		t.Errorf("state = %q, want %q after quarantine reactivation", s, StateAsleep)
	}
	b, _ := store.Get(id)

	// quarantine_cycle should be preserved (for eviction tracking).
	if b.Metadata["quarantine_cycle"] != "3" {
		t.Errorf("quarantine_cycle = %q, want 3 (should be preserved)", b.Metadata["quarantine_cycle"])
	}
	// crash_count should be reset.
	if b.Metadata["crash_count"] != "0" {
		t.Errorf("crash_count = %q, want 0", b.Metadata["crash_count"])
	}
	// quarantined_until should be cleared.
	if b.Metadata["quarantined_until"] != "" {
		t.Error("quarantined_until should be cleared on reactivation")
	}
	// Quarantined non-terminal sessions remain continuity eligible by default.
	if b.Metadata["continuity_eligible"] != "true" {
		t.Errorf("continuity_eligible = %q, want true", b.Metadata["continuity_eligible"])
	}
}

func TestCanonicalLifecycleState(t *testing.T) {
	cases := []struct {
		name string
		in   State
		want State
	}{
		{"empty legacy state normalizes to active", StateNone, StateActive},
		{"awake alias normalizes to active", StateAwake, StateActive},
		{"active is unchanged", StateActive, StateActive},
		{"asleep is unchanged", StateAsleep, StateAsleep},
		{"suspended is unchanged", StateSuspended, StateSuspended},
		{"failed-create is unchanged", StateFailedCreate, StateFailedCreate},
		{"drained is not remapped here", State("drained"), State("drained")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := canonicalLifecycleState(tc.in); got != tc.want {
				t.Errorf("canonicalLifecycleState(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
