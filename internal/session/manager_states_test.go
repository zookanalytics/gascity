package session

import (
	"context"
	"errors"
	"strings"
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

// seedDrainAckStopPending puts a row into draining the way production does:
// the controller's drain-ack path is the only writer of state=draining.
func seedDrainAckStopPending(t *testing.T, m *Manager, id string) {
	t.Helper()
	if err := m.PersistedStore().ApplyPatch(id, DrainAckStopPendingPatch(time.Now())); err != nil {
		t.Fatalf("seeding drain-ack stop-pending: %v", err)
	}
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
	m := newTestManager(t, store, sp)

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

func TestConformance_SuspendDrainingTearsDownRuntimeWithoutRewritingState(t *testing.T) {
	// ga-rxhu2: `gc stop` and `gc restart` issue suspend on every session bead
	// with no state pre-filter. Suspend from draining used to return
	// ErrIllegalTransition, so every restart SKIPPED the draining seats and they
	// survived as live panes still holding their pool slot names. Handle draining
	// the way failed-create is already handled (#2597, same function): tear the
	// runtime down best-effort and report success.
	//
	// Deliberately NOT a new state-machine edge. Adding
	// StateDraining -> StateSuspended to CmdSuspend would release nothing (an OPEN
	// bead owns its session_name whatever its state) and would let an operator
	// "suspend" a seat mid-drain, losing the drain reason. So the early return
	// must leave `state` alone for the drain machinery to finish or the reconciler
	// to reap.
	store := beads.NewMemStore()
	sp := runtime.NewFake()
	m := newTestManager(t, store, sp)

	id := createTestSession(t, m, "worker")
	b, err := store.Get(id)
	if err != nil {
		t.Fatalf("get bead: %v", err)
	}
	sessName := b.Metadata["session_name"]
	if err := sp.Start(context.Background(), sessName, runtime.Config{}); err != nil {
		t.Fatalf("seeding runtime: %v", err)
	}
	seedDrainAckStopPending(t, m, id)

	if err := m.Suspend(context.Background(), testActor(m, ActorSweep), id, false); err != nil {
		t.Fatalf("sweep Suspend(draining) = %v, want nil (must not block gc stop and leave a live pane holding a pool name)", err)
	}
	if sp.CountCalls("Stop", sessName) == 0 {
		t.Errorf("sweep Suspend(draining) did not tear down the runtime session %q", sessName)
	}
	if sp.IsRunning(sessName) {
		t.Errorf("runtime session %q still running after sweep Suspend(draining)", sessName)
	}
	if got := getState(t, m, id); got != StateDraining {
		t.Errorf("state = %q, want %q — the early return must not rewrite state and lose the drain", got, StateDraining)
	}
	after, err := store.Get(id)
	if err != nil {
		t.Fatalf("get bead: %v", err)
	}
	if after.Metadata["suspended_at"] != "" {
		t.Errorf("suspended_at = %q, want empty: this path performs no suspension", after.Metadata["suspended_at"])
	}
	if after.Metadata["drain_at"] == "" {
		t.Error("drain_at was cleared; the drain reason must survive a city stop")
	}
}

// The draining latitude belongs to the city-stop SWEEP, not to an operator
// naming one session. A targeted Suspend of a draining seat has none of the
// gates the reconciler's terminal escalation insists on for the same class of
// kill, so it must keep returning the illegal transition instead of quietly
// killing a live agent mid-drain and reporting 200.
func TestConformance_OperatorSuspendStillRejectsDraining(t *testing.T) {
	store := beads.NewMemStore()
	sp := runtime.NewFake()
	m := newTestManager(t, store, sp)

	id := createTestSession(t, m, "worker")
	b, err := store.Get(id)
	if err != nil {
		t.Fatalf("get bead: %v", err)
	}
	sessName := b.Metadata["session_name"]
	if err := sp.Start(context.Background(), sessName, runtime.Config{}); err != nil {
		t.Fatalf("seeding runtime: %v", err)
	}
	seedDrainAckStopPending(t, m, id)

	err = m.Suspend(context.Background(), testActor(m, ActorOperator), id, false)
	if !errors.Is(err, ErrIllegalTransition) {
		t.Fatalf("Suspend(draining) = %v, want ErrIllegalTransition — an operator suspend must not become an ungated mid-drain kill", err)
	}
	if sp.CountCalls("Stop", sessName) != 0 {
		t.Errorf("operator Suspend(draining) tore down runtime %q anyway", sessName)
	}
	if !sp.IsRunning(sessName) {
		t.Errorf("runtime session %q was killed by a rejected operator suspend", sessName)
	}
}

// A teardown that FAILED must not be reported as success. Callers read nil as
// "the seat stopped" — gc stop prints "Stopped agent", counts it and records
// session.stopped — so swallowing the error makes the sweep claim success over a
// pane still alive holding its pool slot name: the exact wedge class here.
func TestConformance_SuspendForShutdownPropagatesDrainingTeardownFailure(t *testing.T) {
	store := beads.NewMemStore()
	sp := runtime.NewFake()
	m := newTestManager(t, store, sp)

	id := createTestSession(t, m, "worker")
	b, err := store.Get(id)
	if err != nil {
		t.Fatalf("get bead: %v", err)
	}
	sessName := b.Metadata["session_name"]
	if err := sp.Start(context.Background(), sessName, runtime.Config{}); err != nil {
		t.Fatalf("seeding runtime: %v", err)
	}
	sp.StopErrors = map[string]error{sessName: errors.New("provider refused the stop")}
	seedDrainAckStopPending(t, m, id)

	err = m.Suspend(context.Background(), testActor(m, ActorSweep), id, false)
	if err == nil {
		t.Fatal("sweep Suspend(draining) = nil despite a failed teardown; gc stop would report a live pane as stopped")
	}
	if !strings.Contains(err.Error(), "stopping runtime session") {
		t.Errorf("err = %v, want it to name the runtime teardown failure", err)
	}
}

// The scope control for the pin above. The draining escape hatch must be
// draining-ONLY, never a blanket "any state suspends" that would hide a real
// illegal-transition bug elsewhere. (An operator's suspend of a drained row is
// legal and holds it; the sweep and the idle auto-suspend still refuse it, see
// TestOnlyTheOperatorSuspendsADrainedRow.)
func TestConformance_SuspendStillRejectsOtherIllegalStates(t *testing.T) {
	for _, from := range []State{StateStartPending, StateCreating} {
		t.Run(string(from), func(t *testing.T) {
			store := beads.NewMemStore()
			sp := runtime.NewFake()
			m := newTestManager(t, store, sp)

			id := createTestSession(t, m, "worker")
			if err := store.SetMetadata(id, "state", string(from)); err != nil {
				t.Fatalf("set state %q: %v", from, err)
			}

			err := m.Suspend(context.Background(), testActor(m, ActorOperator), id, false)
			if err == nil {
				t.Fatalf("Suspend from %q should return ErrIllegalTransition", from)
			}
			if !errors.Is(err, ErrIllegalTransition) {
				t.Errorf("err = %v, want wrapping ErrIllegalTransition", err)
			}
			var ite *IllegalTransitionError
			if !errors.As(err, &ite) {
				t.Fatalf("err should unwrap to *IllegalTransitionError; got %T", err)
			}
			if ite.From != from {
				t.Errorf("ite.From = %q, want %q", ite.From, from)
			}
			if ite.Command != CmdSuspend {
				t.Errorf("ite.Command = %q, want %q", ite.Command, CmdSuspend)
			}
		})
	}
}

// The other scope control: an ACTIVE seat's suspend is completely unchanged —
// runtime stopped, state rewritten to suspended, suspended_at stamped.
func TestConformance_SuspendActiveSessionUnchanged(t *testing.T) {
	store := beads.NewMemStore()
	sp := runtime.NewFake()
	m := newTestManager(t, store, sp)

	id := createTestSession(t, m, "worker")
	b, err := store.Get(id)
	if err != nil {
		t.Fatalf("get bead: %v", err)
	}
	sessName := b.Metadata["session_name"]
	if err := sp.Start(context.Background(), sessName, runtime.Config{}); err != nil {
		t.Fatalf("seeding runtime: %v", err)
	}

	if err := m.Suspend(context.Background(), testActor(m, ActorOperator), id, false); err != nil {
		t.Fatalf("Suspend(active) = %v, want nil", err)
	}
	if got := getState(t, m, id); got != StateSuspended {
		t.Errorf("state = %q, want %q", got, StateSuspended)
	}
	if sp.IsRunning(sessName) {
		t.Errorf("runtime session %q still running after Suspend(active)", sessName)
	}
	after, err := store.Get(id)
	if err != nil {
		t.Fatalf("get bead: %v", err)
	}
	if after.Metadata["suspended_at"] == "" {
		t.Error("suspended_at not stamped on an ordinary active suspend")
	}
}

// The state machine itself must NOT gain a draining edge: the fix lives in
// Manager.Suspend's pre-checks, and a table edge would release no pool name
// while silently converting a drain into a suspension.
func TestConformance_SuspendTableStillRejectsDraining(t *testing.T) {
	if _, err := Transition(StateDraining, CmdSuspend); !errors.Is(err, ErrIllegalTransition) {
		t.Errorf("Transition(draining, suspend) err = %v, want ErrIllegalTransition — "+
			"the escape hatch belongs in Manager.Suspend, not the transition table", err)
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
	m := newTestManager(t, store, sp)

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
	if err := m.Suspend(context.Background(), testActor(m, ActorOperator), id, false); err != nil {
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
			m := newTestManager(t, store, sp)

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

			err = m.Suspend(context.Background(), testActor(m, ActorOperator), id, false)
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
			m := newTestManager(t, store, sp)

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

			if err := m.Kill(context.Background(), testActor(m, ActorOperator), id); err != nil {
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
	m := newTestManager(t, store, sp)

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

	if err := m.Kill(context.Background(), testActor(m, ActorOperator), id); err != nil {
		t.Fatalf("Kill(start-pending) = %v for a session that was never running, want nil", err)
	}
	if got := getState(t, m, id); got != StateStartPending {
		t.Errorf("state = %q after Kill(start-pending), want it left at %q", got, StateStartPending)
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
