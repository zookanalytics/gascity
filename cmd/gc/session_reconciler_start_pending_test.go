package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/clock"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/events"
	"github.com/gastownhall/gascity/internal/runtime"
	sessionpkg "github.com/gastownhall/gascity/internal/session"
)

// TestPendingCreateLifecycleStatesAreKnown guards the recurring "reconciler
// skips a valid session as unknown state" regression class.
//
// The pending-create lifecycle writes StateStartPending (identity reserved,
// no provider Start in flight yet), then StateCreating (Start in flight), and
// — on rollback — StateFailedCreate to a session bead's metadata["state"].
// Every one of these MUST be a member of knownSessionStates, or the reconciler
// logs "session reconciler: skipping <name> with unknown state ..." every tick
// and skips the bead, so a queued session never advances and instead rolls
// back when its create lease expires.
//
// This exact gap shipped twice: failed-create was missing until #1912, and
// start-pending was missing until #2583 (the gc-2mjzeg incident). Adding a new
// pending-create State without registering it in knownSessionStates must fail
// loudly here rather than silently choking session spawns under load.
func TestPendingCreateLifecycleStatesAreKnown(t *testing.T) {
	for _, state := range []sessionpkg.State{
		sessionpkg.StateStartPending,
		sessionpkg.StateCreating,
		sessionpkg.StateFailedCreate,
	} {
		if !knownSessionStates[string(state)] {
			t.Errorf("knownSessionStates is missing pending-create state %q; the "+
				"reconciler will skip queued sessions in this state as \"unknown "+
				"state\" and roll them back at lease expiry (see gc-2mjzeg)", state)
		}
	}
}

// TestReconcileSessionBeads_StartPendingNotLoggedAsUnknownState is the
// behavioral regression for gc-2mjzeg: a freshly queued start-pending pool
// session whose create lease is still active must be recognized by the
// reconciler — not skipped as "unknown state" and not rolled back.
//
// Before #2583 added start-pending to knownSessionStates, the reconciler
// emitted `session reconciler: skipping <name> with unknown state
// "start-pending"` on every tick until the create lease expired, and the
// session never advanced from start-pending to active. Under bursty spawn
// load this masqueraded as wake-budget exhaustion.
func TestReconcileSessionBeads_StartPendingNotLoggedAsUnknownState(t *testing.T) {
	store := beads.NewMemStore()
	clk := &clock.Fake{Time: time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)}
	sp := runtime.NewFake()
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Agents: []config.Agent{
			{Name: "worker", StartCommand: "true", MinActiveSessions: intPtr(1), MaxActiveSessions: intPtr(3)},
		},
	}

	// A queued pool session: identity reserved (start-pending), claim held,
	// lease fresh (started "now" → not expired). This is the state a sling
	// leaves a session in before the provider Start call goes in flight.
	startPending, err := store.Create(beads.Bead{
		Title:  "worker-1",
		Type:   sessionBeadType,
		Labels: []string{sessionBeadLabel, "agent:worker-1"},
		Metadata: map[string]string{
			"session_name":              "worker-1",
			"agent_name":                "worker-1",
			"template":                  "worker",
			"state":                     string(sessionpkg.StateStartPending),
			"pool_slot":                 "1",
			"pending_create_claim":      boolMetadata(true),
			"pending_create_started_at": pendingCreateStartedAtNow(clk.Now()),
			poolManagedMetadataKey:      boolMetadata(true),
			"live_hash":                 runtime.LiveFingerprint(runtime.Config{Command: "true"}),
			"generation":                "1",
			"instance_token":            "queued-token",
		},
	})
	if err != nil {
		t.Fatalf("Create start-pending bead: %v", err)
	}

	// The session is desired-running: it was queued to satisfy pool demand.
	ds := map[string]TemplateParams{
		"worker-1": {
			TemplateName: "worker",
			InstanceName: "worker-1",
			Command:      "true",
			PoolSlot:     1,
		},
	}

	sessions, _ := loadSessionBeads(store)
	cfgNames := configuredSessionNames(cfg, "", store)
	poolDesired := map[string]int{"worker": 1}
	var stdout, stderr bytes.Buffer
	reconcileSessionBeads(
		context.Background(), sessions, ds, cfgNames,
		cfg, sp, store, nil, nil, nil, newDrainTracker(), poolDesired, false, nil, "test-city",
		nil, clk, events.Discard, 0, 0, &stdout, &stderr,
	)

	if strings.Contains(stderr.String(), "unknown state") {
		t.Errorf("reconciler logged unknown state for a start-pending session: %s", stderr.String())
	}

	// With an active create lease the queued session must not be rolled back
	// (closed) — it should be left to advance to creating/active.
	got, err := store.Get(startPending.ID)
	if err != nil {
		t.Fatalf("Get start-pending bead: %v", err)
	}
	if got.Status == "closed" {
		t.Errorf("start-pending session was closed/rolled back despite an active lease (close_reason=%q)",
			got.Metadata["close_reason"])
	}
}
