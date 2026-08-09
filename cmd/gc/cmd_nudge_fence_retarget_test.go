package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/runtime"
	"github.com/gastownhall/gascity/internal/session"
	"github.com/gastownhall/gascity/internal/worker"
)

// A mode=always/wake_mode=fresh session bumps continuation_epoch on every wake
// (shouldBumpContinuationEpoch) while keeping its session bead id, so a nudge
// queued before a recycle always meets a moved epoch at delivery. Retargeting
// that drift is what keeps the nudge deliverable; only a wait-sourced nudge —
// whose stamped epoch IS the guarantee it was registered under — stays fenced.
func TestQueuedNudgeMatchesTargetFence_RetargetsSameSessionEpochDrift(t *testing.T) {
	target := nudgeTarget{sessionID: "gc-1", continuationEpoch: "4"}

	cases := []struct {
		name string
		item queuedNudge
		want bool
	}{
		{
			name: "exact fence match delivers",
			item: queuedNudge{SessionID: "gc-1", ContinuationEpoch: "4", Source: "session"},
			want: true,
		},
		{
			name: "unfenced item delivers",
			item: queuedNudge{Source: "session"},
			want: true,
		},
		{
			name: "same session stale epoch retargets",
			item: queuedNudge{SessionID: "gc-1", ContinuationEpoch: "3", Source: "session"},
			want: true,
		},
		{
			name: "same session stale epoch retargets for mail source",
			item: queuedNudge{SessionID: "gc-1", ContinuationEpoch: "1", Source: "mail"},
			want: true,
		},
		{
			name: "same session ahead-of-target epoch retargets",
			item: queuedNudge{SessionID: "gc-1", ContinuationEpoch: "9", Source: "session"},
			want: true,
		},
		{
			name: "wait source keeps its registered epoch fenced",
			item: queuedNudge{SessionID: "gc-1", ContinuationEpoch: "3", Source: "wait"},
			want: false,
		},
		{
			name: "other session never retargets",
			item: queuedNudge{SessionID: "gc-2", ContinuationEpoch: "4", Source: "session"},
			want: false,
		},
		{
			name: "epoch without session identity stays fenced",
			item: queuedNudge{ContinuationEpoch: "3", Source: "session"},
			want: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := queuedNudgeMatchesTargetFence(target, tc.item); got != tc.want {
				t.Fatalf("queuedNudgeMatchesTargetFence(%+v) = %v, want %v", tc.item, got, tc.want)
			}
		})
	}
}

// An unresolvable live epoch must not destroy a same-session nudge either: the
// session identity still matches, so the message is still addressed to this
// agent.
func TestQueuedNudgeMatchesTargetFence_RetargetsWhenTargetEpochUnresolved(t *testing.T) {
	target := nudgeTarget{sessionID: "gc-1"}

	item := queuedNudge{SessionID: "gc-1", ContinuationEpoch: "3", Source: "session"}
	if !queuedNudgeMatchesTargetFence(target, item) {
		t.Fatal("queuedNudgeMatchesTargetFence = false, want same-session nudge retargeted when the live epoch is unresolved")
	}

	waitItem := queuedNudge{SessionID: "gc-1", ContinuationEpoch: "3", Source: "wait"}
	if queuedNudgeMatchesTargetFence(target, waitItem) {
		t.Fatal("queuedNudgeMatchesTargetFence = true, want wait-sourced nudge fenced when the live epoch is unresolved")
	}
}

func TestSplitQueuedNudgesForTarget_RetargetsFreshWakeDriftAndKeepsWaitFenced(t *testing.T) {
	target := nudgeTarget{
		agent:             config.Agent{Name: "worker"},
		sessionID:         "gc-1",
		continuationEpoch: "5",
	}
	items := []queuedNudge{
		{ID: "n-session-stale", SessionID: "gc-1", ContinuationEpoch: "4", Source: "session"},
		{ID: "n-wait-stale", SessionID: "gc-1", ContinuationEpoch: "4", Source: "wait"},
		{ID: "n-current", SessionID: "gc-1", ContinuationEpoch: "5", Source: "session"},
	}

	deliverable, rejected := splitQueuedNudgesForTarget(target, items)

	if got := queuedNudgeIDs(deliverable); len(got) != 2 || got[0] != "n-session-stale" || got[1] != "n-current" {
		t.Fatalf("deliverable = %#v, want [n-session-stale n-current]", got)
	}
	if got := queuedNudgeIDs(rejected); len(got) != 1 || got[0] != "n-wait-stale" {
		t.Fatalf("rejected = %#v, want [n-wait-stale]", got)
	}
}

// End-to-end over the delivery path every dispatcher shares: a nudge queued
// against the pre-recycle epoch is delivered to the live conversation instead
// of being dead-lettered as a fence mismatch.
func TestTryDeliverQueuedNudgesByPollerDeliversAcrossFreshWakeEpochBump(t *testing.T) {
	t.Setenv("GC_BEADS", "file")
	dir := t.TempDir()
	queuedAt := time.Now().Add(-1 * time.Minute)

	store := beads.NewMemStore()
	fake := runtime.NewFake()
	mgr := newSessionManagerWithConfig(dir, store, fake, nil)
	info, err := mgr.CreateSession(context.Background(), session.CreateOptions{
		Template:  "worker",
		Title:     "Worker",
		Command:   "codex",
		WorkDir:   dir,
		Provider:  "codex",
		Hints:     runtime.Config{WorkDir: dir},
		ExtraMeta: map[string]string{"session_origin": "manual"},
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if err := mgr.Start(context.Background(), info.ID, "", runtime.Config{WorkDir: dir}); err != nil {
		t.Fatalf("Start: %v", err)
	}
	idleSince := time.Now().Add(-10 * time.Second)
	fake.Activity = map[string]time.Time{info.SessionName: idleSince}

	// Queued while the session was in epoch 1; the fresh wake bumped it to 2.
	stale := newQueuedNudgeWithOptions("worker", "run gc prime and check your hook", "session", queuedAt, queuedNudgeOptions{
		SessionID:         info.ID,
		ContinuationEpoch: "1",
	})
	if err := enqueueQueuedNudgeWithStore(dir, beads.NudgesStore{Store: store}, stale); err != nil {
		t.Fatalf("enqueueQueuedNudgeWithStore: %v", err)
	}

	target := nudgeTarget{
		cityPath:          dir,
		agent:             config.Agent{Name: "worker"},
		sessionID:         info.ID,
		continuationEpoch: "2",
		resolved:          &config.ResolvedProvider{Name: "codex"},
		sessionName:       info.SessionName,
	}
	obs := worker.LiveObservation{Running: true, LastActivity: &idleSince}

	delivered, err := tryDeliverQueuedNudgesByPoller(target, store, store, fake, 3*time.Second, obs)
	if err != nil {
		t.Fatalf("tryDeliverQueuedNudgesByPoller: %v", err)
	}
	if !delivered {
		t.Fatal("delivered = false, want the pre-recycle nudge retargeted onto the live conversation")
	}

	var nudgeCalls []runtime.Call
	for _, call := range fake.Calls {
		if call.Method == "Nudge" {
			nudgeCalls = append(nudgeCalls, call)
		}
	}
	if len(nudgeCalls) != 1 {
		t.Fatalf("nudge calls = %d, want 1", len(nudgeCalls))
	}
	if !strings.Contains(nudgeCalls[0].Message, "run gc prime and check your hook") {
		t.Fatalf("nudge message = %q, want the queued reminder", nudgeCalls[0].Message)
	}

	pending, inFlight, dead, err := listQueuedNudges(dir, "worker", time.Now())
	if err != nil {
		t.Fatalf("listQueuedNudges: %v", err)
	}
	if len(pending) != 0 || len(inFlight) != 0 || len(dead) != 0 {
		t.Fatalf("pending/inFlight/dead = %d/%d/%d, want 0/0/0 (delivered and acked)", len(pending), len(inFlight), len(dead))
	}
}
