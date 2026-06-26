package beads

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
)

// notifyChange must suppress a second emission whose marshaled payload
// is byte-identical to the previous one for the same (eventType,
// beadID). This is the defense against re-emission loops where a
// caller — write path or reconciler diff — fires a notification that
// reflects no actual wire-payload change.
func TestNotifyChangeDedupsIdenticalEmissions(t *testing.T) {
	t.Parallel()

	var emits int
	cache := NewCachingStoreForTest(NewMemStore(), func(_ string, _ string, _ json.RawMessage) {
		emits++
	})

	bead := Bead{ID: "test-1", Title: "Task", Status: "open"}
	cache.notifyChange("bead.updated", bead)
	cache.notifyChange("bead.updated", bead)

	if emits != 1 {
		t.Fatalf("emits = %d, want 1 (identical second emission must be suppressed)", emits)
	}
}

// A real payload change for the same (eventType, beadID) must emit
// again — dedup is keyed on the marshaled payload, not just the IDs.
func TestNotifyChangeEmitsAfterPayloadChange(t *testing.T) {
	t.Parallel()

	var emits int
	cache := NewCachingStoreForTest(NewMemStore(), func(_ string, _ string, _ json.RawMessage) {
		emits++
	})

	cache.notifyChange("bead.updated", Bead{ID: "test-1", Title: "Task", Status: "open"})
	cache.notifyChange("bead.updated", Bead{ID: "test-1", Title: "Task", Status: "in_progress"})

	if emits != 2 {
		t.Fatalf("emits = %d, want 2 (status change must produce a new emission)", emits)
	}
}

// Different event types are tracked independently so a bead.updated
// never suppresses a later bead.closed for the same bead, even if the
// marshaled payloads happen to match.
func TestNotifyChangeIndependentByEventType(t *testing.T) {
	t.Parallel()

	var emits int
	cache := NewCachingStoreForTest(NewMemStore(), func(_ string, _ string, _ json.RawMessage) {
		emits++
	})

	bead := Bead{ID: "test-1", Title: "Task", Status: "open"}
	cache.notifyChange("bead.updated", bead)
	cache.notifyChange("bead.closed", bead)

	if emits != 2 {
		t.Fatalf("emits = %d, want 2 (different event types must emit independently)", emits)
	}
}

// Different bead IDs are tracked independently — emissions for one
// bead must not suppress emissions for another even when the
// payload-as-bytes is similar.
func TestNotifyChangeIndependentByBeadID(t *testing.T) {
	t.Parallel()

	var emits int
	cache := NewCachingStoreForTest(NewMemStore(), func(_ string, _ string, _ json.RawMessage) {
		emits++
	})

	cache.notifyChange("bead.updated", Bead{ID: "test-1", Title: "Task", Status: "open"})
	cache.notifyChange("bead.updated", Bead{ID: "test-2", Title: "Task", Status: "open"})

	if emits != 2 {
		t.Fatalf("emits = %d, want 2 (different bead IDs must emit independently)", emits)
	}
}

// A payload change followed by reverting to the original state must
// still emit both transitions: the dedup compares against only the
// most-recent emission, not history. The bus is observing a real
// state ping-pong (open → closed → open) and consumers need every
// edge.
func TestNotifyChangeEmitsAfterRevertingPayload(t *testing.T) {
	t.Parallel()

	var emits int
	cache := NewCachingStoreForTest(NewMemStore(), func(_ string, _ string, _ json.RawMessage) {
		emits++
	})

	open := Bead{ID: "test-1", Title: "Task", Status: "open"}
	closed := Bead{ID: "test-1", Title: "Task", Status: "closed"}

	cache.notifyChange("bead.updated", open)
	cache.notifyChange("bead.updated", closed)
	cache.notifyChange("bead.updated", open)

	if emits != 3 {
		t.Fatalf("emits = %d, want 3 (open→closed→open must emit each transition)", emits)
	}
}

// Reconciliation that re-runs over a quiescent backing must not pump
// duplicate notifications onto the bus. Even if internal codepaths
// flag a bead as changed when it isn't, the byte-level dedup catches
// the no-op emission.
func TestRunReconciliationDoesNotEmitWhenBackingIsQuiescent(t *testing.T) {
	t.Parallel()

	backing := NewMemStore()
	bead, err := backing.Create(Bead{Title: "Task", Status: "open"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	var events []string
	cache := NewCachingStoreForTest(backing, func(eventType, beadID string, _ json.RawMessage) {
		events = append(events, eventType+":"+beadID)
	})
	if err := cache.Prime(context.Background()); err != nil {
		t.Fatalf("Prime: %v", err)
	}
	events = nil // ignore prime-driven notifications

	// Force-flag the cache as having had recent local mutations so
	// the slow path runs (the code path that historically re-emitted
	// duplicates the most aggressively).
	cache.mu.Lock()
	cache.mutationSeq++
	cache.mu.Unlock()

	cache.runReconciliation()
	cache.runReconciliation()

	for _, e := range events {
		if e == "bead.updated:"+bead.ID || e == "bead.closed:"+bead.ID {
			t.Fatalf("reconciler emitted notification for unchanged bead; events=%v", events)
		}
	}
}

// Concurrent notifyChange calls for the same (eventType, beadID,
// payload) must collapse to exactly one emission. Tests that the
// dedup check + map update is atomic under contention.
func TestNotifyChangeDedupsConcurrentIdenticalEmissions(t *testing.T) {
	t.Parallel()

	var emits int
	var emitMu sync.Mutex
	cache := NewCachingStoreForTest(NewMemStore(), func(_ string, _ string, _ json.RawMessage) {
		emitMu.Lock()
		emits++
		emitMu.Unlock()
	})

	bead := Bead{ID: "test-1", Title: "Task", Status: "open"}

	const goroutines = 32
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			cache.notifyChange("bead.updated", bead)
		}()
	}
	wg.Wait()

	if emits != 1 {
		t.Fatalf("emits = %d, want 1 (concurrent identical emissions must collapse to one)", emits)
	}
}
