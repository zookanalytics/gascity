package beads

import (
	"encoding/json"
	"sync"
	"testing"
)

// notificationRecorder captures cache onChange callbacks for assertions.
type notificationRecorder struct {
	mu     sync.Mutex
	events []recordedNotification
}

type recordedNotification struct {
	eventType string
	beadID    string
}

func (r *notificationRecorder) onChange(eventType, beadID string, _ json.RawMessage) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, recordedNotification{eventType: eventType, beadID: beadID})
}

func (r *notificationRecorder) typesFor(beadID string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for _, e := range r.events {
		if e.beadID == beadID {
			out = append(out, e.eventType)
		}
	}
	return out
}

func cacheHasBead(t *testing.T, cs *CachingStore, id string) bool {
	t.Helper()
	got, err := cs.List(ListQuery{Status: "open", TierMode: TierBoth})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	for _, b := range got {
		if b.ID == id {
			return true
		}
	}
	return false
}

// TestRefreshIDLandsBeadMissingFromStaleCache is the regression test for the
// post-boot session-start hang: a bead created in the backing store after the
// cache primed is invisible to the (live, clean) cache, and RefreshID must
// pull it in so a reader sees it without waiting for a reconcile tick or a
// bd-hook event.
func TestRefreshIDLandsBeadMissingFromStaleCache(t *testing.T) {
	backing := NewMemStore()
	if _, err := backing.Create(Bead{Title: "seed", Type: "task"}); err != nil {
		t.Fatalf("seed create: %v", err)
	}
	rec := &notificationRecorder{}
	cs := NewCachingStoreForTest(backing, rec.onChange)
	if err := cs.PrimeActive(); err != nil {
		t.Fatalf("PrimeActive: %v", err)
	}

	// Simulate a cross-process create: the bead lands in the backing store
	// directly, so the in-memory cache has no knowledge of it.
	created, err := backing.Create(Bead{Title: "new session", Type: "task"})
	if err != nil {
		t.Fatalf("backing create: %v", err)
	}

	// Precondition: the live, clean cache serves its stale snapshot and omits
	// the new bead. If this ever stops holding, the test no longer exercises
	// the staleness it is meant to guard.
	if cacheHasBead(t, cs, created.ID) {
		t.Fatalf("precondition failed: stale cache already contains %s", created.ID)
	}

	if err := cs.RefreshID(created.ID); err != nil {
		t.Fatalf("RefreshID: %v", err)
	}

	if !cacheHasBead(t, cs, created.ID) {
		t.Fatalf("RefreshID did not land %s into the cache", created.ID)
	}
	got, err := cs.Get(created.ID)
	if err != nil {
		t.Fatalf("Get after RefreshID: %v", err)
	}
	if got.Title != "new session" {
		t.Fatalf("Get returned wrong bead: title=%q", got.Title)
	}
	if types := rec.typesFor(created.ID); len(types) != 1 || types[0] != "bead.created" {
		t.Fatalf("expected one bead.created notification, got %v", types)
	}
}

// TestRefreshIDLandsBeadWhenCacheDegraded proves RefreshID does not depend on
// the cache being live: even in a degraded cache — the state in which
// ApplyEvent drops events outright — RefreshID still lands the bead.
func TestRefreshIDLandsBeadWhenCacheDegraded(t *testing.T) {
	backing := NewMemStore()
	rec := &notificationRecorder{}
	cs := NewCachingStoreForTest(backing, rec.onChange)
	if err := cs.PrimeActive(); err != nil {
		t.Fatalf("PrimeActive: %v", err)
	}
	cs.mu.Lock()
	cs.state = cacheDegraded
	cs.mu.Unlock()

	created, err := backing.Create(Bead{Title: "degraded session", Type: "task"})
	if err != nil {
		t.Fatalf("backing create: %v", err)
	}

	// ApplyEvent must drop the create while degraded (documents the gap
	// RefreshID closes).
	payload, _ := json.Marshal(created)
	cs.ApplyEvent("bead.created", payload)
	cs.mu.RLock()
	_, landedByEvent := cs.beads[created.ID]
	cs.mu.RUnlock()
	if landedByEvent {
		t.Fatalf("ApplyEvent unexpectedly landed %s while degraded", created.ID)
	}

	if err := cs.RefreshID(created.ID); err != nil {
		t.Fatalf("RefreshID: %v", err)
	}
	cs.mu.RLock()
	_, landed := cs.beads[created.ID]
	cs.mu.RUnlock()
	if !landed {
		t.Fatalf("RefreshID did not land %s while degraded", created.ID)
	}
}

func TestRefreshIDIgnoresUnownedAndMissingIDs(t *testing.T) {
	backing := NewMemStore()
	rec := &notificationRecorder{}
	// Prefixed cache: only "lx-" beads are owned.
	cs := NewCachingStoreForTestWithPrefix(backing, "lx", rec.onChange)
	if err := cs.PrimeActive(); err != nil {
		t.Fatalf("PrimeActive: %v", err)
	}

	// MemStore mints gc-* IDs, which this lx-prefixed cache does not own.
	created, err := backing.Create(Bead{Title: "foreign", Type: "task"})
	if err != nil {
		t.Fatalf("backing create: %v", err)
	}
	if err := cs.RefreshID(created.ID); err != nil {
		t.Fatalf("RefreshID(unowned): %v", err)
	}
	cs.mu.RLock()
	_, landed := cs.beads[created.ID]
	cs.mu.RUnlock()
	if landed {
		t.Fatalf("RefreshID landed unowned bead %s", created.ID)
	}

	// Missing and blank IDs are no-ops that return nil.
	if err := cs.RefreshID("lx-does-not-exist"); err != nil {
		t.Fatalf("RefreshID(missing): %v", err)
	}
	if err := cs.RefreshID("   "); err != nil {
		t.Fatalf("RefreshID(blank): %v", err)
	}
	if got := rec.typesFor(created.ID); len(got) != 0 {
		t.Fatalf("expected no notifications, got %v", got)
	}
}
