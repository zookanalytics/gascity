package beads

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// wedgeableStore wraps a Store and can simulate a backing connection that has
// been invalidated by a Dolt online GC/compaction: while "wedged" every
// full-scan List fails with the "invalid connection" signature, and the only
// way to clear the wedge is an explicit Reconnect — exactly like the Err1105
// self-invalidation that the production reconcile path must recover from.
type wedgeableStore struct {
	Store

	mu                       sync.Mutex
	wedged                   bool
	reconnectErr             error
	reconnectCalls           int
	listFailures             int
	setMetadataFailures      int
	setMetadataBatchFailures int
}

func newWedgeableStore(backing Store) *wedgeableStore {
	return &wedgeableStore{Store: backing}
}

// wedge invalidates the connection: subsequent full-scan List calls fail until
// Reconnect runs.
func (s *wedgeableStore) wedge() {
	s.mu.Lock()
	s.wedged = true
	s.mu.Unlock()
}

func (s *wedgeableStore) List(query ListQuery) ([]Bead, error) {
	if query.AllowScan {
		s.mu.Lock()
		wedged := s.wedged
		if wedged {
			s.listFailures++
		}
		s.mu.Unlock()
		if wedged {
			// Mirror the real reconcile read failure observed in production:
			// "begin read tx: invalid connection".
			return nil, errors.New("begin read tx: invalid connection")
		}
	}
	return s.Store.List(query)
}

// SetMetadata / SetMetadataBatch fail with the connection-invalidation
// signature while wedged, modeling the session-lifecycle metadata commit
// hitting the same dead handle that wedges the read path. Reconnect clears
// the wedge, so the write guard's retry-once succeeds.
func (s *wedgeableStore) SetMetadata(id, key, value string) error {
	s.mu.Lock()
	wedged := s.wedged
	if wedged {
		s.setMetadataFailures++
	}
	s.mu.Unlock()
	if wedged {
		// Mirror the real write failure observed in production:
		// "dolt commit: invalid connection".
		return errors.New("dolt commit: invalid connection")
	}
	return s.Store.SetMetadata(id, key, value)
}

func (s *wedgeableStore) SetMetadataBatch(id string, kvs map[string]string) error {
	s.mu.Lock()
	wedged := s.wedged
	if wedged {
		s.setMetadataBatchFailures++
	}
	s.mu.Unlock()
	if wedged {
		// Mirror the real batch failure observed in production:
		// "failed to begin transaction: invalid connection".
		return errors.New("failed to begin transaction: invalid connection")
	}
	return s.Store.SetMetadataBatch(id, kvs)
}

// Reconnect clears the wedge unless reconnectErr is configured, modeling a
// backing store whose connection can be re-established in place.
func (s *wedgeableStore) Reconnect(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reconnectCalls++
	if s.reconnectErr != nil {
		return s.reconnectErr
	}
	s.wedged = false
	return nil
}

func (s *wedgeableStore) counters() (reconnects, listFailures int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reconnectCalls, s.listFailures
}

func (s *wedgeableStore) writeCounters() (setMetadata, setMetadataBatch int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.setMetadataFailures, s.setMetadataBatchFailures
}

func cacheHasBeadID(items []Bead, id string) bool {
	for _, b := range items {
		if b.ID == id {
			return true
		}
	}
	return false
}

// TestReconcileReconnectsOnInvalidConnectionAndConverges is the gc-6njbf
// regression: a Dolt online GC invalidates the supervisor's long-lived
// reconcile connection, and a manual session bead created out-of-process during
// that window must still reach the cache (and therefore the session model). The
// reconcile read must reconnect the backing store and retry rather than reuse
// the dead connection forever and wedge in cacheDegraded.
func TestReconcileReconnectsOnInvalidConnectionAndConverges(t *testing.T) {
	mem := NewMemStore()
	if _, err := mem.Create(Bead{Title: "primed before wedge"}); err != nil {
		t.Fatalf("seed Create: %v", err)
	}
	wedge := newWedgeableStore(mem)
	cs := NewCachingStoreForTest(wedge, nil)
	if err := cs.Prime(context.Background()); err != nil {
		t.Fatalf("Prime: %v", err)
	}
	if !cs.IsLive() {
		t.Fatalf("cache not live after prime: state=%s", cs.Stats().State)
	}

	// A manual `gc session new` lands a bead in the backing out-of-process,
	// right as the reconcile connection is invalidated by an online GC.
	created, err := mem.Create(Bead{Title: "manual session bead"})
	if err != nil {
		t.Fatalf("out-of-process Create: %v", err)
	}
	wedge.wedge()

	cs.runReconciliation()

	reconnects, listFailures := wedge.counters()
	if reconnects != 1 {
		t.Fatalf("Reconnect calls = %d, want 1", reconnects)
	}
	if listFailures != 1 {
		t.Fatalf("List failures = %d, want 1 (one invalid-connection failure drove the reconnect)", listFailures)
	}
	if !cs.IsLive() {
		t.Fatalf("cache state = %s after reconnect, want live", cs.Stats().State)
	}
	if sf := cs.Stats().SyncFailures; sf != 0 {
		t.Fatalf("SyncFailures = %d after reconnect, want 0", sf)
	}
	cached, ok := cs.CachedList(ListQuery{AllowScan: true})
	if !ok {
		t.Fatalf("CachedList not authoritative after reconnect-driven reconcile")
	}
	if !cacheHasBeadID(cached, created.ID) {
		t.Fatalf("session bead %s missing from cache after reconnect: %+v", created.ID, cached)
	}
}

// TestReconcileDegradesWhenReconnectFails proves the reconnect attempt does not
// mask an unrecoverable backing: when Reconnect keeps failing (e.g. Dolt is
// still mid-compaction), the cache still degrades after maxCacheSyncFailures,
// preserving the existing backpressure behavior. It also confirms a reconnect
// is attempted on every failing cycle.
func TestReconcileDegradesWhenReconnectFails(t *testing.T) {
	mem := NewMemStore()
	if _, err := mem.Create(Bead{Title: "primed"}); err != nil {
		t.Fatalf("seed Create: %v", err)
	}
	wedge := newWedgeableStore(mem)
	wedge.reconnectErr = errors.New("dolt still compacting")
	cs := NewCachingStoreForTest(wedge, nil)
	if err := cs.Prime(context.Background()); err != nil {
		t.Fatalf("Prime: %v", err)
	}
	wedge.wedge()

	for i := 0; i < maxCacheSyncFailures; i++ {
		cs.runReconciliation()
	}

	if cs.IsLive() {
		t.Fatalf("cache still live after %d unrecoverable failures", maxCacheSyncFailures)
	}
	if got := cs.Stats().State; got != "degraded" {
		t.Fatalf("cache state = %s, want degraded", got)
	}
	reconnects, listFailures := wedge.counters()
	if reconnects != maxCacheSyncFailures {
		t.Fatalf("Reconnect attempts = %d, want %d (one per failing cycle)", reconnects, maxCacheSyncFailures)
	}
	if listFailures != maxCacheSyncFailures {
		t.Fatalf("List failures = %d, want %d (one per failing cycle)", listFailures, maxCacheSyncFailures)
	}
}

// TestNativeDoltStoreReconnectErrorsWithoutHandle locks in the reconnect
// contract for the production backing: it returns a clear error (never panics
// or silently succeeds) when there is no live handle to reopen. The
// compile-time `_ reconnectableStore = (*NativeDoltStore)(nil)` assertion in
// native_dolt_store.go guarantees the reconciler's type-assertion reaches it.
func TestNativeDoltStoreReconnectErrorsWithoutHandle(t *testing.T) {
	var _ reconnectableStore = (*NativeDoltStore)(nil)

	store := newNativeDoltStoreForTest(nil)
	if err := store.Reconnect(context.Background()); err == nil {
		t.Fatal("Reconnect on a store with no live handle: want error, got nil")
	}
}

// TestSetMetadataReconnectsOnInvalidConnection is the gc-mzdaq regression for
// the WRITE path — the sibling of gc-6njbf's read-path fix. A Dolt online GC
// invalidates the supervisor's long-lived connection mid-flight, and the
// session-lifecycle metadata commit (SetMetadata, e.g. "storing session key")
// must reconnect the backing store and retry rather than fail the session
// start. Before the fix the write returned "invalid connection" unretried,
// wedging every new session start until a supervisor restart.
func TestSetMetadataReconnectsOnInvalidConnection(t *testing.T) {
	mem := NewMemStore()
	seed, err := mem.Create(Bead{Title: "session bead"})
	if err != nil {
		t.Fatalf("seed Create: %v", err)
	}
	wedge := newWedgeableStore(mem)
	cs := NewCachingStoreForTest(wedge, nil)
	if err := cs.Prime(context.Background()); err != nil {
		t.Fatalf("Prime: %v", err)
	}

	wedge.wedge()
	if err := cs.SetMetadata(seed.ID, "session_key", "abc123"); err != nil {
		t.Fatalf("SetMetadata after wedge: want nil (reconnect+retry), got %v", err)
	}

	reconnects, _ := wedge.counters()
	if reconnects != 1 {
		t.Fatalf("Reconnect calls = %d, want 1", reconnects)
	}
	setMeta, _ := wedge.writeCounters()
	if setMeta != 1 {
		t.Fatalf("SetMetadata failures = %d, want 1 (one invalid-connection failure drove the reconnect)", setMeta)
	}
	got, err := mem.Get(seed.ID)
	if err != nil {
		t.Fatalf("Get after write: %v", err)
	}
	if got.Metadata["session_key"] != "abc123" {
		t.Fatalf("metadata not durably written after reconnect: %+v", got.Metadata)
	}
}

// TestSetMetadataBatchReconnectsOnInvalidConnection is the gc-mzdaq regression
// for the other reported emitter: the start-commit / pre-wake path stores
// hashes via SetMetadataBatch. It must recover from the same invalidation.
func TestSetMetadataBatchReconnectsOnInvalidConnection(t *testing.T) {
	mem := NewMemStore()
	seed, err := mem.Create(Bead{Title: "session bead"})
	if err != nil {
		t.Fatalf("seed Create: %v", err)
	}
	wedge := newWedgeableStore(mem)
	cs := NewCachingStoreForTest(wedge, nil)
	if err := cs.Prime(context.Background()); err != nil {
		t.Fatalf("Prime: %v", err)
	}

	wedge.wedge()
	kvs := map[string]string{"core_hash": "deadbeef", "session_hash": "cafe"}
	if err := cs.SetMetadataBatch(seed.ID, kvs); err != nil {
		t.Fatalf("SetMetadataBatch after wedge: want nil (reconnect+retry), got %v", err)
	}

	reconnects, _ := wedge.counters()
	if reconnects != 1 {
		t.Fatalf("Reconnect calls = %d, want 1", reconnects)
	}
	_, setMetaBatch := wedge.writeCounters()
	if setMetaBatch != 1 {
		t.Fatalf("SetMetadataBatch failures = %d, want 1 (one invalid-connection failure drove the reconnect)", setMetaBatch)
	}
	got, err := mem.Get(seed.ID)
	if err != nil {
		t.Fatalf("Get after write: %v", err)
	}
	for k, v := range kvs {
		if got.Metadata[k] != v {
			t.Fatalf("metadata[%q] = %q after reconnect, want %q (batch not durably written): %+v", k, got.Metadata[k], v, got.Metadata)
		}
	}
}

// TestSetMetadataDoesNotRetryWhenReconnectFails proves the write guard does not
// mask an unrecoverable backing: when Reconnect keeps failing (Dolt still
// mid-compaction), SetMetadata returns the original connection error after a
// single backing attempt, without retrying against a still-dead handle. This
// mirrors TestReconcileDegradesWhenReconnectFails for the write path.
func TestSetMetadataDoesNotRetryWhenReconnectFails(t *testing.T) {
	mem := NewMemStore()
	seed, err := mem.Create(Bead{Title: "session bead"})
	if err != nil {
		t.Fatalf("seed Create: %v", err)
	}
	wedge := newWedgeableStore(mem)
	wedge.reconnectErr = errors.New("dolt still compacting")
	cs := NewCachingStoreForTest(wedge, nil)
	if err := cs.Prime(context.Background()); err != nil {
		t.Fatalf("Prime: %v", err)
	}

	wedge.wedge()
	if err := cs.SetMetadata(seed.ID, "session_key", "abc123"); err == nil {
		t.Fatal("SetMetadata: want error when reconnect fails, got nil")
	}

	reconnects, _ := wedge.counters()
	if reconnects != 1 {
		t.Fatalf("Reconnect attempts = %d, want 1", reconnects)
	}
	setMeta, _ := wedge.writeCounters()
	if setMeta != 1 {
		t.Fatalf("SetMetadata attempts = %d, want 1 (no retry against a still-dead handle)", setMeta)
	}
}
