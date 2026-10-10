package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/gastownhall/gascity/internal/beads"
)

// TestOrderDispatchTrackingIndexConcurrentGatesAreRaceFree reproduces the
// concurrent-map-writes crash that flaked the `cmd/gc process` shard on
// unrelated PRs (gascity#3256, gascity#3261).
//
// memoryOrderDispatcher.dispatch builds ONE shared orderDispatchTrackingIndex
// (order_dispatch.go: trackingIndex := newOrderDispatchTrackingIndex(m.stderr)) and
// consults it from every order's open-work gate. gateOpenWorkBounded runs each
// gate in its own goroutine and, on a per-order timeout OR ctx cancellation,
// returns WITHOUT waiting for that goroutine (by design, to avoid stalling
// later orders — #2893). The dispatch loop then spawns the next order's gate
// goroutine. Those orphaned goroutines call hasOpenTracking / lastRunFunc
// concurrently, and both write the index's unguarded `entries`/`errs` maps in
// entriesForStore and historyEntriesForStore -> "fatal error: concurrent map
// writes". On ctx cancel the loop orphans every remaining gate at once, so the
// racing writers pile up — in CI a burst of "open-work gate ... aborted:
// context canceled" immediately precedes the crash.
//
// The test hammers a single shared index from many goroutines through the same
// public entry points the dispatch loop uses (hasOpenTracking + the lastRunFunc
// closure), with a small key fan-out so reads and writes collide on the same
// map. Under `-race` the unsynchronized access is reported deterministically;
// making orderDispatchTrackingIndex guard its maps with a mutex fixes it.
func TestOrderDispatchTrackingIndexConcurrentGatesAreRaceFree(t *testing.T) {
	idx := newOrderDispatchTrackingIndex(io.Discard)
	stores := []beads.Store{beads.NewMemStore()}

	const (
		goroutines = 64
		keyFanout  = 8
		iterations = 16
	)

	var wg sync.WaitGroup
	wg.Add(goroutines)
	start := make(chan struct{})
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			// A small fan-out of shared store keys: goroutines sharing a key
			// race their cache writes, and goroutines on other keys read/write
			// the same backing map concurrently.
			storeKeys := []string{fmt.Sprintf("store-%d", g%keyFanout)}
			lastRun := idx.lastRunFunc(stores, storeKeys, nil)
			<-start // release all goroutines together to maximize overlap
			for i := 0; i < iterations; i++ {
				if _, err := idx.hasOpenTracking(stores, storeKeys, "order-x"); err != nil {
					t.Errorf("hasOpenTracking: %v", err)
					return
				}
				if _, err := lastRun("order-x"); err != nil {
					t.Errorf("lastRunFunc: %v", err)
					return
				}
			}
		}(g)
	}
	close(start)
	wg.Wait()
}

// heldIndexReadStore holds each read the tracking index makes until release is
// closed, and counts how many of each kind were started: the order-run scan
// that both open-work gates read, and the order-tracking history behind the
// cooldown clock. A held scan returns scanErr once released, when it is set.
//
// Inside a synctest bubble, synctest.Wait returns only once every caller is
// blocked, so the counts read after it are exactly the reads the concurrent
// callers started while the first one was still in flight.
type heldIndexReadStore struct {
	beads.Store
	release chan struct{}
	scanErr error

	mu           sync.Mutex
	scans        int
	historyReads int
}

func (s *heldIndexReadStore) List(q beads.ListQuery) ([]beads.Bead, error) {
	switch {
	case isOrderGateIndexQuery(q):
		s.count(&s.scans)
		<-s.release
		if s.scanErr != nil {
			return nil, s.scanErr
		}
	case q.Label == labelOrderTracking && q.IncludeClosed:
		s.count(&s.historyReads)
		<-s.release
	}
	return s.Store.List(q)
}

func (s *heldIndexReadStore) count(n *int) {
	s.mu.Lock()
	*n++
	s.mu.Unlock()
}

func (s *heldIndexReadStore) reads() (scans, historyReads int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.scans, s.historyReads
}

// TestOrderDispatchTrackingIndexSharesInFlightReads pins each per-pass index
// read as one read per store, shared by every caller. The dispatch loop
// abandons a gate goroutine at its bound and moves on to the next order's gate,
// so a slow store's read is often still in flight when the next gate on that
// store asks for it. That gate has to wait for the read in flight. A gate that
// started a second scan instead would add one more scan of the already-slow
// store, and one more gate bound, for every order on it.
func TestOrderDispatchTrackingIndexSharesInFlightReads(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mem := beads.NewMemStore()
		tracking, err := mem.Create(beads.Bead{
			Title:  "order:order-x",
			Type:   "task",
			Status: "open",
			Labels: []string{"order-run:order-x", labelOrderTracking},
		})
		if err != nil {
			t.Fatal(err)
		}
		store := &heldIndexReadStore{Store: mem, release: make(chan struct{})}
		stores, keys := []beads.Store{store}, []string{"city"}
		idx := newOrderDispatchTrackingIndex(io.Discard)

		const gates = 8
		type openAnswer struct {
			open bool
			err  error
		}
		type lastRunAnswer struct {
			last time.Time
			err  error
		}
		openAnswers := make(chan openAnswer, gates)
		lastRunAnswers := make(chan lastRunAnswer, gates)
		for i := 0; i < gates; i++ {
			go func() {
				open, err := idx.hasOpenTracking(stores, keys, "order-x")
				openAnswers <- openAnswer{open: open, err: err}
			}()
			go func() {
				last, err := idx.lastRunFunc(stores, keys, nil)("order-x")
				lastRunAnswers <- lastRunAnswer{last: last, err: err}
			}()
		}

		synctest.Wait()
		scans, historyReads := store.reads()
		close(store.release)

		if scans != 1 || historyReads != 1 {
			t.Errorf("%d concurrent callers of each read started %d order-run scans and %d history reads, want 1 of each: a caller that finds a read in flight must wait for it", gates, scans, historyReads)
		}
		for i := 0; i < gates; i++ {
			if a := <-openAnswers; a.err != nil || !a.open {
				t.Errorf("hasOpenTracking = %t, %v; want true from the shared read", a.open, a.err)
			}
			if a := <-lastRunAnswers; a.err != nil || !a.last.Equal(tracking.CreatedAt) {
				t.Errorf("lastRun = %v, %v; want %v from the shared read", a.last, a.err, tracking.CreatedAt)
			}
		}
	})
}

// TestOrderDispatchTrackingIndexSharesFailedRead pins the failure half of the
// shared read. Every gate waiting on a read that fails gets that read's error
// and handles it as its own, and the store's "index unavailable" line is
// written once per store per pass, not once per waiting gate.
func TestOrderDispatchTrackingIndexSharesFailedRead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		scanErr := errors.New("bd list: connection reset")
		store := &heldIndexReadStore{Store: beads.NewMemStore(), release: make(chan struct{}), scanErr: scanErr}
		var stderr bytes.Buffer
		idx := newOrderDispatchTrackingIndex(lockedStderr(&stderr))

		const gates = 8
		errs := make(chan error, gates)
		for i := 0; i < gates; i++ {
			go func() {
				_, err := idx.hasOpenTracking([]beads.Store{store}, []string{"city"}, "order-x")
				errs <- err
			}()
		}

		synctest.Wait()
		scans, _ := store.reads()
		close(store.release)

		if scans != 1 {
			t.Errorf("%d concurrent gates started %d order-run scans, want 1", gates, scans)
		}
		for i := 0; i < gates; i++ {
			if err := <-errs; !errors.Is(err, scanErr) {
				t.Errorf("hasOpenTracking error = %v, want the shared read's %v", err, scanErr)
			}
		}
		if got := strings.Count(stderr.String(), "order-run index for store city unavailable"); got != 1 {
			t.Errorf("index-unavailable line written %d times, want once per store per pass:\n%s", got, stderr.String())
		}
	})
}
