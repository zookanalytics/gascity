package beads

import (
	"errors"
	"strings"
)

// RefreshID pulls a single bead from the backing store into the in-memory
// cache, independent of the cache's freshness state.
//
// Unlike ApplyEvent — which drops events while the cache is uninitialized or
// degraded, and which reconciles a partial field patch against the cached
// row — RefreshID reads the authoritative full row straight from the backing
// store and lands it unconditionally. It exists for the targeted-refresh
// path: when a cross-process writer creates a bead and then pokes the
// controller, the controller calls RefreshID so the new bead is visible to
// readers even when the periodic reconcile loop and the bd-hook event path
// have not — or cannot — land it (a stalled reconcile over a degraded
// backing, or an event dropped because the cache is not yet live).
//
// Behavior by backing Get outcome:
//   - found: inserts or replaces the cached row and its dependencies, marks it
//     a recent local mutation so an in-flight reconcile cannot clobber it with
//     a pre-create snapshot, and emits a bead.created (new) or bead.updated
//     (existing) notification.
//   - ErrNotFound: no-op (the bead is absent from the backing; pruning a
//     removed bead is the reconcile loop's responsibility, not RefreshID's).
//   - other error: recorded and returned without mutating the cache.
//
// A blank ID, or an ID this cache's prefix does not own, is a no-op returning
// nil. RefreshID never changes the cache state: it asserts one bead, not
// whole-cache freshness.
func (c *CachingStore) RefreshID(id string) error {
	id = strings.TrimSpace(id)
	if id == "" || !c.ownsBeadID(id) {
		return nil
	}

	bead, err := c.backing.Get(id)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		c.recordProblem("refresh bead by id", err)
		return err
	}

	depMap, _, depErr := c.fetchDepsForBeads(map[string]Bead{id: bead})
	if depErr != nil {
		// Land the bead with whatever deps the fetch returned; a transient dep
		// fetch failure must not block making the bead itself visible. The next
		// reconcile reconverges dependencies.
		c.recordProblem("refresh bead deps by id", depErr)
	}

	c.mu.Lock()
	_, existed := c.beads[id]
	c.beads[id] = cloneBead(bead)
	c.deps[id] = cloneDeps(depMap[id])
	c.noteLocalMutationLocked(id)
	delete(c.dirty, id)
	delete(c.deletedSeq, id)
	c.updateStatsLocked()
	c.mu.Unlock()

	eventType := "bead.updated"
	if !existed {
		eventType = "bead.created"
	}
	c.notifyChanges([]cacheNotification{{eventType: eventType, bead: cloneBead(bead)}})
	return nil
}
