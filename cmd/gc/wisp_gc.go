package main

import (
	"errors"
	"fmt"
	"time"

	"github.com/gastownhall/gascity/internal/beads"
)

// wispGC performs mechanical garbage collection of closed molecules that
// have exceeded their TTL. Follows the nil-guard tracker pattern used by
// crashTracker and idleTracker: nil means disabled.
type wispGC interface {
	// shouldRun returns true if enough time has elapsed since the last run.
	shouldRun(now time.Time) bool

	// runGC lists closed molecules, deletes those older than TTL, and returns
	// the count of purged entries. Errors from individual deletes are
	// best-effort (logged but not fatal); the returned error is for list
	// failures.
	runGC(store beads.Store, now time.Time) (int, error)
}

// memoryWispGC is the production implementation of wispGC.
type memoryWispGC struct {
	interval time.Duration
	ttl      time.Duration
	lastRun  time.Time
}

// newWispGC creates a wisp GC tracker. Returns nil if disabled (interval or
// TTL is zero). Callers nil-guard before use.
func newWispGC(interval, ttl time.Duration) wispGC {
	if interval <= 0 || ttl <= 0 {
		return nil
	}
	return &memoryWispGC{
		interval: interval,
		ttl:      ttl,
	}
}

func (m *memoryWispGC) shouldRun(now time.Time) bool {
	return now.Sub(m.lastRun) >= m.interval
}

func (m *memoryWispGC) runGC(store beads.Store, now time.Time) (int, error) {
	m.lastRun = now
	if store == nil {
		return 0, fmt.Errorf("listing closed molecules: bead store unavailable")
	}

	entries, err := store.List(beads.ListQuery{Status: "closed", Type: "molecule"})
	if err != nil {
		return 0, fmt.Errorf("listing closed molecules: %w", err)
	}

	cutoff := now.Add(-m.ttl)
	purged := purgeExpiredBeads(store, entries, cutoff)

	trackEntries, trackErr := store.List(beads.ListQuery{Status: "closed", Label: labelOrderTracking})
	if trackErr == nil {
		purged += purgeExpiredBeads(store, trackEntries, cutoff)
	}

	return purged, nil
}

func purgeExpiredBeads(store beads.Store, entries []beads.Bead, cutoff time.Time) int {
	purged := 0
	for _, entry := range entries {
		if entry.CreatedAt.IsZero() || !entry.CreatedAt.Before(cutoff) {
			continue
		}
		if err := deleteExpiredBeadClosure(store, entry.ID); err != nil {
			continue
		}
		purged++
	}
	return purged
}

func deleteExpiredBeadClosure(store beads.Store, rootID string) error {
	ids, err := collectExpiredBeadClosure(store, rootID)
	if err != nil {
		return err
	}
	_, errs := deleteWorkflowBeads(store, ids)
	if len(errs) == 0 {
		return nil
	}
	joined := make([]error, 0, len(errs))
	for _, err := range errs {
		if err != nil {
			joined = append(joined, err)
		}
	}
	if len(joined) == 0 {
		return nil
	}
	return errors.Join(joined...)
}

func collectExpiredBeadClosure(store beads.Store, rootID string) ([]string, error) {
	if store == nil {
		return nil, fmt.Errorf("bead store unavailable")
	}
	queue := []string{rootID}
	if related, err := store.List(beads.ListQuery{
		Metadata:      map[string]string{"gc.root_bead_id": rootID},
		IncludeClosed: true,
	}); err == nil {
		for _, bead := range related {
			queue = append(queue, bead.ID)
		}
	}

	seen := make(map[string]struct{}, len(queue))
	ids := make([]string, 0, len(queue))
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)

		upDeps, err := store.DepList(id, "up")
		if err != nil {
			return nil, fmt.Errorf("list dependents for %s: %w", id, err)
		}
		for _, dep := range upDeps {
			if dep.IssueID != "" {
				queue = append(queue, dep.IssueID)
			}
		}

		children, err := store.Children(id, beads.IncludeClosed)
		if err != nil {
			return nil, fmt.Errorf("list children for %s: %w", id, err)
		}
		for _, child := range children {
			if child.ID != "" {
				queue = append(queue, child.ID)
			}
		}
	}
	return ids, nil
}
