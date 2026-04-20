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
	// best-effort and surfaced without stopping the purge; the returned error
	// also covers list failures.
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
	purged, deleteErr := purgeExpiredBeads(store, entries, cutoff)

	trackEntries, trackErr := store.List(beads.ListQuery{Status: "closed", Label: labelOrderTracking})
	if trackErr == nil {
		trackPurged, trackDeleteErr := purgeExpiredBeads(store, trackEntries, cutoff)
		purged += trackPurged
		deleteErr = errors.Join(deleteErr, trackDeleteErr)
	}

	return purged, deleteErr
}

func purgeExpiredBeads(store beads.Store, entries []beads.Bead, cutoff time.Time) (int, error) {
	purged := 0
	var deleteErr error
	for _, entry := range entries {
		if entry.CreatedAt.IsZero() || !entry.CreatedAt.Before(cutoff) {
			continue
		}
		if err := store.Delete(entry.ID); err != nil {
			deleteErr = errors.Join(deleteErr, fmt.Errorf("deleting expired bead %q: %w", entry.ID, err))
			continue
		}
		purged++
	}
	return purged, deleteErr
}
