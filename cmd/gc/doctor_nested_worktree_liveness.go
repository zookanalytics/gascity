package main

import (
	"errors"
	"fmt"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/doctor"
)

// doctorNestedWorktreeLiveness adapts the shared worktree-liveness boundary
// to the doctor's nested-worktree prune check, making that check a third
// caller of the same gate the closed-bead reaper and reconciler capacity
// accounting already read (bead_worktree_liveness.go) rather than a fourth
// mechanism with its own notion of "in use".
//
// Both of the boundary's signals are required, and either being unavailable
// is reported as an error so the check rejects every candidate:
//
//   - The process-table scan is authoritative: it observes which directory a
//     process actually occupies right now.
//   - The recorded session working directories back-stop a process the scan
//     cannot see. Session metadata is stamped at create/dispatch and is not
//     continuously refreshed, so it is a cross-check on the scan, never a
//     substitute for it.
//
// storeOK carries the doctor's bead-store preflight verdict. False means the
// preflight found the store unreachable, and no open is attempted: re-probing
// it would auto-start orphan Dolt servers for no gain. True does not promise
// the open will succeed — the preflight only runs when city.toml parsed, so a
// city with a broken config reaches the open below and fails there instead.
// Either way the outcome is an error, and an error prunes nothing.
func doctorNestedWorktreeLiveness(cityPath string, storeOK bool, newStore func(string) (beads.Store, error)) doctor.LiveWorktreeProbe {
	return func() (func(string) (bool, string), error) {
		live := collectLiveWorktreeStateFn()
		if !live.scanned {
			return nil, errors.New("process table could not be enumerated: no /proc and no portable fallback succeeded")
		}
		sessionDirs, err := doctorLiveSessionWorktreeDirs(cityPath, storeOK, newStore)
		if err != nil {
			return nil, err
		}
		return func(path string) (bool, string) {
			return worktreeIsLive(path, live, sessionDirs)
		}, nil
	}
}

// doctorLiveSessionWorktreeDirs returns the recorded working directories of
// every open session in the city store. An error means the set could not be
// read at all; callers treat that as indeterminate liveness rather than as an
// empty set, since an empty set reads identically to "no sessions running".
func doctorLiveSessionWorktreeDirs(cityPath string, storeOK bool, newStore func(string) (beads.Store, error)) ([]string, error) {
	if !storeOK {
		return nil, errors.New("session working directories unavailable: the city bead store failed preflight")
	}
	if newStore == nil {
		return nil, errors.New("session working directories unavailable: no bead store factory")
	}
	store, err := newStore(cityPath)
	if err != nil {
		return nil, fmt.Errorf("opening city bead store for session liveness: %w", err)
	}
	snapshot, err := loadSessionBeadSnapshot(store)
	if err != nil {
		return nil, fmt.Errorf("listing sessions for liveness: %w", err)
	}
	return liveSessionWorktreeDirs(snapshot), nil
}
