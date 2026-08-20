package main

// Crash recovery for the production managed dolt scope watchdog (gc-zl5ta).
//
// The watchdog supervised a lifetime but never restarted anything: when its
// `dolt sql-server` child exited, the watchdog logged the exit and returned,
// and via init() that return became os.Exit. The scope was then left holding
// a dead server until some later, unrelated `gc` invocation happened to
// notice and start a fresh one.
//
// On 2026-08-19 nothing did for nearly seven hours. The root filesystem hit
// ENOSPC, dolt died on a fatal journal-write panic (exit status 2), and the
// city data plane stayed down for 6h58m with zero bead writes. Because mail,
// nudges and beads are all bead-backed, every escalation channel was down in
// exactly the situation that needed one, so the outage was also silent.
//
// This file adds the missing half: a bounded restart with backoff, plus the
// alarm that has to survive when the data plane does not.
//
// Three constraints shape the design.
//
// Only a self-inflicted crash is restarted. `gc dolt stop` signals the dolt
// PID directly and never touches the watchdog (stopManagedDoltProcessWithOptions
// resolves its target from the PID file / port holder, not from us), so a
// supervisor that restarts a signaled child makes the managed server
// unstoppable. classifyManagedDoltChildExit therefore restarts only an exit
// the child chose itself, and every other disposition — clean exit, signal
// death, an unclassifiable wait failure — falls through to the pre-fix
// behavior. That is deliberately fail-closed: a missed restart is the status
// quo, an unwanted one is a new and worse bug.
//
// The restart is bounded. An unbounded loop trades a dead server for a crash
// loop, which is the failure mode gc-x1a87 hit at 83439 restarts against a
// condition no restart could fix. The budget is a rolling window rather than
// a lifetime cap, so a city that crashes once a month never exhausts it while
// a server that cannot start at all still stops after a few tries.
//
// The runtime records are kept truthful across a restart. The rest of the
// managed-dolt lifecycle keys on the recorded PID — assessExistingManagedDolt
// refuses to reuse a server whose live PID does not match dolt-state.json and
// starts a second one instead — so a restart that left the old PID on disk
// would trade a dead server for two live servers over one data dir.
//
// Giving up deliberately does NOT rewrite that record. A record still
// claiming a server at a PID that is gone is not a lie about the present —
// every consumer already gates on pidAlive — it is the durable evidence that
// this scope expected a server and lost one, which is what separates a crash
// from an operator's `gc dolt stop` (that path clears the record itself).
// `gc doctor`'s existing dolt-server check already reports the unreachable
// endpoint over TCP without touching the store, so the gap this file closes
// is not "nothing reports the outage" but "nothing says the supervisor tried
// and stopped trying" — which is what the marker below carries.

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	// managedDoltRestartBudgetEnv overrides how many crash restarts the
	// scope watchdog attempts within managedDoltRestartWindow. Tests use it
	// to reach the give-up path in a few hundred milliseconds; setting it to
	// 0 opts out of crash recovery entirely, restoring the pre-fix behavior
	// for an operator running their own supervisor.
	managedDoltRestartBudgetEnv = "GC_DOLT_SCOPE_WATCHDOG_RESTART_BUDGET"

	// managedDoltRestartDelayEnv overrides the first restart backoff in
	// milliseconds. Tests use it to shrink the backoff from seconds to tens
	// of milliseconds.
	managedDoltRestartDelayEnv = "GC_DOLT_SCOPE_WATCHDOG_RESTART_DELAY_MS"

	// managedDoltRestartDefaultBudget is how many crash restarts are allowed
	// inside one managedDoltRestartWindow. Five covers the transient causes
	// worth retrying — a full disk that a sweep clears, a port that frees, a
	// host hiccup — without letting a permanently broken server churn.
	managedDoltRestartDefaultBudget = 5

	// managedDoltRestartWindow is the rolling window the budget is counted
	// over. Rolling, not lifetime: a healthy city that crashes once a week
	// must never arrive at its next crash with the budget already spent.
	managedDoltRestartWindow = 10 * time.Minute

	// managedDoltRestartDefaultBaseDelay is the first restart's backoff. It
	// is long enough that an instantly-failing server cannot spin, and short
	// enough that the common case — one transient crash — is invisible.
	managedDoltRestartDefaultBaseDelay = 2 * time.Second

	// managedDoltRestartMaxDelay caps the exponential backoff. Past this
	// point a longer wait only lengthens the outage; the budget, not the
	// delay, is what stops a hopeless restart loop.
	managedDoltRestartMaxDelay = 30 * time.Second

	// managedDoltRestartGaveUpMarker is the alarm. It is written to the dolt
	// log file and to the watchdog's stderr, both of which are plain files
	// outside the data plane — the only channels that still work when dolt
	// is what died. It is a pinned string so an operator grep and any future
	// reader can key on it; renaming it is a breaking change.
	managedDoltRestartGaveUpMarker = "GIVING UP on the managed dolt sql-server"
)

// managedDoltChildExit classifies why the watchdog's dolt sql-server child
// stopped running, which decides whether restarting it is the watchdog's
// call to make.
type managedDoltChildExit int

const (
	// managedDoltChildExitClean is exit status 0: an orderly shutdown,
	// including the graceful path a SIGTERM from `gc dolt stop` takes.
	managedDoltChildExitClean managedDoltChildExit = iota

	// managedDoltChildExitCrashed is a non-zero status the child chose
	// itself — the ENOSPC journal panic's exit status 2, for one. Nobody
	// else is driving this lifecycle, so recovery is the watchdog's job.
	managedDoltChildExitCrashed

	// managedDoltChildExitSignaled is death by signal: `gc dolt stop`'s
	// SIGKILL escalation, an operator, or the OOM killer. Some other actor
	// is driving the lifecycle and restarting would fight it.
	managedDoltChildExitSignaled

	// managedDoltChildExitUnknown is a wait failure that carries no exit
	// status at all. Absence of evidence is not evidence of a crash.
	managedDoltChildExitUnknown
)

// String renders the disposition for log lines.
func (e managedDoltChildExit) String() string {
	switch e {
	case managedDoltChildExitClean:
		return "clean"
	case managedDoltChildExitCrashed:
		return "crashed"
	case managedDoltChildExitSignaled:
		return "signaled"
	default:
		return "unknown"
	}
}

// classifyManagedDoltChildExit maps a cmd.Wait() error to the disposition
// that decides whether the watchdog restarts the server. Only
// managedDoltChildExitCrashed restarts; see the file header for why every
// other disposition is deliberately left as the pre-fix behavior.
func classifyManagedDoltChildExit(err error) managedDoltChildExit {
	if err == nil {
		return managedDoltChildExitClean
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		return managedDoltChildExitUnknown
	}
	status, ok := exitErr.Sys().(syscall.WaitStatus)
	if !ok {
		return managedDoltChildExitUnknown
	}
	switch {
	case status.Signaled():
		return managedDoltChildExitSignaled
	case !status.Exited():
		return managedDoltChildExitUnknown
	case status.ExitStatus() == 0:
		return managedDoltChildExitClean
	default:
		return managedDoltChildExitCrashed
	}
}

// managedDoltRestartBudget resolves how many crash restarts are allowed in
// one window. A parseable non-negative value wins, including 0 (opt out);
// anything else falls back to the default, since a typo must not silently
// disable crash recovery.
func managedDoltRestartBudget() int {
	raw := strings.TrimSpace(os.Getenv(managedDoltRestartBudgetEnv))
	if raw == "" {
		return managedDoltRestartDefaultBudget
	}
	budget, err := strconv.Atoi(raw)
	if err != nil || budget < 0 {
		return managedDoltRestartDefaultBudget
	}
	return budget
}

// managedDoltRestartBaseDelay resolves the first restart's backoff, honoring
// the millisecond test override when it parses to a positive value.
func managedDoltRestartBaseDelay() time.Duration {
	raw := strings.TrimSpace(os.Getenv(managedDoltRestartDelayEnv))
	if raw == "" {
		return managedDoltRestartDefaultBaseDelay
	}
	ms, err := strconv.Atoi(raw)
	if err != nil || ms <= 0 {
		return managedDoltRestartDefaultBaseDelay
	}
	return time.Duration(ms) * time.Millisecond
}

// managedDoltRestartDelay returns the backoff before the attempt'th restart
// in the current window: base, doubling each attempt, capped at max.
func managedDoltRestartDelay(attempt int, base, maxDelay time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	delay := base
	for i := 1; i < attempt; i++ {
		if delay >= maxDelay {
			return maxDelay
		}
		delay *= 2
	}
	if delay > maxDelay {
		return maxDelay
	}
	return delay
}

// pruneManagedDoltRestartHistory returns the restart timestamps still inside
// the rolling window, leaving the input untouched.
func pruneManagedDoltRestartHistory(history []time.Time, now time.Time, window time.Duration) []time.Time {
	cutoff := now.Add(-window)
	pruned := make([]time.Time, 0, len(history))
	for _, at := range history {
		if at.After(cutoff) {
			pruned = append(pruned, at)
		}
	}
	return pruned
}

// managedDoltRuntimeRecordSaysStopped reports whether the scope's runtime
// record explicitly says no server should be running.
//
// This is the belt to the exit classifier's braces on the stop path.
// `gc dolt stop` clears the record (clearManagedDoltRuntime) once the server
// is gone, so a record reading "not running" means an operator took this
// scope down deliberately and a restart would be fighting them. The
// classifier already declines to restart the two exits a stop actually
// produces — a graceful SIGTERM shutdown exits 0, and the SIGKILL escalation
// is a signal death — but both of those infer intent from how the child died.
// This reads the intent directly, so a dolt build that exited non-zero on its
// way down a stop still could not be resurrected.
//
// Only an explicit stopped record blocks a restart. A missing or unreadable
// record proves nothing about intent and must not veto crash recovery.
func managedDoltRuntimeRecordSaysStopped(cityPath string) bool {
	if strings.TrimSpace(cityPath) == "" {
		return false
	}
	layout, err := resolveManagedDoltRuntimeLayout(cityPath)
	if err != nil {
		return false
	}
	state, err := readDoltRuntimeStateFile(layout.StateFile)
	if err != nil {
		return false
	}
	return !state.Running
}

// refreshManagedDoltRuntimePIDRecord repoints the managed runtime records at
// a restarted server, so `gc dolt stop`, the existing-server reuse
// assessment, and `gc doctor` all see the process that is actually serving
// the data dir.
//
// The update is conditional on previousPID still owning the record. A record
// that has moved on belongs to a `gc` that started its own server while we
// were restarting; overwriting it would hide a live server behind a PID that
// is about to be reaped. A missing or unreadable record is left alone for the
// same reason — the port and data dir in it belong to the parent that wrote
// it, and this path must never invent them.
//
// An empty cityPath (the test-helper spawn shape) has no runtime layout to
// refresh and is a no-op.
func refreshManagedDoltRuntimePIDRecord(cityPath string, previousPID, newPID int) error {
	if strings.TrimSpace(cityPath) == "" || previousPID <= 0 {
		return nil
	}
	layout, err := resolveManagedDoltRuntimeLayout(cityPath)
	if err != nil {
		return fmt.Errorf("resolve managed dolt runtime layout: %w", err)
	}
	state, err := readDoltRuntimeStateFile(layout.StateFile)
	if err != nil {
		// No record, or one this gc cannot parse. Either way there is
		// nothing here this watchdog owns.
		return nil
	}
	if state.PID != previousPID {
		return nil
	}
	state.Running = true
	state.PID = newPID
	state.StartedAt = time.Now().UTC().Format(time.RFC3339)
	if err := writeDoltRuntimeStateFile(layout.StateFile, state); err != nil {
		return fmt.Errorf("refresh managed dolt runtime state: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(layout.PIDFile), 0o755); err != nil {
		return fmt.Errorf("create managed dolt pid dir: %w", err)
	}
	if err := os.WriteFile(layout.PIDFile, []byte(strconv.Itoa(newPID)+"\n"), 0o644); err != nil {
		return fmt.Errorf("refresh managed dolt pid file: %w", err)
	}
	return nil
}
