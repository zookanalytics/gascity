package main

// Scope-death watchdog for production managed dolt sql-servers (ga-gz19s4).
//
// gc spawns one managed `dolt sql-server` per scope (city, worktree, PR
// clone), but nothing owned the server once its scope was deleted: the
// server orphaned to ppid 1 and ran forever. On one dev box 314 servers
// accumulated this way — 230 with deleted working directories — holding
// ~44GB RSS and a sustained ~4.5 cores in aggregate.
//
// The watchdog closes the ownership gap at the source instead of relying on
// after-the-fact reaping: production servers are spawned under a supervisor
// process (a gc re-exec, modeled on the managed-dolt TEST watchdog in
// dolt_start_managed.go) that terminates the server when its scope is
// provably gone. "Provably gone" means the server's --config file no longer
// exists on disk for two consecutive checks — the second check is the grace
// window that protects transient states (crash-adoption moves, mid-flight
// renames) from being misread as deletion. The check queries live
// filesystem state every cycle; there are no status files.
//
// Memory cost: a gc re-exec initializes the full cmd/gc dependency graph
// and holds it for the life of the scope — measured at ~97MB RSS per
// watchdog, of which ~20MB is private dirty (the rest is binary text shared
// across all gc processes). The marginal cost is therefore ~20MB of
// unshareable memory per live scope. This is a deliberate trade against the
// multi-GB orphan leak above; it scales only with live scopes because each
// watchdog dies with its server.

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	// managedDoltScopeWatchdogArg is the argv[1] re-exec marker for the
	// production scope watchdog. No production `gc` invocation collides with
	// it; reaching init() with it set is proof of an intentional re-exec.
	managedDoltScopeWatchdogArg = "__gc-managed-dolt-scope-watchdog"

	// managedDoltScopeWatchdogEnv disables the production scope watchdog
	// when set to "0" (the managed server is then spawned directly, exactly
	// the pre-watchdog behavior).
	managedDoltScopeWatchdogEnv = "GC_DOLT_SCOPE_WATCHDOG"

	// managedDoltScopeWatchdogIntervalEnv overrides the scope poll interval
	// in milliseconds. Tests use it to shrink the deleted-scope reaction
	// time from tens of seconds to tens of milliseconds.
	managedDoltScopeWatchdogIntervalEnv = "GC_DOLT_SCOPE_WATCHDOG_INTERVAL_MS"

	// managedDoltScopeWatchdogDefaultInterval is the production poll
	// cadence. Scope deletion is rare and the response does not need to be
	// fast — it needs to be reliable. A slow cadence keeps the watchdog's
	// steady-state CPU cost negligible; the per-watchdog memory footprint
	// is the re-exec cost documented in the file header.
	managedDoltScopeWatchdogDefaultInterval = 30 * time.Second

	// managedDoltScopeGoneConfirmations is how many consecutive polls must
	// observe the scope missing before the server is terminated. Two checks
	// one full interval apart distinguish "scope permanently deleted" from
	// "scope momentarily absent" (crash-adoption window, transient rename).
	managedDoltScopeGoneConfirmations = 2
)

func init() {
	if len(os.Args) < 2 || os.Args[1] != managedDoltScopeWatchdogArg {
		return
	}
	os.Exit(runManagedDoltScopeWatchdog(os.Args[2:], os.Stdout, os.Stderr))
}

// managedDoltScopeWatchdogEnabled reports whether production managed dolt
// servers are spawned under the scope watchdog. Default on; opt out with
// GC_DOLT_SCOPE_WATCHDOG=0. Always off in managed-dolt test mode: test
// scopes are owned by the test watchdog (or deliberately watchdog-free),
// and interposing a second supervisor would change test process topology.
func managedDoltScopeWatchdogEnabled() bool {
	return managedDoltScopeWatchdogEnabledFor(managedDoltTestModeEnabled(), os.Getenv(managedDoltScopeWatchdogEnv))
}

// managedDoltScopeWatchdogEnabledFor is the pure decision behind
// managedDoltScopeWatchdogEnabled, split out for tests (the test binary is
// always in test mode, so the production default is unreachable in-process).
func managedDoltScopeWatchdogEnabledFor(testMode bool, envValue string) bool {
	if testMode {
		return false
	}
	return strings.TrimSpace(envValue) != "0"
}

// managedDoltScopeWatchdogInterval resolves the poll interval, honoring the
// millisecond test override when it parses to a positive value.
func managedDoltScopeWatchdogInterval() time.Duration {
	raw := strings.TrimSpace(os.Getenv(managedDoltScopeWatchdogIntervalEnv))
	if raw == "" {
		return managedDoltScopeWatchdogDefaultInterval
	}
	ms, err := strconv.Atoi(raw)
	if err != nil || ms <= 0 {
		return managedDoltScopeWatchdogDefaultInterval
	}
	return time.Duration(ms) * time.Millisecond
}

// managedDoltScopeGone reports whether the scope anchoring a managed dolt
// server has been deleted, using the server's --config file as the anchor:
// the config lives inside the scope's runtime directory, so the scope
// disappearing takes the config with it. Only a definitive not-exist counts;
// stat errors (permissions, I/O) lean toward "alive" so a degraded
// filesystem never kills a healthy server.
func managedDoltScopeGone(configFile string) bool {
	if strings.TrimSpace(configFile) == "" {
		return false
	}
	_, err := os.Stat(configFile)
	return errors.Is(err, fs.ErrNotExist)
}

// startManagedDoltSQLServerWithScopeWatchdog spawns the managed dolt
// sql-server under the production scope watchdog. The watchdog process
// (a gc re-exec) starts the server itself and reports the server PID on
// stdout using the same one-line protocol as the test watchdog, so callers
// observe the same managedDoltStartedProcess shape as a direct spawn plus
// the supervising WatchdogPID.
func startManagedDoltSQLServerWithScopeWatchdog(cityPath, configFile, logFilePath string, logFile *os.File) (managedDoltStartedProcess, error) {
	watchdogExecutable, err := managedDoltWatchdogExecutable()
	if err != nil {
		return managedDoltStartedProcess{}, err
	}
	cmd := exec.Command(watchdogExecutable, managedDoltScopeWatchdogArg, configFile, logFilePath, cityPath)
	cmd.Stderr = logFile
	cmd.Stdin = nil
	cmd.SysProcAttr = managedDoltSQLServerSysProcAttr()
	cmd.Env = doltServerEnv(cityPath, os.Environ())
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return managedDoltStartedProcess{}, fmt.Errorf("prepare dolt scope watchdog: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return managedDoltStartedProcess{}, fmt.Errorf("start dolt scope watchdog: %w", err)
	}
	pid, startTimeTicks, startIdentity, err := readManagedDoltScopeWatchdogStart(stdout, cmd.Process.Pid)
	if err != nil {
		_ = terminateManagedDoltPID(cityPath, cmd.Process.Pid)
		_ = cmd.Wait()
		return managedDoltStartedProcess{}, err
	}
	watchdogPID := cmd.Process.Pid
	// Snapshot the watchdog's own OS start identity while it is definitely alive
	// (it has just handed off the dolt PID and entered its supervise loop), before
	// the reaper goroutine below can Wait() it and free the PID. Startup-failure
	// cleanup then re-verifies the watchdog PID the same way it does the dolt
	// child's, so a reused watchdog PID is never signaled.
	watchdogTicks, watchdogIdentity := snapshotManagedDoltStartIdentity(watchdogPID)
	go func() { _ = cmd.Wait() }()
	return managedDoltStartedProcess{
		CityPath:               cityPath,
		PID:                    pid,
		WatchdogPID:            watchdogPID,
		StartTimeTicks:         startTimeTicks,
		StartIdentity:          startIdentity,
		WatchdogStartTimeTicks: watchdogTicks,
		WatchdogStartIdentity:  watchdogIdentity,
	}, nil
}

// managedDoltScopeWatchdogChild is one supervised `dolt sql-server`
// generation: the process, the OS start identity captured before anything
// could reap it, and the channel its wait result arrives on. Crash recovery
// replaces the whole value on restart, so the PID-reuse guard on the
// terminate paths always describes the child that is currently running.
type managedDoltScopeWatchdogChild struct {
	cmd      *exec.Cmd
	pid      int
	ticks    uint64
	identity string
	done     chan error
}

// startManagedDoltScopeWatchdogChild spawns one dolt sql-server generation
// under the watchdog and begins reaping it.
//
// Setpgid: the dolt sql-server leads its own process group, matching the
// direct production spawn (managedDoltSQLServerSysProcAttr) and the test
// watchdog's layout, and keeping the server's descendants out of the
// watchdog's own group. Termination here is leader-only: the guarded
// terminate below signals just this PID — group-kill (kill(-pgid, ...))
// exists only in terminateManagedDoltTestPID, the test-registry reaper.
// Leader-only is accepted on this path because the managed config disables
// auto_gc/stats helper workers (see cmd_dolt_config.go), so descendant
// helpers are rare by construction, and a SIGTERM'd dolt winds down its own
// children; only the SIGKILL escalation of an unresponsive server could
// strand descendants.
//
// The start identity is snapshotted BEFORE the reap goroutine can Wait() the
// child and free its numeric PID. Snapshotting here — while the caller still
// holds the un-reaped child — is race-free and mirrors the direct-spawn
// snapshot in startManagedDoltSQLServer, so neither the parent's
// startup-failure cleanup guard (terminateManagedDoltStartedProcess) nor this
// watchdog's own scope-gone and signal-forward termination
// (terminateManagedDoltScopeWatchdogChild) can ever signal an unrelated
// process that reused the PID. snapshotManagedDoltStartIdentity reads the ps
// fallback lazily, so the parent handshake — which the parent reads under a
// timeout — never blocks on a ps fork when /proc ticks are available.
func startManagedDoltScopeWatchdogChild(configFile, cityPath string, logFile *os.File) (*managedDoltScopeWatchdogChild, error) {
	cmd := exec.Command("dolt", "sql-server", "--config", configFile)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.Stdin = nil
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Env = doltServerEnv(cityPath, os.Environ())
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	pid := cmd.Process.Pid
	ticks, identity := snapshotManagedDoltStartIdentity(pid)
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	return &managedDoltScopeWatchdogChild{cmd: cmd, pid: pid, ticks: ticks, identity: identity, done: done}, nil
}

// runManagedDoltScopeWatchdog is the re-exec'd watchdog process body. It
// spawns the dolt sql-server as its own process-group leader, prints the
// server PID on stdout, then supervises: it terminates the server when the
// scope is gone for managedDoltScopeGoneConfirmations consecutive polls,
// forwards SIGTERM/SIGINT, restarts the server on a bounded budget when it
// crashes on its own, and exits when the server stops for a reason that is
// not the watchdog's to recover from.
//
// Crash recovery — which exits are restarted, why the loop is bounded, and
// how the runtime records are kept truthful across a restart — is documented
// in dolt_scope_watchdog_restart.go.
func runManagedDoltScopeWatchdog(args []string, stdout, stderr *os.File) int {
	if len(args) != 3 {
		fmt.Fprintf(stderr, "usage: %s <config-file> <log-file> <city-path>\n", managedDoltScopeWatchdogArg) //nolint:errcheck
		return 2
	}
	configFile := args[0]
	logFilePath := args[1]
	cityPath := args[2]
	if strings.TrimSpace(configFile) == "" {
		fmt.Fprintln(stderr, "empty config file path") //nolint:errcheck
		return 2
	}
	logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		fmt.Fprintf(stderr, "open dolt log: %v\n", err) //nolint:errcheck
		return 1
	}
	defer logFile.Close() //nolint:errcheck

	child, err := startManagedDoltScopeWatchdogChild(configFile, cityPath, logFile)
	if err != nil {
		fmt.Fprintf(stderr, "start dolt sql-server: %v\n", err) //nolint:errcheck
		return 1
	}
	// Report the dolt child's PID and OS start identity to the parent. Both
	// values were captured before the reap goroutine could free the PID, so
	// the parent's cleanup guard always has a usable identity.
	fmt.Fprintln(stdout, formatManagedDoltWatchdogStartLine(child.pid, child.ticks, child.identity)) //nolint:errcheck

	interval := managedDoltScopeWatchdogInterval()
	budget := managedDoltRestartBudget()
	baseDelay := managedDoltRestartBaseDelay()
	fmt.Fprintf(logFile, "gc scope watchdog: supervising dolt sql-server pid %d (config %s, poll interval %s, restart budget %d per %s)\n", //nolint:errcheck
		child.pid, configFile, interval, budget, managedDoltRestartWindow)

	signals := make(chan os.Signal, 2)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signals)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// restartTimer is non-nil only while a crashed server is waiting out its
	// backoff. Holding the wait in the select — rather than sleeping — keeps
	// signal forwarding and scope-gone detection responsive throughout.
	var restartTimer *time.Timer
	defer func() {
		if restartTimer != nil {
			restartTimer.Stop()
		}
	}()

	goneStreak := 0
	var restarts []time.Time
	// lastPID survives child being cleared during a backoff: it is the PID
	// the runtime record still names, and the one the alarm reports.
	lastPID := child.pid
	for {
		// A nil channel blocks forever in select, which is exactly the
		// behavior wanted for the arms that are not currently armed: no
		// child during a backoff, no backoff while a child is running.
		var childDone <-chan error
		if child != nil {
			childDone = child.done
		}
		var restartDue <-chan time.Time
		if restartTimer != nil {
			restartDue = restartTimer.C
		}

		select {
		case sig := <-signals:
			if child == nil {
				fmt.Fprintf(logFile, "gc scope watchdog: received %v while waiting to restart dolt sql-server; exiting without restarting\n", sig) //nolint:errcheck
				return 0
			}
			fmt.Fprintf(logFile, "gc scope watchdog: received %v; terminating dolt sql-server pid %d\n", sig, child.pid) //nolint:errcheck
			_ = terminateManagedDoltScopeWatchdogChild(cityPath, child.pid, child.ticks, child.identity)
			<-child.done
			return 0

		case <-ticker.C:
			if !managedDoltScopeGone(configFile) {
				goneStreak = 0
				continue
			}
			goneStreak++
			if goneStreak < managedDoltScopeGoneConfirmations {
				continue
			}
			if child == nil {
				fmt.Fprintf(logFile, "gc scope watchdog: config %s gone for %d consecutive checks; abandoning the pending dolt sql-server restart\n", //nolint:errcheck
					configFile, goneStreak)
				return 0
			}
			fmt.Fprintf(logFile, "gc scope watchdog: config %s gone for %d consecutive checks; terminating dolt sql-server pid %d\n", //nolint:errcheck
				configFile, goneStreak, child.pid)
			_ = terminateManagedDoltScopeWatchdogChild(cityPath, child.pid, child.ticks, child.identity)
			<-child.done
			return 0

		case <-restartDue:
			restartTimer = nil
			// The scope can disappear during a backoff. Restarting into a
			// deleted scope would recreate the orphan this watchdog exists
			// to prevent.
			if managedDoltScopeGone(configFile) {
				fmt.Fprintf(logFile, "gc scope watchdog: config %s disappeared during the restart backoff; not restarting dolt sql-server\n", configFile) //nolint:errcheck
				return 0
			}
			// An operator can also take the scope down during a backoff,
			// leaving the scope itself intact. Their intent is recorded.
			if managedDoltRuntimeRecordSaysStopped(cityPath) {
				fmt.Fprintf(logFile, "gc scope watchdog: the managed dolt runtime record says this scope is stopped; not restarting dolt sql-server\n") //nolint:errcheck
				return 0
			}
			next, startErr := startManagedDoltScopeWatchdogChild(configFile, cityPath, logFile)
			if startErr != nil {
				fmt.Fprintf(logFile, "gc scope watchdog: restart %d of %d could not spawn dolt sql-server: %v\n", len(restarts), budget, startErr) //nolint:errcheck
				if timer, ok := scheduleManagedDoltRestart(logFile, &restarts, budget, baseDelay); ok {
					restartTimer = timer
					continue
				}
				return managedDoltScopeWatchdogGiveUp(logFile, stderr, lastPID, len(restarts), budget)
			}
			previousPID := lastPID
			child = next
			lastPID = child.pid
			fmt.Fprintf(logFile, "gc scope watchdog: restarted dolt sql-server as pid %d (restart %d of %d per %s)\n", //nolint:errcheck
				child.pid, len(restarts), budget, managedDoltRestartWindow)
			if err := refreshManagedDoltRuntimePIDRecord(cityPath, previousPID, child.pid); err != nil {
				// The server is up; a stale record degrades discovery, it
				// does not invalidate the recovery. Say so and keep going.
				fmt.Fprintf(logFile, "gc scope watchdog: could not repoint the managed dolt runtime records at pid %d: %v\n", child.pid, err) //nolint:errcheck
			}

		case waitErr := <-childDone:
			exitedPID := child.pid
			child = nil
			switch classifyManagedDoltChildExit(waitErr) {
			case managedDoltChildExitClean:
				fmt.Fprintf(logFile, "gc scope watchdog: dolt sql-server pid %d exited cleanly\n", exitedPID) //nolint:errcheck
				return 0
			case managedDoltChildExitSignaled:
				// `gc dolt stop` signals the server directly and never
				// signals us, so restarting here would make the managed
				// server unstoppable.
				fmt.Fprintf(logFile, "gc scope watchdog: dolt sql-server pid %d was terminated by a signal (%v); an external actor owns this shutdown, not restarting\n", //nolint:errcheck
					exitedPID, waitErr)
				return 0
			case managedDoltChildExitUnknown:
				fmt.Fprintf(logFile, "gc scope watchdog: dolt sql-server pid %d exit could not be classified (%v); not restarting\n", exitedPID, waitErr) //nolint:errcheck
				return 1
			}

			fmt.Fprintf(logFile, "gc scope watchdog: dolt sql-server pid %d exited with error: %v\n", exitedPID, waitErr) //nolint:errcheck
			if managedDoltScopeGone(configFile) {
				fmt.Fprintf(logFile, "gc scope watchdog: config %s is gone; not restarting dolt sql-server\n", configFile) //nolint:errcheck
				return 1
			}
			if timer, ok := scheduleManagedDoltRestart(logFile, &restarts, budget, baseDelay); ok {
				restartTimer = timer
				continue
			}
			return managedDoltScopeWatchdogGiveUp(logFile, stderr, exitedPID, len(restarts), budget)
		}
	}
}

// scheduleManagedDoltRestart charges one restart against the rolling budget
// and returns the armed backoff timer. It reports false when the budget for
// the current window is spent, which is the caller's cue to give up.
func scheduleManagedDoltRestart(logFile *os.File, restarts *[]time.Time, budget int, baseDelay time.Duration) (*time.Timer, bool) {
	now := time.Now()
	*restarts = pruneManagedDoltRestartHistory(*restarts, now, managedDoltRestartWindow)
	if len(*restarts) >= budget {
		return nil, false
	}
	*restarts = append(*restarts, now)
	delay := managedDoltRestartDelay(len(*restarts), baseDelay, managedDoltRestartMaxDelay)
	fmt.Fprintf(logFile, "gc scope watchdog: restarting dolt sql-server in %s (restart %d of %d per %s)\n", //nolint:errcheck
		delay, len(*restarts), budget, managedDoltRestartWindow)
	return time.NewTimer(delay), true
}

// managedDoltScopeWatchdogGiveUp ends supervision when crash recovery cannot
// make progress, and raises the alarm.
//
// The alarm has to work when the data plane does not: mail, nudges and beads
// are all bead-backed, so on 2026-08-19 every escalation channel was down in
// exactly the situation that needed one. Both channels used here are plain
// files — the dolt log and the watchdog's stderr, which the spawning gc points
// at the same log. The runtime record is deliberately left claiming the server
// that died; see dolt_scope_watchdog_restart.go for why that is the signal the
// managed-dolt-server doctor check needs.
func managedDoltScopeWatchdogGiveUp(logFile, stderr *os.File, pid, restarts, budget int) int {
	message := fmt.Sprintf("gc scope watchdog: %s — it crashed through its whole restart budget (%d of %d allowed per %s, last pid %d). This scope has no data plane until an operator intervenes; the crash cause is in the log above.",
		managedDoltRestartGaveUpMarker, restarts, budget, managedDoltRestartWindow, pid)
	fmt.Fprintln(logFile, message) //nolint:errcheck
	fmt.Fprintln(stderr, message)  //nolint:errcheck
	return 1
}

// terminateManagedDoltScopeWatchdogChild terminates the watchdog's own dolt
// sql-server child by PID, guarded against PID reuse with the child's OS start
// identity that the watchdog snapshotted at startup (startTicks/startIdentity,
// captured before the reap goroutine could Wait() the child and free its
// numeric PID). This is the production scope-gone/signal reap path — the reason
// the watchdog exists — so it carries the same reuse hazard the parent-side
// cleanup does: the reaper goroutine frees the PID when a server that outlived
// the SIGTERM grace finally exits, and terminateManagedDoltPIDGuarded re-checks
// identity before both SIGTERM and SIGKILL so neither signal lands on an
// unrelated process that reused the number. A zero identity (no /proc ticks and
// no ps fallback) composes to the legacy unconditional terminate.
func terminateManagedDoltScopeWatchdogChild(cityPath string, pid int, startTicks uint64, startIdentity string) error {
	return terminateManagedDoltPIDGuarded(cityPath, pid, func() bool {
		return managedDoltPIDStartIdentityMatches(pid, startTicks, startIdentity)
	})
}
