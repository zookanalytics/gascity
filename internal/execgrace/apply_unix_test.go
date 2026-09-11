//go:build !windows

package execgrace

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"
)

// TestApplyTrapRunsBeforeKill is the regression test for the staged-content
// data-loss class: a setup script that has moved files aside and registered a
// rollback trap must get to run that trap when its deadline expires. With
// Go's default context-cancel (SIGKILL) the trap can never run; with Apply the
// group interrupt reaches the shell and the trap restores state before the
// grace escalation.
func TestApplyTrapRunsBeforeKill(t *testing.T) {
	t.Parallel()
	marker := filepath.Join(t.TempDir(), "restored")

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	// The trap models worktree-setup.sh's restore_stage: it must observe the
	// interrupt and write the marker (i.e. "move the staged files back").
	script := `trap 'echo restored > "$MARKER"; exit 130' INT TERM; sleep 30`
	cmd := exec.CommandContext(ctx, "sh", "-c", script)
	cmd.Env = append(os.Environ(), "MARKER="+marker)
	result := Apply(cmd, 5*time.Second)

	if err := cmd.Run(); err == nil {
		t.Fatal("expected the canceled command to report an error")
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("rollback trap never ran — staged state would have been lost: %v", err)
	}
	if outcome := result.Outcome(); outcome != CancelGroupSignaled {
		t.Fatalf("expected CancelGroupSignaled, got %v", outcome)
	}
}

// TestApplyReinterruptsForegroundChildThatSurvivesFirstInterrupt proves Apply
// rescues a foreground child that survives the first group interrupt. A single
// interrupt can miss a child that was mid-fork/exec when it was delivered (its
// signal disposition not yet settled), leaving the shell blocked in wait with
// its rollback trap deferred until WaitDelay force-kills it — and the trap
// never runs. Apply must re-interrupt the surviving child until it exits, so
// the trap runs inside the grace window.
//
// The miss is made deterministic here: the foreground child survives the first
// interrupt and exits only on the second, so a run that sends a single
// interrupt hangs until WaitDelay force-kills the shell and the marker is never
// written. The re-interrupt enumerates the process group via /proc, so this
// exercises the Linux path.
func TestApplyReinterruptsForegroundChildThatSurvivesFirstInterrupt(t *testing.T) {
	t.Parallel()
	if runtime.GOOS != "linux" {
		t.Skip("the foreground-child re-interrupt enumerates the process group via /proc; Linux-only")
	}
	marker := filepath.Join(t.TempDir(), "restored")

	// Cancel at 150ms; grace (2s) bounds how long WaitDelay waits before
	// SIGKILL, so a run that never re-interrupts fails within ~2s rather than
	// hanging for the full sleep.
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	// The foreground child counts interrupts and exits only on the second, so
	// the first is absorbed and only a re-interrupt frees the outer shell to run
	// its rollback trap. Passing the child body through the environment keeps the
	// counting trap's $n unexpanded until trap time without nested-quote escaping.
	child := `n=0; trap 'n=$((n + 1)); [ "$n" -ge 2 ] && exit 130' INT; while :; do sleep 0.1; done`
	parent := `trap 'printf "%s\n" restored > "$MARKER"; exit 130' INT
sh -c "$CHILD"`
	cmd := exec.CommandContext(ctx, "sh", "-c", parent)
	cmd.Env = append(os.Environ(), "MARKER="+marker, "CHILD="+child)
	Apply(cmd, 2*time.Second)

	if err := cmd.Run(); err == nil {
		t.Fatal("expected the canceled command to report an error")
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("rollback trap never ran — the foreground child that survived the first interrupt was never re-interrupted: %v", err)
	}
}

// TestApplyReinterruptDoesNotDisruptTrapTeardown proves the re-interrupt ladder
// leaves a running rollback trap alone. The ladder re-signals only the
// foreground children captured before the interrupt, so a teardown process the
// trap spawns afterward — modeled here by a child that sleeps before writing
// its completion marker — must run to completion, not be cut short by a stray
// interrupt. A ladder that re-signaled the whole group would kill it mid-work.
func TestApplyReinterruptDoesNotDisruptTrapTeardown(t *testing.T) {
	t.Parallel()
	if runtime.GOOS != "linux" {
		t.Skip("the foreground-child re-interrupt enumerates the process group via /proc; Linux-only")
	}
	dir := t.TempDir()
	restored := filepath.Join(dir, "restored")
	tornDown := filepath.Join(dir, "torn_down")

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// The trap runs a teardown command that takes 300ms before recording its
	// completion, then writes its own marker. grace (3s) leaves ample room for
	// both to finish before WaitDelay would force-kill.
	script := `trap 'sh -c "sleep 0.3; echo done > \"$TORNDOWN\""; echo restored > "$RESTORED"; exit 130' INT
sleep 30`
	cmd := exec.CommandContext(ctx, "sh", "-c", script)
	cmd.Env = append(os.Environ(), "RESTORED="+restored, "TORNDOWN="+tornDown)
	Apply(cmd, 3*time.Second)

	if err := cmd.Run(); err == nil {
		t.Fatal("expected the canceled command to report an error")
	}
	if _, err := os.Stat(restored); err != nil {
		t.Fatalf("rollback trap never ran: %v", err)
	}
	if _, err := os.Stat(tornDown); err != nil {
		t.Fatalf("teardown spawned by the trap was cut short — the ladder must not re-signal it: %v", err)
	}
}

// TestApplyReinterruptLeavesTrapTeardownAloneWhenNoForegroundChild proves the
// re-interrupt ladder is inert when the shell leader has no foreground child at
// cancellation. The pre-interrupt snapshot is then empty, and an empty snapshot
// cannot tell a child that was mid-exec (which should be re-interrupted) from no
// child at all — in the latter case the shell runs its INT trap immediately and
// the only process to appear afterward is one the trap spawned. Re-signaling
// that teardown process would cut rollback short, so an empty snapshot must
// produce no re-interrupt.
//
// The shell blocks in the read builtin on a pipe whose write end the test holds
// open, so it has no foreground child (empty snapshot) yet still runs its trap
// on the interrupt. The trap spawns a teardown command that sleeps before
// recording completion; its marker must be present.
func TestApplyReinterruptLeavesTrapTeardownAloneWhenNoForegroundChild(t *testing.T) {
	t.Parallel()
	if runtime.GOOS != "linux" {
		t.Skip("the foreground-child re-interrupt enumerates the process group via /proc; Linux-only")
	}
	dir := t.TempDir()
	restored := filepath.Join(dir, "restored")
	tornDown := filepath.Join(dir, "torn_down")

	// A pipe whose write end stays open keeps the shell blocked in read with no
	// foreground child, so the pre-interrupt snapshot is empty. os/exec hands an
	// *os.File stdin straight to the child, and os.Pipe's write end is O_CLOEXEC
	// so the child never inherits it — read sees neither data nor EOF until the
	// deferred close, well after the interrupt.
	pr, pw, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	defer func() { _ = pr.Close() }()
	defer func() { _ = pw.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// The trap runs a teardown command that takes 300ms before recording its
	// completion, then writes its own marker. grace (3s) leaves ample room for
	// both to finish before WaitDelay would force-kill.
	script := `trap 'sh -c "sleep 0.3; echo done > \"$TORNDOWN\""; echo restored > "$RESTORED"; exit 130' INT
read _ignored`
	cmd := exec.CommandContext(ctx, "sh", "-c", script)
	cmd.Stdin = pr
	cmd.Env = append(os.Environ(), "RESTORED="+restored, "TORNDOWN="+tornDown)
	Apply(cmd, 3*time.Second)

	if err := cmd.Run(); err == nil {
		t.Fatal("expected the canceled command to report an error")
	}
	if _, err := os.Stat(restored); err != nil {
		t.Fatalf("rollback trap never ran: %v", err)
	}
	if _, err := os.Stat(tornDown); err != nil {
		t.Fatalf("teardown spawned by the trap was cut short — an empty snapshot must not re-signal a process that appears after the interrupt: %v", err)
	}
}

// TestApplyForceKillsUncooperative proves the grace escalation: a command that
// ignores the interrupt must still die within WaitDelay rather than hanging
// the caller forever.
func TestApplyForceKillsUncooperative(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	cmd := exec.CommandContext(ctx, "sh", "-c", `trap '' INT TERM; sleep 30`)
	result := Apply(cmd, 1*time.Second)

	start := time.Now()
	err := cmd.Run()
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected the canceled command to report an error")
	}
	// Deadline (200ms) + grace (1s) + slack. Well under sleep 30.
	if elapsed > 10*time.Second {
		t.Fatalf("uncooperative command outlived the grace escalation: %v", elapsed)
	}
	// The group signal is still delivered successfully here — the ignoring
	// process just doesn't act on it. That makes this CancelGroupSignaled,
	// not CancelForceKilled: os/exec's own WaitDelay escalation (SIGKILL) is
	// what actually ends the process, running independently of — and after —
	// our cmd.Cancel closure, which already reported delivery. CancelForceKilled
	// is reserved for when interruptProcessGroup itself fails to deliver and
	// *our* fallback cmd.Process.Kill() is what fires (see
	// TestApplyForceKilledWhenGroupSignalFails).
	if outcome := result.Outcome(); outcome != CancelGroupSignaled {
		t.Fatalf("expected CancelGroupSignaled (signal delivered; os/exec's own WaitDelay kill finishes the job), got %v", outcome)
	}
}

// TestApplyAcceptedFlag proves the delivered-cancellation flag contract that
// internal/runtime/exec's cancellation-wins error mapping depends on.
func TestApplyAcceptedFlag(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	cmd := exec.CommandContext(ctx, "sh", "-c", `sleep 30`)
	result := Apply(cmd, 2*time.Second)
	if err := cmd.Run(); err == nil {
		t.Fatal("expected the canceled command to report an error")
	}
	if !result.Delivered() {
		t.Fatal("accepted flag must record the delivered cancellation")
	}

	// A command that finishes on its own must not set the flag. (Cancel
	// requires a context-created command even when the context never fires.)
	cmd2 := exec.CommandContext(context.Background(), "sh", "-c", "true")
	result2 := Apply(cmd2, 2*time.Second)
	if err := cmd2.Run(); err != nil {
		t.Fatalf("healthy command failed: %v", err)
	}
	if result2.Delivered() {
		t.Fatal("accepted flag must stay false when the command completes normally")
	}
}

// TestApplyLeaderSignaledOnlyWhenGetpgidFails proves the leader-only signal
// fallback: when the process group cannot be resolved, Apply must still
// interrupt the process leader directly, and record that no group signal
// was ever attempted.
//
// Not parallel: overrides the package-level getpgid seam.
func TestApplyLeaderSignaledOnlyWhenGetpgidFails(t *testing.T) {
	origGetpgid := getpgid
	getpgid = func(_ int) (int, error) {
		return 0, syscall.EINVAL
	}
	defer func() { getpgid = origGetpgid }()

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	cmd := exec.CommandContext(ctx, "sleep", "30")
	result := Apply(cmd, 2*time.Second)

	if err := cmd.Run(); err == nil {
		t.Fatal("expected the canceled command to report an error")
	}
	if !result.Delivered() {
		t.Fatal("accepted flag must record the delivered cancellation")
	}
	if outcome := result.Outcome(); outcome != CancelLeaderSignaledOnly {
		t.Fatalf("expected CancelLeaderSignaledOnly, got %v", outcome)
	}
}

// TestApplyForceKilledWhenGroupSignalFails proves the last-resort fallback:
// when the process-group signal itself fails outright (not just "already
// gone"), Apply must fall back to killing the leader directly rather than
// leaving the command to run out the clock on WaitDelay.
//
// Not parallel: overrides the package-level killProcessGroup seam.
func TestApplyForceKilledWhenGroupSignalFails(t *testing.T) {
	origKill := killProcessGroup
	killProcessGroup = func(_ int, _ syscall.Signal) error {
		return syscall.EPERM
	}
	defer func() { killProcessGroup = origKill }()

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	cmd := exec.CommandContext(ctx, "sleep", "30")
	result := Apply(cmd, 2*time.Second)

	if err := cmd.Run(); err == nil {
		t.Fatal("expected the canceled command to report an error")
	}
	if !result.Delivered() {
		t.Fatal("accepted flag must record the delivered cancellation")
	}
	if outcome := result.Outcome(); outcome != CancelForceKilled {
		t.Fatalf("expected CancelForceKilled, got %v", outcome)
	}

	status, ok := cmd.ProcessState.Sys().(syscall.WaitStatus)
	if !ok {
		t.Fatalf("expected a syscall.WaitStatus, got %T", cmd.ProcessState.Sys())
	}
	if !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("expected the process to be killed by SIGKILL, got signaled=%v signal=%v", status.Signaled(), status.Signal())
	}
}
