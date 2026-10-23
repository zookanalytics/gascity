package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/supervisor"
)

// requireServiceManagerForTest writes a supervisor.toml under the test's
// GC_HOME that sets [supervisor] require_service_manager.
func requireServiceManagerForTest(t *testing.T) {
	t.Helper()
	path := supervisor.ConfigPath()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("[supervisor]\nrequire_service_manager = true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
}

// stubSupervisorFork counts the `gc supervisor run` children a start forks
// and fails each fork, so a start that should not fork never runs a
// supervisor.
func stubSupervisorFork(t *testing.T) *int {
	t.Helper()
	forks := 0
	old := startSupervisorChild
	t.Cleanup(func() { startSupervisorChild = old })
	startSupervisorChild = func(*exec.Cmd) error {
		forks++
		return errors.New("test stub: refused to fork a supervisor")
	}
	return &forks
}

// stubSupervisorInstall makes the service install in ensureSupervisorRunning
// fail with installErr on stderr, as a failed launchctl or systemctl call
// does, without touching the host's service manager. It counts the installs.
func stubSupervisorInstall(t *testing.T, installErr string) *int {
	t.Helper()
	installs := 0
	old := supervisorInstallHook
	t.Cleanup(func() { supervisorInstallHook = old })
	supervisorInstallHook = func(_, stderr io.Writer) int {
		installs++
		fmt.Fprintln(stderr, installErr) //nolint:errcheck // test stderr
		return 1
	}
	return &installs
}

// supervisorStartOnFakeLaunchd sets up a start on a Mac with an isolated
// GC_HOME and a fake launchd whose plist runs this gc, and counts forks.
func supervisorStartOnFakeLaunchd(t *testing.T) (*fakeLaunchd, string, *int) {
	t.Helper()
	clearGCEnv(t)
	pinRealHome(t) // start refuses a HOME override before it reaches launchd
	t.Setenv("GC_HOME", shortTempDir(t, "gc-home-"))
	t.Setenv("XDG_RUNTIME_DIR", shortTempDir(t, "gc-run-"))
	t.Setenv(supervisorSystemdUnitEnv, "")
	fake, plist := installFakeLaunchd(t)
	return fake, plist, stubSupervisorFork(t)
}

// failLaunchctlEnable makes launchctl refuse to enable the job, as it does
// when the user has no desktop login session for launchd to start it in.
func failLaunchctlEnable(fake *fakeLaunchd, err error) {
	supervisorLaunchctlRun = func(args ...string) error {
		if args[0] == "enable" {
			fake.calls = append(fake.calls, strings.Join(args, " "))
			return err
		}
		return fake.run(args...)
	}
}

func assertStderrContains(t *testing.T, stderr string, wants ...string) {
	t.Helper()
	for _, want := range wants {
		if !strings.Contains(stderr, want) {
			t.Errorf("stderr missing %q:\n%s", want, stderr)
		}
	}
}

// When launchd cannot start the installed job, `gc supervisor start` falls
// back to forking a supervisor that inherits this shell's environment. With
// require_service_manager set it exits non-zero instead, naming launchctl's
// error, the desktop session launchd needs, and, when the job is or may be
// disabled, how to enable it.
func TestSupervisorStartRequireServiceManagerRefusesForkWhenLaunchctlFails(t *testing.T) {
	cases := []struct {
		name          string
		disabled      bool
		disabledKnown bool
		wantDisabled  string // the sentence about a disabled job; "" for none
	}{
		{name: "enabled job", disabledKnown: true},
		{name: "disabled job", disabled: true, disabledKnown: true, wantDisabled: "The service is disabled, so that login does not start it by itself"},
		{name: "launchd cannot say", wantDisabled: "If 'gc supervisor stop' disabled the service, that login does not start it by itself"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake, _, forks := supervisorStartOnFakeLaunchd(t)
			requireServiceManagerForTest(t)
			launchctlErr := errors.New("Bootstrap failed: 125: Domain does not support specified action")
			failLaunchctlEnable(fake, launchctlErr)
			supervisorLaunchdDisabled = func(string) (bool, bool) { return tc.disabled, tc.disabledKnown }

			var stdout, stderr bytes.Buffer
			if code := doSupervisorStart(&stdout, &stderr); code != 1 {
				t.Fatalf("doSupervisorStart = %d, want 1; stderr=%s", code, stderr.String())
			}
			if *forks != 0 {
				t.Fatalf("forked %d supervisor(s) outside launchd, want none; stderr=%s", *forks, stderr.String())
			}
			if stdout.Len() != 0 {
				t.Errorf("stdout = %q, want empty", stdout.String())
			}
			assertStderrContains(t, stderr.String(),
				launchctlErr.Error(),
				"require_service_manager in "+supervisor.ConfigPath(),
				"forbids starting the supervisor outside launchd",
				"desktop login session ("+supervisorLaunchdDomain()+")",
				"log in to it and rerun the command in a terminal there")
			enable := "launchctl enable " + supervisorLaunchdServiceTarget(supervisorLaunchdLabel())
			if tc.wantDisabled != "" {
				assertStderrContains(t, stderr.String(), tc.wantDisabled, enable)
			} else if strings.Contains(stderr.String(), enable) {
				t.Errorf("stderr names %q for a job launchd reports enabled:\n%s", enable, stderr.String())
			}
			if strings.Contains(stderr.String(), "outside launchd instead") {
				t.Errorf("stderr still announces a fork:\n%s", stderr.String())
			}
		})
	}
}

// A plist that runs another gc installation is not this gc's service, and
// `gc supervisor start` would otherwise fork this gc beside it.
func TestSupervisorStartRequireServiceManagerRefusesForeignPlist(t *testing.T) {
	fake, plist, forks := supervisorStartOnFakeLaunchd(t)
	requireServiceManagerForTest(t)
	other := fakeGCBinary(t)
	if err := os.WriteFile(plist, []byte(launchdPlistRunning(other)), 0o600); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	if code := doSupervisorStart(&stdout, &stderr); code != 1 {
		t.Fatalf("doSupervisorStart = %d, want 1; stderr=%s", code, stderr.String())
	}
	if *forks != 0 || len(fake.calls) != 0 {
		t.Fatalf("forks=%d launchctl calls=%v, want neither for another gc's plist", *forks, fake.calls)
	}
	assertStderrContains(t, stderr.String(),
		"runs "+other,
		"require_service_manager in "+supervisor.ConfigPath(),
		"supervisor install --force")
}

// With no launchd service installed there is nothing for launchd to start,
// and the fork is still refused.
func TestSupervisorStartRequireServiceManagerRefusesWithoutPlist(t *testing.T) {
	fake, plist, forks := supervisorStartOnFakeLaunchd(t)
	requireServiceManagerForTest(t)
	if err := os.Remove(plist); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	if code := doSupervisorStart(&stdout, &stderr); code != 1 {
		t.Fatalf("doSupervisorStart = %d, want 1; stderr=%s", code, stderr.String())
	}
	if *forks != 0 || len(fake.calls) != 0 {
		t.Fatalf("forks=%d launchctl calls=%v, want neither without a plist", *forks, fake.calls)
	}
	assertStderrContains(t, stderr.String(),
		"no launchd service is installed at "+plist,
		"require_service_manager in "+supervisor.ConfigPath(),
		"gc supervisor install")
}

// On Linux `gc supervisor start` never asks systemd to start the unit; it
// forks. With require_service_manager set it refuses and names the commands
// that start the supervisor under systemd.
func TestSupervisorStartRequireServiceManagerRefusesForkOnLinux(t *testing.T) {
	clearGCEnv(t)
	pinRealHome(t)
	t.Setenv("GC_HOME", shortTempDir(t, "gc-home-"))
	t.Setenv("XDG_RUNTIME_DIR", shortTempDir(t, "gc-run-"))
	t.Setenv(supervisorSystemdUnitEnv, "")
	oldGOOS := supervisorRuntimeGOOS
	t.Cleanup(func() { supervisorRuntimeGOOS = oldGOOS })
	supervisorRuntimeGOOS = "linux"
	requireServiceManagerForTest(t)
	forks := stubSupervisorFork(t)

	var stdout, stderr bytes.Buffer
	if code := doSupervisorStart(&stdout, &stderr); code != 1 {
		t.Fatalf("doSupervisorStart = %d, want 1; stderr=%s", code, stderr.String())
	}
	if *forks != 0 {
		t.Fatalf("forked %d supervisor(s) outside systemd, want none", *forks)
	}
	unit := supervisorSystemdServiceName()
	assertStderrContains(t, stderr.String(),
		"require_service_manager in "+supervisor.ConfigPath(),
		"forbids starting the supervisor outside systemd",
		"'gc start'",
		"'systemctl --user start "+unit+"'")
}

// gc start reaches the supervisor through ensureSupervisorRunning, which
// falls back to a fork when installing or starting the service fails. With
// require_service_manager set it exits non-zero instead, pointing at the
// install error, and does not ask the service manager a second time.
func TestEnsureSupervisorRunningRequireServiceManagerRefusesForkWhenInstallFails(t *testing.T) {
	cases := []struct {
		goos       string
		installErr string
		want       []string
	}{
		{
			goos:       "darwin",
			installErr: "gc supervisor install: launchctl enable gui/501/com.gascity.supervisor: exit status 112",
			want: []string{
				"the launchd service could not be installed or started (the error above)",
				"forbids starting the supervisor outside launchd",
				"log in to it and rerun the command in a terminal there",
			},
		},
		{
			goos:       "linux",
			installErr: "gc supervisor install: per-user systemd instance is not available",
			want: []string{
				"gc's systemd unit could not be installed or started (the error above)",
				"forbids starting the supervisor outside systemd",
				"Fix that error and rerun the command",
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.goos, func(t *testing.T) {
			fake, _, forks := supervisorStartOnFakeLaunchd(t)
			supervisorRuntimeGOOS = tc.goos
			requireServiceManagerForTest(t)
			installs := stubSupervisorInstall(t, tc.installErr)

			var stdout, stderr bytes.Buffer
			if code := ensureSupervisorRunning(&stdout, &stderr); code != 1 {
				t.Fatalf("ensureSupervisorRunning = %d, want 1; stderr=%s", code, stderr.String())
			}
			if *installs != 1 {
				t.Fatalf("installs = %d, want the one that failed", *installs)
			}
			if *forks != 0 {
				t.Fatalf("forked %d supervisor(s) after the install failed, want none; stderr=%s", *forks, stderr.String())
			}
			if len(fake.calls) != 0 {
				t.Errorf("launchctl calls = %v, want none after the failed install", fake.calls)
			}
			assertStderrContains(t, stderr.String(), append([]string{tc.installErr, "require_service_manager in " + supervisor.ConfigPath()}, tc.want...)...)
		})
	}
}

// Without require_service_manager, the shape of the 2026-10-21 outage still
// forks: the install fails, launchd cannot start the installed job either,
// and ensureSupervisorRunning starts the supervisor outside launchd.
func TestEnsureSupervisorRunningForksWhenLaunchdFailsWithoutRequireServiceManager(t *testing.T) {
	fake, _, _ := supervisorStartOnFakeLaunchd(t)
	failLaunchctlEnable(fake, errors.New("Load failed: 5: Input/output error"))
	stubSupervisorInstall(t, "gc supervisor install: launchctl enable gui/501/com.gascity.supervisor: exit status 112")
	forks := 0
	oldStart, oldAlive := startSupervisorChild, supervisorAliveHook
	t.Cleanup(func() { startSupervisorChild, supervisorAliveHook = oldStart, oldAlive })
	startSupervisorChild = func(*exec.Cmd) error {
		forks++
		return nil
	}
	supervisorAliveHook = func() int {
		if forks > 0 {
			return 4242
		}
		return 0
	}

	var stdout, stderr bytes.Buffer
	if code := ensureSupervisorRunning(&stdout, &stderr); code != 0 {
		t.Fatalf("ensureSupervisorRunning = %d, want 0; stderr=%s", code, stderr.String())
	}
	if forks != 1 {
		t.Fatalf("forks = %d, want the fork fallback", forks)
	}
	assertStderrContains(t, stderr.String(), "starting the supervisor outside launchd instead")
}

// The drift restart replaces a supervisor that no service manager runs by
// killing it and forking a new one from this shell. With
// require_service_manager set it refuses before it touches the running
// supervisor.
func TestRunStartDriftCheckRequireServiceManagerRefusesDirectRestart(t *testing.T) {
	cityPath, setCommit := driftCheckEnv(t, "old-build-id")
	setCommit("new-build-id")
	oldDry, oldNoAR := dryRunMode, noAutoRestartMode
	dryRunMode, noAutoRestartMode = false, false
	oldLaunchdActive := supervisorLaunchdActive
	t.Cleanup(func() {
		dryRunMode, noAutoRestartMode = oldDry, oldNoAR
		supervisorLaunchdActive = oldLaunchdActive
	})
	supervisorLaunchdActive = func(string) bool { return false }
	requireServiceManagerForTest(t)
	restartHelpersHook = func() restartHelpers {
		return restartHelpers{
			Systemctl: func(...string) error { t.Error("systemctl restarted an unmanaged supervisor"); return nil },
			Launchctl: func(...string) error { t.Error("launchctl restarted an unmanaged supervisor"); return nil },
			Kill:      func(int) error { t.Error("killed the running supervisor"); return nil },
			WaitExit:  func(int) error { return nil },
			Spawn:     func(string, ...string) error { t.Error("forked a replacement supervisor"); return nil },
		}
	}

	var stdout, stderr bytes.Buffer
	exitCode, cont := runStartDriftCheck(cityPath, &stdout, &stderr)
	if exitCode != 1 || cont {
		t.Fatalf("runStartDriftCheck = (%d, %v), want (1, false); stderr=%s", exitCode, cont, stderr.String())
	}
	assertStderrContains(t, stderr.String(),
		fmt.Sprintf("PID %d runs outside", os.Getpid()),
		"require_service_manager in "+supervisor.ConfigPath(),
		"forbids replacing it with a forked supervisor",
		"gc supervisor stop --wait")
	if strings.Contains(stdout.String(), "Restarting supervisor") {
		t.Errorf("stdout announces a restart it refused:\n%s", stdout.String())
	}
}

// require_service_manager refuses forks only: a drift restart that launchd
// performs still runs.
func TestRunStartDriftCheckRequireServiceManagerKeepsLaunchdRestart(t *testing.T) {
	cityPath, setCommit := driftCheckEnv(t, "old-build-id")
	setCommit("new-build-id")
	oldDry, oldNoAR := dryRunMode, noAutoRestartMode
	dryRunMode, noAutoRestartMode = false, false
	oldGOOS, oldLaunchdActive := supervisorRuntimeGOOS, supervisorLaunchdActive
	t.Cleanup(func() {
		dryRunMode, noAutoRestartMode = oldDry, oldNoAR
		supervisorRuntimeGOOS, supervisorLaunchdActive = oldGOOS, oldLaunchdActive
	})
	supervisorRuntimeGOOS = "darwin"
	supervisorLaunchdActive = func(label string) bool { return label == supervisorLaunchdLabel() }
	requireServiceManagerForTest(t)
	var launchctlArgs []string
	restartHelpersHook = func() restartHelpers {
		return restartHelpers{
			Launchctl: func(args ...string) error { launchctlArgs = append([]string(nil), args...); return nil },
			Kill:      func(int) error { t.Error("killed a launchd-managed supervisor"); return nil },
			WaitExit:  func(int) error { return nil },
			Spawn:     func(string, ...string) error { t.Error("forked a replacement supervisor"); return nil },
		}
	}

	var stdout, stderr bytes.Buffer
	exitCode, cont := runStartDriftCheck(cityPath, &stdout, &stderr)
	if exitCode != 0 || !cont {
		t.Fatalf("runStartDriftCheck = (%d, %v), want (0, true); stderr=%s", exitCode, cont, stderr.String())
	}
	if want := "kickstart -k " + supervisorLaunchdServiceTarget(supervisorLaunchdLabel()); strings.Join(launchctlArgs, " ") != want {
		t.Fatalf("launchctl args = %v, want %q", launchctlArgs, want)
	}
}
