package scripts_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestMakefileTestEnvIgnoresUserGitConfiguration(t *testing.T) {
	repoRoot := repoRoot(t)
	makefile, err := os.ReadFile(filepath.Join(repoRoot, "Makefile"))
	if err != nil {
		t.Fatalf("read Makefile: %v", err)
	}

	home := t.TempDir()
	if err := os.WriteFile(filepath.Join(home, ".gitconfig"), []byte("[commit]\n\tgpgsign = true\n"), 0o644); err != nil {
		t.Fatalf("write poisoned global git config: %v", err)
	}

	testMakefile := filepath.Join(t.TempDir(), "Makefile")
	content := string(makefile) + `
.PHONY: print-test-env-git
print-test-env-git:
	@$(TEST_ENV) sh -c 'printf "global=%s\nnosystem=%s\ngitdir=%s\ngpgsign=%s\n" "$$GIT_CONFIG_GLOBAL" "$$GIT_CONFIG_NOSYSTEM" "$${GIT_DIR-unset}" "$$(git config --global --get commit.gpgsign 2>/dev/null || printf unset)"'
`
	if err := os.WriteFile(testMakefile, []byte(content), 0o644); err != nil {
		t.Fatalf("write test Makefile: %v", err)
	}

	cmd := makeCommand("--no-print-directory", "-f", testMakefile, "print-test-env-git")
	cmd.Dir = repoRoot
	cmd.Env = []string{
		"PATH=" + offlineBrewDir(t) + string(os.PathListSeparator) + os.Getenv("PATH"),
		"HOME=" + home,
		"USER=" + os.Getenv("USER"),
		"SHELL=/bin/sh",
		"GIT_DIR=/poison/.git",
		// Makefile-internal `go env` probes must not download a toolchain
		// into the isolated HOME: the module cache's read-only files would
		// defeat t.TempDir cleanup.
		"GOTOOLCHAIN=local",
		"GOMODCACHE=" + filepath.Join(os.TempDir(), "gc-makefile-git-gomodcache"),
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("make print-test-env-git failed: %v\n%s", err, out)
	}
	for _, want := range []string{
		"nosystem=1",
		"gitdir=unset",
		"gpgsign=unset",
	} {
		if !strings.Contains(string(out), want+"\n") {
			t.Errorf("TEST_ENV output missing %q:\n%s", want, out)
		}
	}

	// The global config is a real, writable, seeded file outside the user's
	// HOME — not the user's own config and not an unwritable sentinel (a
	// /dev/null sentinel breaks ensure_beads_role's global write path).
	var globalPath string
	for _, line := range strings.Split(string(out), "\n") {
		if v, ok := strings.CutPrefix(line, "global="); ok {
			globalPath = v
		}
	}
	if globalPath == "" {
		t.Fatalf("TEST_ENV output missing global= line:\n%s", out)
	}
	if strings.HasPrefix(globalPath, home+string(os.PathSeparator)) {
		t.Errorf("GIT_CONFIG_GLOBAL %q resolves under the poisoned HOME %q", globalPath, home)
	}
	info, err := os.Stat(globalPath)
	if err != nil {
		t.Fatalf("GIT_CONFIG_GLOBAL %q does not exist: %v", globalPath, err)
	}
	if info.Mode().Perm()&0o200 == 0 {
		t.Errorf("GIT_CONFIG_GLOBAL %q is not writable (mode %v)", globalPath, info.Mode())
	}
}

func TestShardTestEnvsIgnoreUserGitConfiguration(t *testing.T) {
	repoRoot := repoRoot(t)
	for _, path := range []string{
		"scripts/test-local-parallel",
		"scripts/test-go-test-shard",
		"scripts/test-integration-shard",
	} {
		t.Run(path, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join(repoRoot, path))
			if err != nil {
				t.Fatalf("read %s: %v", path, err)
			}
			content := string(data)
			for _, pin := range []string{
				"GIT_CONFIG_NOSYSTEM=1",
				`GIT_CONFIG_GLOBAL="$gc_test_gitconfig"`,
				`gc_test_gitconfig="$("$repo_root/scripts/test-gitconfig-path")"`,
			} {
				if got := strings.Count(content, pin); got != 1 {
					t.Errorf("%s has %d occurrences of %q, want 1", path, got, pin)
				}
			}
			// ga-cesmzs: only test-local-parallel crosses a subprocess boundary
			// (the xargs fan-out worker), so only it must export the variable.
			if path == "scripts/test-local-parallel" {
				if got := strings.Count(content, "\nexport gc_test_gitconfig\n"); got != 1 {
					t.Errorf("%s must export gc_test_gitconfig for the xargs fan-out workers (found %d)", path, got)
				}
			}
		})
	}
}

// shellAssignment finds a SHELL assignment in an env -i allowlist. The leading
// guard keeps names such as GIT_SHELL from matching, and the value stops at
// whitespace or the line-continuation backslash.
var shellAssignment = regexp.MustCompile(`(?:^|[^A-Za-z0-9_])SHELL=([^\s\\]*)`)

// forwardedShell finds every spelling that copies the caller's SHELL into a
// nested environment: a make $$SHELL, a shell $SHELL, or ${SHELL:-...}. A make
// $(SHELL) is not one.
var forwardedShell = regexp.MustCompile(`\$\$?\{?SHELL\b`)

// TestRunnerTestEnvsPinPaneShell guards ga-yghhjf. A test that starts a tmux or
// herdr pane runs the environment's SHELL in it. Forward the invoking user's
// zsh into a fresh HOME and the pane opens zsh's new-user wizard, which
// swallows the text the test types. Every allowlist that builds a test
// environment therefore pins SHELL=/bin/sh once and never forwards the caller's
// value. The four are mirrored copies of one allowlist and change together
// (scripts/AGENTS.md).
func TestRunnerTestEnvsPinPaneShell(t *testing.T) {
	repoRoot := repoRoot(t)
	for _, path := range []string{
		"Makefile",
		"scripts/test-local-parallel",
		"scripts/test-go-test-shard",
		"scripts/test-integration-shard",
	} {
		t.Run(path, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join(repoRoot, path))
			if err != nil {
				t.Fatalf("read %s: %v", path, err)
			}
			var pins []string
			for _, line := range strings.Split(string(data), "\n") {
				// A comment may explain the pin; only live text counts.
				if strings.HasPrefix(strings.TrimSpace(line), "#") {
					continue
				}
				if forwardedShell.MatchString(line) {
					t.Errorf("%s forwards the caller's SHELL: %s", path, strings.TrimSpace(line))
				}
				for _, m := range shellAssignment.FindAllStringSubmatch(line, -1) {
					pins = append(pins, m[1])
				}
			}
			if len(pins) != 1 || pins[0] != "/bin/sh" {
				t.Errorf("%s assigns SHELL %q, want exactly one assignment, to /bin/sh", path, pins)
			}
		})
	}
}

// TestFanOutWorkerReceivesExportedGitConfigGlobal exercises the real
// run_fan_out xargs/bash -c dispatch end to end through runFanOutProbe instead
// of asserting on source text. This catches an unexported gc_test_gitconfig
// (ga-9t7vpl) by the worker actually failing, a class of regression
// TestShardTestEnvsIgnoreUserGitConfiguration above cannot detect since it
// only counts a string, not the variable's export scope.
func TestFanOutWorkerReceivesExportedGitConfigGlobal(t *testing.T) {
	probeCmd := `if [ -n "${GIT_CONFIG_GLOBAL:-}" ] && [ -f "$GIT_CONFIG_GLOBAL" ] && [ -w "$GIT_CONFIG_GLOBAL" ]; then printf "GIT_CONFIG_GLOBAL_OK=%s\n" "$GIT_CONFIG_GLOBAL"; else printf "GIT_CONFIG_GLOBAL_MISSING\n"; exit 1; fi`
	probe := runFanOutProbe(t, probeCmd, nil)

	if probe.runErr != nil {
		t.Fatalf("run_fan_out worker did not receive a usable GIT_CONFIG_GLOBAL: %v\nharness output:\n%s\nprobe log (err=%v):\n%s",
			probe.runErr, probe.out, probe.readErr, probe.log)
	}
	if probe.readErr != nil {
		t.Fatalf("read probe log %s: %v\nharness output:\n%s", probe.logPath, probe.readErr, probe.out)
	}
	if !strings.Contains(string(probe.log), "GIT_CONFIG_GLOBAL_OK=") {
		t.Fatalf("probe job did not confirm GIT_CONFIG_GLOBAL; probe log:\n%s\nharness output:\n%s", probe.log, probe.out)
	}
}

// TestFanOutJobsKeepTheCallersPATH pins the job shell to a non-login bash. A
// job must run under exactly the PATH it was handed, so it resolves the same
// python3, bash and git as the caller. A login shell sources /etc/profile,
// whose macOS path_helper moves /usr/bin ahead of Homebrew, and the login
// profile under HOME. The probe's HOME carries a login profile that prepends a
// marker directory to PATH, so a login shell fails this test on every host,
// not only where /etc/profile rewrites PATH.
func TestFanOutJobsKeepTheCallersPATH(t *testing.T) {
	home := t.TempDir()
	// A login bash reads .bash_profile; a login POSIX sh reads .profile.
	for _, name := range []string{".bash_profile", ".profile"} {
		profile := "PATH=/login-profile-ran:$PATH; export PATH\n"
		if err := os.WriteFile(filepath.Join(home, name), []byte(profile), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}

	callerPATH := os.Getenv("PATH")
	probe := runFanOutProbe(t, `printf "JOB_PATH=%s\n" "$PATH"`, append(os.Environ(), "HOME="+home))
	if probe.runErr != nil || probe.readErr != nil {
		t.Fatalf("PATH probe job failed: %v\nharness output:\n%s\nprobe log (err=%v):\n%s",
			probe.runErr, probe.out, probe.readErr, probe.log)
	}

	var jobPATH string
	for _, line := range strings.Split(string(probe.log), "\n") {
		if value, ok := strings.CutPrefix(line, "JOB_PATH="); ok {
			jobPATH = value
		}
	}
	if jobPATH != callerPATH {
		t.Fatalf("a fan-out job ran under a PATH other than the caller's; run each job with bash -c, not a login shell, which sources /etc/profile and the login profile under HOME\njob PATH:    %s\ncaller PATH: %s\nprobe log:\n%s",
			jobPATH, callerPATH, probe.log)
	}
}

// fanOutProbe is one runFanOutProbe run: the harness output and exit error,
// and the probe job's log.
type fanOutProbe struct {
	out     []byte
	runErr  error
	logPath string
	log     []byte
	readErr error
}

// runFanOutProbe extracts the live gc_test_gitconfig preamble and the
// run_fan_out function body from scripts/test-local-parallel and runs them
// under a minimal synthetic harness whose one jobspec runs probeCmd. The
// jobspec is single-quoted in the harness, so probeCmd must not contain a
// single quote. env is the harness environment; nil inherits the test's.
func runFanOutProbe(t *testing.T, probeCmd string, env []string) fanOutProbe {
	t.Helper()
	repoRoot := repoRoot(t)
	scriptPath := filepath.Join(repoRoot, "scripts", "test-local-parallel")
	data, err := os.ReadFile(scriptPath)
	if err != nil {
		t.Fatalf("read %s: %v", scriptPath, err)
	}
	content := string(data)

	const assignLine = `gc_test_gitconfig="$("$repo_root/scripts/test-gitconfig-path")"`
	if !strings.Contains(content, assignLine) {
		t.Fatalf("%s: gc_test_gitconfig assignment line not found (expected exact text %q)", scriptPath, assignLine)
	}
	preamble := assignLine
	if strings.Contains(content, "\nexport gc_test_gitconfig\n") {
		preamble += "\nexport gc_test_gitconfig"
	}

	const fanOutOpen = "run_fan_out() {\n"
	startIdx := strings.Index(content, fanOutOpen)
	if startIdx == -1 {
		t.Fatalf("%s: run_fan_out() function not found", scriptPath)
	}
	bodyStart := startIdx + len(fanOutOpen)
	endIdx := strings.Index(content[bodyStart:], "\n}\n")
	if endIdx == -1 {
		t.Fatalf("%s: run_fan_out() closing brace not found", scriptPath)
	}
	fanOutBody := content[bodyStart : bodyStart+endIdx]

	logDir := t.TempDir()
	lines := []string{
		"#!/usr/bin/env bash",
		"set -euo pipefail",
		"repo_root=" + shellQuote(repoRoot),
		preamble,
		"run_fan_out() {",
		fanOutBody,
		"}",
		`gate_fd=""`,
		"local_jobs=1",
		"jobspecs=('probe::" + probeCmd + "')",
		"export LOCAL_TEST_LOG_DIR=" + shellQuote(logDir),
		`export TEST_LOCAL_NICE=""`,
		"export TEST_LOCAL_GOPATH=" + shellQuote(goEnvValue(t, "GOPATH")),
		"export TEST_LOCAL_GOCACHE=" + shellQuote(goEnvValue(t, "GOCACHE")),
		"export TEST_LOCAL_GOMODCACHE=" + shellQuote(goEnvValue(t, "GOMODCACHE")),
		"export TEST_LOCAL_GOTMPDIR=" + shellQuote(goEnvValue(t, "GOTMPDIR")),
		"export TEST_LOCAL_GOROOT=" + shellQuote(goEnvValue(t, "GOROOT")),
		"set +e",
		"run_fan_out",
		"status=$?",
		"set -e",
		`exit "$status"`,
	}
	harnessPath := filepath.Join(t.TempDir(), "run_fan_out_harness.sh")
	if err := os.WriteFile(harnessPath, []byte(strings.Join(lines, "\n")+"\n"), 0o755); err != nil {
		t.Fatalf("write harness script: %v", err)
	}

	cmd := testCommand("bash", harnessPath)
	cmd.Env = env
	out, runErr := cmd.CombinedOutput()
	logPath := filepath.Join(logDir, "probe.log")
	probeLog, readErr := os.ReadFile(logPath)
	return fanOutProbe{out: out, runErr: runErr, logPath: logPath, log: probeLog, readErr: readErr}
}
