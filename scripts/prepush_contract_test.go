package scripts_test

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestPrePushFailsClosedWhenStdinHasNoRefs is the regression test for gc-uz8az.
//
// git feeds one "<local ref> <local sha> <remote ref> <remote sha>" line per
// pushed ref on the hook's stdin (githooks(5)). The old hook derived
// `go_changed` purely from those lines and, if it read ZERO lines, left
// `go_changed=0` and `exit 0` immediately — emitting nothing. That degraded the
// push-time test gate to a silent no-op indistinguishable, from the pusher's
// side, from "tests ran and passed": a 581-file rebase push once landed in 3s
// with no hook output and the suite never ran.
//
// The gate must fail CLOSED on a degenerate (no-refs) read: it cannot see what
// is being pushed, so it must run the suite rather than silently pass.
func TestPrePushFailsClosedWhenStdinHasNoRefs(t *testing.T) {
	repoRoot := repoRoot(t)
	hookPath := filepath.Join(repoRoot, ".githooks", "pre-push")

	workDir, callLog, _, _, _ := setupPrePushFakeRepo(t)

	out, log, _ := runPrePush(t, hookPath, workDir, callLog, "" /* empty stdin: no ref lines */)

	if !strings.Contains(log, "test-fast-parallel") {
		t.Fatalf("no-refs stdin must fail closed and run the suite, but make test-fast-parallel was never invoked\n--- make call log ---\n%s\n--- hook output ---\n%s", log, out)
	}
	if strings.TrimSpace(out) == "" {
		t.Fatalf("no-refs stdin must not be silent: the hook produced no output at all")
	}
}

// TestPrePushLoudlySkipsWhenNoGoChanges covers the legitimate skip path: git
// fed ref lines, none of which touched a .go file. The gate correctly skips the
// suite — but the skip must be LOUD (a visible line in push output), never a
// silent exit 0, so an absent test run is always noticeable.
func TestPrePushLoudlySkipsWhenNoGoChanges(t *testing.T) {
	repoRoot := repoRoot(t)
	hookPath := filepath.Join(repoRoot, ".githooks", "pre-push")

	workDir, callLog, _, goSHA, docSHA := setupPrePushFakeRepo(t)

	// Advance the remote ref from the go-change commit to the doc-only commit:
	// the diff touches only README.md, so no .go files changed.
	stdin := fmt.Sprintf("refs/heads/main %s refs/heads/main %s\n", docSHA, goSHA)
	out, log, code := runPrePush(t, hookPath, workDir, callLog, stdin)

	if strings.Contains(log, "test-fast-parallel") {
		t.Fatalf("no .go changes must skip the suite, but make test-fast-parallel was invoked\n--- make call log ---\n%s", log)
	}
	if code != 0 {
		t.Fatalf("skip path must exit 0, got exit %d\n--- hook output ---\n%s", code, out)
	}
	if strings.TrimSpace(out) == "" {
		t.Fatalf("skip path must be loud (a visible skip line), but the hook produced no output")
	}
}

// TestPrePushRunsSuiteOnGoChanges guards the normal path: a pushed ref whose
// diff touches a .go file must run the suite. Ensures the fail-closed/loud-skip
// changes did not regress the gate's core job.
func TestPrePushRunsSuiteOnGoChanges(t *testing.T) {
	repoRoot := repoRoot(t)
	hookPath := filepath.Join(repoRoot, ".githooks", "pre-push")

	workDir, callLog, baseSHA, goSHA, _ := setupPrePushFakeRepo(t)

	// Advance the remote ref from base to the go-change commit: the diff touches
	// main.go, so the suite must run.
	stdin := fmt.Sprintf("refs/heads/main %s refs/heads/main %s\n", goSHA, baseSHA)
	out, log, _ := runPrePush(t, hookPath, workDir, callLog, stdin)

	if !strings.Contains(log, "test-fast-parallel") {
		t.Fatalf("a .go change must run the suite, but make test-fast-parallel was never invoked\n--- make call log ---\n%s\n--- hook output ---\n%s", log, out)
	}
}

// setupPrePushFakeRepo builds a minimal git repo with three commits — a base
// commit, a .go-touching commit, and a doc-only commit — so tests can construct
// stdin ref lines whose remote..local diff does or does not touch a .go file. It
// stubs `make` on PATH to append its args to a call log (and exit 0) so a test
// can detect whether the hook reached `make test-fast-parallel`. Returns the
// worktree path, the call-log path, and the three commit SHAs (base, go, doc).
func setupPrePushFakeRepo(t *testing.T) (workDir, callLog, baseSHA, goSHA, docSHA string) {
	t.Helper()

	workDir = t.TempDir()
	binDir := filepath.Join(workDir, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatalf("mkdir bin: %v", err)
	}
	callLog = filepath.Join(workDir, "calls.log")

	stub := fmt.Sprintf(`#!/usr/bin/env bash
printf '%%s %%s\n' "$(basename "$0")" "$*" >> %q
exit 0
`, callLog)
	writeExecutable(t, filepath.Join(binDir, "make"), stub)

	runGit(t, workDir, "init", "-q", "--initial-branch=main")
	runGit(t, workDir, "config", "user.email", "test@example.com")
	runGit(t, workDir, "config", "user.name", "Pre-push test")
	// The push-time gate runs the suite under `env -i` (see .githooks/pre-push),
	// which drops SSH_AUTH_SOCK. A developer whose global config sets
	// commit.gpgsign=true with gpg.format=ssh would otherwise fail every commit
	// below with "Couldn't get agent socket?". Disable signing locally so the
	// fixture is hermetic regardless of the developer's global git config.
	runGit(t, workDir, "config", "commit.gpgsign", "false")

	writePlainFile(t, filepath.Join(workDir, "main.go"), "package main\n\nfunc main() {}\n")
	writePlainFile(t, filepath.Join(workDir, "README.md"), "# base\n")
	runGit(t, workDir, "add", "-A")
	runGit(t, workDir, "commit", "-q", "-m", "base")
	baseSHA = runGit(t, workDir, "rev-parse", "HEAD")

	writePlainFile(t, filepath.Join(workDir, "main.go"), "package main\n\nfunc main() { _ = 1 }\n")
	runGit(t, workDir, "add", "-A")
	runGit(t, workDir, "commit", "-q", "-m", "go change")
	goSHA = runGit(t, workDir, "rev-parse", "HEAD")

	writePlainFile(t, filepath.Join(workDir, "README.md"), "# base\n\nmore docs\n")
	runGit(t, workDir, "add", "-A")
	runGit(t, workDir, "commit", "-q", "-m", "doc change")
	docSHA = runGit(t, workDir, "rev-parse", "HEAD")

	return workDir, callLog, baseSHA, goSHA, docSHA
}

// runPrePush executes the pre-push hook under bash with a stubbed PATH, feeding
// stdin as the ref lines git would supply. It returns the combined output, the
// contents of the make call log (empty if the hook never invoked make), and the
// hook's exit code.
func runPrePush(t *testing.T, hookPath, workDir, callLog, stdin string) (string, string, int) {
	t.Helper()

	cmd := exec.Command("bash", hookPath, "origin", "https://example.invalid")
	cmd.Dir = workDir
	cmd.Env = []string{
		"PATH=" + filepath.Join(workDir, "bin") + string(os.PathListSeparator) + os.Getenv("PATH"),
		"HOME=" + t.TempDir(),
		"TMPDIR=" + t.TempDir(),
		"GIT_TERMINAL_PROMPT=0",
	}
	cmd.Stdin = strings.NewReader(stdin)

	out, err := cmd.CombinedOutput()
	exitCode := 0
	if err != nil {
		ee := &exec.ExitError{}
		ok := errors.As(err, &ee)
		if !ok {
			t.Fatalf("run pre-push hook: %v\n%s", err, out)
		}
		exitCode = ee.ExitCode()
	}

	logBytes, err := os.ReadFile(callLog)
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("read make call log: %v", err)
	}
	return string(out), string(logBytes), exitCode
}

// writePlainFile writes a non-executable file, creating parent dirs as needed.
func writePlainFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
