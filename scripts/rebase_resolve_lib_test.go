package scripts_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestRebaseResolveLibUsesBash3Syntax(t *testing.T) {
	root := repoRoot(t)
	path := filepath.Join(root, "scripts", "rebase-resolve-lib.sh")
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read rebase-resolve-lib.sh: %v", err)
	}

	bash4LowercaseExpansion := regexp.MustCompile(`\$\{[^}\n]*,,[^}\n]*\}`)
	if match := bash4LowercaseExpansion.Find(contents); match != nil {
		t.Fatalf("rebase-resolve-lib.sh must run under macOS Bash 3; found Bash 4 lowercase expansion %q", match)
	}
}

// TestRebaseResolveLibNoPipefailGrepQ guards against reintroducing the
// `writer | grep -q` SIGPIPE false-negative defect class (gc-u2fyx, following
// gc-atrp1 / gc-d760o / d416a0085). `grep -q` exits the instant it matches
// without draining stdin, so a writer piped into it races into a SIGPIPE;
// under a caller's `set -o pipefail` that 141 becomes the pipeline's exit
// status and a PRESENT match is reported as ABSENT. Use git grep's own exit
// status, capture-then-test, or a here-string (`grep -q ... <<<"$x"`) instead.
//
// rebase-resolve-lib.sh sets no in-file pipefail, but it is sourced by callers
// that do (the deployer's evaluate-gate step), so these pipelines are latent.
func TestRebaseResolveLibNoPipefailGrepQ(t *testing.T) {
	root := repoRoot(t)
	path := filepath.Join(root, "scripts", "rebase-resolve-lib.sh")
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read rebase-resolve-lib.sh: %v", err)
	}

	pipeIntoGrepQ := regexp.MustCompile(`\|\s*grep -q`)
	for i, line := range strings.Split(string(contents), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue // prose describing the defect, not a live pipeline
		}
		if pipeIntoGrepQ.MatchString(line) {
			t.Errorf("rebase-resolve-lib.sh:%d: piping a writer into `grep -q` SIGPIPEs the writer on grep's early exit and reads a present match as absent under `set -o pipefail`; use git grep's exit status, capture-then-test, or a here-string: %s", i+1, strings.TrimSpace(line))
		}
	}
}

// TestRebaseResolveLib runs the shell self-test for
// scripts/rebase-resolve-lib.sh, the deployer's bounded self-rebase
// trivial-conflict classifier. It exercises the classifier against real
// temp git repos (identical/one-side-empty/additive-both hunks, real
// conflicts, structural conflicts) plus attempt_bounded_self_rebase's guard
// rails and --force-with-lease push behavior. Hermetic: temp git repos only,
// no network/gh/model calls.
func TestRebaseResolveLib(t *testing.T) {
	root := repoRoot(t)

	cmd := exec.Command(filepath.Join(root, "scripts", "test-rebase-resolve.sh"))
	cmd.Dir = root
	cmd.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + t.TempDir(),
		"TMPDIR=" + t.TempDir(),
	}

	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("test-rebase-resolve.sh failed: %v\n%s", err, out)
	}
}
