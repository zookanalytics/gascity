package scripts_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/testpolicy/shellpipeline"
)

// TestGitHooksAvoidPipefailGrepQPipelines runs the shared pipefail/`grep -q`
// detector over the repo's git hooks. The core pack's shipped scripts already
// have this guard (TestCoreShippedScriptsAvoidPipefailGrepQPipelines), but it
// walks the embedded PackFS and so never covered .githooks/ — which is exactly
// where the fifth sighting of the class landed (gc-01o2l): the pre-push hook
// derived "did any .go file change?" through `git diff ... | grep -q .`, and
// on a large diff grep's early exit SIGPIPEd git, pipefail promoted the 141,
// and the hook concluded no Go changed and skipped the entire test fan-out.
func TestGitHooksAvoidPipefailGrepQPipelines(t *testing.T) {
	hooksDir := filepath.Join(repoRoot(t), ".githooks")
	entries, err := os.ReadDir(hooksDir)
	if err != nil {
		t.Fatalf("read .githooks: %v", err)
	}
	checked := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		path := filepath.Join(hooksDir, entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		checked++
		for _, offset := range shellpipeline.FindPipefailGrepQPipelines(data) {
			lineNumber, text := shellpipeline.DescribeLine(data, offset)
			t.Errorf(".githooks/%s:%d: %s: %s", entry.Name(), lineNumber, shellpipeline.Remedy, text)
		}
	}
	if checked == 0 {
		t.Fatal("no hooks found under .githooks — the guard would pass vacuously")
	}
}

// prePushSentinel is where the stub Makefile records that the hook actually
// reached `make test-fast-parallel`.
const prePushSentinel = "fan-out-ran"

// setupPrePushRepo builds a hermetic git repo wired with the REAL
// .githooks/pre-push and the REAL ownership-guard lib (so an edit to either
// is caught here), plus a stub Makefile whose test-fast-parallel target only
// records that it ran. Returns the repo dir and the sentinel path.
func setupPrePushRepo(t *testing.T) (string, string) {
	t.Helper()
	root := repoRoot(t)
	repo := t.TempDir()
	sentinel := filepath.Join(t.TempDir(), prePushSentinel)

	run := func(args ...string) {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = repo
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
		}
	}
	run("init", "-q", "-b", "main")
	run("config", "user.email", "test@example.invalid")
	run("config", "user.name", "test")
	run("config", "commit.gpgsign", "false")

	for _, dir := range []string{".githooks", "scripts"} {
		if err := os.MkdirAll(filepath.Join(repo, dir), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
	}
	for _, f := range []struct{ src, dst string }{
		{filepath.Join(root, ".githooks", "pre-push"), filepath.Join(repo, ".githooks", "pre-push")},
		{filepath.Join(root, "scripts", "push-ownership-guard.sh"), filepath.Join(repo, "scripts", "push-ownership-guard.sh")},
	} {
		data, err := os.ReadFile(f.src)
		if err != nil {
			t.Fatalf("read %s: %v", f.src, err)
		}
		if err := os.WriteFile(f.dst, data, 0o755); err != nil {
			t.Fatalf("write %s: %v", f.dst, err)
		}
	}
	// Tabs matter in a Makefile recipe.
	makefile := "test-fast-parallel:\n\t@printf 'ran\\n' > " + sentinel + "\n"
	if err := os.WriteFile(filepath.Join(repo, "Makefile"), []byte(makefile), 0o644); err != nil {
		t.Fatalf("write Makefile: %v", err)
	}
	run("add", "-A")
	run("commit", "-qm", "base")
	return repo, sentinel
}

// runPrePush invokes the hook exactly as git does: argv is the remote name and
// URL, and the pushed refs arrive on stdin. POG_DISABLE keeps the unrelated
// bead-ownership guard out of the way.
func runPrePush(t *testing.T, repo, stdin string) (string, error) {
	t.Helper()
	cmd := exec.Command(filepath.Join(repo, ".githooks", "pre-push"), "origin", "https://example.invalid")
	cmd.Dir = repo
	cmd.Stdin = strings.NewReader(stdin)
	cmd.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + t.TempDir(),
		"POG_DISABLE=1",
	}
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// gitHead returns repo's current HEAD sha.
func gitHead(t *testing.T, repo string) string {
	t.Helper()
	cmd := exec.Command("git", "rev-parse", "HEAD")
	cmd.Dir = repo
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("git rev-parse HEAD: %v", err)
	}
	return strings.TrimSpace(string(out))
}

// TestPrePushRunsFanOutWhenGoDiffExceedsPipeBuffer is the regression test for
// gc-01o2l. It commits enough .go files that `git diff --name-only` output far
// outruns what `grep -q` drains before its early exit, which makes the old
// `| grep -q .` SIGPIPE deterministic rather than the load-sensitive flake
// that was originally observed (the reported push skipped its fan-out on a
// repeat of a push that had run it 29 minutes earlier). The hook MUST still
// reach the test fan-out: concluding "no Go changed" here means a push
// carrying hundreds of changed Go files ships with the suite never having run.
//
// The threshold is empirical, not the 64KiB pipe capacity: grep reads greedily
// before matching, so the writer only reliably blocks-then-SIGPIPEs well past
// that. Measured against the pre-fix pipeline, ~190KiB of path output failed
// 20/20 while ~69KiB passed every time. minDiffBytes keeps a wide margin.
func TestPrePushRunsFanOutWhenGoDiffExceedsPipeBuffer(t *testing.T) {
	repo, sentinel := setupPrePushRepo(t)
	base := gitHead(t, repo)

	// Long paths keep the file count (and so the test's cost) down while
	// pushing the name-only output past the measured threshold.
	const (
		files        = 1200
		minDiffBytes = 128 * 1024
		nameFm       = "internal/generated/deeply/nested/package/path/with/quite/a/few/segments/to/lengthen/each/entry/padding_padding_padding_padding_generated_source_file_%05d.go"
	)
	dir := filepath.Join(repo, "internal", "generated", "deeply", "nested", "package", "path", "with", "quite", "a", "few", "segments", "to", "lengthen", "each", "entry")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	total := 0
	for i := 0; i < files; i++ {
		name := fmt.Sprintf(nameFm, i)
		total += len(name) + 1
		if err := os.WriteFile(filepath.Join(repo, name), []byte("package path\n"), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	if total < minDiffBytes {
		t.Fatalf("diff name-only output is %d bytes, need >=%d to make the SIGPIPE deterministic", total, minDiffBytes)
	}
	for _, args := range [][]string{{"add", "-A"}, {"commit", "-qm", "many go files"}} {
		cmd := exec.Command("git", args...)
		cmd.Dir = repo
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
		}
	}
	head := gitHead(t, repo)

	out, err := runPrePush(t, repo, fmt.Sprintf("refs/heads/main %s refs/heads/main %s\n", head, base))
	if err != nil {
		t.Fatalf("pre-push hook failed: %v\n%s", err, out)
	}
	if _, statErr := os.Stat(sentinel); statErr != nil {
		t.Fatalf("pre-push skipped the test fan-out on a %d-byte .go diff — the gate failed OPEN "+
			"and the push would ship ungated (hook output: %q)", total, out)
	}
}

// TestPrePushRunsFanOutOnSmallGoDiff is the same contract at the ordinary
// size, where the old pipeline usually — but not always — got the right
// answer. It pins the behavior the large-diff case generalizes.
func TestPrePushRunsFanOutOnSmallGoDiff(t *testing.T) {
	repo, sentinel := setupPrePushRepo(t)
	base := gitHead(t, repo)
	if err := os.WriteFile(filepath.Join(repo, "main.go"), []byte("package main\n"), 0o644); err != nil {
		t.Fatalf("write main.go: %v", err)
	}
	for _, args := range [][]string{{"add", "-A"}, {"commit", "-qm", "one go file"}} {
		cmd := exec.Command("git", args...)
		cmd.Dir = repo
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
		}
	}
	head := gitHead(t, repo)

	out, err := runPrePush(t, repo, fmt.Sprintf("refs/heads/main %s refs/heads/main %s\n", head, base))
	if err != nil {
		t.Fatalf("pre-push hook failed: %v\n%s", err, out)
	}
	if _, statErr := os.Stat(sentinel); statErr != nil {
		t.Fatalf("pre-push skipped the test fan-out on a one-file .go diff (hook output: %q)", out)
	}
}

// TestPrePushAnnouncesEverySkip covers the second half of gc-01o2l: the gate
// did not merely skip, it skipped SILENTLY. `exit 0` with no output is
// indistinguishable from "the suite ran and passed", so nothing downstream —
// human or machine — can notice a gate that failed open. Every path that
// declines to run the suite must say so.
func TestPrePushAnnouncesEverySkip(t *testing.T) {
	repo, sentinel := setupPrePushRepo(t)
	head := gitHead(t, repo)
	zero := strings.Repeat("0", 40)

	for _, tc := range []struct {
		name  string
		stdin string
	}{
		{name: "no refs on stdin at all", stdin: ""},
		{name: "branch deletion only", stdin: fmt.Sprintf("refs/heads/gone %s refs/heads/gone %s\n", zero, head)},
		{name: "no go files changed", stdin: fmt.Sprintf("refs/heads/main %s refs/heads/main %s\n", head, head)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_ = os.Remove(sentinel)
			out, err := runPrePush(t, repo, tc.stdin)
			if err != nil {
				t.Fatalf("pre-push hook failed: %v\n%s", err, out)
			}
			if _, statErr := os.Stat(sentinel); statErr == nil {
				t.Fatalf("expected the hook to skip the fan-out, but it ran it")
			}
			if strings.TrimSpace(out) == "" {
				t.Fatal("pre-push skipped the test suite and printed NOTHING — a silent exit 0 is " +
					"indistinguishable from a suite that ran and passed, which is how an ungated push " +
					"goes unnoticed (gc-01o2l, gc-uz8az). Announce every skip.")
			}
		})
	}
}
