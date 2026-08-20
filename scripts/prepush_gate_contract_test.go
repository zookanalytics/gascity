package scripts_test

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/testpolicy/shellpipeline"
)

// TestGitHooksAvoidPipefailGrepQPipelines runs the shared pipefail/`grep -q`
// detector over the repo's git hooks and their libs. The core pack's shipped
// scripts already have this guard (TestCoreShippedScriptsAvoidPipefailGrepQPipelines),
// but it walks the embedded PackFS and so never covered .githooks/ — which is
// exactly where the fifth sighting of the class landed (gc-01o2l): the
// pre-push hook derived "did any .go file change?" through
// `git diff ... | grep -q .`, and on a large diff grep's early exit SIGPIPEd
// git, pipefail promoted the 141, and the hook concluded no Go changed and
// skipped the entire test fan-out.
func TestGitHooksAvoidPipefailGrepQPipelines(t *testing.T) {
	hooksDir := filepath.Join(repoRoot(t), ".githooks")
	checked := 0
	err := filepath.WalkDir(hooksDir, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(hooksDir, path)
		if err != nil {
			return err
		}
		checked++
		for _, offset := range shellpipeline.FindPipefailGrepQPipelines(data) {
			lineNumber, text := shellpipeline.DescribeLine(data, offset)
			t.Errorf(".githooks/%s:%d: %s: %s", rel, lineNumber, shellpipeline.Remedy, text)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking .githooks: %v", err)
	}
	if checked == 0 {
		t.Fatal("no hooks found under .githooks — the guard would pass vacuously")
	}
}

// TestPrePushRunsFanOutWhenGoDiffExceedsPipeBuffer is the regression test for
// gc-01o2l. It commits enough .go files that `git diff --name-only` output far
// outruns what `grep -q` drains before its early exit, which makes the old
// `| grep -q .` SIGPIPE deterministic rather than the load-sensitive flake
// that was originally observed (the reported push skipped its fan-out on a
// repeat of a push that had run it 29 minutes earlier). The hook MUST still
// reach the push-time suite: concluding "no Go changed" here means a push
// carrying hundreds of changed Go files ships with the suite never having run.
//
// The threshold is empirical, not the 64KiB pipe capacity: grep reads greedily
// before matching, so the writer only reliably blocks-then-SIGPIPEs well past
// that. Measured against the pre-fix pipeline, ~190KiB of path output failed
// 20/20 while ~69KiB passed every time. minDiffBytes keeps a wide margin.
//
// The fixture has no bazel on PATH, so push-suite.sh falls back to the
// recorded `make test-fast-parallel`; reaching make is reaching the suite.
func TestPrePushRunsFanOutWhenGoDiffExceedsPipeBuffer(t *testing.T) {
	f := newPrePushFixture(t)

	// Long paths keep the file count (and so the test's cost) down while
	// pushing the name-only output past the measured threshold.
	const (
		files        = 1200
		minDiffBytes = 128 * 1024
		nameFm       = "internal/generated/deeply/nested/package/path/with/quite/a/few/segments/to/lengthen/each/entry/padding_padding_padding_padding_generated_source_file_%05d.go"
	)
	dir := filepath.Join(f.repo, "internal", "generated", "deeply", "nested", "package", "path", "with", "quite", "a", "few", "segments", "to", "lengthen", "each", "entry")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	total := 0
	for i := 0; i < files; i++ {
		name := fmt.Sprintf(nameFm, i)
		total += len(name) + 1
		if err := os.WriteFile(filepath.Join(f.repo, name), []byte("package path\n"), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	if total < minDiffBytes {
		t.Fatalf("diff name-only output is %d bytes, need >=%d to make the SIGPIPE deterministic", total, minDiffBytes)
	}
	f.git(t, "add", "-A")
	f.git(t, "commit", "-q", "--no-verify", "-m", "many go files")
	head := f.gitOut(t, "rev-parse", "HEAD")

	code, out := f.run(t, "refs/heads/main "+head+" refs/heads/main "+f.commitOld+"\n")
	if code != 0 {
		t.Fatalf("pre-push exit = %d, want 0\n%s", code, out)
	}
	if got := f.read(t, f.makeRuns); !strings.Contains(got, prePushMakeArgs) {
		t.Fatalf("pre-push skipped the push-time suite on a %d-byte .go diff — the gate failed OPEN "+
			"and the push would ship ungated (make invocations = %q; hook output: %q)", total, got, out)
	}
}

// TestPrePushAnnouncesEverySkip covers the second half of gc-01o2l: the gate
// did not merely skip, it skipped SILENTLY. `exit 0` with no output is
// indistinguishable from "the suite ran and passed", so nothing downstream —
// human or machine — can notice a gate that failed open. Every path that
// declines to run the suite must say so on stderr.
func TestPrePushAnnouncesEverySkip(t *testing.T) {
	zero := strings.Repeat("0", 40)
	const skipNotice = "skipping the Go suite"

	for _, tc := range []struct {
		name  string
		stdin func(f *prePushFixture) string
	}{
		{name: "no refs on stdin at all", stdin: func(*prePushFixture) string { return "" }},
		{name: "branch deletion only", stdin: func(f *prePushFixture) string {
			return "refs/heads/gone " + zero + " refs/heads/gone " + f.commitOld + "\n"
		}},
		{name: "no go files changed", stdin: func(f *prePushFixture) string {
			return "refs/heads/main " + f.commitOld + " refs/heads/main " + f.commitOld + "\n"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newPrePushFixture(t)
			code, out := f.run(t, tc.stdin(f))
			if code != 0 {
				t.Fatalf("pre-push exit = %d, want 0\n%s", code, out)
			}
			if got := f.read(t, f.makeRuns); got != "" {
				t.Fatalf("expected the hook to skip the push-time suite, but it ran it: %q", got)
			}
			if !strings.Contains(out, skipNotice) {
				t.Fatalf("pre-push skipped the test suite without announcing it (want %q in the output) — a silent "+
					"exit 0 is indistinguishable from a suite that ran and passed, which is how an ungated push "+
					"goes unnoticed (gc-01o2l, gc-uz8az). Output:\n%s", skipNotice, out)
			}
		})
	}
}
