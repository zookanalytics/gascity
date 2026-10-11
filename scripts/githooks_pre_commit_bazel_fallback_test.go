package scripts_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The pre-commit hook lints the staged Go packages with nogo (make
// lint-changed) and runs the docs sync test (make check-docs) under Bazel,
// as CI does. Where the command the Makefile would run for a step (NOGO_BAZEL
// or BAZEL, default bazel) is not on PATH, the step runs its plain-Go twin
// (make lint-changed-go, make check-docs-go) and says loudly that the twin is
// not what CI gates on.

const preCommitTwinBanner = "CI gates on."

// preCommitFixture is a scratch repository carrying the real pre-commit hook
// and beads chain, with one Go file and one Markdown file staged. make and go
// are stubs that record their argv (go also its CGO_ENABLED), so a run
// exercises the hook's control flow and nothing else.
type preCommitFixture struct {
	repo    string
	makeLog string
	goLog   string
	// stubBazel is the path of an executable bazel stub outside PATH, for
	// NOGO_BAZEL and BAZEL to name.
	stubBazel string
}

func newPreCommitFixture(t *testing.T) *preCommitFixture {
	t.Helper()
	root := repoRoot(t)
	logs := t.TempDir()
	f := &preCommitFixture{
		repo:      t.TempDir(),
		makeLog:   filepath.Join(logs, "make.log"),
		goLog:     filepath.Join(logs, "go.log"),
		stubBazel: filepath.Join(t.TempDir(), "bazel"),
	}
	writeExecutable(t, f.stubBazel, "#!/usr/bin/env sh\nexit 0\n")

	installBeadsChainForTempRepo(t, root, f.repo)
	// The real formatter needs the pinned golangci-lint. This stub drains the
	// staged file list the hook pipes in, as the real script does, so the
	// hook's printf never dies of SIGPIPE; `read` is a builtin because the
	// restricted PATH has no cat.
	if err := os.MkdirAll(filepath.Join(f.repo, "scripts"), 0o755); err != nil {
		t.Fatalf("create scripts dir: %v", err)
	}
	writeExecutable(t, filepath.Join(f.repo, "scripts", "precommit-format-staged-go"), "#!/usr/bin/env bash\nwhile IFS= read -r _; do :; done\n")
	// Every path the Go block stages after codegen must exist, or its
	// `git add` fails the hook under `set -e`.
	for _, rel := range []string{
		"internal/api/openapi.json",
		"docs/reference/schema/openapi.json",
		"docs/reference/schema/openapi.txt",
		"internal/api/genclient/client_gen.go",
		"docs/reference/schema/city-schema.json",
		"docs/reference/schema/city-schema.txt",
		"docs/reference/config.md",
		"docs/reference/cli.md",
	} {
		writeTestFile(t, filepath.Join(f.repo, rel), "{}\n")
	}
	writeTestFile(t, filepath.Join(f.repo, "main.go"), "package main\n\nfunc main() {}\n")
	writeTestFile(t, filepath.Join(f.repo, "README.md"), "fixture\n")
	f.git(t, "init", "-q")
	f.git(t, "add", "-A")
	f.git(t, "commit", "-q", "--no-verify", "-m", "init")

	writeTestFile(t, filepath.Join(f.repo, "main.go"), "package main\n\nfunc main() { println(1) }\n")
	writeTestFile(t, filepath.Join(f.repo, "README.md"), "fixture, changed\n")
	f.git(t, "add", "main.go", "README.md")
	return f
}

func (f *preCommitFixture) git(t *testing.T, args ...string) {
	t.Helper()
	cmd := testCommand("git", args...)
	cmd.Dir = f.repo
	cmd.Env = append(os.Environ(),
		"GIT_AUTHOR_NAME=test", "GIT_AUTHOR_EMAIL=test@test.invalid",
		"GIT_COMMITTER_NAME=test", "GIT_COMMITTER_EMAIL=test@test.invalid",
	)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
}

// run runs the hook with PATH holding only bash, sh, env, git, xargs and the
// make, go and bd stubs, plus a bazel stub when withBazel is set. npm is
// absent, so the dashboard block only warns.
func (f *preCommitFixture) run(t *testing.T, withBazel bool, env ...string) (string, error) {
	t.Helper()
	stubs := map[string]string{
		"make": "#!/usr/bin/env sh\nprintf '%s\\n' \"$*\" >> '" + f.makeLog + "'\n",
		"go":   "#!/usr/bin/env sh\nprintf 'CGO_ENABLED=%s %s\\n' \"${CGO_ENABLED-unset}\" \"$*\" >> '" + f.goLog + "'\n",
	}
	if withBazel {
		stubs["bazel"] = "#!/usr/bin/env sh\nexit 0\n"
	}
	cmd := testCommand("bash", filepath.Join(repoRoot(t), ".githooks", "pre-commit"))
	cmd.Dir = f.repo
	cmd.Env = append([]string{
		"PATH=" + restrictedPathWithoutNpm(t, stubs),
		"HOME=" + t.TempDir(),
	}, env...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func (f *preCommitFixture) logLines(t *testing.T, path string) []string {
	t.Helper()
	body, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return strings.Split(strings.TrimSuffix(string(body), "\n"), "\n")
}

func TestPreCommitRunsGoTwinsWhenBazelIsMissing(t *testing.T) {
	for _, tc := range []struct {
		name      string
		withBazel bool
		env       func(f *preCommitFixture) []string
		wantMake  []string
		// wantWhy lists the reason line of each banner, in order; none when
		// every step ran under bazel.
		wantWhy []string
	}{
		{
			name:      "bazel on PATH runs nogo and the bazel docs test",
			withBazel: true,
			wantMake:  []string{"lint-changed LINT_CHANGED_SCOPE=staged", "check-docs"},
		},
		{
			name:     "no bazel runs the go twins and says so",
			wantMake: []string{"lint-changed-go LINT_CHANGED_SCOPE=staged", "check-docs-go"},
			wantWhy:  []string{"why: bazel is not on PATH", "why: bazel is not on PATH"},
		},
		{
			name: "NOGO_BAZEL and BAZEL name a bazel off PATH",
			env: func(f *preCommitFixture) []string {
				return []string{"NOGO_BAZEL=" + f.stubBazel, "BAZEL=" + f.stubBazel}
			},
			wantMake: []string{"lint-changed LINT_CHANGED_SCOPE=staged", "check-docs"},
		},
		{
			name:      "a missing NOGO_BAZEL falls back for lint alone",
			withBazel: true,
			env: func(*preCommitFixture) []string {
				return []string{"NOGO_BAZEL=/nonexistent/bazel"}
			},
			wantMake: []string{"lint-changed-go LINT_CHANGED_SCOPE=staged", "check-docs"},
			wantWhy:  []string{"why: /nonexistent/bazel is not on PATH"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newPreCommitFixture(t)
			var env []string
			if tc.env != nil {
				env = tc.env(f)
			}
			out, err := f.run(t, tc.withBazel, env...)
			if err != nil {
				t.Fatalf("pre-commit failed: %v\n%s", err, out)
			}
			if got := f.logLines(t, f.makeLog); strings.Join(got, "\n") != strings.Join(tc.wantMake, "\n") {
				t.Errorf("make ran %q, want %q\n%s", got, tc.wantMake, out)
			}
			if got := strings.Count(out, preCommitTwinBanner); got != len(tc.wantWhy) {
				t.Errorf("printed %d go-twin banners, want %d:\n%s", got, len(tc.wantWhy), out)
			}
			rest := out
			for _, why := range tc.wantWhy {
				i := strings.Index(rest, why)
				if i < 0 {
					t.Errorf("output lacks %q in order:\n%s", why, out)
					break
				}
				rest = rest[i+len(why):]
			}
			if len(tc.wantWhy) > 0 {
				for _, want := range []string{"CI runs bazel test //...", "fix: install bazelisk (engdocs/bazel-quickstart.md)"} {
					if !strings.Contains(out, want) {
						t.Errorf("go-twin banner lacks %q:\n%s", want, out)
					}
				}
			}
		})
	}
}

// TestPreCommitBuildsCodegenWithoutCgo: genspec, gen-client (through go
// generate) and genschema build with cgo off, so their transitive ICU
// dependency never needs ICU headers on the committing host.
func TestPreCommitBuildsCodegenWithoutCgo(t *testing.T) {
	f := newPreCommitFixture(t)
	out, err := f.run(t, true)
	if err != nil {
		t.Fatalf("pre-commit failed: %v\n%s", err, out)
	}
	got := f.logLines(t, f.goLog)
	for _, want := range []string{
		"CGO_ENABLED=0 run ./cmd/genspec",
		"CGO_ENABLED=0 generate ./internal/api/genclient",
		"CGO_ENABLED=0 run ./cmd/genschema",
	} {
		found := false
		for _, line := range got {
			if line == want {
				found = true
			}
		}
		if !found {
			t.Errorf("go was not called as %q; calls:\n%s", want, strings.Join(got, "\n"))
		}
	}
}
