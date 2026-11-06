package scripts_test

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestChangedFormattingScopesToTheDiff(t *testing.T) {
	t.Run("changed Go file", func(t *testing.T) {
		fixture := newPRStaticScopeFixture(t, map[string]string{
			"alpha/alpha.go":     "package alpha\n\nfunc Value() int { return 1 }\n",
			"alpha/unchanged.go": "package alpha\n\nfunc Unchanged() {}\n",
			"beta/beta.go":       "package beta\n\nfunc Value() int { return 1 }\n",
			"README.md":          "baseline\n",
		})
		writeTestFile(t, filepath.Join(fixture.repoRoot, "alpha", "alpha.go"), "package alpha\n\nfunc Value() int { return 2 }\n")

		fixture.resetCalls(t)
		if output, err := fixture.runMakeTarget("fmt-check-changed"); err != nil {
			t.Errorf("fmt-check-changed failed for one changed Go file: %v\n%s", err, output)
		}
		fixture.requireCalls(t, []string{"fmt", "--diff", "--", "alpha/alpha.go"})
	})

	t.Run("deleted Go file", func(t *testing.T) {
		fixture := newPRStaticScopeFixture(t, map[string]string{
			"alpha/delete.go": "package alpha\n\nfunc Deleted() {}\n",
			"alpha/keep.go":   "package alpha\n\nfunc Keep() {}\n",
		})
		if err := os.Remove(filepath.Join(fixture.repoRoot, "alpha", "delete.go")); err != nil {
			t.Fatalf("delete tracked Go file: %v", err)
		}

		fixture.resetCalls(t)
		if output, err := fixture.runMakeTarget("fmt-check-changed"); err != nil {
			t.Errorf("fmt-check-changed failed for a deleted Go file: %v\n%s", err, output)
		}
		fixture.requireNoCalls(t)
	})

	t.Run("cross-package rename with a spaced file name", func(t *testing.T) {
		const movedBody = `
func Moved() int {
	total := 0
	for i := 0; i < 10; i++ {
		total += i
	}
	return total
}
`
		fixture := newPRStaticScopeFixture(t, map[string]string{
			"oldpkg/keep.go":       "package oldpkg\n\nfunc Keep() {}\n",
			"oldpkg/moved file.go": "package oldpkg\n" + movedBody,
		})
		newPath := filepath.Join(fixture.repoRoot, "newpkg", "moved file.go")
		if err := os.MkdirAll(filepath.Dir(newPath), 0o755); err != nil {
			t.Fatalf("create renamed package: %v", err)
		}
		if err := os.Rename(filepath.Join(fixture.repoRoot, "oldpkg", "moved file.go"), newPath); err != nil {
			t.Fatalf("rename tracked Go file across packages: %v", err)
		}
		writeTestFile(t, newPath, "package newpkg\n"+movedBody)
		runGitFixtureCommands(t, fixture.repoRoot, fixture.commandEnv(), "git add -A")
		status := runGitFixtureCommands(t, fixture.repoRoot, fixture.commandEnv(), "git diff --name-status -M HEAD --")
		if !strings.Contains(status, "R") || !strings.Contains(status, "oldpkg/moved file.go") || !strings.Contains(status, "newpkg/moved file.go") {
			t.Fatalf("fixture is not an across-package Git rename:\n%s", status)
		}

		fixture.resetCalls(t)
		if output, err := fixture.runMakeTarget("fmt-check-changed"); err != nil {
			t.Errorf("fmt-check-changed failed for a cross-package rename: %v\n%s", err, output)
		}
		fixture.requireCalls(t, []string{"fmt", "--diff", "--", "newpkg/moved file.go"})
	})

	t.Run("newline in changed file name", func(t *testing.T) {
		const name = "alpha/line\nbreak.go"
		fixture := newPRStaticScopeFixture(t, map[string]string{
			name: "package alpha\n\nfunc Value() int { return 1 }\n",
		})
		writeTestFile(t, filepath.Join(fixture.repoRoot, name), "package alpha\n\nfunc Value() int { return 2 }\n")

		fixture.resetCalls(t)
		if output, err := fixture.runMakeTarget("fmt-check-changed"); err != nil {
			t.Errorf("fmt-check-changed failed for a newline-containing file name: %v\n%s", err, output)
		}
		fixture.requireCalls(t, []string{"fmt", "--diff", "--", name})
	})

	t.Run("invalid ref falls back to full formatting", func(t *testing.T) {
		fixture := newPRStaticScopeFixture(t, map[string]string{
			"alpha/alpha.go": "package alpha\n\nfunc Value() int { return 1 }\n",
		})
		writeTestFile(t, filepath.Join(fixture.repoRoot, "alpha", "alpha.go"), "package alpha\n\nfunc Value() int { return 2 }\n")

		fixture.resetCalls(t)
		if output, err := fixture.runMakeTargetWithRef("fmt-check-changed", "refs/heads/missing-static-base"); err != nil {
			t.Errorf("fmt-check-changed did not fail closed for an invalid ref: %v\n%s", err, output)
		}
		fixture.requireCalls(t, []string{"fmt", "--diff", "./..."})
	})

	t.Run("changed Go symlink is not formatted", func(t *testing.T) {
		fixture := newPRStaticScopeFixture(t, map[string]string{
			"alpha/alpha.go": "package alpha\n\nfunc Value() int { return 1 }\n",
		})
		outside := filepath.Join(t.TempDir(), "outside.go")
		writeTestFile(t, outside, "package outside\n")
		path := filepath.Join(fixture.repoRoot, "alpha", "alpha.go")
		if err := os.Remove(path); err != nil {
			t.Fatalf("replace Go file with symlink: %v", err)
		}
		if err := os.Symlink(outside, path); err != nil {
			t.Fatalf("create Go symlink: %v", err)
		}

		fixture.resetCalls(t)
		if output, err := fixture.runMakeTarget("fmt-check-changed"); err != nil {
			t.Errorf("fmt-check-changed failed for a Go symlink: %v\n%s", err, output)
		}
		fixture.requireNoCalls(t)
	})

	t.Run("non-Go diff", func(t *testing.T) {
		fixture := newPRStaticScopeFixture(t, map[string]string{
			"alpha/alpha.go": "package alpha\n\nfunc Value() int { return 1 }\n",
			"README.md":      "baseline\n",
		})
		writeTestFile(t, filepath.Join(fixture.repoRoot, "README.md"), "documentation only\n")

		fixture.resetCalls(t)
		if output, err := fixture.runMakeTarget("fmt-check-changed"); err != nil {
			t.Errorf("fmt-check-changed failed for a non-Go diff: %v\n%s", err, output)
		}
		fixture.requireNoCalls(t)
	})
}

// TestLintChangedBuildsNogoForChangedBazelPackages pins the pre-commit lint
// gate: the Bazel packages of the changed Go files are built for nogo's
// output group only, so the same analyzers CI runs gate the commit without
// linking binaries.
func TestLintChangedBuildsNogoForChangedBazelPackages(t *testing.T) {
	nogoBuild := []string{"build", "--keep_going", "--output_groups=nogo_fix"}

	t.Run("changed Go files select their packages", func(t *testing.T) {
		fixture := newPRStaticScopeFixture(t, map[string]string{
			"BUILD.bazel":         "",
			"root.go":             "package root\n",
			"alpha/BUILD.bazel":   "",
			"alpha/alpha.go":      "package alpha\n\nfunc Value() int { return 1 }\n",
			"alpha/alpha_test.go": "package alpha\n",
			"beta/BUILD.bazel":    "",
			"beta/beta.go":        "package beta\n",
		})
		writeTestFile(t, filepath.Join(fixture.repoRoot, "alpha", "alpha.go"), "package alpha\n\nfunc Value() int { return 2 }\n")
		writeTestFile(t, filepath.Join(fixture.repoRoot, "alpha", "alpha_test.go"), "package alpha\n\n// changed\n")
		writeTestFile(t, filepath.Join(fixture.repoRoot, "root.go"), "package root\n\n// changed\n")

		fixture.resetCalls(t)
		if output, err := fixture.runMakeTarget("lint-changed"); err != nil {
			t.Fatalf("lint-changed failed: %v\n%s", err, output)
		}
		fixture.requireBazelCalls(t, append(slices.Clone(nogoBuild), "//:all", "//alpha:all"))
	})

	t.Run("testdata Go files are not Bazel packages", func(t *testing.T) {
		fixture := newPRStaticScopeFixture(t, map[string]string{
			"alpha/BUILD.bazel":          "",
			"alpha/testdata/fixture.go":  "package fixture\n",
			"alpha/testdata/sub/deep.go": "package sub\n",
		})
		writeTestFile(t, filepath.Join(fixture.repoRoot, "alpha", "testdata", "fixture.go"), "package fixture\n\n// changed\n")
		writeTestFile(t, filepath.Join(fixture.repoRoot, "alpha", "testdata", "sub", "deep.go"), "package sub\n\n// changed\n")

		fixture.resetCalls(t)
		if output, err := fixture.runMakeTarget("lint-changed"); err != nil {
			t.Fatalf("lint-changed failed: %v\n%s", err, output)
		}
		fixture.requireBazelCalls(t)
	})

	t.Run("package without BUILD file fails closed", func(t *testing.T) {
		fixture := newPRStaticScopeFixture(t, map[string]string{
			"alpha/alpha.go": "package alpha\n",
		})
		writeTestFile(t, filepath.Join(fixture.repoRoot, "alpha", "alpha.go"), "package alpha\n\n// changed\n")

		fixture.resetCalls(t)
		output, err := fixture.runMakeTarget("lint-changed")
		if err == nil {
			t.Fatalf("lint-changed succeeded for a package without BUILD.bazel:\n%s", output)
		}
		if !strings.Contains(output, "make bazel-sync") {
			t.Errorf("lint-changed error does not point at make bazel-sync:\n%s", output)
		}
		fixture.requireBazelCalls(t)
	})

	t.Run("non-Go diff", func(t *testing.T) {
		fixture := newPRStaticScopeFixture(t, map[string]string{
			"alpha/BUILD.bazel": "",
			"alpha/alpha.go":    "package alpha\n",
			"README.md":         "baseline\n",
		})
		writeTestFile(t, filepath.Join(fixture.repoRoot, "README.md"), "documentation only\n")

		fixture.resetCalls(t)
		if output, err := fixture.runMakeTarget("lint-changed"); err != nil {
			t.Fatalf("lint-changed failed for a non-Go diff: %v\n%s", err, output)
		}
		fixture.requireBazelCalls(t)
	})
}

// TestLintChangedGoLintsAndVetsChangedPackages pins nogo's plain-Go twin, the
// pre-commit lint step on hosts without bazel: golangci-lint and go vet over
// the packages of the changed Go files lint-changed would select, and no
// Bazel.
func TestLintChangedGoLintsAndVetsChangedPackages(t *testing.T) {
	t.Run("changed Go files select their packages", func(t *testing.T) {
		fixture := newPRStaticScopeFixture(t, map[string]string{
			"root.go":                   "package root\n",
			"alpha/alpha.go":            "package alpha\n\nfunc Value() int { return 1 }\n",
			"alpha/alpha_test.go":       "package alpha\n",
			"alpha/testdata/fixture.go": "package fixture\n",
			"beta/beta.go":              "package beta\n",
		})
		writeTestFile(t, filepath.Join(fixture.repoRoot, "alpha", "alpha.go"), "package alpha\n\nfunc Value() int { return 2 }\n")
		writeTestFile(t, filepath.Join(fixture.repoRoot, "alpha", "alpha_test.go"), "package alpha\n\n// changed\n")
		writeTestFile(t, filepath.Join(fixture.repoRoot, "alpha", "testdata", "fixture.go"), "package fixture\n\n// changed\n")
		writeTestFile(t, filepath.Join(fixture.repoRoot, "root.go"), "package root\n\n// changed\n")

		fixture.resetCalls(t)
		vetLog, env := fixture.withVetRecorder(t)
		if output, err := fixture.runMakeTargetWithEnv("lint-changed-go", env...); err != nil {
			t.Fatalf("lint-changed-go failed: %v\n%s", err, output)
		}
		fixture.requireCalls(t, []string{"run", ".", "./alpha"})
		requireFramedCalls(t, "go vet", readFramedCalls(t, vetLog, "go vet"), [][]string{{"vet", ".", "./alpha"}})
		fixture.requireBazelCalls(t)
	})

	t.Run("non-Go diff", func(t *testing.T) {
		fixture := newPRStaticScopeFixture(t, map[string]string{
			"alpha/alpha.go": "package alpha\n",
			"README.md":      "baseline\n",
		})
		writeTestFile(t, filepath.Join(fixture.repoRoot, "README.md"), "documentation only\n")

		fixture.resetCalls(t)
		vetLog, env := fixture.withVetRecorder(t)
		if output, err := fixture.runMakeTargetWithEnv("lint-changed-go", env...); err != nil {
			t.Fatalf("lint-changed-go failed for a non-Go diff: %v\n%s", err, output)
		}
		fixture.requireNoCalls(t)
		requireFramedCalls(t, "go vet", readFramedCalls(t, vetLog, "go vet"), nil)
	})
}

// TestLintEnvPinsTheGoModToolchain: golangci-lint type-checks the standard
// library from source, so the lint targets run it under go.mod's Go rather
// than whatever go, or GOTOOLCHAIN, the host carries. LINT_GOTOOLCHAIN
// overrides the pin.
func TestLintEnvPinsTheGoModToolchain(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  []string
		want string
	}{
		// The fixture's go.mod says `go 1.23`, and its environment sets
		// GOTOOLCHAIN=local, which the pin replaces.
		{name: "go.mod's version", want: "go1.23"},
		{name: "LINT_GOTOOLCHAIN override", env: []string{"LINT_GOTOOLCHAIN=local"}, want: "local"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newPRStaticScopeFixture(t, map[string]string{
				"alpha/alpha.go": "package alpha\n",
			})
			writeExecutable(t, fixture.fakeLint, `#!/bin/sh
printf '%s\n' "${GOTOOLCHAIN-unset}" >> "$STATIC_SCOPE_LINT_LOG"
`)
			fixture.resetCalls(t)
			if output, err := fixture.runMakeTargetWithEnv("lint-golangci", tc.env...); err != nil {
				t.Fatalf("lint-golangci failed: %v\n%s", err, output)
			}
			body, err := os.ReadFile(fixture.lintLog)
			if err != nil {
				t.Fatalf("read fake golangci-lint log: %v", err)
			}
			if got := strings.TrimSpace(string(body)); got != tc.want {
				t.Errorf("golangci-lint ran with GOTOOLCHAIN=%q, want %q", got, tc.want)
			}
		})
	}
}

type prStaticScopeFixture struct {
	repoRoot           string
	productionMakefile string
	fakeLint           string
	fakeBazel          string
	lintLog            string
	bazelLog           string
	homeDir            string
	brewDir            string
}

func newPRStaticScopeFixture(t *testing.T, files map[string]string) prStaticScopeFixture {
	t.Helper()

	repo := filepath.Join(t.TempDir(), "repo")
	if err := os.MkdirAll(repo, 0o755); err != nil {
		t.Fatalf("create temporary repository: %v", err)
	}
	writeTestFile(t, filepath.Join(repo, "go.mod"), "module example.com/static-scope\n\ngo 1.23\n")
	for name, content := range files {
		writeTestFile(t, filepath.Join(repo, name), content)
	}

	toolDir := t.TempDir()
	lintLog := filepath.Join(toolDir, "golangci.calls")
	bazelLog := filepath.Join(toolDir, "bazel.calls")
	fakeLint := filepath.Join(toolDir, "golangci-lint")
	writeExecutable(t, fakeLint, framedCallRecorder("STATIC_SCOPE_LINT_LOG"))
	fakeBazel := filepath.Join(toolDir, "bazel")
	writeExecutable(t, fakeBazel, framedCallRecorder("STATIC_SCOPE_BAZEL_LOG"))

	fixture := prStaticScopeFixture{
		repoRoot:           repo,
		productionMakefile: filepath.Join(repoRoot(t), "Makefile"),
		fakeLint:           fakeLint,
		fakeBazel:          fakeBazel,
		lintLog:            lintLog,
		bazelLog:           bazelLog,
		homeDir:            t.TempDir(),
		brewDir:            offlineBrewDir(t),
	}
	setupMakefile := filepath.Join(t.TempDir(), "git-init.mk")
	writeTestFile(t, setupMakefile, `.PHONY: init
init:
	@git init -q -b main
	@git config user.email static-scope@example.invalid
	@git config user.name static-scope-test
	@git add .
	@git commit -qm baseline
`)
	cmd := makeCommand("--no-print-directory", "-C", repo, "-f", setupMakefile, "init")
	cmd.Env = fixture.commandEnv()
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("initialize temporary Git repository: %v\n%s", err, output)
	}
	return fixture
}

// framedCallRecorder is a fake tool that appends each invocation's arguments,
// NUL-framed, to the log file named by logEnv.
func framedCallRecorder(logEnv string) string {
	return `#!/bin/sh
set -eu
: "${` + logEnv + `:?}"
printf 'CALL\000' >> "$` + logEnv + `"
for arg in "$@"; do
  printf 'ARG\000%s\000' "$arg" >> "$` + logEnv + `"
done
printf 'END\000' >> "$` + logEnv + `"
`
}

func (f prStaticScopeFixture) runMakeTarget(target string) (string, error) {
	return f.runMakeTargetWithRef(target, "HEAD")
}

func (f prStaticScopeFixture) runMakeTargetWithRef(target, ref string) (string, error) {
	return f.runMake(target, ref)
}

// runMakeTargetWithEnv runs target against HEAD with env appended to the
// fixture's environment (a later entry wins).
func (f prStaticScopeFixture) runMakeTargetWithEnv(target string, env ...string) (string, error) {
	return f.runMake(target, "HEAD", env...)
}

// withVetRecorder returns the log a fake `go` records each `go vet` argv to,
// NUL-framed, and the PATH entry that puts that fake first; every other go
// subcommand runs the next go on PATH.
func (f prStaticScopeFixture) withVetRecorder(t *testing.T) (string, []string) {
	t.Helper()
	vetLog := filepath.Join(t.TempDir(), "vet.calls")
	bin := t.TempDir()
	writeExecutable(t, filepath.Join(bin, "go"), `#!/bin/sh
set -eu
if [ "${1-}" = vet ]; then
  printf 'CALL\000' >> "`+vetLog+`"
  for arg in "$@"; do
    printf 'ARG\000%s\000' "$arg" >> "`+vetLog+`"
  done
  printf 'END\000' >> "`+vetLog+`"
  exit 0
fi
PATH="${PATH#*:}"
export PATH
exec go "$@"
`)
	return vetLog, []string{"PATH=" + bin + string(os.PathListSeparator) + os.Getenv("PATH")}
}

func (f prStaticScopeFixture) runMake(target, ref string, env ...string) (string, error) {
	cmd := makeCommand(
		"--no-print-directory",
		"-f", f.productionMakefile,
		"GOLANGCI_LINT="+f.fakeLint,
		"NOGO_BAZEL="+f.fakeBazel,
		"LINT_CHANGED_SCOPE=tracked",
		"LINT_CHANGED_REF="+ref,
		"LINT_FLAGS=",
		"SYS_USR_CGO_FALLBACK=0",
		target,
	)
	cmd.Dir = f.repoRoot
	cmd.Env = append(f.commandEnv(), env...)
	output, err := cmd.CombinedOutput()
	return string(output), err
}

func (f prStaticScopeFixture) commandEnv() []string {
	env := make([]string, 0, len(os.Environ())+7)
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if name == "PATH" ||
			name == "HOME" ||
			// A forwarded GOROOT can pair a 1.2x driver with a different
			// toolchain's compile binaries (bazel forwards GOROOT for its own
			// type-checking tests); the real go must resolve its own.
			name == "GOROOT" ||
			name == "STATIC_SCOPE_LINT_LOG" ||
			name == "STATIC_SCOPE_BAZEL_LOG" ||
			name == "SYS_USR_CGO_FALLBACK" ||
			name == "GOFLAGS" ||
			name == "GOENV" ||
			name == "GOWORK" ||
			name == "LINT_FLAGS" ||
			name == "LINT_GOTOOLCHAIN" ||
			name == "GIT_CONFIG" ||
			strings.HasPrefix(name, "GIT_CONFIG_") {
			continue
		}
		env = append(env, entry)
	}
	return append(env,
		"PATH="+f.brewDir+string(os.PathListSeparator)+os.Getenv("PATH"),
		"HOME="+f.homeDir,
		"STATIC_SCOPE_LINT_LOG="+f.lintLog,
		"STATIC_SCOPE_BAZEL_LOG="+f.bazelLog,
		"SYS_USR_CGO_FALLBACK=0",
		"GOFLAGS=-mod=readonly",
		"GOENV=off",
		"GOWORK=off",
		"GOTOOLCHAIN=local",
		"GIT_CONFIG_NOSYSTEM=1",
		"GIT_CONFIG_GLOBAL=/dev/null",
	)
}

func (f prStaticScopeFixture) resetCalls(t *testing.T) {
	t.Helper()
	for label, path := range map[string]string{"golangci": f.lintLog, "bazel": f.bazelLog} {
		if err := os.WriteFile(path, nil, 0o644); err != nil {
			t.Fatalf("reset fake %s log: %v", label, err)
		}
	}
}

func (f prStaticScopeFixture) calls(t *testing.T) [][]string {
	t.Helper()
	return readFramedCalls(t, f.lintLog, "golangci")
}

func (f prStaticScopeFixture) bazelCalls(t *testing.T) [][]string {
	t.Helper()
	return readFramedCalls(t, f.bazelLog, "bazel")
}

func readFramedCalls(t *testing.T, path, label string) [][]string {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatalf("read fake %s log: %v", label, err)
	}
	if len(body) == 0 {
		return nil
	}
	fields := bytes.Split(body, []byte{0})
	if len(fields) > 0 && len(fields[len(fields)-1]) == 0 {
		fields = fields[:len(fields)-1]
	}
	calls := make([][]string, 0)
	for index := 0; index < len(fields); {
		if string(fields[index]) != "CALL" {
			t.Fatalf("malformed fake %s log token %q at %d", label, fields[index], index)
		}
		index++
		call := make([]string, 0)
		for {
			if index >= len(fields) {
				t.Fatalf("unterminated fake %s call", label)
			}
			switch string(fields[index]) {
			case "END":
				index++
				calls = append(calls, call)
				goto nextCall
			case "ARG":
				if index+1 >= len(fields) {
					t.Fatalf("missing fake %s argument after token %d", label, index)
				}
				call = append(call, string(fields[index+1]))
				index += 2
			default:
				t.Fatalf("malformed fake %s call token %q at %d", label, fields[index], index)
			}
		}
	nextCall:
		continue
	}
	return calls
}

func (f prStaticScopeFixture) requireCalls(t *testing.T, want ...[]string) {
	t.Helper()
	requireFramedCalls(t, "golangci", f.calls(t), want)
}

func (f prStaticScopeFixture) requireBazelCalls(t *testing.T, want ...[]string) {
	t.Helper()
	requireFramedCalls(t, "bazel", f.bazelCalls(t), want)
}

func requireFramedCalls(t *testing.T, label string, got, want [][]string) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("%s calls = %v, want %v", label, got, want)
		return
	}
	for i := range want {
		if !slices.Equal(got[i], want[i]) {
			t.Errorf("%s call %d = %v, want %v", label, i, got[i], want[i])
		}
	}
}

func (f prStaticScopeFixture) requireNoCalls(t *testing.T) {
	t.Helper()
	f.requireCalls(t)
	f.requireBazelCalls(t)
}

func runGitFixtureCommands(t *testing.T, repo string, env []string, commands ...string) string {
	t.Helper()
	makefile := filepath.Join(t.TempDir(), "git-fixture.mk")
	var body strings.Builder
	body.WriteString(".PHONY: run\nrun:\n")
	for _, command := range commands {
		body.WriteString("\t@")
		body.WriteString(command)
		body.WriteByte('\n')
	}
	writeTestFile(t, makefile, body.String())
	cmd := makeCommand("--no-print-directory", "-C", repo, "-f", makefile, "run")
	cmd.Env = env
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("run Git fixture command: %v\n%s", err, output)
	}
	return string(output)
}
