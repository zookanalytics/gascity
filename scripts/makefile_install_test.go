package scripts_test

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestMakeInstallFailsClosedWhenCopyFails(t *testing.T) {
	repoRoot := repoRoot(t)
	tmp := t.TempDir()
	t.Cleanup(func() {
		_ = filepath.WalkDir(tmp, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return nil
			}
			if d.IsDir() {
				_ = os.Chmod(path, 0o755)
			} else {
				_ = os.Chmod(path, 0o644)
			}
			return nil
		})
	})
	buildDir := filepath.Join(tmp, "build")
	installDir := filepath.Join(tmp, "install")
	binDir := filepath.Join(tmp, "bin")
	for _, dir := range []string{buildDir, installDir, binDir} {
		if err := os.Mkdir(dir, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
	}

	sourceBinary := filepath.Join(buildDir, "gc")
	if err := os.WriteFile(sourceBinary, []byte("new binary"), 0o755); err != nil {
		t.Fatalf("write source binary: %v", err)
	}
	installedBinary := filepath.Join(installDir, "gc")
	if err := os.WriteFile(installedBinary, []byte("old binary"), 0o755); err != nil {
		t.Fatalf("write installed binary: %v", err)
	}

	writeExecutable(t, filepath.Join(binDir, "cp"), `#!/usr/bin/env sh
for last do :; done
printf 'partial binary' > "$last"
exit 1
`)

	makefile, err := os.ReadFile(filepath.Join(repoRoot, "Makefile"))
	if err != nil {
		t.Fatalf("read Makefile: %v", err)
	}
	testMakefile := filepath.Join(tmp, "Makefile")
	makefileText := string(makefile)
	if !strings.Contains(makefileText, "\ninstall: check-self-contained\n") {
		t.Fatal("Makefile install target no longer depends on check-self-contained as expected")
	}
	makefileContent := strings.Replace(makefileText, "\ninstall: check-self-contained\n", "\ninstall:\n", 1)
	if err := os.WriteFile(testMakefile, []byte(makefileContent), 0o644); err != nil {
		t.Fatalf("write test Makefile: %v", err)
	}

	cmd := exec.Command("make", "--no-print-directory", "-f", testMakefile, "install",
		"BUILD_DIR="+buildDir,
		"INSTALL_DIR="+installDir,
		"BINARY=gc",
	)
	cmd.Dir = repoRoot
	cmd.Env = append(os.Environ(),
		"PATH="+binDir+string(os.PathListSeparator)+os.Getenv("PATH"),
		"HOME="+filepath.Join(tmp, "home"),
	)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("make install succeeded after cp failure:\n%s", out)
	}

	content, readErr := os.ReadFile(installedBinary)
	if readErr != nil {
		t.Fatalf("read installed binary: %v\nmake output:\n%s", readErr, out)
	}
	if string(content) != "old binary" {
		t.Fatalf("installed binary = %q, want old binary after cp failure\nmake output:\n%s", content, out)
	}

	entries, readDirErr := os.ReadDir(installDir)
	if readDirErr != nil {
		t.Fatalf("read install dir: %v", readDirErr)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".gc.tmp.") {
			t.Fatalf("temporary install file was not cleaned up: %s\nmake output:\n%s", entry.Name(), out)
		}
	}
}

// TestMakeInstallOapiCodegenRetriesTransientGoInstallFailure pins that the
// install-oapi-codegen prerequisite of spec-ci retries a transient module-proxy
// failure the way $(GOLANGCI_LINT) has since #2041, and still fails closed once
// the retries run out. Unretried, one proxy.golang.org stream error failed the
// required Check on main at 7416d68a3e before spec-ci ever compared a spec
// (ga-6hg5mi).
func TestMakeInstallOapiCodegenRetriesTransientGoInstallFailure(t *testing.T) {
	for _, tc := range []struct {
		name         string
		failures     int
		wantSuccess  bool
		wantAttempts int
	}{
		{name: "transient", failures: 2, wantSuccess: true, wantAttempts: 3},
		{name: "persistent", failures: 99, wantSuccess: false, wantAttempts: 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repoRoot := repoRoot(t)
			tmp := t.TempDir()
			binDir := filepath.Join(tmp, "bin")
			attemptsDir := filepath.Join(tmp, "attempts")
			for _, dir := range []string{binDir, attemptsDir} {
				if err := os.Mkdir(dir, 0o755); err != nil {
					t.Fatalf("mkdir %s: %v", dir, err)
				}
			}

			// The first $GO_INSTALL_FAILURES `go install` calls fail with the
			// error proxy.golang.org returned on 7416d68a3e; any other go call
			// (the Makefile's parse-time `go env` probes) succeeds silently.
			// Builtins only, so the filtered PATH below cannot break the fake.
			writeExecutable(t, filepath.Join(binDir, "go"), `#!/bin/sh
[ "$1" = install ] || exit 0
n=1
while [ -e "$GO_INSTALL_ATTEMPTS_DIR/$n" ]; do n=$((n + 1)); done
: > "$GO_INSTALL_ATTEMPTS_DIR/$n"
if [ "$n" -le "$GO_INSTALL_FAILURES" ]; then
	echo 'golang.org/x/tools@v0.30.0: read "https://proxy.golang.org/golang.org/x/tools/@v/v0.30.0.zip": stream error: stream ID 49; INTERNAL_ERROR; received from peer' >&2
	exit 1
fi
`)
			writeExecutable(t, filepath.Join(binDir, "sleep"), "#!/bin/sh\n")

			cmd := exec.Command("make", "--no-print-directory", "-f", filepath.Join(repoRoot, "Makefile"), "install-oapi-codegen")
			cmd.Dir = repoRoot
			cmd.Env = append(os.Environ(),
				"PATH="+binDir+string(os.PathListSeparator)+pathWithoutCommand("oapi-codegen"),
				"GO_INSTALL_ATTEMPTS_DIR="+attemptsDir,
				"GO_INSTALL_FAILURES="+strconv.Itoa(tc.failures),
			)
			out, err := cmd.CombinedOutput()
			if tc.wantSuccess && err != nil {
				t.Fatalf("make install-oapi-codegen failed on a transient go install error: %v\n%s", err, out)
			}
			if !tc.wantSuccess {
				if err == nil {
					t.Fatalf("make install-oapi-codegen succeeded although every go install failed:\n%s", out)
				}
				if !strings.Contains(string(out), "ERROR: failed to install oapi-codegen") {
					t.Fatalf("make output lacks the terminal install error:\n%s", out)
				}
			}

			attempts, err := os.ReadDir(attemptsDir)
			if err != nil {
				t.Fatalf("read attempts dir: %v", err)
			}
			if len(attempts) != tc.wantAttempts {
				t.Fatalf("go install attempts = %d, want %d\nmake output:\n%s", len(attempts), tc.wantAttempts, out)
			}
		})
	}
}

// pathWithoutCommand returns $PATH minus every directory holding name, so an
// installed copy cannot satisfy a Makefile's `command -v` guard and skip the
// install path under test.
func pathWithoutCommand(name string) string {
	var kept []string
	for _, dir := range filepath.SplitList(os.Getenv("PATH")) {
		if _, err := os.Stat(filepath.Join(dir, name)); err == nil {
			continue
		}
		kept = append(kept, dir)
	}
	return strings.Join(kept, string(os.PathListSeparator))
}

// golangciLintGuardTarget is the prerequisite every lint/fmt target uses to get
// a golangci-lint that matches GOLANGCI_LINT_VERSION, built with the Go the
// lint targets run it under.
const golangciLintGuardTarget = "golangci-lint-pinned"

// fakeGolangciLint reports version and the Go that built it in the layout
// golangci-lint itself uses, so the guard's parse is exercised rather than
// bypassed.
func fakeGolangciLint(version, builtWith string) string {
	return fmt.Sprintf(`#!/bin/sh
if [ "$1" = "version" ]; then
	echo "golangci-lint has version %s built with %s from (unknown) on (unknown)"
	exit 0
fi
echo "unexpected golangci-lint invocation: $*" >&2
exit 1
`, version, builtWith)
}

// goModGo is the Go that go.mod's go directive names, as the Makefile's
// LINT_GOTOOLCHAIN default reads it: go<version> from the first go line.
func goModGo(t *testing.T, root string) string {
	t.Helper()
	for _, line := range strings.Split(readFile(t, root, "go.mod"), "\n") {
		if fields := strings.Fields(line); len(fields) >= 2 && fields[0] == "go" {
			return "go" + fields[1]
		}
	}
	t.Fatal("go.mod no longer declares a go directive")
	return ""
}

// golangciLintPin reads the pinned version out of the Makefile. CI parses the
// same line shape (ci.yml "Get golangci-lint + Go toolchain versions").
func golangciLintPin(t *testing.T, root string) string {
	t.Helper()
	makefile, err := os.ReadFile(filepath.Join(root, "Makefile"))
	if err != nil {
		t.Fatalf("read Makefile: %v", err)
	}
	for _, line := range strings.Split(string(makefile), "\n") {
		if rest, ok := strings.CutPrefix(line, "GOLANGCI_LINT_VERSION := "); ok {
			return strings.TrimSpace(rest)
		}
	}
	t.Fatal("Makefile no longer declares GOLANGCI_LINT_VERSION := <version>")
	return ""
}

type golangciLintGuardFixture struct {
	repoRoot   string
	binDir     string
	shimDir    string
	installLog string
	freshLint  string
	pin        string
	goModGo    string
}

// newGolangciLintGuardFixture points BIN_DIR at a scratch directory holding a
// golangci-lint that reports installedVersion built with installedGo (an empty
// installedVersion means none installed), and puts a `go` shim ahead of the
// real one. The shim records each `go install` with the GOTOOLCHAIN it ran
// under and copies in a binary reporting the pin instead of reaching the
// network. It answers `go env GOVERSION` as the go command does for a
// GOTOOLCHAIN of the form go<version>, by naming that version, so no test
// needs a second Go toolchain. Every other `go` invocation delegates, so the
// Makefile's parse-time `go env` calls still work.
func newGolangciLintGuardFixture(t *testing.T, installedVersion, installedGo string) *golangciLintGuardFixture {
	t.Helper()
	root := repoRoot(t)
	pin := golangciLintPin(t, root)
	modGo := goModGo(t, root)

	tmp := t.TempDir()
	binDir := filepath.Join(tmp, "bin")
	shimDir := filepath.Join(tmp, "shim")
	for _, dir := range []string{binDir, shimDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
	}
	if installedVersion != "" {
		writeExecutable(t, filepath.Join(binDir, "golangci-lint"), fakeGolangciLint(installedVersion, installedGo))
	}

	freshLint := filepath.Join(tmp, "golangci-lint.fresh")
	writeExecutable(t, freshLint, fakeGolangciLint(pin, modGo))

	installLog := filepath.Join(tmp, "go-install.log")
	writeExecutable(t, filepath.Join(shimDir, "go"), fmt.Sprintf(`#!/bin/sh
if [ "$1" = "install" ]; then
	printf 'GOTOOLCHAIN=%%s %%s\n' "${GOTOOLCHAIN-unset}" "$*" >> "%s"
	cp -f "%s" "${GOBIN:?go install shim requires GOBIN}/golangci-lint"
	exit 0
fi
if [ "$#" -eq 2 ] && [ "$1" = "env" ] && [ "$2" = "GOVERSION" ]; then
	case "${GOTOOLCHAIN-}" in
	go1.*) printf '%%s\n' "$GOTOOLCHAIN"; exit 0 ;;
	esac
	echo "go env GOVERSION shim: GOTOOLCHAIN=${GOTOOLCHAIN-unset} is not go<version>" >&2
	exit 1
fi
PATH="%s"
export PATH
exec go "$@"
`, installLog, freshLint, os.Getenv("PATH")))

	return &golangciLintGuardFixture{
		repoRoot:   root,
		binDir:     binDir,
		shimDir:    shimDir,
		installLog: installLog,
		freshLint:  freshLint,
		pin:        pin,
		goModGo:    modGo,
	}
}

func (f *golangciLintGuardFixture) run(t *testing.T, extraArgs ...string) {
	t.Helper()
	if out, err := f.runResult(extraArgs...); err != nil {
		t.Fatalf("make %s: %v\n%s", golangciLintGuardTarget, err, out)
	}
}

func (f *golangciLintGuardFixture) runResult(extraArgs ...string) (string, error) {
	args := []string{"--no-print-directory", "-f", filepath.Join(f.repoRoot, "Makefile"), "BIN_DIR=" + f.binDir}
	args = append(args, extraArgs...)
	args = append(args, golangciLintGuardTarget)
	cmd := makeCommand(args...)
	cmd.Dir = f.repoRoot
	cmd.Env = make([]string, 0, len(os.Environ())+1)
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, "LINT_GOTOOLCHAIN=") {
			cmd.Env = append(cmd.Env, entry)
		}
	}
	cmd.Env = append(cmd.Env, "PATH="+f.shimDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// requireOneInstallUnder asserts the guard ran exactly one `go install` of the
// pin, under GOTOOLCHAIN=toolchain.
func (f *golangciLintGuardFixture) requireOneInstallUnder(t *testing.T, toolchain string) {
	t.Helper()
	installs := f.installs(t)
	if len(installs) != 1 {
		t.Fatalf("go install invocations = %d, want 1: %v", len(installs), installs)
	}
	if want := "GOTOOLCHAIN=" + toolchain + " "; !strings.HasPrefix(installs[0], want) {
		t.Fatalf("go install %q did not run under %s", installs[0], strings.TrimSpace(want))
	}
	if want := "@v" + f.pin; !strings.Contains(installs[0], want) {
		t.Fatalf("go install %q does not request the pin %q", installs[0], want)
	}
}

func (f *golangciLintGuardFixture) installs(t *testing.T) []string {
	t.Helper()
	log, err := os.ReadFile(f.installLog)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatalf("read go install log: %v", err)
	}
	var lines []string
	for _, line := range strings.Split(strings.TrimSpace(string(log)), "\n") {
		if line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

// requirePinnedBinaryInstalled asserts the install landed in BIN_DIR, by content:
// the shim copies in a binary reporting the pin.
func (f *golangciLintGuardFixture) requirePinnedBinaryInstalled(t *testing.T) {
	t.Helper()
	want, err := os.ReadFile(f.freshLint)
	if err != nil {
		t.Fatalf("read reference binary: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(f.binDir, "golangci-lint"))
	if err != nil {
		t.Fatalf("read installed binary: %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("binary in BIN_DIR was not replaced with the pinned build")
	}
}

// A binary left over from an earlier pin is the whole defect: a file-existence
// prerequisite makes every pin bump a silent no-op on a host that already has
// golangci-lint, so the host keeps linting with the version it happened to have.
func TestGolangciLintGuardReinstallsWhenInstalledVersionDriftsFromPin(t *testing.T) {
	root := repoRoot(t)
	fixture := newGolangciLintGuardFixture(t, "0.0.1", goModGo(t, root))

	fixture.run(t)

	fixture.requireOneInstallUnder(t, fixture.goModGo)
	fixture.requirePinnedBinaryInstalled(t)
}

func TestGolangciLintGuardInstallsWhenBinaryIsMissing(t *testing.T) {
	fixture := newGolangciLintGuardFixture(t, "", "")

	fixture.run(t)

	fixture.requireOneInstallUnder(t, fixture.goModGo)
	fixture.requirePinnedBinaryInstalled(t)
}

func TestGolangciLintGuardLeavesAPinnedBinaryAlone(t *testing.T) {
	root := repoRoot(t)
	fixture := newGolangciLintGuardFixture(t, golangciLintPin(t, root), goModGo(t, root))

	fixture.run(t)

	if installs := fixture.installs(t); len(installs) != 0 {
		t.Fatalf("guard reinstalled a binary already at the pin: %v", installs)
	}
}

// golangci-lint's formatters are compiled into it, so it formats as the gofmt
// of the Go that built it does. A binary a newer Go built can flag files that
// CI's linter, built with go.mod's Go, accepts. The pin therefore covers the Go
// that built the binary as well as its version. That Go is the one the lint
// targets run the linter under: go.mod's, unless LINT_GOTOOLCHAIN names
// another.
func TestGolangciLintGuardPinsTheGoThatBuildsTheLinter(t *testing.T) {
	root := repoRoot(t)
	modGo := goModGo(t, root)
	// otherGo stands in for any Go other than go.mod's.
	const otherGo = "go1.99.0"
	for _, tc := range []struct {
		name      string
		builtWith string
		args      []string
		// wantInstallUnder is the GOTOOLCHAIN the one reinstall runs under;
		// empty means the guard keeps the installed binary.
		wantInstallUnder string
	}{
		{name: "built with go.mod's Go", builtWith: modGo},
		{name: "built with another Go", builtWith: otherGo, wantInstallUnder: modGo},
		{name: "LINT_GOTOOLCHAIN names another Go", builtWith: modGo, args: []string{"LINT_GOTOOLCHAIN=" + otherGo}, wantInstallUnder: otherGo},
		{name: "built with the Go LINT_GOTOOLCHAIN names", builtWith: otherGo, args: []string{"LINT_GOTOOLCHAIN=" + otherGo}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newGolangciLintGuardFixture(t, golangciLintPin(t, root), tc.builtWith)

			fixture.run(t, tc.args...)

			if tc.wantInstallUnder == "" {
				if installs := fixture.installs(t); len(installs) != 0 {
					t.Fatalf("guard reinstalled a binary already at the pin and built with the lint Go: %v", installs)
				}
				return
			}
			fixture.requireOneInstallUnder(t, tc.wantInstallUnder)
			fixture.requirePinnedBinaryInstalled(t)
		})
	}
}

// When go cannot say which Go LINT_GOTOOLCHAIN selects, the guard cannot tell
// whether the installed binary is the pin, so it fails before keeping or
// replacing it.
func TestGolangciLintGuardFailsWhenTheLintGoCannotBeResolved(t *testing.T) {
	root := repoRoot(t)
	fixture := newGolangciLintGuardFixture(t, golangciLintPin(t, root), goModGo(t, root))

	out, err := fixture.runResult("LINT_GOTOOLCHAIN=unresolvable")

	if err == nil {
		t.Fatalf("guard succeeded without resolving the lint Go:\n%s", out)
	}
	if want := "cannot resolve the Go that GOTOOLCHAIN=unresolvable selects"; !strings.Contains(out, want) {
		t.Fatalf("guard output does not say %q:\n%s", want, out)
	}
	if installs := fixture.installs(t); len(installs) != 0 {
		t.Fatalf("guard installed without resolving the lint Go: %v", installs)
	}
}

// Several Makefile contract tests point GOLANGCI_LINT at a purpose-built fake
// that answers one lint invocation and nothing else. The guard manages the
// version of the binary it installs itself, so an explicitly supplied binary is
// used as given.
func TestGolangciLintGuardHonorsAnExplicitBinaryOverride(t *testing.T) {
	fixture := newGolangciLintGuardFixture(t, "", "")
	supplied := filepath.Join(t.TempDir(), "golangci-lint")
	writeExecutable(t, supplied, `#!/bin/sh
echo "unexpected golangci-lint invocation: $*" >&2
exit 1
`)

	fixture.run(t, "GOLANGCI_LINT="+supplied)

	if installs := fixture.installs(t); len(installs) != 0 {
		t.Fatalf("guard installed over an explicitly supplied binary: %v", installs)
	}
}
