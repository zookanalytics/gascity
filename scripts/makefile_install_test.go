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

// golangciLintGuardTarget is the prerequisite every lint/fmt target uses to get
// a golangci-lint that matches GOLANGCI_LINT_VERSION.
const golangciLintGuardTarget = "golangci-lint-pinned"

// fakeGolangciLint reports version in the layout golangci-lint itself uses, so
// the guard's parse is exercised rather than bypassed.
func fakeGolangciLint(version string) string {
	return fmt.Sprintf(`#!/bin/sh
if [ "$1" = "version" ]; then
	echo "golangci-lint has version %s built with go1.26.6 from (unknown) on (unknown)"
	exit 0
fi
echo "unexpected golangci-lint invocation: $*" >&2
exit 1
`, version)
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
}

// newGolangciLintGuardFixture points BIN_DIR at a scratch directory holding a
// golangci-lint that reports installedVersion (empty means none installed), and
// puts a `go` shim ahead of the real one that records `go install` and copies in
// a binary reporting the pin instead of reaching the network. Every other `go`
// invocation delegates, so the Makefile's parse-time `go env` calls still work.
func newGolangciLintGuardFixture(t *testing.T, installedVersion string) *golangciLintGuardFixture {
	t.Helper()
	root := repoRoot(t)
	pin := golangciLintPin(t, root)

	tmp := t.TempDir()
	binDir := filepath.Join(tmp, "bin")
	shimDir := filepath.Join(tmp, "shim")
	for _, dir := range []string{binDir, shimDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
	}
	if installedVersion != "" {
		writeExecutable(t, filepath.Join(binDir, "golangci-lint"), fakeGolangciLint(installedVersion))
	}

	freshLint := filepath.Join(tmp, "golangci-lint.fresh")
	writeExecutable(t, freshLint, fakeGolangciLint(pin))

	installLog := filepath.Join(tmp, "go-install.log")
	writeExecutable(t, filepath.Join(shimDir, "go"), fmt.Sprintf(`#!/bin/sh
if [ "$1" = "install" ]; then
	printf '%%s\n' "$*" >> "%s"
	cp -f "%s" "${GOBIN:?go install shim requires GOBIN}/golangci-lint"
	exit 0
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
	}
}

func (f *golangciLintGuardFixture) run(t *testing.T, extraArgs ...string) {
	t.Helper()
	args := []string{"--no-print-directory", "-f", filepath.Join(f.repoRoot, "Makefile"), "BIN_DIR=" + f.binDir}
	args = append(args, extraArgs...)
	args = append(args, golangciLintGuardTarget)
	cmd := makeCommand(args...)
	cmd.Dir = f.repoRoot
	cmd.Env = append(os.Environ(), "PATH="+f.shimDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("make %s: %v\n%s", golangciLintGuardTarget, err, out)
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
	fixture := newGolangciLintGuardFixture(t, "0.0.1")

	fixture.run(t)

	installs := fixture.installs(t)
	if len(installs) != 1 {
		t.Fatalf("go install invocations = %d, want 1 for a stale binary: %v", len(installs), installs)
	}
	if want := "@v" + fixture.pin; !strings.Contains(installs[0], want) {
		t.Fatalf("go install %q does not request the pin %q", installs[0], want)
	}
	fixture.requirePinnedBinaryInstalled(t)
}

func TestGolangciLintGuardInstallsWhenBinaryIsMissing(t *testing.T) {
	fixture := newGolangciLintGuardFixture(t, "")

	fixture.run(t)

	if installs := fixture.installs(t); len(installs) != 1 {
		t.Fatalf("go install invocations = %d, want 1 when nothing is installed: %v", len(installs), installs)
	}
	fixture.requirePinnedBinaryInstalled(t)
}

func TestGolangciLintGuardLeavesAPinnedBinaryAlone(t *testing.T) {
	fixture := newGolangciLintGuardFixture(t, golangciLintPin(t, repoRoot(t)))

	fixture.run(t)

	if installs := fixture.installs(t); len(installs) != 0 {
		t.Fatalf("guard reinstalled a binary already at the pin: %v", installs)
	}
}

// Several Makefile contract tests point GOLANGCI_LINT at a purpose-built fake
// that answers one lint invocation and nothing else. The guard manages the
// version of the binary it installs itself, so an explicitly supplied binary is
// used as given.
func TestGolangciLintGuardHonorsAnExplicitBinaryOverride(t *testing.T) {
	fixture := newGolangciLintGuardFixture(t, "")
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
