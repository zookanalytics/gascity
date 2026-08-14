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

// `go version -m` fixtures. Real output is tab-separated with a leading tab on
// every module line; a replaced dependency puts the replacement on a following
// "=>" line, and a module built from a local source tree reports "(devel)".
const (
	buildInfoGCPinned = "/install/gc: go1.24.0\n" +
		"\tpath\tgithub.com/gastownhall/gascity/cmd/gc\n" +
		"\tmod\tgithub.com/gastownhall/gascity\t(devel)\n" +
		"\tdep\tgithub.com/steveyegge/beads\tv1.1.1-0.20260805093327-bf97b73749ac\n" +
		"\tdep\tgithub.com/BurntSushi/toml\tv1.4.0\n"

	buildInfoGCLocalReplace = "/install/gc: go1.24.0\n" +
		"\tpath\tgithub.com/gastownhall/gascity/cmd/gc\n" +
		"\tmod\tgithub.com/gastownhall/gascity\t(devel)\n" +
		"\tdep\tgithub.com/steveyegge/beads\tv1.1.0\n" +
		"\t=>\t/home/dev/beads\t(devel)\n"

	// A replace onto a *versioned* fork module is still a pin: the binary
	// links a published snapshot, not the local checkout bd was built from.
	buildInfoGCForkPinned = "/install/gc: go1.24.0\n" +
		"\tpath\tgithub.com/gastownhall/gascity/cmd/gc\n" +
		"\tdep\tgithub.com/steveyegge/beads\tv1.1.0\n" +
		"\t=>\tgithub.com/zookanalytics/beads\tv0.0.0-20260625154543-d05de7acf095\n"

	buildInfoGCNoBeads = "/install/gc: go1.24.0\n" +
		"\tpath\tgithub.com/gastownhall/gascity/cmd/gc\n" +
		"\tdep\tgithub.com/BurntSushi/toml\tv1.4.0\n"

	buildInfoBDPinnedNewer = "/bin/bd: go1.24.0\n" +
		"\tpath\tgithub.com/steveyegge/beads/cmd/bd\n" +
		"\tmod\tgithub.com/steveyegge/beads\tv1.2.2-0.20260812111556-4ad99760b895\t\n"

	buildInfoBDPinnedSame = "/bin/bd: go1.24.0\n" +
		"\tpath\tgithub.com/steveyegge/beads/cmd/bd\n" +
		"\tmod\tgithub.com/steveyegge/beads\tv1.1.1-0.20260805093327-bf97b73749ac\t\n"

	// The %s verb is the replaced checkout's path, filled in by the harness:
	// whether that path is a beads checkout on disk is what the guard reads.
	buildInfoBDLocalReplace = "/bin/bd: go1.24.0\n" +
		"\tpath\tgithub.com/steveyegge/beads/cmd/bd\n" +
		"\tmod\tgithub.com/steveyegge/beads\tv1.2.2-0.20260812111556-4ad99760b895\n" +
		"\t=>\t%s\t(devel)\n"

	buildInfoBDDevel = "/bin/bd: go1.24.0\n" +
		"\tpath\tgithub.com/steveyegge/beads/cmd/bd\n" +
		"\tmod\tgithub.com/steveyegge/beads\t(devel)\n"
)

// beadsCheckoutSignal is the local beads source, if any, that a synthetic
// environment provides. It decides whether the guard's advice — rebuild gc
// against your beads checkout — is even applicable on that machine.
type beadsCheckoutSignal int

const (
	// noCheckout is a machine with no local beads source at all: CI, a fresh
	// clone, an upstream contributor tracking the go.mod pin.
	noCheckout beadsCheckoutSignal = iota
	// homeCheckout is a beads checkout at $HOME/beads, the location Gas
	// Town's build-optimized.sh resolves for its `replace`.
	homeCheckout
	// bdReplaceCheckout is a beads checkout named by bd's own `replace`,
	// which is how a deployment that keeps its checkout elsewhere reveals it.
	bdReplaceCheckout
	// homeDirNotACheckout is a $HOME/beads that exists but is not the beads
	// module — a same-named directory, not a replace target.
	homeDirNotACheckout
)

// beadsLinkageCase is one synthetic install environment for the guard.
type beadsLinkageCase struct {
	name string
	// gcBuildInfo is what `go version -m` reports for the binary being
	// installed. Empty means the command fails (stripped or non-Go binary).
	gcBuildInfo string
	// bdBuildInfo is what `go version -m` reports for the bd on PATH. Empty
	// means bd is not installed at all (CI, upstream contributor). A %s verb
	// is filled with this case's replaced-checkout path.
	bdBuildInfo string
	// checkout is the local beads source this machine has, if any.
	checkout beadsCheckoutSignal
	// noGo removes the Go toolchain from PATH.
	noGo bool
	// wantWarning is whether the guard must report a skewed install.
	wantWarning bool
	// wantMentions are substrings the warning must name.
	wantMentions []string
}

// TestCheckBeadsLinkageGuard pins scripts/check-beads-linkage.sh, the
// post-install guard that reports when `make install` produced a gc linking a
// pinned beads module instead of the local beads source bd was built from
// (gc-ykvko). gc runs the beads library in-process, so that skew is real, and
// the native-store version_compat preflight cannot see it: an unconfirmable
// library version passes the check, so nothing at runtime says the binary is
// wrong. The guard is advisory by contract — it must never fail the install it
// reports on, and must stay silent wherever it lacks the evidence to judge,
// which includes every machine with no local beads checkout to rebuild against.
func TestCheckBeadsLinkageGuard(t *testing.T) {
	cases := []beadsLinkageCase{
		{
			name:        "warns when installed gc pins beads and bd is newer",
			gcBuildInfo: buildInfoGCPinned,
			bdBuildInfo: buildInfoBDPinnedNewer,
			checkout:    homeCheckout,
			wantWarning: true,
			wantMentions: []string{
				"build-optimized.sh",
				"v1.1.1-0.20260805093327-bf97b73749ac",
				"v1.2.2-0.20260812111556-4ad99760b895",
			},
		},
		{
			name:         "warns when installed gc pins beads and bd builds from a local checkout",
			gcBuildInfo:  buildInfoGCPinned,
			bdBuildInfo:  buildInfoBDLocalReplace,
			checkout:     bdReplaceCheckout,
			wantWarning:  true,
			wantMentions: []string{"build-optimized.sh"},
		},
		{
			name:         "warns when installed gc pins beads and bd is a source build",
			gcBuildInfo:  buildInfoGCPinned,
			bdBuildInfo:  buildInfoBDDevel,
			checkout:     homeCheckout,
			wantWarning:  true,
			wantMentions: []string{"build-optimized.sh"},
		},
		{
			name:         "warns when a replace pins a versioned fork module",
			gcBuildInfo:  buildInfoGCForkPinned,
			bdBuildInfo:  buildInfoBDPinnedNewer,
			checkout:     homeCheckout,
			wantWarning:  true,
			wantMentions: []string{"v0.0.0-20260625154543-d05de7acf095"},
		},
		{
			// The install every upstream contributor and CI job runs: gc
			// links the pinned beads library because that is the only thing
			// `make install` can produce, and a bd from some other published
			// version is not evidence of a mistake. Warning here would send a
			// reader after build-optimized.sh, a Gas Town script their machine
			// does not have, to rebuild against a checkout they do not have.
			name:        "quiet when bd differs but the machine has no beads checkout",
			gcBuildInfo: buildInfoGCPinned,
			bdBuildInfo: buildInfoBDPinnedNewer,
			checkout:    noCheckout,
		},
		{
			name:        "quiet when the checkout path is a directory of the same name",
			gcBuildInfo: buildInfoGCPinned,
			bdBuildInfo: buildInfoBDPinnedNewer,
			checkout:    homeDirNotACheckout,
		},
		{
			// bd names a replace whose checkout is gone: a stale path is no
			// more a replace target than no path at all.
			name:        "quiet when bd names a replace that no longer exists",
			gcBuildInfo: buildInfoGCPinned,
			bdBuildInfo: buildInfoBDLocalReplace,
			checkout:    noCheckout,
		},
		{
			name:        "quiet when installed gc links the local beads source",
			gcBuildInfo: buildInfoGCLocalReplace,
			bdBuildInfo: buildInfoBDPinnedNewer,
			checkout:    homeCheckout,
		},
		{
			name:        "quiet when the pinned version matches bd",
			gcBuildInfo: buildInfoGCPinned,
			bdBuildInfo: buildInfoBDPinnedSame,
			checkout:    homeCheckout,
		},
		{
			name:        "quiet when bd is not installed",
			gcBuildInfo: buildInfoGCPinned,
			checkout:    homeCheckout,
		},
		{
			name:        "quiet when the Go toolchain is unavailable",
			gcBuildInfo: buildInfoGCPinned,
			bdBuildInfo: buildInfoBDPinnedNewer,
			checkout:    homeCheckout,
			noGo:        true,
		},
		{
			name:        "quiet when build info cannot be read",
			bdBuildInfo: buildInfoBDPinnedNewer,
			checkout:    homeCheckout,
		},
		{
			name:        "quiet when the binary links no beads library",
			gcBuildInfo: buildInfoGCNoBeads,
			bdBuildInfo: buildInfoBDPinnedNewer,
			checkout:    homeCheckout,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			output, exitCode, checkoutPath := runBeadsLinkageGuard(t, testCase)

			// The guard is advisory: a non-zero exit would fail `make install`
			// on exactly the machines it is meant to inform.
			if exitCode != 0 {
				t.Fatalf("guard exited %d, want 0 (it must never fail an install)\noutput:\n%s", exitCode, output)
			}

			warned := strings.Contains(output, "WARNING")
			if warned != testCase.wantWarning {
				t.Fatalf("warning reported = %t, want %t\noutput:\n%s", warned, testCase.wantWarning, output)
			}
			for _, mention := range testCase.wantMentions {
				if !strings.Contains(output, mention) {
					t.Fatalf("warning does not name %q\noutput:\n%s", mention, output)
				}
			}
			// The checkout is what makes the advice actionable, so the reader
			// has to be told which one to rebuild against.
			if testCase.wantWarning && !strings.Contains(output, checkoutPath) {
				t.Fatalf("warning does not name the local beads checkout %q\noutput:\n%s", checkoutPath, output)
			}
		})
	}
}

// runBeadsLinkageGuard runs the guard against a synthetic environment: a fake
// Go toolchain that serves each binary's canned `go version -m` output from a
// sibling .buildinfo file, a fake bd whose presence on PATH is what the guard
// probes for, and whichever local beads checkout this case provides. It returns
// the guard's output, its exit code, and the checkout path the guard should
// name when it warns (empty when the case provides none).
func runBeadsLinkageGuard(t *testing.T, testCase beadsLinkageCase) (string, int, string) {
	t.Helper()
	root := repoRoot(t)
	tmp := t.TempDir()
	pathDir := filepath.Join(tmp, "bin")
	if err := os.Mkdir(pathDir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", pathDir, err)
	}

	// replacePath is what bd's `replace` names; checkoutPath is the checkout
	// the guard should find. They coincide only when bd's replace target is a
	// beads checkout that is still on disk.
	replacePath := filepath.Join(tmp, "removed", "beads")
	checkoutPath := ""
	switch testCase.checkout {
	case noCheckout:
	case homeCheckout:
		checkoutPath = writeBeadsCheckout(t, filepath.Join(tmp, "beads"))
	case bdReplaceCheckout:
		replacePath = writeBeadsCheckout(t, filepath.Join(tmp, "src", "beads"))
		checkoutPath = replacePath
	case homeDirNotACheckout:
		mkdirAllForTest(t, filepath.Join(tmp, "beads"))
	}

	gcBinary := filepath.Join(tmp, "gc")
	writeExecutable(t, gcBinary, "#!/usr/bin/env sh\nexit 0\n")
	if testCase.gcBuildInfo != "" {
		writeTestFile(t, gcBinary+".buildinfo", testCase.gcBuildInfo)
	}
	if testCase.bdBuildInfo != "" {
		bdBuildInfo := testCase.bdBuildInfo
		if strings.Contains(bdBuildInfo, "%s") {
			bdBuildInfo = fmt.Sprintf(bdBuildInfo, replacePath)
		}
		bdBinary := filepath.Join(pathDir, "bd")
		writeExecutable(t, bdBinary, "#!/usr/bin/env sh\nexit 0\n")
		writeTestFile(t, bdBinary+".buildinfo", bdBuildInfo)
	}
	if !testCase.noGo {
		// Serves build info for any binary that has a .buildinfo sibling and
		// fails like the real toolchain for anything else.
		writeExecutable(t, filepath.Join(pathDir, "go"), `#!/usr/bin/env sh
for last do :; done
if [ -f "$last.buildinfo" ]; then
	while IFS= read -r line; do printf '%s\n' "$line"; done < "$last.buildinfo"
	exit 0
fi
echo "go: no build info in $last" >&2
exit 1
`)
	}

	command := exec.Command(filepath.Join(root, "scripts", "check-beads-linkage.sh"), gcBinary)
	command.Dir = root
	command.Env = []string{
		// A deliberately narrow PATH: the fakes plus the coreutils the guard
		// needs, and neither the real go nor the real bd.
		"PATH=" + pathDir + ":/usr/bin:/bin",
		// HOME decides where the guard looks for a beads checkout, so pointing
		// it at the case's own tmp dir keeps the developer's real ~/beads out
		// of the result.
		"HOME=" + tmp,
	}
	output, err := command.CombinedOutput()
	exitCode := 0
	if err != nil {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			t.Fatalf("run guard: %v\noutput:\n%s", err, output)
		}
		exitCode = exitErr.ExitCode()
	}
	return string(output), exitCode, checkoutPath
}

// writeBeadsCheckout creates a beads source checkout at dir — a directory whose
// go.mod declares the beads module, which is what makes it a replace target —
// and returns its path.
func writeBeadsCheckout(t *testing.T, dir string) string {
	t.Helper()
	mkdirAllForTest(t, dir)
	writeTestFile(t, filepath.Join(dir, "go.mod"), "module github.com/steveyegge/beads\n\ngo 1.24\n")
	return dir
}

// mkdirAllForTest creates dir and any missing parents.
func mkdirAllForTest(t *testing.T, dir string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
}

// TestMakeInstallRunsBeadsLinkageGuard keeps the guard wired into the install
// target. The guard only prevents a repeat of gc-ykvko while `make install`
// actually runs it; an unhooked script is the same silent skew as no script.
func TestMakeInstallRunsBeadsLinkageGuard(t *testing.T) {
	root := repoRoot(t)
	makefile, err := os.ReadFile(filepath.Join(root, "Makefile"))
	if err != nil {
		t.Fatalf("read Makefile: %v", err)
	}

	recipe := installRecipe(t, string(makefile))
	if !strings.Contains(recipe, "scripts/check-beads-linkage.sh") {
		t.Fatalf("install target does not run scripts/check-beads-linkage.sh:\n%s", recipe)
	}
}

// installRecipe returns the body of the Makefile's install target.
func installRecipe(t *testing.T, makefile string) string {
	t.Helper()
	const target = "\ninstall: check-self-contained\n"
	start := strings.Index(makefile, target)
	if start < 0 {
		t.Fatal("Makefile has no `install: check-self-contained` target")
	}
	lines := strings.Split(makefile[start+len(target):], "\n")
	for index, line := range lines {
		// Recipe lines are tab-indented; the first line that is neither
		// indented nor blank ends the target.
		if line == "" || strings.HasPrefix(line, "\t") {
			continue
		}
		return strings.Join(lines[:index], "\n")
	}
	return strings.Join(lines, "\n")
}

// TestBeadsLinkageGuardTracksBeadsModulePath keeps the guard's default module
// path equal to the one the preflight checker resolves. If the fork ever
// re-homes the beads module, a guard left behind would silently find no
// dependency and pass every skewed install.
func TestBeadsLinkageGuardTracksBeadsModulePath(t *testing.T) {
	root := repoRoot(t)
	checker, err := os.ReadFile(filepath.Join(root, "internal", "beads", "contract", "preflight_checker.go"))
	if err != nil {
		t.Fatalf("read preflight checker: %v", err)
	}
	const marker = "beadsModulePath = \""
	start := strings.Index(string(checker), marker)
	if start < 0 {
		t.Fatal("preflight checker no longer declares beadsModulePath")
	}
	rest := string(checker)[start+len(marker):]
	end := strings.Index(rest, "\"")
	if end < 0 {
		t.Fatal("preflight checker declares an unterminated beadsModulePath")
	}
	modulePath := rest[:end]

	guard, err := os.ReadFile(filepath.Join(root, "scripts", "check-beads-linkage.sh"))
	if err != nil {
		t.Fatalf("read guard: %v", err)
	}
	if !strings.Contains(string(guard), modulePath) {
		t.Fatalf("guard does not default to the linked beads module path %q", modulePath)
	}
}
