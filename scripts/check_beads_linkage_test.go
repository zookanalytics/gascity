package scripts_test

import (
	"errors"
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

	buildInfoBDLocalReplace = "/bin/bd: go1.24.0\n" +
		"\tpath\tgithub.com/steveyegge/beads/cmd/bd\n" +
		"\tmod\tgithub.com/steveyegge/beads\tv1.2.2-0.20260812111556-4ad99760b895\n" +
		"\t=>\t/home/dev/beads\t(devel)\n"

	buildInfoBDDevel = "/bin/bd: go1.24.0\n" +
		"\tpath\tgithub.com/steveyegge/beads/cmd/bd\n" +
		"\tmod\tgithub.com/steveyegge/beads\t(devel)\n"
)

// beadsLinkageCase is one synthetic install environment for the guard.
type beadsLinkageCase struct {
	name string
	// gcBuildInfo is what `go version -m` reports for the binary being
	// installed. Empty means the command fails (stripped or non-Go binary).
	gcBuildInfo string
	// bdBuildInfo is what `go version -m` reports for the bd on PATH. Empty
	// means bd is not installed at all (CI, upstream contributor).
	bdBuildInfo string
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
// reports on, and must stay silent wherever it lacks the evidence to judge.
func TestCheckBeadsLinkageGuard(t *testing.T) {
	cases := []beadsLinkageCase{
		{
			name:        "warns when installed gc pins beads and bd is newer",
			gcBuildInfo: buildInfoGCPinned,
			bdBuildInfo: buildInfoBDPinnedNewer,
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
			wantWarning:  true,
			wantMentions: []string{"build-optimized.sh", "/home/dev/beads"},
		},
		{
			name:         "warns when installed gc pins beads and bd is a source build",
			gcBuildInfo:  buildInfoGCPinned,
			bdBuildInfo:  buildInfoBDDevel,
			wantWarning:  true,
			wantMentions: []string{"build-optimized.sh"},
		},
		{
			name:         "warns when a replace pins a versioned fork module",
			gcBuildInfo:  buildInfoGCForkPinned,
			bdBuildInfo:  buildInfoBDPinnedNewer,
			wantWarning:  true,
			wantMentions: []string{"v0.0.0-20260625154543-d05de7acf095"},
		},
		{
			name:        "quiet when installed gc links the local beads source",
			gcBuildInfo: buildInfoGCLocalReplace,
			bdBuildInfo: buildInfoBDPinnedNewer,
		},
		{
			name:        "quiet when the pinned version matches bd",
			gcBuildInfo: buildInfoGCPinned,
			bdBuildInfo: buildInfoBDPinnedSame,
		},
		{
			name:        "quiet when bd is not installed",
			gcBuildInfo: buildInfoGCPinned,
		},
		{
			name:        "quiet when the Go toolchain is unavailable",
			gcBuildInfo: buildInfoGCPinned,
			bdBuildInfo: buildInfoBDPinnedNewer,
			noGo:        true,
		},
		{
			name:        "quiet when build info cannot be read",
			bdBuildInfo: buildInfoBDPinnedNewer,
		},
		{
			name:        "quiet when the binary links no beads library",
			gcBuildInfo: buildInfoGCNoBeads,
			bdBuildInfo: buildInfoBDPinnedNewer,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			output, exitCode := runBeadsLinkageGuard(t, testCase)

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
		})
	}
}

// runBeadsLinkageGuard runs the guard against a synthetic environment: a fake
// Go toolchain that serves each binary's canned `go version -m` output from a
// sibling .buildinfo file, and a fake bd whose presence on PATH is what the
// guard probes for.
func runBeadsLinkageGuard(t *testing.T, testCase beadsLinkageCase) (string, int) {
	t.Helper()
	root := repoRoot(t)
	tmp := t.TempDir()
	pathDir := filepath.Join(tmp, "bin")
	if err := os.Mkdir(pathDir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", pathDir, err)
	}

	gcBinary := filepath.Join(tmp, "gc")
	writeExecutable(t, gcBinary, "#!/usr/bin/env sh\nexit 0\n")
	if testCase.gcBuildInfo != "" {
		writeTestFile(t, gcBinary+".buildinfo", testCase.gcBuildInfo)
	}
	if testCase.bdBuildInfo != "" {
		bdBinary := filepath.Join(pathDir, "bd")
		writeExecutable(t, bdBinary, "#!/usr/bin/env sh\nexit 0\n")
		writeTestFile(t, bdBinary+".buildinfo", testCase.bdBuildInfo)
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
	return string(output), exitCode
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
