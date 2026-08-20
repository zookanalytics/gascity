package doctor

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/config"
)

// binaryFreshnessRepo builds a git repo with `count` commits on defaultBranch
// and points refs/remotes/origin/<defaultBranch> at the tip, mimicking a repo
// whose last `git fetch` saw the whole history. Returns the repo path and the
// commit SHAs oldest-first.
func binaryFreshnessRepo(t *testing.T, defaultBranch string, count int) (string, []string) {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skipf("git unavailable: %v", err)
	}
	dir := t.TempDir()
	runGitForRigRootBranchTest(t, dir, "init")
	runGitForRigRootBranchTest(t, dir, "checkout", "-b", defaultBranch)
	runGitForRigRootBranchTest(t, dir, "config", "user.name", "Binary Freshness Test")
	runGitForRigRootBranchTest(t, dir, "config", "user.email", "binary-freshness@example.invalid")

	var shas []string
	for i := 0; i < count; i++ {
		name := filepath.Join(dir, "f"+string(rune('a'+i))+".txt")
		// Seed the content with the repo's unique temp path. Git commits are
		// content-addressed, so two repos built from identical trees, messages
		// and timestamps produce identical SHAs — which would silently defeat
		// any test asserting that one repo is distinguished from another.
		if err := os.WriteFile(name, []byte(dir+"\n"), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
		runGitForRigRootBranchTest(t, dir, "add", ".")
		runGitForRigRootBranchTest(t, dir, "commit", "-m", "commit")
		shas = append(shas, gitOutForBinaryFreshnessTest(t, dir, "rev-parse", "HEAD"))
	}
	// Stand in for a completed fetch without touching the network.
	runGitForRigRootBranchTest(t, dir, "update-ref",
		"refs/remotes/origin/"+defaultBranch, shas[len(shas)-1])
	return dir, shas
}

// gitOutForBinaryFreshnessTest reads a git value via the package's own
// runGitCommand rather than constructing a command here. Mutating calls go
// through runGitForRigRootBranchTest for the same reason: the repository
// budgets os/exec construction sites per test file, so a new file that reuses
// the package's existing helpers costs nothing against that budget.
func gitOutForBinaryFreshnessTest(t *testing.T, dir string, args ...string) string {
	t.Helper()
	out, err := runGitCommand("git", dir, args...)
	if err != nil {
		t.Fatalf("git %s: %v", strings.Join(args, " "), err)
	}
	return strings.TrimSpace(out)
}

func TestBinaryFreshnessCheck_BuildAtOriginTip_OK(t *testing.T) {
	dir, shas := binaryFreshnessRepo(t, "main", 3)
	rigs := []config.Rig{{Name: "src", Path: dir, DefaultBranch: "main"}}

	r := NewBinaryFreshnessCheckForRigs(rigs, shas[len(shas)-1]).Run(&CheckContext{})

	if r.Status != StatusOK {
		t.Fatalf("status = %d (%s), want StatusOK", r.Status, r.Message)
	}
	if r.FixHint != "" {
		t.Errorf("FixHint = %q, want empty when current", r.FixHint)
	}
}

func TestBinaryFreshnessCheck_BuildBehindOrigin_WarnsAdvisoryWithCountAndTimes(t *testing.T) {
	dir, shas := binaryFreshnessRepo(t, "main", 4)
	// Built from the first commit: three commits have landed since.
	rigs := []config.Rig{{Name: "src", Path: dir, DefaultBranch: "main"}}

	r := NewBinaryFreshnessCheckForRigs(rigs, shas[0]).Run(&CheckContext{})

	if r.Status != StatusWarning {
		t.Fatalf("status = %d (%s), want StatusWarning", r.Status, r.Message)
	}
	if r.Severity != SeverityAdvisory {
		t.Fatalf("severity = %d, want SeverityAdvisory (the remedy bounces the town — operator's call)", r.Severity)
	}
	if !strings.Contains(r.Message, "3") {
		t.Errorf("message = %q, want the stranded-commit count", r.Message)
	}
	if !strings.Contains(r.Message, shas[0][:7]) {
		t.Errorf("message = %q, want the build commit", r.Message)
	}
	// Both timestamps must appear so the operator can see the size of the gap.
	if strings.Count(r.Message+strings.Join(r.Details, " "), "T") < 2 {
		t.Errorf("message/details = %q / %q, want both build and origin timestamps",
			r.Message, r.Details)
	}
	if !strings.Contains(r.FixHint, "systemctl") {
		t.Errorf("FixHint = %q, want the restart half of the remedy", r.FixHint)
	}
}

func TestBinaryFreshnessCheck_BuildAheadOfOrigin_OK(t *testing.T) {
	dir, shas := binaryFreshnessRepo(t, "main", 3)
	// Rewind the tracking ref: the build is newer than the last fetch saw.
	runGitForRigRootBranchTest(t, dir, "update-ref", "refs/remotes/origin/main", shas[0])
	rigs := []config.Rig{{Name: "src", Path: dir, DefaultBranch: "main"}}

	r := NewBinaryFreshnessCheckForRigs(rigs, shas[len(shas)-1]).Run(&CheckContext{})

	if r.Status != StatusOK {
		t.Fatalf("status = %d (%s), want StatusOK for a build ahead of origin", r.Status, r.Message)
	}
}

func TestBinaryFreshnessCheck_UnknownRevision_SkipsOK(t *testing.T) {
	dir, _ := binaryFreshnessRepo(t, "main", 2)
	rigs := []config.Rig{{Name: "src", Path: dir, DefaultBranch: "main"}}

	r := NewBinaryFreshnessCheckForRigs(rigs, "").Run(&CheckContext{})

	if r.Status != StatusOK {
		t.Fatalf("status = %d (%s), want StatusOK when the build stamps no revision", r.Status, r.Message)
	}
	if !strings.Contains(r.Message, "revision") {
		t.Errorf("message = %q, want it to name the missing revision as the reason", r.Message)
	}
}

func TestBinaryFreshnessCheck_NoRigContainsBuildCommit_SkipsOK(t *testing.T) {
	dir, _ := binaryFreshnessRepo(t, "main", 2)
	rigs := []config.Rig{{Name: "src", Path: dir, DefaultBranch: "main"}}

	// A well-formed SHA that exists in no configured rig.
	r := NewBinaryFreshnessCheckForRigs(rigs, "0123456789abcdef0123456789abcdef01234567").Run(&CheckContext{})

	if r.Status != StatusOK {
		t.Fatalf("status = %d (%s), want StatusOK when no rig holds the build commit", r.Status, r.Message)
	}
}

func TestBinaryFreshnessCheck_MissingTrackingRef_SkipsOK(t *testing.T) {
	dir, shas := binaryFreshnessRepo(t, "main", 2)
	runGitForRigRootBranchTest(t, dir, "update-ref", "-d", "refs/remotes/origin/main")
	rigs := []config.Rig{{Name: "src", Path: dir, DefaultBranch: "main"}}

	r := NewBinaryFreshnessCheckForRigs(rigs, shas[0]).Run(&CheckContext{})

	if r.Status != StatusOK {
		t.Fatalf("status = %d (%s), want StatusOK when origin/main was never fetched", r.Status, r.Message)
	}
}

// A rig whose default branch is not "main" must be compared against its own
// tracking ref. The check reads rig.EffectiveDefaultBranch(), so hardcoding
// origin/main would silently compare against a ref that does not exist and
// report the scope as unfetched rather than stale.
func TestBinaryFreshnessCheck_NonMainDefaultBranch_ComparesAgainstItsOwnRef(t *testing.T) {
	dir, shas := binaryFreshnessRepo(t, "develop", 3)
	rigs := []config.Rig{{Name: "src", Path: dir, DefaultBranch: "develop"}}

	r := NewBinaryFreshnessCheckForRigs(rigs, shas[0]).Run(&CheckContext{})

	if r.Status != StatusWarning {
		t.Fatalf("status = %d (%s), want StatusWarning", r.Status, r.Message)
	}
	if !strings.Contains(r.Message, "origin/develop") {
		t.Errorf("message = %q, want it to name origin/develop", r.Message)
	}
	if strings.Contains(r.Message, "origin/main") {
		t.Errorf("message = %q, must not fall back to origin/main", r.Message)
	}
}

func TestBinaryFreshnessCheck_PicksRigContainingBuildCommit(t *testing.T) {
	other, _ := binaryFreshnessRepo(t, "main", 2)
	src, shas := binaryFreshnessRepo(t, "main", 4)
	rigs := []config.Rig{
		{Name: "other", Path: other, DefaultBranch: "main"},
		{Name: "src", Path: src, DefaultBranch: "main"},
	}

	r := NewBinaryFreshnessCheckForRigs(rigs, shas[0]).Run(&CheckContext{})

	if r.Status != StatusWarning {
		t.Fatalf("status = %d (%s), want StatusWarning from the rig that holds the commit", r.Status, r.Message)
	}
	if !strings.Contains(r.Message, "src") {
		t.Errorf("message = %q, want the owning rig named", r.Message)
	}
}
