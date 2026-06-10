package testutil

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// execGitInherit runs git in dir inheriting the ambient process environment
// (so t.Setenv-injected GIT_CONFIG_* take effect) and returns trimmed output.
func execGitInherit(t *testing.T, dir string, args ...string) (string, error) {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	return strings.TrimSpace(string(out)), err
}

func requireGit(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skipf("git unavailable: %v", err)
	}
}

// TestIsolatedGitConfigNeutralizesSigning proves the helper isolates the test
// from a hostile host config that signs commits, so `git commit` succeeds even
// when no SSH agent is reachable (the `make test` env -i condition that strips
// SSH_AUTH_SOCK). This is the tk-9zgnf failure mode.
func TestIsolatedGitConfigNeutralizesSigning(t *testing.T) {
	requireGit(t)

	// Simulate the developer/CI hostile global config: signing on, ssh format,
	// and no agent socket reachable.
	hostileHome := t.TempDir()
	hostileCfg := filepath.Join(hostileHome, ".gitconfig")
	hostile := "[commit]\n\tgpgsign = true\n[gpg]\n\tformat = ssh\n[user]\n\tsigningkey = /nonexistent/key\n"
	if err := os.WriteFile(hostileCfg, []byte(hostile), 0o644); err != nil {
		t.Fatalf("write hostile gitconfig: %v", err)
	}
	t.Setenv("HOME", hostileHome)
	t.Setenv("GIT_CONFIG_GLOBAL", hostileCfg)
	t.Setenv("SSH_AUTH_SOCK", "")

	// Activate isolation — this must override the hostile GIT_CONFIG_GLOBAL.
	IsolatedGitConfig(t)

	if got := os.Getenv("GIT_CONFIG_GLOBAL"); got == hostileCfg {
		t.Fatalf("GIT_CONFIG_GLOBAL still points at hostile config %q", got)
	}
	if got, _ := execGitInherit(t, hostileHome, "config", "--global", "commit.gpgsign"); got != "false" {
		t.Fatalf("commit.gpgsign = %q, want false", got)
	}

	repo := t.TempDir()
	if out, err := execGitInherit(t, repo, "init"); err != nil {
		t.Fatalf("git init: %s: %v", out, err)
	}
	// Identity comes from the isolated global config; no repo-local identity set.
	if out, err := execGitInherit(t, repo, "commit", "--allow-empty", "-m", "init"); err != nil {
		t.Fatalf("git commit should succeed under isolation (no signing): %s: %v", out, err)
	}
}

// TestIsolatedGitConfigAllowsGlobalWrite proves the isolated config is a real
// writable file (never /dev/null), so a global config WRITE — like
// ensure_beads_role's `git config --global beads.role maintainer` — succeeds.
// This is the gc-sms19 failure mode.
func TestIsolatedGitConfigAllowsGlobalWrite(t *testing.T) {
	requireGit(t)
	IsolatedGitConfig(t)

	if out, err := execGitInherit(t, t.TempDir(), "config", "--global", "beads.role", "maintainer"); err != nil {
		t.Fatalf("global config write should succeed: %s: %v", out, err)
	}
	got, err := execGitInherit(t, t.TempDir(), "config", "--global", "beads.role")
	if err != nil {
		t.Fatalf("read back beads.role: %v", err)
	}
	if got != "maintainer" {
		t.Fatalf("beads.role = %q, want maintainer", got)
	}
}

// TestWriteIsolatedGitConfig verifies the testing.T-free building block writes
// a parseable config seeded with signing disabled.
func TestWriteIsolatedGitConfig(t *testing.T) {
	requireGit(t)
	dir := t.TempDir()
	path, err := WriteIsolatedGitConfig(dir)
	if err != nil {
		t.Fatalf("WriteIsolatedGitConfig: %v", err)
	}
	if filepath.Dir(path) != dir {
		t.Fatalf("config path %q not under dir %q", path, dir)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat config: %v", err)
	}
	if info.Mode().Perm()&0o200 == 0 {
		t.Fatalf("config file %q is not writable (mode %v)", path, info.Mode())
	}
	for _, key := range []string{"commit.gpgsign", "tag.gpgsign"} {
		out, err := execGitInherit(t, dir, "config", "--file", path, "--type=bool", key)
		if err != nil {
			t.Fatalf("git config --file %s %s: %v", path, key, err)
		}
		if out != "false" {
			t.Fatalf("%s = %q, want false", key, out)
		}
	}
}

// TestIsolatedGitConfigContents documents the canonical keys the seeded config
// must carry so the contract stays visible if the format is edited.
func TestIsolatedGitConfigContents(t *testing.T) {
	contents := IsolatedGitConfigContents()
	for _, want := range []string{"gpgsign = false", "[commit]", "[tag]", "defaultBranch = main"} {
		if !strings.Contains(contents, want) {
			t.Errorf("IsolatedGitConfigContents() missing %q\n--- got ---\n%s", want, contents)
		}
	}
}

// TestIsolatedGitConfigEnv verifies the env-entry form used by helpers that
// build a subprocess environment explicitly.
func TestIsolatedGitConfigEnv(t *testing.T) {
	env := IsolatedGitConfigEnv("/tmp/x/.gitconfig")
	want := map[string]bool{
		"GIT_CONFIG_GLOBAL=/tmp/x/.gitconfig": false,
		"GIT_CONFIG_SYSTEM=" + os.DevNull:     false,
	}
	for _, e := range env {
		if _, ok := want[e]; ok {
			want[e] = true
		}
	}
	for entry, seen := range want {
		if !seen {
			t.Errorf("IsolatedGitConfigEnv missing entry %q (got %v)", entry, env)
		}
	}
}
