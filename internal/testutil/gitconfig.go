package testutil

import (
	"os"
	"path/filepath"
	"testing"
)

// Environment variables that select which git configuration files are read.
// GIT_CONFIG_GLOBAL and GIT_CONFIG_SYSTEM are deliberately NOT in
// internal/git's gitEnvBlacklist, so pointing them at an isolated file takes
// effect even for git spawned through the production git wrapper.
const (
	envGitConfigGlobal = "GIT_CONFIG_GLOBAL"
	envGitConfigSystem = "GIT_CONFIG_SYSTEM"

	isolatedGitConfigName = ".gitconfig"
)

// isolatedGitConfigContents is the canonical seeded global git config used to
// shield tests from the host/developer/CI git configuration.
//
//   - Signing is disabled (commit.gpgsign + tag.gpgsign = false) so commits
//     succeed under `make test`'s `env -i` sandbox, which strips SSH_AUTH_SOCK:
//     inheriting a host `commit.gpgsign=true` + `gpg.format=ssh` makes every
//     commit fail with "Couldn't get agent socket?".
//   - A neutral identity and init.defaultBranch keep `git init`/`git commit`
//     deterministic without leaning on any host configuration.
//
// The config is always written to a real, writable file (never os.DevNull) so
// global config WRITES — such as ensure_beads_role's
// `git config --global beads.role maintainer` — can lock and update it instead
// of failing with "could not lock config file /dev/null".
const isolatedGitConfigContents = "" +
	"[user]\n" +
	"\tname = Gas City Test\n" +
	"\temail = gascity-test@example.invalid\n" +
	"[commit]\n" +
	"\tgpgsign = false\n" +
	"[tag]\n" +
	"\tgpgsign = false\n" +
	"[init]\n" +
	"\tdefaultBranch = main\n"

// IsolatedGitConfigContents returns the canonical contents of the isolated
// global git config. Callers that need extra keys (for example a test-specific
// beads.role) can use it as the base for their own config file.
func IsolatedGitConfigContents() string {
	return isolatedGitConfigContents
}

// WriteIsolatedGitConfig writes the isolated global git config into dir and
// returns the file path. It is the *testing.T-free building block used by
// TestMain and by helpers that construct their own subprocess environment.
func WriteIsolatedGitConfig(dir string) (string, error) {
	path := filepath.Join(dir, isolatedGitConfigName)
	if err := os.WriteFile(path, []byte(isolatedGitConfigContents), 0o644); err != nil {
		return "", err
	}
	return path, nil
}

// IsolatedGitConfigEnv returns the environment entries that point git's global
// and system configuration at the isolated file (system at os.DevNull). Use it
// when building a subprocess environment explicitly (env -i style) rather than
// mutating the ambient process environment.
func IsolatedGitConfigEnv(path string) []string {
	return []string{
		envGitConfigGlobal + "=" + path,
		envGitConfigSystem + "=" + os.DevNull,
	}
}

// IsolatedGitConfig writes the isolated global git config into a test temp dir
// and points GIT_CONFIG_GLOBAL and GIT_CONFIG_SYSTEM at it for the duration of
// the test via t.Setenv. It returns the config path for callers that also need
// to thread it into a child process environment.
//
// This is the one-call entry point for tests that exec git while inheriting the
// ambient process environment. Tests that build a subprocess env explicitly
// should instead use WriteIsolatedGitConfig together with IsolatedGitConfigEnv.
func IsolatedGitConfig(t *testing.T) string {
	t.Helper()
	path, err := WriteIsolatedGitConfig(t.TempDir())
	if err != nil {
		t.Fatalf("write isolated git config: %v", err)
	}
	t.Setenv(envGitConfigGlobal, path)
	t.Setenv(envGitConfigSystem, os.DevNull)
	return path
}
