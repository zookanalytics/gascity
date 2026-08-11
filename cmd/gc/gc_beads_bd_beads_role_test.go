package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestEnsureBeadsRoleGitConfigGlobal exercises the ensure_beads_role helper
// from examples/bd/assets/scripts/gc-beads-bd.sh against a real git binary.
//
// The bug being guarded against: with GIT_CONFIG_GLOBAL set but EMPTY, git
// resolves the global config file to the empty path. Reads survive that (git
// treats the empty path as /dev/null), so the helper's read-then-write guard
// always falls through to a write that dies with
//
//	error: could not write config file : <errno>
//
// The empty filename between the colon and the errno is the only clue to the
// cause, and the errno text is not even stable across systems. Decoding it
// cost a full investigation once already (gc-f7wx8), so the helper must name
// the variable itself.
//
// The remaining cases pin the guard's precision: it must fire ONLY on
// set-but-empty. An unset GIT_CONFIG_GLOBAL is the normal, correct state.
func TestEnsureBeadsRoleGitConfigGlobal(t *testing.T) {
	t.Parallel()

	if _, err := exec.LookPath("sh"); err != nil {
		t.Skip("sh not available; skipping shell-function test")
	}
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available; skipping shell-function test")
	}

	root := repoRootForLint(t)
	scriptPath := filepath.Join(root, "examples", "bd", "assets", "scripts", "gc-beads-bd.sh")
	scriptBytes, err := os.ReadFile(scriptPath)
	if err != nil {
		t.Fatalf("read script: %v", err)
	}
	fnSrc := extractShellFunction(t, string(scriptBytes), "ensure_beads_role")

	const (
		globalUnset = "\x00unset"
		globalEmpty = ""
	)

	cases := []struct {
		name string
		// global is the GIT_CONFIG_GLOBAL value for the case: globalUnset
		// removes the variable, globalEmpty sets it to the empty string, and
		// any other value is joined to the case's temp HOME.
		global string
		// seedRole, when non-empty, pre-writes beads.role into the config git
		// will read, exercising the early return.
		seedRole       string
		wantExitOK     bool
		mustContain    []string
		mustNotContain []string
		wantRole       string
	}{
		{
			name:       "set_but_empty_names_the_variable",
			global:     globalEmpty,
			wantExitOK: false,
			// The whole point of the bead: say which variable is at fault.
			mustContain: []string{"GIT_CONFIG_GLOBAL"},
			// The opaque message that used to be the only output. The guard
			// must replace it, not merely bury it in more noise.
			mustNotContain: []string{"failed to set git config beads.role"},
		},
		{
			name:       "writable_path_writes_the_role",
			global:     ".gitconfig",
			wantExitOK: true,
			wantRole:   "maintainer",
		},
		{
			name:       "unset_falls_back_to_home",
			global:     globalUnset,
			wantExitOK: true,
			wantRole:   "maintainer",
			// An unset variable is the correct state; the empty-value guard
			// must not misfire on it.
			mustNotContain: []string{"GIT_CONFIG_GLOBAL"},
		},
		{
			name:       "already_set_returns_early",
			global:     ".gitconfig",
			seedRole:   "contributor",
			wantExitOK: true,
			wantRole:   "contributor",
			// The early return happens before any write attempt.
			mustNotContain: []string{"setting git config"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			home := t.TempDir()
			env := envForGitConfigTest(home)
			globalPath := filepath.Join(home, ".gitconfig")
			switch tc.global {
			case globalUnset:
				// HOME resolution: git writes $HOME/.gitconfig.
			case globalEmpty:
				env = append(env, "GIT_CONFIG_GLOBAL=")
			default:
				globalPath = filepath.Join(home, tc.global)
				env = append(env, "GIT_CONFIG_GLOBAL="+globalPath)
			}
			if tc.seedRole != "" {
				if err := os.WriteFile(globalPath, []byte("[beads]\n\trole = "+tc.seedRole+"\n"), 0o600); err != nil {
					t.Fatalf("seed git config: %v", err)
				}
			}

			script := fnSrc + "\n" +
				"die() { printf '%s\\n' \"$*\" >&2; exit 1; }\n" +
				"ensure_beads_role\n"

			cmd := exec.Command("sh", "-c", script)
			cmd.Env = env
			var stdout, stderr bytes.Buffer
			cmd.Stdout = &stdout
			cmd.Stderr = &stderr
			runErr := cmd.Run()

			if tc.wantExitOK && runErr != nil {
				t.Fatalf("expected success, got %v\nstderr:\n%s", runErr, stderr.String())
			}
			if !tc.wantExitOK && runErr == nil {
				t.Fatalf("expected non-zero exit, got success\nstderr:\n%s", stderr.String())
			}

			out := stderr.String()
			for _, frag := range tc.mustContain {
				if !strings.Contains(out, frag) {
					t.Errorf("stderr missing %q:\n%s", frag, out)
				}
			}
			for _, frag := range tc.mustNotContain {
				if strings.Contains(out, frag) {
					t.Errorf("stderr should not contain %q:\n%s", frag, out)
				}
			}

			if tc.wantRole != "" {
				read := exec.Command("git", "config", "--global", "beads.role")
				read.Env = env
				roleOut, readErr := read.Output()
				if readErr != nil {
					t.Fatalf("read back beads.role: %v", readErr)
				}
				if got := strings.TrimSpace(string(roleOut)); got != tc.wantRole {
					t.Errorf("beads.role = %q, want %q", got, tc.wantRole)
				}
			}
		})
	}
}

// envForGitConfigTest builds a hermetic environment for a `git config
// --global` exercise: the caller's own GIT_CONFIG_GLOBAL and XDG_CONFIG_HOME
// are dropped so the case can set them (the test binary itself runs under a
// GIT_CONFIG_GLOBAL from the Makefile's TEST_ENV), HOME points at the case's
// temp dir, and the system config is neutralized so a host /etc/gitconfig
// cannot answer the read-back.
func envForGitConfigTest(home string) []string {
	env := make([]string, 0, len(os.Environ())+3)
	for _, kv := range os.Environ() {
		switch {
		case strings.HasPrefix(kv, "HOME="),
			strings.HasPrefix(kv, "GIT_CONFIG_GLOBAL="),
			strings.HasPrefix(kv, "GIT_CONFIG_SYSTEM="),
			strings.HasPrefix(kv, "XDG_CONFIG_HOME="):
			continue
		}
		env = append(env, kv)
	}
	return append(env,
		"HOME="+home,
		"XDG_CONFIG_HOME="+filepath.Join(home, ".config"),
		"GIT_CONFIG_SYSTEM=/dev/null",
	)
}
