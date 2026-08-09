package scripts_test

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// gitConfigIsolationVars are the two variables that carry the Makefile's
// git-config isolation (Makefile TEST_ENV, ISOLATED_GITCONFIG) down to the test
// binaries. TEST_ENV runs `env -i` and deliberately does NOT allowlist
// SSH_AUTH_SOCK, so a host ~/.gitconfig with commit.gpgsign=true +
// gpg.format=ssh makes every test that execs `git commit` die with
// "error: Couldn't get agent socket?" / "fatal: failed to write commit object".
// Pointing GIT_CONFIG_GLOBAL at the seeded gpgsign=false file neutralizes that
// for every test binary at once.
var gitConfigIsolationVars = []string{"GIT_CONFIG_GLOBAL", "GIT_CONFIG_SYSTEM"}

// testFanoutEnvScripts lists every script in the local/CI test fan-out that
// re-execs jobs through its OWN `env -i` with a hardcoded allowlist. Each such
// allowlist is a second (or third) isolation boundary below the Makefile's
// TEST_ENV: a variable missing here is silently dropped on the way to `go test`,
// no matter what TEST_ENV set. That is gc-fzl4 — the allowlists forwarded
// HOME but not GIT_CONFIG_GLOBAL, so git fell back to the host ~/.gitconfig and
// 47 examples/gastown tests failed inside the pre-push gate on every host whose
// user has commit signing enabled.
//
// The fan-out nests, so every layer must forward independently:
//
//	make test-fast-parallel
//	  -> TEST_ENV (sets both vars)          Makefile
//	    -> test-local-parallel per-job env -i   <- layer 2
//	      -> test-go-test-shard   run_in_test_env env -i   <- layer 3 (cmd/gc shards)
//	      -> test-integration-shard        env -i          <- layer 3 (integration shards)
//
// Fixing only layer 2 leaves the cmd/gc and integration jobs broken, because
// layer 3 re-drops what layer 2 forwarded. The shard scripts are also invoked
// directly by .github/workflows/ci.yml, with no TEST_ENV above them at all.
var testFanoutEnvScripts = []string{
	"scripts/test-local-parallel",
	"scripts/test-go-test-shard",
	"scripts/test-integration-shard",
}

// TestTestFanoutScriptsForwardGitConfigIsolation guards gc-fzl4: every `env -i`
// layer in the test fan-out must forward the git-config isolation rather than
// silently dropping it and letting git fall back to the host config.
//
// The required form is the forward-or-empty idiom already used for GOENV and
// friends in these same allowlists:
//
//	GIT_CONFIG_GLOBAL="${GIT_CONFIG_GLOBAL-}"
//
// Forwarding is only half the contract. The value being forwarded must already
// be a real writable file, which is what gc_resolve_isolated_gitconfig
// guarantees before these allowlists are built —
// TestTestFanoutScriptsResolveWritableGitConfig pins that half. An EMPTY value
// is NOT the fail-safe it looks like: git treats an empty GIT_CONFIG_GLOBAL as
// /dev/null for READS (no global config at all) but resolves the WRITE
// destination to the empty path, so `git config --global <k> <v>` dies with
// "error: could not write config file :" (gc-f7wx8).
func TestTestFanoutScriptsForwardGitConfigIsolation(t *testing.T) {
	root := repoRoot(t)
	for _, relPath := range testFanoutEnvScripts {
		t.Run(relPath, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join(root, relPath))
			if err != nil {
				t.Fatalf("read %s: %v", relPath, err)
			}
			content := string(data)
			if !strings.Contains(content, "env -i") {
				t.Fatalf("%s no longer builds an `env -i` allowlist; if the fan-out changed, update testFanoutEnvScripts deliberately", relPath)
			}
			for _, v := range gitConfigIsolationVars {
				want := v + `="${` + v + `-}"`
				if !strings.Contains(content, want) {
					t.Errorf("%s does not forward %s through its `env -i` allowlist (want the literal %s).\n"+
						"Without it the Makefile's ISOLATED_GITCONFIG is discarded at this layer, git falls back to the host ~/.gitconfig, "+
						"and every test that execs `git commit` fails with \"Couldn't get agent socket?\" on a host with commit signing enabled (gc-fzl4).",
						relPath, v, want)
				}
			}
		})
	}
}

// TestMakefileTestEnvSetsGitConfigIsolation pins the source of truth the
// fan-out scripts forward. If TEST_ENV stops exporting these, forwarding them
// downstream is worthless, so the two assertions belong together.
func TestMakefileTestEnvSetsGitConfigIsolation(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(repoRoot(t), "Makefile"))
	if err != nil {
		t.Fatalf("read Makefile: %v", err)
	}
	content := string(data)
	if !strings.Contains(content, `GIT_CONFIG_GLOBAL="$(ISOLATED_GITCONFIG)"`) {
		t.Errorf("Makefile TEST_ENV no longer points GIT_CONFIG_GLOBAL at $(ISOLATED_GITCONFIG); the host ~/.gitconfig is back in play for every test binary")
	}
	if !strings.Contains(content, "GIT_CONFIG_SYSTEM=/dev/null") {
		t.Errorf("Makefile TEST_ENV no longer neutralizes GIT_CONFIG_SYSTEM")
	}
	// The seeded contents live in scripts/lib/common.sh so the Makefile and the
	// directly-invoked shard scripts cannot drift into two different "isolated"
	// configs. The Makefile must therefore derive ISOLATED_GITCONFIG from that
	// one seeder rather than inlining a second copy of the file body.
	if !strings.Contains(content, isolatedGitConfigSeeder) {
		t.Errorf("Makefile no longer derives ISOLATED_GITCONFIG from %s in %s; inlining a second copy of the seeded body lets the make path and the direct-invocation path drift apart",
			isolatedGitConfigSeeder, isolatedGitConfigLib)
	}
}

// isolatedGitConfigLib holds the two shell entry points every test entrypoint
// shares for git-config isolation, and isolatedGitConfigSeeder/Resolver name
// them. The seeder writes the canonical isolated config and prints its path
// (the Makefile's ISOLATED_GITCONFIG); the resolver exports GIT_CONFIG_GLOBAL,
// seeding only when the caller supplied nothing usable.
const (
	isolatedGitConfigLib      = "scripts/lib/common.sh"
	isolatedGitConfigSeeder   = "gc_seed_isolated_gitconfig"
	isolatedGitConfigResolver = "gc_resolve_isolated_gitconfig"
)

// TestTestFanoutScriptsResolveWritableGitConfig guards gc-f7wx8: forwarding
// GIT_CONFIG_GLOBAL is worthless if the value forwarded is the empty string.
//
// CI invokes the shard scripts directly, with no Makefile TEST_ENV above them,
// so `${GIT_CONFIG_GLOBAL-}` used to expand to "" and `env -i` handed the test
// binaries a SET-but-EMPTY variable. Reads survive that (git treats it as
// /dev/null), writes do not: gc-beads-bd's ensure_beads_role runs
// `git config --global beads.role maintainer` during `gc init`, git resolved
// the destination to the empty path, and six Integration shards failed with
// "error: could not write config file :".
//
// Every fan-out layer must therefore resolve the variable to a real writable
// file BEFORE building its `env -i` allowlist.
func TestTestFanoutScriptsResolveWritableGitConfig(t *testing.T) {
	root := repoRoot(t)
	for _, relPath := range testFanoutEnvScripts {
		t.Run(relPath, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join(root, relPath))
			if err != nil {
				t.Fatalf("read %s: %v", relPath, err)
			}
			content := string(data)
			if !strings.Contains(content, isolatedGitConfigLib) {
				t.Errorf("%s does not source %s, so it cannot call %s", relPath, isolatedGitConfigLib, isolatedGitConfigResolver)
			}
			if !strings.Contains(content, isolatedGitConfigResolver) {
				t.Errorf("%s does not call %s before building its `env -i` allowlist.\n"+
					"Without it a direct CI invocation forwards a SET-but-EMPTY GIT_CONFIG_GLOBAL, and every `git config --global` write "+
					"under that env dies with \"could not write config file :\" (gc-f7wx8).", relPath, isolatedGitConfigResolver)
			}
		})
	}
}

// TestIsolatedGitConfigResolverYieldsWritableGlobalConfig is the behavioral
// half of gc-f7wx8: whatever the caller's environment looks like, the resolver
// must leave GIT_CONFIG_GLOBAL naming a file that `git config --global` can
// actually write, while still neutralizing the host ~/.gitconfig.
func TestIsolatedGitConfigResolverYieldsWritableGlobalConfig(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		// caller is the GIT_CONFIG_GLOBAL entry the caller's env carries, or
		// nil when the caller sets the variable at all.
		caller *string
	}{
		{name: "unset", caller: nil},
		{name: "set-but-empty", caller: ptr("")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			home := t.TempDir()
			tmp := t.TempDir()
			env := []string{"PATH=" + os.Getenv("PATH"), "HOME=" + home, "TMPDIR=" + tmp}
			if tc.caller != nil {
				env = append(env, "GIT_CONFIG_GLOBAL="+*tc.caller)
			}

			resolved := runIsolatedGitConfigResolver(t, env)
			if resolved == "" {
				t.Fatalf("%s left GIT_CONFIG_GLOBAL empty; git cannot write global config through an empty path", isolatedGitConfigResolver)
			}
			if !strings.HasPrefix(resolved, tmp+string(os.PathSeparator)) {
				t.Errorf("resolved GIT_CONFIG_GLOBAL = %q, want a path under TMPDIR %q so concurrent runs and CI containers stay isolated", resolved, tmp)
			}
			body, err := os.ReadFile(resolved)
			if err != nil {
				t.Fatalf("resolved GIT_CONFIG_GLOBAL %q is not a readable file: %v", resolved, err)
			}
			if !strings.Contains(string(body), "gpgsign = false") {
				t.Errorf("seeded config %q does not disable gpgsign:\n%s\nthat setting is what keeps a host ~/.gitconfig from breaking every `git commit` under env -i (gc-fzl4)", resolved, body)
			}
			assertGlobalGitConfigIsWritable(t, home, resolved)
		})
	}

	t.Run("caller-supplied", func(t *testing.T) {
		t.Parallel()

		// The Makefile TEST_ENV path already picked a config; the resolver must
		// hand it back untouched rather than substituting its own.
		home := t.TempDir()
		supplied := filepath.Join(home, "caller-gitconfig")
		if err := os.WriteFile(supplied, []byte("[commit]\n\tgpgsign = false\n"), 0o644); err != nil {
			t.Fatalf("write caller config: %v", err)
		}
		env := []string{
			"PATH=" + os.Getenv("PATH"),
			"HOME=" + home,
			"TMPDIR=" + t.TempDir(),
			"GIT_CONFIG_GLOBAL=" + supplied,
		}
		if resolved := runIsolatedGitConfigResolver(t, env); resolved != supplied {
			t.Fatalf("resolved GIT_CONFIG_GLOBAL = %q, want the caller's own %q left untouched", resolved, supplied)
		}
	})
}

// TestIsolatedGitConfigSeederIsWritableAndIdempotent pins the Makefile's half:
// $(ISOLATED_GITCONFIG) must name a writable file, and re-seeding must be safe
// because every `make` invocation runs it.
func TestIsolatedGitConfigSeederIsWritableAndIdempotent(t *testing.T) {
	t.Parallel()

	home := t.TempDir()
	tmp := t.TempDir()
	env := []string{"PATH=" + os.Getenv("PATH"), "HOME=" + home, "TMPDIR=" + tmp}

	first := runIsolatedGitConfigShell(t, env, isolatedGitConfigSeeder)
	if first == "" {
		t.Fatalf("%s printed no path; the Makefile would set GIT_CONFIG_GLOBAL to the empty string", isolatedGitConfigSeeder)
	}
	assertGlobalGitConfigIsWritable(t, home, first)

	if second := runIsolatedGitConfigShell(t, env, isolatedGitConfigSeeder); second != first {
		t.Fatalf("%s returned %q then %q; the path must be stable across invocations", isolatedGitConfigSeeder, first, second)
	}
	body, err := os.ReadFile(first)
	if err != nil {
		t.Fatalf("read re-seeded config: %v", err)
	}
	if !strings.Contains(string(body), "gpgsign = false") {
		t.Errorf("re-seeded config lost `gpgsign = false`:\n%s", body)
	}
}

// runIsolatedGitConfigResolver runs the resolver under env and returns the
// GIT_CONFIG_GLOBAL it leaves behind.
func runIsolatedGitConfigResolver(t *testing.T, env []string) string {
	t.Helper()
	return runIsolatedGitConfigShell(t, env, isolatedGitConfigResolver+` && printf '%s' "${GIT_CONFIG_GLOBAL-}"`)
}

// runIsolatedGitConfigShell sources the shared library under env and evaluates
// script, returning its trimmed stdout.
func runIsolatedGitConfigShell(t *testing.T, env []string, script string) string {
	t.Helper()

	lib := filepath.Join(repoRoot(t), filepath.FromSlash(isolatedGitConfigLib))
	cmd := testCommand("bash", "-c", `set -euo pipefail; . "$1"; shift; eval "$@"`, "bash", lib, script)
	cmd.Env = env
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("run %q: %v\nstdout: %s\nstderr: %s", script, err, stdout.String(), stderr.String())
	}
	return strings.TrimSpace(stdout.String())
}

// assertGlobalGitConfigIsWritable proves the resolved path survives the write
// that broke the Integration shards: gc-beads-bd's `git config --global
// beads.role maintainer`.
func assertGlobalGitConfigIsWritable(t *testing.T, home, gitConfig string) {
	t.Helper()

	cmd := testCommand("git", "config", "--global", "beads.role", "maintainer")
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + home, "GIT_CONFIG_GLOBAL=" + gitConfig}
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("`git config --global beads.role maintainer` against %q failed: %v\n%s\n"+
			"gc-beads-bd runs exactly this during `gc init`; a non-writable global config fails the whole init (gc-f7wx8)", gitConfig, err, out)
	}
}

// ptr returns a pointer to v, distinguishing "set to empty" from "unset" in
// the resolver table above.
func ptr[T any](v T) *T { return &v }
