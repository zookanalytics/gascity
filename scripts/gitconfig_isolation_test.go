package scripts_test

import (
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
// Forwarding an EMPTY value when the caller set nothing is deliberate and is
// the fail-safe direction: git treats an empty GIT_CONFIG_GLOBAL the same as
// /dev/null (it reads NO global config), not as "unset". So a shard script
// invoked directly by CI, with no TEST_ENV above it, still gets a host config
// that cannot inject commit.gpgsign. Tests that need a committer identity set
// it per-repo (`git config user.email ...` next to their `git commit`), so
// dropping the global config costs them nothing.
//
// Do NOT "harden" this into a hard failure when the value is empty: that would
// break the direct-invocation entry point CI relies on, and empty is already
// the safe case.
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
	if !strings.Contains(content, "gpgsign = false") {
		t.Errorf("Makefile ISOLATED_GITCONFIG no longer seeds `gpgsign = false`; that setting is the whole point of the isolated file")
	}
}
