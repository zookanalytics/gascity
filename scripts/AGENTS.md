# scripts — change guide

## Hermetic Git test config is mirrored

`Makefile`'s `TEST_ENV` and the nested `env -i` wrappers in
`scripts/test-local-parallel`, `scripts/test-go-test-shard`, and
`scripts/test-integration-shard` must all pin `GIT_CONFIG_NOSYSTEM=1` and
`GIT_CONFIG_GLOBAL=/dev/null`. Updating only the Makefile is insufficient
because each nested runner rebuilds the environment and would otherwise
restore user Git configuration through the preserved `HOME`.

## Test-env pane shell is pinned

The same four allowlists pin `SHELL=/bin/sh` and never forward the caller's
`SHELL`. A test that opens a tmux or herdr pane runs that shell in it, and the
invoking user's zsh under a fresh `HOME` (a release gate, CI) opens its new-user
wizard, which swallows the typed text and fails the test by timeout (ga-1wilql,
ga-sux0ij). Change all four together: `TestRunnerTestEnvsPinPaneShell` in
`scripts/git_test_env_test.go` fails when one drifts.
`test/acceptance/helpers/env.go` builds its own environment and still forwards
`SHELL`: its panes start with an explicit command, which tmux runs as
`$SHELL -c`, so they never reach the wizard.

## Fixture HOMEs never reach Homebrew

On macOS the Makefile and the shard runners resolve ICU with
`brew --prefix icu4c`. Under a fresh `HOME` the real brew first downloads
Homebrew's API data, tens of megabytes whose download can outlast a test's
whole wait budget. A test that runs the Makefile or a shard runner with a
temporary `HOME`
puts a brew stub ahead of the real one on `PATH`. `writeOfflineBrew` writes the
stub into a fixture's fake-tool directory, and `offlineBrewDir` makes a
directory holding it for a fixture without one. Both live in
`scripts/precommit_contract_test.go`. A fixture that fakes `uname` as Linux
never reaches brew.

## Git hooks chain to beads

Each `.githooks` hook forwards to `.githooks/lib/beads-chain.sh`. Adding a
hook that beads manages means adding its `.githooks` counterpart too —
`TestGitHooksCoverEveryBeadsManagedHook` in `scripts/` fails otherwise. Hook
ownership is explained in `CONTRIBUTING.md` ("Git hook ownership").

## Make targets run Bazel; `-go` twins are the escape hatch

`make test`, `check`, `check-all`, `check-docs`, `test-acceptance` and
`test-integration` run the `bazel test` commands `bazel.yml`'s lanes run.
Each has a plain-`go test` twin named `<target>-go`. A workflow that runs a
Go-native suite calls the `-go` name explicitly (`TestWorkflowsNameTheirTestEngine`
in `scripts/` fails on a workflow calling a bazel-backed primary name). A new
test runner gets a Bazel target first; a `go test` recipe is an extra, never
the only route. `TestMakePrimaryTargetsRunBazel` in `scripts/` pins this.

`.githooks/lib/push-suite.sh` runs `bazel test //...` at push time. Its
`make test-fast-parallel` fallback prints a banner with the reason, because
that suite is not what CI enforces; `GC_PREPUSH_SUITE=go` opts in explicitly.

`.githooks/pre-commit` does the same per step. Where the command
`NOGO_BAZEL` or `BAZEL` names (default `bazel`) is not on PATH, it runs
`make lint-changed-go` in place of `make lint-changed` and `make check-docs-go`
in place of `make check-docs`, each under the same kind of banner.
`scripts/githooks_pre_commit_bazel_fallback_test.go` pins the choice.
