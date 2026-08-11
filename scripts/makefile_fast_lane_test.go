package scripts_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// cmdGCImportPath is the one package the fast unit lane must never run inside
// the shared `./...` sweep budget. Under `make test` every package shares a
// single `-timeout` and cmd/gc alone needs ~838s of it, so at ~7% headroom any
// concurrent load kills the sweep on the wall clock. go test reports that as a
// package-level FAIL with zero `--- FAIL` lines, so the false red lands on
// whichever diff happened to be running and names nothing that actually broke
// (gc-kye20).
const cmdGCImportPath = "github.com/gastownhall/gascity/cmd/gc"

// TestFastLaneExcludesCmdGCFromTheSharedSweepBudget pins the exclusion itself:
// the package list `make test` sweeps must not contain cmd/gc, so cmd/gc's
// runtime is never measured against a deadline it shares with 164 other
// packages.
func TestFastLaneExcludesCmdGCFromTheSharedSweepBudget(t *testing.T) {
	for _, listVar := range []string{"UNIT_PKGS_NONCMDGC", "MAC_UNIT_PKGS", "UNIT_COVER_PKGS_NONCMDGC"} {
		t.Run(listVar, func(t *testing.T) {
			pkgs := printMakefileVar(t, listVar)
			// An undefined variable expands to the empty string, which would
			// satisfy "does not contain cmd/gc" while sweeping nothing at all.
			if len(pkgs) == 0 {
				t.Fatalf("%s is empty or undefined; an empty sweep list passes this check vacuously while testing nothing", listVar)
			}
			for _, pkg := range pkgs {
				if pkg == cmdGCImportPath {
					t.Fatalf("%s still contains %s; the fast lane would run it inside the shared sweep budget", listVar, cmdGCImportPath)
				}
			}
		})
	}
}

// TestFastLaneSweepPlusCmdGCShardsCoverEveryPackage is the other half of the
// exclusion, and the reason it is safe. Routing cmd/gc out of `./...` trades a
// shared deadline for a coverage hazard: the sweep list is now a filter, and a
// filter that drops more than it means to drops those packages silently — the
// gate stays green while testing less. Assert the union is exactly `./...`, so
// the only package the sweep may omit is the one the shard loop below runs.
func TestFastLaneSweepPlusCmdGCShardsCoverEveryPackage(t *testing.T) {
	all := printMakefileVar(t, "GO_LIST_ALL_PKGS_FOR_TEST")
	swept := printMakefileVar(t, "UNIT_PKGS_NONCMDGC")

	covered := make(map[string]bool, len(swept)+1)
	for _, pkg := range swept {
		covered[pkg] = true
	}
	// The shard loop asserted by TestFastLaneShardsCmdGCWithItsOwnBudget.
	covered[cmdGCImportPath] = true

	var missing []string
	for _, pkg := range all {
		if !covered[pkg] {
			missing = append(missing, pkg)
		}
	}
	if len(swept) == 0 {
		t.Fatal("UNIT_PKGS_NONCMDGC is empty or undefined; the sweep would run no packages at all")
	}
	if len(missing) > 0 {
		shown := missing
		if len(shown) > 10 {
			shown = shown[:10]
		}
		t.Fatalf("`make test` no longer runs %d package(s) in any lane, e.g. %s",
			len(missing), strings.Join(shown, " "))
	}
	if len(all) != len(swept)+1 {
		t.Fatalf("sweep list has %d packages and `go list ./...` has %d; want exactly one excluded (%s)",
			len(swept), len(all), cmdGCImportPath)
	}
}

// TestFastLaneShardsCmdGCWithItsOwnBudget proves the excluded package is
// actually still run, and run under a deadline of its own. Excluding cmd/gc
// without the shard loop would turn a false red into a false green, which is
// strictly worse than the bug being fixed.
func TestFastLaneShardsCmdGCWithItsOwnBudget(t *testing.T) {
	recipe := dryRunMakeTarget(t, "test")

	for _, line := range strings.Split(recipe, "\n") {
		if !strings.Contains(line, "go test") && !strings.Contains(line, "go-test-observable") {
			continue
		}
		for _, field := range strings.Fields(line) {
			if field == "./..." {
				t.Fatalf("`make test` still passes ./... to a test command, which puts cmd/gc back on the shared budget:\n%s", line)
			}
		}
	}
	if !strings.Contains(recipe, "scripts/test-go-test-shard ./cmd/gc") {
		t.Fatalf("`make test` does not shard cmd/gc via scripts/test-go-test-shard; the package would go untested:\n%s", recipe)
	}
	if !strings.Contains(recipe, "GO_TEST_TIMEOUT=") {
		t.Fatalf("`make test` cmd/gc shards carry no GO_TEST_TIMEOUT, so they inherit the shard script default rather than a budget this lane chose:\n%s", recipe)
	}
	if !strings.Contains(recipe, "GC_FAST_UNIT=1") {
		t.Fatalf("`make test` cmd/gc shards must stay on the fast-unit subset (GC_FAST_UNIT=1); the process suite is test-cmd-gc-process:\n%s", recipe)
	}

	// One shard is not sharding: it restores the whole-package deadline this
	// change exists to remove, and it also blows the argv ceiling guarded by
	// TestFastLaneShardRegexStaysUnderArgvLimit.
	if shardTotal := makefileVarInt(t, "CMD_GC_UNIT_TOTAL"); shardTotal < 2 {
		t.Fatalf("CMD_GC_UNIT_TOTAL = %d; a single shard restores the whole-package deadline this split removed", shardTotal)
	}
}

// maxArgStrLen is Linux's per-argument ceiling for exec (MAX_ARG_STRLEN, fixed
// at 32 pages). It is a separate limit from the much larger total ARG_MAX, and
// it is the one a `-run` alternation hits: one oversized argv element fails the
// exec outright with a bare "Argument list too long", before any test runs and
// naming nothing about which package or shard was at fault.
const maxArgStrLen = 32 * 4096

// argvBudgetPercent is how much of that ceiling a single shard's -run regex
// may occupy. The margin absorbs three things the source scan cannot see
// exactly: round-robin shards are near-even but not exactly even, `go test
// -list` enumerates build-tagged tests this scan may miss, and the shard count
// should be raised deliberately when cmd/gc grows rather than after a cryptic
// exec failure lands on someone's unrelated diff.
const argvBudgetPercent = 80

// TestFastLaneShardRegexStaysUnderArgvLimit guards the constraint that sets the
// floor on CMD_GC_UNIT_TOTAL. Sharding cmd/gc by test name means each shard
// passes its selection as one `-run '^(A|B|...)$'` argument; at 8231 fast-unit
// tests the un-sharded regex is ~419KB, over 3x this ceiling. The time budget
// says "shard"; this says "and not too few", and it is the constraint that
// tightens as upstream adds cmd/gc tests.
func TestFastLaneShardRegexStaysUnderArgvLimit(t *testing.T) {
	names := cmdGCTestFuncNames(t)
	if len(names) == 0 {
		t.Fatal("found no Test functions in cmd/gc; the scan below is broken, not the package")
	}

	totalBytes := 0
	for _, name := range names {
		totalBytes += len(name) + 1 // + the '|' alternation separator
	}

	shardTotal := makefileVarInt(t, "CMD_GC_UNIT_TOTAL")
	if shardTotal < 1 {
		t.Fatalf("CMD_GC_UNIT_TOTAL = %d; want a positive shard count", shardTotal)
	}
	// `^(` + alternation + `)$`
	perShard := totalBytes/shardTotal + len("^()$")
	budget := maxArgStrLen * argvBudgetPercent / 100

	if perShard > budget {
		t.Fatalf("cmd/gc -run regex is ~%d bytes per shard across %d shards, over the %d-byte budget (%d%% of the %d-byte per-argument limit): "+
			"raise CMD_GC_UNIT_TOTAL — %d tests no longer fit this few shards, and the failure mode is a bare \"Argument list too long\" with no test output",
			perShard, shardTotal, budget, argvBudgetPercent, maxArgStrLen, len(names))
	}
}

// cmdGCTestFuncNames enumerates cmd/gc's top-level Test functions from source.
// Deliberately a source scan and not `go test -list`: -list has to build the
// test binary, which is minutes of work and would push this out of the fast
// unit tier for a check whose input is just the set of declared names.
func cmdGCTestFuncNames(t *testing.T) []string {
	t.Helper()
	dir := filepath.Join(repoRoot(t), "cmd", "gc")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read cmd/gc: %v", err)
	}
	seen := make(map[string]bool)
	var names []string
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		body, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			t.Fatalf("read %s: %v", entry.Name(), err)
		}
		for _, line := range strings.Split(string(body), "\n") {
			name, ok := topLevelTestFuncName(line)
			if ok && !seen[name] {
				seen[name] = true
				names = append(names, name)
			}
		}
	}
	return names
}

// topLevelTestFuncName reports the test name declared by a `func TestXxx(` line,
// matching what `go test -list '^Test'` would enumerate.
func topLevelTestFuncName(line string) (string, bool) {
	const prefix = "func Test"
	if !strings.HasPrefix(line, prefix) {
		return "", false
	}
	open := strings.IndexByte(line, '(')
	if open < 0 {
		return "", false
	}
	name := strings.TrimSpace(line[len("func "):open])
	if name == "" || strings.ContainsAny(name, " \t[") {
		return "", false // generic or malformed declaration
	}
	return name, true
}

func makefileVarInt(t *testing.T, name string) int {
	t.Helper()
	raw := printMakefileVarValue(t, name)
	n, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		t.Fatalf("Makefile variable %s = %q, want an integer: %v", name, raw, err)
	}
	return n
}

// printMakefileVar evaluates a Makefile variable and returns its whitespace
// separated words, by appending a print target to a staged copy of the real
// Makefile (the idiom in tmpdir_default_test.go).
func printMakefileVar(t *testing.T, name string) []string {
	t.Helper()
	return strings.Fields(printMakefileVarValue(t, name))
}

func printMakefileVarValue(t *testing.T, name string) string {
	t.Helper()
	repo := repoRoot(t)
	makefile, err := os.ReadFile(filepath.Join(repo, "Makefile"))
	if err != nil {
		t.Fatalf("read Makefile: %v", err)
	}
	tmp := t.TempDir()
	stageRelocatedMakefileSiblings(t, repo, tmp)
	staged := filepath.Join(tmp, "Makefile")

	// GO_LIST_ALL_PKGS_FOR_TEST is defined only here, so the union assertion
	// above compares the sweep list against a `go list ./...` taken through the
	// same make/shell path rather than a separately-shelled one.
	content := string(makefile) + fmt.Sprintf(`
GO_LIST_ALL_PKGS_FOR_TEST = $(shell go list ./...)
.PHONY: print-makefile-var-for-test
print-makefile-var-for-test:
	@printf '%%s\n' "$(%s)"
`, name)
	if err := os.WriteFile(staged, []byte(content), 0o644); err != nil {
		t.Fatalf("write staged Makefile: %v", err)
	}

	cmd := makeCommand("--no-print-directory", "-f", staged, "print-makefile-var-for-test")
	cmd.Dir = repo
	cmd.Env = makefileProbeEnv()
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("make print-makefile-var-for-test (%s) failed: %v\n%s", name, err, out)
	}
	return strings.TrimSpace(string(out))
}

// dryRunMakeTarget returns the recipe `make` would run for a target, without
// running it. `-n` still expands variables and $(shell ...), so the returned
// text is the real command line the gate executes.
func dryRunMakeTarget(t *testing.T, target string) string {
	t.Helper()
	repo := repoRoot(t)
	cmd := makeCommand("--no-print-directory", "-n", target)
	cmd.Dir = repo
	cmd.Env = makefileProbeEnv()
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("make -n %s failed: %v\n%s", target, err, out)
	}
	return string(out)
}

func makefileProbeEnv() []string {
	return []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + os.Getenv("HOME"),
		"USER=" + os.Getenv("USER"),
		"SHELL=/bin/sh",
		"GOCACHE=" + os.Getenv("GOCACHE"),
		"GOPATH=" + os.Getenv("GOPATH"),
		"GOMODCACHE=" + os.Getenv("GOMODCACHE"),
	}
}
