package scripts_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestPreCommitFormatterPreservesFileMode(t *testing.T) {
	repoRoot := repoRoot(t)
	binDir := t.TempDir()
	fakeLint := filepath.Join(binDir, "golangci-lint")
	writeExecutable(t, fakeLint, `#!/usr/bin/env bash
set -euo pipefail
if [ "$#" -ne 2 ] || [ "$1" != "fmt" ] || [ "$2" != "--stdin" ]; then
  echo "unexpected golangci-lint args: $*" >&2
  exit 2
fi
cat
printf '\n'
`)

	source := filepath.Join(t.TempDir(), "needs_format.go")
	if err := os.WriteFile(source, []byte("package main"), 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}

	cmd := exec.Command(filepath.Join(repoRoot, "scripts", "precommit-format-staged-go"))
	cmd.Dir = repoRoot
	cmd.Env = []string{
		"PATH=" + binDir + string(os.PathListSeparator) + os.Getenv("PATH"),
		"HOME=" + t.TempDir(),
		"TMPDIR=" + t.TempDir(),
	}
	cmd.Stdin = strings.NewReader(source + "\n")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("precommit formatter failed: %v\n%s", err, out)
	}

	info, err := os.Stat(source)
	if err != nil {
		t.Fatalf("stat formatted source: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o644 {
		t.Fatalf("formatted source mode = %o, want 644", got)
	}
	content, err := os.ReadFile(source)
	if err != nil {
		t.Fatalf("read formatted source: %v", err)
	}
	if string(content) != "package main\n" {
		t.Fatalf("formatted content = %q, want package main with newline", content)
	}
}

func TestTestFastParallelUsesSanitizedEnvironmentAndMachineAwareConcurrency(t *testing.T) {
	repoRoot := repoRoot(t)
	baseEnv := make([]string, 0, len(os.Environ()))
	for _, entry := range os.Environ() {
		if strings.HasPrefix(entry, "LOCAL_TEST_JOBS=") ||
			strings.HasPrefix(entry, "GC_TEST_LOCAL_CPUS=") ||
			strings.HasPrefix(entry, "GC_TEST_LOCAL_MEMORY_KIB=") ||
			strings.HasPrefix(entry, "GC_TEST_LOCAL_MEMINFO=") ||
			strings.HasPrefix(entry, "GC_TEST_LOCAL_PROC_CGROUP=") ||
			strings.HasPrefix(entry, "GC_TEST_LOCAL_CGROUP_ROOT=") {
			continue
		}
		baseEnv = append(baseEnv, entry)
	}
	tests := []struct {
		name      string
		cpus      string
		memoryKiB string
		makeArgs  []string
		wantJobs  string
		cgroup    string
		limit     string
		current   string
	}{
		{name: "large host uses automatic ceiling", cpus: "192", memoryKiB: "536870912", wantJobs: "16"},
		{name: "memory constrains fanout", cpus: "16", memoryKiB: "12582912", wantJobs: "3"},
		{name: "cpu constrains fanout", cpus: "2", memoryKiB: "67108864", wantJobs: "2"},
		{name: "small machine still runs one job", cpus: "8", memoryKiB: "2097152", wantJobs: "1"},
		{name: "unknown memory preserves safe fallback", cpus: "64", memoryKiB: "0", wantJobs: "3"},
		{name: "nested cgroup v2 ancestor constrains fanout", cpus: "16", wantJobs: "3", cgroup: "v2", limit: "12884901888", current: "0"},
		{name: "nested cgroup v1 ancestor constrains fanout", cpus: "16", wantJobs: "2", cgroup: "v1", limit: "8589934592", current: "0"},
		{name: "hybrid cgroup falls through to v1 memory controller", cpus: "16", wantJobs: "3", cgroup: "hybrid", limit: "12884901888", current: "0"},
		{name: "exhausted cgroup forces one job", cpus: "16", wantJobs: "1", cgroup: "v2", limit: "4294967296", current: "4294967296"},
		{name: "explicit override wins", cpus: "192", memoryKiB: "536870912", makeArgs: []string{"LOCAL_TEST_JOBS=7"}, wantJobs: "7"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := append([]string{"-n"}, tt.makeArgs...)
			args = append(args, "test-fast-parallel")
			cmd := exec.Command("make", args...)
			cmd.Dir = repoRoot
			cmd.Env = append(append([]string(nil), baseEnv...), "GC_TEST_LOCAL_CPUS="+tt.cpus)
			if tt.memoryKiB != "" {
				cmd.Env = append(cmd.Env, "GC_TEST_LOCAL_MEMORY_KIB="+tt.memoryKiB)
			}
			if tt.cgroup != "" {
				cmd.Env = append(cmd.Env, localTestCgroupEnv(t, tt.cgroup, tt.limit, tt.current)...)
			}
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("make -n test-fast-parallel failed: %v\n%s", err, out)
			}
			command := string(out)
			if !strings.Contains(command, "env -i") {
				t.Fatalf("test-fast-parallel recipe should use TEST_ENV env -i wrapper:\n%s", command)
			}
			if !strings.Contains(command, "./scripts/test-local-parallel fast") {
				t.Fatalf("test-fast-parallel recipe should still dispatch the sharded fast runner:\n%s", command)
			}
			wantJobAssignment := " LOCAL_TEST_JOBS=" + tt.wantJobs + " CMD_GC_PROCESS_TOTAL="
			if !strings.Contains(command, wantJobAssignment) {
				t.Fatalf("test-fast-parallel job count should be %s:\n%s", tt.wantJobs, command)
			}
		})
	}
}

func localTestCgroupEnv(t *testing.T, version, limit, current string) []string {
	t.Helper()
	root := t.TempDir()
	cgroupRoot := filepath.Join(root, "cgroup")
	procCgroup := filepath.Join(root, "proc-self-cgroup")
	meminfo := filepath.Join(root, "meminfo")
	writeTestFile(t, meminfo, "MemAvailable: 67108864 kB\n")

	var controllerRoot, procLine, limitFile, currentFile string
	switch version {
	case "v2":
		controllerRoot = cgroupRoot
		procLine = "0::/parent/child\n"
		limitFile = "memory.max"
		currentFile = "memory.current"
	case "v1":
		controllerRoot = filepath.Join(cgroupRoot, "memory")
		procLine = "5:memory:/parent/child\n"
		limitFile = "memory.limit_in_bytes"
		currentFile = "memory.usage_in_bytes"
	case "hybrid":
		controllerRoot = filepath.Join(cgroupRoot, "memory")
		procLine = "0::/unified/child\n5:memory:/parent/child\n"
		limitFile = "memory.limit_in_bytes"
		currentFile = "memory.usage_in_bytes"
	default:
		t.Fatalf("unsupported cgroup fixture version %q", version)
	}

	writeTestFile(t, procCgroup, procLine)
	if err := os.MkdirAll(filepath.Join(controllerRoot, "parent", "child"), 0o755); err != nil {
		t.Fatalf("create nested cgroup fixture: %v", err)
	}
	writeTestFile(t, filepath.Join(controllerRoot, "parent", limitFile), limit+"\n")
	writeTestFile(t, filepath.Join(controllerRoot, "parent", currentFile), current+"\n")

	return []string{
		"GC_TEST_LOCAL_MEMINFO=" + meminfo,
		"GC_TEST_LOCAL_PROC_CGROUP=" + procCgroup,
		"GC_TEST_LOCAL_CGROUP_ROOT=" + cgroupRoot,
	}
}

func TestPrePushUsesCanonicalMachineAwareConcurrency(t *testing.T) {
	repoRoot := repoRoot(t)
	script, err := os.ReadFile(filepath.Join(repoRoot, ".githooks", "pre-push"))
	if err != nil {
		t.Fatalf("read pre-push hook: %v", err)
	}
	content := string(script)
	if strings.Contains(content, `LOCAL_TEST_JOBS="${LOCAL_TEST_JOBS:-3}"`) {
		t.Fatal("pre-push hook must not replace the canonical machine-aware default with a fixed three-job cap")
	}
	if !strings.Contains(content, "exec make test-fast-parallel") {
		t.Fatal("pre-push hook must continue delegating the unchanged fast-suite inventory to make test-fast-parallel")
	}
	for _, path := range []string{"Makefile", filepath.Join("scripts", "test-local-parallel")} {
		content, err := os.ReadFile(filepath.Join(repoRoot, path))
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		if !strings.Contains(string(content), "scripts/test-local-job-count") {
			t.Fatalf("%s must use the canonical machine-aware job detector", path)
		}
	}
}

func TestNativeDoltliteBeadsTargetRunsTaggedSuite(t *testing.T) {
	repoRoot := repoRoot(t)
	makefile, err := os.ReadFile(filepath.Join(repoRoot, "Makefile"))
	if err != nil {
		t.Fatalf("read Makefile: %v", err)
	}
	if err := validateNativeDoltliteMakefile(string(makefile)); err != nil {
		t.Fatalf("test-native-doltlite-beads recipe: %v", err)
	}

	cmd := exec.Command("make", "-n", "test-native-doltlite-beads")
	cmd.Dir = repoRoot
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("make -n test-native-doltlite-beads failed: %v\n%s", err, out)
	}
	command := string(out)
	if err := validateNativeDoltliteDryRun(command); err != nil {
		t.Fatalf("make -n test-native-doltlite-beads output: %v", err)
	}
	for _, want := range []string{
		"CGO_ENABLED=0",
		"-tags gascity_native_beads",
		"-run '^TestDoltlite'",
		"./internal/beads",
	} {
		if !strings.Contains(command, want) {
			t.Fatalf("test-native-doltlite-beads recipe missing %q:\n%s", want, command)
		}
	}
	for _, banned := range []string{
		"CGO_ENABLED=1",
		"cgo,gascity_native_beads",
	} {
		if strings.Contains(command, banned) {
			t.Fatalf("test-native-doltlite-beads recipe must not contain %q (doltlite store now uses pure-Go modernc):\n%s", banned, command)
		}
	}
	assertNativeDoltliteBeadsSelectionMatchesTaggedOwners(t, repoRoot)
}

func TestLocalParallelAllowlistIncludesObservableEnv(t *testing.T) {
	repoRoot := repoRoot(t)
	script, err := os.ReadFile(filepath.Join(repoRoot, "scripts", "test-local-parallel"))
	if err != nil {
		t.Fatalf("read test-local-parallel: %v", err)
	}
	content := string(script)
	for _, key := range []string{"OBSERVABLE_TEST_LOG", "OBSERVABLE_FAILURE_LINES"} {
		if !strings.Contains(content, key+"=") {
			t.Fatalf("test-local-parallel job env should pass through %s", key)
		}
	}
	for _, key := range []string{"GC_CITY", "GC_HOME", "GC_SESSION_ID"} {
		if strings.Contains(content, key+"=") {
			t.Fatalf("test-local-parallel job env must not pass through live session env %s", key)
		}
	}
}

// TestPreCommitHookSkipHeavyMatrix exercises the agent-context skip-set
// added in gc-53c8k4. Upstream #3628/#3634 moved `make test-fast-parallel`
// out of pre-commit to pre-push entirely, so the SKIP_HEAVY gate now governs
// only `make dashboard-check dashboard-smoke`: the hook must skip the
// dashboard checks when GC_AGENT is set (or GC_PRECOMMIT_SKIP_HEAVY=1 is
// forced) and must run them otherwise. Behavioral test — runs the actual
// hook script with PATH-stubbed make/go/scripts and verifies which
// subcommands fire.
func TestPreCommitHookSkipHeavyMatrix(t *testing.T) {
	repoRoot := repoRoot(t)
	hookPath := filepath.Join(repoRoot, ".githooks", "pre-commit")

	cases := []struct {
		name        string
		env         map[string]string
		stageSpec   bool
		expectCalls []string
		forbidCalls []string
	}{
		{
			name:        "agent context skips dashboard checks",
			env:         map[string]string{"GC_AGENT": "test-agent"},
			stageSpec:   true,
			expectCalls: []string{"make vet"},
			forbidCalls: []string{"dashboard-check", "dashboard-smoke"},
		},
		{
			name:        "non-agent context runs the full validation chain",
			env:         map[string]string{},
			stageSpec:   true,
			expectCalls: []string{"make vet", "dashboard-check dashboard-smoke"},
		},
		{
			name:        "GC_PRECOMMIT_SKIP_HEAVY=0 forces heavy in agent context",
			env:         map[string]string{"GC_AGENT": "test-agent", "GC_PRECOMMIT_SKIP_HEAVY": "0"},
			stageSpec:   true,
			expectCalls: []string{"make vet", "dashboard-check dashboard-smoke"},
		},
		{
			name:        "GC_PRECOMMIT_SKIP_HEAVY=1 forces skip without agent",
			env:         map[string]string{"GC_PRECOMMIT_SKIP_HEAVY": "1"},
			stageSpec:   false,
			expectCalls: []string{"make vet"},
			forbidCalls: []string{"dashboard-check", "dashboard-smoke"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			workDir, callLog := setupPreCommitFakeRepo(t, tc.stageSpec)

			env := []string{
				"PATH=" + filepath.Join(workDir, "bin") + string(os.PathListSeparator) + os.Getenv("PATH"),
				"HOME=" + t.TempDir(),
				"TMPDIR=" + t.TempDir(),
				"GIT_TERMINAL_PROMPT=0",
			}
			for k, v := range tc.env {
				env = append(env, k+"="+v)
			}

			cmd := exec.Command("bash", hookPath)
			cmd.Dir = workDir
			cmd.Env = env

			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("pre-commit hook failed: %v\n--- hook output ---\n%s", err, out)
			}

			logBytes, err := os.ReadFile(callLog)
			if err != nil {
				t.Fatalf("read call log: %v", err)
			}
			log := string(logBytes)

			for _, want := range tc.expectCalls {
				if !strings.Contains(log, want) {
					t.Errorf("call log missing expected %q\n--- log ---\n%s\n--- hook output ---\n%s", want, log, out)
				}
			}
			for _, forbid := range tc.forbidCalls {
				if strings.Contains(log, forbid) {
					t.Errorf("call log unexpectedly contains %q\n--- log ---\n%s\n--- hook output ---\n%s", forbid, log, out)
				}
			}
		})
	}
}

// setupPreCommitFakeRepo builds a minimal git repo that mirrors the file
// layout the pre-commit hook expects, stubs the external commands it
// invokes (make, go, npm, scripts/precommit-format-staged-go) to log + succeed,
// stages a Go file (and optionally the openapi spec to trigger the
// dashboard block), and returns the worktree path plus the path to the
// call-log file.
func setupPreCommitFakeRepo(t *testing.T, stageSpec bool) (string, string) {
	t.Helper()

	workDir := t.TempDir()
	binDir := filepath.Join(workDir, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatalf("mkdir bin: %v", err)
	}
	callLog := filepath.Join(workDir, "calls.log")

	stub := fmt.Sprintf(`#!/usr/bin/env bash
printf '%%s %%s\n' "$(basename "$0")" "$*" >> %q
exit 0
`, callLog)
	writeExecutable(t, filepath.Join(binDir, "make"), stub)
	writeExecutable(t, filepath.Join(binDir, "go"), stub)
	writeExecutable(t, filepath.Join(binDir, "npm"), stub)

	scriptsDir := filepath.Join(workDir, "scripts")
	if err := os.MkdirAll(scriptsDir, 0o755); err != nil {
		t.Fatalf("mkdir scripts: %v", err)
	}
	writeExecutable(t, filepath.Join(scriptsDir, "precommit-format-staged-go"),
		fmt.Sprintf(`#!/usr/bin/env bash
printf 'precommit-format-staged-go %%s\n' "$*" >> %q
cat >/dev/null
exit 0
`, callLog))

	// Placeholder files so the hook's `git add <path>` lines do not fail.
	// These are stubs — the real Go genspec/genschema steps are stubbed
	// out above, so we just need the files to exist for git add.
	placeholders := []string{
		"internal/api/openapi.json",
		"docs/reference/schema/openapi.json",
		"docs/reference/schema/openapi.txt",
		"internal/api/genclient/client_gen.go",
		"docs/reference/schema/city-schema.json",
		"docs/reference/schema/city-schema.txt",
		"docs/reference/config.md",
		"docs/reference/cli.md",
		"internal/api/dashboardspa/dist/placeholder.txt",
	}
	for _, rel := range placeholders {
		abs := filepath.Join(workDir, rel)
		if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", filepath.Dir(abs), err)
		}
		if err := os.WriteFile(abs, []byte("placeholder\n"), 0o644); err != nil {
			t.Fatalf("write placeholder %s: %v", rel, err)
		}
	}

	runGit(t, workDir, "init", "-q", "--initial-branch=main")
	runGit(t, workDir, "config", "user.email", "test@example.com")
	runGit(t, workDir, "config", "user.name", "Pre-commit test")

	goFile := filepath.Join(workDir, "main.go")
	if err := os.WriteFile(goFile, []byte("package main\n"), 0o644); err != nil {
		t.Fatalf("write main.go: %v", err)
	}
	runGit(t, workDir, "add", "main.go")

	if stageSpec {
		runGit(t, workDir, "add", "internal/api/openapi.json")
	}

	return workDir, callLog
}

func runGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
}

func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	return filepath.Dir(wd)
}

func writeExecutable(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o755); err != nil {
		t.Fatalf("write executable %s: %v", path, err)
	}
}

func writeTestFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create parent for %s: %v", path, err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
