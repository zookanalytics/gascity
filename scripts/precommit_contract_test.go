package scripts_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// unconstrainedDiskKiB is a scratch-space reading (1 TiB) far above anything
// the fan-out policy can clamp on. Cases that are not about the disk budget
// inject it so the host's real free space cannot perturb their expectations.
const unconstrainedDiskKiB = "1073741824"

// localTestJobsOverride is the explicit fan-out override the Makefile honors
// ahead of the detector (`LOCAL_TEST_JOBS ?=`); localTestDetectorEnvPrefix is
// the namespace of stubs scripts/test-local-job-count reads to fake a host's
// CPUs, memory and free scratch.
const (
	localTestJobsOverride      = "LOCAL_TEST_JOBS="
	localTestDetectorEnvPrefix = "GC_TEST_LOCAL_"
)

// sanitizedLocalTestEnv returns the inherited environment with every variable
// that decides the local fan-out removed. Every test asserting a computed job
// count must build its child environment from this rather than os.Environ():
// a LOCAL_TEST_JOBS exported by the caller — a developer's shell, an outer
// sharded runner — beats the Makefile's `?=` default, so an inherited value
// would answer the assertion instead of the detector under test.
func sanitizedLocalTestEnv() []string {
	inherited := os.Environ()
	env := make([]string, 0, len(inherited))
	for _, entry := range inherited {
		if strings.HasPrefix(entry, localTestJobsOverride) ||
			strings.HasPrefix(entry, localTestDetectorEnvPrefix) {
			continue
		}
		env = append(env, entry)
	}
	return env
}

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
	baseEnv := sanitizedLocalTestEnv()
	tests := []struct {
		name      string
		cpus      string
		memoryKiB string
		diskKiB   string
		makeArgs  []string
		wantJobs  string
		wantWarn  string
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

		// Free build scratch is the third budget: every concurrent shard writes
		// its own multi-GiB Go $WORK tree, so a fan-out sized purely on CPU and
		// memory can fill the volume that also carries the Dolt data plane
		// (gc-k1b5h).
		{name: "disk constrains fanout", cpus: "16", memoryKiB: "536870912", diskKiB: "20971520", wantJobs: "3"},
		{name: "disk at the reserve floor still runs one job", cpus: "16", memoryKiB: "536870912", diskKiB: "8388608", wantJobs: "1", wantWarn: "free for build scratch"},
		{name: "exhausted disk still runs one job", cpus: "16", memoryKiB: "536870912", diskKiB: "1048576", wantJobs: "1", wantWarn: "free for build scratch"},
		{name: "unknown disk leaves fanout to cpu and memory", cpus: "16", memoryKiB: "536870912", diskKiB: "0", wantJobs: "16"},
		{name: "tightest of the three budgets wins", cpus: "16", memoryKiB: "16777216", diskKiB: "41943040", wantJobs: "4"},
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
			// Cases that do not exercise the disk budget still pin it, or the
			// host's own free space decides their fan-out and the expectation
			// becomes machine-dependent.
			diskKiB := tt.diskKiB
			if diskKiB == "" {
				diskKiB = unconstrainedDiskKiB
			}
			cmd.Env = append(cmd.Env, "GC_TEST_LOCAL_DISK_KIB="+diskKiB)
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
			// Below the reserve the fan-out is already at its one-job floor, so
			// clamping can no longer protect the volume. Staying silent there
			// hides a host that is about to ENOSPC mid-link.
			if tt.wantWarn != "" && !strings.Contains(command, tt.wantWarn) {
				t.Fatalf("exhausted scratch space should warn with %q:\n%s", tt.wantWarn, command)
			}
			if tt.wantWarn == "" && strings.Contains(command, "free for build scratch") {
				t.Fatalf("job count should not warn about scratch space when it is not exhausted:\n%s", command)
			}
		})
	}
}

// TestLocalJobCountProbesScratchFilesystem covers the two halves of the disk
// budget that GC_TEST_LOCAL_DISK_KIB stubs out in the table above: the real
// `df` probe against a real directory, and the fail-open path when no scratch
// filesystem can be measured. Fail-open matters because an unreadable
// free-space reading must leave today's CPU-and-memory fan-out untouched rather
// than silently throttle every suite on an unsupported host — the same stance
// the doctor's tmp-scratch-space check takes.
func TestLocalJobCountProbesScratchFilesystem(t *testing.T) {
	repoRoot := repoRoot(t)
	missing := filepath.Join(t.TempDir(), "definitely-absent")

	tests := []struct {
		name     string
		tmpDir   string
		goTmpDir string
		wantMin  int
		wantMax  int
	}{
		// A readable scratch dir must yield a usable count: any parse failure
		// would surface here as a non-numeric line or a zero.
		{name: "real filesystem yields a usable job count", tmpDir: t.TempDir(), wantMin: 1, wantMax: 16},
		// Neither candidate is measurable, so the disk budget must not clamp.
		{name: "unmeasurable scratch leaves fanout unclamped", tmpDir: missing, goTmpDir: missing, wantMin: 16, wantMax: 16},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := exec.Command(filepath.Join(repoRoot, "scripts", "test-local-job-count"))
			cmd.Dir = repoRoot
			cmd.Env = []string{
				"PATH=" + os.Getenv("PATH"),
				"HOME=" + os.Getenv("HOME"),
				"TMPDIR=" + tt.tmpDir,
				"GC_TEST_LOCAL_CPUS=16",
				"GC_TEST_LOCAL_MEMORY_KIB=536870912",
			}
			if tt.goTmpDir != "" {
				cmd.Env = append(cmd.Env, "GOTMPDIR="+tt.goTmpDir)
			}
			out, err := cmd.Output()
			if err != nil {
				t.Fatalf("test-local-job-count failed: %v\n%s", err, out)
			}
			jobs, err := strconv.Atoi(strings.TrimSpace(string(out)))
			if err != nil {
				t.Fatalf("test-local-job-count printed %q, want an integer", out)
			}
			if jobs < tt.wantMin || jobs > tt.wantMax {
				t.Fatalf("job count = %d, want between %d and %d", jobs, tt.wantMin, tt.wantMax)
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

// canonicalScratchDefault is the build-scratch filesystem every local test
// entry point falls back to when TMPDIR is unset. /tmp is conventionally a
// size-capped tmpfs; /var/tmp is persistent disk with room for the multi-GiB
// Go $WORK trees a sharded run creates (gc-v2z1p).
const canonicalScratchDefault = "/var/tmp"

// scratchDefaultPattern captures X out of a ${TMPDIR:-X} fallback, including
// the $${TMPDIR:-X} spelling make uses to escape its own expansion.
var scratchDefaultPattern = regexp.MustCompile(`\$\{TMPDIR:-([^}]*)\}`)

// scratchScopedFiles are the entry points whose TMPDIR carries `go test` build
// scratch: the detector that sizes the fan-out against free space, the runner
// that fans jobs out, and the two shard wrappers TESTING.md documents as direct
// entry points. Deliberately excludes scripts whose temp files are small and
// unrelated to build scratch (go-test-observable's JSONL log,
// precommit-format-staged-go's single formatted file, test-slice-enroll-test's
// workdir) — those are not part of the disk budget and need not move volumes.
var scratchScopedFiles = []string{
	"Makefile",
	filepath.Join("scripts", "test-local-job-count"),
	filepath.Join("scripts", "test-local-parallel"),
	filepath.Join("scripts", "test-go-test-shard"),
	filepath.Join("scripts", "test-integration-shard"),
}

// TestLocalTestScratchDefaultsAgree pins the detector and the runners to one
// scratch filesystem. scripts/test-local-job-count budgets 4 GiB of free space
// per concurrent shard above an 8 GiB reserve, so sizing the fan-out against
// one volume while the shards write their $WORK trees to another defeats the
// ENOSPC guard completely: the parallel targets other than test-fast-parallel
// invoke the runner without the Makefile's TEST_ENV wrapper, and the shard
// wrappers are documented for direct invocation, so each has to repeat the
// default rather than inherit it (gc-k1b5h).
func TestLocalTestScratchDefaultsAgree(t *testing.T) {
	repoRoot := repoRoot(t)

	for _, rel := range scratchScopedFiles {
		content, err := os.ReadFile(filepath.Join(repoRoot, rel))
		if err != nil {
			t.Fatalf("read %s: %v", rel, err)
		}
		matches := scratchDefaultPattern.FindAllStringSubmatch(string(content), -1)
		if len(matches) == 0 {
			t.Errorf("%s has no ${TMPDIR:-...} fallback; it must pin the scratch default to %s",
				rel, canonicalScratchDefault)
			continue
		}
		for _, match := range matches {
			if match[1] != canonicalScratchDefault {
				t.Errorf("%s falls back to TMPDIR=%s, want %s: the fan-out is sized against %s, "+
					"so shards writing elsewhere can still exhaust it",
					rel, match[1], canonicalScratchDefault, canonicalScratchDefault)
			}
		}
	}

	// The runner resolves the scratch dir once and hands that exact value to
	// every child, so the per-job env cannot drift away from the volume the
	// detector measured.
	runner, err := os.ReadFile(filepath.Join(repoRoot, "scripts", "test-local-parallel"))
	if err != nil {
		t.Fatalf("read test-local-parallel: %v", err)
	}
	if !strings.Contains(string(runner), `TMPDIR="${TEST_LOCAL_TMPDIR}"`) {
		t.Error("test-local-parallel must pass the resolved TEST_LOCAL_TMPDIR to each job, " +
			"not re-derive a default in the child shell")
	}
}

// TestParallelTargetsSizeFanoutAgainstMeasuredScratch covers every make target
// that fans jobs out, not just test-fast-parallel. The other three dispatch the
// runner without the TEST_ENV wrapper, so they are the paths where a detector
// and runner that disagree about the scratch volume would go unnoticed.
func TestParallelTargetsSizeFanoutAgainstMeasuredScratch(t *testing.T) {
	repoRoot := repoRoot(t)

	// 20 GiB free, minus the 8 GiB reserve, at 4 GiB per job = 3 jobs — well
	// under the 16 CPUs and 512 GiB of memory, so only the disk budget can
	// produce this number and every target must show it.
	const (
		cpus      = "16"
		memoryKiB = "536870912"
		diskKiB   = "20971520"
		wantJobs  = "3"
	)

	targets := []struct {
		target string
		mode   string
	}{
		{target: "test-fast-parallel", mode: "fast"},
		{target: "test-cmd-gc-process-parallel", mode: "cmd-gc-process"},
		{target: "test-integration-shards-parallel", mode: "integration"},
		{target: "test-local-full-parallel", mode: "full"},
	}

	// Plant the override the Makefile honors ahead of the detector. The
	// assertions below only mean anything if the child environment strips it
	// back out, and planting it here makes that failure deterministic: without
	// it the regression is invisible on a host that happens not to export
	// LOCAL_TEST_JOBS and fails only for whoever does.
	t.Setenv("LOCAL_TEST_JOBS", "7")

	for _, tt := range targets {
		t.Run(tt.target, func(t *testing.T) {
			cmd := exec.Command("make", "-n", tt.target)
			cmd.Dir = repoRoot
			cmd.Env = append(sanitizedLocalTestEnv(),
				"GC_TEST_LOCAL_CPUS="+cpus,
				"GC_TEST_LOCAL_MEMORY_KIB="+memoryKiB,
				"GC_TEST_LOCAL_DISK_KIB="+diskKiB,
			)
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("make -n %s failed: %v\n%s", tt.target, err, out)
			}
			command := string(out)
			if !strings.Contains(command, "./scripts/test-local-parallel "+tt.mode) {
				t.Fatalf("%s should dispatch the %s runner mode:\n%s", tt.target, tt.mode, command)
			}
			if !strings.Contains(command, "LOCAL_TEST_JOBS="+wantJobs+" ") {
				t.Fatalf("%s should clamp the fan-out to %s jobs on a scratch-constrained host:\n%s",
					tt.target, wantJobs, command)
			}
		})
	}
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
