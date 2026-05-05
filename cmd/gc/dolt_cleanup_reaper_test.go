package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestExtractConfigPath_SpaceSeparated(t *testing.T) {
	argv := []string{"dolt", "sql-server", "--config", "/tmp/TestFoo123/config.yaml"}
	got := extractConfigPath(argv)
	want := "/tmp/TestFoo123/config.yaml"
	if got != want {
		t.Errorf("extractConfigPath() = %q, want %q", got, want)
	}
}

func TestExtractConfigPath_EqualsForm(t *testing.T) {
	argv := []string{"dolt", "sql-server", "--config=/tmp/TestFoo/config.yaml"}
	got := extractConfigPath(argv)
	want := "/tmp/TestFoo/config.yaml"
	if got != want {
		t.Errorf("extractConfigPath() = %q, want %q", got, want)
	}
}

func TestExtractConfigPath_Missing(t *testing.T) {
	argv := []string{"dolt", "sql-server", "--port", "3307"}
	got := extractConfigPath(argv)
	if got != "" {
		t.Errorf("extractConfigPath() = %q, want empty", got)
	}
}

func TestExtractConfigPath_FlagAtEnd(t *testing.T) {
	// --config with no value should return empty (malformed cmdline).
	argv := []string{"dolt", "sql-server", "--config"}
	got := extractConfigPath(argv)
	if got != "" {
		t.Errorf("extractConfigPath() = %q, want empty for trailing --config", got)
	}
}

func TestIsTestConfigPath_TmpTestPrefix(t *testing.T) {
	if !isTestConfigPath("/tmp/TestOrchestrator123/config.yaml", "/home/u", "") {
		t.Error("expected /tmp/Test* to be a test path")
	}
}

func TestIsTestConfigPath_HomeGotmpTestPrefix(t *testing.T) {
	if !isTestConfigPath("/home/u/.gotmp/TestFuzz/config.yaml", "/home/u", "") {
		t.Error("expected $HOME/.gotmp/Test* to be a test path")
	}
}

func TestIsTestConfigPath_ProcessTempDirTestPrefix(t *testing.T) {
	if !isTestConfigPath("/var/tmp/go-test/TestRepro/config.yaml", "/home/u", "/var/tmp/go-test") {
		t.Error("expected os.TempDir()/Test* to be a test path")
	}
}

func TestIsTestConfigPath_KnownGCTestPrefix(t *testing.T) {
	if !isTestConfigPath("/data/tmp/gc-state-mutation-builtin-123/.gc/runtime/packs/dolt/dolt-config.yaml", "/home/u", "/data/tmp") {
		t.Error("expected known gc-* test prefix under os.TempDir() to be a test path")
	}
}

func TestIsTestConfigPath_NotTest(t *testing.T) {
	cases := []string{
		"/tmp/be-s9d-bench-dolt/config.yaml", // benchmark
		"/var/lib/dolt/config.yaml",          // production-ish
		"/tmp/random/config.yaml",            // tmp but not Test prefix
		"/home/u/.gotmp/other/config.yaml",   // gotmp but not Test prefix
		"/var/tmp/go-test/Other/config.yaml", // temp root but not Test prefix
		"",                                   // missing
	}
	for _, p := range cases {
		if isTestConfigPath(p, "/home/u", "/var/tmp/go-test") {
			t.Errorf("isTestConfigPath(%q) = true, want false", p)
		}
	}
}

func TestClassifyDoltProcess_ProtectedByRigPort(t *testing.T) {
	p := DoltProcInfo{
		PID:   1234,
		Argv:  []string{"dolt", "sql-server", "--config", "/tmp/TestFoo/config.yaml"},
		Ports: []int{28231},
	}
	got := classifyDoltProcess(p, map[int]string{28231: "beads"}, "/home/u", "", nil)

	if got.Action != "protect" {
		t.Errorf("Action = %q, want protect", got.Action)
	}
	if got.Reason == "" || !strings.Contains(got.Reason, "rig") || !strings.Contains(got.Reason, "beads") {
		t.Errorf("Reason = %q, want rig+beads reference", got.Reason)
	}
}

func TestClassifyDoltProcess_OrphanByTestPath(t *testing.T) {
	p := DoltProcInfo{
		PID:   2222,
		Argv:  []string{"dolt", "sql-server", "--config", "/tmp/TestMailRouter9182/config.yaml"},
		Ports: []int{},
	}
	got := classifyDoltProcess(p, nil, "/home/u", "", nil)

	if got.Action != "reap" {
		t.Errorf("Action = %q, want reap", got.Action)
	}
	if got.ConfigPath != "/tmp/TestMailRouter9182/config.yaml" {
		t.Errorf("ConfigPath = %q", got.ConfigPath)
	}
}

func TestClassifyDoltProcess_ProtectsActiveTestRoot(t *testing.T) {
	p := DoltProcInfo{
		PID:   2223,
		Argv:  []string{"dolt", "sql-server", "--config", "/tmp/TestPersonalWorkFormulaCompileAndRun123/001/city/.gc/runtime/packs/dolt/dolt-config.yaml"},
		Ports: []int{},
	}
	got := classifyDoltProcess(p, nil, "/home/u", "", []string{"/tmp/TestPersonalWorkFormulaCompileAndRun123"})

	if got.Action != "protect" {
		t.Errorf("Action = %q, want protect", got.Action)
	}
	if !strings.Contains(got.Reason, "active test root") {
		t.Errorf("Reason = %q, want active-test-root reason", got.Reason)
	}
}

func TestClassifyDoltProcess_ProtectedByPathNotOnAllowlist(t *testing.T) {
	// Active benchmark — config path doesn't match /tmp/Test*.
	p := DoltProcInfo{
		PID:   3333,
		Argv:  []string{"dolt", "sql-server", "--config", "/tmp/be-s9d-bench-dolt/config.yaml"},
		Ports: []int{33400},
	}
	got := classifyDoltProcess(p, nil, "/home/u", "", nil)

	if got.Action != "protect" {
		t.Errorf("Action = %q, want protect", got.Action)
	}
	if !strings.Contains(got.Reason, "allowlist") {
		t.Errorf("Reason = %q, want mention of allowlist", got.Reason)
	}
	// Reason should echo the actual config path so operators can see it.
	if !strings.Contains(got.Reason, "/tmp/be-s9d-bench-dolt") {
		t.Errorf("Reason = %q, want config path echoed (architect Open Q 0)", got.Reason)
	}
}

func TestClassifyDoltProcess_ProtectedWhenConfigMissing(t *testing.T) {
	p := DoltProcInfo{
		PID:   4444,
		Argv:  []string{"dolt", "sql-server"},
		Ports: []int{},
	}
	got := classifyDoltProcess(p, nil, "/home/u", "", nil)

	if got.Action != "protect" {
		t.Errorf("Action = %q, want protect", got.Action)
	}
	if !strings.Contains(got.Reason, "config") {
		t.Errorf("Reason = %q, want config-path-related reason", got.Reason)
	}
}

func TestClassifyDoltProcess_RigPortBeatsConfigPath(t *testing.T) {
	// Even if the cmdline says /tmp/Test*, a rig-port match always protects.
	p := DoltProcInfo{
		PID:   5555,
		Argv:  []string{"dolt", "sql-server", "--config", "/tmp/TestSomething/config.yaml"},
		Ports: []int{28231},
	}
	got := classifyDoltProcess(p, map[int]string{28231: "beads"}, "/home/u", "", nil)

	if got.Action != "protect" {
		t.Errorf("Action = %q, want protect (rig port wins)", got.Action)
	}
}

func TestPlanReap_BuildsOrphanAndProtectedLists(t *testing.T) {
	procs := []DoltProcInfo{
		{PID: 1138290, Ports: []int{28231}, Argv: []string{"dolt", "sql-server"}},
		{PID: 1281044, Argv: []string{"dolt", "sql-server", "--config", "/tmp/TestA/config.yaml"}},
		{PID: 1319499, Ports: []int{33400}, Argv: []string{"dolt", "sql-server", "--config", "/tmp/be-s9d-bench-dolt/config.yaml"}},
		{PID: 1281099, Argv: []string{"dolt", "sql-server", "--config", "/tmp/TestB/config.yaml"}},
		{PID: 1281100, Argv: []string{"dolt", "sql-server", "--config", "/data/tmp/gc-state-runtime-builtin-1/.gc/runtime/packs/dolt/dolt-config.yaml"}},
		{PID: 1281101, Argv: []string{"dolt", "sql-server", "--config", "/tmp/TestActive/001/city/.gc/runtime/packs/dolt/dolt-config.yaml"}},
	}
	rigPorts := map[int]string{28231: "beads"}

	plan := planOrphanReap(procs, rigPorts, "/home/u", "/data/tmp", []string{"/tmp/TestActive"})

	wantReap := []int{1281044, 1281099, 1281100}
	gotReap := make([]int, 0, len(plan.Reap))
	for _, target := range plan.Reap {
		gotReap = append(gotReap, target.PID)
	}
	if !reflect.DeepEqual(gotReap, wantReap) {
		t.Errorf("Reap PIDs = %v, want %v", gotReap, wantReap)
	}

	wantProtected := []int{1138290, 1319499, 1281101}
	gotProtected := make([]int, 0, len(plan.Protected))
	for _, e := range plan.Protected {
		gotProtected = append(gotProtected, e.PID)
	}
	if !reflect.DeepEqual(gotProtected, wantProtected) {
		t.Errorf("Protected PIDs = %v, want %v", gotProtected, wantProtected)
	}
}

func TestOrphanedDoltsUnderCity_MatchesConfigUnderCity(t *testing.T) {
	procs := []DoltProcInfo{
		{PID: 100, Argv: []string{"dolt", "sql-server", "--config", "/tmp/gc-state-runtime-builtin-1/.gc/runtime/packs/dolt/dolt-config.yaml"}},
		{PID: 101, Argv: []string{"dolt", "sql-server", "--config=/tmp/gc-state-runtime-builtin-1/.gc/runtime/packs/dolt/dolt-config.yaml"}},
		{PID: 200, Argv: []string{"dolt", "sql-server", "--config", "/tmp/other-city/.gc/runtime/packs/dolt/dolt-config.yaml"}},
		{PID: 300, Argv: []string{"dolt", "sql-server"}},
	}
	got := orphanedDoltsUnderCity("/tmp/gc-state-runtime-builtin-1", procs)
	want := []int{100, 101}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("orphanedDoltsUnderCity = %v, want %v", got, want)
	}
}

func TestOrphanedDoltsUnderCity_TrailingSlashCityPath(t *testing.T) {
	procs := []DoltProcInfo{
		{PID: 100, Argv: []string{"dolt", "sql-server", "--config", "/tmp/city/.gc/runtime/packs/dolt/dolt-config.yaml"}},
	}
	got := orphanedDoltsUnderCity("/tmp/city/", procs)
	want := []int{100}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("orphanedDoltsUnderCity (trailing slash) = %v, want %v", got, want)
	}
}

func TestOrphanedDoltsUnderCity_DoesNotMatchSiblingPrefix(t *testing.T) {
	// /tmp/city2 must not match cityPath=/tmp/city even though it shares
	// the prefix string — filepath.Clean + separator boundary prevents this.
	procs := []DoltProcInfo{
		{PID: 100, Argv: []string{"dolt", "sql-server", "--config", "/tmp/city2/.gc/runtime/packs/dolt/dolt-config.yaml"}},
	}
	got := orphanedDoltsUnderCity("/tmp/city", procs)
	if len(got) != 0 {
		t.Errorf("orphanedDoltsUnderCity = %v, want empty (sibling path must not match)", got)
	}
}

func TestOrphanedDoltsUnderCity_RejectsRootCityPath(t *testing.T) {
	// A cityPath of "/" or "" must never reap — it would match every
	// dolt process on the host.
	procs := []DoltProcInfo{
		{PID: 100, Argv: []string{"dolt", "sql-server", "--config", "/var/lib/dolt/config.yaml"}},
	}
	for _, cityPath := range []string{"", "/", "."} {
		got := orphanedDoltsUnderCity(cityPath, procs)
		if len(got) != 0 {
			t.Errorf("orphanedDoltsUnderCity(%q) = %v, want empty (root path guard)", cityPath, got)
		}
	}
}

func TestOrphanedDoltsUnderCity_IgnoresProcsWithoutConfig(t *testing.T) {
	procs := []DoltProcInfo{
		{PID: 100, Argv: []string{"dolt", "sql-server"}},
		{PID: 101, Argv: []string{"dolt", "sql-server", "--port", "3307"}},
	}
	got := orphanedDoltsUnderCity("/tmp/city", procs)
	if len(got) != 0 {
		t.Errorf("orphanedDoltsUnderCity = %v, want empty for procs without --config", got)
	}
}

// fakeDoltBinaryPath compiles a tiny Go binary that calls time.Sleep, returns
// its path. Reuses a process-wide path cached in fakeDoltBin so the cost is
// paid once per `go test` invocation. The binary's argv[0] is overridden to
// "dolt" via exec.Cmd.Args (see spawnFakeDolt) so /proc/PID/cmdline reads as
// `dolt sql-server --config <path>` — what discoverDoltProcesses matches on.
func fakeDoltBinaryPath(t *testing.T) string {
	t.Helper()
	fakeDoltBinOnce.Do(func() {
		dir, err := os.MkdirTemp("", "fake-dolt-")
		if err != nil {
			fakeDoltBinErr = err
			return
		}
		src := "package main\nimport \"time\"\nfunc main() { time.Sleep(time.Hour) }\n"
		srcPath := filepath.Join(dir, "fake_dolt.go")
		if err := os.WriteFile(srcPath, []byte(src), 0o644); err != nil {
			fakeDoltBinErr = err
			return
		}
		bin := filepath.Join(dir, "fake-dolt-bin")
		if out, err := exec.Command("go", "build", "-o", bin, srcPath).CombinedOutput(); err != nil {
			fakeDoltBinErr = fmt.Errorf("build fake dolt: %w: %s", err, string(out))
			return
		}
		fakeDoltBin = bin
		registerProcessCleanup(func() { _ = os.RemoveAll(dir) })
	})
	if fakeDoltBinErr != nil {
		t.Fatalf("fake dolt binary: %v", fakeDoltBinErr)
	}
	return fakeDoltBin
}

var (
	fakeDoltBinOnce sync.Once
	fakeDoltBin     string
	fakeDoltBinErr  error
)

// spawnFakeDolt spawns a long-running process whose /proc/PID/cmdline reads
// as `dolt sql-server --config <configPath>`. The actual binary is the tiny
// time.Sleep helper from fakeDoltBinaryPath, symlinked as <dir>/dolt so
// discoverDoltProcesses' `filepath.Base(argv[0]) == "dolt"` check matches.
func spawnFakeDolt(t *testing.T, dir, configPath string) *exec.Cmd {
	t.Helper()
	bin := fakeDoltBinaryPath(t)
	doltLink := filepath.Join(dir, "dolt")
	if err := os.Symlink(bin, doltLink); err != nil {
		t.Fatalf("symlink fake dolt: %v", err)
	}
	cmd := &exec.Cmd{
		Path: doltLink,
		Args: []string{"dolt", "sql-server", "--config", configPath},
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("spawn fake dolt: %v", err)
	}
	return cmd
}

// TestReapOrphanDoltUnderTestCity_KillsLeakedDolt verifies the end-to-end
// fallback: a process with a `dolt sql-server --config <cityPath>/...` argv
// signature is killed by reapOrphanDoltUnderTestCity, even though the structured
// stop path (state file, bd stop op) doesn't know about it.
func TestReapOrphanDoltUnderTestCity_KillsLeakedDolt(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("discoverDoltProcesses is Linux-only")
	}

	cityPath := t.TempDir()
	configPath := filepath.Join(cityPath, ".gc", "runtime", "packs", "dolt", "dolt-config.yaml")

	cmd := spawnFakeDolt(t, t.TempDir(), configPath)
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	})

	// Wait for the process to be visible in /proc with the expected argv.
	waitForFakeDoltCmdline(t, cmd.Process.Pid)

	reapOrphanDoltUnderTestCity(t, cityPath)

	// cmd.Wait() returns once the kernel has reaped the child. The reaper
	// sent SIGKILL via os.FindProcess(pid).Signal — that races against the
	// parent's Wait() but the process *will* exit once SIGKILL lands.
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	select {
	case <-exited:
		// Killed and reaped.
	case <-time.After(3 * time.Second):
		t.Fatalf("process %d still alive after reapOrphanDoltUnderTestCity", cmd.Process.Pid)
	}
}

// TestReapOrphanDoltUnderTestCity_PreservesUnrelatedDolt verifies the helper
// does not touch dolt processes whose --config is outside cityPath, even if
// they were spawned by some other test or a production server.
func TestReapOrphanDoltUnderTestCity_PreservesUnrelatedDolt(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("discoverDoltProcesses is Linux-only")
	}

	cityPath := t.TempDir()
	otherPath := t.TempDir()
	otherConfig := filepath.Join(otherPath, ".gc", "runtime", "packs", "dolt", "dolt-config.yaml")

	cmd := spawnFakeDolt(t, t.TempDir(), otherConfig)
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	})

	waitForFakeDoltCmdline(t, cmd.Process.Pid)

	reapOrphanDoltUnderTestCity(t, cityPath)

	if err := cmd.Process.Signal(syscall.Signal(0)); err != nil {
		t.Fatalf("process %d was killed but should have been preserved (config under %s, cityPath %s): %v",
			cmd.Process.Pid, otherPath, cityPath, err)
	}
}

// waitForFakeDoltCmdline blocks until /proc/<pid>/cmdline contains the
// fake-dolt argv signature so the reaper can see it. spawnFakeDolt's child
// is observable in /proc almost immediately after Start, but the timing is
// not guaranteed across runtime variations.
func waitForFakeDoltCmdline(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		data, _ := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "cmdline"))
		if strings.Contains(string(data), "sql-server") {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("fake dolt PID %d cmdline not visible in /proc within 2s", pid)
}
