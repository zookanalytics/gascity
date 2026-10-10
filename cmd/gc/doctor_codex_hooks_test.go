package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/doctor"
)

// managedCodexHooksForDoctorTest is a Codex hooks file holding Gas City's
// managed hooks next to an operator's own hook.
const managedCodexHooksForDoctorTest = `{
  "hooks": {
    "SessionStart": [{
      "matcher": "startup",
      "hooks": [{
        "type": "command",
        "command": "export PATH=\"$PATH:$HOME/go/bin:$HOME/.local/bin\" && GC_MANAGED_SESSION_HOOK=1 GC_HOOK_EVENT_NAME=SessionStart \"${GC_BIN:-gc}\" --city /old/city prime --hook --hook-format codex"
      }]
    }],
    "PreCompact": [{
      "hooks": [{
        "type": "command",
        "command": "export PATH=\"$PATH:$HOME/go/bin:$HOME/.local/bin\" && \"${GC_BIN:-gc}\" handoff --auto --hook-format codex \"context cycle\""
      }]
    }],
    "PreToolUse": [{
      "matcher": "Bash",
      "hooks": [{
        "type": "command",
        "command": "/opt/operator-guard.sh"
      }]
    }]
  }
}`

func TestCodexHooksDriftCheckReportsManagedCopies(t *testing.T) {
	dir := t.TempDir()
	writeCodexHooksForDoctorTest(t, dir, managedCodexHooksForDoctorTest)

	check := newCodexHooksDriftCheck([]string{dir})
	result := check.Run(&doctor.CheckContext{})

	if result.Status != doctor.StatusWarning {
		t.Fatalf("status = %v, want warning; message=%s", result.Status, result.Message)
	}
	if !strings.Contains(result.Message, "repeat Gas City's managed hooks") {
		t.Fatalf("message = %q, want it to name the repeated managed hooks", result.Message)
	}
	if want := filepath.Join(dir, ".codex", "hooks.json"); len(result.Details) != 1 || result.Details[0] != want {
		t.Fatalf("details = %q, want [%s]", result.Details, want)
	}
}

func TestCodexHooksDriftCheckPassesFilesWithoutManagedEntries(t *testing.T) {
	custom := t.TempDir()
	writeCodexHooksForDoctorTest(t, custom, `{
  "hooks": {
    "UserPromptSubmit": [{
      "hooks": [{
        "type": "command",
        "command": "FOO=1 gc mail check --inject --hook-format codex"
      }]
    }]
  }
}`)
	malformed := t.TempDir()
	writeCodexHooksForDoctorTest(t, malformed, `{not-json`)
	missing := t.TempDir()

	check := newCodexHooksDriftCheck([]string{custom, malformed, missing})
	result := check.Run(&doctor.CheckContext{})

	if result.Status != doctor.StatusOK {
		t.Fatalf("status = %v, want ok; message=%s details=%q", result.Status, result.Message, result.Details)
	}
}

func TestCodexHooksDriftCheckFixRemovesOnlyManagedEntries(t *testing.T) {
	mixed := t.TempDir()
	writeCodexHooksForDoctorTest(t, mixed, managedCodexHooksForDoctorTest)
	managedOnly := filepath.Join(t.TempDir(), ".gc", "agents", "reviewer")
	writeCodexHooksForDoctorTest(t, managedOnly, `{
  "hooks": {
    "SessionStart": [{
      "hooks": [{
        "type": "command",
        "command": "export PATH=\"$PATH:$HOME/go/bin:$HOME/.local/bin\" && \"${GC_BIN:-gc}\" prime --hook --hook-format codex"
      }]
    }]
  }
}`)

	check := newCodexHooksDriftCheck([]string{mixed, managedOnly})
	if err := check.Fix(&doctor.CheckContext{}); err != nil {
		t.Fatalf("Fix: %v", err)
	}
	if result := check.Run(&doctor.CheckContext{}); result.Status != doctor.StatusOK {
		t.Fatalf("status after fix = %v, want ok; message=%s details=%q", result.Status, result.Message, result.Details)
	}

	data, err := os.ReadFile(filepath.Join(mixed, ".codex", "hooks.json"))
	if err != nil {
		t.Fatalf("read hooks: %v", err)
	}
	got := string(data)
	if !strings.Contains(got, "/opt/operator-guard.sh") {
		t.Fatalf("fix dropped the operator's own hook:\n%s", got)
	}
	for _, managed := range []string{"prime --hook", "handoff --auto"} {
		if strings.Contains(got, managed) {
			t.Fatalf("fix kept the managed %q hook:\n%s", managed, got)
		}
	}
	if _, err := os.Stat(filepath.Join(managedOnly, ".codex", "hooks.json")); !os.IsNotExist(err) {
		t.Fatalf("a hooks file holding only managed hooks survived the fix: stat err = %v", err)
	}
}

func TestNewCodexHooksDriftCheckCleansDedupesAndSortsDirs(t *testing.T) {
	check := newCodexHooksDriftCheck([]string{" /z/../z ", "", "/a", "/a/."})

	if got, want := strings.Join(check.dirs, ","), "/a,/z"; got != want {
		t.Fatalf("dirs = %q, want %q", got, want)
	}
	if got, want := check.Name(), "codex-hooks-drift"; got != want {
		t.Fatalf("Name = %q, want %q", got, want)
	}
	if !check.CanFix() {
		t.Fatal("CanFix = false, want true")
	}
}

func TestCodexHookWorkDirsIncludesActiveRigPaths(t *testing.T) {
	cfg := &config.City{
		Rigs: []config.Rig{
			{Name: "active", Path: "/rig/active"},
			{Name: "blank", Path: " "},
			{Name: "suspended", Path: "/rig/suspended", SuspendedOnStart: true},
		},
	}

	got := codexHookWorkDirs("/city", cfg)
	if strings.Join(got, ",") != "/city,/rig/active" {
		t.Fatalf("work dirs = %#v, want city plus active rig only", got)
	}
	if got := codexHookWorkDirs("/city", nil); len(got) != 1 || got[0] != "/city" {
		t.Fatalf("nil config work dirs = %#v, want city only", got)
	}
}

func TestCodexHookWorkDirsIncludesResolvedAgentWorkDirs(t *testing.T) {
	cityDir := t.TempDir()
	activeRig := filepath.Join(cityDir, "rigs", "active")
	suspendedRig := filepath.Join(cityDir, "rigs", "suspended")
	agentWorkDir := filepath.Join(cityDir, ".gc", "agents", "reviewer")
	cfg := &config.City{
		Workspace: config.Workspace{InstallAgentHooks: []string{"codex"}},
		Rigs: []config.Rig{
			{Name: "active", Path: activeRig},
			{Name: "suspended", Path: suspendedRig, SuspendedOnStart: true},
		},
		Agents: []config.Agent{
			{Name: "reviewer", Dir: "active", WorkDir: agentWorkDir},
			{Name: "gemini", Dir: "active", InstallAgentHooks: []string{"gemini"}, WorkDir: filepath.Join(cityDir, ".gc", "agents", "gemini")},
			{Name: "parked", Dir: "active", WorkDir: filepath.Join(cityDir, ".gc", "agents", "parked"), Suspended: true},
			{Name: "codex", Dir: "suspended", WorkDir: filepath.Join(cityDir, ".gc", "agents", "suspended")},
		},
	}

	got := codexHookWorkDirs(cityDir, cfg)

	assertDoctorPathPresent(t, got, cityDir)
	assertDoctorPathPresent(t, got, activeRig)
	assertDoctorPathPresent(t, got, agentWorkDir)
	assertDoctorPathAbsent(t, got, suspendedRig)
	assertDoctorPathAbsent(t, got, filepath.Join(cityDir, ".gc", "agents", "gemini"))
	assertDoctorPathAbsent(t, got, filepath.Join(cityDir, ".gc", "agents", "parked"))
	assertDoctorPathAbsent(t, got, filepath.Join(cityDir, ".gc", "agents", "suspended"))
}

func TestCodexHookWorkDirsIncludesBoundedPoolInstanceWorkDirs(t *testing.T) {
	cityDir := t.TempDir()
	rigDir := filepath.Join(cityDir, "rigs", "active")
	maxSessions := 2
	cfg := &config.City{
		Workspace: config.Workspace{InstallAgentHooks: []string{"codex"}},
		Rigs:      []config.Rig{{Name: "active", Path: rigDir}},
		Agents: []config.Agent{{
			Name:              "worker",
			Dir:               "active",
			WorkDir:           filepath.Join(".gc", "worktrees", "{{.Rig}}", "{{.AgentBase}}"),
			MaxActiveSessions: &maxSessions,
		}},
	}

	got := codexHookWorkDirs(cityDir, cfg)

	assertDoctorPathPresent(t, got, filepath.Join(cityDir, ".gc", "worktrees", "active", "worker"))
	assertDoctorPathPresent(t, got, filepath.Join(cityDir, ".gc", "worktrees", "active", "worker-1"))
	assertDoctorPathPresent(t, got, filepath.Join(cityDir, ".gc", "worktrees", "active", "worker-2"))
}

func TestCodexHooksHaveManagedEntriesRejectsUnreadableMalformedAndCustomFiles(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".codex", "hooks.json")
	if codexHooksHaveManagedEntries(path) {
		t.Fatal("missing file reported a managed entry")
	}

	writeCodexHooksForDoctorTest(t, dir, `{not-json`)
	if codexHooksHaveManagedEntries(path) {
		t.Fatal("malformed JSON reported a managed entry")
	}

	writeCodexHooksForDoctorTest(t, dir, `{"notHooks": {}}`)
	if codexHooksHaveManagedEntries(path) {
		t.Fatal("file without a hooks map reported a managed entry")
	}

	writeCodexHooksForDoctorTest(t, dir, `{"hooks":{"UserPromptSubmit":[{"hooks":[{"type":"command","command":"FOO=1 gc mail check --inject --hook-format codex"}]}]}}`)
	if codexHooksHaveManagedEntries(path) {
		t.Fatal("env-prefixed custom hooks reported a managed entry")
	}

	writeCodexHooksForDoctorTest(t, dir, managedCodexHooksForDoctorTest)
	if !codexHooksHaveManagedEntries(path) {
		t.Fatal("managed hooks were not reported")
	}
}

func assertDoctorPathPresent(t *testing.T, paths []string, want string) {
	t.Helper()
	want = filepath.Clean(want)
	for _, path := range paths {
		if filepath.Clean(path) == want {
			return
		}
	}
	t.Fatalf("paths = %#v, want %s present", paths, want)
}

func assertDoctorPathAbsent(t *testing.T, paths []string, want string) {
	t.Helper()
	want = filepath.Clean(want)
	for _, path := range paths {
		if filepath.Clean(path) == want {
			t.Fatalf("paths = %#v, want %s absent", paths, want)
		}
	}
}

func writeCodexHooksForDoctorTest(t *testing.T, dir, data string) {
	t.Helper()
	hookDir := filepath.Join(dir, ".codex")
	if err := os.MkdirAll(hookDir, 0o755); err != nil {
		t.Fatalf("mkdir hooks dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(hookDir, "hooks.json"), []byte(data), 0o644); err != nil {
		t.Fatalf("write hooks: %v", err)
	}
}
