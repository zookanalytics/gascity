package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/bootstrap/packs/core"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/doctor"
	"github.com/gastownhall/gascity/internal/materialize"
	"github.com/gastownhall/gascity/internal/runtime"
	"github.com/gastownhall/gascity/internal/shellquote"
)

func TestCodexHooksDriftCheckReportsManagedMissingPreCompact(t *testing.T) {
	dir := t.TempDir()
	writeCodexHooksForDoctorTest(t, dir, `{
  "hooks": {
    "SessionStart": [{
      "hooks": [{
        "type": "command",
        "command": "export PATH=\"$HOME/go/bin:$HOME/.local/bin:$PATH\" && gc prime --hook --hook-format codex"
      }]
    }]
  }
}`)

	check := newCodexHooksDriftCheck(dir, []string{dir})
	result := check.Run(&doctor.CheckContext{})

	if result.Status != doctor.StatusWarning {
		t.Fatalf("status = %v, want warning; message=%s", result.Status, result.Message)
	}
	if !strings.Contains(result.Message, "need upgrade") {
		t.Fatalf("message = %q, want need upgrade", result.Message)
	}
}

func TestCodexHooksDriftCheckPassesCurrentHooks(t *testing.T) {
	dir := t.TempDir()
	writeCodexHooksForDoctorTest(t, dir, fmt.Sprintf(`{
  "hooks": {
    "SessionStart": [{
      "hooks": [{
        "type": "command",
        "command": "export PATH=\"$HOME/go/bin:$HOME/.local/bin:$PATH\" && GC_MANAGED_SESSION_HOOK=1 GC_HOOK_EVENT_NAME=SessionStart gc --city %s prime --hook --hook-format codex"
      }]
    }],
    "PreCompact": [{
      "hooks": [{
        "type": "command",
        "command": "export PATH=\"$HOME/go/bin:$HOME/.local/bin:$PATH\" && gc --city %s handoff --auto --hook-format codex \"context cycle\""
      }]
    }]
  }
}`, shellquote.Quote(dir), shellquote.Quote(dir)))

	check := newCodexHooksDriftCheck(dir, []string{dir})
	result := check.Run(&doctor.CheckContext{})

	if result.Status != doctor.StatusOK {
		t.Fatalf("status = %v, want ok; message=%s", result.Status, result.Message)
	}
}

func TestCodexHooksDriftCheckIgnoresCustomHooks(t *testing.T) {
	dir := t.TempDir()
	writeCodexHooksForDoctorTest(t, dir, `{
  "hooks": {
    "UserPromptSubmit": [{
      "hooks": [{
        "type": "command",
        "command": "printf custom-codex-hook"
      }]
    }]
  }
}`)

	check := newCodexHooksDriftCheck(dir, []string{dir})
	result := check.Run(&doctor.CheckContext{})

	if result.Status != doctor.StatusOK {
		t.Fatalf("status = %v, want ok for user-owned hooks; message=%s", result.Status, result.Message)
	}
}

func TestCodexHooksDriftCheckFixUpgradesManagedHooks(t *testing.T) {
	dir := t.TempDir()
	writeCodexHooksForDoctorTest(t, dir, `{
  "hooks": {
    "SessionStart": [{
      "hooks": [{
        "type": "command",
        "command": "export PATH=\"$HOME/go/bin:$HOME/.local/bin:$PATH\" && gc prime --hook --hook-format codex"
      }]
    }]
  }
}`)

	check := newCodexHooksDriftCheck(dir, []string{dir})
	if err := check.Fix(&doctor.CheckContext{}); err != nil {
		t.Fatalf("Fix: %v", err)
	}
	result := check.Run(&doctor.CheckContext{})
	if result.Status != doctor.StatusOK {
		t.Fatalf("status after fix = %v, want ok; message=%s", result.Status, result.Message)
	}
	data, err := os.ReadFile(filepath.Join(dir, ".codex", "hooks.json"))
	if err != nil {
		t.Fatalf("read hooks: %v", err)
	}
	if !strings.Contains(string(data), "PreCompact") {
		t.Fatalf("fixed hooks missing PreCompact:\n%s", string(data))
	}
}

func TestNewCodexHooksDriftCheckCleansDedupesAndSortsDirs(t *testing.T) {
	check := newCodexHooksDriftCheck("/city", []string{" /z/../z ", "", "/a", "/a/."})

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

func TestCodexHooksDriftCheckFixBindsAgentWorkDirToCityRoot(t *testing.T) {
	cityDir := t.TempDir()
	agentDir := filepath.Join(cityDir, ".gc", "agents", "reviewer")
	writeCodexHooksForDoctorTest(t, agentDir, `{
  "hooks": {
    "SessionStart": [{
      "hooks": [{
        "type": "command",
        "command": "export PATH=\"$HOME/go/bin:$HOME/.local/bin:$PATH\" && gc prime --hook --hook-format codex"
      }]
    }]
  }
}`)

	check := newCodexHooksDriftCheck(cityDir, []string{agentDir})
	if err := check.Fix(&doctor.CheckContext{}); err != nil {
		t.Fatalf("Fix: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(agentDir, ".codex", "hooks.json"))
	if err != nil {
		t.Fatalf("read hooks: %v", err)
	}
	got := string(data)
	if !strings.Contains(got, `gc --city `) {
		t.Fatalf("fixed hooks missing explicit --city binding:\n%s", got)
	}
	if !strings.Contains(got, shellquote.Quote(cityDir)) {
		t.Fatalf("fixed hooks missing city root %q:\n%s", cityDir, got)
	}
	if strings.Contains(got, shellquote.Quote(agentDir)) {
		t.Fatalf("fixed hooks rebound to agent workdir %q:\n%s", agentDir, got)
	}
}

func TestCodexHooksDriftCheckReportsManagedWrongCityBinding(t *testing.T) {
	cityDir := t.TempDir()
	writeCodexHooksForDoctorTest(t, cityDir, `{
  "hooks": {
    "SessionStart": [{
      "hooks": [{
        "type": "command",
        "command": "export PATH=\"$HOME/go/bin:$HOME/.local/bin:$PATH\" && GC_MANAGED_SESSION_HOOK=1 GC_HOOK_EVENT_NAME=SessionStart gc --city /old/city prime --hook --hook-format codex"
      }]
    }],
    "PreCompact": [{
      "hooks": [{
        "type": "command",
        "command": "export PATH=\"$HOME/go/bin:$HOME/.local/bin:$PATH\" && gc --city /old/city handoff --auto --hook-format codex \"context cycle\""
      }]
    }]
  }
}`)

	check := newCodexHooksDriftCheck(cityDir, []string{cityDir})
	result := check.Run(&doctor.CheckContext{})
	if result.Status != doctor.StatusWarning {
		t.Fatalf("status = %v, want warning; message=%s", result.Status, result.Message)
	}
}

func TestCodexHooksDriftCheckFixRebindsManagedWrongCityBinding(t *testing.T) {
	cityDir := t.TempDir()
	writeCodexHooksForDoctorTest(t, cityDir, `{
  "hooks": {
    "SessionStart": [{
      "hooks": [{
        "type": "command",
        "command": "export PATH=\"$HOME/go/bin:$HOME/.local/bin:$PATH\" && GC_MANAGED_SESSION_HOOK=1 GC_HOOK_EVENT_NAME=SessionStart gc --city /old/city prime --hook --hook-format codex"
      }]
    }],
    "PreCompact": [{
      "hooks": [{
        "type": "command",
        "command": "export PATH=\"$HOME/go/bin:$HOME/.local/bin:$PATH\" && gc --city /old/city handoff --auto --hook-format codex \"context cycle\""
      }]
    }]
  }
}`)

	check := newCodexHooksDriftCheck(cityDir, []string{cityDir})
	if err := check.Fix(&doctor.CheckContext{}); err != nil {
		t.Fatalf("Fix: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(cityDir, ".codex", "hooks.json"))
	if err != nil {
		t.Fatalf("read hooks: %v", err)
	}
	got := string(data)
	if !strings.Contains(got, shellquote.Quote(cityDir)) {
		t.Fatalf("fixed hooks missing city root %q:\n%s", cityDir, got)
	}
	if strings.Contains(got, "/old/city") {
		t.Fatalf("stale city binding survived:\n%s", got)
	}
}

// TestStagedCoreCodexHooksAssetIsCurrent pins the contract that replaced
// hooks.NormalizeManagedCodexHooks (gc-fbc9d): the core pack ships its Codex
// hooks overlay as a templated asset, so overlay staging alone leaves a
// workdir whose .codex/hooks.json this check reports as current — no post-hoc
// rewrite of the file staging just wrote.
//
// Staging alone is the load-bearing part. An agent whose resolved provider is
// codex stages that slot without declaring install_agent_hooks, so hooks.Install
// — the other writer of managed form — never runs for it, while this check still
// audits the file (agentUsesCodexHookSurface matches the provider). That gap is
// gc-beez: each tick re-staged a raw overlay over whatever `gc doctor --fix` had
// upgraded, so the check could never go green.
func TestStagedCoreCodexHooksAssetIsCurrent(t *testing.T) {
	cityDir := t.TempDir()
	workDir := t.TempDir()
	overlayDir := materializeCoreOverlayForTest(t)

	if err := runtime.StageProviderOverlayDir(
		overlayDir, workDir, []string{"codex"},
		codexStageTemplateData(cityDir), io.Discard,
	); err != nil {
		t.Fatalf("StageProviderOverlayDir: %v", err)
	}

	staged := filepath.Join(workDir, ".codex", "hooks.json")
	data, err := os.ReadFile(staged)
	if err != nil {
		t.Fatalf("core codex hooks asset not staged at its target name: %v", err)
	}
	if _, err := os.Stat(filepath.Join(workDir, ".codex", "hooks.template.json")); !os.IsNotExist(err) {
		t.Errorf("the templated source staged as a stray file alongside the rendered target")
	}

	check := newCodexHooksDriftCheck(cityDir, []string{workDir})
	if result := check.Run(&doctor.CheckContext{}); result.Status != doctor.StatusOK {
		t.Fatalf("staged core Codex hooks need an upgrade — the codex-hooks-drift\n"+
			"check flags this file on every tick and `gc doctor --fix` is undone by\n"+
			"the next stage (gc-beez).\nstatus=%v message=%s\nstaged content:\n%s",
			result.Status, result.Message, data)
	}
	if want := shellquote.Quote(cityDir); !strings.Contains(string(data), "--city "+want) {
		t.Fatalf("staged hooks missing explicit city binding %q:\n%s", want, data)
	}

	// Every reconciler tick re-stages the overlay over the staged file, so the
	// merge must reach a fixed point rather than accumulating entries.
	if err := runtime.StageProviderOverlayDir(
		overlayDir, workDir, []string{"codex"},
		codexStageTemplateData(cityDir), io.Discard,
	); err != nil {
		t.Fatalf("StageProviderOverlayDir (second pass): %v", err)
	}
	second, err := os.ReadFile(staged)
	if err != nil {
		t.Fatalf("ReadFile(staged, second pass): %v", err)
	}
	if !bytes.Equal(data, second) {
		t.Fatalf("staged Codex hooks are not stable across staging passes:\nfirst:\n%s\nsecond:\n%s", data, second)
	}
}

// TestStagedCoreCodexHooksBindApostropheCityRoot pins that a city root
// containing a shell metacharacter renders a shell-safe --city binding
// (gc-h33ju). The staged command is executed by a shell, and the doctor audits
// it against shellquote.Quote form, so a raw single-quoted binding is both
// malformed shell and permanent drift for such a city.
func TestStagedCoreCodexHooksBindApostropheCityRoot(t *testing.T) {
	cityDir := filepath.Join(t.TempDir(), "gc city'quote")
	if err := os.MkdirAll(cityDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(cityDir): %v", err)
	}
	workDir := t.TempDir()
	overlayDir := materializeCoreOverlayForTest(t)

	if err := runtime.StageProviderOverlayDir(
		overlayDir, workDir, []string{"codex"},
		codexStageTemplateData(cityDir), io.Discard,
	); err != nil {
		t.Fatalf("StageProviderOverlayDir: %v", err)
	}

	staged := filepath.Join(workDir, ".codex", "hooks.json")
	data, err := os.ReadFile(staged)
	if err != nil {
		t.Fatalf("core codex hooks asset not staged at its target name: %v", err)
	}
	// The staged hooks document must be valid JSON: shellquote.Quote renders an
	// embedded apostrophe as the sequence '\'', whose backslash is not a valid
	// JSON string escape, so binding the raw shell-quoted form leaves a document
	// no JSON parser can load. Assert on the decoded commands, not the raw file
	// bytes — the on-disk JSON-escaped bytes and the shell-safe decoded value
	// differ once the render is valid JSON.
	cmds := codexStagedHookCommands(t, data)
	if len(cmds) == 0 {
		t.Fatalf("no managed command strings in staged hooks:\n%s", data)
	}
	want := "--city " + shellquote.Quote(cityDir)
	bad := "--city '" + cityDir + "'"
	for _, cmd := range cmds {
		if !strings.Contains(cmd, want) {
			t.Fatalf("staged hook command missing shell-safe city binding %q:\n%s", want, cmd)
		}
		if strings.Contains(cmd, bad) {
			t.Fatalf("staged hook command carries a malformed unescaped city binding %q:\n%s", bad, cmd)
		}
	}
	check := newCodexHooksDriftCheck(cityDir, []string{workDir})
	if result := check.Run(&doctor.CheckContext{}); result.Status != doctor.StatusOK {
		t.Fatalf("apostrophe city root left staged hooks needing upgrade: status=%v message=%s\n%s",
			result.Status, result.Message, data)
	}
}

// codexStagedHookCommands unmarshals a staged Codex hooks document — failing
// the test if it is not valid JSON — and returns every managed command string
// it carries (the decoded values, not the raw file bytes). The JSON-validity
// check is the point: a staged hooks.json that does not parse is unusable by
// Codex.
func codexStagedHookCommands(t *testing.T, data []byte) []string {
	t.Helper()
	var doc any
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatalf("staged Codex hooks is not valid JSON: %v\n%s", err, data)
	}
	var cmds []string
	var walk func(any)
	walk = func(v any) {
		switch node := v.(type) {
		case map[string]any:
			for key, val := range node {
				if key == "command" {
					if s, ok := val.(string); ok {
						cmds = append(cmds, s)
					}
				}
				walk(val)
			}
		case []any:
			for _, item := range node {
				walk(item)
			}
		}
	}
	walk(doc)
	return cmds
}

// codexStageTemplateData mirrors the subset of the overlay template vocabulary
// materialize.PackTemplateData binds that the core Codex hooks asset expands
// against, for tests that stage the asset directly rather than through the
// production builder. The asset's managed commands expand the city root
// through CityRootShellQuotedJSON — shell-safe and JSON-string-safe — so the
// staged .codex/hooks.json is valid JSON even for a city root with an
// apostrophe (see internal/materialize.CityRootShellQuotedJSON).
func codexStageTemplateData(cityDir string) map[string]string {
	return map[string]string{
		"CityRoot":                cityDir,
		"CityRootShellQuoted":     shellquote.Quote(cityDir),
		"CityRootShellQuotedJSON": materialize.CityRootShellQuotedJSON(cityDir),
	}
}

// materializeCoreOverlayForTest writes the core pack's embedded overlay tree to
// a temp directory, the on-disk shape city bootstrap produces and the shape
// overlay staging consumes.
func materializeCoreOverlayForTest(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	err := fs.WalkDir(core.PackFS, "overlay", func(name string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		dst := filepath.Join(root, filepath.FromSlash(strings.TrimPrefix(name, "overlay/")))
		if d.IsDir() {
			return os.MkdirAll(dst, 0o755)
		}
		data, err := fs.ReadFile(core.PackFS, name)
		if err != nil {
			return err
		}
		return os.WriteFile(dst, data, 0o644)
	})
	if err != nil {
		t.Fatalf("materializing core overlay: %v", err)
	}
	return root
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

func TestCodexHooksMissingPreCompactRejectsUnreadableAndMalformedFiles(t *testing.T) {
	dir := t.TempDir()
	missingPath := filepath.Join(dir, ".codex", "hooks.json")
	if codexHooksMissingPreCompact(missingPath) {
		t.Fatal("missing file reported as stale")
	}

	writeCodexHooksForDoctorTest(t, dir, `{not-json`)
	if codexHooksMissingPreCompact(missingPath) {
		t.Fatal("malformed JSON reported as stale")
	}

	writeCodexHooksForDoctorTest(t, dir, `{"notHooks": {}}`)
	if codexHooksMissingPreCompact(missingPath) {
		t.Fatal("file without hooks map reported as stale")
	}
}

func TestCodexHooksMissingPreCompactRequiresManagedCommand(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".codex", "hooks.json")
	writeCodexHooksForDoctorTest(t, dir, `{
  "hooks": {
    "UserPromptSubmit": [{
      "hooks": [{
        "type": "command",
        "command": "printf custom"
      }]
    }]
  }
}`)

	if codexHooksMissingPreCompact(path) {
		t.Fatal("custom-only hooks reported as missing managed PreCompact")
	}
}

func TestCodexHooksNeedUpgradeRejectsUnreadableMalformedAndCustomFiles(t *testing.T) {
	dir := t.TempDir()
	missingPath := filepath.Join(dir, ".codex", "hooks.json")
	if codexHooksNeedUpgrade(missingPath, "/city") {
		t.Fatal("missing file reported stale")
	}

	writeCodexHooksForDoctorTest(t, dir, `{not-json`)
	if codexHooksNeedUpgrade(missingPath, "/city") {
		t.Fatal("malformed JSON reported stale")
	}

	writeCodexHooksForDoctorTest(t, dir, `{"hooks":{"UserPromptSubmit":[{"hooks":[{"type":"command","command":"FOO=1 gc mail check --inject --hook-format codex"}]}]}}`)
	if codexHooksNeedUpgrade(missingPath, "/city") {
		t.Fatal("env-prefixed custom hooks reported stale")
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
