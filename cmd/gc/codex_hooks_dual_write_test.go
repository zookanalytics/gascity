package main

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/fsys"
	"github.com/gastownhall/gascity/internal/hooks"
	"github.com/gastownhall/gascity/internal/runtime"
	"github.com/gastownhall/gascity/internal/session"
	"github.com/gastownhall/gascity/internal/shellquote"
)

// packCodexOverlayHooks is a pack's per-provider/codex/.codex/hooks.json that
// ships Gas City's own hook commands, unbound to a city, next to a hook of the
// pack's own.
const packCodexOverlayHooks = `{
  "hooks": {
    "SessionStart": [{"matcher": "", "hooks": [{"type": "command", "command": "export PATH=\"$PATH:$HOME/go/bin:$HOME/.local/bin\" && GC_MANAGED_SESSION_HOOK=1 GC_HOOK_EVENT_NAME=SessionStart \"${GC_BIN:-gc}\" prime --hook --hook-format codex"}]}],
    "PreCompact": [{"matcher": "", "hooks": [{"type": "command", "command": "export PATH=\"$PATH:$HOME/go/bin:$HOME/.local/bin\" && \"${GC_BIN:-gc}\" handoff --auto --hook-format codex \"context cycle\""}]}],
    "UserPromptSubmit": [{"matcher": "", "hooks": [
      {"type": "command", "command": "export PATH=\"$PATH:$HOME/go/bin:$HOME/.local/bin\" && \"${GC_BIN:-gc}\" hook run --timeout 15s --timeout-exit-code 0 -- nudge drain --inject --hook-format codex"},
      {"type": "command", "command": "export PATH=\"$PATH:$HOME/go/bin:$HOME/.local/bin\" && \"${GC_BIN:-gc}\" hook run --timeout 15s --timeout-exit-code 0 -- mail check --inject --hook-format codex"}
    ]}],
    "PreToolUse": [{"matcher": "Bash", "hooks": [{"type": "command", "command": "/opt/pack-guard.sh"}]}]
  }
}`

// gastownCodexOverlayHooks is a pack's per-provider/codex/.codex/hooks.json
// that ships nothing but Gas City's own hook commands. It is copied from the
// gastown pack's codex overlay in gascity-packs.
const gastownCodexOverlayHooks = `{
  "hooks": {
    "SessionStart": [{"matcher": "startup", "hooks": [{"type": "command", "command": "export PATH=\"$HOME/go/bin:$HOME/.local/bin:$PATH\" && GC_MANAGED_SESSION_HOOK=1 GC_HOOK_EVENT_NAME=SessionStart gc prime --hook --hook-format codex"}]}],
    "PreCompact": [{"matcher": "", "hooks": [{"type": "command", "command": "export PATH=\"$HOME/go/bin:$HOME/.local/bin:$PATH\" && gc handoff --auto \"context cycle\""}]}],
    "UserPromptSubmit": [{"matcher": "", "hooks": [
      {"type": "command", "command": "export PATH=\"$HOME/go/bin:$HOME/.local/bin:$PATH\" && gc nudge drain --inject --hook-format codex"},
      {"type": "command", "command": "export PATH=\"$HOME/go/bin:$HOME/.local/bin:$PATH\" && gc mail check --inject --hook-format codex"}
    ]}]
  }
}`

// seedCodexOverlay writes packCodexOverlayHooks into a temp overlay source dir
// (per-provider/codex/.codex/hooks.json) for staging to copy.
func seedCodexOverlay(t *testing.T) string {
	t.Helper()
	src := t.TempDir()
	dstDir := filepath.Join(src, "per-provider", "codex", ".codex")
	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		t.Fatalf("mkdir codex overlay: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dstDir, "hooks.json"), []byte(packCodexOverlayHooks), 0o644); err != nil {
		t.Fatalf("write codex overlay: %v", err)
	}
	return src
}

// driftedCodexHooks is a live hybrid captured from a drifted Codex agent: a
// city-bound `matcher:"startup"` SessionStart entry next to an unbound
// `matcher:""` `gc prime` entry, plus unbound PreCompact and UserPromptSubmit
// entries. Every entry in it is a managed copy.
const driftedCodexHooks = `{
  "hooks": {
    "PreCompact": [
      {
        "hooks": [
          {
            "command": "export PATH=\"$HOME/go/bin:$HOME/.local/bin:$PATH\" && gc handoff --auto --hook-format codex \"context cycle\"",
            "type": "command"
          }
        ],
        "matcher": ""
      }
    ],
    "SessionStart": [
      {
        "hooks": [
          {
            "command": "export PATH=\"$HOME/go/bin:$HOME/.local/bin:$PATH\" && GC_MANAGED_SESSION_HOOK=1 GC_HOOK_EVENT_NAME=SessionStart gc --city '__CITY__' prime --hook --hook-format codex",
            "type": "command"
          }
        ],
        "matcher": "startup"
      },
      {
        "hooks": [
          {
            "command": "export PATH=\"$HOME/go/bin:$HOME/.local/bin:$PATH\" && GC_MANAGED_SESSION_HOOK=1 GC_HOOK_EVENT_NAME=SessionStart gc prime --hook --hook-format codex",
            "type": "command"
          }
        ],
        "matcher": ""
      }
    ],
    "UserPromptSubmit": [
      {
        "hooks": [
          {
            "command": "export PATH=\"$HOME/go/bin:$HOME/.local/bin:$PATH\" && gc hook run --timeout 15s --timeout-exit-code 0 -- nudge drain --inject --hook-format codex",
            "type": "command"
          },
          {
            "command": "export PATH=\"$HOME/go/bin:$HOME/.local/bin:$PATH\" && gc hook run --timeout 15s --timeout-exit-code 0 -- mail check --inject --hook-format codex",
            "type": "command"
          }
        ],
        "matcher": ""
      }
    ]
  }
}`

// seedDriftedHybrid writes the live hybrid fixture into workDir/.codex/hooks.json
// with its bound SessionStart entry pinned to cityDir, reproducing the drifted
// starting state a reconcile tick must converge.
func seedDriftedHybrid(t *testing.T, cityDir, workDir string) {
	t.Helper()
	dir := filepath.Join(workDir, ".codex")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir .codex: %v", err)
	}
	body := strings.ReplaceAll(driftedCodexHooks, "__CITY__", cityDir)
	if err := os.WriteFile(filepath.Join(dir, "hooks.json"), []byte(body), 0o644); err != nil {
		t.Fatalf("seed drifted hybrid: %v", err)
	}
}

// stageCodex stages the codex overlay into workDir. onTick selects the
// build_desired_state home-dir tick's path, which skips only the mergeable
// files hooks.Install writes for codex, over session-start staging, which skips
// nothing.
func stageCodex(t *testing.T, overlaySrc, workDir string, onTick bool) {
	t.Helper()
	var err error
	if onTick {
		installed := hooks.InstalledWorkDirPaths([]string{"codex"}, nil)
		err = runtime.StageProviderOverlayDirSkippingInstalledMergeable(overlaySrc, workDir, []string{"codex"}, func(relPath string) bool { return installed[relPath] }, nil)
	} else {
		err = runtime.StageProviderOverlayDir(overlaySrc, workDir, []string{"codex"}, nil)
	}
	if err != nil {
		t.Fatalf("stage codex overlay (onTick=%v): %v", onTick, err)
	}
}

// installCodex runs the hooks.Install half of the tick on the same workDir.
func installCodex(t *testing.T, cityDir, workDir string) {
	t.Helper()
	if err := hooks.Install(fsys.OSFS{}, cityDir, workDir, []string{"codex"}); err != nil {
		t.Fatalf("hooks.Install codex: %v", err)
	}
}

// TestCodexHooksTickStripsManagedCopies covers the writers of a Codex agent's
// .codex/hooks.json. Session-start staging and the tick's staging both merge a
// pack overlay that ships Gas City's own hook commands into the file, and the
// tick's hooks.Install removes every managed entry again, because a Codex
// session gets those hooks from its launch command and a copy in a file Codex
// reads would run them twice. The pack's own hook stays, and every tick leaves
// the same document, whatever session-start staging merged in since the last.
func TestCodexHooksTickStripsManagedCopies(t *testing.T) {
	overlaySrc := seedCodexOverlay(t)
	cityDir := t.TempDir()
	workDir := t.TempDir()
	hooksPath := filepath.Join(workDir, ".codex", "hooks.json")

	assertOnlyPackHook := func(t *testing.T, when string) {
		t.Helper()
		data, err := os.ReadFile(hooksPath)
		if err != nil {
			t.Fatalf("%s: read %s: %v", when, hooksPath, err)
		}
		if hooks.CodexHooksHaveManagedEntries(data) {
			t.Fatalf("%s: managed hook entries survived\n%s", when, data)
		}
		var doc struct {
			Hooks map[string][]struct {
				Hooks []struct {
					Command string `json:"command"`
				} `json:"hooks"`
			} `json:"hooks"`
		}
		if err := json.Unmarshal(data, &doc); err != nil {
			t.Fatalf("%s: unmarshal %s: %v", when, hooksPath, err)
		}
		var commands []string
		for _, entries := range doc.Hooks {
			for _, e := range entries {
				for _, h := range e.Hooks {
					commands = append(commands, h.Command)
				}
			}
		}
		if len(commands) != 1 || commands[0] != "/opt/pack-guard.sh" {
			t.Fatalf("%s: hooks = %q, want only the pack's own hook\n%s", when, commands, data)
		}
	}

	startSession := func(t *testing.T) {
		t.Helper()
		stageCodex(t, overlaySrc, workDir, false)
		if data, _ := os.ReadFile(hooksPath); !hooks.CodexHooksHaveManagedEntries(data) {
			t.Fatalf("session-start staging did not merge the pack's managed copy in; the strip below proves nothing\n%s", data)
		}
	}
	tick := func() {
		stageCodex(t, overlaySrc, workDir, true)
		installCodex(t, cityDir, workDir)
	}

	seedDriftedHybrid(t, cityDir, workDir)
	startSession(t)
	tick()
	assertOnlyPackHook(t, "after the tick")
	afterTick, err := os.ReadFile(hooksPath)
	if err != nil {
		t.Fatalf("read %s: %v", hooksPath, err)
	}
	startSession(t)
	tick()
	assertOnlyPackHook(t, "after the next tick")
	if again, _ := os.ReadFile(hooksPath); !bytes.Equal(again, afterTick) {
		t.Fatalf("the next tick left a different document\nfirst:\n%s\nnext:\n%s", afterTick, again)
	}
}

// TestResolveTemplateRegistersCodexHooksOnACPLaunch covers a Codex template
// whose provider has an ACP launch but which resolves the CLI transport, here
// by naming it, as a template moved off ACP does. It keeps an ACP launch for a
// session recorded on ACP (templateParamsForSession), and that launch is the
// command the same agent resolves on the ACP transport, so it registers Gas
// City's managed hooks as that command does. A session kept on its ACP launch
// then starts with its hooks, and the drift hash it is compared by reads the
// same command it started with.
func TestResolveTemplateRegistersCodexHooksOnACPLaunch(t *testing.T) {
	supportsACP := true
	params := upstreamTestParams(t, &config.City{})
	params.workspace = &config.Workspace{Provider: "codex-acp"}
	params.providers = map[string]config.ProviderSpec{"codex-acp": {
		Base:        stringPtr("builtin:codex"),
		ACPCommand:  "codex-acp",
		SupportsACP: &supportsACP,
	}}
	hookArgs, err := hooks.CodexLaunchArgs(params.cityPath)
	if err != nil {
		t.Fatalf("CodexLaunchArgs: %v", err)
	}
	registration := shellquote.Join(hookArgs)
	resolve := func(transport string) TemplateParams {
		t.Helper()
		agent := &config.Agent{Name: "runner", Session: transport}
		tp, err := resolveTemplate(params, agent, agent.QualifiedName(), nil)
		if err != nil {
			t.Fatalf("resolveTemplate(session=%q): %v", transport, err)
		}
		return tp
	}

	cli := resolve(config.SessionTransportTmux)
	if cli.IsACP || cli.ACPLaunch == nil {
		t.Fatalf("template IsACP = %v, ACPLaunch = %v; want a CLI template that keeps an ACP launch", cli.IsACP, cli.ACPLaunch)
	}
	if !strings.Contains(cli.Command, registration) {
		t.Fatalf("CLI launch %q does not register the managed hooks %q", cli.Command, registration)
	}
	if !strings.Contains(cli.ACPLaunch.Command, registration) {
		t.Fatalf("ACP launch %q does not register the managed hooks %q", cli.ACPLaunch.Command, registration)
	}
	acp := resolve(config.SessionTransportACP)
	if !acp.IsACP {
		t.Fatalf("template with session=%q resolved IsACP = false", config.SessionTransportACP)
	}
	if cli.ACPLaunch.Command != acp.Command {
		t.Fatalf("ACP launch = %q, want the command the agent resolves on the ACP transport, %q", cli.ACPLaunch.Command, acp.Command)
	}

	recorded := session.Info{Transport: config.SessionTransportACP}
	kept, swapped := templateParamsForSession(cli, recorded)
	if !swapped || kept.Command != acp.Command {
		t.Fatalf("session recorded on ACP starts with %q (swapped = %v), want %q", kept.Command, swapped, acp.Command)
	}
	if got := sessionCoreConfigForHashInfo(cli, recorded).Command; got != acp.Command {
		t.Fatalf("drift hash of a session recorded on ACP reads %q, want %q", got, acp.Command)
	}
}

// TestMaterializeProviderOverlays_SkipsInstallerOwnedMergeableHook guards the
// production caller wiring: materializeProviderOverlaysBeforeFingerprint (the
// staging-only half of prepareTemplateResolution) must skip a reconciler-owned
// mergeable hook file that hooks.Install writes on the same tick
// (install_agent_hooks names gemini, whose installer writes
// .gemini/settings.json), while still staging non-mergeable overlay siblings.
// This is the observation point where skip vs non-skip staging diverge — the
// trailing hooks.Install converges either way, so only the staging-only state
// distinguishes a reverted caller wiring.
func TestMaterializeProviderOverlays_SkipsInstallerOwnedMergeableHook(t *testing.T) {
	workDir := materializeProviderOverlayForTest(t, "gemini", []string{"gemini"}, map[string]string{
		".gemini/settings.json": `{"hooks":{"SessionStart":[]}}`,
		"AGENTS.gemini.md":      "gemini",
	})
	if _, err := os.Stat(filepath.Join(workDir, ".gemini", "settings.json")); !os.IsNotExist(err) {
		t.Fatalf("build_desired_state staging wrote reconciler-owned .gemini/settings.json (err=%v); caller must use the skip variant so hooks.Install is sole writer", err)
	}
	if _, err := os.Stat(filepath.Join(workDir, "AGENTS.gemini.md")); err != nil {
		t.Fatalf("non-mergeable gemini overlay sibling not staged: %v", err)
	}
}

// TestMaterializeProviderOverlays_StagesCodexHookWithoutManagedCopies covers a
// Codex agent's .codex/hooks.json on the tick. Installing for codex writes no
// work-dir file, because a Codex session takes Gas City's managed hooks on its
// launch command, so whatever install_agent_hooks names, the staging pass is
// the file's only tick writer and stages a pack's copy before the fingerprint.
// It then strips the managed entries from it, since a copy Codex reads would
// run each managed hook a second time. The pack's own hooks stay, and a file
// left holding only managed entries is removed.
func TestMaterializeProviderOverlays_StagesCodexHookWithoutManagedCopies(t *testing.T) {
	for _, tc := range []struct {
		name         string
		installHooks []string
	}{
		{name: "install_agent_hooks names codex", installHooks: []string{"codex"}},
		{name: "install_agent_hooks omits codex"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			workDir := materializeProviderOverlayForTest(t, "codex", tc.installHooks, map[string]string{
				".codex/hooks.json": packCodexOverlayHooks,
				"AGENTS.codex.md":   "codex",
			})
			data, err := os.ReadFile(filepath.Join(workDir, ".codex", "hooks.json"))
			if err != nil {
				t.Fatalf("staging skipped .codex/hooks.json, which no installer writes on the tick: %v", err)
			}
			if hooks.CodexHooksHaveManagedEntries(data) {
				t.Fatalf("managed hook entries survived the tick's staging pass\n%s", data)
			}
			if !strings.Contains(string(data), "/opt/pack-guard.sh") {
				t.Fatalf("the pack's own hook did not survive the tick's staging pass\n%s", data)
			}
			if _, err := os.Stat(filepath.Join(workDir, "AGENTS.codex.md")); err != nil {
				t.Fatalf("non-mergeable codex overlay sibling not staged: %v", err)
			}

			workDir = materializeProviderOverlayForTest(t, "codex", tc.installHooks, map[string]string{
				".codex/hooks.json": gastownCodexOverlayHooks,
			})
			if _, err := os.Stat(filepath.Join(workDir, ".codex", "hooks.json")); !os.IsNotExist(err) {
				t.Fatalf("a hooks file holding only managed copies survived the tick's staging pass (err=%v)", err)
			}
		})
	}
}

// materializeProviderOverlayForTest runs the staging-only half of
// prepareTemplateResolution for a rig-scoped agent of the given builtin
// provider family, with the given install_agent_hooks and with files (paths
// relative to the provider's per-provider overlay) in its rig's pack overlay,
// and returns its work dir.
func materializeProviderOverlayForTest(t *testing.T, family string, installHooks []string, files map[string]string) string {
	t.Helper()
	cityDir := t.TempDir()
	rigDir := filepath.Join(cityDir, "myrig")
	if err := os.MkdirAll(rigDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(rig): %v", err)
	}
	overlayDir := filepath.Join(cityDir, "packs", "myrig", "overlay")
	for rel, body := range files {
		dst := filepath.Join(overlayDir, "per-provider", family, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			t.Fatalf("MkdirAll(%s): %v", filepath.Dir(dst), err)
		}
		if err := os.WriteFile(dst, []byte(body), 0o644); err != nil {
			t.Fatalf("write %s overlay %s: %v", family, rel, err)
		}
	}

	base := "builtin:" + family
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Agents: []config.Agent{{
			Name:              "polecat",
			Provider:          family,
			Scope:             "rig",
			Dir:               "myrig",
			InstallAgentHooks: installHooks,
		}},
		Providers: map[string]config.ProviderSpec{
			// Explicit command + resume_command so resolution does not depend on
			// a real provider binary on PATH; base still yields the family.
			family: {Base: &base, Command: "/bin/echo", ResumeCommand: "/bin/echo resume {{.SessionKey}}"},
		},
		Rigs:           []config.Rig{{Name: "myrig", Path: rigDir}},
		RigOverlayDirs: map[string][]string{"myrig": {overlayDir}},
	}

	bp := newAgentBuildParams("test-city", cityDir, cfg, runtime.NewFake(), time.Now().UTC(), nil, io.Discard)
	cfgAgent := &cfg.Agents[0]
	resolved, err := config.ResolveProvider(cfgAgent, bp.workspace, bp.providers, bp.lookPath)
	if err != nil {
		t.Fatalf("ResolveProvider: %v", err)
	}
	workDir, err := resolveConfiguredWorkDir(bp.cityPath, bp.cityName, "myrig/polecat", cfgAgent, bp.rigs)
	if err != nil {
		t.Fatalf("resolveConfiguredWorkDir: %v", err)
	}
	rigName := sessionSetupContextForAgent(bp.cityPath, bp.cityName, "myrig/polecat", cfgAgent, bp.rigs).Rig

	// Staging only — hooks.Install is a separate step in prepareTemplateResolution.
	materializeProviderOverlaysBeforeFingerprint(bp, cfgAgent, resolved, "myrig/polecat", rigName, workDir, io.Discard)
	return workDir
}
