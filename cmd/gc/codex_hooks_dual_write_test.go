package main

import (
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

// stageCodex runs the staging half of the build_desired_state home-dir tick.
// skipMergeable selects the fixed (skip) vs legacy (no-skip) path.
func stageCodex(t *testing.T, overlaySrc, workDir string, skipMergeable bool) {
	t.Helper()
	var err error
	if skipMergeable {
		err = runtime.StageProviderOverlayDirSkippingMergeable(overlaySrc, workDir, []string{"codex"}, nil)
	} else {
		err = runtime.StageProviderOverlayDir(overlaySrc, workDir, []string{"codex"}, nil)
	}
	if err != nil {
		t.Fatalf("stage codex overlay (skip=%v): %v", skipMergeable, err)
	}
}

// installCodex runs the hooks.Install half of the tick on the same workDir.
func installCodex(t *testing.T, cityDir, workDir string) {
	t.Helper()
	if err := hooks.Install(fsys.OSFS{}, cityDir, workDir, []string{"codex"}); err != nil {
		t.Fatalf("hooks.Install codex: %v", err)
	}
}

// TestCodexHooksTickStripsManagedCopies covers the reconcile tick's two
// writers of a Codex agent's .codex/hooks.json. Session-start staging merges a
// pack overlay that ships Gas City's own hook commands into the file, and the
// tick's hooks.Install removes every managed entry again, because a Codex
// session gets those hooks from its launch command and a copy in a file Codex
// reads would run them twice. The pack's own hook stays, and the skipping
// staging path the tick uses never brings the managed copy back.
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

	seedDriftedHybrid(t, cityDir, workDir)
	stageCodex(t, overlaySrc, workDir, false)
	if data, _ := os.ReadFile(hooksPath); !hooks.CodexHooksHaveManagedEntries(data) {
		t.Fatalf("session-start staging did not merge the pack's managed copy in; the strip below proves nothing\n%s", data)
	}
	stageCodex(t, overlaySrc, workDir, true)
	installCodex(t, cityDir, workDir)
	assertOnlyPackHook(t, "after the tick")
	stageCodex(t, overlaySrc, workDir, true)
	assertOnlyPackHook(t, "after the next tick's staging")
}

// TestMaterializeProviderOverlays_SkipsMergeableCodexHook guards the production
// caller wiring: materializeProviderOverlaysBeforeFingerprint (the
// staging-only half of prepareTemplateResolution) must skip the reconciler-owned
// mergeable .codex/hooks.json while still staging non-mergeable overlay
// siblings. This is the observation point where skip vs non-skip staging
// diverge — the trailing hooks.Install converges either way, so only the
// staging-only state distinguishes a reverted caller wiring.
func TestMaterializeProviderOverlays_SkipsMergeableCodexHook(t *testing.T) {
	cityDir := t.TempDir()
	rigDir := filepath.Join(cityDir, "myrig")
	if err := os.MkdirAll(rigDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(rig): %v", err)
	}
	overlayDir := filepath.Join(cityDir, "packs", "myrig", "overlay")
	codexOverlay := filepath.Join(overlayDir, "per-provider", "codex", ".codex")
	if err := os.MkdirAll(codexOverlay, 0o755); err != nil {
		t.Fatalf("MkdirAll(overlay): %v", err)
	}
	if err := os.WriteFile(filepath.Join(codexOverlay, "hooks.json"), []byte(`{"hooks":{"SessionStart":[]}}`), 0o644); err != nil {
		t.Fatalf("write codex hooks overlay: %v", err)
	}
	sibling := filepath.Join(overlayDir, "per-provider", "codex", "AGENTS.codex.md")
	if err := os.WriteFile(sibling, []byte("codex"), 0o644); err != nil {
		t.Fatalf("write codex sibling overlay: %v", err)
	}

	codexBase := "builtin:codex"
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Agents: []config.Agent{{
			Name:     "polecat",
			Provider: "codex",
			Scope:    "rig",
			Dir:      "myrig",
		}},
		Providers: map[string]config.ProviderSpec{
			// Explicit command + resume_command so resolution does not depend on
			// a real codex binary on PATH; base still yields the codex family.
			"codex": {Base: &codexBase, Command: "/bin/echo", ResumeCommand: "/bin/echo resume {{.SessionKey}}"},
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

	if _, err := os.Stat(filepath.Join(workDir, ".codex", "hooks.json")); !os.IsNotExist(err) {
		t.Fatalf("build_desired_state staging wrote reconciler-owned .codex/hooks.json (err=%v); caller must use the skip variant so hooks.Install is sole writer", err)
	}
	if _, err := os.Stat(filepath.Join(workDir, "AGENTS.codex.md")); err != nil {
		t.Fatalf("non-mergeable codex overlay sibling not staged: %v", err)
	}
}
