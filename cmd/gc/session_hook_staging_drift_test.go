package main

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/hooks"
	"github.com/gastownhall/gascity/internal/runtime"
)

// codexHookStagingCity builds a city whose codex agent declares installHooks
// as its install_agent_hooks. Installing for codex writes no work-dir file, so
// whatever that names, nothing but overlay staging writes the work dir's
// .codex/hooks.json. Its pack overlay ships overlayHooks as that file.
func codexHookStagingCity(t *testing.T, overlayHooks string, installHooks []string) (cityPath string, cfg *config.City) {
	t.Helper()
	cityPath = t.TempDir()
	packOverlay := filepath.Join(cityPath, "packs", "core", "overlay")
	overlayHook := filepath.Join(packOverlay, "per-provider", "codex", ".codex", "hooks.json")
	if err := os.MkdirAll(filepath.Dir(overlayHook), 0o755); err != nil {
		t.Fatalf("mkdir codex overlay: %v", err)
	}
	if err := os.WriteFile(overlayHook, []byte(overlayHooks), 0o644); err != nil {
		t.Fatalf("write codex overlay: %v", err)
	}
	cfg = &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Providers: map[string]config.ProviderSpec{
			"codex": {Command: "echo", PromptMode: "none"},
		},
		PackOverlayDirs: []string{packOverlay},
		Agents: []config.Agent{{
			Name:              "run-operator",
			Provider:          "codex",
			MaxActiveSessions: intPtr(1),
			ScaleCheck:        "echo 1",
			WorkDir:           "worker",
			InstallAgentHooks: installHooks,
		}},
	}
	return cityPath, cfg
}

func onlyDesiredSession(t *testing.T, state map[string]TemplateParams) (string, TemplateParams) {
	t.Helper()
	if len(state) != 1 {
		t.Fatalf("desired state size = %d, want 1: %#v", len(state), state)
	}
	for name, tp := range state {
		return name, tp
	}
	return "", TemplateParams{}
}

// TestConfigDrift_SessionStartHookStagingIsNotDrift reproduces the fresh-worker
// config-drift drain: a codex session starts, the session-start overlay staging
// writes the work dir's .codex/hooks.json (a file GC itself generates), and the
// next tick's CopyFiles probe finds a hook file the started fingerprint never
// saw. The started fingerprint and the next tick's fingerprint must agree, and
// the reconciler must not drain the session for it.
//
// The pack overlays here ship Gas City's own hook commands. Session-start
// staging copies them into the work dir after the fingerprint, and every tick
// stages the file and strips them again before it probes the file, because a
// Codex session takes them from its launch command. The probe therefore reads
// the same content on every tick: no file when the pack ships nothing else,
// and the pack's own hooks when it does. Both hold whatever
// install_agent_hooks names.
func TestConfigDrift_SessionStartHookStagingIsNotDrift(t *testing.T) {
	for _, tc := range []struct {
		name         string
		overlayHooks string
		installHooks []string
		wantFile     bool
	}{
		{name: "pack ships only managed hooks", overlayHooks: gastownCodexOverlayHooks},
		{name: "pack ships managed hooks and its own", overlayHooks: packCodexOverlayHooks, wantFile: true},
		{name: "install_agent_hooks names codex, pack ships only managed hooks", overlayHooks: gastownCodexOverlayHooks, installHooks: []string{"codex"}},
		{name: "install_agent_hooks names codex, pack ships managed hooks and its own", overlayHooks: packCodexOverlayHooks, installHooks: []string{"codex"}, wantFile: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cityPath, cfg := codexHookStagingCity(t, tc.overlayHooks, tc.installHooks)

			first := buildDesiredState("test-city", cityPath, time.Now().UTC(), cfg, runtime.NewFake(), nil, io.Discard)
			sessionName, firstTP := onlyDesiredSession(t, first.State)
			startedCfg := templateParamsToConfig(firstTP)
			startedHash := runtime.CoreFingerprint(startedCfg)

			// The session start: every runtime provider stages the provider overlay
			// (pack overlays + agent overlay) into the work dir before the process
			// launches (tmux.stageStartFiles, runtime.StageSessionWorkDir).
			if err := runtime.StageSessionWorkDir(startedCfg); err != nil {
				t.Fatalf("StageSessionWorkDir: %v", err)
			}
			workHook := filepath.Join(startedCfg.WorkDir, ".codex", "hooks.json")
			data, err := os.ReadFile(workHook)
			if err != nil {
				t.Fatalf("session-start staging did not write %s: %v", workHook, err)
			}
			if !hooks.CodexHooksHaveManagedEntries(data) {
				t.Fatalf("session-start staging wrote no managed copy into %s, so the strip below proves nothing\n%s", workHook, data)
			}

			second := buildDesiredState("test-city", cityPath, time.Now().UTC(), cfg, runtime.NewFake(), nil, io.Discard)
			secondTP, ok := second.State[sessionName]
			if !ok {
				t.Fatalf("second desired state missing %q: %#v", sessionName, second.State)
			}
			currentCfg := templateParamsToConfig(secondTP)
			if got := runtime.CoreFingerprint(currentCfg); got != startedHash {
				t.Errorf("core fingerprint moved after GC staged its own hook file: started=%s current=%s drifted=%v\nstarted CopyFiles=%#v\ncurrent CopyFiles=%#v",
					startedHash, got, runtime.CoreFingerprintDriftFields(runtime.CoreFingerprintBreakdown(startedCfg), currentCfg),
					startedCfg.CopyFiles, currentCfg.CopyFiles)
			}
			data, err = os.ReadFile(workHook)
			switch {
			case !tc.wantFile && !os.IsNotExist(err):
				t.Errorf("tick left %s holding only managed copies (err=%v)\n%s", workHook, err, data)
			case tc.wantFile && err != nil:
				t.Errorf("tick removed %s, which holds the pack's own hook: %v", workHook, err)
			case tc.wantFile && hooks.CodexHooksHaveManagedEntries(data):
				t.Errorf("managed copies survived the tick in %s\n%s", workHook, data)
			}

			// The reconciler's drift check over the same two ticks.
			env := newReconcilerTestEnv(t)
			env.cfg = cfg
			env.desiredState[sessionName] = secondTP
			if err := env.sp.Start(context.Background(), sessionName, startedCfg); err != nil {
				t.Fatalf("Start: %v", err)
			}
			session := env.createSessionBead(sessionName, secondTP.TemplateName)
			env.markSessionActive(&session)
			env.setSessionMetadata(&session, map[string]string{
				"started_config_hash": startedHash,
				"started_live_hash":   runtime.LiveFingerprint(startedCfg),
				"live_hash":           runtime.LiveFingerprint(startedCfg),
			})
			env.reconcile([]beads.Bead{session})

			if ds := env.dt.get(session.ID); ds != nil && ds.reason == "config-drift" {
				t.Fatalf("fresh codex session drained for config-drift after GC staged its own hooks file: %+v\nstderr=%s", ds, env.stderr.String())
			}
			if strings.Contains(env.stderr.String(), "config-drift "+sessionName) {
				t.Fatalf("reconciler reported config drift for GC-staged hook file:\n%s", env.stderr.String())
			}
		})
	}
}

// TestConfigDrift_RealHookContentChangeStillDrifts guards the other half of the
// invariant: once the hook file is part of the started fingerprint, an edit to
// its content is still a real config change.
func TestConfigDrift_RealHookContentChangeStillDrifts(t *testing.T) {
	cityPath, cfg := codexHookStagingCity(t, packCodexOverlayHooks, nil)

	first := buildDesiredState("test-city", cityPath, time.Now().UTC(), cfg, runtime.NewFake(), nil, io.Discard)
	sessionName, firstTP := onlyDesiredSession(t, first.State)
	startedCfg := templateParamsToConfig(firstTP)
	startedHash := runtime.CoreFingerprint(startedCfg)
	if err := runtime.StageSessionWorkDir(startedCfg); err != nil {
		t.Fatalf("StageSessionWorkDir: %v", err)
	}

	workHook := filepath.Join(startedCfg.WorkDir, ".codex", "hooks.json")
	edited := `{"hooks":{"SessionStart":[{"matcher":"","hooks":[{"type":"command","command":"operator-edited"}]}]}}`
	if err := os.WriteFile(workHook, []byte(edited), 0o644); err != nil {
		t.Fatalf("edit %s: %v", workHook, err)
	}
	// A real edit to the pack overlay is what changes what GC stages.
	overlayHook := filepath.Join(cfg.PackOverlayDirs[0], "per-provider", "codex", ".codex", "hooks.json")
	if err := os.WriteFile(overlayHook, []byte(edited), 0o644); err != nil {
		t.Fatalf("edit %s: %v", overlayHook, err)
	}

	second := buildDesiredState("test-city", cityPath, time.Now().UTC(), cfg, runtime.NewFake(), nil, io.Discard)
	secondTP, ok := second.State[sessionName]
	if !ok {
		t.Fatalf("second desired state missing %q", sessionName)
	}
	if got := runtime.CoreFingerprint(templateParamsToConfig(secondTP)); got == startedHash {
		t.Fatalf("core fingerprint unchanged after a real hook content change: %s", got)
	}
}
