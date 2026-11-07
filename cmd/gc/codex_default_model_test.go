package main

import (
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/fsys"
)

// resolveCodexBuiltinTemplate resolves a codex agent through the production
// template path against a city that declares `[providers.codex] base =
// "builtin:codex"` and nothing else, the shape a stock city uses.
func resolveCodexBuiltinTemplate(t *testing.T, optionDefaults map[string]string) TemplateParams {
	t.Helper()

	params := &agentBuildParams{
		cityName:   "codex-city",
		cityPath:   t.TempDir(),
		workspace:  &config.Workspace{Provider: "codex"},
		providers:  builtinProviderAliasesForTest("codex"),
		lookPath:   func(name string) (string, error) { return filepath.Join("/usr/bin", name), nil },
		fs:         fsys.OSFS{},
		beaconTime: time.Unix(0, 0),
		beadNames:  make(map[string]string),
		stderr:     io.Discard,
	}
	agentCfg := &config.Agent{
		Name:           "worker",
		Provider:       "codex",
		Session:        "tmux",
		WorkDir:        filepath.Join(".gc", "agents", "codex-default-model"),
		OptionDefaults: optionDefaults,
	}
	tp, err := resolveTemplate(params, agentCfg, agentCfg.QualifiedName(), nil)
	if err != nil {
		t.Fatalf("resolveTemplate(codex): %v", err)
	}
	return tp
}

// A builtin:codex session with no model or effort chosen anywhere must leave
// both to the user's own Codex configuration (~/.codex/config.toml). Forcing
// a catalog model broke every turn for ChatGPT-account logins whose plan does
// not serve that model.
func TestResolveTemplate_CodexBuiltinWithoutChoiceDefersModelAndEffortToCodexConfig(t *testing.T) {
	tp := resolveCodexBuiltinTemplate(t, nil)

	fields := strings.Fields(tp.Command)
	for _, tok := range fields {
		if tok == "--model" || tok == "-m" {
			t.Fatalf("command %q passes a model flag without an explicit model choice", tp.Command)
		}
	}
	if strings.Contains(tp.Command, "model_reasoning_effort") {
		t.Fatalf("command %q passes a reasoning effort without an explicit effort choice", tp.Command)
	}
	if !strings.Contains(tp.Command, "--dangerously-bypass-approvals-and-sandbox") {
		t.Fatalf("command %q lost the permission-mode default", tp.Command)
	}
	if rp := tp.ResolvedProvider; rp != nil {
		if v, ok := rp.EffectiveDefaults["model"]; ok && v != "" {
			t.Fatalf("EffectiveDefaults[model] = %q, want unset", v)
		}
		if v, ok := rp.EffectiveDefaults["effort"]; ok && v != "" {
			t.Fatalf("EffectiveDefaults[effort] = %q, want unset", v)
		}
	}
}

func TestResolveTemplate_CodexBuiltinExplicitModelAndEffortStillEmitFlags(t *testing.T) {
	tp := resolveCodexBuiltinTemplate(t, map[string]string{
		"model":  "gpt-6.1-sol",
		"effort": "high",
	})

	if !strings.Contains(tp.Command, "--model gpt-6.1-sol") {
		t.Fatalf("command %q should carry the explicit --model gpt-6.1-sol", tp.Command)
	}
	if !strings.Contains(tp.Command, "-c model_reasoning_effort=high") {
		t.Fatalf("command %q should carry the explicit effort", tp.Command)
	}
	if n := strings.Count(tp.Command, "--model"); n != 1 {
		t.Fatalf("command %q has %d --model flags, want 1", tp.Command, n)
	}
}

// A builtin:codex session must run its own app server. Without --no-daemon a
// codex TUI attaches to any shared background server already running and runs
// the session's commands in that server's environment, without the session's
// GC_* identity. Codex refuses the flag twice, so it must appear exactly once,
// also next to the -c override an effort choice adds.
func TestResolveTemplate_CodexBuiltinRunsWithoutSharedDaemon(t *testing.T) {
	for _, tc := range []struct {
		name           string
		optionDefaults map[string]string
		want           string
	}{
		{
			name: "no choice",
			want: "codex --no-daemon --dangerously-bypass-approvals-and-sandbox",
		},
		{
			name:           "explicit model and effort",
			optionDefaults: map[string]string{"model": "gpt-6.1-sol", "effort": "xhigh"},
			want:           "codex --no-daemon --dangerously-bypass-approvals-and-sandbox --model gpt-6.1-sol -c model_reasoning_effort=xhigh",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tp := resolveCodexBuiltinTemplate(t, tc.optionDefaults)
			if tp.Command != tc.want {
				t.Fatalf("command = %q, want %q", tp.Command, tc.want)
			}
		})
	}
}

func TestResolveResumeCommand_CodexBuiltinKeepsNoDaemon(t *testing.T) {
	tp := resolveCodexBuiltinTemplate(t, nil)
	if tp.ResolvedProvider == nil {
		t.Fatal("resolveTemplate(codex) returned no resolved provider")
	}

	got := resolveResumeCommand(tp.Command, "def-456", tp.ResolvedProvider)
	want := "codex resume def-456 --no-daemon --dangerously-bypass-approvals-and-sandbox"
	if got != want {
		t.Fatalf("resume command = %q, want %q", got, want)
	}
}

func TestBuildPreparedStart_CodexWithoutChoiceOmitsModelAndEffort(t *testing.T) {
	candidate, cfg, store := newOptionSessionWithWork(t, codexEffortResolvedProvider(), "codex", nil)

	prepared, _, err := buildPreparedStart(candidate, cfg, store)
	if err != nil {
		t.Fatalf("buildPreparedStart: %v", err)
	}
	for _, tok := range strings.Fields(prepared.cfg.Command) {
		if tok == "--model" || tok == "-m" {
			t.Fatalf("command %q passes a model flag without an explicit model choice", prepared.cfg.Command)
		}
	}
	if strings.Contains(prepared.cfg.Command, "model_reasoning_effort") {
		t.Fatalf("command %q passes a reasoning effort without an explicit effort choice", prepared.cfg.Command)
	}
}

func TestBuildPreparedStart_CodexDispatchModelOptionPresent(t *testing.T) {
	candidate, cfg, store := newOptionSessionWithWork(t, codexEffortResolvedProvider(), "codex", map[string]string{"model": "gpt-6.1-sol"})

	prepared, _, err := buildPreparedStart(candidate, cfg, store)
	if err != nil {
		t.Fatalf("buildPreparedStart: %v", err)
	}
	if !strings.Contains(prepared.cfg.Command, "--model gpt-6.1-sol") {
		t.Fatalf("command %q should carry the dispatched --model gpt-6.1-sol", prepared.cfg.Command)
	}
}
