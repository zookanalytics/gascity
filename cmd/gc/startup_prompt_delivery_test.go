package main

import (
	"errors"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/runtime"
	"github.com/gastownhall/gascity/internal/shellquote"
)

// TestPlanStartupPromptTruthTable pins the one decision every gc-managed launch
// builder shares: which of argv suffix, argv flag and nudge carries a rendered
// startup prompt, and whether the GC_STARTUP_PROMPT_DELIVERED marker is set.
// A fresh launch follows promptDelivery's routing; a resume launch always
// carries the prompt in the post-start nudge, because an argv prompt after a
// resume command would be submitted to the continued conversation as a new turn.
func TestPlanStartupPromptTruthTable(t *testing.T) {
	const (
		prompt = "do the work"
		hint   = "hint nudge"
	)
	quoted := shellquote.Quote(prompt)
	promptThenHint := prompt + startupPromptNudgeSeparator + hint
	flagMode := &config.ResolvedProvider{PromptMode: "flag", PromptFlag: "--prompt"}
	flagModeNoFlag := &config.ResolvedProvider{PromptMode: "flag"}
	noneMode := &config.ResolvedProvider{PromptMode: "none"}
	oversized := repeatToBytes("a", maxPromptSuffixRawBytes)
	nudgeFallback := map[string]config.DiscoveredRuntime{
		"custom": {PromptDelivery: config.PromptDeliveryNudgeFallback},
	}

	cases := []struct {
		name         string
		src          startupPromptSource
		launch       startupPromptLaunch
		wantSuffix   string
		wantFlag     string
		wantNudge    string
		wantMarked   bool
		wantFallback bool
		wantErr      error
	}{
		{
			name:       "fresh arg mode carries the prompt in argv",
			src:        startupPromptSource{Prompt: prompt, HintNudge: hint},
			launch:     startupPromptFreshLaunch,
			wantSuffix: quoted,
			wantNudge:  hint,
			wantMarked: true,
		},
		{
			name:       "fresh flag mode carries the prompt in argv with its flag",
			src:        startupPromptSource{Prompt: prompt, ResolvedProvider: flagMode},
			launch:     startupPromptFreshLaunch,
			wantSuffix: quoted,
			wantFlag:   "--prompt",
			wantMarked: true,
		},
		{
			name:       "fresh flag mode without a flag name is not marked delivered",
			src:        startupPromptSource{Prompt: prompt, ResolvedProvider: flagModeNoFlag},
			launch:     startupPromptFreshLaunch,
			wantSuffix: quoted,
		},
		{
			name:       "fresh none mode carries the prompt in the nudge",
			src:        startupPromptSource{Prompt: prompt, ResolvedProvider: noneMode, HintNudge: hint},
			launch:     startupPromptFreshLaunch,
			wantNudge:  promptThenHint,
			wantMarked: true,
		},
		{
			name:       "fresh ACP carries the prompt in the nudge",
			src:        startupPromptSource{Prompt: prompt, IsACP: true},
			launch:     startupPromptFreshLaunch,
			wantNudge:  prompt,
			wantMarked: true,
		},
		{
			name:      "fresh empty prompt delivers nothing",
			src:       startupPromptSource{HintNudge: hint},
			launch:    startupPromptFreshLaunch,
			wantNudge: hint,
		},
		{
			name:         "fresh oversized prompt on a nudge-capable runtime falls back to the nudge",
			src:          startupPromptSource{Prompt: oversized, RuntimeName: "tmux"},
			launch:       startupPromptFreshLaunch,
			wantNudge:    oversized,
			wantMarked:   true,
			wantFallback: true,
		},
		{
			name:         "fresh oversized prompt on a pack-declared nudge-fallback runtime falls back to the nudge",
			src:          startupPromptSource{Prompt: oversized, RuntimeName: "custom", CityRuntimes: nudgeFallback},
			launch:       startupPromptFreshLaunch,
			wantNudge:    oversized,
			wantMarked:   true,
			wantFallback: true,
		},
		{
			name:    "fresh oversized prompt on a runtime with no fallback hard-fails",
			src:     startupPromptSource{Prompt: oversized, RuntimeName: "subprocess", TemplateName: "worker", SessionName: "sess"},
			launch:  startupPromptFreshLaunch,
			wantErr: errOversizedPromptUnsupportedRuntime,
		},
		{
			name:       "resume arg mode moves the prompt from argv to the nudge",
			src:        startupPromptSource{Prompt: prompt, HintNudge: hint},
			launch:     startupPromptResumeLaunch,
			wantNudge:  promptThenHint,
			wantMarked: true,
		},
		{
			name:       "resume flag mode clears the flag and uses the nudge",
			src:        startupPromptSource{Prompt: prompt, ResolvedProvider: flagMode},
			launch:     startupPromptResumeLaunch,
			wantNudge:  prompt,
			wantMarked: true,
		},
		{
			name:       "resume ACP uses the nudge",
			src:        startupPromptSource{Prompt: prompt, IsACP: true},
			launch:     startupPromptResumeLaunch,
			wantNudge:  prompt,
			wantMarked: true,
		},
		{
			name:      "resume empty prompt keeps the hint nudge and does not mark",
			src:       startupPromptSource{HintNudge: hint},
			launch:    startupPromptResumeLaunch,
			wantNudge: hint,
		},
		{
			name:      "resume whitespace-only prompt is treated as empty",
			src:       startupPromptSource{Prompt: " \n\t ", HintNudge: hint},
			launch:    startupPromptResumeLaunch,
			wantNudge: hint,
		},
		{
			name:         "resume oversized prompt on a nudge-capable runtime uses the nudge",
			src:          startupPromptSource{Prompt: oversized, RuntimeName: "tmux"},
			launch:       startupPromptResumeLaunch,
			wantNudge:    oversized,
			wantMarked:   true,
			wantFallback: true,
		},
		{
			name:    "resume oversized prompt on a runtime with no fallback hard-fails like a fresh launch",
			src:     startupPromptSource{Prompt: oversized, RuntimeName: "subprocess", TemplateName: "worker", SessionName: "sess"},
			launch:  startupPromptResumeLaunch,
			wantErr: errOversizedPromptUnsupportedRuntime,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan, err := planStartupPrompt(tc.src, tc.launch)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("planStartupPrompt() error = %v, want errors.Is(%v)", err, tc.wantErr)
				}
				if strings.Contains(err.Error(), tc.src.Prompt) {
					t.Errorf("planStartupPrompt() error leaks prompt content")
				}
				return
			}
			if err != nil {
				t.Fatalf("planStartupPrompt() unexpected error: %v", err)
			}
			if plan.PromptSuffix != tc.wantSuffix {
				t.Errorf("PromptSuffix = %q, want %q", abbreviate(plan.PromptSuffix), abbreviate(tc.wantSuffix))
			}
			if plan.PromptFlag != tc.wantFlag {
				t.Errorf("PromptFlag = %q, want %q", plan.PromptFlag, tc.wantFlag)
			}
			if plan.Nudge != tc.wantNudge {
				t.Errorf("Nudge = %q (%d bytes), want %q (%d bytes)", abbreviate(plan.Nudge), len(plan.Nudge), abbreviate(tc.wantNudge), len(tc.wantNudge))
			}
			if plan.MarkDelivered != tc.wantMarked {
				t.Errorf("MarkDelivered = %v, want %v", plan.MarkDelivered, tc.wantMarked)
			}
			if plan.Delivery.OversizedFallback != tc.wantFallback {
				t.Errorf("Delivery.OversizedFallback = %v, want %v", plan.Delivery.OversizedFallback, tc.wantFallback)
			}
		})
	}
}

// TestPlanStartupPromptSystemPromptChannel pins when a launch carries the
// rendered prompt in the agent's system prompt, and what its first turn is
// then. Every launch on a provider that declares the flag, on a runtime that
// passes a file to the agent's CLI, passes the prompt that way, resumes
// included. A fresh launch routes the opener (beacon, blank line, nudge or the
// default instruction) where the prompt went before; a resume types the opener
// only for a conversation that began with the prompt in its system prompt, and
// replays the prompt for any other. Everything else keeps today's delivery.
func TestPlanStartupPromptSystemPromptChannel(t *testing.T) {
	const (
		beacon = "[city] gascity/worker-1 \u2022 2026-11-22T10:00:00"
		flag   = "--append-system-prompt-file"
		claim  = "Claim your work."
	)
	prompt := beacon + "\n\n# Role\n\nDo the work."
	oversized := beacon + "\n\n" + repeatToBytes("a", maxPromptSuffixRawBytes)
	claude := &config.ResolvedProvider{Name: "claude", PromptMode: "arg", SystemPromptFileFlag: flag}
	claudeNone := &config.ResolvedProvider{Name: "claude", PromptMode: "none", SystemPromptFileFlag: flag}
	claudeFlag := &config.ResolvedProvider{Name: "claude", PromptMode: "flag", PromptFlag: "--prompt", SystemPromptFileFlag: flag}
	switchedOff := &config.ResolvedProvider{Name: "claude", PromptMode: "arg"}
	opener := func(nudge string) string { return beacon + "\n\n" + nudge }
	defaultOpener := opener(startupOpenerInstruction)
	src := func(edit func(*startupPromptSource)) startupPromptSource {
		s := startupPromptSource{Prompt: prompt, Beacon: beacon, ResolvedProvider: claude, RuntimeName: "tmux", TemplateName: "worker", SessionName: "sess"}
		if edit != nil {
			edit(&s)
		}
		return s
	}

	cases := []struct {
		name       string
		src        startupPromptSource
		launch     startupPromptLaunch
		wantSuffix string
		wantFlag   string
		wantNudge  string
		wantSystem string
		wantMarked bool
	}{
		{
			name:       "fresh: the prompt rides the system prompt and the opener is the first message",
			src:        src(nil),
			wantSuffix: shellquote.Quote(defaultOpener),
			wantSystem: prompt,
			wantMarked: true,
		},
		{
			name:       "fresh with a nudge: the opener carries it and nothing is typed after",
			src:        src(func(s *startupPromptSource) { s.HintNudge = claim }),
			wantSuffix: shellquote.Quote(opener(claim)),
			wantSystem: prompt,
			wantMarked: true,
		},
		{
			name:       "fresh on the default session provider",
			src:        src(func(s *startupPromptSource) { s.RuntimeName = "" }),
			wantSuffix: shellquote.Quote(defaultOpener),
			wantSystem: prompt,
			wantMarked: true,
		},
		{
			name:       "fresh on herdr",
			src:        src(func(s *startupPromptSource) { s.RuntimeName = "herdr" }),
			wantSuffix: shellquote.Quote(defaultOpener),
			wantSystem: prompt,
			wantMarked: true,
		},
		{
			name:       "fresh none mode types the opener",
			src:        src(func(s *startupPromptSource) { s.ResolvedProvider = claudeNone; s.HintNudge = claim }),
			wantNudge:  opener(claim),
			wantSystem: prompt,
			wantMarked: true,
		},
		{
			name:       "fresh flag mode passes the opener with the prompt flag",
			src:        src(func(s *startupPromptSource) { s.ResolvedProvider = claudeFlag }),
			wantSuffix: shellquote.Quote(defaultOpener),
			wantFlag:   "--prompt",
			wantSystem: prompt,
			wantMarked: true,
		},
		{
			name:       "fresh oversized prompt rides the file with no argv fallback",
			src:        src(func(s *startupPromptSource) { s.Prompt = oversized }),
			wantSuffix: shellquote.Quote(defaultOpener),
			wantSystem: oversized,
			wantMarked: true,
		},
		{
			name:       "resume of a conversation that began with it types only the opener",
			src:        src(func(s *startupPromptSource) { s.ConversationHasSystemPrompt = true; s.HintNudge = claim }),
			launch:     startupPromptResumeLaunch,
			wantNudge:  opener(claim),
			wantSystem: prompt,
			wantMarked: true,
		},
		{
			name:       "resume of it with no nudge types the default instruction",
			src:        src(func(s *startupPromptSource) { s.ConversationHasSystemPrompt = true }),
			launch:     startupPromptResumeLaunch,
			wantNudge:  defaultOpener,
			wantSystem: prompt,
			wantMarked: true,
		},
		{
			name:       "resume of a conversation that began without it replays the prompt, and the file still rides",
			src:        src(func(s *startupPromptSource) { s.HintNudge = claim }),
			launch:     startupPromptResumeLaunch,
			wantNudge:  prompt + startupPromptNudgeSeparator + claim,
			wantSystem: prompt,
			wantMarked: true,
		},
		{
			name:       "a provider whose city switched the flag off keeps today's delivery",
			src:        src(func(s *startupPromptSource) { s.ResolvedProvider = switchedOff; s.HintNudge = claim }),
			wantSuffix: shellquote.Quote(prompt),
			wantNudge:  claim,
			wantMarked: true,
		},
		{
			name:       "a switched-off provider's resume keeps today's replay",
			src:        src(func(s *startupPromptSource) { s.ResolvedProvider = switchedOff; s.ConversationHasSystemPrompt = true }),
			launch:     startupPromptResumeLaunch,
			wantNudge:  prompt,
			wantMarked: true,
		},
		{
			name:       "ACP keeps today's delivery",
			src:        src(func(s *startupPromptSource) { s.IsACP = true }),
			wantNudge:  prompt,
			wantMarked: true,
		},
		{
			name:       "k8s keeps today's delivery",
			src:        src(func(s *startupPromptSource) { s.RuntimeName = "k8s" }),
			wantSuffix: shellquote.Quote(prompt),
			wantMarked: true,
		},
		{
			name:       "hybrid keeps today's delivery",
			src:        src(func(s *startupPromptSource) { s.RuntimeName = "hybrid" }),
			wantSuffix: shellquote.Quote(prompt),
			wantMarked: true,
		},
		{
			name:       "a forked conversation keeps today's delivery",
			src:        src(func(s *startupPromptSource) { s.Fork = true }),
			wantSuffix: shellquote.Quote(prompt),
			wantMarked: true,
		},
		{
			name:       "a beacon alone keeps today's delivery",
			src:        src(func(s *startupPromptSource) { s.Prompt = beacon }),
			wantSuffix: shellquote.Quote(beacon),
			wantMarked: true,
		},
		{
			name:      "an empty prompt delivers nothing",
			src:       src(func(s *startupPromptSource) { s.Prompt = ""; s.HintNudge = claim }),
			wantNudge: claim,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan, err := planStartupPrompt(tc.src, tc.launch)
			if err != nil {
				t.Fatalf("planStartupPrompt() unexpected error: %v", err)
			}
			if plan.PromptSuffix != tc.wantSuffix {
				t.Errorf("PromptSuffix = %q, want %q", abbreviate(plan.PromptSuffix), abbreviate(tc.wantSuffix))
			}
			if plan.PromptFlag != tc.wantFlag {
				t.Errorf("PromptFlag = %q, want %q", plan.PromptFlag, tc.wantFlag)
			}
			if plan.Nudge != tc.wantNudge {
				t.Errorf("Nudge = %q, want %q", abbreviate(plan.Nudge), abbreviate(tc.wantNudge))
			}
			if plan.SystemPrompt != tc.wantSystem {
				t.Errorf("SystemPrompt = %q, want %q", abbreviate(plan.SystemPrompt), abbreviate(tc.wantSystem))
			}
			wantFileFlag := ""
			if tc.wantSystem != "" {
				wantFileFlag = flag
			}
			if plan.SystemPromptFileFlag != wantFileFlag {
				t.Errorf("SystemPromptFileFlag = %q, want %q", plan.SystemPromptFileFlag, wantFileFlag)
			}
			if plan.MarkDelivered != tc.wantMarked {
				t.Errorf("MarkDelivered = %v, want %v", plan.MarkDelivered, tc.wantMarked)
			}
			if plan.Delivery.OversizedFallback {
				t.Errorf("Delivery.OversizedFallback = true, want false")
			}
			if tc.launch == startupPromptFreshLaunch && plan.Delivery.Delivered != tc.wantMarked {
				t.Errorf("Delivery.Delivered = %v, want %v", plan.Delivery.Delivered, tc.wantMarked)
			}
		})
	}
}

// TestStartupPromptPlanApplyOwnsPayloadAndMarker pins that applyTo is the single
// writer of the launch payload and the delivered marker, and that it preserves
// the two behaviors the reconciler relied on before the decision was shared: a
// resume launch strips a stale marker and argv payload, a fresh launch that
// delivers nothing leaves the environment untouched.
func TestStartupPromptPlanApplyOwnsPayloadAndMarker(t *testing.T) {
	t.Run("marked plan sets the payload and the marker", func(t *testing.T) {
		plan, err := planStartupPrompt(startupPromptSource{Prompt: "p"}, startupPromptFreshLaunch)
		if err != nil {
			t.Fatal(err)
		}
		cfg := runtime.Config{Env: map[string]string{"KEEP": "me"}}
		plan.applyTo(&cfg)
		if cfg.Env[startupPromptDeliveredEnv] != "1" {
			t.Errorf("marker = %q, want 1", cfg.Env[startupPromptDeliveredEnv])
		}
		if cfg.Env["KEEP"] != "me" {
			t.Errorf("applyTo dropped unrelated env, got %v", cfg.Env)
		}
		if cfg.PromptSuffix != shellquote.Quote("p") {
			t.Errorf("PromptSuffix = %q, want the quoted prompt", cfg.PromptSuffix)
		}
	})

	t.Run("marked plan allocates the env when there is none", func(t *testing.T) {
		plan, err := planStartupPrompt(startupPromptSource{Prompt: "p"}, startupPromptFreshLaunch)
		if err != nil {
			t.Fatal(err)
		}
		var cfg runtime.Config
		plan.applyTo(&cfg)
		if cfg.Env[startupPromptDeliveredEnv] != "1" {
			t.Errorf("marker = %q, want 1", cfg.Env[startupPromptDeliveredEnv])
		}
	})

	t.Run("fresh launch that delivers nothing leaves the env untouched", func(t *testing.T) {
		plan, err := planStartupPrompt(startupPromptSource{HintNudge: "hint"}, startupPromptFreshLaunch)
		if err != nil {
			t.Fatal(err)
		}
		var cfg runtime.Config
		plan.applyTo(&cfg)
		if cfg.Env != nil {
			t.Errorf("env = %v, want nil", cfg.Env)
		}
		if cfg.Nudge != "hint" {
			t.Errorf("Nudge = %q, want the hint nudge", cfg.Nudge)
		}
	})

	t.Run("a plan writes the system prompt and a plan without one clears it", func(t *testing.T) {
		src := startupPromptSource{
			Prompt:           "[c] a \u2022 t\n\n# Role",
			Beacon:           "[c] a \u2022 t",
			ResolvedProvider: &config.ResolvedProvider{PromptMode: "arg", SystemPromptFileFlag: "--append-system-prompt-file"},
			RuntimeName:      "tmux",
		}
		plan, err := planStartupPrompt(src, startupPromptFreshLaunch)
		if err != nil {
			t.Fatal(err)
		}
		var cfg runtime.Config
		plan.applyTo(&cfg)
		if cfg.SystemPrompt != src.Prompt || cfg.SystemPromptFileFlag != "--append-system-prompt-file" {
			t.Fatalf("system prompt = %q via %q, want the prompt via the provider flag", cfg.SystemPrompt, cfg.SystemPromptFileFlag)
		}
		src.ResolvedProvider = &config.ResolvedProvider{PromptMode: "arg"}
		plan, err = planStartupPrompt(src, startupPromptResumeLaunch)
		if err != nil {
			t.Fatal(err)
		}
		plan.applyTo(&cfg)
		if cfg.SystemPrompt != "" || cfg.SystemPromptFileFlag != "" {
			t.Fatalf("a plan without the channel left system prompt %q via %q behind", cfg.SystemPrompt, cfg.SystemPromptFileFlag)
		}
	})

	t.Run("resume launch strips argv payload", func(t *testing.T) {
		plan, err := planStartupPrompt(startupPromptSource{Prompt: "p"}, startupPromptResumeLaunch)
		if err != nil {
			t.Fatal(err)
		}
		cfg := runtime.Config{PromptSuffix: "'stale'", PromptFlag: "--stale"}
		plan.applyTo(&cfg)
		if cfg.PromptSuffix != "" || cfg.PromptFlag != "" {
			t.Errorf("resume left argv payload behind: suffix=%q flag=%q", cfg.PromptSuffix, cfg.PromptFlag)
		}
		if cfg.Nudge != "p" {
			t.Errorf("Nudge = %q, want the prompt", cfg.Nudge)
		}
	})

	t.Run("resume launch with an empty prompt strips a stale marker", func(t *testing.T) {
		plan, err := planStartupPrompt(startupPromptSource{}, startupPromptResumeLaunch)
		if err != nil {
			t.Fatal(err)
		}
		cfg := runtime.Config{Env: map[string]string{startupPromptDeliveredEnv: "1", "KEEP": "me"}}
		plan.applyTo(&cfg)
		if _, ok := cfg.Env[startupPromptDeliveredEnv]; ok {
			t.Errorf("stale marker survived a resume that delivers nothing: %v", cfg.Env)
		}
		if cfg.Env["KEEP"] != "me" {
			t.Errorf("applyTo dropped unrelated env, got %v", cfg.Env)
		}
	})
}

// abbreviate shortens a possibly huge string for a failure message.
func abbreviate(s string) string {
	const limit = 80
	if len(s) <= limit {
		return s
	}
	return s[:limit] + "..."
}
