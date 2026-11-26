package main

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/runtime"
)

// startupPromptLaunch is the kind of launch a startup prompt is delivered to.
type startupPromptLaunch int

const (
	// startupPromptFreshLaunch begins a conversation: promptDelivery routes the
	// prompt through the argv suffix, the argv flag or the nudge, by provider
	// prompt mode and prompt size.
	startupPromptFreshLaunch startupPromptLaunch = iota
	// startupPromptResumeLaunch continues a conversation that already has
	// history. The prompt is re-delivered as the post-start nudge and never
	// through argv: an argv prompt behind a resume command would be submitted to
	// the continued conversation as a new turn.
	startupPromptResumeLaunch
)

// startupPromptSource is what a launch builder knows about the prompt it must
// deliver. TemplateParams projects onto it for the reconciler; the worker
// resolvers build it from the session and its agent.
type startupPromptSource struct {
	TemplateName string
	SessionName  string
	InstanceName string
	Prompt       string
	// Beacon is the session's bare identity line (`[city] agent • ts`), which
	// opens the first message of a conversation whose prompt rides the system
	// prompt (startupOpener). A Prompt that is the Beacon alone has no
	// instructions to carry there.
	Beacon           string
	IsACP            bool
	ResolvedProvider *config.ResolvedProvider
	HintNudge        string
	RuntimeName      string
	CityRuntimes     map[string]config.DiscoveredRuntime
	// Fork marks a session forked off a parent conversation
	// (gc.brain_parent_sid). The fork resumes the parent and so keeps the
	// system prompt the parent recorded; its own prompt keeps the first-turn
	// delivery.
	Fork bool
	// ConversationHasSystemPrompt reports that the conversation a resume
	// continues began with the rendered prompt in its system prompt
	// (session.Info.SystemPromptCarriesPrompt).
	ConversationHasSystemPrompt bool
}

// startupOpenerInstruction starts a conversation whose prompt rides the
// system prompt when the agent has no nudge of its own.
const startupOpenerInstruction = "Follow your Gas City instructions now, starting with the first action they name."

// startupOpener is the first message of a conversation whose rendered prompt
// rides the system prompt, and the whole turn a wake of it types: the
// session's beacon line, a blank line, then the agent's nudge, or
// startupOpenerInstruction when it has none. The beacon keeps the
// conversation findable in Claude Code's /resume picker; the text after it
// starts the turn, since a beacon alone gets a reply that acknowledges and
// waits.
func startupOpener(beacon, nudge string) string {
	if strings.TrimSpace(nudge) == "" {
		nudge = startupOpenerInstruction
	}
	if beacon == "" {
		return nudge
	}
	return beacon + "\n\n" + nudge
}

// runtimePassesSystemPromptFile reports whether a runtime writes
// runtime.Config.SystemPrompt to a file and passes it to the agent's CLI: tmux
// ("" is the default session provider, which constructs tmux) and herdr. Every
// other runtime ignores the field, so its launches keep the prompt in their
// first turn.
func runtimePassesSystemPromptFile(runtimeName string) bool {
	switch strings.TrimSpace(runtimeName) {
	case "", "tmux", "herdr":
		return true
	}
	return false
}

// systemPromptFileFlagFor is the flag that carries a launch's rendered prompt
// into the agent's system prompt, or "" when the launch keeps the prompt in
// its first turn: the provider declares no flag or a city switched it off,
// the transport is ACP, or the runtime cannot pass a file to the agent's CLI.
func systemPromptFileFlagFor(rp *config.ResolvedProvider, isACP bool, runtimeName string) string {
	if rp == nil || isACP || !runtimePassesSystemPromptFile(runtimeName) {
		return ""
	}
	return rp.SystemPromptFileFlag
}

// systemPromptFileFlag is systemPromptFileFlagFor for this launch, and also ""
// for a forked session and for a prompt with nothing past the beacon.
func (s startupPromptSource) systemPromptFileFlag() string {
	if s.Fork || strings.TrimSpace(s.Prompt) == "" || s.Prompt == s.Beacon {
		return ""
	}
	return systemPromptFileFlagFor(s.ResolvedProvider, s.IsACP, s.RuntimeName)
}

// configuredMode names the prompt-delivery mode the source's provider is
// configured for, for the oversized-prompt log record.
func (s startupPromptSource) configuredMode() string {
	switch {
	case s.IsACP:
		return "acp"
	case s.ResolvedProvider != nil && s.ResolvedProvider.PromptMode != "":
		return s.ResolvedProvider.PromptMode
	}
	return "arg"
}

// startupPromptPlan is how one launch carries its startup prompt. applyTo is the
// only writer of the launch payload and the delivered marker, so a launch cannot
// carry one without the other: a non-empty prompt with no payload and no marker
// would leave the SessionStart hook as the sole, size-limited carrier
// (ga-4k4zfk).
type startupPromptPlan struct {
	PromptSuffix string
	PromptFlag   string
	Nudge        string
	// SystemPrompt and SystemPromptFileFlag pass the rendered prompt into the
	// agent's system prompt (runtime.Config.SystemPrompt); both are empty
	// for a launch that keeps the prompt in its first turn.
	SystemPrompt         string
	SystemPromptFileFlag string
	// MarkDelivered reports whether the launch sets GC_STARTUP_PROMPT_DELIVERED,
	// which tells the SessionStart hook to add context without repeating the
	// prompt. Like promptDeliveryResult.Delivered it means delivery was selected
	// for this launch, not that the agent consumed the prompt.
	MarkDelivered bool
	// Delivery is promptDelivery's pure routing decision. The reconciler stamps
	// its priming markers from it, never from the env marker, which a resume
	// launch sets for hook consumption even though it delivers nothing new.
	Delivery promptDeliveryResult
	launch   startupPromptLaunch
}

// planStartupPrompt decides how a launch of the given kind carries src's prompt.
// A launch whose prompt rides the system prompt (systemPromptFreshPlan,
// systemPromptResumePlan) puts no prompt in argv. Every other launch runs promptDelivery first, so the
// oversized-prompt policy (fallback to the nudge, or a hard failure on a
// runtime with no fallback, ga-q8wgom.1.1) is identical for every launch
// builder.
func planStartupPrompt(src startupPromptSource, launch startupPromptLaunch) (startupPromptPlan, error) {
	if flag := src.systemPromptFileFlag(); flag != "" {
		if launch == startupPromptResumeLaunch {
			return src.systemPromptResumePlan(flag), nil
		}
		return src.systemPromptFreshPlan(flag)
	}
	delivery, err := promptDelivery(src.Prompt, src.IsACP, src.ResolvedProvider, src.HintNudge, src.RuntimeName, src.CityRuntimes)
	if err != nil {
		logOversizedPromptDelivery(slog.Default().Error,
			"startup prompt exceeds argv-safety threshold; no fallback delivery available",
			src, "hard-fail")
		return startupPromptPlan{}, fmt.Errorf("template %q (session %q): %w", src.TemplateName, src.SessionName, err)
	}
	if delivery.OversizedFallback {
		logOversizedPromptDelivery(slog.Default().Warn,
			"startup prompt exceeds argv-safety threshold; falling back to nudge delivery",
			src, "nudge-fallback")
	}
	if launch == startupPromptResumeLaunch {
		plan := resumeStartupPromptPlan(src.Prompt, src.HintNudge)
		plan.Delivery = delivery
		return plan, nil
	}
	return startupPromptPlan{
		PromptSuffix:  delivery.PromptSuffix,
		PromptFlag:    delivery.PromptFlag,
		Nudge:         delivery.Nudge,
		MarkDelivered: delivery.Delivered,
		Delivery:      delivery,
		launch:        launch,
	}, nil
}

// systemPromptFreshPlan is the plan of a fresh launch whose rendered prompt
// rides the agent's system prompt through flag. The opener goes where the
// prompt went before, by the provider's prompt mode, and nothing is typed
// after it. The launch is marked delivered, so the SessionStart hook never
// carries the prompt.
func (s startupPromptSource) systemPromptFreshPlan(flag string) (startupPromptPlan, error) {
	first, err := promptDelivery(startupOpener(s.Beacon, s.HintNudge), false, s.ResolvedProvider, "", s.RuntimeName, s.CityRuntimes)
	if err != nil {
		return startupPromptPlan{}, fmt.Errorf("template %q (session %q): opener: %w", s.TemplateName, s.SessionName, err)
	}
	return startupPromptPlan{
		PromptSuffix:         first.PromptSuffix,
		PromptFlag:           first.PromptFlag,
		Nudge:                first.Nudge,
		SystemPrompt:         s.Prompt,
		SystemPromptFileFlag: flag,
		MarkDelivered:        true,
		Delivery: promptDeliveryResult{
			PromptSuffix:   first.PromptSuffix,
			PromptFlag:     first.PromptFlag,
			Nudge:          first.Nudge,
			Delivered:      true,
			ConfiguredMode: first.ConfiguredMode,
			EffectiveMode:  "system-prompt",
		},
		launch: startupPromptFreshLaunch,
	}, nil
}

// systemPromptResumePlan is the plan of a resume whose rendered prompt rides
// the agent's system prompt through flag. The prompt is passed on every
// launch, resumes included: the agent records a conversation's system prompt
// at its first request and, once the conversation compacts, renders it again
// from the flags of the launch that makes the next request. The resume types
// the opener alone when the conversation began with the prompt in its system
// prompt; any other conversation's recorded system prompt lacks the prompt, so
// its resume replays it as resumeStartupPromptPlan does.
func (s startupPromptSource) systemPromptResumePlan(flag string) startupPromptPlan {
	nudge := restartPromptNudge(s.Prompt, s.HintNudge)
	if s.ConversationHasSystemPrompt {
		nudge = startupOpener(s.Beacon, s.HintNudge)
	}
	return startupPromptPlan{
		Nudge:                nudge,
		SystemPrompt:         s.Prompt,
		SystemPromptFileFlag: flag,
		MarkDelivered:        true,
		Delivery:             promptDeliveryResult{Nudge: nudge, Delivered: true, ConfiguredMode: s.configuredMode(), EffectiveMode: "system-prompt"},
		launch:               startupPromptResumeLaunch,
	}
}

// resumePlan is the plan for a launch that continues s's conversation,
// without planStartupPrompt's oversized-prompt check, which a launch builder
// has already run on its fresh plan.
func (s startupPromptSource) resumePlan() startupPromptPlan {
	if flag := s.systemPromptFileFlag(); flag != "" {
		return s.systemPromptResumePlan(flag)
	}
	return resumeStartupPromptPlan(s.Prompt, s.HintNudge)
}

// resumeStartupPromptPlan is the plan for a launch that continues a conversation:
// the prompt rides the nudge and the marker is set whenever there is a prompt.
func resumeStartupPromptPlan(prompt, hintNudge string) startupPromptPlan {
	return startupPromptPlan{
		Nudge:         restartPromptNudge(prompt, hintNudge),
		MarkDelivered: strings.TrimSpace(prompt) != "",
		launch:        startupPromptResumeLaunch,
	}
}

// applyTo writes the plan's payload and marker into cfg. cfg.Env must be owned
// by the caller: it is written in place, and allocated when the plan marks a
// launch that has no env. A resume launch also strips a stale marker, so a
// resume that delivers nothing never inherits one.
func (p startupPromptPlan) applyTo(cfg *runtime.Config) {
	cfg.PromptSuffix = p.PromptSuffix
	cfg.PromptFlag = p.PromptFlag
	cfg.Nudge = p.Nudge
	cfg.SystemPrompt = p.SystemPrompt
	cfg.SystemPromptFileFlag = p.SystemPromptFileFlag
	if p.launch == startupPromptResumeLaunch {
		delete(cfg.Env, startupPromptDeliveredEnv)
	}
	if p.MarkDelivered {
		if cfg.Env == nil {
			cfg.Env = map[string]string{}
		}
		cfg.Env[startupPromptDeliveredEnv] = "1"
	}
}

// startupPromptSource projects the reconciler's resolved template onto the
// inputs of the shared delivery decision.
func (tp TemplateParams) startupPromptSource() startupPromptSource {
	return startupPromptSource{
		TemplateName:     tp.TemplateName,
		SessionName:      tp.SessionName,
		InstanceName:     tp.InstanceName,
		Prompt:           tp.Prompt,
		Beacon:           tp.Beacon,
		IsACP:            tp.IsACP,
		ResolvedProvider: tp.ResolvedProvider,
		HintNudge:        tp.Hints.Nudge,
		RuntimeName:      tp.EffectiveSessionProvider,
		CityRuntimes:     tp.CityRuntimes,
	}
}
