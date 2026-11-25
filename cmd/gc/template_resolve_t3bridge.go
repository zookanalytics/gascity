package main

import (
	"encoding/json"
	"path/filepath"
	"strings"

	"github.com/gastownhall/gascity/internal/config"
)

func applyT3BridgeRuntimeConfig(tp TemplateParams, env map[string]string) {
	if !templateParamsUseT3Bridge(tp) {
		return
	}
	alias := strings.TrimSpace(tp.Alias)
	if alias == "" {
		alias = strings.TrimSpace(env["GC_AGENT"])
	}
	if alias == "" {
		alias = strings.TrimSpace(tp.InstanceName)
	}
	if alias == "" {
		alias = strings.TrimSpace(tp.TemplateName)
	}
	if alias != "" && strings.TrimSpace(env["GC_ALIAS"]) == "" {
		env["GC_ALIAS"] = alias
	}
	if strings.TrimSpace(env["GC_PROVIDER"]) == "" && tp.ResolvedProvider != nil {
		env["GC_PROVIDER"] = tp.ResolvedProvider.Name
	}
}

func buildT3BridgeStartupEnvelope(tp TemplateParams, startupPrompt string) json.RawMessage {
	if !templateParamsUseT3Bridge(tp) {
		return nil
	}
	provider := ""
	if tp.ResolvedProvider != nil {
		provider = tp.ResolvedProvider.Name
	}
	if provider == "" {
		provider = tp.Env["GC_PROVIDER"]
	}
	envelope := map[string]any{
		"version": 1,
		"gc": map[string]any{
			"cityPath":    tp.Env["GC_CITY_PATH"],
			"cityName":    filepath.Base(tp.Env["GC_CITY_PATH"]),
			"rigName":     tp.RigName,
			"rigPath":     tp.RigRoot,
			"agent":       tp.TemplateName,
			"template":    tp.TemplateName,
			"sessionName": tp.SessionName,
		},
		"runtime": map[string]any{
			"provider":         provider,
			"model":            t3BridgeStartupEnvelopeModel(provider, tp),
			"sessionTransport": tp.EffectiveSessionProvider,
			"runtimeMode":      "full-access",
			"interactionMode":  "default",
			"workDir":          tp.WorkDir,
			"command":          tp.Command,
		},
		"startup": map[string]any{
			"promptTemplate": tp.TemplateName,
			"startupPrompt":  startupPrompt,
			"initialNudge":   tp.Hints.Nudge,
		},
		"context": map[string]any{
			"gcEnv": map[string]any{
				"GC_AGENT":        tp.Env["GC_AGENT"],
				"GC_PROVIDER":     provider,
				"GC_TEMPLATE":     tp.Env["GC_TEMPLATE"],
				"GC_CITY_PATH":    tp.Env["GC_CITY_PATH"],
				"GC_RIG":          tp.Env["GC_RIG"],
				"GC_SESSION_NAME": tp.Env["GC_SESSION_NAME"],
			},
		},
		"resume": map[string]any{
			"policy":                 "match-or-recreate",
			"allowThreadReuse":       true,
			"requiredThreadProvider": provider,
			"requiredThreadModel":    t3BridgeStartupEnvelopeModel(provider, tp),
		},
	}
	data, err := json.Marshal(envelope)
	if err != nil {
		return nil
	}
	return json.RawMessage(data)
}

func templateParamsUseT3Bridge(tp TemplateParams) bool {
	sessionProvider := strings.TrimSpace(tp.EffectiveSessionProvider)
	if sessionProvider == "" {
		sessionProvider = strings.TrimSpace(tp.SessionOverride)
	}
	if sessionProvider == "t3bridge" {
		return true
	}
	if strings.HasPrefix(sessionProvider, "exec:") {
		return isLegacyT3BridgeExecScript(strings.TrimPrefix(sessionProvider, "exec:"))
	}
	return false
}

// effectiveSessionProvider is the runtime a session of an agent runs on, as
// the startup prompt plan and the prompt-budget diagnostics read it. The
// session provider runs every session it does not route to acp on the city's
// selection, which effectiveProviderName reads as the provider is built from
// it: GC_SESSION, then [session] provider, then the platform default. An
// agent's tmux override names the transport that carries every runtime but
// acp (transportForRuntimeName), so it reads as that selection, and as tmux
// where the selection is empty. Any other override reads as given.
func effectiveSessionProvider(sessionOverride, citySessionProvider string) string {
	override := strings.TrimSpace(sessionOverride)
	if override != "" && override != config.SessionTransportTmux {
		return override
	}
	if selection := strings.TrimSpace(effectiveProviderName(strings.TrimSpace(citySessionProvider))); selection != "" {
		return selection
	}
	return override
}

func t3BridgeStartupEnvelopeModel(provider string, tp TemplateParams) string {
	if strings.Contains(tp.Command, "--model ") {
		parts := strings.Fields(tp.Command)
		for i := 0; i < len(parts)-1; i++ {
			if parts[i] == "--model" {
				return parts[i+1]
			}
		}
	}
	if v := tp.Env["GC_MODEL"]; v != "" {
		return v
	}
	if provider == "codex" {
		return "gpt-5-codex"
	}
	return "claude-opus-4-6"
}
