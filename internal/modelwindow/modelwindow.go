// Package modelwindow resolves an LLM model ID to its context-window size in
// tokens. It is the single source of truth shared by the session-log context
// reader (internal/sessionlog) and the CLI context-pressure injector
// (cmd/gc/context_inject.go), so both report identical windows for the same
// model ID and cannot drift apart (the failure mode behind gc-os8fn, where the
// API path under-reported a 1M window as 200K and saturated context_pct).
package modelwindow

import "strings"

const (
	// Million is the context window, in tokens, for 1M-token model variants.
	Million = 1_000_000
	// Default is the conservative fallback window for a recognized Claude
	// family that is not a 1M variant (e.g. Haiku, Opus 4.5 and earlier).
	Default = 200_000
)

// familyWindows maps a model-family keyword to its context-window size in
// tokens. Claude families default to the conservative window here; the 1M
// Claude variants are matched ahead of this table by millionMarkers in Window.
var familyWindows = map[string]int{
	"opus":   Default,
	"sonnet": Default,
	"haiku":  Default,
	"gemini": Million,
	"gpt-5":  258_000,
	"codex":  258_000,
	"gpt-4":  128_000,
	"gpt-4o": 128_000,
}

// millionMarkers are substrings that force a 1M window regardless of the base
// family default. They cover the explicit "[1m]" launch suffix, the 1M-window
// Claude families (Fable, Mythos), and the version-specific Claude variants
// whose window is 1M even without the suffix (Opus 4.6/4.7/4.8, Sonnet 4.6).
// The provider echoes a model ID back without its launch flag, so a bare
// "claude-opus-4-8" must resolve to 1M (gc-os8fn). Substring matching keeps
// dated-suffix variants (e.g. "claude-opus-4-8-20260101") recognized.
var millionMarkers = []string{
	"[1m]", "fable", "mythos", "opus-4-6", "opus-4-7", "opus-4-8", "sonnet-4-6",
}

// familyOrder lists family keywords longest-match-first so a longer keyword is
// tried before a shorter one it contains (e.g. "gpt-4o" before "gpt-4").
var familyOrder = []string{"gpt-4o", "gpt-5", "gpt-4", "opus", "sonnet", "haiku", "gemini", "codex"}

// Window returns the context-window size, in tokens, for a model ID. Modern
// Claude families (Opus 4.6/4.7/4.8, Sonnet 4.6, Fable, Mythos) and any model
// carrying the explicit "[1m]" launch suffix resolve to the 1M window; older or
// unknown Claude variants use the 200K Default. Returns 0 when the model family
// is entirely unrecognized, so callers can apply their own unknown-model policy
// (the session-log/API path treats 0 as "window unknown"; the injector floors
// it to Default).
func Window(model string) int {
	lower := strings.ToLower(model)
	for _, marker := range millionMarkers {
		if strings.Contains(lower, marker) {
			return Million
		}
	}
	for _, family := range familyOrder {
		if strings.Contains(lower, family) {
			return familyWindows[family]
		}
	}
	return 0
}
