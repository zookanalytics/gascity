// Package sessionlog reads Claude Code JSONL session files for
// lightweight metadata extraction (model, context usage).
package sessionlog

import "github.com/gastownhall/gascity/internal/modelwindow"

// ModelContextWindow returns the context window size for a model ID. It
// delegates to the shared modelwindow resolver so the API/session-log path and
// the CLI context-pressure injector agree on every model's window. Modern
// Claude families (Opus 4.6/4.7/4.8, Sonnet 4.6, Fable, Mythos) and any model
// carrying the "[1m]" suffix resolve to the 1M window; older/unknown Claude
// variants use the 200K default. Returns 0 if the model family is unknown, so
// callers can treat the window as undetermined.
func ModelContextWindow(model string) int {
	return modelwindow.Window(model)
}
