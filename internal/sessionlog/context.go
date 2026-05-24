// Package sessionlog reads Claude Code JSONL session files for
// lightweight metadata extraction (model, context usage).
package sessionlog

import (
	"strconv"
	"strings"
)

// modelFamilyWindows maps model family keywords to their context window sizes.
var modelFamilyWindows = map[string]int{
	"opus":   200_000,
	"sonnet": 200_000,
	"haiku":  200_000,
	"gemini": 1_000_000,
	"gpt-5":  258_000,
	"codex":  258_000,
	"gpt-4":  128_000,
	"gpt-4o": 128_000,
}

// ModelContextWindow returns the context window size for a model ID.
//
// An explicit window suffix in the form `[<n>(k|m)]` (case-insensitive)
// overrides the family-table default. Examples:
//
//	claude-opus-4-7[1m]   → 1_000_000
//	claude-opus-4-7[200k] →   200_000
//	claude-opus-4-7       → family-table default (200_000)
//
// A malformed or unrecognized suffix falls back to the family default;
// an unknown family with no parseable suffix returns 0.
func ModelContextWindow(model string) int {
	lower := strings.ToLower(model)
	if window, ok := parseWindowSuffix(lower); ok {
		return window
	}
	// Try longer matches first to avoid "gpt-4" matching before "gpt-4o".
	for _, family := range []string{"gpt-4o", "gpt-5", "gpt-4", "opus", "sonnet", "haiku", "gemini", "codex"} {
		if strings.Contains(lower, family) {
			return modelFamilyWindows[family]
		}
	}
	return 0
}

// parseWindowSuffix extracts an explicit `[<n>(k|m)]` window suffix from a
// lowercased model ID. Returns (window, true) on a valid suffix, (0, false)
// when no suffix is present or the suffix is malformed.
func parseWindowSuffix(lower string) (int, bool) {
	open := strings.LastIndexByte(lower, '[')
	if open < 0 || !strings.HasSuffix(lower, "]") {
		return 0, false
	}
	inner := lower[open+1 : len(lower)-1]
	if len(inner) < 2 {
		return 0, false
	}
	var mult int
	switch inner[len(inner)-1] {
	case 'k':
		mult = 1_000
	case 'm':
		mult = 1_000_000
	default:
		return 0, false
	}
	n, err := strconv.Atoi(inner[:len(inner)-1])
	if err != nil || n <= 0 {
		return 0, false
	}
	return n * mult, true
}
