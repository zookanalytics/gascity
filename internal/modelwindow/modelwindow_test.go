package modelwindow

import "testing"

func TestWindow(t *testing.T) {
	tests := []struct {
		model string
		want  int
	}{
		// Modern Claude families resolve to 1M WITHOUT the "[1m]" suffix — the
		// provider echoes the model ID back without the launch flag (gc-os8fn).
		{"claude-opus-4-8", Million},
		{"claude-opus-4-7", Million},
		{"claude-opus-4-6", Million},
		{"claude-sonnet-4-6", Million},
		{"claude-opus-4-8-20260101", Million}, // dated variant still matches
		{"CLAUDE-OPUS-4-8", Million},          // case-insensitive
		{"claude-fable-5", Million},
		{"claude-mythos-1", Million},
		// The explicit "[1m]" suffix forces 1M for any Claude family, including
		// ones whose bare form is 200K.
		{"claude-opus-4-8[1m]", Million},
		{"sonnet[1m]", Million},
		{"claude-haiku-4-5-20251001[1m]", Million},
		// Older Claude families stay at the conservative default.
		{"claude-opus-4-5-20251101", Default},
		{"claude-sonnet-4-5-20251101", Default},
		{"claude-haiku-4-5-20251001", Default},
		// Non-Claude families.
		{"gemini-2.5-pro", Million},
		{"gpt-5-20260101", 258_000},
		{"codex-mini-latest", 258_000},
		{"gpt-4o-2024-08-06", 128_000},
		{"gpt-4-turbo", 128_000},
		// Unrecognized families return 0 so callers apply their own policy.
		{"unknown-model-xyz", 0},
		{"", 0},
	}
	for _, tt := range tests {
		t.Run(tt.model, func(t *testing.T) {
			if got := Window(tt.model); got != tt.want {
				t.Errorf("Window(%q) = %d, want %d", tt.model, got, tt.want)
			}
		})
	}
}
