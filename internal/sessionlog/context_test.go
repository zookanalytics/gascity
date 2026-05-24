package sessionlog

import "testing"

func TestModelContextWindow(t *testing.T) {
	tests := []struct {
		model string
		want  int
	}{
		// Family-table defaults (no suffix).
		{"claude-opus-4-5-20251101", 200_000},
		{"claude-sonnet-4-5-20251101", 200_000},
		{"claude-haiku-4-5-20251001", 200_000},
		{"gemini-2.5-pro", 1_000_000},
		{"gpt-5-20260101", 258_000},
		{"codex-mini-latest", 258_000},
		{"gpt-4o-2024-08-06", 128_000},
		{"gpt-4-turbo", 128_000},
		{"unknown-model-xyz", 0},
		{"", 0},

		// Explicit window suffix overrides family default.
		{"claude-opus-4-7[1m]", 1_000_000},
		{"claude-opus-4-7[200k]", 200_000},
		{"claude-opus-4-7[128k]", 128_000},
		{"claude-sonnet-4-6[1m]", 1_000_000},
		{"claude-haiku-4-5-20251001[500k]", 500_000},

		// Suffix grammar is case-insensitive.
		{"claude-opus-4-7[1M]", 1_000_000},
		{"claude-opus-4-7[200K]", 200_000},

		// Suffix applies even when family is unknown.
		{"unknown-model[1m]", 1_000_000},
		{"unknown-model[200k]", 200_000},

		// Malformed/unknown suffix falls back to family default — does not error.
		{"claude-opus-4-7[abc]", 200_000},
		{"claude-opus-4-7[]", 200_000},
		{"claude-opus-4-7[1g]", 200_000},
		{"claude-opus-4-7[1]", 200_000},
	}
	for _, tt := range tests {
		t.Run(tt.model, func(t *testing.T) {
			got := ModelContextWindow(tt.model)
			if got != tt.want {
				t.Errorf("ModelContextWindow(%q) = %d, want %d", tt.model, got, tt.want)
			}
		})
	}
}
