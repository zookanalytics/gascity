package tmux

import (
	"strings"
	"testing"
)

// Fixtures are kept inline as Go raw-string literals so a new regression
// is one variable + one table entry. The `dim` macros below mirror the
// SGR sequences tmux capture-pane -e emits; using them in the fixtures
// keeps the wire bytes visible to a reader scanning the file. Real
// captures from a live session match these patterns byte-for-byte; the
// Appendix C fixture in engdocs/design/input-area-state.md is the
// canonical reference.

const (
	dimOn    = "\x1b[2m"
	dimGray  = "\x1b[38;5;240m"
	brBlack  = "\x1b[90m"
	dimReset = "\x1b[0m"
	dimOff22 = "\x1b[22m"
)

// claudeIdleWithGhost reproduces the empirical 2026-05-09 false positive
// from engdocs/design/input-area-state.md Appendix C: the prompt char is
// outside any SGR wrapper and the ghost suggestion is wrapped in dim SGR.
const claudeIdleWithGhost = "" +
	"some scrollback line\n" +
	"❯ " + dimOn + "keep patrolling                                     " + dimReset + "\n" +
	"[status bar]\n"

// claudeIdleNBSP confirms the parser accepts the non-breaking-space
// variant Claude Code uses on some builds. matchesPromptPrefix has the
// same NBSP-tolerance for the idle path; we lock it in here.
const claudeIdleNBSP = "" +
	"❯ " + dimOn + "ghost text " + dimReset + "\n" +
	"status row\n"

const claudeTypedPlain = "" +
	"chrome row\n" +
	"❯ hello world\n" +
	"status row\n"

const claudeTypedWithSGR = "" +
	"❯ \x1b[39mhello\x1b[0m\n" +
	"status row\n"

// claudeTypedMultilineJoined reflects what -J gives us when an operator
// types past the terminal width: a single logical row with the full
// typed content. The parser does not need to re-stitch wrap rows.
const claudeTypedMultilineJoined = "" +
	"❯ this is a long typed message that wrapped to several visual rows in the terminal before -J joined them\n" +
	"status row\n"

const claudeBusyToolCall = "" +
	"⏺ Reading file …\n" +
	"  esc to interrupt\n"

// claudeBusyScrollbackPrompt covers the inter-tool-call gap: the prompt
// is still in scrollback but the busy indicator means the agent is
// processing. Busy beats prompt for state — the parser must report
// Busy=true and not surface the stale scrollback prompt.
const claudeBusyScrollbackPrompt = "" +
	"❯ earlier prompt that is now scrollback\n" +
	"⏺ Running tool …\n" +
	"  esc to interrupt\n"

const claudeApprovalPrompt = "" +
	"This command requires approval\n" +
	"  > Yes\n" +
	"    No\n"

// claudeDimAsBrightBlack covers the SGR 90 variant (bright-black
// foreground reads as grey on most terminals). Q1 in the spec calls out
// this as one of the variants the parser must classify as ghost.
const claudeDimAsBrightBlack = "" +
	"❯ " + brBlack + "alt-dim ghost" + dimReset + "\n"

// claudeDim38_5_240 covers the 256-color dim grey variant.
const claudeDim38_5_240 = "" +
	"❯ " + dimGray + "grey ghost" + dimReset + "\n"

const codexIdle = "" +
	"╭───────────────────────────────────────╮\n" +
	"│ >                                     │\n" +
	"╰───────────────────────────────────────╯\n" +
	"  Press Enter to send\n"

const codexTypedBoxed = "" +
	"╭───────────────────────────────────────╮\n" +
	"│ > fix the failing test                │\n" +
	"╰───────────────────────────────────────╯\n"

const codexBusy = "" +
	"Running shell command …\n" +
	"Press Esc or Ctrl+C to cancel\n"

// codexBoxPersistedInScrollback exercises the multi-line spec note: an
// older box in scrollback must not win over the most-recent prompt row.
const codexBoxPersistedInScrollback = "" +
	"╭───────────────────────────────────────╮\n" +
	"│ > older box content                   │\n" +
	"╰───────────────────────────────────────╯\n" +
	"[tool output]\n" +
	"╭───────────────────────────────────────╮\n" +
	"│ > most recent input                   │\n" +
	"╰───────────────────────────────────────╯\n"

const geminiIdle = "" +
	"Gemini interactive shell\n" +
	"\n" +
	"> \n"

const geminiTyped = "" +
	"\n" +
	"> draft the email\n"

const geminiRewindPicker = "" +
	"Rewind to previous turn?\n" +
	"  > Rewind\n" +
	"    Cancel rewind and stay here\n"

const unknownCustomPrompt = "" +
	"$ ls -la\n" +
	"$ \n"

func TestStripANSI(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"no escapes", "plain text", "plain text"},
		{"single SGR", "before\x1b[2mDIM\x1b[0mafter", "beforeDIMafter"},
		{"multiple SGR", "\x1b[1m\x1b[31mred bold\x1b[0m", "red bold"},
		{"with extended 256-color", "\x1b[38;5;240mgrey\x1b[0m", "grey"},
		{"with true color", "\x1b[38;2;100;100;100mtc\x1b[0m", "tc"},
		{"erase line", "\x1b[2Kgone", "gone"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := stripANSI(c.in); got != c.want {
				t.Fatalf("stripANSI(%q) = %q, want %q", c.in, got, c.want)
			}
		})
	}
}

func TestSplitDimSegments(t *testing.T) {
	cases := []struct {
		name      string
		in        string
		wantTyped string
		wantGhost string
	}{
		{"plain text only", "hello", "hello", ""},
		{"dim only via SGR 2", dimOn + "ghost" + dimReset, "", "ghost"},
		{"dim only via SGR 22 reset", dimOn + "ghost" + dimOff22 + "back", "back", "ghost"},
		{"bright-black is dim", brBlack + "grey" + dimReset, "", "grey"},
		{"256-color 240 is dim", dimGray + "x" + dimReset, "", "x"},
		{"256-color 244 is not dim", "\x1b[38;5;244mx\x1b[0m", "x", ""},
		{"true-color not classified", "\x1b[38;2;100;100;100mx\x1b[0m", "x", ""},
		{"mixed typed + ghost", "typed " + dimOn + "and ghost" + dimReset + " more", "typed  more", "and ghost"},
		{"empty params resets dim", dimOn + "g" + "\x1b[m" + "t", "t", "g"},
		{"unrelated SGR (color) does not toggle dim", "\x1b[31mred typed\x1b[0m", "red typed", ""},
		// SGR 1 (bold) co-set with 2 (dim) — both attributes apply, parser must classify as dim.
		{"bold+dim combined", "\x1b[1;2mboldim\x1b[0m", "", "boldim"},
		{"unterminated CSI is treated as text", "before\x1b[2mghost no reset", "before", "ghost no reset"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			typed, ghost := splitDimSegments(c.in)
			if typed != c.wantTyped || ghost != c.wantGhost {
				t.Fatalf("splitDimSegments(%q)\n  got  typed=%q ghost=%q\n  want typed=%q ghost=%q",
					c.in, typed, ghost, c.wantTyped, c.wantGhost)
			}
		})
	}
}

func TestParseClaudeInputArea(t *testing.T) {
	cases := []struct {
		name       string
		fixture    string
		wantBusy   bool
		wantPrompt string
		wantTyped  string
		wantGhost  string
	}{
		{
			name:       "idle with ghost (empirical 2026-05-09)",
			fixture:    claudeIdleWithGhost,
			wantPrompt: ClaudePromptChar,
			wantTyped:  "",
			wantGhost:  "keep patrolling",
		},
		{
			name:       "idle NBSP variant",
			fixture:    claudeIdleNBSP,
			wantPrompt: ClaudePromptChar,
			wantGhost:  "ghost text",
		},
		{
			name:       "typed plain",
			fixture:    claudeTypedPlain,
			wantPrompt: ClaudePromptChar,
			wantTyped:  "hello world",
		},
		{
			name:       "typed with default-fg SGR",
			fixture:    claudeTypedWithSGR,
			wantPrompt: ClaudePromptChar,
			wantTyped:  "hello",
		},
		{
			name:       "typed multiline joined by -J",
			fixture:    claudeTypedMultilineJoined,
			wantPrompt: ClaudePromptChar,
			wantTyped:  "this is a long typed message that wrapped to several visual rows in the terminal before -J joined them",
		},
		{
			name:     "busy tool call (no prompt visible)",
			fixture:  claudeBusyToolCall,
			wantBusy: true,
		},
		{
			name:     "busy beats scrollback prompt",
			fixture:  claudeBusyScrollbackPrompt,
			wantBusy: true,
		},
		{
			name:    "approval prompt is not an input area",
			fixture: claudeApprovalPrompt,
			// No prompt char, no typed/ghost — partial state is fine.
		},
		{
			name:       "dim via bright-black (SGR 90)",
			fixture:    claudeDimAsBrightBlack,
			wantPrompt: ClaudePromptChar,
			wantGhost:  "alt-dim ghost",
		},
		{
			name:       "dim via 256-color 240",
			fixture:    claudeDim38_5_240,
			wantPrompt: ClaudePromptChar,
			wantGhost:  "grey ghost",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := parseClaudeInputArea(c.fixture, nil)
			if got.Provider != InputAreaProviderClaude {
				t.Fatalf("Provider = %q, want %q", got.Provider, InputAreaProviderClaude)
			}
			if got.Busy != c.wantBusy {
				t.Errorf("Busy = %v, want %v", got.Busy, c.wantBusy)
			}
			if got.PromptChar != c.wantPrompt {
				t.Errorf("PromptChar = %q, want %q", got.PromptChar, c.wantPrompt)
			}
			if got.Typed != c.wantTyped {
				t.Errorf("Typed = %q, want %q", got.Typed, c.wantTyped)
			}
			if got.Ghost != c.wantGhost {
				t.Errorf("Ghost = %q, want %q", got.Ghost, c.wantGhost)
			}
			if got.Raw != c.fixture {
				t.Errorf("Raw not preserved")
			}
			if got.Detected.IsZero() {
				t.Error("Detected timestamp not set")
			}
		})
	}
}

func TestParseCodexInputArea(t *testing.T) {
	cases := []struct {
		name       string
		fixture    string
		wantBusy   bool
		wantPrompt string
		wantTyped  string
	}{
		{
			name:       "idle empty box",
			fixture:    codexIdle,
			wantPrompt: "> ",
			wantTyped:  "",
		},
		{
			name:       "typed inside box",
			fixture:    codexTypedBoxed,
			wantPrompt: "> ",
			wantTyped:  "fix the failing test",
		},
		{
			name:     "busy",
			fixture:  codexBusy,
			wantBusy: true,
		},
		{
			name:       "most recent box wins over scrollback box",
			fixture:    codexBoxPersistedInScrollback,
			wantPrompt: "> ",
			wantTyped:  "most recent input",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := parseCodexInputArea(c.fixture, nil)
			if got.Provider != InputAreaProviderCodex {
				t.Fatalf("Provider = %q, want %q", got.Provider, InputAreaProviderCodex)
			}
			if got.Busy != c.wantBusy {
				t.Errorf("Busy = %v, want %v", got.Busy, c.wantBusy)
			}
			if got.PromptChar != c.wantPrompt {
				t.Errorf("PromptChar = %q, want %q", got.PromptChar, c.wantPrompt)
			}
			if got.Typed != c.wantTyped {
				t.Errorf("Typed = %q, want %q", got.Typed, c.wantTyped)
			}
			// Ghost is not classified for Codex in stage 2 — see Q2.
			if got.Ghost != "" {
				t.Errorf("Ghost = %q, want empty (Codex ghost classification deferred)", got.Ghost)
			}
		})
	}
}

func TestParseGeminiInputArea(t *testing.T) {
	cases := []struct {
		name       string
		fixture    string
		wantBusy   bool
		wantPrompt string
		wantTyped  string
	}{
		{
			name:       "idle clean row",
			fixture:    geminiIdle,
			wantPrompt: "> ",
			wantTyped:  "",
		},
		{
			name:       "typed",
			fixture:    geminiTyped,
			wantPrompt: "> ",
			wantTyped:  "draft the email",
		},
		{
			name:     "rewind picker reports busy",
			fixture:  geminiRewindPicker,
			wantBusy: true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := parseGeminiInputArea(c.fixture, nil)
			if got.Provider != InputAreaProviderGemini {
				t.Fatalf("Provider = %q, want %q", got.Provider, InputAreaProviderGemini)
			}
			if got.Busy != c.wantBusy {
				t.Errorf("Busy = %v, want %v", got.Busy, c.wantBusy)
			}
			if got.PromptChar != c.wantPrompt {
				t.Errorf("PromptChar = %q, want %q", got.PromptChar, c.wantPrompt)
			}
			if got.Typed != c.wantTyped {
				t.Errorf("Typed = %q, want %q", got.Typed, c.wantTyped)
			}
		})
	}
}

func TestParseGenericInputArea(t *testing.T) {
	cases := []struct {
		name       string
		fixture    string
		cfg        *RuntimeConfig
		wantPrompt string
		wantTyped  string
	}{
		{
			name:       "no cfg returns empty input area",
			fixture:    unknownCustomPrompt,
			cfg:        nil,
			wantPrompt: "",
			wantTyped:  "",
		},
		{
			name:       "cfg with prompt prefix finds prompt row",
			fixture:    unknownCustomPrompt,
			cfg:        &RuntimeConfig{Tmux: &RuntimeTmuxConfig{ReadyPromptPrefix: "$ "}},
			wantPrompt: "$ ",
			wantTyped:  "",
		},
		{
			name: "cfg with prompt prefix and typed content",
			fixture: "" +
				"chrome row\n" +
				"$ echo hi\n",
			cfg:        &RuntimeConfig{Tmux: &RuntimeTmuxConfig{ReadyPromptPrefix: "$ "}},
			wantPrompt: "$ ",
			wantTyped:  "echo hi",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := parseGenericInputArea(c.fixture, c.cfg)
			if got.Provider != InputAreaProviderUnknown {
				t.Fatalf("Provider = %q, want %q", got.Provider, InputAreaProviderUnknown)
			}
			if got.PromptChar != c.wantPrompt {
				t.Errorf("PromptChar = %q, want %q", got.PromptChar, c.wantPrompt)
			}
			if got.Typed != c.wantTyped {
				t.Errorf("Typed = %q, want %q", got.Typed, c.wantTyped)
			}
			if got.Ghost != "" {
				t.Errorf("Ghost = %q, want empty (generic parser does not classify)", got.Ghost)
			}
		})
	}
}

func TestParseGenericInputArea_BusyIndicator(t *testing.T) {
	// Even unknown providers should surface Busy=true when the pane shows
	// a known busy indicator. The consumer guards against false buffered
	// input by checking Busy first, so this case must never silently fall
	// through to Typed extraction.
	fixture := "" +
		"some output\n" +
		"esc to interrupt\n"
	got := parseGenericInputArea(fixture, nil)
	if !got.Busy {
		t.Fatalf("Busy = false, want true for fixture %q", fixture)
	}
}

func TestParseClaudeInputArea_RawSizeBounded(t *testing.T) {
	// The InputArea entry point caps Raw at inputAreaRawCap; the pure
	// parser does not enforce a cap of its own (the cap is a property of
	// the wrapper, not the parser). Lock that contract in so a future
	// refactor that moves the cap into the parser surfaces here.
	huge := strings.Repeat("x", inputAreaRawCap+1024)
	got := parseClaudeInputArea(huge, nil)
	if len(got.Raw) != len(huge) {
		t.Fatalf("parser truncated Raw; len=%d want %d", len(got.Raw), len(huge))
	}
}
