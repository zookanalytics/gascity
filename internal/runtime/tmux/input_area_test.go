package tmux

import (
	"testing"
)

// Fixtures are kept inline as Go raw-string literals so a new regression
// is one variable + one table entry. The SGR macros below mirror the
// sequences tmux capture-pane -e emits; using them in the fixtures keeps the
// wire bytes visible to a reader scanning the file. Real captures from a live
// session match these patterns byte-for-byte; the Appendix C fixture in
// engdocs/design/input-area-state.md is the canonical reference.
//
// Ghost text is detected by the faint attribute (dimOn = SGR 2) alone. The
// color macros (gray256, brBlack) are present only to prove that color is NOT
// treated as ghost — only the faint attribute is.
const (
	dimOn     = "\x1b[2m"        // SGR 2 — faint; the load-bearing ghost signal
	dimReset  = "\x1b[0m"        // SGR 0 — reset all attributes
	dimOff22  = "\x1b[22m"       // SGR 22 — normal intensity (clears faint)
	defaultFg = "\x1b[39m"       // SGR 39 — default foreground (also clears faint)
	gray256   = "\x1b[38;5;240m" // 256-color gray — a color, not faint
	brBlack   = "\x1b[90m"       // bright-black foreground — a color, not faint
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

// claudeColorAfterPrompt proves the faint rule end-to-end at the parser
// level: bright-black is a color, not the faint attribute, so text rendered in
// it is operator-typed input, not ghost. This is the inverse of the old
// SGR-90/256-gray "ghost" fixtures the gray-range heuristic used to match —
// under the faint rule, only ESC[2m is ghost (gc-8g41r.7).
const claudeColorAfterPrompt = "" +
	"❯ " + brBlack + "bright black input" + dimReset + "\n"

// claudeFeedbackSurvey reproduces the Claude Code session-feedback overlay
// (operator clarification 2026-05-30, gc-8g41r). It is dismiss-on-any-keystroke,
// not a blocking dialog. With no normal "❯ " row visible, the pre-survey parser
// returned an empty non-prompt state that idle-detection read as a 30h stall
// (boot lo-wisp-h3vuf). Survey detection reclassifies it as ready-for-input so
// the idle→input path dismisses it. The 0–3 options row must not be read as
// buffered input.
const claudeFeedbackSurvey = "" +
	"⏺ Pushed the branch; handed off to the refinery.\n" +
	"\n" +
	"How is Claude doing this session?\n" +
	"  0 Bad   1 Poor   2 Good   3 Great\n"

// claudeSurveyDuringBusy locks in precedence: a busy indicator outranks the
// survey overlay, so the parser still reports Busy=true and never treats a
// working engine as ready for input.
const claudeSurveyDuringBusy = "" +
	"How is Claude doing this session?\n" +
	"⏺ Running tool …\n" +
	"  esc to interrupt\n"

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

// codexArrowIdle is the current Codex arrow shape with an empty input area.
const codexArrowIdle = "" +
	"› \n"

// codexArrowTyped is operator-typed text after the arrow prompt.
const codexArrowTyped = "" +
	"› wire up the parser\n"

// codexArrowGhost is a faint suggestion after the arrow prompt. The shared
// faint rule must land it in Ghost, never Typed — the false positive this
// parser exists to prevent. No Codex-specific ghost code is involved.
const codexArrowGhost = "" +
	"› " + dimOn + "run the failing test" + dimReset + "\n"

// codexArrowQueuedFollowup puts a "Queued follow-up inputs" affordance above
// the prompt. Only the arrow row is the input area; the queued chrome must be
// ignored (§3.2.2) or it reintroduces the false-positive class.
const codexArrowQueuedFollowup = "" +
	"Queued follow-up inputs:\n" +
	"  - deploy after merge\n" +
	"\n" +
	"› actual typed text\n"

// codexArrowBelowStaleBox proves the bottom-up scan binds to the live arrow
// row even when a stale box lingers in scrollback above it.
const codexArrowBelowStaleBox = "" +
	"╭───────────────────────────────────────╮\n" +
	"│ > stale boxed content                 │\n" +
	"╰───────────────────────────────────────╯\n" +
	"[tool output]\n" +
	"› fresh arrow input\n"

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
		{"faint via SGR 2 is ghost", dimOn + "ghost" + dimReset, "", "ghost"},
		{"SGR 22 clears faint", dimOn + "ghost" + dimOff22 + "back", "back", "ghost"},
		{"SGR 39 default-fg clears faint", dimOn + "ghost" + defaultFg + "back", "back", "ghost"},
		// Color attributes are NOT faint: the faint rule classifies them as
		// typed, replacing the old gray-range heuristic (codex finding #2).
		{"bright-black (SGR 90) is color, not faint", brBlack + "grey" + dimReset, "grey", ""},
		{"256-color gray (38;5;240) is color, not faint", gray256 + "x" + dimReset, "x", ""},
		{"256-color 244 is color, not faint", "\x1b[38;5;244mx\x1b[0m", "x", ""},
		// The "2" in a 256-color index must not be misread as the faint code.
		{"256-color index 2 is not the faint attribute", "\x1b[38;5;2mx\x1b[0m", "x", ""},
		{"true-color is not faint", "\x1b[38;2;100;100;100mx\x1b[0m", "x", ""},
		{"mixed typed + faint ghost", "typed " + dimOn + "and ghost" + dimReset + " more", "typed  more", "and ghost"},
		{"empty params resets faint", dimOn + "g" + "\x1b[m" + "t", "t", "g"},
		{"unrelated SGR (color) does not set faint", "\x1b[31mred typed\x1b[0m", "red typed", ""},
		// SGR 1 (bold) co-set with 2 (faint) — both apply; faint wins for ghost.
		{"bold+faint combined is ghost", "\x1b[1;2mboldim\x1b[0m", "", "boldim"},
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
			name:       "color after prompt is typed, not ghost (faint rule)",
			fixture:    claudeColorAfterPrompt,
			wantPrompt: ClaudePromptChar,
			wantTyped:  "bright black input",
		},
		{
			// Survey overlay with no normal prompt row visible. Without
			// survey detection this returns PromptChar="" (a non-prompt
			// "stall"); with it, ready-for-input so the idle→input path
			// dismisses it. Option text must not surface as Typed.
			name:       "feedback survey is ready-for-input, not a stall",
			fixture:    claudeFeedbackSurvey,
			wantPrompt: ClaudePromptChar,
			wantTyped:  "",
			wantGhost:  "",
		},
		{
			name:     "busy beats feedback survey",
			fixture:  claudeSurveyDuringBusy,
			wantBusy: true,
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
		wantGhost  string
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
		{
			name:       "arrow idle (current Codex shape)",
			fixture:    codexArrowIdle,
			wantPrompt: "› ",
			wantTyped:  "",
		},
		{
			name:       "arrow typed",
			fixture:    codexArrowTyped,
			wantPrompt: "› ",
			wantTyped:  "wire up the parser",
		},
		{
			name:       "arrow faint suggestion is ghost, not typed",
			fixture:    codexArrowGhost,
			wantPrompt: "› ",
			wantTyped:  "",
			wantGhost:  "run the failing test",
		},
		{
			name:       "arrow ignores queued-follow-up chrome above prompt",
			fixture:    codexArrowQueuedFollowup,
			wantPrompt: "› ",
			wantTyped:  "actual typed text",
		},
		{
			name:       "live arrow row below stale box wins (bottom-up)",
			fixture:    codexArrowBelowStaleBox,
			wantPrompt: "› ",
			wantTyped:  "fresh arrow input",
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
			if got.Ghost != c.wantGhost {
				t.Errorf("Ghost = %q, want %q", got.Ghost, c.wantGhost)
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
