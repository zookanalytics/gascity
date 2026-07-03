package tmux

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"
)

// InputAreaProvider identifies the LLM runtime backing a session. It is the
// enum used by [InputAreaState.Provider] — the new struct field — and is
// distinct from the existing [Provider] struct in adapter.go, which adapts
// the tmux executor to runtime.Provider.
//
// JSON wire form: "claude" | "codex" | "gemini" | "unknown".
type InputAreaProvider string

// Known InputAreaProvider values. Stable wire identifiers — adding a new
// provider requires registering a parser in [Tmux.InputArea] and listing
// the value here so callers can discriminate without string magic.
const (
	InputAreaProviderClaude  InputAreaProvider = "claude"
	InputAreaProviderCodex   InputAreaProvider = "codex"
	InputAreaProviderGemini  InputAreaProvider = "gemini"
	InputAreaProviderUnknown InputAreaProvider = "unknown"
)

// ClaudePromptChar is the Claude Code prompt character followed by a regular
// space. The NBSP variant is normalized at parse time, so this is the
// canonical form returned in [InputAreaState.PromptChar].
const ClaudePromptChar = "❯ "

// claudeFeedbackSurveyMarker is the stable question text of the Claude Code
// session-feedback survey ("How is Claude doing this session?", rated 0–3).
// Detection anchors on this invariant question text rather than the survey's
// box-drawing chrome, which varies across builds. See
// [parseClaudeInputArea] for why the survey is classified as ready-for-input.
const claudeFeedbackSurveyMarker = "How is Claude doing this session?"

// InputAreaState reports what the agent's input area is showing at the
// moment of capture. Snapshot, not a stream — callers that poll should
// rate-limit themselves.
//
// Field semantics are defined in engdocs/design/input-area-state.md §4.2.
// Briefly:
//   - Busy=true   ⇒ Typed/Ghost may be empty even if pane text exists.
//   - PromptChar=="" && !Busy ⇒ pane is in a non-prompt state.
//   - Typed and Ghost are not mutually exclusive in the type system, but
//     the underlying LLMs all clear ghost text once the operator types.
type InputAreaState struct {
	Provider   InputAreaProvider `json:"provider"`
	PromptChar string            `json:"prompt_char"`
	Busy       bool              `json:"busy"`
	Typed      string            `json:"typed"`
	Ghost      string            `json:"ghost"`
	Detected   time.Time         `json:"detected"`
}

// InputAreaCapturer is implemented by runtime providers that expose
// pane-level input-area state. Tmux is the only implementation today;
// non-tmux runtimes (exec, k8s, fake) may add an implementation later
// without changing consumers.
type InputAreaCapturer interface {
	InputArea(ctx context.Context, session string) (*InputAreaState, error)
}

// inputAreaCaptureLines bounds the pane capture. Tuned large enough to reach
// the most-recent prompt row even when a full-screen agent UI leaves several
// blank rows below it.
const inputAreaCaptureLines = 50

// InputArea returns the current input-area state for session. The call hits
// tmux directly (no cache). For non-tmux providers, prefer adding an
// [InputAreaCapturer] implementation rather than fanning out heuristics.
//
// Errors wrap [ErrSessionNotFound] for missing sessions and otherwise carry
// a "capturing input area for %q: %w" context prefix.
func (t *Tmux) InputArea(ctx context.Context, session string) (*InputAreaState, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	rawANSI, err := t.capturePaneANSI(ctx, session, inputAreaCaptureLines)
	if err != nil {
		return nil, fmt.Errorf("capturing input area for %q: %w", session, err)
	}

	provider := t.detectInputAreaProvider(session)

	// Build a minimal cfg for the generic parser when a session-level
	// prompt prefix is set. Provider-specific parsers do not need cfg.
	var cfg *RuntimeConfig
	if prefix, prefixErr := t.GetEnvironment(session, sessionReadyPromptEnvKey); prefixErr == nil && strings.TrimSpace(prefix) != "" {
		cfg = &RuntimeConfig{Tmux: &RuntimeTmuxConfig{ReadyPromptPrefix: prefix}}
	}

	var state InputAreaState
	switch provider {
	case InputAreaProviderClaude:
		state = parseClaudeInputArea(rawANSI, cfg)
	case InputAreaProviderCodex:
		state = parseCodexInputArea(rawANSI, cfg)
	case InputAreaProviderGemini:
		state = parseGeminiInputArea(rawANSI, cfg)
	default:
		state = parseGenericInputArea(rawANSI, cfg)
	}
	return &state, nil
}

// capturePaneANSI runs `tmux capture-pane -p -e -J -t <session> -S -<lines>`.
//
// The -e flag preserves SGR escape sequences (needed for ghost-text
// detection). The -J flag joins terminal-wrapped lines into single logical
// rows; we picked -J in stage 2 because the ghost-vs-typed parser is much
// simpler when wrapped input rows do not need to be re-stitched after the
// fact. Real "operator pressed Enter" newlines submit and never appear in
// the input area, so the -J trade-off has no downside for buffered-input
// detection. (See engdocs/design/input-area-state.md §10 Q5.)
func (t *Tmux) capturePaneANSI(ctx context.Context, session string, lines int) (string, error) {
	return t.runCtx(ctx, "capture-pane", "-p", "-e", "-J", "-t", session, "-S", fmt.Sprintf("-%d", lines))
}

// detectInputAreaProvider chooses the parser using GC_PROVIDER first (set
// by the supervisor on every managed session) and falling back to
// process-tree inspection when the env var is empty.
func (t *Tmux) detectInputAreaProvider(session string) InputAreaProvider {
	switch t.providerEnv(session) {
	case "claude":
		return InputAreaProviderClaude
	case "codex":
		return InputAreaProviderCodex
	case "gemini":
		return InputAreaProviderGemini
	case "":
		// Fall through to process-tree detection below.
	default:
		// Explicit but unrecognized provider — treat as unknown rather
		// than misclassifying as one of the known shapes.
		return InputAreaProviderUnknown
	}

	if t.targetLooksLikeProvider(session, "claude") {
		return InputAreaProviderClaude
	}
	if t.targetLooksLikeProvider(session, "codex") {
		return InputAreaProviderCodex
	}
	if t.targetLooksLikeProvider(session, "gemini") {
		return InputAreaProviderGemini
	}
	return InputAreaProviderUnknown
}

// ---------------------------------------------------------------------------
// Per-provider parsers. Each is pure: same input → same output, no I/O.
// Stable contract for table-driven fixture tests.
// ---------------------------------------------------------------------------

// claudePromptRegex matches the Claude prompt char followed by either a
// regular space or a non-breaking space (U+00A0). Claude Code switches
// between them across builds; both forms must be recognized identically.
// See engdocs/design/input-area-state.md §3.1 and tmux.go matchesPromptPrefix.
var claudePromptRegex = regexp.MustCompile("❯[  ]")

// parseClaudeInputArea parses a Claude Code pane capture, distinguishing
// ghost text (wrapped in the faint SGR attribute) from operator-typed input.
// Pure function.
func parseClaudeInputArea(rawANSI string, _ *RuntimeConfig) InputAreaState {
	state := InputAreaState{
		Provider: InputAreaProviderClaude,
		Detected: time.Now().UTC(),
	}

	strippedLines := strings.Split(stripANSI(rawANSI), "\n")
	if paneContainsBusyIndicator(strippedLines) {
		state.Busy = true
		return state
	}

	// Claude Code's session-feedback survey ("How is Claude doing this
	// session?", rated 0–3) is a dismiss-on-any-keystroke overlay, not a
	// blocking dialog. Classify it as ready-for-input — Busy=false with the
	// prompt char set — so idle-detection consumers send their next action
	// (which dismisses the survey) instead of warranting a false stall. The
	// check runs after the busy gate (a working engine still wins) and before
	// the prompt scan so survey option text is never read as buffered input.
	// See engdocs/design/input-area-state.md §3.1 and the 2026-05-30 operator
	// clarification on gc-8g41r.
	//
	// Input-safety note for consumers: auto-input senders must never send a
	// bare standalone digit 0–3 — the survey records that as a rating. Any
	// other input (a command, "311", a nudge) dismisses it without rating.
	if paneShowsClaudeFeedbackSurvey(strippedLines) {
		state.PromptChar = ClaudePromptChar
		return state
	}

	// Walk lines from the bottom so the most recent prompt wins over any
	// scrollback echoes of an earlier prompt that happen to remain after
	// tool-call output cleared.
	ansiLines := strings.Split(rawANSI, "\n")
	for i := len(ansiLines) - 1; i >= 0; i-- {
		loc := claudePromptRegex.FindStringIndex(ansiLines[i])
		if loc == nil {
			continue
		}
		state.PromptChar = ClaudePromptChar
		content := ansiLines[i][loc[1]:]
		typed, ghost := splitDimSegments(content)
		state.Typed = strings.TrimRight(typed, " \t")
		state.Ghost = strings.TrimRight(ghost, " \t")
		return state
	}
	return state
}

// paneShowsClaudeFeedbackSurvey reports whether the stripped pane lines show
// the Claude Code session-feedback survey. It keys on the invariant question
// text (see [claudeFeedbackSurveyMarker]) rather than the box-drawing chrome,
// which differs across builds. Pure function — same input, same output.
func paneShowsClaudeFeedbackSurvey(strippedLines []string) bool {
	for _, line := range strippedLines {
		if strings.Contains(line, claudeFeedbackSurveyMarker) {
			return true
		}
	}
	return false
}

// codexBoxedAnchor is the inner prompt cell of Codex's older boxed input
// shape; codexArrowPrompt is the current clean arrow prompt (U+203A + space).
// The parser recognizes both — see engdocs/design/input-area-state.md §3.2.
const (
	codexBoxedAnchor = "│ > "
	codexArrowPrompt = "› "
)

// parseCodexInputArea parses a Codex pane capture. Codex has shipped two
// input-area shapes: the older Unicode box (with a "│ > " prompt cell) and the
// current clean arrow prompt ("› "). The parser scans bottom-up and binds to
// the most-recent prompt row of either shape, so a stale box or arrow row left
// in scrollback never wins over the live prompt row. Ghost text after the
// arrow prompt is wrapped in the faint SGR attribute and split out by the same
// faint rule the Claude parser uses — there is no Codex-specific ghost code.
// The boxed shape has no observed ghost text, so its cell content is all typed.
// Pure function.
func parseCodexInputArea(rawANSI string, _ *RuntimeConfig) InputAreaState {
	state := InputAreaState{
		Provider: InputAreaProviderCodex,
		Detected: time.Now().UTC(),
	}

	ansiLines := strings.Split(rawANSI, "\n")
	if paneContainsBusyIndicator(strings.Split(stripANSI(rawANSI), "\n")) {
		state.Busy = true
		return state
	}

	// Scan bottom-up so the live prompt row wins over any stale box or arrow
	// row still in scrollback (§3.2).
	for i := len(ansiLines) - 1; i >= 0; i-- {
		raw := ansiLines[i]
		stripped := stripANSI(raw)

		// Shape A — boxed. The full "│ > " anchor (left border, padding,
		// prompt glyph, space) avoids false-matching a stray ">" elsewhere.
		// Trim the right border "│" and the padding spaces before it.
		if idx := strings.Index(stripped, codexBoxedAnchor); idx != -1 {
			state.PromptChar = "> "
			state.Typed = strings.TrimRight(stripped[idx+len(codexBoxedAnchor):], "│ \t")
			return state
		}

		// Shape B — arrow. Require the prompt to start the row (only leading
		// whitespace before it) so a "›" inside a "Queued follow-up inputs"
		// affordance above the prompt is never read as the input area. Match
		// on the raw line so the styled content survives for faint-ghost
		// separation; the arrow glyph itself is never styled.
		if idx := strings.Index(raw, codexArrowPrompt); idx != -1 &&
			strings.TrimLeft(stripANSI(raw[:idx]), " \t") == "" {
			state.PromptChar = codexArrowPrompt
			typed, ghost := splitDimSegments(raw[idx+len(codexArrowPrompt):])
			state.Typed = strings.TrimRight(typed, " \t")
			state.Ghost = strings.TrimRight(ghost, " \t")
			return state
		}
	}
	return state
}

// parseGeminiInputArea parses a Gemini pane capture. Gemini's input area is
// a clean `> ` row (no box framing in current builds); rewind dialogs are
// distinct UIs and indicate the pane is not at a normal prompt.
//
// Live verification against polecat-gemini is currently blocked (tk-mmny1),
// so this parser defaults to the spec's clean-row assumption from §3.3.
// Stage 4 should re-verify against a live Gemini capture.
func parseGeminiInputArea(rawANSI string, _ *RuntimeConfig) InputAreaState {
	state := InputAreaState{
		Provider: InputAreaProviderGemini,
		Detected: time.Now().UTC(),
	}

	strippedLines := strings.Split(stripANSI(rawANSI), "\n")

	// Gemini rewind dialogs occupy the pane; treat them as non-prompt by
	// reporting Busy=true so consumers do not interpret dialog text as
	// buffered input.
	for _, line := range strippedLines {
		if strings.Contains(line, "Cancel rewind and stay here") ||
			strings.Contains(line, "> Rewind") {
			state.Busy = true
			return state
		}
	}
	if paneContainsBusyIndicator(strippedLines) {
		state.Busy = true
		return state
	}

	for i := len(strippedLines) - 1; i >= 0; i-- {
		trimmed := strings.TrimLeft(strippedLines[i], " \t")
		if !strings.HasPrefix(trimmed, "> ") {
			continue
		}
		state.PromptChar = "> "
		state.Typed = strings.TrimRight(trimmed[len("> "):], " \t")
		return state
	}
	return state
}

// parseGenericInputArea is the unknown-provider fallback. It only fills in
// PromptChar/Typed when the session sets a non-empty ReadyPromptPrefix via
// [sessionReadyPromptEnvKey]; otherwise it returns a partial state with the
// Raw capture and an empty input area. Ghost detection is off — generic
// parsers cannot know which SGR convention a custom runtime uses.
func parseGenericInputArea(rawANSI string, cfg *RuntimeConfig) InputAreaState {
	state := InputAreaState{
		Provider: InputAreaProviderUnknown,
		Detected: time.Now().UTC(),
	}

	strippedLines := strings.Split(stripANSI(rawANSI), "\n")
	if paneContainsBusyIndicator(strippedLines) {
		state.Busy = true
		return state
	}

	if cfg == nil || cfg.Tmux == nil || strings.TrimSpace(cfg.Tmux.ReadyPromptPrefix) == "" {
		return state
	}
	prefix := cfg.Tmux.ReadyPromptPrefix
	normalizedPrefix := strings.ReplaceAll(prefix, " ", " ")
	for i := len(strippedLines) - 1; i >= 0; i-- {
		line := strings.ReplaceAll(strippedLines[i], " ", " ")
		trimmed := strings.TrimLeft(line, " \t")
		if !strings.HasPrefix(trimmed, normalizedPrefix) {
			continue
		}
		state.PromptChar = prefix
		state.Typed = strings.TrimRight(trimmed[len(normalizedPrefix):], " \t")
		return state
	}
	return state
}

// ---------------------------------------------------------------------------
// ANSI helpers. Shared by the parsers; tested directly in input_area_test.go.
// ---------------------------------------------------------------------------

// ansiCSIRegex matches a CSI sequence: ESC [ <params> <final byte>.
// Params are 0-9, ;, : (separator forms), and the private-mode markers
// ?, <, >, =. The final byte is any letter (uppercase or lowercase).
// See https://en.wikipedia.org/wiki/ANSI_escape_code#CSI_(Control_Sequence_Introducer)_sequences
var ansiCSIRegex = regexp.MustCompile("\x1b\\[[0-9;:?<>=]*[a-zA-Z]")

// stripANSI removes CSI sequences from s. Other escape forms (OSC, DCS) are
// not observed in tmux capture-pane -e output and are left untouched.
func stripANSI(s string) string {
	return ansiCSIRegex.ReplaceAllString(s, "")
}

// splitDimSegments walks input as a stream of (text, faint) segments and
// returns the concatenated typed and ghost runs. Ghost text is whatever is
// rendered with the faint SGR attribute (`ESC[2m`); everything else is typed.
// The faint state mutates across SGR codes; non-CSI runs of bytes are emitted
// under the current state.
//
// Faint is the load-bearing, LLM-agnostic ghost signal: real typed input is
// never rendered faint, so no per-provider color table is needed (gc-8g41r.7).
// Color attributes (bright-black, 256-color grays, true color) are NOT treated
// as ghost — only the faint attribute is. Resets `ESC[0m` / `ESC[22m` /
// `ESC[39m` (and an empty SGR, which means 0) clear the faint state.
func splitDimSegments(input string) (typed string, ghost string) {
	var typedB, ghostB strings.Builder
	dim := false
	for len(input) > 0 {
		idx := strings.Index(input, "\x1b[")
		if idx == -1 {
			emitInputSegment(&typedB, &ghostB, input, dim)
			break
		}
		if idx > 0 {
			emitInputSegment(&typedB, &ghostB, input[:idx], dim)
		}
		rest := input[idx+2:]
		end := indexOfCSIFinalByte(rest)
		if end == -1 {
			// Malformed CSI — emit the rest as text under the current
			// state. This is defensive; capture-pane -e does not emit
			// half-formed CSIs in practice.
			emitInputSegment(&typedB, &ghostB, input[idx:], dim)
			break
		}
		params := rest[:end]
		finalByte := rest[end]
		input = rest[end+1:]
		if finalByte == 'm' {
			dim = updateDimState(dim, params)
		}
	}
	return typedB.String(), ghostB.String()
}

func emitInputSegment(typed, ghost *strings.Builder, text string, dim bool) {
	if text == "" {
		return
	}
	if dim {
		ghost.WriteString(text)
		return
	}
	typed.WriteString(text)
}

// indexOfCSIFinalByte returns the index of the first byte in s that ends
// a CSI sequence (an ASCII letter), or -1 if no terminator is present.
func indexOfCSIFinalByte(s string) int {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') {
			return i
		}
	}
	return -1
}

// updateDimState parses one SGR parameter group and returns the next faint
// state. Ghost text is detected solely by the faint attribute (SGR 2); the
// terminators SGR 0 (reset all), 22 (normal intensity), and 39 (default
// foreground) clear it, as does an empty parameter (which means 0). Extended
// color introducers (38/48 with a 5;n 256-color or 2;r;g;b true-color payload)
// are skipped so a color index can never be misread as the standalone faint
// code (e.g. the "2" in "38;5;2").
func updateDimState(faint bool, params string) bool {
	if params == "" {
		// Empty params == SGR 0 == reset all attributes.
		return false
	}
	parts := strings.Split(params, ";")
	for i := 0; i < len(parts); i++ {
		switch parts[i] {
		case "0", "", "22", "39":
			faint = false
		case "2":
			faint = true
		case "38", "48":
			// Extended fg/bg color: "5;n" (256-color) or "2;r;g;b" (true
			// color). Consume the payload so its numbers are never read as
			// SGR attribute codes.
			if i+2 < len(parts) && parts[i+1] == "5" {
				i += 2
			} else if i+4 < len(parts) && parts[i+1] == "2" {
				i += 4
			}
		}
	}
	return faint
}
