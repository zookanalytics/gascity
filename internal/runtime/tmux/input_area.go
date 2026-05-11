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
	Raw        string            `json:"raw,omitempty"`
	Detected   time.Time         `json:"detected"`
}

// InputAreaCapturer is implemented by runtime providers that expose
// pane-level input-area state. Tmux is the only implementation today;
// non-tmux runtimes (exec, k8s, fake) may add an implementation later
// without changing consumers.
type InputAreaCapturer interface {
	InputArea(ctx context.Context, session string) (*InputAreaState, error)
}

// Capture parameters. Tuned so the default capture is large enough to
// reach the most-recent prompt row even when Claude's full-screen UI
// leaves several blank rows below it, while bounding Raw at 16 KiB so
// the field stays manageable for CLI JSON output and trace logs.
const (
	inputAreaCaptureLines = 50
	inputAreaRawCap       = 16 * 1024
)

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
	if len(rawANSI) > inputAreaRawCap {
		rawANSI = rawANSI[len(rawANSI)-inputAreaRawCap:]
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
// ghost text (wrapped in dim SGR) from operator-typed input. Pure function.
func parseClaudeInputArea(rawANSI string, _ *RuntimeConfig) InputAreaState {
	state := InputAreaState{
		Provider: InputAreaProviderClaude,
		Raw:      rawANSI,
		Detected: time.Now().UTC(),
	}

	strippedLines := strings.Split(stripANSI(rawANSI), "\n")
	if paneContainsBusyIndicator(strippedLines) {
		state.Busy = true
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

// parseCodexInputArea parses a Codex pane capture. Codex frames its input
// area inside a Unicode box; we locate the most-recent `│ > ` row, which
// is the prompt cell inside the active box. Ghost text is not observed in
// current Codex builds (see engdocs/design/input-area-state.md §10 Q2);
// stage 4 will re-verify against live captures before adding ghost support.
func parseCodexInputArea(rawANSI string, _ *RuntimeConfig) InputAreaState {
	state := InputAreaState{
		Provider: InputAreaProviderCodex,
		Raw:      rawANSI,
		Detected: time.Now().UTC(),
	}

	strippedLines := strings.Split(stripANSI(rawANSI), "\n")
	if paneContainsBusyIndicator(strippedLines) {
		state.Busy = true
		return state
	}

	// Scan bottom-up for the most-recent box-prompt row. Matching the full
	// "│ > " sequence anchors to the active box and ignores older box
	// fragments that may still be in scrollback (see §3.2 multi-line note).
	for i := len(strippedLines) - 1; i >= 0; i-- {
		line := strippedLines[i]
		idx := strings.Index(line, "│ > ")
		if idx == -1 {
			continue
		}
		state.PromptChar = "> "
		content := line[idx+len("│ > "):]
		// Trim the trailing box-right side and any padding spaces it
		// leaves behind. The box right side can be "│" preceded by run
		// of spaces used to pad the cell out to the box width.
		content = strings.TrimRight(content, "│ \t")
		state.Typed = content
		return state
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
		Raw:      rawANSI,
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
		Raw:      rawANSI,
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

// splitDimSegments walks input as a stream of (text, dim) segments and
// returns concatenated typed and ghost runs. Dim state mutates across SGR
// codes; non-CSI runs of bytes are emitted with the current state.
//
// Union of "dim" SGRs recognized today (engdocs/design/input-area-state.md §10 Q1):
//   - SGR 2  — canonical faint/dim attribute Claude Code uses today
//   - SGR 90 — bright-black foreground (renders as gray on most terminals)
//   - SGR 38;5;{240..243} — common 256-color dim grays
//
// True-color dim (38;2;r;g;b) is intentionally not classified — there is no
// stable cutoff between "dim color" and "regular dim-ish theme color" and
// no current LLM uses it. Resets (SGR 0, empty, 22) clear the dim flag.
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

// updateDimState parses one SGR parameter group and returns the next dim
// state. Recognizes the canonical Claude wrapper (`ESC[2m`...`ESC[0m`) plus
// the dim-equivalent variants documented in splitDimSegments.
func updateDimState(dim bool, params string) bool {
	if params == "" {
		// Empty params is SGR-default-zero, which resets attributes.
		return false
	}
	parts := strings.Split(params, ";")
	for i := 0; i < len(parts); i++ {
		switch parts[i] {
		case "0", "":
			dim = false
		case "2":
			dim = true
		case "22":
			dim = false
		case "90", "91", "92", "93", "94", "95", "96", "97":
			// "Bright" foreground 90 is dim-grey on most terminals.
			// 91-97 are bright colors (not dim); only 90 sets dim, but
			// we still need to consume the param to avoid misreading
			// later params in the same group.
			if parts[i] == "90" {
				dim = true
			}
		case "38":
			// Extended foreground: 38;5;n (256-color) or 38;2;r;g;b
			// (true color). Consume those params so we don't misread n
			// as an SGR code on its own.
			if i+2 < len(parts) && parts[i+1] == "5" {
				switch parts[i+2] {
				case "240", "241", "242", "243":
					dim = true
				}
				i += 2
			} else if i+4 < len(parts) && parts[i+1] == "2" {
				i += 4
			}
		}
	}
	return dim
}
