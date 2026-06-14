//go:build integration

package tmux

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestInputAreaLiveTmux exercises the full InputArea capture→parse→classify
// path against a REAL, isolated tmux server (its own -L socket, killed on
// cleanup — it never touches the gc tmux server or any live agent session).
//
// The pure fixture tests in input_area_test.go prove the parser logic against
// saved bytes. This proves the one thing they cannot: that `tmux capture-pane
// -e` on the host's tmux build actually preserves the SGR sequences the
// ghost-text parser depends on, and that capture→parse→classify yields the
// right typed/ghost split on genuine wire bytes. It needs no LLM binary — a
// printf simulates the prompt char plus a dim ghost suggestion. See
// engdocs/design/input-area-state.md §7.2.
//
// The pane runs `bash --norc --noprofile` deliberately: the host operator's
// interactive shell may theme its prompt with ❯ (Powerlevel10k and friends do
// exactly this), and that glyph would leak into the capture and race the
// printf output for the bottom-up prompt scan. A hermetic shell guarantees the
// only ❯ in the pane is the one printf emits.
func TestInputAreaLiveTmux(t *testing.T) {
	if !hasTmux() {
		t.Skip("tmux not installed")
	}

	tm := NewTmuxWithConfig(Config{SocketName: "gc-input-area-it"})
	const sess = "inputarea"
	_, _ = tm.run("kill-server") // clean slate on this isolated socket; ignore if none
	if _, err := tm.run("new-session", "-d", "-s", sess, "-x", "80", "-y", "24", "bash --norc --noprofile"); err != nil {
		t.Skipf("cannot create tmux session (tmux unavailable?): %v", err)
	}
	t.Cleanup(func() { _, _ = tm.run("kill-server") })

	// Force Claude classification regardless of the host (spec §7.2 step 3).
	// InputArea reads GC_PROVIDER from the tmux session environment via
	// show-environment, so this takes effect even though bash is already
	// running with its own (unset) GC_PROVIDER.
	if err := tm.SetEnvironment(sess, "GC_PROVIDER", "claude"); err != nil {
		t.Fatalf("SetEnvironment GC_PROVIDER=claude: %v", err)
	}

	// Wait for the hermetic bash prompt before driving the pane so the first
	// keystrokes are not swallowed by shell startup. `bash --norc` renders a
	// default PS1 of the form "bash-<version>$ ".
	if !waitForPaneContains(t, tm, sess, "bash-", 10*time.Second) {
		t.Skip("hermetic bash prompt never appeared (bash unavailable?)")
	}

	// --- Ghost suggestion: ❯ <dim>ghost text<reset> ---
	// printf turns the \x escapes into real bytes: the ❯ prompt char (UTF-8
	// e2 9d af), a space, the SGR-2 dim wrapper, the suggestion, and a reset.
	// The trailing \n parks the next bash prompt on its own line so it cannot
	// pollute the parsed prompt row.
	sendShellLine(t, tm, sess, `printf '\xe2\x9d\xaf \x1b[2mghost text\x1b[0m\n'`)
	ghost := pollInputArea(t, tm, sess, 10*time.Second, func(s *InputAreaState) bool {
		return s.Ghost != "" || s.Typed != ""
	})
	if ghost.Provider != InputAreaProviderClaude {
		t.Errorf("ghost capture: Provider = %q, want %q", ghost.Provider, InputAreaProviderClaude)
	}
	if ghost.PromptChar != ClaudePromptChar {
		t.Errorf("ghost capture: PromptChar = %q, want %q", ghost.PromptChar, ClaudePromptChar)
	}
	if ghost.Ghost != "ghost text" {
		t.Errorf("ghost capture: Ghost = %q, want %q", ghost.Ghost, "ghost text")
	}
	if ghost.Typed != "" {
		t.Errorf("ghost capture: Typed = %q, want empty — a dim suggestion must not read as typed input", ghost.Typed)
	}
	if ghost.Busy {
		t.Errorf("ghost capture: Busy = true, want false")
	}

	// --- Operator-typed input: ❯ typed input (no dim wrapper) ---
	// A fresh prompt row printed below the ghost row. The parser scans
	// bottom-up, so it binds to this newer row and reports it as typed.
	sendShellLine(t, tm, sess, `printf '\xe2\x9d\xaf typed input\n'`)
	typed := pollInputArea(t, tm, sess, 10*time.Second, func(s *InputAreaState) bool {
		return s.Typed != ""
	})
	if typed.Typed != "typed input" {
		t.Errorf("typed capture: Typed = %q, want %q", typed.Typed, "typed input")
	}
	if typed.Ghost != "" {
		t.Errorf("typed capture: Ghost = %q, want empty — plain input must not read as ghost text", typed.Ghost)
	}
}

// sendShellLine sends line to the session's shell as literal keystrokes and
// submits it with Enter, mirroring how an operator would type a command.
func sendShellLine(t *testing.T, tm *Tmux, sess, line string) {
	t.Helper()
	if _, err := tm.run("send-keys", "-t", sess, "-l", line); err != nil {
		t.Fatalf("send-keys %q: %v", line, err)
	}
	if _, err := tm.run("send-keys", "-t", sess, "Enter"); err != nil {
		t.Fatalf("send-keys Enter: %v", err)
	}
}

// waitForPaneContains polls the plain (ANSI-stripped) pane capture until it
// contains substr or timeout elapses. Returns whether substr appeared.
func waitForPaneContains(t *testing.T, tm *Tmux, sess, substr string, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		out, err := tm.run("capture-pane", "-p", "-t", sess, "-S", "-30")
		if err == nil && strings.Contains(out, substr) {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

// pollInputArea calls InputArea until ready reports the state is settled or
// timeout elapses. Polling (rather than a fixed sleep) absorbs pane-rendering
// latency without coupling the test to a magic delay. It fails the test on a
// capture error or if the state never settles, surfacing the last snapshot.
func pollInputArea(t *testing.T, tm *Tmux, sess string, timeout time.Duration, ready func(*InputAreaState) bool) *InputAreaState {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last *InputAreaState
	for time.Now().Before(deadline) {
		st, err := tm.InputArea(context.Background(), sess)
		if err != nil {
			t.Fatalf("InputArea(%q): %v", sess, err)
		}
		last = st
		if ready(st) {
			return st
		}
		time.Sleep(100 * time.Millisecond)
	}
	if last != nil {
		t.Fatalf("InputArea did not settle within %v; last capture = %+v", timeout, *last)
	}
	t.Fatalf("InputArea did not settle within %v (no captures taken)", timeout)
	return nil
}
