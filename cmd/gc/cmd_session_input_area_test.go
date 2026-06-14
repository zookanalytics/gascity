package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/runtime"
	"github.com/gastownhall/gascity/internal/runtime/tmux"
	"github.com/gastownhall/gascity/internal/session"
)

// fakeInputAreaProvider embeds the runtime fake and adds the tmux capture
// capabilities (PeekRaw, InputArea) so CLI tests can exercise raw peek and
// input-area without a live tmux server.
type fakeInputAreaProvider struct {
	*runtime.Fake
	rawOut   string
	rawErr   error
	state    *tmux.InputAreaState
	stateErr error
}

func (f *fakeInputAreaProvider) PeekRaw(_ string, _ int) (string, error) {
	return f.rawOut, f.rawErr
}

func (f *fakeInputAreaProvider) InputArea(_ context.Context, _ string) (*tmux.InputAreaState, error) {
	return f.state, f.stateErr
}

// newInputAreaTestSession wires a fake provider with the capture capabilities
// into the CLI and creates an awake session bead pointing at "runtime-session".
// Returns the bead id and the fake so tests can set capture results.
func newInputAreaTestSession(t *testing.T) (sessionID string, fake *fakeInputAreaProvider) {
	t.Helper()
	clearGCEnv(t)
	clearInheritedCityRoutingEnv(t)
	t.Setenv("GC_BEADS", "file")
	t.Setenv("GC_SESSION", "fake")

	cityDir := t.TempDir()
	t.Setenv("GC_CITY", cityDir)
	writeNamedSessionCityTOML(t, cityDir)

	fake = &fakeInputAreaProvider{Fake: runtime.NewFake()}
	fake.SetPeekOutput("runtime-session", "stripped\n")
	oldBuild := buildSessionProviderByName
	buildSessionProviderByName = func(string, config.SessionConfig, string, string) (runtime.Provider, error) {
		return fake, nil
	}
	t.Cleanup(func() { buildSessionProviderByName = oldBuild })

	store, err := openCityStoreAt(cityDir)
	if err != nil {
		t.Fatalf("openCityStoreAt(%q): %v", cityDir, err)
	}
	b, err := store.Create(beads.Bead{
		Title:  "input-area session",
		Type:   session.BeadType,
		Labels: []string{session.LabelSession},
		Metadata: map[string]string{
			"session_name": "runtime-session",
			"template":     "worker",
			"state":        "awake",
			"work_dir":     cityDir,
		},
	})
	if err != nil {
		t.Fatalf("store.Create(session): %v", err)
	}
	return b.ID, fake
}

func TestCmdSessionInputAreaJSONSeparatesGhostFromTyped(t *testing.T) {
	sessionID, fake := newInputAreaTestSession(t)
	fake.state = &tmux.InputAreaState{
		Provider:   tmux.InputAreaProviderClaude,
		PromptChar: tmux.ClaudePromptChar,
		Ghost:      "keep patrolling",
		Raw:        "❯ \x1b[2mkeep patrolling\x1b[0m",
	}

	var stdout, stderr bytes.Buffer
	if code := cmdSessionInputArea(sessionID, "json", false, &stdout, &stderr); code != 0 {
		t.Fatalf("cmdSessionInputArea = %d, want 0; stderr=%s", code, stderr.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
	if lines := strings.Count(strings.TrimSuffix(stdout.String(), "\n"), "\n"); lines != 0 {
		t.Fatalf("stdout should be a single JSONL record, got:\n%s", stdout.String())
	}
	var got sessionInputAreaJSON
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("stdout not JSON: %v\n%s", err, stdout.String())
	}
	if got.Provider != tmux.InputAreaProviderClaude {
		t.Fatalf("provider = %q, want claude", got.Provider)
	}
	if got.Ghost != "keep patrolling" || got.Typed != "" {
		t.Fatalf("ghost/typed split wrong: typed=%q ghost=%q", got.Typed, got.Ghost)
	}
	if got.Session != sessionID {
		t.Fatalf("session = %q, want %q", got.Session, sessionID)
	}
	if got.Raw != "" {
		t.Fatalf("raw must be omitted without --include-raw, got %q", got.Raw)
	}
}

func TestCmdSessionInputAreaIncludeRaw(t *testing.T) {
	sessionID, fake := newInputAreaTestSession(t)
	fake.state = &tmux.InputAreaState{
		Provider: tmux.InputAreaProviderClaude,
		Raw:      "❯ \x1b[2mghost\x1b[0m",
	}

	var stdout, stderr bytes.Buffer
	if code := cmdSessionInputArea(sessionID, "json", true, &stdout, &stderr); code != 0 {
		t.Fatalf("cmdSessionInputArea(--include-raw) = %d; stderr=%s", code, stderr.String())
	}
	var got sessionInputAreaJSON
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("stdout not JSON: %v\n%s", err, stdout.String())
	}
	if got.Raw != "❯ \x1b[2mghost\x1b[0m" {
		t.Fatalf("raw = %q, want the ANSI capture", got.Raw)
	}
}

func TestCmdSessionInputAreaKVAndText(t *testing.T) {
	sessionID, fake := newInputAreaTestSession(t)
	fake.state = &tmux.InputAreaState{
		Provider:   tmux.InputAreaProviderClaude,
		PromptChar: tmux.ClaudePromptChar,
		Typed:      "hello",
	}

	for _, format := range []string{"kv", "text"} {
		var stdout, stderr bytes.Buffer
		if code := cmdSessionInputArea(sessionID, format, false, &stdout, &stderr); code != 0 {
			t.Fatalf("format %s = %d; stderr=%s", format, code, stderr.String())
		}
		out := stdout.String()
		if !strings.Contains(out, `typed="hello"`) {
			t.Fatalf("format %s missing typed: %q", format, out)
		}
		if !strings.Contains(out, "claude") {
			t.Fatalf("format %s missing provider: %q", format, out)
		}
	}
}

func TestCmdSessionInputAreaInvalidFormat(t *testing.T) {
	sessionID, _ := newInputAreaTestSession(t)
	var stdout, stderr bytes.Buffer
	if code := cmdSessionInputArea(sessionID, "yaml", false, &stdout, &stderr); code != 1 {
		t.Fatalf("invalid --format code = %d, want 1", code)
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want empty on invalid format", stdout.String())
	}
}

func TestCmdSessionInputAreaUnknownSessionExits2(t *testing.T) {
	newInputAreaTestSession(t)
	var stdout, stderr bytes.Buffer
	if code := cmdSessionInputArea("no-such-session", "json", false, &stdout, &stderr); code != 2 {
		t.Fatalf("unknown session code = %d, want 2; stderr=%s", code, stderr.String())
	}
}

func TestCmdSessionInputAreaCaptureNotFoundExits2(t *testing.T) {
	sessionID, fake := newInputAreaTestSession(t)
	fake.stateErr = runtime.ErrSessionNotFound

	var stdout, stderr bytes.Buffer
	if code := cmdSessionInputArea(sessionID, "json", false, &stdout, &stderr); code != 2 {
		t.Fatalf("capture-not-found code = %d, want 2; stderr=%s", code, stderr.String())
	}
}

func TestCmdSessionInputAreaUnsupportedProviderExits1(t *testing.T) {
	clearGCEnv(t)
	clearInheritedCityRoutingEnv(t)
	t.Setenv("GC_BEADS", "file")
	t.Setenv("GC_SESSION", "fake")
	cityDir := t.TempDir()
	t.Setenv("GC_CITY", cityDir)
	writeNamedSessionCityTOML(t, cityDir)

	// A bare runtime fake implements runtime.Provider but not the tmux
	// capture capabilities, so input-area must report unsupported (exit 1).
	bare := runtime.NewFake()
	oldBuild := buildSessionProviderByName
	buildSessionProviderByName = func(string, config.SessionConfig, string, string) (runtime.Provider, error) {
		return bare, nil
	}
	t.Cleanup(func() { buildSessionProviderByName = oldBuild })

	store, err := openCityStoreAt(cityDir)
	if err != nil {
		t.Fatalf("openCityStoreAt: %v", err)
	}
	b, err := store.Create(beads.Bead{
		Title:  "unsupported session",
		Type:   session.BeadType,
		Labels: []string{session.LabelSession},
		Metadata: map[string]string{
			"session_name": "runtime-session",
			"template":     "worker",
			"state":        "awake",
			"work_dir":     cityDir,
		},
	})
	if err != nil {
		t.Fatalf("store.Create: %v", err)
	}

	var stdout, stderr bytes.Buffer
	if code := cmdSessionInputArea(b.ID, "json", false, &stdout, &stderr); code != 1 {
		t.Fatalf("unsupported provider code = %d, want 1; stderr=%s", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "not supported") {
		t.Fatalf("stderr = %q, want unsupported-provider message", stderr.String())
	}
}

func TestCmdSessionPeekRawPreservesANSI(t *testing.T) {
	sessionID, fake := newInputAreaTestSession(t)
	fake.rawOut = "❯ \x1b[2mghost\x1b[0m\n"

	var stdout, stderr bytes.Buffer
	if code := cmdSessionPeek([]string{sessionID}, 5, false, true, &stdout, &stderr); code != 0 {
		t.Fatalf("peek --raw = %d; stderr=%s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "\x1b[2m") {
		t.Fatalf("peek --raw should preserve ANSI, got %q", stdout.String())
	}
}

func TestCmdSessionPeekRawJSON(t *testing.T) {
	sessionID, fake := newInputAreaTestSession(t)
	fake.rawOut = "\x1b[2mghost\x1b[0m\n"

	var stdout, stderr bytes.Buffer
	if code := cmdSessionPeek([]string{sessionID}, 5, true, true, &stdout, &stderr); code != 0 {
		t.Fatalf("peek --raw --json = %d; stderr=%s", code, stderr.String())
	}
	var got sessionPeekJSONResult
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("stdout not JSON: %v\n%s", err, stdout.String())
	}
	if !strings.Contains(got.Output, "\x1b[2m") {
		t.Fatalf("raw JSON output should preserve ANSI, got %q", got.Output)
	}
}

func TestCmdSessionPeekRawUnsupportedProviderExits1(t *testing.T) {
	clearGCEnv(t)
	clearInheritedCityRoutingEnv(t)
	t.Setenv("GC_BEADS", "file")
	t.Setenv("GC_SESSION", "fake")
	cityDir := t.TempDir()
	t.Setenv("GC_CITY", cityDir)
	writeNamedSessionCityTOML(t, cityDir)

	bare := runtime.NewFake()
	oldBuild := buildSessionProviderByName
	buildSessionProviderByName = func(string, config.SessionConfig, string, string) (runtime.Provider, error) {
		return bare, nil
	}
	t.Cleanup(func() { buildSessionProviderByName = oldBuild })

	store, err := openCityStoreAt(cityDir)
	if err != nil {
		t.Fatalf("openCityStoreAt: %v", err)
	}
	b, err := store.Create(beads.Bead{
		Title:  "unsupported raw session",
		Type:   session.BeadType,
		Labels: []string{session.LabelSession},
		Metadata: map[string]string{
			"session_name": "runtime-session",
			"template":     "worker",
			"state":        "awake",
			"work_dir":     cityDir,
		},
	})
	if err != nil {
		t.Fatalf("store.Create: %v", err)
	}

	var stdout, stderr bytes.Buffer
	if code := cmdSessionPeek([]string{b.ID}, 5, false, true, &stdout, &stderr); code != 1 {
		t.Fatalf("peek --raw unsupported code = %d, want 1; stderr=%s", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "not supported") {
		t.Fatalf("stderr = %q, want unsupported-provider message", stderr.String())
	}
}
