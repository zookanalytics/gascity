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

// fakeInputAreaProvider embeds the runtime fake and adds the tmux InputArea
// capture capability so CLI tests can exercise "gc session input-area" without
// a live tmux server.
type fakeInputAreaProvider struct {
	*runtime.Fake
	state    *tmux.InputAreaState
	stateErr error
}

func (f *fakeInputAreaProvider) InputArea(_ context.Context, _ string) (*tmux.InputAreaState, error) {
	return f.state, f.stateErr
}

// newInputAreaTestSession wires a fake provider with the InputArea capability
// into the CLI and creates an awake session bead pointing at "runtime-session".
// Returns the bead id and the fake so tests can set the capture result.
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
	buildSessionProviderByName = func(*config.City, string, config.SessionConfig, string, string) (runtime.Provider, error) {
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
	}

	var stdout, stderr bytes.Buffer
	if code := cmdSessionInputArea(sessionID, "json", &stdout, &stderr); code != 0 {
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
		if code := cmdSessionInputArea(sessionID, format, &stdout, &stderr); code != 0 {
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
	if code := cmdSessionInputArea(sessionID, "yaml", &stdout, &stderr); code != 1 {
		t.Fatalf("invalid --format code = %d, want 1", code)
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want empty on invalid format", stdout.String())
	}
}

func TestCmdSessionInputAreaUnknownSessionExits2(t *testing.T) {
	newInputAreaTestSession(t)
	var stdout, stderr bytes.Buffer
	if code := cmdSessionInputArea("no-such-session", "json", &stdout, &stderr); code != 2 {
		t.Fatalf("unknown session code = %d, want 2; stderr=%s", code, stderr.String())
	}
}

func TestCmdSessionInputAreaCaptureNotFoundExits2(t *testing.T) {
	sessionID, fake := newInputAreaTestSession(t)
	fake.stateErr = runtime.ErrSessionNotFound

	var stdout, stderr bytes.Buffer
	if code := cmdSessionInputArea(sessionID, "json", &stdout, &stderr); code != 2 {
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
	// InputArea capability, so input-area must report unsupported (exit 1).
	bare := runtime.NewFake()
	oldBuild := buildSessionProviderByName
	buildSessionProviderByName = func(*config.City, string, config.SessionConfig, string, string) (runtime.Provider, error) {
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
	if code := cmdSessionInputArea(b.ID, "json", &stdout, &stderr); code != 1 {
		t.Fatalf("unsupported provider code = %d, want 1; stderr=%s", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "not supported") {
		t.Fatalf("stderr = %q, want unsupported-provider message", stderr.String())
	}
}
