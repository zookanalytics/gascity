package hooks

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/gastownhall/gascity/internal/fsys"
	"github.com/gastownhall/gascity/internal/shellquote"
)

// decodeCodexLaunchHooks parses the -c value CodexLaunchArgs returns the way
// Codex does, as TOML, and returns its hooks table normalized through JSON.
func decodeCodexLaunchHooks(t *testing.T, args []string) map[string]any {
	t.Helper()
	if len(args) != 2 || args[0] != "-c" || !strings.HasPrefix(args[1], "hooks=") {
		t.Fatalf("CodexLaunchArgs = %q, want [-c hooks=<table>]", args)
	}
	var parsed map[string]any
	if _, err := toml.Decode(args[1], &parsed); err != nil {
		t.Fatalf("launch override does not parse as TOML: %v\n%s", err, args[1])
	}
	return jsonRoundTrip(t, parsed["hooks"])
}

func jsonRoundTrip(t *testing.T, v any) map[string]any {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return out
}

func TestCodexLaunchArgsRegistersTheManagedHooksDocument(t *testing.T) {
	const city = "/city with space"
	args, err := CodexLaunchArgs(city)
	if err != nil {
		t.Fatalf("CodexLaunchArgs: %v", err)
	}
	got := decodeCodexLaunchHooks(t, args)
	state, ok := got["state"].(map[string]any)
	if !ok {
		t.Fatalf("launch override has no state table: %v", got)
	}
	delete(got, "state")

	doc, err := managedCodexHooksDoc(city)
	if err != nil {
		t.Fatalf("managedCodexHooksDoc: %v", err)
	}
	want := jsonRoundTrip(t, doc.Hooks)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("launch override hooks differ from the managed document:\ngot:  %v\nwant: %v", got, want)
	}

	handlers := 0
	for event, groups := range doc.Hooks {
		for gi, group := range groups {
			for hi, handler := range group.Hooks {
				handlers++
				key := fmt.Sprintf("/<session-flags>/config.toml:%s:%d:%d", codexHookEventKeyLabels[event], gi, hi)
				entry, ok := state[key].(map[string]any)
				if !ok {
					t.Fatalf("state has no entry for %s (%s %q); state = %v", key, event, handler.Command, state)
				}
				wantHash, err := codexHookTrustHash(event, group.Matcher, handler)
				if err != nil {
					t.Fatalf("codexHookTrustHash: %v", err)
				}
				if entry["trusted_hash"] != wantHash {
					t.Errorf("state[%s].trusted_hash = %v, want %s", key, entry["trusted_hash"], wantHash)
				}
			}
		}
	}
	if len(state) != handlers {
		t.Errorf("state has %d entries for %d handlers: %v", len(state), handlers, state)
	}
}

func TestCodexLaunchArgsCarryTheCityBoundManagedCommands(t *testing.T) {
	doc, err := managedCodexHooksDoc("/city")
	if err != nil {
		t.Fatalf("managedCodexHooksDoc: %v", err)
	}
	commands := func(event string) []string {
		var out []string
		for _, group := range doc.Hooks[event] {
			for _, handler := range group.Hooks {
				out = append(out, handler.Command)
			}
		}
		return out
	}

	sessionStart := commands("SessionStart")
	if len(sessionStart) != 1 {
		t.Fatalf("SessionStart commands = %q, want one", sessionStart)
	}
	for _, want := range []string{
		`"${GC_BIN:-gc}" --city '/city' prime --hook --hook-format codex`,
		"GC_HOOK_EVENT_NAME=SessionStart",
		"GC_MANAGED_SESSION_HOOK=1",
	} {
		if !strings.Contains(sessionStart[0], want) {
			t.Errorf("SessionStart command %q missing %q", sessionStart[0], want)
		}
	}
	if m := doc.Hooks["SessionStart"][0].Matcher; m == nil || *m != "startup" {
		t.Errorf("SessionStart matcher = %v, want startup", m)
	}
	preCompact := commands("PreCompact")
	if len(preCompact) != 1 || !strings.Contains(preCompact[0], `"${GC_BIN:-gc}" --city '/city' handoff --auto --hook-format codex "context cycle"`) {
		t.Errorf("PreCompact commands = %q, want the city-bound auto handoff", preCompact)
	}
	prompt := strings.Join(commands("UserPromptSubmit"), "\n")
	for _, want := range []string{
		`"${GC_BIN:-gc}" --city '/city' hook run --timeout 15s --timeout-exit-code 0 -- nudge drain --inject --hook-format codex`,
		`"${GC_BIN:-gc}" --city '/city' hook run --timeout 15s --timeout-exit-code 0 -- mail check --inject --hook-format codex`,
	} {
		if !strings.Contains(prompt, want) {
			t.Errorf("UserPromptSubmit commands missing bounded command %q:\n%s", want, prompt)
		}
	}
}

// TestCodexLaunchArgsCommandsAreRecognizedAsManaged keeps the launch
// registration and StripManagedCodexHooks in step: a copy of any registered
// hook in a hooks file is one the strip removes.
func TestCodexLaunchArgsCommandsAreRecognizedAsManaged(t *testing.T) {
	for _, city := range []string{"", "/city", "/city with 'quotes'"} {
		doc, err := managedCodexHooksDoc(city)
		if err != nil {
			t.Fatalf("managedCodexHooksDoc(%q): %v", city, err)
		}
		for event, groups := range doc.Hooks {
			for _, group := range groups {
				for _, handler := range group.Hooks {
					if !codexHookCommandLooksManaged(event, handler.Command) {
						t.Errorf("city %q: %s command %q is not recognized as managed", city, event, handler.Command)
					}
				}
			}
		}
	}
}

// TestCodexHookTrustHashMatchesCodex pins the trust hash to the values
// codex-cli 0.160.1 reported as currentHash through app-server hooks/list for
// the same hooks declared in a -c override.
func TestCodexHookTrustHashMatchesCodex(t *testing.T) {
	str := func(s string) *string { return &s }
	u64 := func(v uint64) *uint64 { return &v }
	const pathPrefix = `export PATH="$PATH:$HOME/go/bin:$HOME/.local/bin" && `
	cases := []struct {
		name    string
		event   string
		matcher *string
		handler codexHookHandler
		want    string
	}{
		{
			name:    "session start keeps its matcher",
			event:   "SessionStart",
			matcher: str("startup"),
			handler: codexHookHandler{Type: "command", Command: "echo hi"},
			want:    "sha256:96fce91e330a75049046e8e6e6ee74528be2d90871e21bafdff0b41859318628",
		},
		{
			name:    "managed session start",
			event:   "SessionStart",
			matcher: str("startup"),
			handler: codexHookHandler{Type: "command", Command: pathPrefix + `GC_MANAGED_SESSION_HOOK=1 GC_HOOK_EVENT_NAME=SessionStart "${GC_BIN:-gc}" --city '/Users/gc/Code/loomington' prime --hook --hook-format codex`},
			want:    "sha256:e79271fc8cb559ab41ece0ac95b5e1fd79fc9b4e1d6af2a41af8359937ae6b16",
		},
		{
			name:    "pre compact keeps an empty matcher",
			event:   "PreCompact",
			matcher: str(""),
			handler: codexHookHandler{Type: "command", Command: pathPrefix + `"${GC_BIN:-gc}" --city '/Users/gc/Code/loomington' handoff --auto --hook-format codex "context cycle"`},
			want:    "sha256:c6171ec900feb7367bf88622f14428ff6ddd7d957aacd57d64ac7d6dc8c6c35b",
		},
		{
			name:    "prompt submit drops its matcher",
			event:   "UserPromptSubmit",
			matcher: str(""),
			handler: codexHookHandler{Type: "command", Command: pathPrefix + `"${GC_BIN:-gc}" --city '/Users/gc/Code/loomington' hook run --timeout 15s --timeout-exit-code 0 -- nudge drain --inject --hook-format codex`},
			want:    "sha256:7402b928ac531cbc25d071800d0208a3eae9d6181e1da2db7580715eed888721",
		},
		{
			name:    "escaped characters and a zero timeout",
			event:   "PreCompact",
			handler: codexHookHandler{Type: "command", Command: `printf "a\tb" & echo 'q'`, Timeout: u64(0)},
			want:    "sha256:2e6090ef6a09b337aa65cae84547cd0633255180272bbfd0da10fc403e5d8812",
		},
		{
			name:    "session end defaults its timeout",
			event:   "SessionEnd",
			matcher: str("x"),
			handler: codexHookHandler{Type: "command", Command: "echo end"},
			want:    "sha256:8dfb2239e5830fb88c9cad683b89d07ae0164c7fa0ce75767a3f84acf89c3dc2",
		},
		{
			name:    "session end clamps its timeout",
			event:   "SessionEnd",
			matcher: str("x"),
			handler: codexHookHandler{Type: "command", Command: "echo end2", Timeout: u64(10)},
			want:    "sha256:64679ddc6add2d0f35ceffe4f324fe627ac34cf320156c19782af57b24be6bde",
		},
		{
			name:    "stop drops its matcher and keeps async and a status message",
			event:   "Stop",
			matcher: str("ignored"),
			handler: codexHookHandler{Type: "command", Command: "echo stop", Timeout: u64(5), Async: true, StatusMessage: str("Stopping")},
			want:    "sha256:faee4da1a615e806c39ff2899154c846eb60d5434c282d10da94f23550bc65f5",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := codexHookTrustHash(tc.event, tc.matcher, tc.handler)
			if err != nil {
				t.Fatalf("codexHookTrustHash: %v", err)
			}
			if got != tc.want {
				t.Fatalf("codexHookTrustHash = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestCodexHookTrustHashRejectsUnknownEvent(t *testing.T) {
	if _, err := codexHookTrustHash("OnLaunch", nil, codexHookHandler{Type: "command", Command: "true"}); err == nil {
		t.Fatal("codexHookTrustHash accepted an event Codex does not have")
	}
}

func TestSessionConfigTOMLRoundTripsEveryHandlerField(t *testing.T) {
	str := func(s string) *string { return &s }
	u64 := func(v uint64) *uint64 { return &v }
	command := "printf \"q\" \\ 'x' $HOME \t tab \n line \x01 ctl \x7f del é"
	doc := codexHooksDoc{Hooks: map[string][]codexHookGroup{
		"Stop": {{
			Matcher: str("m\"x"),
			Hooks: []codexHookHandler{{
				Type: "command", Command: command, Timeout: u64(7), Async: true, StatusMessage: str("s\\m"),
			}},
		}},
	}}
	value, err := doc.sessionConfigTOML()
	if err != nil {
		t.Fatalf("sessionConfigTOML: %v", err)
	}
	got := decodeCodexLaunchHooks(t, []string{"-c", "hooks=" + value})
	delete(got, "state")
	want := jsonRoundTrip(t, doc.Hooks)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("TOML round trip changed the document:\ngot:  %v\nwant: %v", got, want)
	}
}

func TestSessionConfigTOMLRejectsNonCommandHandlers(t *testing.T) {
	doc := codexHooksDoc{Hooks: map[string][]codexHookGroup{
		"Stop": {{Hooks: []codexHookHandler{{Type: "prompt"}}}},
	}}
	if _, err := doc.sessionConfigTOML(); err == nil {
		t.Fatal("sessionConfigTOML rendered a handler type it cannot hash")
	}
}

// managedCodexHookGenerations are Codex hooks files Gas City staged into work
// directories: the raw overlay asset, older unbound and pack-merged forms
// observed on long-running cities, and the city-bound form Install wrote.
func managedCodexHookGenerations(t *testing.T) map[string][]byte {
	t.Helper()
	const p = `export PATH=\"$HOME/go/bin:$HOME/.local/bin:$PATH\" && `
	raw, err := readEmbedded(codexManagedHooksAsset)
	if err != nil {
		t.Fatalf("reading managed asset: %v", err)
	}
	bound, err := managedCodexHooksDoc("/city")
	if err != nil {
		t.Fatalf("managedCodexHooksDoc: %v", err)
	}
	boundData, err := json.Marshal(bound)
	if err != nil {
		t.Fatalf("marshal bound doc: %v", err)
	}
	return map[string][]byte{
		"raw-overlay-asset": raw,
		"city-bound":        boundData,
		"unbound-core-asset": []byte(`{"hooks":{"SessionStart":[{"matcher":"","hooks":[{"type":"command","command":"` + p + `GC_MANAGED_SESSION_HOOK=1 GC_HOOK_EVENT_NAME=SessionStart gc prime --hook --hook-format codex"}]}],` +
			`"PreCompact":[{"matcher":"","hooks":[{"type":"command","command":"` + p + `gc handoff --auto --hook-format codex \"context cycle\""}]}],` +
			`"UserPromptSubmit":[{"matcher":"","hooks":[{"type":"command","command":"` + p + `gc nudge drain --inject --hook-format codex"},{"type":"command","command":"` + p + `gc mail check --inject --hook-format codex"}]}]}}`),
		"pack-overlay-merged": []byte(`{"hooks":{"SessionStart":[{"matcher":"startup","hooks":[{"type":"command","command":"` + p + `GC_MANAGED_SESSION_HOOK=1 GC_HOOK_EVENT_NAME=SessionStart gc prime --hook --hook-format codex"}]},` +
			`{"matcher":"","hooks":[{"type":"command","command":"` + p + `GC_MANAGED_SESSION_HOOK=1 GC_HOOK_EVENT_NAME=SessionStart gc prime --hook --hook-format codex"}]}],` +
			`"PreCompact":[{"matcher":"","hooks":[{"type":"command","command":"` + p + `gc handoff --auto \"context cycle\""}]}],` +
			`"UserPromptSubmit":[{"matcher":"","hooks":[{"type":"command","command":"` + p + `gc nudge drain --inject --hook-format codex"},{"type":"command","command":"` + p + `gc mail check --inject --hook-format codex"}]}]}}`),
		"stale-city-binding": []byte(`{"hooks":{"SessionStart":[{"hooks":[{"type":"command","command":"` + p + `GC_MANAGED_SESSION_HOOK=1 GC_HOOK_EVENT_NAME=SessionStart gc --city /old/city prime --hook --hook-format codex"}]}],` +
			`"PreCompact":[{"hooks":[{"type":"command","command":"` + p + `gc --city /old/city handoff --auto --hook-format codex \"context cycle\""}]}]}}`),
		"extra-env-on-managed-hook": []byte(`{"hooks":{"SessionStart":[{"hooks":[{"type":"command","command":"` + p + `FOO=1 GC_MANAGED_SESSION_HOOK=1 GC_HOOK_EVENT_NAME=SessionStart gc prime --hook --hook-format codex"}]}]}}`),
	}
}

func TestStripManagedCodexHooksRemovesEveryManagedGeneration(t *testing.T) {
	for name, data := range managedCodexHookGenerations(t) {
		t.Run(name, func(t *testing.T) {
			if !CodexHooksHaveManagedEntries(data) {
				t.Fatalf("CodexHooksHaveManagedEntries = false for a managed file:\n%s", data)
			}
			fs := fsys.NewFake()
			fs.Files["/work/.codex/hooks.json"] = append([]byte(nil), data...)
			if err := StripManagedCodexHooks(fs, "/work"); err != nil {
				t.Fatalf("StripManagedCodexHooks: %v", err)
			}
			if got, ok := fs.Files["/work/.codex/hooks.json"]; ok {
				t.Fatalf("a hooks file holding only managed hooks was kept:\n%s", got)
			}
		})
	}
}

func TestStripManagedCodexHooksKeepsEveryUnmanagedEntry(t *testing.T) {
	const p = `export PATH=\"$HOME/go/bin:$HOME/.local/bin:$PATH\" && `
	fs := fsys.NewFake()
	fs.Files["/work/.codex/hooks.json"] = []byte(`{
  "description": "operator hooks",
  "hooks": {
    "SessionStart": [{"matcher": "startup", "hooks": [
      {"type": "command", "command": "` + p + `GC_MANAGED_SESSION_HOOK=1 GC_HOOK_EVENT_NAME=SessionStart gc prime --hook --hook-format codex"},
      {"type": "command", "command": "printf custom-start"}
    ]}],
    "PreToolUse": [{"matcher": "Bash", "hooks": [{"type": "command", "command": "/opt/guard.sh"}]}],
    "UserPromptSubmit": [
      {"hooks": [{"type": "command", "command": "` + p + `gc nudge drain --inject --hook-format codex"}]},
      {"hooks": [{"type": "command", "command": "FOO=1 gc mail check --inject --hook-format codex"}]}
    ],
    "PreCompact": [{"hooks": [{"type": "command", "command": "` + p + `gc handoff --auto --hook-format codex \"context cycle\""}]}]
  }
}`)

	if err := StripManagedCodexHooks(fs, "/work"); err != nil {
		t.Fatalf("StripManagedCodexHooks: %v", err)
	}
	got := fs.Files["/work/.codex/hooks.json"]
	var doc struct {
		Description string                      `json:"description"`
		Hooks       map[string][]codexHookGroup `json:"hooks"`
	}
	if err := json.Unmarshal(got, &doc); err != nil {
		t.Fatalf("stripped file is not JSON: %v\n%s", err, got)
	}
	if doc.Description != "operator hooks" {
		t.Errorf("description = %q, want it kept", doc.Description)
	}
	commands := map[string][]string{}
	for event, groups := range doc.Hooks {
		for _, group := range groups {
			for _, handler := range group.Hooks {
				commands[event] = append(commands[event], handler.Command)
			}
		}
	}
	want := map[string][]string{
		"SessionStart":     {"printf custom-start"},
		"PreToolUse":       {"/opt/guard.sh"},
		"UserPromptSubmit": {"FOO=1 gc mail check --inject --hook-format codex"},
	}
	if !reflect.DeepEqual(commands, want) {
		t.Fatalf("hooks left after the strip = %v, want %v\n%s", commands, want, got)
	}
	if m := doc.Hooks["SessionStart"][0].Matcher; m == nil || *m != "startup" {
		t.Errorf("SessionStart matcher = %v, want the group's startup matcher kept", m)
	}
	if CodexHooksHaveManagedEntries(got) {
		t.Fatalf("stripped file still reports a managed entry:\n%s", got)
	}

	if err := StripManagedCodexHooks(fs, "/work"); err != nil {
		t.Fatalf("second StripManagedCodexHooks: %v", err)
	}
	if again := fs.Files["/work/.codex/hooks.json"]; !bytes.Equal(again, got) {
		t.Fatalf("a second strip rewrote the file:\nbefore:\n%s\nafter:\n%s", got, again)
	}
}

func TestStripManagedCodexHooksLeavesFilesItCannotRead(t *testing.T) {
	fs := fsys.NewFake()
	if err := StripManagedCodexHooks(fs, "/work"); err != nil {
		t.Fatalf("StripManagedCodexHooks(missing): %v", err)
	}
	if len(fs.Files) != 0 {
		t.Fatalf("a strip with no hooks file wrote %v", fs.Files)
	}
	if err := StripManagedCodexHooks(fs, " "); err != nil {
		t.Fatalf("StripManagedCodexHooks(blank workDir): %v", err)
	}

	for _, data := range []string{
		`{not json`,
		`{"hooks": []}`,
		`{"hooks": {"SessionStart": "gc prime --hook"}}`,
		`{"hooks": {"UserPromptSubmit": [{"hooks": [{"type": "command", "command": "printf custom"}]}]}}`,
	} {
		fs.Files["/work/.codex/hooks.json"] = []byte(data)
		if CodexHooksHaveManagedEntries([]byte(data)) {
			t.Errorf("CodexHooksHaveManagedEntries = true for %s", data)
		}
		if err := StripManagedCodexHooks(fs, "/work"); err != nil {
			t.Fatalf("StripManagedCodexHooks(%s): %v", data, err)
		}
		if got := string(fs.Files["/work/.codex/hooks.json"]); got != data {
			t.Errorf("a file with no managed entry was rewritten:\nbefore: %s\nafter:  %s", data, got)
		}
	}
}

func TestInstallCodexStripsManagedHooksAndWritesNothing(t *testing.T) {
	fs := fsys.NewFake()
	if err := Install(fs, "/city", "/work", []string{"codex"}); err != nil {
		t.Fatalf("Install: %v", err)
	}
	if len(fs.Files) != 0 {
		t.Fatalf("Install for Codex wrote %v; Codex takes its managed hooks from its launch command", fs.Files)
	}

	fs.Files["/work/.codex/hooks.json"] = managedCodexHookGenerations(t)["raw-overlay-asset"]
	if err := Install(fs, "/city", "/work", []string{"codex"}); err != nil {
		t.Fatalf("Install: %v", err)
	}
	if got, ok := fs.Files["/work/.codex/hooks.json"]; ok {
		t.Fatalf("Install for Codex kept a staged managed hooks file:\n%s", got)
	}
}

func TestCodexLaunchArgsQuoteThroughTheShell(t *testing.T) {
	args, err := CodexLaunchArgs("/city with 'quotes'")
	if err != nil {
		t.Fatalf("CodexLaunchArgs: %v", err)
	}
	if got := shellquote.Split("codex " + shellquote.Join(args)); !reflect.DeepEqual(got[1:], args) {
		t.Fatalf("launch args did not survive shell quoting:\ngot:  %q\nwant: %q", got[1:], args)
	}
}
