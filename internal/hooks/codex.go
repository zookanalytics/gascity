package hooks

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/gastownhall/gascity/internal/fsys"
	"github.com/gastownhall/gascity/internal/overlay"
)

// codexManagedHooksAsset is the embedded Codex hooks document holding Gas
// City's managed hooks, before they are bound to a city.
const codexManagedHooksAsset = "config/codex.json"

// codexSessionFlagsHookSource is the source Codex records for a hook declared
// in a -c/--config override, and so the first part of that hook's trust key.
// It mirrors config_toml_source_path for ConfigLayerSource::SessionFlags in
// codex-rs/hooks/src/engine/discovery.rs.
const codexSessionFlagsHookSource = "/<session-flags>/config.toml"

// codexHookEventKeyLabels maps a Codex hook event to the label Codex uses for
// it in a trust key and in a hook's trust identity (hook_event_key_label in
// codex-rs/hooks/src/lib.rs).
var codexHookEventKeyLabels = map[string]string{
	"PreToolUse":        "pre_tool_use",
	"PermissionRequest": "permission_request",
	"PostToolUse":       "post_tool_use",
	"PreCompact":        "pre_compact",
	"PostCompact":       "post_compact",
	"SessionStart":      "session_start",
	"SessionEnd":        "session_end",
	"UserPromptSubmit":  "user_prompt_submit",
	"SubagentStart":     "subagent_start",
	"SubagentStop":      "subagent_stop",
	"Stop":              "stop",
	"Interrupt":         "interrupt",
}

// codexHooksDoc is a Codex hooks document restricted to the command handlers
// Gas City registers.
type codexHooksDoc struct {
	Hooks map[string][]codexHookGroup `json:"hooks"`
}

type codexHookGroup struct {
	Matcher *string            `json:"matcher,omitempty"`
	Hooks   []codexHookHandler `json:"hooks"`
}

type codexHookHandler struct {
	Type          string  `json:"type"`
	Command       string  `json:"command"`
	Timeout       *uint64 `json:"timeout,omitempty"`
	Async         bool    `json:"async,omitempty"`
	StatusMessage *string `json:"statusMessage,omitempty"`
}

// CodexLaunchArgs returns the codex command-line arguments that register Gas
// City's managed Codex hooks for one session, bound to cityDir, and mark each
// of them trusted.
//
// Codex loads the hooks of a -c override in every working directory. A
// .codex/hooks.json file in the session's directory does not reach that far:
// in a linked git worktree Codex reads project hooks only from the main
// checkout. The override also carries each hook's trusted hash, so Codex runs
// the hooks without asking for a review first.
func CodexLaunchArgs(cityDir string) ([]string, error) {
	doc, err := managedCodexHooksDoc(cityDir)
	if err != nil {
		return nil, err
	}
	value, err := doc.sessionConfigTOML()
	if err != nil {
		return nil, fmt.Errorf("rendering %s: %w", codexManagedHooksAsset, err)
	}
	return []string{"-c", "hooks=" + value}, nil
}

// managedCodexHooksDoc returns the managed Codex hooks document bound to
// cityDir, in the form StripManagedCodexHooks recognizes as managed.
func managedCodexHooksDoc(cityDir string) (codexHooksDoc, error) {
	data, err := readEmbedded(codexManagedHooksAsset)
	if err != nil {
		return codexHooksDoc{}, err
	}
	normalized, _, err := normalizeCodexHookCommands(data, cityDir)
	if err != nil {
		return codexHooksDoc{}, fmt.Errorf("binding %s to %s: %w", codexManagedHooksAsset, cityDir, err)
	}
	var doc codexHooksDoc
	dec := json.NewDecoder(bytes.NewReader(normalized))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&doc); err != nil {
		return codexHooksDoc{}, fmt.Errorf("decoding %s: %w", codexManagedHooksAsset, err)
	}
	return doc, nil
}

// sessionConfigTOML renders d as the inline TOML table that a -c hooks=
// override takes, with a state entry that marks every handler trusted.
func (d codexHooksDoc) sessionConfigTOML() (string, error) {
	events := make([]string, 0, len(d.Hooks))
	for event := range d.Hooks {
		events = append(events, event)
	}
	sort.Strings(events)
	entries := make([]string, 0, len(events)+1)
	var state []string
	for _, event := range events {
		label, ok := codexHookEventKeyLabels[event]
		if !ok {
			return "", fmt.Errorf("unknown Codex hook event %q", event)
		}
		groups := make([]string, 0, len(d.Hooks[event]))
		for gi, group := range d.Hooks[event] {
			handlers := make([]string, 0, len(group.Hooks))
			for hi, handler := range group.Hooks {
				if handler.Type != "command" {
					return "", fmt.Errorf("%s hook %d.%d: unsupported handler type %q", event, gi, hi, handler.Type)
				}
				hash, err := codexHookTrustHash(event, group.Matcher, handler)
				if err != nil {
					return "", fmt.Errorf("%s hook %d.%d: %w", event, gi, hi, err)
				}
				handlers = append(handlers, handler.inlineTOML())
				key := fmt.Sprintf("%s:%s:%d:%d", codexSessionFlagsHookSource, label, gi, hi)
				state = append(state, tomlBasicString(key)+" = {trusted_hash = "+tomlBasicString(hash)+"}")
			}
			fields := make([]string, 0, 2)
			if group.Matcher != nil {
				fields = append(fields, "matcher = "+tomlBasicString(*group.Matcher))
			}
			fields = append(fields, "hooks = ["+strings.Join(handlers, ", ")+"]")
			groups = append(groups, "{"+strings.Join(fields, ", ")+"}")
		}
		entries = append(entries, event+" = ["+strings.Join(groups, ", ")+"]")
	}
	entries = append(entries, "state = {"+strings.Join(state, ", ")+"}")
	return "{" + strings.Join(entries, ", ") + "}", nil
}

func (h codexHookHandler) inlineTOML() string {
	fields := []string{
		"type = " + tomlBasicString(h.Type),
		"command = " + tomlBasicString(h.Command),
	}
	if h.Timeout != nil {
		fields = append(fields, "timeout = "+strconv.FormatUint(*h.Timeout, 10))
	}
	if h.Async {
		fields = append(fields, "async = true")
	}
	if h.StatusMessage != nil {
		fields = append(fields, "statusMessage = "+tomlBasicString(*h.StatusMessage))
	}
	return "{" + strings.Join(fields, ", ") + "}"
}

// codexHookTrustHash returns the hash Codex compares against a hook's
// trusted_hash: SHA-256 over the canonical JSON of the handler's normalized
// identity (hook_hash in codex-rs/hooks/src/engine/discovery.rs and
// version_for_toml in codex-rs/config/src/fingerprint.rs). The identity names
// the event, the matcher Codex applies for that event, and the handler with
// its timeout and async flag filled in.
func codexHookTrustHash(event string, matcher *string, h codexHookHandler) (string, error) {
	label, ok := codexHookEventKeyLabels[event]
	if !ok {
		return "", fmt.Errorf("unknown Codex hook event %q", event)
	}
	handler := map[string]any{
		"type":    h.Type,
		"command": h.Command,
		"timeout": codexHookTimeout(event, h.Timeout),
		"async":   h.Async,
	}
	if h.StatusMessage != nil {
		handler["statusMessage"] = *h.StatusMessage
	}
	identity := map[string]any{
		"event_name": label,
		"hooks":      []any{handler},
	}
	if m := codexMatcherForEvent(event, matcher); m != nil {
		identity["matcher"] = *m
	}
	var buf bytes.Buffer
	if err := writeSerdeCanonicalJSON(&buf, identity); err != nil {
		return "", err
	}
	sum := sha256.Sum256(buf.Bytes())
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

// codexHookTimeout returns the timeout Codex applies to a command hook
// (normalize_command_hook in codex-rs/hooks/src/engine/discovery.rs).
func codexHookTimeout(event string, timeout *uint64) uint64 {
	switch event {
	case "SessionEnd", "Interrupt":
		t := uint64(1)
		if timeout != nil {
			t = *timeout
		}
		return min(max(t, 1), 3)
	default:
		if timeout == nil {
			return 600
		}
		return max(*timeout, 1)
	}
}

// codexMatcherForEvent returns the matcher Codex applies for event: none for
// the events that take no matcher (matcher_pattern_for_event in
// codex-rs/hooks/src/events/common.rs).
func codexMatcherForEvent(event string, matcher *string) *string {
	switch event {
	case "UserPromptSubmit", "Stop", "Interrupt":
		return nil
	default:
		return matcher
	}
}

// writeSerdeCanonicalJSON writes v the way serde_json::to_vec writes a value
// whose object keys are sorted: compact, keys in byte order, and only quotes,
// backslashes, and control characters escaped.
func writeSerdeCanonicalJSON(buf *bytes.Buffer, v any) error {
	switch val := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(val))
		for k := range val {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		buf.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				buf.WriteByte(',')
			}
			if err := writeSerdeJSONString(buf, k); err != nil {
				return err
			}
			buf.WriteByte(':')
			if err := writeSerdeCanonicalJSON(buf, val[k]); err != nil {
				return err
			}
		}
		buf.WriteByte('}')
	case []any:
		buf.WriteByte('[')
		for i, item := range val {
			if i > 0 {
				buf.WriteByte(',')
			}
			if err := writeSerdeCanonicalJSON(buf, item); err != nil {
				return err
			}
		}
		buf.WriteByte(']')
	case string:
		return writeSerdeJSONString(buf, val)
	case bool:
		buf.WriteString(strconv.FormatBool(val))
	case uint64:
		buf.WriteString(strconv.FormatUint(val, 10))
	default:
		return fmt.Errorf("canonical JSON: unsupported value %T", v)
	}
	return nil
}

func writeSerdeJSONString(buf *bytes.Buffer, s string) error {
	if !utf8.ValidString(s) {
		return fmt.Errorf("canonical JSON: invalid UTF-8 in %q", s)
	}
	buf.WriteByte('"')
	for i := 0; i < len(s); i++ {
		switch c := s[i]; c {
		case '"':
			buf.WriteString(`\"`)
		case '\\':
			buf.WriteString(`\\`)
		case '\b':
			buf.WriteString(`\b`)
		case '\f':
			buf.WriteString(`\f`)
		case '\n':
			buf.WriteString(`\n`)
		case '\r':
			buf.WriteString(`\r`)
		case '\t':
			buf.WriteString(`\t`)
		default:
			if c < 0x20 {
				fmt.Fprintf(buf, `\u%04x`, c)
			} else {
				buf.WriteByte(c)
			}
		}
	}
	buf.WriteByte('"')
	return nil
}

// tomlBasicString quotes s as a TOML basic string.
func tomlBasicString(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\b':
			b.WriteString(`\b`)
		case '\f':
			b.WriteString(`\f`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		default:
			if r < 0x20 || r == 0x7f {
				fmt.Fprintf(&b, `\u%04X`, r)
			} else {
				b.WriteRune(r)
			}
		}
	}
	b.WriteByte('"')
	return b.String()
}

// StripManagedCodexHooks removes Gas City's managed hook entries from the
// Codex hooks file in workDir. CodexLaunchArgs registers those hooks on every
// managed Codex session's launch command, so a copy in a hooks file that Codex
// also reads runs each of them a second time. Entries Gas City does not manage
// stay as they are, and a file left holding nothing is removed. A missing,
// unreadable, or malformed file is left untouched, since none of them is a
// file this call can show holds a managed entry.
func StripManagedCodexHooks(fs fsys.FS, workDir string) error {
	if strings.TrimSpace(workDir) == "" {
		return nil
	}
	dst := filepath.Join(workDir, ".codex", "hooks.json")
	data, err := fs.ReadFile(dst)
	if err != nil {
		return nil
	}
	stripped, changed, empty := stripManagedCodexHookEntries(data)
	if !changed {
		return nil
	}
	if empty {
		if err := fs.Remove(dst); err != nil {
			return fmt.Errorf("removing %s: %w", dst, err)
		}
		return nil
	}
	return writeManagedData(fs, dst, stripped)
}

// CodexHooksHaveManagedEntries reports whether data is a Codex hooks document
// holding a Gas City managed hook entry, one StripManagedCodexHooks removes.
func CodexHooksHaveManagedEntries(data []byte) bool {
	_, changed, _ := stripManagedCodexHookEntries(data)
	return changed
}

// stripManagedCodexHookEntries removes every managed command handler from a
// Codex hooks document, then each matcher group and event left with no
// handlers. It reports whether anything was removed and whether the document
// left holds nothing. A document it cannot parse is reported unchanged.
func stripManagedCodexHookEntries(data []byte) ([]byte, bool, bool) {
	var doc map[string]any
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, false, false
	}
	events, ok := doc["hooks"].(map[string]any)
	if !ok {
		return nil, false, false
	}
	changed := false
	for event, value := range events {
		groups, ok := value.([]any)
		if !ok {
			continue
		}
		kept, removed := stripManagedCodexHookGroups(event, groups)
		if !removed {
			continue
		}
		changed = true
		if len(kept) == 0 {
			delete(events, event)
		} else {
			events[event] = kept
		}
	}
	if !changed {
		return nil, false, false
	}
	if len(events) == 0 && len(doc) == 1 {
		return nil, true, true
	}
	out, err := overlay.MarshalCanonicalJSON(doc)
	if err != nil {
		return nil, false, false
	}
	return out, true, false
}

// stripManagedCodexHookGroups drops the managed handlers from one event's
// matcher groups and reports whether it dropped any.
func stripManagedCodexHookGroups(event string, groups []any) ([]any, bool) {
	kept := make([]any, 0, len(groups))
	removed := false
	for _, groupValue := range groups {
		group, ok := groupValue.(map[string]any)
		if !ok {
			kept = append(kept, groupValue)
			continue
		}
		handlers, ok := group["hooks"].([]any)
		if !ok {
			kept = append(kept, groupValue)
			continue
		}
		keptHandlers := make([]any, 0, len(handlers))
		for _, handler := range handlers {
			if !codexHandlerIsManaged(event, handler) {
				keptHandlers = append(keptHandlers, handler)
			}
		}
		if len(keptHandlers) == len(handlers) {
			kept = append(kept, groupValue)
			continue
		}
		removed = true
		if len(keptHandlers) > 0 {
			group["hooks"] = keptHandlers
			kept = append(kept, group)
		}
	}
	return kept, removed
}

func codexHandlerIsManaged(event string, handlerValue any) bool {
	handler, ok := handlerValue.(map[string]any)
	if !ok {
		return false
	}
	command, ok := handler["command"].(string)
	return ok && codexHookCommandLooksManaged(event, command)
}
