# InputAreaState Spec Review: gc-ce6u81

## Verdict

needs-delta

The direction is right: moving buffered-input detection out of prompts and
into a typed, provider-aware runtime surface is the correct SDK boundary. I
would not block the convoy on the concept. I would make a short spec delta
before treating this as upstream-ready, because a few API-contract details
will otherwise leak into implementation and prompt consumers.

## Per-section comments

### 1. Problem statement

The problem statement is strong. It ties the bug to a concrete production
capture and explains why stripped pane text is the wrong abstraction for
buffered-input decisions.

One useful addition: explicitly say this is not only a Claude rendering bug.
The root bug is consumers treating terminal presentation text as semantic
input state. That framing makes the proposal easier to defend upstream.

### 2. Goals and non-goals

The goals are appropriately narrow. The API reports state; it does not decide
whether to restart, quarantine, or escalate a session.

I would add a non-goal that `InputAreaState` does not guarantee ghost-text
classification for unknown/custom providers. For `Provider="unknown"`,
`Ghost=""` means "not detected", not "no ghost text exists." Prompt consumers
need that distinction to avoid reintroducing false confidence.

### 3. Per-LLM input-area shapes

Claude is specified well, including NBSP tolerance and busy-over-prompt
precedence.

Codex needs one more fixture requirement: multi-line content inside the box,
not just stale older boxes in scrollback. Current Codex boxes can redraw and
wrap; the parser contract should say whether it only reads the prompt row or
collects continuation rows in the most recent box.

Gemini is correctly marked as needing release verification. I would keep the
Gemini parser permissive and partial until a live boxed-input capture exists.

### 4. Library API proposal

The proposed `type Provider string` name collides with the existing
`internal/runtime/tmux.Provider` adapter type. The spec should name the enum
`InputAreaProvider` or move provider-neutral types to `internal/runtime`.
The integration branch already uses `InputAreaProvider`, which is the right
shape; the spec should match it before upstream review.

The parser-purity contract also needs tightening. Section 4.3 says provider
parsers are pure functions over `(rawANSI, cfg)`, while section 4.2 says
`Detected` is set by the library. Those are compatible only if the wrapper
sets `Detected` after parsing, or if the parser accepts a clock/capture time.
Otherwise table tests get a nondeterministic timestamp from a supposedly pure
parser.

I also recommend placing the provider-neutral interface where CLI and worker
code can depend on it without importing the tmux package. The spec says
"runtime-level interface"; keep that as the contract.

### 5. CLI surface

The two-surface split is ergonomic: `peek --raw` for debugging and
`session input-area` for prompt consumers.

Define the CLI JSON as its own response shape. The example includes
`"session"`, while `InputAreaState` itself does not. That is fine, but it
should be explicit so the CLI contract is not an accidental wrapper.

For prompt consumers, JSON should be the stable contract. `kv` and `text` are
useful for humans, but they should be documented as display formats, not as
surfaces prompts should parse.

### 6. Consumer rewrite

The before/after witness example proves the agent-prompt ergonomics. One
shell-out plus `jq` is something a polecat, witness, or refinery prompt can
comfortably consume.

The example should handle command failure explicitly. A failed capture should
produce "unknown, do not conclude", not empty typed input. That is especially
important for non-tmux providers and sessions in boot/dialog states.

### 7. Test strategy

The layered test strategy is good. Pure parser tests should carry most of
the weight, with one narrow tmux integration test proving `capture-pane -e`
behavior on real tmux.

I would keep real capture fixtures as files, even if unit tests also use
small inline strings. File fixtures preserve provenance and make upstream
review of terminal bytes easier. Add cases for:

- Claude visual wrap vs actual embedded newline.
- Codex typed text that wraps inside the active box.
- Unknown provider with a configured prompt prefix and dim SGR after it,
  proving `Ghost` stays empty and consumers must treat unknown as partial.
- Raw-size cap cutting on a line boundary, or an explicit note that truncation
  can cut arbitrary bytes and parsers must tolerate partial CSI sequences.

### 8. Backward-compatibility audit

The audit covers the important existing callers and the additive migration
plan is sound.

The one compatibility note I would add is worker-boundary routing. Production
`cmd/gc` session commands should go through `worker.Handle` or an equivalent
worker-level capability, not reach around to tmux/session internals. That
keeps the current worker-boundary migration intact.

### 9. Upstream PR considerations

The upstream PR shape is mostly ready. The telemetry paragraph needs one
load-bearing caveat: if `tmux.input_area_capture` becomes a first-class event,
it must be a typed event with a registered payload. No hand-written wire maps,
and no unregistered event constants.

The schema recommendation should also say whether this is CLI-only JSON or an
HTTP/API surface. If it enters `internal/api`, OpenAPI and generated dashboard
types must be regenerated through the normal Huma path rather than hand-written
schema files.

### 10. Open questions

These are the right questions for stage 1b. I would convert a few of them
into explicit implementation requirements:

- Claude dim detection should start with SGR 2 and include only captured,
  tested alternatives such as SGR 90 or specific 256-color gray values.
  Do not broadly classify arbitrary true-color gray as ghost text.
- Codex ghost text remains "not observed" until a live capture proves
  otherwise.
- Gemini boxed input remains unresolved; the parser should stay conservative.
- Visual wrapping should not introduce newline characters into `Typed`.
  Actual input-buffer newlines, if an LLM supports them, should be represented
  as `\n`.
- `-J` is a reasonable parser default for visual wrapping, but `peek --raw`
  may still want an unjoined debug view. If both exist, document which capture
  flags produced `Raw`.
- Non-tmux providers should return a clear unsupported/partial result through
  the provider interface, not be special-cased in prompts.

## Open question answers

1. **Claude ghost wrapper**: Match SGR 2 as canonical. Add tested support for
   SGR 90 and narrow 256-color gray values only when backed by fixture bytes.
   Avoid broad color heuristics.
2. **Codex ghost text**: I have not seen current Codex ghost text. Leave
   `Ghost` empty for Codex until a capture proves a wrapper.
3. **Gemini boxed input**: Treat as unresolved. The clean-row parser is fine
   as a partial implementation, but boxed Gemini should be verified before
   claiming full support.
4. **Multi-line typed encoding**: Return the logical input buffer. Visual wraps
   should join without separators; real embedded newlines should be `\n`.
5. **`capture-pane -J`**: Use `-J` for parser input if fixtures prove it
   reduces false splits. Keep raw-debug behavior clear, because joined output
   is less faithful to what an operator saw.

## Risk callouts

- Unknown-provider results can look deceptively certain unless consumers treat
  `Provider="unknown"` as partial.
- A nondeterministic `Detected` timestamp inside pure parsers weakens parser
  tests and makes the purity claim false.
- The enum name in the spec currently collides with the existing tmux adapter
  type.
- CLI failure handling must not collapse to `typed=""`.
- Any event/API expansion must follow the typed-event and Huma/OpenAPI rules
  already enforced by CI.
