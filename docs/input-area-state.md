---
title: "InputAreaState: ghost-text-aware input-area inspection"
status: draft (stage 1 of convoy gc-8g41r)
owner: gascity/gc-toolkit pack
target_branch: integration/input-area-state
last_updated: 2026-05-09
---

# InputAreaState: ghost-text-aware input-area inspection

## TL;DR

`gc session peek` and `Tmux.CapturePane` strip ANSI escape sequences, so any
consumer that compares pane text against patterns like
`❯ <text>` cannot distinguish operator-typed characters from ghost-text
suggestions Claude Code renders dim with `ESC[2m...ESC[0m`. Every "buffered
input stall" check in agent prompts (deacon, witness, refinery) inherits
that blind spot and can fire on a UI suggestion the operator never typed.

This document proposes a typed library API — `InputAreaState(session)` —
that returns the current input-area state with per-LLM rendering knowledge
(typed text, ghost text, busy flag, prompt char), plus a CLI surface
(`gc session peek --raw` and a typed inspection command) for human and
programmatic consumers. Implementation is staged separately under the same
convoy.

## 1. Problem statement

### Empirical reproduction

On 2026-05-09 the `signal-loom/witness` session exhibited the symptom in
production:

- `gc session peek signal-loom/witness 5` returned a final line
  `❯ keep patrolling` for several minutes.
- A pattern that says *"if there is text after the prompt char that
  the agent has not consumed, it is buffered operator input"* would
  conclude the operator had typed `keep patrolling` and the agent
  ignored it.
- `tmux capture-pane -t <socket>:<sess> -p -e -S -5` revealed the line
  was actually `❯ ESC[2mkeep patrolling   ESC[0m`. Claude Code wraps
  ghost-text suggestions in dim ANSI (SGR 2 / SGR 0) so the operator
  sees a low-contrast hint after the prompt char. Nothing was typed.

Because `tmux capture-pane -p` strips SGR sequences, the plain capture and
the typed-input case are byte-for-byte identical in the consumer's view.
This is the false-positive surface area for every "is there buffered input"
check in any consumer that uses pane text.

### Why this matters

The buffered-input check is one of the few signals an external observer
has for "the operator typed something the agent has not consumed." It
shows up in:

- **witness** — escalations like "polecat is stuck and has buffered input".
- **deacon** — town-wide handoff signal when an operator wants to
  redirect a coordination agent.
- **refinery** — "polecat finished but left input in the buffer"
  resubmit handling.

A false positive on any of these is a real cross-role cost: the wrong
session gets restarted, escalated, or quarantined. The empirical
ratio is unknown but the *floor* is "every Claude Code session that
ever shows ghost text," which is most of them.

### Root cause is asymmetry

Engine state — *is the agent busy or idle?* — already lives in the
library with per-LLM awareness:

- `RuntimeTmuxConfig.ReadyPromptPrefix` (configurable per provider).
- `paneContainsBusyIndicator` (`tmux.go:2646`) — knows about Claude's
  `esc to interrupt` and Codex `Press Esc or Ctrl+C to cancel`.
- `WaitForInterruptBoundary` (`tmux.go:2521`) — Codex transcript-tail
  marker for durable interrupt acknowledgement.

But *"the operator typed something we have not processed"* is not in the
library. Each consumer prompt re-rolls a pattern match on the stripped
pane text, blind to ghost text and fragile across LLM versions. The
buffered-input check belongs alongside the busy/idle check for the same
reason those moved into the library: it requires per-LLM rendering
knowledge.

## 2. Goals and non-goals

### Goals

1. Library API the consumer can call to get a structured answer:
   `{Provider, PromptChar, Busy, Typed, Ghost, Raw, Detected}`.
2. ANSI-aware capture inside the library — consumers never see escape
   sequences unless they ask for them.
3. Per-LLM dispatch (Claude / Codex / Gemini today; pluggable for new
   providers) using the existing `GC_PROVIDER` env + process-tree
   detection already in `tmux.go`.
4. Distinguish typed text, ghost text, and "busy / not at a prompt"
   as separate fields. None of the three subsumes the others.
5. CLI surface so humans can inspect the state and so prompt
   templates can shell out to a stable JSON contract.
6. Drop-in replacement for current ad-hoc checks without breaking
   the existing approval-prompt and idle paths in `interaction.go`
   and `tmux.go`.

### Non-goals

- Reading agent intent, classifying ghost-text quality, or deciding
  whether ghost text is "good." We only report what is on screen.
- Replacing `WaitForIdle` or `WaitForRuntimeReady`. Those answer
  *"is the engine idle?"* InputAreaState answers *"what is in the
  input field right now?"* They share substrate (pane capture)
  but not semantics.
- Cross-pane / multi-window state. The session has one input area
  per pane and we look at the focused pane.
- Making `Tmux.CapturePane` ANSI-preserving by default. That would
  silently change the contract for every existing caller. The
  ANSI-aware capture is a new method; existing `CapturePane` keeps
  its current behavior.
- Implementation. This is stage 1 (spec). Stage 2 of convoy
  `gc-8g41r` is the implementation bead.

## 3. Per-LLM input-area shapes

This section is the empirical reference the library targets. Each shape
is *what we see on the wire today*; the library should model these
explicitly so a new LLM can register its own shape without consumers
changing.

### 3.1 Claude Code

- **Prompt char**: `❯ ` (U+276F + space). NBSP (U+00A0) variant exists
  and is normalized in `matchesPromptPrefix` (`tmux.go:2332`).
- **Typed input**: rendered after the prompt in default style (no
  SGR wrapper, or default-foreground SGR like `ESC[39m`).
- **Ghost text**: rendered after the prompt wrapped in `ESC[2m...ESC[0m`
  (SGR 2 = dim / faint). On wide suggestions Claude pads with trailing
  spaces inside the dim wrapper. The ghost text disappears as soon as
  the operator types one character; partial typed prefixes are not
  shown dim.
- **Busy indicator**: status-bar line containing literal substring
  `esc to interrupt` (sometimes preceded by a spinner glyph). Matched
  today in `paneContainsBusyIndicator`. While busy, the prompt char
  may still be visible in scrollback — busy beats prompt for state.
- **Approval prompt**: full-screen UI matched by
  `requiresApprovalRe` (`This command requires approval`,
  `Approve edits?`). Distinct from input area; orthogonal to this
  spec.
- **Multi-line input**: Claude wraps long buffered input into
  multiple visual rows below the prompt. The library must consider
  rows after the row containing `❯ ` until the next non-input
  visual block (status bar, divider, blank-then-status).

### 3.2 Codex

- **Prompt char**: not a single glyph; Codex shows a multi-line
  input area framed by a Unicode box (`╭─ ... ─╮ │ > │ ╰─ ... ─╯`)
  in current builds. The literal `> ` inside the frame is the
  practical "prompt char" but is wrapped in box-drawing context.
- **Typed input**: appears on the row to the right of `> ` inside
  the box.
- **Ghost text**: not observed in current Codex builds. If Codex
  introduces it, the dim wrapper will likely use the same SGR 2
  convention; the library should leave the field present but
  empty for now and verify per release.
- **Busy indicator**: `Press Esc or Ctrl+C to cancel` (matched
  today). Also a durable transcript-tail marker scanned by
  `waitForCodexInterruptBoundary` for *interrupt acknowledged*; that
  is a different signal — turn boundary, not "currently busy" — and
  stays out of `InputAreaState`.
- **Multi-line input**: Codex re-renders the entire box on each
  keystroke, so old box rows can persist in scrollback and produce
  false matches. The library must scope to the most recent box.

### 3.3 Gemini

- **Prompt char**: `> ` on a clean row, no box framing (in current
  builds; verify per release).
- **Typed input**: standard line after the prompt.
- **Ghost text**: not observed.
- **Busy indicator**: best-effort; Gemini has rewind dialog
  states in `adapter.go:geminiRewindDialogVisible` and
  `geminiRewindConfirmationVisible`. While a dialog is up, the
  pane is not at a normal prompt and `Busy=true` is the safe
  classification.
- **Approval / dialog UIs**: Gemini's rewind picker is matched
  via `Cancel rewind and stay here` / `> Rewind`. Distinct from
  input area.

### 3.4 Unknown / custom

For sessions where `GC_PROVIDER` is empty and process-tree
detection (`targetLooksLikeProvider`) does not match a known
provider, the library falls back to a generic shape: prompt char
from `RuntimeTmuxConfig.ReadyPromptPrefix` if set, no ghost-text
detection, busy indicator from the existing union match. Unknown
provider must not crash; it returns a partial `InputAreaState`
with `Provider="unknown"` and `Ghost=""`.

### 3.5 Summary table

| Provider | PromptChar | Ghost wrapper | Busy substring | Multi-line input |
|----------|-----------|---------------|----------------|------------------|
| claude   | `❯ ` (NBSP-tolerant) | `ESC[2m…ESC[0m` after prompt | `esc to interrupt` | wrap rows below prompt |
| codex    | `> ` inside `╭…╯` box | not observed | `Press Esc or Ctrl+C to cancel` | re-rendered box |
| gemini   | `> ` | not observed | rewind/confirm dialog visible | single line |
| unknown  | configured prefix or "" | not detected | union of known | best-effort |

## 4. Library API proposal

### 4.1 Surface

New file `internal/runtime/tmux/input_area.go`. Exposed through
`internal/runtime/tmux.Provider` (the `Tmux` adapter) and a new
runtime-level interface so non-tmux providers can implement it
later without code changes in consumers.

```go
// Provider identifies the LLM runtime backing a session.
type Provider string

const (
    ProviderClaude  Provider = "claude"
    ProviderCodex   Provider = "codex"
    ProviderGemini  Provider = "gemini"
    ProviderUnknown Provider = "unknown"
)

// InputAreaState reports what the agent's input area is showing
// at the moment of capture. Snapshot, not a stream.
type InputAreaState struct {
    Provider   Provider
    PromptChar string    // e.g. "❯ ", "> "; "" if no prompt visible
    Busy       bool      // engine is processing; not at a prompt
    Typed      string    // operator-typed buffered text; empty if none
    Ghost      string    // ghost-text suggestion; empty if none
    Raw        string    // ANSI-preserved capture used for parsing
    Detected   time.Time // capture time (UTC)
}

// InputAreaCapturer is implemented by runtime providers that expose
// pane-level state. Tmux is the only implementation today.
type InputAreaCapturer interface {
    InputArea(ctx context.Context, session string) (*InputAreaState, error)
}
```

### 4.2 Semantics

- `Busy=true` ⇒ `Typed` and `Ghost` may be empty even if pane
  text exists. The library does not attempt to parse an input
  field while the engine is rendering tool calls or progress
  output.
- `Typed != ""` and `Ghost != ""` are not mutually exclusive in
  the type system, but the underlying LLMs all clear ghost text
  the moment the operator types. The library returns whatever is
  on screen; consumers should treat `Typed != ""` as authoritative
  and `Ghost` as advisory.
- `PromptChar == ""` and `Busy=false` ⇒ the pane is in a
  non-prompt state (boot, dialog, error). Consumers should not
  treat empty `Typed` here as proof of "operator did not type
  anything."
- `Raw` is the ANSI-preserved capture the parser ran against. It
  exists for debugging and for consumers who need to do their own
  pattern match without re-capturing. Bounded: capped at 16 KiB
  by default (configurable via option).
- `Detected` is set by the library, not the caller — guarantees
  every consumer logs a comparable timestamp.

### 4.3 Implementation sketch

1. Capture pane with ANSI preserved via a new private method
   `capturePaneANSI(session string, lines int) (string, error)`
   that runs `tmux capture-pane -p -e -t <sess> -S -<n>`. The
   `-e` flag includes SGR sequences in output. The `-J` flag
   joins wrapped lines into single lines without the line break,
   which may help for Claude's wrapped multi-line input — the
   spec leaves the `-J` decision to implementation but recommends
   experimentation in stage 2.
2. Resolve the provider via existing helpers:
   `t.providerEnv(session)` then `t.targetLooksLikeProvider`.
3. Dispatch to a provider-specific parser
   (`parseClaudeInputArea`, `parseCodexInputArea`,
   `parseGeminiInputArea`, `parseGenericInputArea`). Each parser
   takes `(rawANSI string, cfg *RuntimeConfig) -> InputAreaState`.
4. Each parser is pure: no I/O, no `t.run`. They are
   table-testable from byte fixtures.

### 4.4 Errors

- Session not found ⇒ wrap `runtime.ErrSessionNotFound` like
  existing methods.
- tmux subprocess error ⇒ wrap with context like
  `"capturing input area for %q: %w"`.
- Parse never fails — a parser that can't make sense of pane
  text returns `InputAreaState{Provider: <p>, Raw: rawANSI,
  Detected: now}` with empty `Typed/Ghost` and `Busy=false`.

### 4.5 What this does *not* do

- It does not write to the pane.
- It does not block or poll. A waiter built on top
  (`WaitForOperatorInput`, `WaitForGhostClear`) is a separate
  follow-up if needed; this spec does not introduce it.
- It does not cache. Each call hits tmux. Consumers that poll
  should rate-limit themselves (existing `WaitForIdle` polls at
  200ms; the same interval is fine here).

## 5. CLI surface

Two changes, both additive.

### 5.1 `gc session peek --raw`

Add a flag to the existing `gc session peek` command
(`cmd_session.go:1410`) that preserves ANSI sequences in the
output. Default behavior (no flag) keeps the current
plain-stripped capture so today's piped scripts and screenshots
are unaffected.

```text
gc session peek <session> [--lines N] [--raw]
```

`--raw` calls a new method on the worker handle
(`handle.PeekRaw(ctx, lines)`) that wraps `tmux capture-pane -p -e`.
For non-tmux providers, `--raw` returns `runtime.ErrUnsupported`
with a clear message.

### 5.2 `gc session input-area`

A new command that returns a typed `InputAreaState` as JSON for
programmatic consumption.

```text
gc session input-area <session>
```

Default output:

```json
{
  "session": "signal-loom/witness",
  "provider": "claude",
  "prompt_char": "❯ ",
  "busy": false,
  "typed": "",
  "ghost": "keep patrolling",
  "detected": "2026-05-09T22:14:33Z"
}
```

Flags:

- `--include-raw` — adds `"raw"` field with the ANSI-preserved
  capture used for parsing. Off by default to keep output small
  and grep-friendly.
- `--format=json|kv|text` — JSON is default; `kv` prints
  `provider=claude busy=false typed="" ghost="keep patrolling"`
  for shell-friendly use; `text` prints a one-line human summary.

Exit codes:

- `0` — state captured successfully (regardless of `Busy` value).
- `2` — session not found.
- `1` — other errors (tmux call failed, parser crashed).

This command is the contract prompt templates consume. It is
deliberately small: one shell-out, one JSON parse, no scrollback
heuristics in the prompt.

### 5.3 Why two surfaces

`--raw` is for humans and one-off debugging. `input-area` is for
prompts and tests. Without `--raw`, an operator seeing a false
positive cannot reproduce the bug; without `input-area`, every
consumer re-rolls a parser in shell or templated Markdown.

## 6. Consumer rewrite (before / after)

This section demonstrates *consumability* — the spec's claim that
agent prompts can replace today's blind text matching with one
shell-out. The illustration uses the witness `Stuck Polecat
Detection` flow because that is where the empirical ghost-text
false positive surfaced.

### 6.1 Before — current witness pattern (gc-toolkit pack)

A representative buffered-input check a witness prompt could
write today, blind to ghost text:

```bash
# Witness: detect stuck polecat with buffered operator input
peek=$(gc session peek "$rig/$polecat" 5 | tail -n 1)
if [[ "$peek" == ❯* ]]; then
  typed="${peek#❯ }"
  if [[ -n "$typed" ]]; then
    # FALSE POSITIVE on Claude ghost text
    gc bd create --type=warrant --title="Stuck: $polecat with buffered input" \
      --metadata "{\"target\":\"$polecat\",\"reason\":\"buffered: $typed\"}" \
      --label=pool:dog
  fi
fi
```

Failure mode: `$peek` is `❯ keep patrolling`, but the bytes after
the prompt are a Claude ghost-text suggestion. The warrant fires;
the dog pool wakes; the polecat is interrogated for a stall the
operator did not cause.

### 6.2 After — InputAreaState consumer

```bash
# Witness: detect stuck polecat with buffered operator input
state=$(gc session input-area "$rig/$polecat")
typed=$(echo "$state" | jq -r '.typed')
busy=$(echo "$state"  | jq -r '.busy')

if [[ "$busy" == "false" && -n "$typed" ]]; then
  # Real buffered input — ghost text is in .ghost, not .typed
  gc bd create --type=warrant --title="Stuck: $polecat with buffered input" \
    --metadata "{\"target\":\"$polecat\",\"reason\":\"buffered: $typed\"}" \
    --label=pool:dog
fi
```

What changed:

- The shell no longer parses pane text. The library does it once,
  with provider-specific knowledge.
- Ghost text lands in `.ghost` and never triggers a warrant.
- `busy` gate covers the "agent is mid-tool-call, scrollback
  prompt is stale" case the existing prompt does not check.
- The pattern works across Claude, Codex, and Gemini without
  per-LLM branches in the prompt.

### 6.3 Why one example is enough

The deacon and refinery use the same shape — text after a prompt
char with provider awareness. A spec review can reasonably extrapolate
from one consumer rewrite to the others. Stage 2 (implementation)
will land the API; the gc-toolkit pack consumer rollout is tracked
as a separate `tk` bead post-implementation, per the convoy parent
description.

## 7. Test strategy

Three layers, in order of cost.

### 7.1 Pure parser tests (cheap, broad)

Each provider parser is a pure function over `(rawANSI, cfg)`. We
ship a fixtures directory:

```
internal/runtime/tmux/testdata/input_area/
  claude/
    idle_no_ghost.ansi
    idle_with_ghost.ansi
    idle_typed_short.ansi
    idle_typed_multiline.ansi
    busy_tool_call.ansi
    busy_with_scrollback_prompt.ansi
    approval_prompt.ansi
  codex/
    idle.ansi
    idle_typed.ansi
    busy.ansi
    box_persisted_in_scrollback.ansi
  gemini/
    idle.ansi
    rewind_picker.ansi
  unknown/
    custom_prompt.ansi
```

Fixtures are real captures from `tmux capture-pane -p -e` saved
verbatim (LF-terminated, ANSI included). Each test calls
`parseClaudeInputArea(fixture, cfg)` and asserts the full
`InputAreaState`. Adding a fixture for a regression is one file
plus one test entry.

A README in `testdata/input_area/` documents how to capture a
fresh fixture from a live session — `tmux capture-pane -t <sess>
-p -e -S -50 > fixture.ansi`.

### 7.2 Live tmux integration test (medium, narrow)

A `//go:build integration` test under
`internal/runtime/tmux/input_area_integration_test.go` that:

1. Starts a tmux pane running `bash` (no LLM dependency).
2. Sends `printf '\xe2\x9d\xaf \x1b[2mghost text\x1b[0m'` —
   prints the prompt char with a simulated dim ghost suggestion.
3. Calls `Tmux.InputArea(session)` and asserts
   `Provider==claude` (forced via `GC_PROVIDER=claude` set on
   the tmux env), `Typed==""`, `Ghost=="ghost text"`.
4. Sends `printf 'typed input'` and re-queries; asserts
   `Typed=="typed input"`.

This validates the capture-pane `-e` flag works on the host's
tmux build and that the parser handles real wire bytes. It does
not require Claude / Codex / Gemini binaries.

### 7.3 Optional smoke test against real LLMs (expensive, manual)

For each release that bumps a supported LLM, an operator can run
a short manual flow:

1. `gc session start <provider>`.
2. Type half a prompt, leave ghost text visible.
3. `gc session input-area <session>` — verify `Typed`/`Ghost`
   split.
4. Press Enter, watch agent run — verify `Busy=true`.

A short Markdown checklist under `engdocs/contributors/input-area-smoke.md`
captures the steps. Not gated by CI.

### 7.4 Negative space

We do not run end-to-end witness tests in this spec. Those live
in the gc-toolkit pack consumer rollout bead and exercise the
warrant path against simulated polecats. The library tests above
are a sufficient quality bar for stage 2 to land.

## 8. Backward-compatibility audit

Every existing `CapturePane` / `CapturePaneAll` / `CapturePaneLines`
caller was reviewed against the proposed API.

### 8.1 `internal/runtime/tmux/tmux.go`

| Caller | Line | Purpose | Affected? |
|--------|------|---------|-----------|
| `CapturePaneLines` | 2017 | helper that splits `CapturePane` output | No — same plain capture |
| Closure passed to dialog dismisser | 1542 | `func(lines int) (string, error)` for `runtime.AcceptStartupDialogs` | No — startup dialog patterns are plain text |
| `WaitForRuntimeReady` | 2378 | bootstrap prompt detection | No — ZFC-bootstrap path; plain capture is fine |
| `WaitForIdle` | 2452 | steady-state idle detection | Possibly — see 8.4 |

### 8.2 `internal/runtime/tmux/interaction.go`

| Caller | Line | Purpose | Affected? |
|--------|------|---------|-----------|
| `Pending` (approval detection) | 178 | `parseApprovalPrompt` against pane text | No — Claude approval UI uses plain text patterns (`This command requires approval`, `Approve edits?`) |
| `Respond` pre-verify | 232 | re-check approval before sending key | No — same as above |
| `Respond` post-verify | 282 | confirm prompt cleared | No — same as above |

### 8.3 `internal/runtime/tmux/adapter.go`

| Caller | Line | Purpose | Affected? |
|--------|------|---------|-----------|
| `gemini rewind confirmation` | 315 | match `No code changes to revert.` | No — plain text |
| `Provider.Peek` (CLI peek path) | 436, 438 | user-facing `gc session peek` | New path: `--raw` adds an ANSI-preserved capture; default behavior unchanged |
| `waitForPane` generic matcher | 474 | gemini dialog wait | No — plain text matchers |

### 8.4 The one place worth double-checking — `WaitForIdle`

`WaitForIdle` (`tmux.go:2437`) decides "the agent is idle" based
on `paneContainsBusyIndicator` (clear) plus a prompt-char match
in plain capture. This is correct today because the prompt char
itself is not rendered dim — only the ghost text after it is. So
plain-stripped capture and ANSI capture both contain `❯ ` at the
same row, and the idle path is unaffected.

If a future LLM renders the prompt char itself in dim ANSI, the
plain-stripped capture would still see the glyph (SGR strip is
non-destructive to the underlying chars), so this remains safe.
We note this for the spec review — it is the only place where
the "plain capture is good enough" assumption is load-bearing
beyond approval detection.

### 8.5 Migration plan

- Phase 1 (stage 2 implementation): add new methods, do not
  change existing methods. All existing callers continue to use
  plain `CapturePane`.
- Phase 2 (gc-toolkit pack rollout, separate bead): consumer
  prompts switch from ad-hoc `gc session peek` parsing to
  `gc session input-area`.
- Phase 3 (optional, future): if `WaitForIdle` ever needs ghost
  awareness, switch its capture to the ANSI variant. Not part of
  this convoy.

No rename or deprecation of `CapturePane` is proposed. The plain
strip is the right default for the cases it serves today.

## 9. Upstream PR considerations

### 9.1 Audience

Every Gas City deployment using `gc session peek` for
buffered-input detection has the same blind spot. The fix lives
in `internal/runtime/tmux/`, which is upstreamable to
`gastownhall/gascity`.

### 9.2 PR shape

One PR per stage, gated on convoy graduation:

- **Stage 2 PR (library + CLI):** new `input_area.go`, the
  `--raw` flag, the `gc session input-area` command, parser
  fixtures, integration test. Self-contained, no consumer
  changes.
- **Stage 3+ PRs (code reviews + graduation):** synthesis of
  reviewer feedback, then squash-merge of
  `integration/input-area-state` into `main`.
- **Consumer rollout PR (gc-toolkit pack):** lives in the `tk`
  repo, follows the gascity merge. Out of scope for upstream
  gascity.

### 9.3 Risk to upstream consumers

- Forks that already have a custom `CapturePane` extension will
  get a new method on the same type. No method-rename, no
  signature changes.
- Forks that depend on `gc session peek` plain-text shape are
  unaffected — `--raw` is opt-in.
- Forks that have their own buffered-input detection in agent
  prompts get a strictly better surface to migrate to.

### 9.4 Versioning

The `InputAreaState` struct is a wire contract once the CLI
returns it as JSON. Adding fields is safe (consumers `jq` for
known keys); removing or renaming is breaking. The spec
recommends marking the JSON schema in `docs/schema/` once
implementation lands so downstream consumers can pin.

### 9.5 Telemetry

Optional but cheap: emit an event
(`tmux.input_area_capture`) on each call with provider and
detected fields. Lets us measure ghost-text incidence in the
field and validate the model holds across LLM versions. Hidden
behind `GC_INPUT_AREA_TRACE=1` so default deployments stay
quiet.

## 10. Open questions for stage 1b review

1. **Ghost-text wrapper across Claude versions.** Current evidence
   is one Claude Code build (`signal-loom/witness`, 2026-05-09).
   Do older or newer builds use different SGR codes (e.g.
   `ESC[38;5;240m` for a specific dim color instead of `ESC[2m`)?
   If yes, the parser should match a small union of dim-style
   sequences, not just `ESC[2m`.
2. **Codex ghost text.** Confirm whether any current Codex build
   renders ghost text. If it does, the Codex parser needs
   pattern coverage; if not, leave the field empty and revisit
   per release.
3. **Gemini boxed input.** Some Gemini builds box the prompt like
   Codex. Verify per release before assuming `> ` on a clean row.
4. **Multi-line typed input.** When operator typing wraps to a
   second visual row, does the parser join with a literal newline
   in `Typed`, or with a space? Both are defensible; the spec
   defaults to literal newline so consumers can detect multi-line
   intent.
5. **`tmux capture-pane -J`.** Does joining wrapped lines remove
   the multi-line ambiguity in question 4? Stage 2 should
   prototype both with and without `-J` and pick.
6. **Non-tmux providers.** When the SDK adds a non-tmux runtime
   provider (e.g. exec / k8s), how does `InputArea` map? The
   spec leaves the interface in place but expects only tmux to
   implement it for now.

## Appendix A: ANSI primer

For reviewers without recent terminal-rendering context.

- ANSI escape sequences (CSI, "Control Sequence Introducer")
  start with `ESC [` (`0x1b 0x5b`) and end with a letter that
  names the operation. SGR (`Select Graphic Rendition`) ends in
  `m` and sets text style.
- `ESC[2m` = dim / faint. `ESC[0m` = reset all attributes.
  Claude Code wraps ghost text in this pair.
- `tmux capture-pane -p` runs the pane through tmux's
  text-stripping pipeline by default. The `-e` flag
  re-introduces SGR sequences; `-J` joins wrapped lines.
- Real terminals interpret SGR; dumb consumers (grep, scripts)
  see escape bytes and need to strip or pattern-match around them.

## Appendix B: Capture-pane flag reference (tmux 3.x)

| Flag | Effect | Used today | Used in spec |
|------|--------|------------|--------------|
| `-p` | Print to stdout | yes (all callers) | yes |
| `-e` | Include SGR escape sequences | no | yes (new ANSI capture) |
| `-J` | Preserve trailing spaces, join wrapped lines | no | candidate (stage 2) |
| `-S -<n>` | Start n lines back | yes | yes |
| `-S -` | Start from beginning of scrollback | yes (`CapturePaneAll`) | yes |
| `-N` | Preserve trailing whitespace | no | candidate |

## Appendix C: Empirical capture from 2026-05-09

Bytes captured from `signal-loom/witness` reproducing the false
positive (ANSI shown as `ESC` for readability):

```
[chrome rows omitted]
❯ ESC[2mkeep patrolling                                     ESC[0m
[status row]
```

Plain-strip view (today's `CapturePane`):

```
❯ keep patrolling
[status row]
```

A consumer matching `^❯ (.+)$` on the plain view extracts
`keep patrolling` and treats it as buffered operator input. The
ANSI view shows the SGR 2 wrapper and the spec's parser would
classify it as `Ghost="keep patrolling"`, `Typed=""`.
