# Spec review: InputAreaState design (gc-9yoqyg)

| Field | Value |
|---|---|
| Spec | `engdocs/design/input-area-state.md` |
| Reviewer | Claude polecat `gc-toolkit.furiosa` |
| Date | 2026-05-28 |
| Parent | gc-8g41r.1 |
| Convoy | gc-8g41r |

## Verdict

**needs-delta** — accept the core design and proceed to stage 2; tighten
seven items in the spec (or in stage 2's design notes) before
implementation lands. None of the deltas change the API shape; they
close ambiguities that would otherwise be relitigated during
implementation review.

The motivating analysis (empirical capture + per-LLM survey + consumer
rewrite) is solid. The proposed API is the smallest thing that solves
the false-positive problem and gives prompts a stable contract. The
test strategy is the right shape and right cost ordering.

## Headline deltas (the seven items)

D1. **Match a union of dim-style SGRs, not just `ESC[2m`.** Single
sample point. Cheap defense.

D2. **Specify `Raw` truncation semantics** when capture exceeds the
16 KiB cap (keep newest = bottom of pane).

D3. **JSON contract: empty string, never `null`** for `typed`, `ghost`,
`prompt_char`. Shell consumers using `jq -r` will see the string
`"null"` and treat it as a positive match.

D4. **Disambiguate `PromptChar=""` + `Busy=false`** across boot,
post-bootstrap idle without prompt, dialog, and parse-empty. Today
they collapse to one state and a consumer cannot tell them apart.

D5. **Specify provider-resolution order** when env says `claude` and
the process tree says `codex`.

D6. **Codex "most recent box" detection** needs an algorithm sketch.
Last `╭` to nearest `╯` after it? What about partial re-renders
mid-keystroke?

D7. **Add fuzz to the test layer.** Pure parsers over arbitrary ANSI
bytes are the textbook fuzz target.

Each is expanded under Per-section comments and Open question answers
below.

## Per-section comments

### Section 1 (Problem statement) — accept

The empirical capture (Appendix C echoes it) makes the case
unambiguous: plain-strip and ANSI-aware capture are byte-identical to
the consumer. The list of affected consumers (witness / deacon /
refinery) correctly identifies where the false positive matters. No
delta.

Minor: "the empirical ratio is unknown but the *floor* is 'every
Claude Code session that ever shows ghost text,' which is most of
them" understates it slightly. Ghost text fires every time the input
field is empty and the model has a recent suggestion in cache, which
is the steady-state for idle witness/deacon/refinery polling. The
floor is closer to "every quiescent Claude session that isn't in
approval or busy state." Worth strengthening for stage 2 PR copy.

### Section 2 (Goals / non-goals) — accept

Goals 1–6 are tight. The non-goals are well chosen — explicitly
declining to read agent intent or replace `WaitForIdle` keeps the
scope honest. The decision to leave `CapturePane` plain by default
(rather than flipping its contract) is the right call; flipping it
would force a Phase 1 audit of every existing caller for SGR
tolerance.

### Section 3 (Per-LLM shapes) — needs-delta

Section is the empirical reference the library depends on; that means
every assumption here is load-bearing.

**3.1 Claude.** Ghost wrapper `ESC[2m...ESC[0m` from a single sample
point (Appendix C, 2026-05-09). This is the place to be paranoid: see
[D1](#d1-dim-sgr-union) and [Q1](#q1-ghost-text-wrapper-across-claude-versions).

**3.1 Approval prompt callout.** "Distinct from input area; orthogonal
to this spec." Defensible scope decision but creates a fifth state for
consumers (prompt / busy / approval / dialog / parse-empty). See
[D4](#d4-disambiguate-promptchar--busyfalse).

**3.2 Codex.** "The library must scope to the most recent box." Stated
as a requirement but no algorithm. See [D6](#d6-codex-most-recent-box).
The mid-keystroke partial re-render case is the failure mode worth
designing for.

**3.3 Gemini.** Polecat-gemini blocked (tk-mmny1) so I cannot verify
empirically. Author's stated approach (rewind/confirm dialogs ⇒
`Busy=true`) is the safe classification. Accept on author's authority.

**3.4 Unknown / custom.** Good fallback. One nit: "no ghost-text
detection" for unknown providers is the right default, but the spec
could allow a runtime config knob (e.g. `RuntimeTmuxConfig.GhostTextSGRs
[]string`) so an operator running a fork with a known dim wrapper can
opt in without code changes. Small, optional.

**3.5 Summary table.** Useful. Add a "Notes" column for the per-row
caveats (single sample point for Claude ghost; not verified for Codex
ghost; etc.) so a future reviewer doesn't have to re-read the prose.

### Section 4 (Library API) — accept with two notes

**4.1 Surface.** `InputAreaCapturer` interface is good — it puts the
contract at the runtime layer, not the tmux layer, so a future
non-tmux provider can implement it. The `Provider` enum is closed at
compile time (string const block); that's the right call for the
SDK's internal use. A pack that wants to add `ProviderCustom` should
file a bead, not register at runtime.

Nit on field naming: `Detected` is fine but `CapturedAt` is more
idiomatic Go (matches `time.Time`-suffix-`At` convention used
elsewhere in `internal/runtime/`). Bikesheddable.

**4.2 Semantics.** Three observations.

- "`Typed != ""` and `Ghost != ""` are not mutually exclusive in the
  type system, but the underlying LLMs all clear ghost text the moment
  the operator types." Strong claim; worth a fixture that asserts it
  for each LLM (`idle_typed_clears_ghost.ansi`).
- "`PromptChar == ""` and `Busy=false` ⇒ the pane is in a non-prompt
  state (boot, dialog, error)." See [D4](#d4-disambiguate-promptchar--busyfalse).
- Parse never fails. Good — robust contract. But the consumer cannot
  distinguish "parser ran and the input is empty" from "parser ran
  and produced nothing useful" from "parser bailed because input was
  garbled." Consider adding a `ParseConfidence` enum
  (`high`/`medium`/`low`) or a `ParseNotes []string` field for
  debugging. Optional; not blocking.

**4.3 Implementation sketch.** Step 1 mentions `-J` and defers the
decision to stage 2. See [Q5](#q5-tmux-capture-pane--j-vs-unjoined).
Important: when `-J` joins wrapped lines, `-S -<n>` semantics shift —
n logical lines vs n visual lines. Stage 2 should pin which.

**4.4 Errors.** Good error wrapping. Specify `runtime.ErrSessionNotFound`
maps to CLI exit code 2 (matches section 5.2). Currently those are
coordinated only by author convention — pin them in one place.

**4.5 What this does *not* do.** Accept. The cache decision (no
caching, each call hits tmux) is right; a 200ms poll matches existing
`WaitForIdle` cost.

### Section 5 (CLI surface) — accept with one delta

**5.1 `gc session peek --raw`.** Clean opt-in. The "non-tmux providers
return `runtime.ErrUnsupported`" branch is the right behavior.

**5.2 `gc session input-area`.** API ergonomics for prompt templates
are good. Three notes:

- **D3 (json contract).** The JSON example shows `"typed": ""` for
  empty typed input. Pin this in the contract: empty fields are empty
  strings, never `null`. The reason is consumer-side: a polecat or
  witness shelling out via `jq -r '.typed'` gets the literal string
  `"null"` if a field is null, and `[[ -n "null" ]]` is true. Warrant
  fires on a missing field. Same risk for `prompt_char` and `ghost`.
- **--format=kv escaping.** The example output
  `provider=claude busy=false typed="" ghost="keep patrolling"` is
  shell-friendly until `typed` contains `"`, `\`, a newline, or a
  literal `=`. Multi-line typed input (Q4 — newline-joined) breaks
  the kv format immediately. Recommend: drop `kv`, keep `json` and
  `text`, and document `jq -r` as the prompt-template idiom.
  Alternatively, define kv as POSIX-shell-safe quoting (use
  `printf '%q'` semantics).
- **--include-raw default.** Off is right for grep-friendliness. Add
  a one-line operator hint in the `text` format output: *"rerun with
  `--include-raw` to see the underlying ANSI capture."* Removes one
  iteration for incident response.

**5.3 Why two surfaces.** Accept.

### Section 6 (Consumer rewrite) — accept

The before/after demonstrates consumability. One trip wire to call out
explicitly in the spec (so consumer authors don't re-discover it):

```bash
state=$(gc session input-area "$rig/$polecat")
typed=$(echo "$state" | jq -r '.typed')
busy=$(echo "$state"  | jq -r '.busy')
```

Two `jq` calls on the same JSON is fine but invites a third when
adding `.ghost` or `.prompt_char` checks. A single `jq -r '@sh
"typed=\(.typed) busy=\(.busy) ghost=\(.ghost)"'` (or `jq --raw-output0`
+ `eval`) collapses the calls and quotes safely. Worth a paragraph in
section 6 (or a `engdocs/contributors/input-area-recipes.md` followup)
so each consumer gets the same idiom rather than re-rolling.

The example also doesn't show what happens when `input-area` exits
non-zero. A defensive prompt would `set -e` and handle the exit. The
spec should say: "if `gc session input-area` exits non-zero, treat
the result as unknown — do not fire warrants on missing state."

### Section 7 (Test strategy) — needs-delta (D7)

7.1 fixtures, 7.2 integration test, 7.3 manual smoke, 7.4 negative
space — all the right layers. Add:

- **7.5 Fuzz the pure parsers.** `parseClaudeInputArea` and friends
  take arbitrary ANSI bytes; fuzz them. Cheap, catches the "tmux
  output contains an unterminated CSI" class of bugs that
  Appendix C's "ESC for readability" shorthand could otherwise hide.
  `go test -fuzz=.` on each parser, seed corpus = the fixtures in
  7.1. Acceptance: zero parser panics, no allocations >16 KiB.
- **7.1 negative fixtures.** Add `claude/sgr_unterminated.ansi`
  (`ESC[2m...` with no `ESC[0m`), `codex/box_mid_render.ansi`,
  `gemini/dialog_no_clear.ansi`. These pin the "parse-but-don't-crash"
  contract.
- **7.2 integration: assert the 16 KiB cap.** Pipe a 100 KiB SGR
  payload into the pane and verify `Raw` is bounded and parse still
  returns reasonable values (D2).

### Section 8 (Backward-compat audit) — accept

Tables are thorough. Section 8.4 (`WaitForIdle`) correctly identifies
the one assumption worth double-checking and explains why plain
capture remains safe. The "future LLM renders prompt char dim ⇒ SGR
strip is non-destructive to underlying glyph" argument is right and
worth keeping.

One additional caller worth a row in 8.2: any test helper that uses
`CapturePane` to assert pane content (e.g. integration tests under
`test/`). If those are stable on plain capture they don't need
migration; just confirm no `t.Helper()` somewhere greps for SGR bytes
and silently passes.

### Section 9 (Upstream PR) — accept

Stage-gated PR shape (library → reviews → graduation → consumer
rollout) is clean. Three notes:

- **9.4 Versioning.** "Adding fields is safe (consumers `jq` for
  known keys); removing or renaming is breaking." True for `jq`
  consumers but typed Go consumers via the SDK get breakage on
  rename. Mark the struct with a stability annotation
  (`// Stability: experimental until v0.X` or similar) in stage 2 so
  downstream forks know.
- **9.5 Telemetry.** Good cheap idea. Gate on
  `GC_INPUT_AREA_TRACE=1` is fine. Make sure the event payload
  registers with `events.RegisterPayload` per the typed-events
  invariant in `AGENTS.md` — the event constant has to live in
  `events.KnownEventTypes` and have a non-`NoPayload` sample if it
  carries fields. Trivial but enforced by CI
  (`TestEveryKnownEventTypeHasRegisteredPayload`).
- **9.2 PR shape — affected-tests gate.** With pure parsers + fixtures,
  the affected-tests path will be tight (parser tests + integration
  test only). Good for fast CI.

### Section 10 (Open questions) — see answers below

## Open question answers

### Q1: Ghost-text wrapper across Claude versions

**My view: yes, match a union.** The current evidence is one Claude
Code build. Across the ANSI ecosystem, "dim" gets rendered in at
least three ways:

- `ESC[2m` (SGR 2 = dim/faint, then `ESC[0m` or `ESC[22m` to reset)
- `ESC[38;5;N` with N a low-contrast 8-bit color (240–244 are
  conventional greys)
- `ESC[38;2;R;G;B` with low-contrast truecolor (e.g. matching the
  terminal background tinted up)
- `ESC[90m` (bright black = grey)

Recommend: stage 2 implements a small `isDimSGR(seq string) bool`
helper with `ESC[2m` and `ESC[90m` as the defaults, plus a
configurable allow-list per provider (`RuntimeTmuxConfig.GhostSGRs`).
Adding a new pattern when Claude bumps becomes a fixture + one entry,
not a parser rewrite. Cost: ~50 lines.

### Q2: Codex ghost text

**My view: leave the field empty, ship it, revisit per release.**
The spec's plan is right. If/when Codex ships ghost text, the union
matcher from Q1 likely catches it on first capture (Codex's
underlying terminal renderer almost certainly uses SGR 2 or SGR 90).
Add a Codex-specific fixture at that time.

### Q3: Gemini boxed input

**Defer.** Cannot verify (polecat-gemini blocked per tk-mmny1).
Author's stated approach is reasonable; verify on a per-release
basis. Suggest: stage 2 adds a `gemini/idle_boxed.ansi` fixture *if
and when* a Gemini build with boxed input ships, behind a `t.Skip()`
guard until then. Don't speculate the format up front.

### Q4: Multi-line typed input encoding

**My view: literal newline, as the spec proposes.** Two reasons:

1. Newline preserves operator intent (multi-line vs single-line is
   semantically distinct for many consumers — e.g. a witness deciding
   whether to escalate vs nudge).
2. Lossy → lossless conversion is impossible; lossless → lossy is
   trivial (`tr '\n' ' '` or `jq -r '.typed | gsub("\n";" ")'`).

Document in the CLI help that consumers wanting flat single-line
output should pipe through `jq`'s `gsub`. The reverse — a consumer
that needs newlines from a space-joined `Typed` — is unrecoverable.

### Q5: tmux capture-pane -J vs unjoined

**My view: use `-J` for the parser path, not for the `Raw` field.**
Two captures may be the right answer:

- `-p -e -J -S -<n>` for parsing (joined, ANSI preserved): gives the
  parser a single logical line per input row.
- `-p -e -S -<n>` (or no second capture, just the joined one) for
  `Raw`.

If two captures are too costly, capture once with `-J -e` and accept
that `Raw` may contain joined lines (it's for debugging, not for
re-parsing by consumers).

The trickier issue is `-S -<n>` semantics under `-J`. Stage 2 should
test:

```bash
tmux capture-pane -t <s> -p -e    -S -10 | wc -l   # visual lines
tmux capture-pane -t <s> -p -e -J -S -10 | wc -l   # joined lines
```

and document which the parser asks for. My guess (verify): `-J`
counts logical (post-join) lines, so asking for `-S -5` may return
fewer rows of scrollback than expected. Probably means: ask for more
scrollback (`-S -50`) and let the parser scope down. Cheap.

### Q6: Non-tmux providers

**Accept the spec's stance.** Interface lives in `runtime/`; only
tmux implements it today; non-tmux providers return
`runtime.ErrUnsupported` from `gc session peek --raw` and from
`gc session input-area`. The exec/k8s/subprocess providers don't have
a pane to capture; the concept doesn't map.

When/if a non-tmux interactive provider arrives (e.g. an SSH-backed
terminal), the interface accommodates it without changes.

## Risk callouts (consumer-side trip wires)

These are not blocking — they're the things stage 2's
`engdocs/contributors/input-area-recipes.md` (or equivalent) should
warn consumer authors about. Each has been a real bug in similar
detection code I've seen in this codebase or analogues.

**R1. `jq -r` and `null`.** The JSON contract delta (D3) is the fix;
the trip wire deserves a callout in consumer docs. Sample:

```bash
# WRONG — fires on null too
[[ -n "$(jq -r '.typed' <<< "$state")" ]] && warrant

# RIGHT — explicit null guard
typed=$(jq -r '.typed // ""' <<< "$state")
[[ -n "$typed" ]] && warrant
```

**R2. State staleness between capture and act.** `InputAreaState` is
a snapshot. Consumers should re-capture before acting on a state
older than the polling interval, especially across a `gc bd` round
trip. Warrant-creation consumers should be idempotent (existing
warrant for the same `(target, reason)` ⇒ no duplicate).

**R3. Approval-state blind spot.** Section 3.1 explicitly punts on
approval prompts. Consumers asking "is the polecat stuck?" should
also check the approval surface (`gc session pending-approval` or
equivalent) before firing a stuck warrant. Otherwise a polecat
parked on an approval looks identical to "idle at prompt, no typed
input" via `InputAreaState`.

**R4. Bootstrap race.** During engine startup, `Busy=false`,
`PromptChar=""`, `Typed=""`, `Ghost=""` — same shape as steady-state
idle on a non-prompt screen. Consumers polling at session start
should gate on `WaitForRuntimeReady` first (existing primitive) and
only then trust `InputAreaState` output. Worth a sentence in section
4.5 or section 6.

**R5. Approval/dialog `Busy=true` semantics.** Gemini's rewind
dialog ⇒ `Busy=true` (per section 3.3) means a polecat parked on a
rewind dialog reports busy. A "is this polecat making progress"
heuristic that polls `Busy=true` over time will look like a stall
without ever reading the dialog content. Stage 2's recipes should
document this — `Busy=true` means "not at prompt," not "actively
processing."

**R6. CLI exit code on tmux subprocess failure.** Section 5.2 says
exit 1 for "other errors (tmux call failed, parser crashed)." A
consumer polling on a tmux server restart will see exit 1 transiently
and could fire warrants on the noise. Recommend: stage 2's CLI
retries the tmux call once on subprocess failure (existing pattern
in `tmux.go`), surfaces a distinct exit code (3?) for "transient
runtime error," and consumer recipes treat exits 1 and 3 as
"unknown — don't act."

## Delta detail (the seven items, with implementation pointers)

### D1: Dim-SGR union

Section 3.1: "wrapped in `ESC[2m...ESC[0m`" → "wrapped in a
dim-style SGR pair (commonly `ESC[2m...ESC[0m` or `ESC[90m...ESC[0m`;
parser matches a configurable union)."

### D2: `Raw` truncation semantics

Section 4.2: "`Raw` is the ANSI-preserved capture..." → append: "If
the capture exceeds the cap (default 16 KiB), `Raw` is truncated
from the **start** so the bottom of the pane (the current input
area) is preserved. The first preserved byte starts at the next
newline boundary."

### D3: JSON contract — empty string, not null

Section 5.2: add a paragraph before "Flags": "All string fields in
the JSON output are empty strings (`""`) when absent, never `null`.
This is enforced by the `omitempty`-free JSON encoder path and is a
stable contract for shell consumers."

### D4: Disambiguate `PromptChar=""` + `Busy=false`

Section 4.2: refactor the "Semantics" list to enumerate states:

| State | `Busy` | `PromptChar` | `Typed`/`Ghost` | Meaning |
|---|---|---|---|---|
| busy | true | maybe-set | empty | engine processing |
| at-prompt | false | set | possibly set | normal input field |
| approval | false | empty | empty | approval UI (out of scope; see other API) |
| dialog | false | empty | empty | provider-specific dialog (Gemini rewind, etc.) |
| boot | false | empty | empty | session starting; gate on `WaitForRuntimeReady` |
| parse-unknown | false | empty | empty | parser found no recognizable shape |

The last four collapse in the current spec. Either add a `Phase`
field (`busy`/`prompt`/`approval`/`dialog`/`boot`/`unknown`) or
document that consumers should disambiguate via existing primitives
(`WaitForRuntimeReady` for boot, `pending-approval` for approval).

### D5: Provider resolution order

Section 4.3 step 2: append: "`providerEnv` wins; `targetLooksLikeProvider`
is the fallback. Mismatch is logged at debug level but env wins —
this lets operators override process-tree detection for fork builds
or wrappers."

### D6: Codex "most recent box" detection

Section 3.2: append: "The library finds the most recent box by
scanning from the bottom of the capture for a `╯` (bottom-right
corner), then scanning upward to the nearest unmatched `╭`
(top-left). Partial re-renders mid-keystroke produce
top-without-matching-bottom; the parser treats those as in-flight
and returns `Busy=false`, `Typed=""`, `Ghost=""`, `PromptChar="> "`,
which the consumer disambiguates via re-poll."

### D7: Fuzz the parsers

Section 7: add subsection 7.5 (text above under section 7
review).

## What I am explicitly *not* asking for

- A registry for providers. Compile-time enum is right for the SDK's
  internal scope.
- A streaming API. Snapshot-only is the right shape; a waiter on top
  is a future bead.
- Caching. 200ms poll cadence ⇒ no win, complicates invalidation.
- Renaming `Detected` → `CapturedAt`. Nice-to-have only.
- A `Phase` enum (D4 could be solved either way). Documenting the
  state table is enough.

## Bottom line

Approve the spec with the seven deltas folded in (or addressed in
stage 2's design notes). The library API is the right abstraction;
the test strategy is the right cost ordering; the CLI surface is the
right size for prompt-template consumers. The risk callouts above are
for the consumer rollout bead, not for stage 2. Stage 2 should land
this without an architectural redesign.
