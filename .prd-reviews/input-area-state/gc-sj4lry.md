# Spec review: InputAreaState (gc-sj4lry, Claude polecat #1)

**Spec:** `engdocs/design/input-area-state.md` @ `integration/input-area-state`
**Convoy:** gc-8g41r — Library-level buffered-input detection (ghost-text aware)
**Reviewer:** gascity/gc-toolkit.nux (Claude polecat)

## Verdict

**needs-delta** — accept the API shape and staging plan as written; merge
after a short editorial pass that addresses the deltas in §"Required
deltas" below. None of the deltas change the API surface or invalidate
the audit work in §8; they harden details that today read as
"implementer's discretion" but are load-bearing for consumers.

The spec is unusually well-grounded for a stage-1 deliverable: a real
empirical capture (Appendix C), a comprehensive `CapturePane` caller
audit (§8), a three-stage PR plan (§9), and a parser test layout that
takes the fixtures-from-real-captures discipline seriously. Stage 2 can
start the day the deltas land.

## Required deltas (must address before stage 2)

1. **Ghost-text wrapper: match a small SGR union, not just `ESC[2m`.**
   §3.1 grounds ghost detection in one capture from 2026-05-09. Claude
   Code builds across the last year have rendered ghost text with at
   least three dim-style SGRs in the wild: `ESC[2m` (SGR 2 faint),
   `ESC[90m` (bright black foreground), and 8-bit `ESC[38;5;<n>m` for
   `n` in the gray range (roughly 232–245). A parser keyed to `ESC[2m`
   only will under-detect when a future build switches conventions, and
   the failure mode is silent — `Typed` populates with what should be
   ghost text and the consumer fires a false-positive warrant, which is
   exactly what this spec exists to prevent. Update §3.1 and §4.3 to
   say: parser matches any SGR sequence whose semantics are "dim
   foreground" (SGR 2; SGR 38;5;n for n∈[232,245]; SGR 90), wrapped by
   any reset (`ESC[0m` or `ESC[39m` returning to default fg). Keep the
   match table extensible — a `dimSGRPattern` regex constant in the
   parser is the simplest shape.

2. **Default `-J` (join wrapped lines) in the ANSI capture.**
   §10 open question 5 leaves this to stage 2. Recommend pinning the
   answer in the spec: **default `-J` on.** Rationale: the
   multi-line-input ambiguity in §10 question 4 dissolves if the
   capture joins visually wrapped rows back into one logical line.
   Without `-J`, the parser has to reconstruct line continuation by
   guessing, which re-introduces exactly the kind of heuristic the spec
   is trying to eliminate from consumer prompts. The cost of `-J` is
   one extra flag; the benefit is "the typed string is the typed
   string." Encoded answer to §10.4 falls out: literal newline survives
   `-J` only when the operator pressed Enter, so multi-line intent
   stays detectable.

3. **Specify "most-recent prompt row" requirement explicitly.**
   §3.1 says "busy beats prompt for state" when scrollback contains an
   old prompt char. But the parser still has to pick a row when
   `Busy=false` and the capture window includes two prompt rows (e.g.
   the last completed turn's prompt plus the current input row). The
   spec is silent on this. Add to §4.3: "Parsers scan the captured
   region bottom-up and bind to the first prompt-char row found.
   Earlier prompt rows in scrollback are ignored." Otherwise different
   implementations will diverge and consumers will see flapping
   `Typed`/`Ghost` values for the same pane state.

4. **Define behavior during approval prompts.**
   §8.2 audits approval-prompt callers and concludes they're
   unaffected. Good. But `InputAreaState` itself needs a defined return
   value when an approval prompt is up — the pane is neither busy in
   the `paneContainsBusyIndicator` sense nor at a normal input row. Add
   to §4.2: "When `requiresApprovalRe` matches the capture region,
   `InputAreaState` returns `Provider=<p>`, `PromptChar=""`,
   `Busy=false`, `Typed=""`, `Ghost=""`. Consumers needing approval
   state already use `Pending()`/`Respond()`; InputAreaState does not
   re-expose it." Without this, a witness consumer asking "is the
   operator stuck with buffered input?" while an approval dialog is up
   gets ambiguous output.

5. **Tie the optional telemetry event to the typed-events contract.**
   §9.5 proposes `tmux.input_area_capture` behind `GC_INPUT_AREA_TRACE=1`.
   The project's load-bearing CI invariant
   (`TestEveryKnownEventTypeHasRegisteredPayload`) requires every event
   constant in `events.KnownEventTypes` to register a typed payload via
   `events.RegisterPayload`. Add a line to §9.5: "If implemented, the
   event constant must register a typed payload (struct containing at
   minimum `Provider`, `Busy`, `HasTyped bool`, `HasGhost bool`,
   `Detected`). `events.NoPayload` is not appropriate — the value of
   the telemetry is in the fields." This is a one-line addition that
   prevents an avoidable CI break in stage 2.

## Recommended deltas (worth landing but not blocking)

6. **Pin the 2026-05-09 capture as the canonical Claude ghost fixture.**
   Appendix C reproduces the exact bytes that motivated the spec. The
   fixture set in §7.1 lists `claude/idle_with_ghost.ansi` but doesn't
   say it should match Appendix C verbatim. Make this explicit:
   `claude/idle_with_ghost.ansi` is the Appendix C capture, used as the
   regression for the original bug. Future Claude builds that change
   ghost rendering get a new fixture (`idle_with_ghost_v2.ansi`); the
   original stays as a permanent regression guard.

7. **`session` field in the CLI JSON is not in the Go struct.**
   §5.2 example shows `{"session": "signal-loom/witness", ...}`. §4.1
   `InputAreaState` doesn't include a session field. That's fine — the
   CLI is allowed to wrap the struct — but be explicit: "CLI output is
   `{session, ...InputAreaState}`. Library callers already know the
   session they passed; the field is for CLI consumers piping JSON
   between sessions." One sentence in §5.2 closes the gap.

8. **Multi-line typed input encoding (§10 question 4) — answer it.**
   With `-J` (delta 2), literal newline in `Typed` only appears when
   the operator pressed Enter. That's the right default. Pin it in
   §4.2 so the spec doesn't kick the decision to stage 2.

9. **Rate-limiting guidance for high-frequency callers.**
   §4.5 says "consumers polling should rate-limit themselves." For the
   dashboard (which may want pane state on a hot path), this could mean
   one tmux subprocess per N sessions per refresh tick. Either commit
   to "this is acceptable load — tmux capture-pane is cheap" or add a
   note that a future bead may add a short-TTL cache layer if usage
   warrants it. As written, the spec leaves the question hanging.

10. **CLI exit code 2 vs "not stuck" semantics.**
    §5.2 distinguishes exit code 2 ("session not found") from exit
    code 1 ("other errors"). Good. The before/after consumer in §6.2
    doesn't check exit codes — a session that disappears mid-poll
    would currently produce `state` containing the CLI's stderr error,
    and the `jq` calls would fail or return null. The example in §6.2
    should at minimum check `$?` after `gc session input-area` and
    treat exit code 2 as "session gone — not our problem." One added
    line in the example would model this for consumer authors.

## Section-by-section notes

### §1 — Problem statement
Strong. The reproduction is concrete (timestamp, session name, exact
bytes), the false-positive cost is enumerated across three consumer
roles (witness/deacon/refinery), and §1.3 ("Root cause is asymmetry")
correctly frames why this belongs in the library rather than each
consumer prompt. No deltas needed.

### §2 — Goals and non-goals
Tight. The "drop-in replacement for current ad-hoc checks without
breaking existing approval-prompt and idle paths" goal pairs cleanly
with the audit in §8. The non-goal of not making `CapturePane`
ANSI-preserving by default is the right call — silently changing the
contract for every existing caller would create a much larger blast
radius than what this spec proposes. No deltas.

### §3 — Per-LLM input-area shapes
Empirical and well-organized. Three deltas land here:
- §3.1 ghost-text union (required delta 1)
- §3.4 unknown provider is well-defined (no delta)
- §3.5 summary table is the right shape

One small clarification worth adding to §3.2: the spec calls Codex's
box drawing "the practical prompt char is `> ` inside the box." For
the parser, this means the prompt-char match must look for `>` in box
context (e.g. preceded by `│ ` line-by-line) rather than treating any
`>` on a line as a prompt. Not a blocker — the implementation will
discover this — but worth flagging because a naive `>`-anywhere match
would false-positive on plenty of natural prose.

### §4 — Library API
Surface is clean. `InputAreaState{Provider, PromptChar, Busy, Typed,
Ghost, Raw, Detected}` is the right minimal field set. Three deltas:
- §4.2 approval-prompt behavior (required delta 4)
- §4.2 multi-line encoding (recommended delta 8)
- §4.3 most-recent-prompt-row (required delta 3)

`Raw` cap at 16 KiB is fine — large enough for any realistic pane
window, small enough that consumers logging it won't accidentally
DoS their own logs.

The `InputAreaCapturer` interface placement is sound. Putting it at
the runtime level (rather than only on the Tmux adapter) leaves room
for a future non-tmux provider to implement it, without forcing the
issue today.

### §5 — CLI surface
Two-surface design (`--raw` for humans, `input-area` for programs) is
the right split. Deltas:
- §5.2 `session` field clarification (recommended delta 7)
- §5.2 exit codes worth modeling in §6.2 example (recommended delta 10)

The `--format=json|kv|text` triad is well-chosen. `kv` is the format
shell consumers actually want — `eval $(gc session input-area X
--format=kv)` is the natural shell idiom and the spec gets it right.

### §6 — Consumer rewrite
The witness before/after is convincing. The reasoning in §6.3 — "one
example is enough; deacon and refinery use the same shape" — is
defensible for a stage-1 spec but worth verifying once stage 2 lands.
Specifically: the refinery's case (polecat finished but left input in
the buffer) interacts with the post-merge timing window in a way
witness's case does not. Refinery may need a "give the polecat N
seconds to flush before deciding the input was abandoned" wrapper
that `InputAreaState` does not provide. Out of scope for this spec,
but flag it as a "consumer rollout open question" tracked separately.

### §7 — Test strategy
Three-layer test plan is exactly right for this surface. Deltas:
- §7.1 canonical fixture from Appendix C (recommended delta 6)
- §7.2 integration test scope: extend to cover multi-line typed input
  (one extra `printf` step), or be explicit that multi-line testing is
  fixture-only.

One technical note on §7.2: the integration test sends `printf
'\xe2\x9d\xaf \x1b[2m...\x1b[0m'`. Tmux interprets escape sequences on
the way in, so the bytes captured back out will faithfully include
SGRs. That's correct, but the test must use a tmux build new enough
that `capture-pane -e` is supported (tmux 2.8+ from late 2018). Worth
a one-line note in the test or its README.

Fixture format question for §7.1: are SGRs stored as raw ESC bytes
(file is binary-ish but readable) or escape-encoded (`\x1b` literals)?
The first is closer to what `tmux capture-pane -p -e > file` produces
naturally; the second is friendlier to `git diff`. Recommend raw bytes
+ a `make-fixture` helper that captures from a live session, since
the spec already proposes a README documenting how to capture. Pick
one and document it; either is fine.

### §8 — Backward-compatibility audit
Thorough and trustworthy. Every `CapturePane` caller audited with a
specific "affected?" answer. §8.4's note on `WaitForIdle` ("the prompt
char itself is not rendered dim, so plain capture is fine for idle
detection") is exactly the kind of load-bearing invariant a future
reviewer will want documented. Keep it.

§8.5's three-phase migration (library only → consumer prompts → maybe
WaitForIdle later) gives a clean rollout shape. No deltas.

### §9 — Upstream PR considerations
Sound. Three-stage PR plan, JSON schema versioning called out, fork
impact enumerated. Delta:
- §9.5 telemetry event tied to typed-events contract (required delta 5)

One small addition to §9.2 worth considering: the stage 2 PR title
shape. The project convention is conventional commits
(`feat(scope): …`); a clear title like
`feat(runtime/tmux): InputAreaState — ghost-text-aware input inspection`
saves the upstream maintainer a rename. Optional polish.

§9.4 ("the InputAreaState struct is a wire contract once the CLI
returns it as JSON") is a real and important point. Endorsing the
recommendation to publish a JSON schema in `docs/schema/` — this is
the right time to do it, before downstream consumers pin against
implicit shape.

### §10 — Open questions
Answered in the "Open question responses" section below.

## Open question responses

**Q1: Ghost-text wrapper across Claude versions.**
Match a small union of dim-style SGRs, not just `ESC[2m`. See required
delta 1. Concrete recommendation: a `dimSGRPattern` regex matching
`\x1b\[(2|90|38;5;(23[2-9]|24[0-5]))m` opens; close with `\x1b\[(0|39)m`.
That covers the three rendering conventions seen in Claude Code so
far and any plausible near-future variant.

**Q2: Codex ghost text.**
Not observed in 2026-04/05 Codex builds. Leave the `Ghost` field
present but empty for Codex. Do not conflate Codex's slash-command
autocomplete dropdown with ghost text — that's a separate UI element
(a popup, not inline after the prompt) and not the same buffered-input
false-positive risk. Worth noting in §3.2.

**Q3: Gemini boxed input.**
Polecat-gemini is blocked (tk-mmny1) so I cannot verify empirically.
The spec's "verify per release" stance is the right answer. Recommend
landing the Gemini parser with the `> ` on clean row assumption,
gated on the polecat-gemini unblock; if a release boxes the input,
the parser pattern flips to Codex-style box detection without
breaking the API.

**Q4: Multi-line typed input encoding.**
Literal newline (`\n`) in `Typed`. With `-J` on (required delta 2),
literal newlines only appear when the operator pressed Enter — exactly
the signal multi-line intent should carry. Consumers that don't care
about multi-line can `jq -r '.typed | gsub("\n";" ")'`. Pin the
answer in §4.2.

**Q5: tmux `capture-pane -J` vs unjoined.**
Use `-J` by default. See required delta 2. The wrap-to-second-row
ambiguity dissolves; the implementation gets simpler; the
consumer-side semantics get cleaner.

**Q6: Non-tmux providers.**
The spec's "leave the interface in place, only tmux implements it"
stance is correct. Exec / k8s providers may not have a meaningful
"pane" at all — for them, `InputArea` returning
`runtime.ErrUnsupported` is the right semantic, not an empty
`InputAreaState`. Add to §4.1: "Providers that don't model a pane
return `runtime.ErrUnsupported` from `InputArea`; consumers must
treat this as 'no input-area concept' and not as 'no input.'"

## Risk callouts (consumer-side trip wires)

1. **Scrollback prompt false positives.** If a long scrollback window
   includes a prompt row from a previous turn, a parser that finds the
   first match top-down will bind to the wrong row. Required delta 3
   addresses this in the spec, but it bears repeating: consumers
   should not assume `InputAreaState` is robust against parser bugs
   here — file a fixture and a test if seen in the wild.

2. **Approval prompt timing window.** A session that transitions from
   "approval up" to "approval cleared" between two `InputAreaState`
   calls produces two different states. Consumers polling at 200ms
   will see flapping; this is correct behavior, not a bug, but worth
   documenting so consumer prompts don't treat the flap as evidence of
   a stuck session.

3. **`gc session peek --raw` vs `gc session input-area` overlap.**
   Both produce ANSI-aware output. The spec is clear on the
   distinction (humans vs programs) but consumers will be tempted to
   `peek --raw | grep` instead of using `input-area`. The CLI help
   text for `--raw` should explicitly say "for human debugging — use
   `gc session input-area` for programmatic checks." Otherwise the
   nice typed contract gets bypassed and the prompt re-rolls
   parsing again.

4. **No `WaitForOperatorInput` / `WaitForGhostClear`.** §4.5 correctly
   defers these. But consumers wanting "wait until operator finishes
   typing" will build polling loops on top. That's fine for v0;
   stage 3+ may add waiters. Worth being explicit in the spec that
   polling is the v0 contract.

5. **Rate-limiting and tmux subprocess cost.** §4.5 leaves this to
   consumers. For high-concurrency observers (a dashboard refreshing
   pane state for 30 sessions every 2 seconds), the subprocess cost is
   non-trivial. Recommended delta 9 asks the spec to commit on this.
   Either "tmux capture-pane is cheap, no concerns" or "future bead
   may add caching" — pick one and document.

6. **Refinery's post-merge timing window.** Section "§6 notes" above —
   the refinery use case has a "wait briefly before deciding" wrinkle
   the witness use case doesn't. Out of scope for this spec but should
   be tracked in the consumer rollout bead so it doesn't get lost.

## Summary

Spec is implementable and load-bearing. The five required deltas are
small (one to three sentences each in the spec) and harden details
that today are "implementer's discretion." The five recommended
deltas are quality-of-life polish but not blockers.

Verdict reiterated: **needs-delta**, stage 2 can start immediately
after editorial pass.
