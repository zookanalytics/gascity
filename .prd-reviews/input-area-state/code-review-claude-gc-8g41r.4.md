# Code Review (Claude): InputAreaState Implementation (gc-8g41r.4)

Stage 4 review of convoy `gc-8g41r`, against the **current**
`integration/input-area-state` at `e0e90a2ce` (post lean-fix `gc-8g41r.7`).
This is the post-pivot review: it assesses the minimal faint=ghost detector,
not the original gray-range design the Codex review (`gc-8g41r.3`) saw.

## Verdict

**approve — graduation-ready** (squash `integration/input-area-state` → `main`).

The lean-fix resolves all three Codex findings (two of them by a *better*
route than Codex proposed), the parser is correct, the dropped surface is
removed without dangling references, and the test suite is excellent. Every
gate I ran is green (build, vet, unit, live-tmux integration — see Validation).
The three observations below are **non-blocking follow-ups**, not graduation
blockers; none is a defect in a currently-shipping rendering.

## Cross-check of the Codex findings (gc-8g41r.3)

I agree with all three of Codex's original findings *as written against the
pre-lean branch*, and I agree with how `gc-8g41r.7` resolved each — including
the two cases where the resolution diverged from Codex's suggested fix.

1. **Codex #1 — Codex arrow prompt not parsed → RESOLVED, agree.**
   `parseCodexInputArea` now binds both shapes bottom-up: the boxed `│ > `
   anchor (`input_area.go:269-273`) and the current arrow `› `
   (`input_area.go:280-287`). Arrow ghost reuses the shared faint helper —
   no Codex-specific ghost code, matching spec §3.2. Tests cover arrow
   idle/typed/faint-ghost/queued-followup/bottom-up
   (`input_area_test.go:374-404`). This is a complete fix.

2. **Codex #2 — dim-SGR union narrower than spec (240-243 vs 232-245,
   missing `ESC[39m`) → RESOLVED BY REFRAME, agree (and this is the stronger
   fix).** Rather than widening the gray range, the lean-fix *deletes* the
   gray-range heuristic and replaces it with a single rule: faint (`ESC[2m`)
   is ghost, everything else is typed (`splitDimSegments`/`updateDimState`,
   `input_area.go:396-481`). This is strictly better than the range Codex
   asked for: it is LLM-agnostic (no per-provider color table), it is
   empirically validated on live Claude (operator+host, 2026-06-15), and it
   eliminates the false-positive class in *both* directions Codex worried
   about (a gray-but-not-faint themed input is no longer misread as ghost; a
   faint suggestion outside the old range is no longer misread as typed).
   `ESC[39m` is now a faint terminator (`updateDimState` case `"39"`), and
   the `38;5;2`/`38;2;r;g;b` "is the 2 a faint code?" trap Codex implicitly
   raised is closed by the extended-color skip (`input_area.go:469-478`),
   with a dedicated test (`input_area_test.go:219`). Do not re-flag the old
   range — it is intentionally gone.

3. **Codex #3 — missing `shape_variant`/`raw_truncated` → RESOLVED BY DROP,
   agree.** Per the operator's "no consumers" decision, the lean-fix removed
   the whole raw surface rather than adding the promised discriminators. The
   struct is now `{Provider, PromptChar, Busy, Typed, Ghost, Detected}`
   (`input_area.go:51-58`), spec §4.2 matches, and the CLI JSON mirrors it
   (`cmd_session.go:2183-2191`). This is the right call under YAGNI; I am not
   re-flagging the dropped fields.

**Codex's one open architectural note still stands** (see Finding 2 below):
defining the capability in `internal/runtime/tmux` means a non-tmux provider
would have to import `tmux`. The lean-fix did not change this (out of scope),
so it remains an open item for the upstream PR.

## New findings (missed angles) — all non-blocking

1. **Latent ghost→typed misread if a build ever renders the prompt glyph
   itself faint.** `splitDimSegments` is always seeded with `dim=false` at the
   first byte *after* the prompt char / arrow
   (`input_area.go:210` for Claude, `input_area.go:283` for Codex arrow). The
   faint state of the bytes *before* the prompt is discarded — for the arrow
   path it is explicitly thrown away by the whitespace guard, which only keeps
   `stripANSI(raw[:idx])` (`input_area.go:281`). This is correct for every
   rendering observed today, because the validated shape is
   `❯ ESC[2m…ESC[0m` / `› ESC[2m…ESC[0m` — the glyph sits *outside* the faint
   wrapper, so the first `ESC[2m` always lands inside `content`. But if a
   future build wraps the entire input row faint
   (`ESC[2m❯ suggestion ESC[0m`), the post-prompt ghost would carry no leading
   `ESC[2m` in `content` and would be classified as **Typed** — recreating the
   exact false-stall class this convoy exists to kill (a ghost suggestion
   firing a buffered-input warrant). This is speculative and currently
   unobserved; it is sharper for Codex than Claude only because the Codex
   arrow ghost path is fixture-tested but not live-validated (Finding 3).
   *Concrete hardening if/when wanted:* compute the carried faint state by
   running the SGR scan over the pre-prompt slice and seed `splitDimSegments`
   with it, instead of starting at `false`. Cheap, removes the assumption.
   **Recommend a follow-up bead, not a graduation blocker.**

2. **`InputAreaState` + `InputAreaCapturer` live in package `tmux`
   (architecture / upstream-PR note).** A second runtime (exec, k8s, fake)
   that wanted input-area support would have to import `internal/runtime/tmux`
   to implement the interface or return the type. This is *acceptable today*
   and is in fact the correct call under the repo's "No premature abstraction
   — don't build interfaces until two implementations exist" rule (there is
   one implementation). I raise it only because it is the thing an upstream
   reviewer will ask about: when a real second provider appears, the type and
   the `InputAreaCapturer` interface should move up to `internal/runtime` as a
   runtime-level contract (spec §4 calls this out as unresolved). Worth a
   sentence in the upstream PR description so it reads as a known, deliberate
   staging choice rather than a layering miss. **Non-blocking.**

3. **Codex-arrow and Gemini paths are not live-validated.** The faint rule is
   live-validated end-to-end for Claude (`TestInputAreaLiveTmux` drives real
   `capture-pane -e` bytes through capture→parse→classify and passes here on
   tmux 3.6b). The Codex *arrow* ghost path is only fixture-tested
   (`input_area_test.go:387-392`) — the spec itself hedges to "faint text …
   has been observed" (§3.2). Gemini is explicitly provisional and blocked on
   a live capture (`tk-mmny1`, noted at `input_area.go:296-298`). Because all
   three share one faint helper that *is* live-validated, the risk is low and
   this is correctly scoped as later-stage verification, not a blocker. Flag
   only so graduation does not silently imply "Codex/Gemini live-proven."
   **Non-blocking — already tracked.**

## Per-area review

- **Faint=ghost correctness / robustness.** Correct. `updateDimState`
  (`input_area.go:457-481`) handles the load-bearing cases: SGR 2 sets faint;
  0/22/39/empty clear it; `38`/`48` extended-color introducers consume their
  `5;n` (256) or `2;r;g;b` (truecolor) payload so a color index is never
  misread as the faint code. I traced the bound checks
  (`i+2 < len`, `i+4 < len`) against `["38","5","2"]`, `["2","38","5","0"]`,
  `["38","2","0","0","0"]`, and truncated `["38","5"]` — all classify
  correctly, and truncated/malformed groups fail safe (faint unchanged).
  Byte/rune handling is correct: `len(codexArrowPrompt)` and
  `strings.Index` are both byte-based, so the multibyte `›` slices cleanly.
  The only robustness gap is Finding 1 (pre-prompt faint state), which is
  out of the currently-observed envelope.

- **Codex-arrow parsing.** Correct and well-guarded. Bottom-up scan returns
  the lowest (most recent) prompt row; the "arrow must start the line" guard
  (`input_area.go:281`) keeps a `›` inside a queued-follow-up affordance from
  being read as the input area; a typed line that itself contains `› ` is fine
  because `strings.Index` binds the first (prompt) arrow and the remainder is
  content. Matches spec §3.2.4 step-for-step.

- **Drops are clean.** Verified no dangling references to `RawPeeker`,
  `CapturePaneRaw`/`CapturePaneAllRaw`, `PeekRaw`/`doSessionPeekRawFallback`,
  `--include-raw`, `peek --raw`, `shape_variant`/`raw_truncated`, or a `Raw`
  struct field anywhere in `*.go`, in `docs/reference/cli.md`, or in the spec.
  The only surviving `streamSessionPeekRaw*` symbols are in `internal/api/`
  (the SSE `format=raw` stream) — a *different* mechanism that never used
  `tmux.RawPeeker`; the lean-fix correctly left it untouched and it still
  builds. `raw_capture_test.go` is replaced by `input_area_adapter_test.go`,
  keeping only the delegation test. cli.md is regenerated (input-area entry
  present, no `--raw`).

- **Test design quality.** Excellent. Table-driven, fixtures as raw-string
  literals with named SGR macros (`dimOn`, `gray256`, `brBlack`) so the wire
  bytes are visible inline; each fixture carries a comment tying it to the
  real incident it reproduces (e.g. the 2026-05-09 `signal-loom` false
  positive, the 2026-05-30 feedback-survey clarification). `TestSplitDimSegments`
  pins the load-bearing helper directly, including the `38;5;2` trap and
  `bold+faint` precedence; `TestInputAreaLiveTmux` proves the one thing
  fixtures cannot — that the host tmux build preserves the SGR bytes — on an
  isolated `-L` socket killed on cleanup (tmux-safety compliant). Gap is
  intentional: no whole-line-faint fixture (Finding 1) because that rendering
  is unobserved.

- **Prompt-consumer ergonomics (agent-prompt-author view).** Strong and
  ZFC-aligned. The witness consumer
  (`examples/gastown/packs/gastown/agents/witness/prompt.template.md:143-175`)
  consumes `gc session input-area` → `jq .typed/.busy` with **no per-provider
  branches** — the library carries the rendering knowledge, the prompt carries
  the judgment ("is there buffered input the agent ignored?"). The struct
  exposes raw facts (typed/ghost/busy/prompt_char) rather than a baked
  `ready_for_input` verdict, which is the correct boundary: a "ready" flag
  would be a judgment call in Go (a ZFC violation). The three output formats
  (json/kv/text) cleanly separate machine vs human consumers. I would ship
  this consumer pattern as-is.

- **Graduation readiness.** Ready. Spec ↔ code in sync (struct, faint rule,
  Codex shapes); status deliberately stays `Proposed` (an operator call to
  flip to `Accepted` on graduation, not a blocker). Net −272 LOC — the change
  genuinely shrinks the maintained surface, which is the right direction for a
  fork tracking upstream.

## Validation (all run against `e0e90a2ce` in this worktree)

- `go build ./internal/runtime/tmux/... ./cmd/gc/... ./internal/api/...` — OK
  (confirms the SSE `format=raw` path still compiles after the tmux drops).
- `go test ./internal/runtime/tmux -run 'InputArea|SplitDim|StripANSI'` — ok
- `go test ./cmd/gc -run 'InputArea'` — ok
- `go vet ./internal/runtime/tmux/... ./cmd/gc/...` — clean
- `go test -tags=integration ./internal/runtime/tmux -run TestInputAreaLiveTmux`
  — PASS (tmux 3.6b), faint=ghost validated on real `capture-pane -e` bytes.

## Suggested follow-up beads (none block graduation)

- Harden `splitDimSegments` seeding against a faint-wrapped prompt glyph
  (Finding 1).
- When a second non-tmux input-area provider is needed, lift `InputAreaState`
  / `InputAreaCapturer` to `internal/runtime` (Finding 2 / spec §4 open item).
- Live-validate the Codex arrow ghost path and unblock Gemini verification
  (`tk-mmny1`) in a later stage (Finding 3).
