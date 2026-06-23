# Code Review: InputAreaState Implementation (gc-8g41r.3)

## Verdict

needs-delta

The implementation has the right shape for Claude and the CLI/consumer surface
now exists, including `gc session peek --raw`, `gc session input-area`, a witness
consumer example, and a live tmux integration test. I would not graduate the
convoy as-is, though: the current branch still misses several sharpened-spec
deltas from the stage-1b reviews, especially the current Codex arrow prompt and
the structured wire contract fields.

## Findings

1. **Codex current arrow prompt is not parsed.**

   `internal/runtime/tmux/input_area.go:239` still describes and implements
   Codex as a boxed-only UI. `parseCodexInputArea` scans only for `│ > `
   (`input_area.go:260-273`) and returns an empty prompt state for anything
   else. The sharpened spec requires a union parser for both `codex_boxed` and
   current `codex_arrow` (`› `) shapes, with dim-ghost separation and queued
   follow-up chrome ignored (`engdocs/design/input-area-state.md:218-307`).

   Impact: current Codex sessions can show real typed text or dim suggestion
   text at `› `, but `gc session input-area` will report no prompt, no typed
   input, and no ghost text. That undercuts the multi-LLM claim and leaves Codex
   consumers blind to the exact state this API is meant to expose. The tests
   currently cover only boxed fixtures (`internal/runtime/tmux/input_area_test.go:104-128`,
   `314-365`).

2. **The dim-SGR union is narrower than the accepted spec.**

   The sharpened spec requires `ESC[38;5;<n>m` for `n` in `232..245`
   (`engdocs/design/input-area-state.md:164-188`). `updateDimState` only marks
   `240..243` as ghost (`internal/runtime/tmux/input_area.go:464-472`), and the
   test suite explicitly asserts `244` is not dim
   (`internal/runtime/tmux/input_area_test.go:182`). The implementation also
   does not clear dim state on `ESC[39m`, even though the spec allows default
   foreground reset as a ghost wrapper terminator (`engdocs/design/input-area-state.md:157-160`).

   Impact: Claude or Codex builds using gray 232-239, 244, or 245 will silently
   put ghost text into `Typed`, recreating the original false-positive class.

3. **The `InputAreaState`/CLI JSON contract is missing `shape_variant` and `raw_truncated`.**

   The spec's API and default JSON include `ShapeVariant` and `RawTruncated`
   (`engdocs/design/input-area-state.md:109-113`, `378-390`, `500-527`).
   The Go struct has only provider, prompt, busy, typed, ghost, raw, and detected
   (`internal/runtime/tmux/input_area.go:51-59`), and the CLI JSON mirrors that
   reduced shape (`cmd/gc/cmd_session.go:2236-2245`, `2353-2364`).

   Impact: consumers cannot distinguish Codex build variants, and they cannot
   tell whether `Raw` was clipped. The implementation does keep newest content
   when applying the 16 KiB cap (`input_area.go:92-94`), but without a flag the
   structured output violates the documented wire contract.

## Per-Area Review

- **Parser correctness:** Claude handles the primary ghost path, NBSP prompt,
  busy precedence, and feedback survey case. Gemini is best-effort and
  provisional. Codex needs the arrow-shape delta before the implementation can
  claim current Codex support.
- **ANSI edge cases:** Good direct tests for SGR 2, SGR 90, and one 256-color
  gray. Coverage and implementation need to match the full 232-245 range and
  `ESC[39m` reset behavior.
- **Test coverage:** Unit coverage is solid for the implemented subset, and the
  new live tmux test proves `capture-pane -e` preserves SGR bytes. Missing
  coverage: Codex `› ` + dim ghost, queued follow-up block above the prompt,
  full gray range, `ESC[39m`, and wrapper-level raw truncation.
- **Error handling:** CLI unsupported-provider and session-not-found paths are
  covered and return the intended exit codes. Parser failures degrade to an
  empty state rather than errors, matching the spec.
- **Multi-LLM extensibility:** The public shape is close, but defining the
  capability and state in `internal/runtime/tmux` means non-tmux providers must
  depend on tmux types to implement input-area support. That is acceptable for
  the first tmux-only implementation if treated as transitional, but the spec's
  "runtime-level interface" remains unresolved.
- **Fit with sharpened spec:** D2 is satisfied: `raw` is omitted by default and
  gated behind `--include-raw`. D1, D3, and D5 still need deltas.

## Open Question

The bead comment asked whether the missing CLI, consumer example, and live tmux
integration test should be a follow-up child or held until stage 4. On the
current branch those are no longer missing (`569d180ee`, `d9a8eb58e`,
`73fe485e8`). The follow-up should instead be a delta child for the findings
above before graduation.

## Validation

- `go test ./internal/runtime/tmux -run 'InputArea|Raw'`
- `go test ./cmd/gc -run 'InputArea|PeekRaw'`
- `go test -tags=integration ./internal/runtime/tmux -run TestInputAreaLiveTmux -count=1`
