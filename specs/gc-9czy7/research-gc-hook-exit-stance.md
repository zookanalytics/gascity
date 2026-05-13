# Research: `gc hook` exit 1 on no work — intent, upstream stance, options

## Provenance

| Doc-type / artifact | Producer | Source location | Surveyed at |
|---|---|---|---|
| Bead | `gc-toolkit__mechanik` (filer) | bd `gc-9czy7` | 2026-05-10 |
| Source — current | `cmd/gc/cmd_hook.go` (`doHook`, lines 215–241) | local worktree `/home/zook/loomington/.gc/worktrees/gascity/polecats/gc-toolkit.furiosa` @ origin/main `42c44620` | 2026-05-10 |
| Source — upstream | `gastownhall/gascity` @ `upstream/main` `3424878d`, file `cmd/gc/cmd_hook.go` lines 215–241 (byte-identical to fork) | git remote `upstream` | 2026-05-10 |
| Commit — file genesis | `18077c2f` "Add gc hook --inject and Stop hook for autonomous work pickup" — julianknutsen, 2026-02-25 | git log `cmd/gc/cmd_hook.go` --reverse | 2026-05-10 |
| Commit — no-work text detection | `2e5091df` "Fix Dolt port resolution and no-work detection" — Gastown Test, 2026-03-17 | `git show 2e5091df -- cmd/gc/cmd_hook.go` | 2026-05-10 |
| Commit — output normalization | `e6644f335` (julianknutsen, 2026-03-24) — introduced `normalizeWorkQueryOutput` and switched the print to `normalized` | `git blame cmd/gc/cmd_hook.go` | 2026-05-10 |
| Commit — inject hardening (claim-protocol only) | `cea7b34bd` "Adopt PR #1454: harden hook inject behavior" — Julian Knutsen, 2026-04-30 (fork) / upstream PR #1517 `ff5d7eaf` "harden(hook): keep claim flow non-intrusive", 2026-05-03 | `git show cea7b34bd`, `git show ff5d7eaf` | 2026-05-10 |
| Cobra `Long:` doc string | `cmd/gc/cmd_hook.go:24` — "Without --inject: prints raw output, exits 0 if work exists, 1 if empty." | local worktree | 2026-05-10 |
| `doHook` doc comment | `cmd/gc/cmd_hook.go:213–216` — "Without inject: prints raw output, returns 0 if work, 1 if empty." | local worktree | 2026-05-10 |
| Test — exit 1 + empty stdout for no work | `TestHookNoWork` in `cmd/gc/cmd_hook_test.go:12–22` | local worktree | 2026-05-10 |
| Test — exit 1 on subprocess error | `TestHookCommandError` in `cmd/gc/cmd_hook_test.go:36–46` | local worktree | 2026-05-10 |
| Test — exit 1 even when no-work *message* printed | `TestHookNoReadyMessagePrintsButExitsOne` in `cmd/gc/cmd_hook_test.go:60–72` | local worktree | 2026-05-10 |
| Test — inject mode always exits 0 (even on error) | `TestHookInjectAlwaysExitsZero` in `cmd/gc/cmd_hook_test.go:277–285` | local worktree | 2026-05-10 |
| Test — `[]` JSON array classified as no-work | `TestWorkQueryHasReadyWorkEmptyJSONArray` in `cmd/gc/cmd_hook_test.go:305–309` | local worktree | 2026-05-10 |
| Test — session-template context expects code 1 for empty | `TestCmdHookSessionTemplateContextDoesNotScanSessionsForName` in `cmd/gc/cmd_hook_test.go:229–275` | local worktree | 2026-05-10 |
| Default work_query | `Agent.EffectiveWorkQuery` in `internal/config/config.go:2026–2095` | local worktree | 2026-05-10 |
| Architecture doc | `engdocs/architecture/life-of-a-bead.md:118–141` (describes hook mechanism; only `--inject` exit code is documented, not non-inject) | local worktree | 2026-05-10 |
| Architecture doc — glossary | `engdocs/architecture/glossary.md:85` — references `gc hook` neutrally | local worktree | 2026-05-10 |
| Pack caller — status line | `examples/gastown/packs/gastown/assets/scripts/status-line.sh:10–11` — `w=$(gc hook "$agent" 2>/dev/null \| grep -c . \|\| true)` | local worktree | 2026-05-10 |
| Pack caller — integration test | `test/agents/graph-dispatch.sh:263–266` — `timeout 10 gc hook 2>/dev/null` | local worktree | 2026-05-10 |
| Pack caller — doctor | `cmd/gc/doctor_codex_hooks.go:132` — uses `gc hook --inject` (not non-inject) | local worktree | 2026-05-10 |
| Filer-cited symptom #1 | `gc-toolkit.mechanik` startup ran `gc hook`, got exit 1 with empty stdout | bead `gc-9czy7` description | 2026-05-10 |
| Filer-cited symptom #2 | `gascity/gc-toolkit.polecat-codex-adhoc-2b302e6e38` work-discovery flow ran `gc hook`, got exit 1 with no output | bead `gc-9czy7` description | 2026-05-10 |
| Operator framing | "it's known, apparently intentional, although causes parallel commands to fail … if it aligns with the goals of GasCity, harder to propose a fix" | bead `gc-9czy7` description | 2026-05-10 |
| Standard 3-options reference | `MEMORY.md` → `feedback_upstream_engagement.md` (cited by the bead) | not available in this worktree (gascity repo, not loomington memory) | n/a |

## Source-code summary

`gc hook` (no flags) returns exit 1 when the agent's `work_query` does not
return ready work. The semantic is set in two places that mirror each other:

- **Cobra `Long:`** at `cmd/gc/cmd_hook.go:24`:
  > "Without --inject: prints raw output, exits 0 if work exists, 1 if empty."

- **`doHook` doc comment** at `cmd/gc/cmd_hook.go:213–216`:
  > "results based on mode. Without inject: prints raw output, returns 0 if
  > work, 1 if empty. With inject: skips the work query and returns 0."

The implementation at `cmd/gc/cmd_hook.go:217–241` has three return paths:

```go
func doHook(workQuery, dir string, inject bool, runner WorkQueryRunner, stdout, stderr io.Writer) int {
    if inject {
        return 0
    }

    output, err := runner(workQuery, dir)
    if err != nil {
        fmt.Fprintf(stderr, "gc hook: %v\n", err)
        return 1                                            // (A) subprocess error
    }

    trimmed := strings.TrimSpace(output)
    normalized := normalizeWorkQueryOutput(trimmed)
    hasWork := workQueryHasReadyWork(normalized)

    if !hasWork {
        if normalized != "" {
            fmt.Fprint(stdout, normalized)                  // may print "[]" or "No ready work found …"
        }
        return 1                                            // (B) no work
    }
    fmt.Fprint(stdout, normalized)
    return 0                                                // (C) work
}
```

`workQueryHasReadyWork` (`cmd_hook.go:243–264`) classifies output as no-work when:
- output is empty (after trim);
- output contains the string `No ready work found` (the human-readable line newer bd versions emit); or
- output JSON-decodes to an empty array, empty object, or `null`.

`normalizeWorkQueryOutput` (`cmd_hook.go:266–282`) wraps a bare JSON object
into a single-element array so the rest of the system sees a uniform shape.

**Stdout shape on exit 1 — what callers actually see:**

| Scenario | Stdout on exit 1 |
|---|---|
| (A) subprocess error (work_query failed / timed out) | empty |
| Agent suspended, city suspended, agent not found, agent flag missing | empty (stderr carries the message) |
| (B) work_query printed `[]` (the default work_query's final fallback at `internal/config/config.go:2058`) | `[]` |
| (B) work_query printed `No ready work found …` | the no-work message |
| (B) work_query exited cleanly with truly empty stdout (custom or legacy work_query) | empty |

The default `Agent.EffectiveWorkQuery` *for non-legacy targets* ends with
`printf "[]"` (`internal/config/config.go:2058`), so the typical no-work
path produces stdout `[]` plus exit 1. The legacy-control-dispatcher
branch (`config.go:2060–2094`) has no `printf "[]"` fallback — its
trailing `bd ready … --metadata-field gc.routed_to=<legacy> --unassigned`
can return nothing, yielding **truly empty stdout + exit 1**.

The two symptoms the filer cites (mechanik startup, codex-adhoc polecat
work-discovery) are both **case (B) "no work"**: by `cmd_hook.go:218–238`,
exit 1 is the documented contract for "no work was found." The exact
stdout — empty vs `[]` vs `No ready work found …` — is a function of the
work_query the agent uses, not of `gc hook` itself. **Exit 1 is the
universal constant; stdout varies by caller.**

### History of the contract

The contract has been stable since `cmd_hook.go` was introduced:

- **`18077c2f`** (2026-02-25, julianknutsen) — file genesis. Original body
  was `if trimmed == "" { return 1 }`. The cobra `Long:` already read:
  > "Without --inject: prints raw output, exits 0 if work exists, 1 if empty."

- **`2e5091df`** (2026-03-17, Gastown Test, "Fix Dolt port resolution and
  no-work detection") — refactored to `if !hasWork { return 1 }` and added
  `workQueryHasReadyWork` to recognize newer bd's "No ready work found"
  text as also-no-work. The 1-on-empty contract was preserved verbatim;
  the *commit title* names "no-work detection" as the fix target,
  confirming the exit-1 path is part of the contract being maintained.

- **`e6644f335`** (2026-03-24, julianknutsen) — added
  `normalizeWorkQueryOutput` and switched the printed payload from the
  raw `output` to `normalized`. Exit-code contract unchanged.

- **`cea7b34bd`** (2026-04-30, Julian Knutsen, "Adopt PR #1454: harden hook
  inject behavior" / upstream `ff5d7eaf` PR #1517 "harden(hook): keep
  claim flow non-intrusive") — hardened the **inject** branch to skip
  the work query entirely and always exit 0. Non-inject return-1-on-empty
  semantics were left untouched. The cobra `Short:` was simplified and the
  `--hook-format` flag hidden, but no exit-code semantics changed for the
  non-inject path.

Across four touches over three months by three authors, every change has
**preserved** the non-inject exit-1-on-empty contract verbatim. The
contract is also locked in by multiple tests (see Upstream evidence
below); altering it would require deleting or rewriting `TestHookNoWork`,
`TestHookNoReadyMessagePrintsButExitsOne`, and at least three other
tests.

## Upstream evidence

### Fork vs upstream drift on `cmd_hook.go`

`git diff upstream/main..origin/main -- cmd/gc/cmd_hook.go` produces **no
output** as of `42c44620` (fork) and `3424878d` (upstream). The file is
byte-identical. The `cea7b34bd` "Adopt PR #1454" commit in the fork
corresponds to upstream PR #1517 (`ff5d7eaf`); both branches converged on
the same final code.

### Tests asserting the contract (`cmd/gc/cmd_hook_test.go`)

The following tests *durably encode* the exit-1-on-no-work contract. A
fix that changes the exit code would have to delete or rewrite each one:

| Test | Lines | Asserts |
|---|---|---|
| `TestHookNoWork` | 12–22 | `runner` returns `""` → `doHook` returns **1** AND `stdout.Len() == 0`. This is the exact "empty stdout + exit 1" shape the filer observed, encoded as desired behavior. |
| `TestHookCommandError` | 36–46 | `runner` returns error → exit **1** with error written to stderr. |
| `TestHookNoReadyMessagePrintsButExitsOne` | 60–72 | `runner` returns the "✨ No ready work found …" line → **stdout contains the message** AND exit is **1**. Explicitly documents that the no-work message is printed but exit is still 1. |
| `TestHookInjectAlwaysExitsZero` | 277–285 | Inject mode + subprocess error → exit **0**. Documents the asymmetry: inject swallows everything, non-inject propagates no-work and error as exit 1. |
| `TestHookInjectNoWork` | 48–58 | Inject + no work → exit 0, empty stdout. |
| `TestHookInjectSuppressesNoReadyMessage` | 74–86 | Inject + "No ready work found" → exit 0, empty stdout. |
| `TestHookInjectIsNonIntrusiveWithWork` | 88–98 | Inject + work present → exit 0, empty stdout (inject is non-intrusive). |
| `TestHookInjectDoesNotRunWorkQuery` | 100–120 | Inject never runs the runner at all. |
| `TestWorkQueryHasReadyWorkEmptyJSONArray` | 305–309 | `workQueryHasReadyWork("[]") == false` → `[]` is no-work. |
| `TestWorkQueryHasReadyWorkNonEmptyJSONArray` | 311–315 | Non-empty JSON array is work. |
| `TestCmdHookSessionTemplateContextDoesNotScanSessionsForName` | 229–275 | Asserts code **1** with `[]` output for empty pool ("`code != 1 → fatal …`"). |
| `TestDoHookNormalizesSingleObjectOutputToArray` | 840–853 | Single JSON object normalized to a 1-element array — exit 0 for "work present." |

The presence of `TestHookNoWork` is especially decisive: it asserts the
literal symptom shape (`stdout empty AND code == 1`) the filer observed,
as the *desired* outcome. The behavior is not a bug; it's a tested
invariant.

### Upstream commits touching `gc hook` (`gastownhall/gascity`)

Selected from `git log upstream/main --grep='gc hook'` (full list in
provenance table). None of these change the non-inject exit-code
contract; all of them either re-shape environment plumbing or harden the
inject mode further:

| SHA | Subject | Effect on exit-code contract |
|---|---|---|
| `ff5d7eaf` PR #1517 | harden(hook): keep claim flow non-intrusive | none — only inject path hardened |
| `043e61ea` #1007 | sling: add --reassign flag for human→pool-worker handoff | none — doesn't touch `cmd_hook.go` semantics |
| `5e1505a8` #650 PR #514 | scope gc hook work query env to agent's rig store | environment plumbing only |
| `2afaf00d` | mirror of #650 follow-up | environment plumbing only |
| `83e303a9` | tests for agent and city suspend/resume | adds the suspended-agent return-1 path |
| `ee69f95e` #1380 | emit codex hook context as JSON | inject formatting only |
| `9424106e` | compose.expand fanout, session lifecycle | unrelated |

### Architecture / design docs

- `engdocs/architecture/life-of-a-bead.md:118–141` describes the hook
  mechanism. It documents the `--inject` exit code ("exits 0 without
  running the work query and emits no output") but does **not** document
  or justify the non-inject exit-1 behavior — it's treated as an
  implementation detail.
- `engdocs/design/session-model-unification.md:431, 608` reference
  `gc hook` in the session-context routing matrix; no exit-code
  discussion.
- `examples/gastown/FUTURE.md:18, 31, 34` describe `gc hook` as a "thin
  wrapper over bd protocol"; no exit-code discussion.
- `CHANGELOG.md:86–89` documents only the `--inject` change ("now silent
  legacy compatibility … fresh managed hook configs no longer install
  it"); no mention of non-inject exit code.

**Net upstream stance, derived from artifacts:** the exit-1-on-no-work
behavior is intentional, documented in the cobra command's own `Long:`
help text, tested with the exact symptom shape the filer observed
(`TestHookNoWork`), preserved through four refactors by three authors,
and identical between zookanalytics fork and gastownhall upstream. There
is no design doc that *explains* the choice, but every act of
maintenance on the file has implicitly re-affirmed it.

## Failure-mode catalog

The filer's framing — *"causes parallel commands to fail"* — is correct
under common shell idioms. Concrete failure modes that hit Loomington
callers:

### A. `&&` short-circuit

```sh
gc hook && do_next_thing
```
Exit 1 from `gc hook` short-circuits the `&&`; `do_next_thing` never runs.
This is a no-work-is-not-an-error semantic mismatch: the user wants
"check for work, then proceed regardless"; `&&` reads exit 1 as failure.

**Mitigation in tree (status-line.sh):**
`examples/gastown/packs/gastown/assets/scripts/status-line.sh:11` —
`w=$(gc hook "$agent" 2>/dev/null | grep -c . || true)`.
The `|| true` swallows the non-zero exit. Authors who write the workaround
know the trap; new callers don't.

### B. `set -e` in scripts

```sh
#!/bin/sh
set -e
gc hook
echo "did some bookkeeping"
```
Aborts after `gc hook` with no diagnostic. Any wrapper script that
defensively uses `set -e` and calls `gc hook` directly is a foot-gun.

### C. `pipefail` pipelines

```sh
set -o pipefail
gc hook | jq '.[0].id'   # entire pipeline exits 1; jq sees "[]" or "" first
```
Even without `set -e`, `pipefail` propagates the non-zero from `gc hook`
through the pipeline. `jq` may also error on empty input on top of that.

### D. Sequenced shell snippets that branch on `$?`

```sh
gc hook
[ $? -eq 0 ] && claim || handle_no_work
```
This *works correctly* — exit 1 is exactly "no work." But callers who
expected exit 0 with empty payload have to learn the convention; nothing
in the help text outside the `Long:` flag itself surfaces it at the
shell prompt unless the user runs `gc hook --help`.

### E. Tee-style consumers cannot distinguish "no work" from "real error"

```sh
gc hook 2>/dev/null
echo "exit=$?"
```
Both cases (work_query returned `[]` *and* work_query crashed/timed out)
yield exit 1. Without stderr, the caller has no way to distinguish them.
Stderr does carry the diagnostic, so the workaround is "don't
`2>/dev/null`," but several in-tree callers silently redirect stderr
(`status-line.sh:11`, `graph-dispatch.sh:266`) to avoid noise.

### F. Polecat work-discovery loops in prompt templates

Prompt scripts that bake `gc hook` into a discovery loop (e.g.
`gc hook || drain`) can drain the agent when work is simply absent. None
of the in-tree templates do this today — they call `gc hook` standalone
and read the JSON — but it's a foot-gun for community prompt authors who
follow the "find work, run it" idiom verbatim from the `Long:` help.

### G. Concrete Loomington touchpoints

| Caller | File | Behavior under exit-1 | Risk |
|---|---|---|---|
| Status-line agent counter | `examples/gastown/packs/gastown/assets/scripts/status-line.sh:11` | `\|\| true` swallows exit 1; `grep -c .` counts lines. With `[]` stdout, this counts as 1 line of work, which is the wrong sign. | Low. Pre-existing bug in tree, not caused by this stance — but a stricter `--exit-zero` semantic would let the script just `gc hook --exit-zero \| wc -l` and get a real count. |
| Integration test | `test/agents/graph-dispatch.sh:266` | `timeout 10 gc hook 2>/dev/null` — exit 1 propagates to the test runner under `set -e`. | Test-side defenses (the `||` pattern surrounding it) likely handle this; not verified end-to-end. |
| Doctor check | `cmd/gc/doctor_codex_hooks.go:132` | Uses `gc hook --inject`, not the non-inject path; doctor is unaffected. | None. |
| Polecat / graph-worker prompt templates | `internal/bootstrap/packs/core/assets/prompts/{pool-worker,graph-worker}.md` | Agents are told to "Run `gc hook`." Whatever the LLM does with the exit code is the LLM's problem; the framework doesn't compose `gc hook` with `&&`. | Low — agent-level, not script-level. |
| Mechanik nudge | `rigs/gc-toolkit/agents/mechanik/agent.toml:4` — "Check mail (gc mail inbox) and assigned work (gc hook), then act accordingly." | Same as above. | Low. |

The genuine, repeatable foot-gun is **idiomatic shell composition** (`&&`,
`set -e`, `pipefail`) by humans and prompt authors who reasonably read
"check for available work" as a question whose negative answer is *not*
an error. The framework's well-formed callers all either (a) skip the
non-inject path entirely, (b) wrap with `|| true`, or (c) parse JSON
directly. The "parallel commands fail" lament cited by the filer reads
as a reaction to A/B/C running into the contract for the first time,
not a structural failure of the framework.

## 3-options framework

Per `feedback_upstream_engagement.md` (Loomington memory; cited by the
bead). Each option below names a concrete next bead or explicitly says
why none is needed.

### Option 1 — Ignore (accept the behavior; document the workaround)

**Posture:** The contract is intentional, tested, byte-identical between
fork and upstream, and the workaround (`|| true`, or check `$?` against
0/1 explicitly) is well-known to shell authors. The status-line script
in the example pack already demonstrates the idiom.

**What "ignore" looks like in practice:**

- Add a short note to `engdocs/architecture/life-of-a-bead.md` § "The
  hook mechanism" calling out that non-inject `gc hook` returns 1 for
  "no work," symmetric to the existing `--inject` note. One paragraph,
  one bead.
- Add a one-line note to prompt templates that bake `gc hook` into a
  procedural step, so future authors don't reach for `&&`.
- Recommend `gc hook || true` (or `if gc hook; then …; fi`) in any new
  shell snippet that touches `gc hook`.

**Concrete next bead under this option:**
**lx-XXXXX** (file in gascity rig) — *"Document `gc hook` non-inject
exit-1-on-no-work contract in life-of-a-bead.md + prompt templates;
add `\|\| true` idiom note."* Read-only, doc-only, no code change.

**When this option is dominant:** when the cost of changing the contract
(test rewrites, caller audits) outweighs the cost of teaching every new
caller the convention. With the test surface listed above, this is the
case today.

### Option 2 — Local patch

**Posture:** Fork the binary, change the exit shape locally, wear the
divergence.

**What "local patch" looks like in practice:**

Two plausible patch shapes:
- **2a.** Change `doHook` to return 0 on "no work" while preserving stdout
  semantics (so callers branching on stdout content still work). This
  would also require rewriting `TestHookNoWork`,
  `TestHookNoReadyMessagePrintsButExitsOne`, the
  session-template-context test, and any other site that asserts
  `code == 1` for empty.
- **2b.** Add a non-breaking `--exit-zero` flag (default off) that
  converts the no-work path to exit 0. Mirror it as `GC_HOOK_EXIT_ZERO=1`
  env for shell-level opt-in. No test rewrites needed; new tests added.

**Cost:**
- 2a: high. Six tests touch the contract directly; downstream callers
  (status-line.sh, etc.) silently change semantics.
- 2b: moderate. Two-line `cmd.Flags()` addition, branch inside `doHook`,
  two new tests. Maintenance risk: every upstream merge that touches
  `cmd_hook.go` re-encounters the divergence.

**Concrete next bead under this option:**
**gc-XXXXX** (file in gascity rig) — *"Add `gc hook --exit-zero` (and
`GC_HOOK_EXIT_ZERO` env) for no-work-isn't-an-error callers; default
off, no behavior change for existing callers."* This is the "local
patch" with the smallest divergence footprint and the clearest opt-in
ergonomics. If chosen, propose this same change upstream first
(Option 3) and only land the local patch if upstream declines.

**When this option is dominant:** when a specific Loomington workflow
needs the exit-zero variant *now* and upstream is slow to decide. Not
the case from the bead's framing — operator was scoping the question,
not blocked.

### Option 3 — Engage upstream

**Posture:** File an issue against `gastownhall/gascity` raising the
"parallel-command composition" concern and proposing the `--exit-zero`
opt-in.

**What "engage upstream" looks like in practice:**

- File a `gastownhall/gascity` **issue** (not a PR — let maintainers
  steer), referencing:
  - the `Long:` doc string at `cmd/gc/cmd_hook.go:24`;
  - the test surface that locks the contract;
  - the failure modes A–E above with concrete shell snippets;
  - and proposing `--exit-zero` as a non-breaking opt-in (Option 2b
    shape).
- Cite the two Loomington symptoms (mechanik startup, codex-adhoc
  polecat) as concrete user-facing reports.
- Explicitly *acknowledge* the contract is intentional and tested; this
  is a request for an opt-in, not a bug report.

The framing matters: "make `gc hook` exit 0" reads as "drop the
contract", which an upstream reviewer will rightly reject. "Add
`--exit-zero` for callers that treat no-work as success" reads as
ergonomic surface area, which is a normal product question.

**Concrete next bead under this option:**
**lx-XXXXX** (file in this Loomington memory / coordination rig) —
*"File issue at gastownhall/gascity proposing `gc hook --exit-zero`
opt-in; cite cmd_hook.go:24 doc string, test surface, and the
parallel-command failure modes from gc-9czy7. Operator-gated per the
keeper protocol."* The bead's deliverable is "issue filed (or rejected
with reason)."

**When this option is dominant:** when the ergonomic complaint is real
and we'd accept upstream's answer either way. Given the contract is
clearly intentional, the most likely upstream outcome is "we'll consider
`--exit-zero`" or "won't fix; here's the recommended `|| true`
idiom" — both are useful answers.

### Recommended posture

**No option is unambiguously dominant; recommendation: Option 1 + Option
3 in sequence.**

- **Option 1 (Ignore + document) is the *immediate* dominant choice.**
  The contract is intentional, tested, and upstream-aligned. The fastest
  way to stop the foot-gun is a one-paragraph doc update plus a prompt
  template note. This is read-only, low-risk, and doesn't preclude any
  later move.
- **Option 3 (Engage upstream) is the dominant choice for the
  *ergonomic* gap.** If the operator wants `gc hook` to be safely
  composable with `&&` / `set -e` without a wrapping idiom, the right
  forum is an upstream issue proposing `--exit-zero`. The doc and
  the issue are independent — Option 1 lands today, Option 3 is filed
  whenever the operator wants to spend the upstream-engagement budget.
- **Option 2 (Local patch) is dominated** by Option 3 unless upstream
  declines. The maintenance cost of a fork-local exit-code patch is
  out of proportion to the benefit; the only justification is "upstream
  said no and we need it anyway."

**No follow-up bead is needed for Option 2 yet** — it becomes a candidate
only if Option 3 is filed and the upstream answer is "won't add."

---

*Doc prepared 2026-05-10 by `gascity/gc-toolkit.furiosa` (polecat) on
bead `gc-9czy7`, branch `polecat/gc-9czy7-gc-hook-exit-stance`.*
