---
name: Why agent cost telemetry was blind to the patrol tier (gc-kawr5)
description: Root-cause analysis of the two independent defects that made gc.agent.invocation.cost_usd omit every always-on agent and gc.agent.tokens.* flatline for them, verified against production usage facts, plus the fix and what remains uncovered.
---

# gc-kawr5: the patrol tier was unmeasured, and the metrics said $0.00

## Verdict

Two **independent** defects, either of which alone is sufficient to erase an
agent from cost telemetry. Defect 1 is fixed here; defect 2 was fixed upstream
while this work was in flight (see *Defect 2 landed upstream* below).

1. **The pricing table stopped at the Claude 4.x family.** Every patrol agent
   runs `claude-sonnet-5`, which had no entry. Cost is *skipped* rather than
   zero-filled for an unpriced pair, so those agents produced no cost datapoint
   at all — not a zero, an absence.
2. **Model-usage sweeps only ran for sessions that stop.** The end-of-interval
   sweep is gated on a compute-terminal state. An always-on agent never reaches
   one, so it was never swept; its only samples came from whatever the prompt-op
   seam caught at a nudge.

The reported symptoms map cleanly onto these: defect 1 explains "no refinery,
witness, deacon, mayor, keeper or control-dispatcher ever appears in
`cost_usd`", and defect 2 explains "`tokens.*` covers patrol agents but the
series are flat and then stop".

## Evidence

Both were confirmed against the live city rather than inferred from code.

**Defect 1.** Grouping 24h of `kind=model` facts in `.gc/usage.jsonl` by
worker class, priced-ness, and model:

```
  58 pool    priced    claude-opus-4-8
  38 pool    UNPRICED  claude-opus-5
   8 patrol  UNPRICED  claude-sonnet-5
```

Every patrol fact carries `unpriced=true` and `cost_usd_estimate=0`. The
registry matches `(provider, model)` **exactly** — no alias or prefix fallback —
so `claude-opus-4-8` being present did nothing for `claude-opus-5`. Note this
was never a patrol-only problem: pool workers on `claude-opus-5` were equally
uncosted; the patrol tier was simply the group where it was total.

**Defect 2.** `gc session list --state all` shows the entire patrol tier
(`refinery`, `witness`, `deacon`, `mayor`, `mechanik`, `control-dispatcher`)
sitting at `state=active` permanently, while polecats and converse sessions
cycle through `asleep`. The set of agent names that *did* appear in `cost_usd`
over 24h — polecats and converse sessions — is exactly the set that reaches a
terminal state. The correlation is total, and it is the sweep gate:
`computeFactGetCandidate` requires `isComputeTerminalState`.

The volume gap makes the same point: patrol agents produced 8 model facts in
24 hours of continuous running, against 22 for a single polecat session.

## Why the metric read as "free" rather than "broken"

This is the part worth keeping. Cost is deliberately skipped, not zero-filled,
for an unpriced pair — the reasoning being that a fabricated zero is worse than
no data. That is right, but it was only half-implemented: skipping the datapoint
removed the wrong answer without producing any signal that a right answer was
missing. Downstream, "no series for this agent" and "this agent cost nothing"
render identically, so a query answers `0 calls / $0.00` with total confidence.

The fix adds `gc.agent.invocation.unpriced`, emitted at the same seam that would
have recorded cost and carrying the same labels. A missing pricing entry now
announces itself, and names the model it is missing for. This is the durable
half of the fix: the pricing rows added here will themselves be stale within a
model generation.

## The fix

- **`internal/pricing/defaults.go`** — entries for `claude-opus-5`,
  `claude-sonnet-5`, `claude-fable-5`, `claude-mythos-5`, `claude-opus-4-6`,
  and the undated `claude-haiku-4-5` alias. Sonnet 5 is priced at $2/$10,
  which is the standard rate: it launched as introductory pricing through
  2026-08-31, and the scheduled 2026-09-01 increase to $3/$15 was cancelled
  on 2026-08-10, so no step-up is pending.
- **`gc.agent.invocation.unpriced`** — new counter, wired into both emission
  seams (prompt-op finish and the model-usage sweep).
- **`agent-token-telemetry` doctor check** — reports awake sessions with no
  recorded model usage for over an hour, and distinguishes one silent session
  from every awake session going silent at once (the shared-cutoff shape, which
  points at the emission path rather than at an agent).

## Defect 2 landed upstream

This branch originally carried an in-interval sweep for defect 2. Upstream
landed its own — `ad4d0ab4a`, "fix(usage): sweep model usage from live sessions,
not only at retirement" (#4994) — while this branch sat in the pre-open gate,
and the fork rebase brought it into `main`. That implementation is strictly the
better of the two: it routes off the freshly-read bead rather than the tick's
snapshot, so a session that woke since the snapshot is swept in the same pass;
it skips the live lane on the boot reconcile, keeping a whole-fleet transcript
scan off the synchronous readiness path; and it floors re-sweeps at 30s rather
than carrying a persisted `usage_model_live_swept_at` marker, which drops a
`session.Info` field and a metadata key from the schema entirely. Its test suite
is a superset of this branch's, adding throttle, boot-pass, discovery-back-off
and settled-miss coverage.

So the live-sweep half of this work was **dropped whole** rather than merged or
reconciled: the implementation, its three tests, the `Info.UsageModelLiveSweptAt`
field and its codec entry are all gone from this branch. Only defect 1's fix,
the `unpriced` counter and the doctor check remain — none of which upstream has,
and none of which depend on which live-sweep implementation is underneath.

The analysis of defect 2 above is left standing because it is still the correct
diagnosis and it is what the doctor check below is built around; only the fix
for it moved.

## Two things considered and rejected

These were live design questions for the in-interval sweep this branch no longer
carries. They are kept because the same trade-offs apply to upstream's lane, and
because rejected alternatives are the part of an analysis that is expensive to
rediscover.

**Clamping the live lane's discovery window** to a recent lookback, to bound the
codex rollout scan. Rejected: a rollout's filename timestamp is its *first*
start, so a narrow window excludes the transcript of precisely the long-awake
sessions the lane exists to measure. The unclamped window is identical in width
to the one the terminal lane would use when the session finally sleeps.

**Decoupling metric emission from the usage-fact sink.** Both lanes are gated on
a configured sink, so a city running `[usage] provider = "discard"` still gets
only the prompt-op seam's metrics. That gate is inherited — the sweep's OTel
emission is documented as a mirror of its fact emission — and unpicking it is a
separate change with its own blast radius.

## What remains uncovered

- **The 02:45:00Z cutoff shared by 15 agents** is consistent with a controller
  restart resetting cumulative counters, but that was not proven here. The
  in-interval sweep makes the series self-healing either way: emission resumes
  within one cadence of any restart rather than never.
- **The bounded tail ceiling still applies.** Each sweep recovers only the
  extractor's 64KB transcript tail, so a very busy 10-minute window can still
  drop its earliest invocations. Widening that re-opens the unbounded-scan and
  misattribution risks the bound exists to prevent.
- **Runtime-only sessions** remain permanently out of scope (ga-tkvb31).
