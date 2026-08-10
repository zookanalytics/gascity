---
name: census-owner-liveness dangling refs — measurement error, not ledger rot
description: Why the 8 dangling owner_bead findings in test/test-resources.toml were already cleared by PR#109, why the follow-up count was measured with a stale gc binary, and why re-pointing those 8 rows must never be done.
---

# gc-ytmda: the 8 dangling `owner_bead` refs were already fixed

## Verdict

**No code change. No ledger change.** The condition gc-ytmda tracks was
already cleared by PR#109 (`f538105cd`). The follow-up count that appeared
to disprove the fix was produced by a `gc` binary built five days before
that merge.

The bead's remaining-work item 1 — *"re-point (or retire) the 8 dangling
owner_bead rows"* — **must not be executed.** It would corrupt correct
upstream provenance. See [Why re-pointing is destructive](#why-re-pointing-is-destructive).

## The measurement

Same rig, same ledger, same bead store, same working directory. The only
variable is which `gc` binary runs the check:

| Binary | Built | `census-owner-liveness` |
|---|---|---|
| `/home/zook/go/bin/gc` (installed, on `PATH`) | 2026-08-05 05:44 | `warning: found 8 dangling owner_bead reference(s)` |
| `go build ./cmd/gc/` at `f538105cd` | 2026-08-10 | `ok: no dangling owner_bead references found` |

PR#109 merged at `2026-08-10T12:49:49-06:00` (18:49:49Z). The installed
binary predates it by five days, so it still carries the pre-PR#109 check
and cannot see PR#109's fix. The re-check recorded on gc-ytmda ("Verified
18:52Z, after the merge") ran that stale binary: it was three minutes after
the merge, but the merge had not been built or installed.

## What PR#109 actually fixed

PR#109 was scoped to the checker, and that was the correct place to fix it.
It added the foreign-namespace filter — `censusOwnerBeadIsLocal`,
`liveBeadPrefixes`, `normalizeCensusBeadPrefix` — which drops ledger rows
whose `owner_bead` sits in a bead namespace this city does not own, before
the store is ever consulted.

That filter is precisely what these 8 rows needed. gc-ytmda read the
PR as "improved the CHECK but re-pointed no ledger row", treating the
absence of a ledger edit as an incomplete fix. The ledger was never wrong;
the checker was.

## Why the 8 rows are correct

All 8 ids are `ga-80po0c.*`. `ga-` is **upstream** gascity's bead prefix
(`gastownhall/gascity`), not this city's. This city's configured prefixes
are `tk`, `sl`, `gc`, `su` plus the HQ prefix; the gascity rig itself mints
`gc-`. Upstream's own branches are named `upstream/builder/ga-*`.

Every row arrived with a vendored upstream commit, all authored upstream and
all ancestors of `upstream/main`:

| `owner_bead` | Introduced by |
|---|---|
| `ga-80po0c.2` | `9b6d91e17` — test: add checked test resource census (#4218) |
| `ga-80po0c.2.1` | `0f9709fc0` — test: enforce exact Medium resource ownership (#4227) |
| `ga-80po0c.2.2` | `d36a8ccad` — test: ratchet httptest server resource debt (#4228) |
| `ga-80po0c.2.2.1` | `1d24deb01` — test(policy): enforce exact tmux resource ownership (#4571) |
| `ga-80po0c.2.2.2` | `2faba0414` — test(policy): classify typed listener resources (#4573) |
| `ga-80po0c.2.2.3` | `97e1cb527` — test(policy): ratchet listener-owning helpers (#4599) |
| `ga-80po0c.2.3` | `76d631575` — test: ratchet cmd/gc process-global resource debt (#4223) |
| `ga-80po0c.23.1` | `b7d312eb5` — test: replace Docker session waits with strict protocol contracts (#4344) |

These ids resolve in upstream's bead store and can never resolve here — by
construction, not by rot. Every downstream city that vendors this repo
inherits the same ids and would see the same finding.

This also explains the growth gc-ytmda flagged as evidence of accumulating
rot ("the set grew 5 → 8 between 07-27 and 08-09"). The three newest ids
entered via upstream ratchet PRs #4571, #4573 and #4599. The set grows
because upstream keeps ratcheting its census, not because local references
are decaying.

## Why re-pointing is destructive

The checker's own doc comment names this repair as the one to avoid, and
`cmd/gc/doctor_census_owner_liveness.go` says so in the type comment:
reporting foreign ids "would invite the one repair that is actually
destructive: re-pointing correct upstream-authored rows."

Re-pointing these 8 rows at local `gc-` beads would:

1. **Destroy correct attribution.** Each row records which upstream bead
   owns that resource debt. A local id makes the ledger claim ownership
   this city does not hold.
2. **Create a permanent upstream conflict.** `test/test-resources.toml` is
   actively maintained upstream — it has changed in 20+ upstream commits.
   Rewriting owner fields guarantees a conflict on every future rebase, on
   a file the fork has no reason to own. That runs directly against the
   `AGENTS.md` rule to keep `upstream/main` easy to merge.
3. **Fix nothing.** The finding is already gone on current `main`.

## What actually remains

One operational action, outside this repo's diff: **the installed `gc`
binary on the city host is stale.** Until it is rebuilt and reinstalled,
every agent running `gc doctor` will keep seeing the 8 findings, and the
next observer will file this bead a fourth time. That is an operator
decision — reinstalling changes behavior for every agent on the host — so
it is flagged, not performed here.

## Correction for the `tk-fwspr` pattern

gc-ytmda records this as a third instance of "referenced fix beads closed
but findings persist", and proposes a close-time gate: a bead naming a
doctor finding should not close while that finding still fires.

**This instance is not that pattern.** The fix landed and the finding
cleared; the re-check was wrong. A close-time doctor gate would have run
the same stale binary, seen the same 8 findings, and held `gc-ddvrx` open
against a fix that had already worked — converting a measurement error into
a permanent block.

If that gate is still wanted, it needs to pin the binary it measures with —
build from the merge commit under test, rather than invoking whatever `gc`
happens to be on `PATH`. Verification is only as current as the binary
doing it.
