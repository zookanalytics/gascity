---
name: providerledger convergence with upstream 11edccc17 already landed — Scope A is a no-op (gc-32x8j)
description: The prescribed convergence is already in main. Upstream's subprocess discharge is an ancestor of our own gc-q7wwq fix, which was authored on top of it and explicitly kept it. The conflict the bead reports is real only against the merge-base, not against main. Scope B's extraction recipe is corrected: 0271f8c3e cherry-picks onto upstream/main with both Go files byte-identical at its parent.
---

# gc-32x8j: the convergence already happened

## Verdict

**Scope A requires no code change.** Every element of its resolution intent is
already present on `origin/main` (`a67d2a21a`), and the tests that guard it
pass unmodified.

The bead's premise — that upstream `11edccc17` and our `gc-q7wwq` are competing
fixes still awaiting a hand-resolved merge — was accurate against the merge-base
`e6135a435`. It is not accurate against `main`. Upstream's commit is an ancestor
not just of `main` but of **our own fix**: `0271f8c3e` (gc-q7wwq) was authored
directly on top of `11edccc17` and deliberately preserved the subprocess
discharge. Its commit message says so:

> Keeps upstream's contract for `subprocess.NewSeamBacked` — upstream proved
> that composition rather than renewing its waiver, so the entry is dropped
> rather than carried.

The resolution the bead prescribes is a description of the state that already
exists.

## Evidence

Ancestry, checked directly:

```
git merge-base --is-ancestor 11edccc17 HEAD        # true
git merge-base --is-ancestor 11edccc17 0271f8c3e   # true
```

The decisive check is the parent tree. `0271f8c3e^:internal/testutil/providerledger/ledger.go`
already contains `provedRuntime(NewSeamBacked, …)` and carries 8 waivers, not 9 —
upstream had discharged the subprocess entry before our fix was written.

Each Scope A requirement against current `main`:

| Scope A requirement | State on `main` | Where |
|---|---|---|
| subprocess `NewSeamBacked` → `provedRuntime` | present, with upstream's allowed calls (`fmt.Sprintf`, `os.Getpid`, `sync/atomic.AddInt64`) | `internal/testutil/providerledger/ledger.go:164-171` |
| proved by upstream's verbatim test | `TestSubprocessDefaultDirSeamConformance` present, unmodified | `internal/runtime/subprocess/seam_conformance_test.go:36` |
| keep our per-entry `expires` signature | `waivedRuntime(constructor, reason, expires)` | `ledger.go:344` |
| keep staggered dates, drop the shared cliff | 8 waivers, 2026-10-19 → 2026-11-09, three days apart; no `runtimeWaiverExpiry` | `ledger.go:183-260` |
| `maxWaiverHorizon` bound retained | `90 * 24 * time.Hour` | `ledger.go:75` |
| stagger stays enforced with 8 entries | `TestCatalogWaiverExpiriesAreStaggered` passes | `ledger_test.go:1590` |
| re-index `first[N]` against **our** catalog | `first[3]` — correct (0 fake, 1 fail, 2 subprocess, 3 acp, whose `Claims[0]` is the waiver) | `ledger_test.go:1646` |
| `TESTING.md` table in sync | `TestCatalogMatchesProductionWiringAndDocumentation` passes; it reads the file | `ledger_test.go:1544`, `:1575` |

Verification run in the worktree:

```
go test ./internal/testutil/providerledger/                → ok (2.037s)
go test -tags integration -run TestSubprocessDefaultDirSeamConformance \
        -count=1 ./internal/runtime/subprocess/            → ok, 34 cases PASS
go vet -tags integration ./internal/runtime/subprocess/    → clean
```

No commit after `11edccc17` reverted any part of it on the four affected paths;
the only later commit touching the two ledger files is `0271f8c3e` itself.

## Two corrections to the bead's own description

Recorded because they explain the false alarm and would otherwise be re-derived:

1. **"ledger.go CONFLICTS."** True — but the `git show 11edccc17 | git apply --check -3`
   survey was run against the merge-base, where upstream's commit is genuinely
   unapplied. Against `main` the question does not arise, because the commit is
   already in history. A conflict check is only meaningful against the tree you
   intend to apply to.

2. **"staggered all 9 runtime.Provider waivers Oct 16 - Nov 9."** The *merged*
   gc-q7wwq staggers **8**, 2026-10-19 → 2026-11-09. The bead describes a
   pre-merge draft of our own fix, written before the subprocess entry was
   dropped in favour of upstream's proof. The dropped entry is exactly the one
   Scope A asks to drop.

## Scope B: corrected extraction recipe

Scope B (offer the staggering upstream) remains live and remains operator-gated —
nothing below was executed. But its recipe is now simpler than the bead states,
and the correction is worth having before `mol-upstream-gc-pr-prep` runs.

The bead says the extractable change is "gc-q7wwq's ledger.go + ledger_test.go
diff vs merge-base `e6135a435`". Reconstructing that diff is unnecessary.
`0271f8c3e` is a single self-contained commit whose **parent tree is byte-identical
to current `upstream/main` (`1534d46e2`) on both Go files**:

| Path | `0271f8c3e^` | `upstream/main` | |
|---|---|---|---|
| `internal/testutil/providerledger/ledger.go` | — | — | **identical** |
| `internal/testutil/providerledger/ledger_test.go` | — | — | **identical** |
| `TESTING.md` | `60eea6bde` | `a25ad1c29` | differs |

So the extraction is a straight `git cherry-pick 0271f8c3e` (or `format-patch -1`)
onto `upstream/main` — no merge-base diffing, no conflict on the Go files.

Two notes for whoever opens it:

- **`TESTING.md` should be re-rendered, not applied.** It differs only because
  the fork landed three unrelated `TESTING.md` commits (`de63a623f`,
  `054092819`, `dfcce4c06`) afterwards. The hunk in `0271f8c3e` is confined to
  the generated ledger table between the `providerledger` markers, which is
  produced from the Go ledger. Apply the Go change, then let `CheckMarkdown`
  print the exact replacement block on drift (`ledger.go:742`).
- **The dates are absolute and remain legal.** The stagger runs 2026-10-19 →
  2026-11-09 against a 90-day `maxWaiverHorizon`, so the furthest date is valid
  for any merge on or after 2026-08-11. An upstream merge anywhere in
  Aug–Oct 2026 needs no date adjustment.

Upstream still carries the single-date cliff this fixes: `upstream/main` has
`runtimeWaiverExpiry = 2026-08-26` at `ledger.go:344`, with all eight remaining
waivers pointing at it. Upstream's own comment concedes the debt and asks that
the next renewal say so, which makes the pre-2026-08-26 window the natural one.
