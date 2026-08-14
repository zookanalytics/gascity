---
name: TEMP commit 705be0ff6 is absorbed upstream, but the drop belongs to the next upstream sync (gc-qnmgk)
description: Read before the next sync onto upstream/main. Records that the bead's trigger condition fired, that our TEMP commit and upstream's 40689650a are the same patch byte-for-byte, and why the commit is still load-bearing on main until upstream's version actually arrives.
---

# gc-qnmgk: the trigger fired; the drop belongs to the next upstream sync

## Verdict

**The trigger condition is met, and there is no code change to make now.**

Upstream PR #5247 merged as `40689650a`, so the bead's precondition — "once
upstream PR #5247 merges" — is satisfied. But its prescribed action, dropping
`705be0ff6`, **cannot be executed as a standalone change on `main` today, and
must not be attempted as one.** `origin/main` does not yet contain
`40689650a`; until it does, our TEMP commit is the only thing supplying the
fix, and reverting it re-breaks `go vet ./cmd/gc` at head.

That is not a deferral of the bead — it is what the bead says. The description
scopes the action to "the next upstream sync," and the sync is where it
happens. This document exists so that sync does not have to re-derive any of
it: the survey verdict is settled (**`drop-merged-upstream`**), the identity
proof is recorded, and the one way to get this wrong is named.

## Evidence

### 1. The trigger fired

Upstream's fix is merged and live on `upstream/main`:

```
$ git log -1 --format='%cI %H' 40689650a
2026-08-13T17:38:49-07:00 40689650a8601d55bacf70f2a7f4ebfd2544fc44
    fix(cmd/gc): unbreak the cmd/gc test build after the order-dispatcher
    routes change (#5247)

$ git merge-base --is-ancestor 40689650a upstream/main   # true
```

### 2. Our commit and upstream's are the same patch, byte for byte

This is the decisive fact, and it is stronger than "same intent":

```
$ git show 705be0ff6 | git patch-id --stable
042bb76732aba26520d5bcac31662bc0fcfec0e4 705be0ff6...
$ git show 40689650a | git patch-id --stable
042bb76732aba26520d5bcac31662bc0fcfec0e4 40689650a...
```

Identical patch-IDs. Both commits change the same two files by the same three
lines — one added `nil` argument per call site — which is exactly what
`705be0ff6`'s own message claims ("Verbatim cherry-pick of upstream PR #5247").
The claim is verified, not taken on faith.

Because the patches are identical, whichever replay mechanism the next sync
uses, the resulting tree is the same. There is no merge judgment left to make.

### 3. `origin/main` does not have upstream's commit yet

```
$ git merge-base --is-ancestor 705be0ff6 origin/main    # true  — ours is on main
$ git merge-base --is-ancestor 40689650a origin/main    # false — upstream's is not
```

`origin/main` (`3dcdc66e6`) is 47 commits ahead of and 11 commits behind
`upstream/main` (`5a600ec65`). The sync that closes that gap has not run.

The bead description's own verification command still returns the TEMP commit,
and will keep returning it until the sync lands — that output is the
outstanding-work signal, not a failure:

```
$ git log --oneline upstream/main..origin/main | tail -1
705be0ff6 TEMP: unbreak the cmd/gc test build after the order-dispatcher routes change
```

### 4. Why reverting today would break the build

On `origin/main` the routes parameter is present but upstream's call-site fix
is not — that combination is precisely what `705be0ff6` repairs:

```
cmd/gc/order_dispatch.go:436:
    func newMemoryOrderDispatcher(routes *storageRoutes, aa []orders.Order,
        cityPath string, cfg *config.City, rec events.Recorder,
        stderr io.Writer) *memoryOrderDispatcher
```

Six parameters. The three test call sites pass six arguments only because the
TEMP commit added the leading `nil`:

| Call site | At head | After a revert |
| --- | --- | --- |
| `cmd/gc/order_dispatch_test.go:2017` | `(nil, aa, cityDir, …)` | `(aa, cityDir, …)` |
| `cmd/gc/order_dispatch_bench_test.go:93` | `(nil, aa, cityDir, …)` | `(aa, cityDir, …)` |
| `cmd/gc/order_dispatch_bench_test.go:113` | `(nil, aa, cityDir, …)` | `(aa, cityDir, …)` |

Five arguments to a six-parameter function is a compile error, so a revert
ahead of the sync reintroduces the exact failure upstream recorded in #5247:

```
vet: cmd/gc/order_dispatch_bench_test.go:93:78: not enough arguments in call to
newMemoryOrderDispatcher
    have ([]orders.Order, string, *config.City, events.Recorder, io.Writer)
```

`go vet ./cmd/gc/` is clean at `3dcdc66e6` with the TEMP commit in place.

## What the next upstream sync should do

Nothing beyond letting `mol-upstream-gc-rebase` run. The survey verdict is
already determined — this is a textbook **`drop-merged-upstream`**, the
formula's own term for a commit that is patch-id-equal to one on
`upstream/main`, and the `dropped-absorbed` case in the rebase-conventions
prose. The survey row, ready for the verdicts table:

```
| sha | subject | verdict | rationale |
|-----|---------|---------|-----------|
| 705be0ff6 | TEMP: unbreak the cmd/gc test build after the order-dispatcher routes change | drop-merged-upstream | matches upstream 40689650a by patch-id (042bb7673) |
```

The rebase step then needs **no intervention at all**: `drop-merged-upstream`
commits are dropped automatically by patch-id matching, and this one is an
exact match rather than a "trivially-equal" judgment call. It should never
reach a conflict, so it should never be given a `git rebase --skip` — if it
somehow halts, that is a signal something else changed, not a cue to skip past
it.

"Drop the whole commit" costs nothing here. The commit is two files and three
lines, all of them the same `nil` argument. Nothing rides along: no bundled
refactor, no test written against a now-fixed upstream bug, nothing with
independent value worth filing separately.

After the sync, confirm the fix arrived from upstream rather than from us:

```bash
git merge-base --is-ancestor 40689650a HEAD    # expect true
git log --oneline upstream/main..HEAD | grep TEMP   # expect no output
git grep -n "newMemoryOrderDispatcher(nil" -- cmd/gc/   # expect the 3 call sites
go vet ./cmd/gc/
```

The first two are the ones that decide it: together they say the fix in our
history is upstream's and ours is gone. If either fails, the drop did not
happen and this document's premise needs rechecking.

(gc-qnmgk itself closes when *this* document merges — it is an implementation
bead the refinery closes on merge, so do not expect to find it still open at
sync time. The checks above verify the drop, not the bead.)

## What must not happen

- **Do not revert `705be0ff6` on `main` ahead of the sync.** It looks like the
  bead's instruction and it breaks the build — see Evidence §4. The commit is
  redundant with upstream *in content* but not yet *in our history*.
- **Do not author a fork-local equivalent** of upstream's fix. The standing
  lens on gc-b0pmq prefers an existing upstream commit over our own fix for the
  same defect at near-zero rebase cost, and here the cost is zero: the patches
  are identical.
- **Do not preserve any part of the commit** through the sync. It has no
  reason-to-exist once `40689650a` is in our history.
