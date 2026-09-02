---
title: Reclaim Finished Worktrees
description: Remove the per-bead git worktrees left behind by finished work before they fill the disk.
---

## Overview

Every bead an agent works on gets its own git worktree under
`.gc/worktrees/<rig>/`. The worktree outlives the bead: closing the bead
does not remove the checkout, so a busy city accumulates one full copy of
the repository per finished bead, indefinitely. A few dozen beads a day
against a large repo is tens of gigabytes a week.

`gc worktree reap` classifies those worktrees and removes the ones that are
finished and safe to remove.

## Why it matters — the disk-full cascade

Finished worktrees share a disk with the toolchain's build and link scratch
space, so the accumulation surfaces first as build failures that look like
code failures:

```
/usr/local/go/pkg/tool/linux_amd64/link: cannot write
  /var/tmp/go-link-1291483678/000040.o: copy_file_range: no space left on device
FAIL github.com/example/project [build failed]
```

Nothing in that output names the disk as the cause, so the natural reading
is that the change under test broke the build. It did not: the same commit
passes once space is free. Worse, a store that mmaps its writes can enter a
retry loop that does not recover when space is returned — see
[Clean Up bd Auto-Backups](/troubleshooting/bd-backup-cleanup) for that
failure mode.

## Inspect

Run the command with no flags. It classifies every worktree and removes
nothing:

```bash
gc worktree reap
```

```
would reap  myrig  ab-1o5wz  /home/me/city/.gc/worktrees/myrig/worker-1/worktrees/ab-1o5wz
protected   myrig  ab-33l2e  /home/me/city/.gc/worktrees/myrig/worker-1/worktrees/ab-33l2e  live: process 41207 cwd
1 reclaimable, 1 protected. Nothing removed — re-run with --apply to reclaim.
```

Add `--json` for the same verdicts as machine-readable output, including the
reason each protected worktree was held.

## Reclaim

```bash
gc worktree reap --apply
```

Removal deletes the checkout only. Commits stay reachable through their
branches, and pushed branches are untouched on the remote, so a reaped
worktree reproduces with `git worktree add <path> origin/<branch>`.

## What is protected, and why

Every gate fails closed: an answer the command cannot determine protects
the worktree rather than removing it.

| Gate | A worktree is protected when |
|---|---|
| Bead status | its bead is still open, or cannot be read |
| Freshness | it is newer than `auto_reap_closed_bead_worktrees_min_age_minutes`, or its age cannot be determined |
| Reference | any unfinished bead still names its path as a working directory |
| Liveness | a live process or an open session is working in it — or the process scan was unavailable, which protects every worktree in the pass |
| Git state | it has uncommitted changes, or commits reachable from no branch, tag, or remote |
| Agent home | it is an agent's home directory rather than a per-bead worktree |

Stashed work is deliberately not a gate. `refs/stash` is a single
repository-wide ref that carries no worktree identity, so `git stash list`
answers identically in every worktree of a repository — one stash anywhere
would protect every worktree in the rig — and removal leaves the stash
intact in any case.

## Automate it

The orchestrator can run the same classification on every reconcile pass.
Enable the dry run first, watch the `bead.worktree.reap_skipped` events with
`gc events` until you are satisfied that nothing live appears in the
would-reap set, then switch to real removal:

```toml
[daemon]
# Stage one: classify and report, remove nothing.
auto_reap_closed_bead_worktrees_dry_run = true

# Stage two: remove. Supersedes the dry run when both are set.
auto_reap_closed_bead_worktrees = true

# How long a worktree is exempt after creation. Defaults to 10.
auto_reap_closed_bead_worktrees_min_age_minutes = 10
```

`gc worktree reap` works whether or not these are set, so an operator can
reclaim space without changing the city's configuration.
