---
name: Keyed Claude transcripts filed outside the session's work_dir folder (gc-ysf8tb)
description: Why some sessions' Claude transcripts sit under a project folder other than their bead's work_dir, how the keyed lookup finds them, why the stale-resume probe still asks only about the launch folder, and which open beads own the rest.
---

# gc-ysf8tb: keyed Claude transcripts outside the work_dir folder

## Verdict

Claude files a session's transcript at
`<projects root>/<slug of the directory the session started in>/<session key>.jsonl`.
The keyed lookup looked only under the slug of the session bead's `work_dir`.
A session that started anywhere else was never found, so its model-usage sweep
never settled and the supervisor's vanished-session lane retried it on every
pass.

The keyed lookup now looks in every project folder for a UUID key when the
`work_dir` folders miss. The stale-resume probe keeps the folder-scoped lookup.
Recording where a session started is gc-07kwpp.

## Where the stuck sessions started

Read on 2026-10-10 from the session beads and `/Users/gc/.claude/projects`.
Paths are under `/Users/gc/Code/loomington/.gc/worktrees/`. Each session key
matches exactly one transcript, and every session uses `wake_mode = fresh`.

| Session | Epoch | `work_dir` | Directory of the first transcript record |
|---|---|---|---|
| lx-wisp-frwq4 | 3 | `gascity/polecats/gc-toolkit.polecat-3` | `<work_dir>/worktrees/gc-8g3jj` |
| lx-wisp-j2soq | 2 | `gascity/polecats/gc-toolkit.polecat-5` | `<work_dir>/worktrees/gc-stxs1f` |
| lx-wisp-fxx7f | 2 | `gc-toolkit/polecats/gc-toolkit.polecat-3` | `<work_dir>/worktrees/tk-w76qch6` |
| lx-wisp-8rnv1 | 4 | `gascity/polecats/gc-toolkit.polecat-3` | `<work_dir>/worktrees/gc-l7u6ad` |
| lx-wisp-61zxu | 2 | `gascity/polecats/gc-toolkit.polecat-1` | `<work_dir>/worktrees/gc-dz7j6r` |
| lx-wisp-c1bpi | 6 | `gascity/polecats/gc-toolkit.polecat-2` | `<work_dir>/worktrees/gc-92bf3j` |
| lx-wisp-vd700 | 2 | `gascity/polecats/gc-toolkit.polecat-5` | `<work_dir>/worktrees/gc-5cq3k3` |
| lx-wisp-xri6c | 1 | `gascity/polecats/gc-toolkit.polecat-1` | `gascity/polecats/gc-toolkit.polecat` |

### Started in the held bead's worktree

The first seven are later wakes of pool sessions. Each started in the per-bead
worktree under its `work_dir`. `buildPreparedStartWithWorkDirResolver`
(`cmd/gc/session_lifecycle_parallel.go`) replaces the launch directory with a
task work_dir when one resolves (`resolvePreparedTaskWorkDir`). Outside a
drain, that is the `work_dir` of an in-progress bead assigned to the session,
read from the reconciler's snapshot (`newAssignedTaskWorkDirResolver`) or else
from the store (`resolveTaskWorkDir`), both in `cmd/gc/session_reconciler.go`.
The choice is not written back to the session bead. A pool session that wakes while it holds a
bead therefore starts in that bead's worktree, and Claude files the new
conversation there.

lx-wisp-frwq4 shows the timing. Its interval started at 07:10:08Z on
2026-10-09, and its transcript's first record, at 07:10:10Z, carries
`<work_dir>/worktrees/gc-8g3jj` as its cwd. Its bead's `work_dir` and
`gc.work_dir` both name `gascity/polecats/gc-toolkit.polecat-3`.

### Started in the pool template's directory

lx-wisp-xri6c started at 05:15:55Z on 2026-10-09 in the pool template's
directory, not a slot's. Its bead records the template name as
`canonical_instance_name` and in `alias_history`, and it names slot 1 in
`agent_name`, `pool_slot` and `work_dir`. Other sessions of the pool record
their slot as `canonical_instance_name` and carry no alias history. The session
was created under the template identity and re-identified as slot 1 after it
started. The write that moved its `work_dir` is not identified; gc-wye0uw
tracks it.

## The lookup

`sessionlog.FindSessionFileByID` checks the project folders derived from
`work_dir` first. When they miss and the key is a canonical UUID, it checks
`<root>/<folder>/<key>.jsonl` for every folder under each search root. That
costs one directory read per root and one stat per folder, and this host has
389 project folders. The earliest root that holds the file wins, and within it
the newest copy. A key that is not a UUID stays within the `work_dir` folders,
because only a UUID names one session wherever its file sits. gc mints session
keys as version 4 UUIDs (`session.GenerateSessionKey`).

`sessionlog.FindSessionFileByIDInWorkDir` is the folder-scoped lookup on its
own.

Every transcript reader reaches the wider lookup through
`transcript.DiscoverKeyedPath`. The readers are transcript resolution for the
usage sweep and session history (`TranscriptPathClassified`), `gc session
logs`, the API's keyed transcript paths, the worker's sessionlog adapter, and
the transcript-metadata sidecar pass. With it the sweep reads these sessions'
transcripts and settles, so the vanished-session lane stops retrying them.
`TestFactorySweepSessionModelUsageClaudeTranscriptOutsideWorkDirFolder` shows
the sweep settling.

## The stale-resume probe

The probe (`HasKeyedTranscript`, behind `staleResumeKeyProbe` in `cmd/gc`)
asks whether a resume started in the launch directory reattaches to the
session's transcript. When the answer is no, it clears the session key, so the
next start opens a new conversation instead of a resume that dies on launch. It
receives the launch directory after the task override (`agentCfg.WorkDir`), and
it keeps the folder-scoped lookup.

Claude Code 2.1.296, the installed version, resolves `claude --resume <id>` in
three steps, read from the source bundled in its binary:

1. It loads `<id>.jsonl` from the starting directory's project folder.
2. Failing that, it tries the project folders of the repository's other git
   worktrees, from `git worktree list --porcelain`.
3. Failing that, it scans every project folder and accepts a match only when
   exactly one folder holds the file.

Otherwise it exits with "No conversation found with session ID". Older versions
were not checked.

A transcript filed under another folder is therefore resumable only through
fallbacks that have conditions of their own. Counting it would let the probe
keep a key that a resume may not reach, which is the failure the probe exists to
prevent. Not counting it costs at most a new conversation.
`TestKeyedClaudeTranscriptInAnotherProjectFolder` pins both answers.

## Owned elsewhere

- gc-07kwpp records the directory a session started in (`worker_dir`) at start.
  The codex usage sweep already reads `worker_dir` before `work_dir`
  (`contract.WorkerDirFromMetadata`) and matches a rollout's `session_meta` cwd
  exactly, so that change also covers a codex session started in a task
  work_dir. None of the 139 codex rollouts dated 2026-10-08 and 2026-10-09 on
  this host started in a per-bead worktree. With `worker_dir` written, the
  Claude lookup can look in the launch folder first and skip the scan.
- gc-wye0uw covers the template-identity start above.
- gc-oh4e2p bounds the vanished-session lane's retries and keys
  `sameWorkDirSessionBeads` on `work_dir`.
- gc-7zhki2 widens the stale-resume probe's search roots.
