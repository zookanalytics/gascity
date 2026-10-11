---
name: A running pool session's work_dir across a cap change (gc-wye0uw)
description: Traces how pool session lx-wisp-xri6c came to run under its pool's template identity and which two writes moved it to slot 1 while it ran. Records the decisions that a pool hands out the template identity only while its cap is one, that a running session is not restarted when its identity moves, and that its recorded work_dir keeps naming the directory its process runs in until it stops. Also records the cap-lowering case the change does not cover.
---

# gc-wye0uw: a running pool session's work_dir across a cap change

## What happened

Times are UTC on 2026-10-09. Sources: the session bead's write history
(`gc bd history lx-wisp-xri6c --events`), the supervisor log, and the city
repo's history of `city.toml`.

| Time | Event |
|---|---|
| 05:10:17 | City commit 124cc03 drops the host-migration freeze patch that held the gascity polecat pool at 0 sessions. A second patch, `max_active_sessions = 1`, stays in force. |
| 05:11:36 | lx-wisp-9f7hn is created under the template identity `gascity/gc-toolkit.polecat`, in the template's directory. It drains at 05:15:39 unchanged. |
| 05:15:45 | lx-wisp-xri6c is created under the same template identity. It starts at 05:16:03 in `.../polecats/gc-toolkit.polecat`. |
| 05:21:29 | City commit 0385848 removes the cap, so the pool returns to its pack default of 5. |
| 05:22:52 | Write 1 moves the session to slot 1: `agent_name` becomes `gascity/gc-toolkit.polecat-1`, `pool_slot` becomes 1, `alias` is cleared and the template name moves to `alias_history`, and the drift hashes are rebaselined. `work_dir` does not change. |
| 05:33:40 | The session claims gc-7s8mo itself (`current_claim_bead_id`). |
| 05:35:04 | Write 2 sets `gc.trigger_bead_id` from gc-tubh4r to gc-7s8mo and moves `work_dir` and `gc.work_dir` to `.../polecats/gc-toolkit.polecat-1`, in one write. |
| 05:36:08 | The session starts draining. It closes at 05:38:23. |

The supervisor log shows two polecat wakes under the template name, at about
05:11:56 and 05:16:01. They are these two sessions.

## Where the template identity came from

It is designed behavior. A pool with no namepool whose effective
`max_active_sessions` is 1 is a canonical singleton
(`config.Agent.UsesCanonicalSingletonPoolIdentity`): its one session takes the
template name, slot 0 and the template's work dir. A pool
that hands out slots never creates such a session. Both template-identity
sessions were created during the 11 minutes the cap was 1.

## Which writes moved the session

**Write 1 is `syncSessionBeads`' legacy-template-identity upgrade**
(`cmd/gc/session_beads.go`). The fields it changed are the alias-guarded batch
(`agent_name`, `pool_slot`) plus the alias move from
`session.UpdatedAliasMetadata` and `queueAliasChangeDriftRebaseline`. The same
code holds `work_dir` back for an active session, under the comment "Legacy
active sessions are still running in their original work_dir. Don't repoint
metadata until the session stops."

**Write 2 is the pool trigger binding** (`bindPoolSessionTriggerBead` and
`computePoolTriggerBindingPatch` in `cmd/gc/build_desired_state.go`). The same
write changed `gc.trigger_bead_id`, and the binding is the only code that sets
the trigger and both work_dir keys in one patch on an existing session. After
write 1, the binding derived the work dir from the slot-1 identity. When the
trigger changes, it keeps the recorded dir only for a session that is active,
holds a resume request for itself, and does not use `wake_mode = "fresh"`. The
polecat pool uses fresh, so the binding wrote the slot-1 dir. That exemption
assumes the reconciler cycles a fresh session when its bead changes, but the
reconciler skips the cycle for a bead the session claimed itself (`selfClaimed`
in `cmd/gc/session_reconciler.go`). The process therefore kept running in the
template directory.

The first reaction suspected `repairConcretePoolTemplateWorkDirOverride`, which
a running session can reach through `relaunchAgentForLaunchDrift`. That repair
writes only the two work_dir keys and never the trigger, so it is not write 2.

## Decisions

1. **A pool that hands out slots should not create a template-identity
   session, and it does not.** The template identity belongs to a pool capped
   at one session. Nothing changes here.
2. **Moving a running session to a new identity does not restart it.** The
   identity fields keep moving at once. `pool_slot` reserves the slot so a new
   session cannot take it, the template alias is released, and the drift hashes
   are rebaselined so the move does not read as config drift. A restart would
   end the worker's bead in flight to correct bookkeeping.
3. **The recorded work_dir waits for the stop.** Worktree reclaim
   (`sessionRecordedWorktreeDirs`) and transcript lookup read `work_dir` as
   where the process runs, and so does the start-time ownership check in open
   PR #195 (gc-2ddu7). So it keeps naming the template directory while the
   session is active.
   The first start after the session stops moves it to the slot's dir
   (`repairConcretePoolTemplateWorkDirOverride`), and
   `validateConcretePoolPreparedWorkDir` refuses to start a slot session in the
   template directory. `syncSessionBeads` already followed this rule. The
   binding now follows it too: `keepLiveTemplateWorkDir` keeps an active
   session's template dir when the binding derives the slot's own dir.

## What the change does not cover

- **A work dir that belongs to the work bead.** When the binding derives a pack
  workspace or a managed worktree for a new trigger, an active fresh session's
  `work_dir` still follows the trigger, as before. The change holds only the
  move from the template dir to the slot's own dir.
- **Lowering a cap to 1 while a slot session runs.** A test run at origin/main
  d9d05b065 (not committed) gave a slot-1 session at cap 5, marked it awake and
  lowered the cap to 1. Realization collapsed the session's identity to the
  template at once ("collapsing phantom pool identity"), clearing `pool_slot`.
  When the session then took a new bead, a fresh session's `work_dir` moved to
  the template dir while its process stayed in the slot-1 dir. A resume-mode
  session kept its dir. The binding cannot hold this case the same way: the
  collapse erases the slot identity before the binding runs, and no start-path
  repair moves a singleton's recorded slot dir to the template dir. Recording
  the directory a session started in (gc-07kwpp, `worker_dir`) would give the
  binding and the readers an identity-free answer. Filed as gc-esr4il.
- **`canonical_instance_name`.** lx-wisp-xri6c kept the template name there.
  The S19 canonical identity record is written at create and adoption. It is
  read only by `concretePoolPreparedIdentity`, and only when the desired
  instance name is empty, which a realized pool session never has.

## Verification

- `TestPoolCapRaiseKeepsLiveSessionWorkDirUntilItStops`
  (`cmd/gc/build_desired_state_test.go`) runs the controller's realize-then-sync
  order. It creates the session under a cap of 1, marks it awake, raises the cap
  to 5, lets the session claim its next bead, then stops it and prepares its
  next start. At origin/main the `wake_mode = "fresh"` case fails exactly where
  write 2 happened (`gc.work_dir` names the slot-1 dir while the session is
  awake), and the resume case passes. With the change both pass, and the
  prepared start runs in the slot's dir.
- `TestKeepLiveTemplateWorkDir` covers the edges: a record in `work_dir` alone,
  a symlinked spelling of the template dir, stopped, creating and manual
  sessions, a derived dir that belongs to the work bead, a recorded dir that is
  not the template dir, and a singleton pool.
