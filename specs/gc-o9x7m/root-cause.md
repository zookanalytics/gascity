---
name: The model-usage zero-emission gap is a discovery-fallback + memo-pin bug, not a stale session_key (gc-o9x7m)
description: Corrected root-cause record for the model-usage zero-emission gap. Supersedes tk-md98fc's R1 section. The failing session's session_key is NOT stale: a wake_mode=fresh claude session re-mints a matching key on every fresh-wake before the process starts. The whole-interval outage comes from the transcript-discovery fallback resolving a dead sibling transcript when the keyed file is momentarily absent (the fresh-wake startup race), pinned for the entire awake interval by the live-sweep memo. Confirms the real rotation surface is the fresh-wake conversation boundary, never resume or compaction.
---

# gc-o9x7m: why a fresh-woken claude session records zero model usage

## Verdict

The reported symptom is real: a long-lived, model-invoking claude session
(mechanik, the gascity refinery) records no model-usage fact for a whole awake
interval while working, while a peer (deacon) records normally. The tk-md98fc
diagnosis of *why* is wrong.

- **The session_key is not stale.** mechanik, the refinery, and deacon are all
  `wake_mode = fresh` (`agents/{mechanik,refinery,deacon}/agent.toml`). A
  fresh-wake mints a new `session_key` and persists it to the session bead
  *before* the claude process starts, and hands it to claude as
  `--session-id <uuid>`. The stored key equals the new transcript's filename.
  Confirmed live: mechanik's bead key `30362cec…` and the gc-toolkit refinery's
  `b879dc96…` each equal the newest transcript on disk right now.
- **resume and compaction do not rotate the transcript id.** `claude --resume`
  appends to the same file; a compaction appends an in-place marker to the same
  file. The whole keyed-resume design (`internal/worker/builtin/profiles.go:123`
  `ResumeFlag: "--resume"`) presumes this. If either rotated the id, keyed
  discovery would break for every resumed session, not only the long-lived ones.
- **The real defect is in transcript discovery and the live-sweep memo, not the
  key.** When a session carries a `session_key` but its keyed transcript is not
  yet on disk, `TranscriptPathClassified` falls through to a newest-wins workdir
  fallback that resolves a *different, older* transcript and reports it as found.
  The live sweep memoizes that dead path for the entire awake interval and never
  re-checks it. Every recorder then reads a transcript whose cursor already sits
  at its last entry, and records nothing.
- **The rotation surface is the fresh-wake conversation boundary.** A fresh-wake
  starts a new conversation with a new transcript file. `/clear` and
  `--fork-session` also rotate the id but the managed fleet does not send them in
  normal operation. resume and compaction never rotate it.

This record supersedes the `## Fix` R1 section of
`gc-toolkit:specs/tk-md98fc/model-usage-emission-gap.md`. R1a (gascity gc-zb3st,
PR#179) and R1b (gascity gc-39arr, PR#184) are retired. R2 (gascity gc-1fke8) is
unaffected and still wanted: it is premise-independent, and its "resolve the live
transcript independently of session_key" shape is the same idea as the fix below.

## The emission path

A claude session's model usage reaches `.gc/usage.jsonl` through two recorders
that both resolve the transcript the same way, so both fail together when the
resolution is wrong.

1. **Prompt-op seam** — `internal/worker/invocation_telemetry.go:109`
   `recordInvocationTelemetry`, on a turn driven through the worker handle. A
   pool-routed or self-driving session that runs turns after its claim nudge
   does not reach this seam, so for those sessions it records little or nothing.
2. **Live in-interval sweep** — `cmd/gc/usage_compute.go:535`
   `sweepLiveSessionModelUsage`, run each reconcile tick (floored to 30s by
   `liveModelSweepMinInterval`) for every awake session. For a session whose
   interval runs for hours, this is the only recorder until the interval ends.

A third recorder, the terminal sweep (`SweepSessionModelUsage`, run once when the
interval ends), matters for the discriminator below.

Both live recorders resolve claude's transcript through
`internal/session/chat.go:1217` `TranscriptPathClassified`.

## Root cause: a keyed session binds to a sibling transcript, and the memo pins it

`TranscriptPathClassified` tries the keyed file first and, on a miss, walks a
workdir fallback:

- `chat.go:1233` — `DiscoverKeyedPath(..., session_key)`. For claude this stats
  `<project-slug>/<session_key>.jsonl` and returns it when the file exists.
- `chat.go:1256` — if more than one *live* session bead shares the workdir, and
  no key resolved, refuse as `TranscriptAmbiguous`. A named session has exactly
  one live bead, so this arm does not fire for it.
- `chat.go:1268` — otherwise `DiscoverPath(searchPaths, provider, workDir, "")`.
  The empty `gcSessionID` is the trap. `DiscoverPath` carries a guard
  (`internal/worker/transcript/discovery.go:28`) that returns "" when an id is
  present and its keyed lookup missed, precisely so a keyed provider never falls
  back to a sibling file. Passing "" skips that guard, so the call proceeds to
  `FindSessionFileForProvider`, whose claude route
  (`internal/sessionlog/reader.go:715` `findSlugSessionFileForCandidates`) returns
  the newest `.jsonl` in the project-slug directory by modification time.

So when a claude session has a `session_key` whose transcript is not on disk yet,
discovery does not report the keyed file absent. It returns the newest *other*
transcript in the directory as `TranscriptFound`.

The live sweep then makes that binding permanent for the interval
(`cmd/gc/usage_compute.go:549`):

- The memo is keyed on `(session_id, awake_started_at, session_key)`
  (`liveSweepMemoFor`, `usage_compute.go:588`). All three are stable across one
  awake interval, so the memo is not invalidated within it.
- Discovery runs only when `memo.path == ""` (`usage_compute.go:557`).
  `DiscoverSweepTranscript` returns a found path as settled
  (`internal/worker/invocation_telemetry.go:604` "A found path is always
  settled"), so once the memo holds a non-empty path it is never re-resolved.
- Every subsequent tick reads that memoized path
  (`SweepSessionModelUsageAtPath`), whose `invocation_usage_cursor` already sits
  at its final entry, so `usagesSinceCursor` returns nothing.

The dead sibling transcript is read every 30s for the rest of the interval, and
nothing is recorded.

## The trigger: the fresh-wake startup race

The keyed transcript is absent at exactly one moment: the start of a fresh
conversation, before claude has written its first entry. A fresh-wake creates a
new transcript file named after the freshly minted `session_key`, and the file
does not exist until claude writes to it. If the first live-sweep tick of the new
interval runs in that window, `DiscoverKeyedPath` misses and the fallback resolves
the previous conversation's transcript, which is still the newest file that
exists. The memo pins it, and the interval is lost.

This is why the outage is intermittent. When claude writes its first entry before
the first sweep tick, the keyed lookup hits, the correct transcript is memoized,
and the session records normally. A heavier session with a slower first turn
widens the window.

The session_key value at that instant does not change the outcome. A freshly
minted key whose file is not written yet misses the keyed lookup; an empty key
skips it outright. Either way the fallback resolves the newest existing sibling.
The binding is a property of discovery and the memo, not of the key being stale.

A genuinely stale key is possible on the relaunch and crash-adoption paths, where
`buildPreparedStartWithWorkDirResolver` skips the mint because the key is already
non-empty (`cmd/gc/session_lifecycle_parallel.go:1147`). Those paths are a
secondary trigger for the same dead-file bind. The fix below covers every trigger
because it acts on the reader, not on how the key was set.

## Why deacon is the control

The tk-md98fc report named deacon as the control: same provider, same
named-session shape, records fine. deacon's difference is interval length, not
key freshness. deacon runs a short patrol cadence, so its terminal sweep fires
every few minutes and recovers each short interval even when the live sweep is
memo-pinned. mechanik and the refinery hold a single conversation for hours, so a
poisoned live-sweep memo silences the entire interval, and the terminal sweep does
not run until the interval finally ends.

## Forensic re-reading (mechanik, 2026-09-02)

The tk-md98fc forensic stands, read under the corrected mechanism. The last
recorded fact came from the pre-wake transcript `fd110900` (its final turn at
23:11:48, last fact 23:12:01). mechanik woke into `41348736` at 23:22:09, and the
154 turns of that conversation went unrecorded for 135 minutes. The live sweep
reading `fd110900` throughout `41348736`'s interval is only possible if discovery
resolved `fd110900` while `41348736` was the session's key. That is the fallback
resolving the newest existing sibling during the startup race, pinned by the memo.
A stale key would read the same file, so the forensic alone does not separate the
two, but the fresh-wake mint re-points the key on this path, so the stale-key
account requires an exception this path does not take.

## A latent second issue: the cursor is not reset across conversations

`invocation_usage_cursor` is not a member of `freshWakeConversationResetKeys`
(`internal/session/lifecycle_transition.go:84`), and no fresh-wake or restart
patch clears it. Across a fresh-wake the cursor still names the previous
conversation's last entry. This is benign today because the identity is not found
in the new transcript, so `usagesSinceCursor` returns the whole window. It is
fragile: it leaves the cursor naming a different conversation than the transcript
being read, and it is worth resetting on the conversation reset for cleanliness.

## The fix

The actionable defect is on the reader, so the fix is premise-independent and
does not depend on enumerating every way a keyed transcript can be momentarily
absent. Recommended, smallest first:

1. **Do not resolve a sibling transcript for a keyed session.** In
   `TranscriptPathClassified`, when the session carries a non-empty `session_key`
   and the keyed lookup missed, classify the result `TranscriptAbsent` (transient)
   rather than falling through to the newest-wins fallback. Equivalently, pass the
   `session_key` into the `DiscoverPath` call at `chat.go:1268` so its existing
   keyed-miss guard (`discovery.go:28`) fires instead of being bypassed by the
   empty argument. The keyless newest-wins fallback stays intact for sessions that
   genuinely have no `session_key`.
2. **Re-validate a memoized live-sweep path.** When the memoized transcript's tail
   has not advanced past the cursor while a newer transcript exists in the same
   workdir, re-run discovery instead of reading the stale path. This is the
   defense-in-depth tk-md98fc noted, promoted to a real fix, and it also covers the
   relaunch and crash-adoption stale-key triggers.
3. **Reset `invocation_usage_cursor` on the fresh-wake conversation reset**, so the
   cursor never names a different conversation than the transcript being read.

Fix 1 closes the primary trigger. Fix 2 makes the sweep self-correct against any
future mis-resolution. The follow-up bead gc-k05x0 carries the implementation and
its test: a claude session whose bead `session_key` names a transcript not yet on
disk while an older sibling exists in the same workdir, asserting the live sweep
records nothing from the sibling and recovers the keyed transcript once it appears.

## Relationship to prior beads

- **gascity gc-zb3st (R1a, retired).** Its premise — that a stale `session_key` is
  never reconciled to the live conversation — is refuted here: the fresh-wake mint
  reconciles it, and the outage does not need a stale key.
- **gascity gc-39arr (R1b, retired).** Widening the managed Claude SessionStart
  matcher does not apply. The hook is a no-op once a key is present
  (`cmd/gc/cmd_prime.go:871`), and for claude the up-front mint sets the key, so the
  hook never overwrites it regardless of source.
- **gascity gc-1fke8 (R2, open).** Unaffected and still wanted. It resolves the
  live transcript independently of `session_key` to tell a real emission gap from
  benign idle. That is the same independent-resolution idea as fix 2, applied to
  detection instead of emission.
