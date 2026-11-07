---
name: What PR #219 still saves on main after upstream's one-read-per-leg scan (gc-xmah9h)
description: Records the re-verification, before PR #219 was brought current with main, of which of its control-dispatcher read savings upstream already delivers and which still pay. Holds the live loomington measurements the verdict rests on (the cost of one leg read with and without brief rows and a bounded page, and an hour of real bead events replayed through the serve loop's wake rules), what the branch dropped and what it carries after the merge, the bd version facts behind the brief-row fallback, and how the measurements were taken.
---

# gc-xmah9h: what PR #219 still saves on main

## Verdict

The savings still pay, in part. The upstream ready cache does not reach the
control dispatcher at all. Upstream's own scan already delivers the branch's
headline change, one read per scan in place of the snapshot prime. Three
pieces of the branch buy something measurable on main, and the branch now
carries only those:

- bead events from stores a dispatcher never reads no longer wake its serve
  loop;
- a bd leg's read asks for brief rows, without the free-form text;
- that read takes a bounded page first and reads the whole ready set again when
  the page comes back full.

On loomington's gc-toolkit store the bounded page cuts a leg read by about a
third whatever the rows carry. Brief rows pay only while ready beads carry long
notes: with 24 MB of notes in the ready set, the two together took the read
from 1.73 s to 0.53 s, and with the notes compacted, from 1.05 s to 0.69 s.
With the wake filter as well, an hour of that dispatcher's scanning drops from
about 2,000 s to between about 390 and 760 s.

## What upstream already delivers

- The ready cache (`8e920748b`, gastownhall/gascity#7863) serves
  `GET /beads/ready`. Only that handler and the bead-policy store's forwarder
  call `ReadyPreferCache`. The control scan calls `Ready` on the leg's store,
  which for a bd workspace is a raw `BdStore` opened for that scan. The live
  gc-toolkit dispatcher's bd child, read with `ps`, ran exactly
  `bd ready --json --include-ephemeral --limit 0`.
- `8a52426d6` (#7404) reads every scan fresh, and `5fa137b28` (#7446) reads
  each leg once through its store, applying `BdStore.Ready`'s
  `gc.work_outcome=blocked` veto. Both reached main in the 2026-10-13 sync.

The branch carried #7404 as a cherry-pick and answered a bd workspace with its
own shell `bd ready`. Both are dropped in the merge. The shell read would also
have skipped the veto that `BdStore.Ready` applies.

## What the branch carries after the merge

- The wake-scope filter, as the branch had it:
  `cmd/gc/dispatch_control_wake_scope.go` and the event pump's skip hook.
- `beads.ReadyQuery.Brief`. `BdStore` answers a brief read with `--brief` rows
  from a page of 5,000, and reads again with `--limit 0` when bd printed a
  full page. A bd that rejects `--brief` is read again with full rows, and
  its scope is recorded so later stores skip the attempt. Other stores ignore
  the field.
- The control scan's leg read asks for brief rows and returns the leg's whole
  ready set. Main cut every leg at 5,000 rows before the scan picked out the
  dispatcher's beads, which is the gap the branch's last commit (`fbfbb3625`)
  closed on its own read. The shell arm reads one page, then the whole set
  when that page fills.

Not carried: the branch's Dolt host and port prefix for its shell read. On
main the scan's read is the in-process leg read, whose bd child carried the
city's Dolt port in the live capture, and the shell arm runs only when the
legs cannot be opened.

## Measurements

Taken on the live city from 2026-10-17 23:28Z to 2026-10-18 01:10Z, host load
30 to 50, with gc built from main `11a09362a`.

### One leg read

Read-only `bd --readonly --sandbox ready --json --include-ephemeral` runs
from the store's directory, with the dispatcher's bd environment and the
variants interleaved: six rounds each on gc-toolkit, four on gascity.

At 23:30Z the gc-toolkit ready set held 403 rows and 24.3 MB of notes, most of
it on a handful of doctor-finding beads (11.2 MB on one, 6.5 MB on the next).
Those notes were compacted at about 23:43Z, and the second set of rounds ran
at 01:09Z the next day, on 401 rows.

| Store, time | Read | Median | Range | Bytes |
|---|---|---|---|---|
| gc-toolkit, 23:30Z | `--limit 0` (main) | 1.73 s | 1.25 to 3.03 s | 27,506,947 |
| gc-toolkit, 23:30Z | `--limit 0 --brief` | 0.84 to 0.92 s | 0.75 to 1.15 s | 404,112 |
| gc-toolkit, 23:30Z | `--limit 5000 --brief` | 0.53 s | 0.45 to 0.58 s | the same 403 rows |
| gc-toolkit, 01:09Z | `--limit 0` (main) | 1.05 s | 0.95 to 1.11 s | 1,981,078 |
| gc-toolkit, 01:09Z | `--limit 0 --brief` | 1.08 s | 0.91 to 1.27 s | 409,925 |
| gc-toolkit, 01:09Z | `--limit 5000 --brief` | 0.69 s | 0.61 to 0.90 s | 409,925 |
| gascity, 23:37Z | `--limit 0` (main) | 0.72 s | 0.63 to 1.19 s | 558,544 |
| gascity, 23:37Z | `--limit 5000 --brief` | 0.69 s | 0.40 to 0.93 s | 169,084 |

The bounded page is faster in both gc-toolkit states. Brief rows pay only while
the ready rows carry megabytes of text, and gascity's never did.
`--exclude-type=epic` changed nothing measurable. On all 403 rows of the first
state, the brief and full reads agree on every field except description,
design and notes: id, status, assignee, issue type, priority, timestamps,
labels, metadata, parent and dependencies (156 rows carry dependencies).

### One hour of scans

From the gc-toolkit dispatcher's trace log, 22:28 to 23:28Z:

- 1,068 scans and 2,002 s of scan time, 56% of the hour. None found a control
  bead.
- Wakes: 557 idle sweeps, 56 bead events from its own store, and 455 from other
  stores (city 293, shutupandlisten 52, sprintshow 48, gascity 46,
  signal-loom 16).

The gascity dispatcher's log covered 14.6 minutes after a restart: 393 scans
and 344 s, with 184 sweeps, 26 own-store wakes and 182 from other stores.

### The wake filter, replayed

The 1,289 bead events the city recorded in that hour (`gc events --type
bead.created|bead.updated|bead.closed`) were replayed through main's serve-loop
rules: an idle backoff of 1, 2, 4 and then 5 s, reset to 1 s by an event wake;
a 250 ms debounce that folds a burst into one wake; and the loop's own events
ignored. Scan durations were replayed in order from the same trace.

| Dispatcher, window | Read cost | Main's wake rule | Store-filtered |
|---|---|---|---|
| gc-toolkit, 60 min | as measured | 1,072 scans, 2,008 s | 656 scans, 1,078 s |
| gc-toolkit, 60 min | × 0.65 (the page alone) | 1,255 scans, 1,497 s | 708 scans, 760 s |
| gc-toolkit, 60 min | × 0.3 (page and brief, long notes) | 1,508 scans, 808 s | 784 scans, 386 s |
| gascity, 14 min | as measured | 392 scans, 342 s | 192 scans, 163 s |

A cheaper read lets the loop scan more often under event wakes, which is why
the scan counts rise as the read cost falls.

The replay of main's rule matches what the logs recorded (1,068 and 375
scans), which is what lets the filtered column stand in for a measurement.

The filter's cost is latency in one case. When only other stores' events would
have woken the loop, its backoff now climbs to the 5 s cap, so a close in the
dispatcher's own store that publishes no event is seen within about 5 s plus
one scan rather than 1 to 2 s. A close that publishes an event in that store
still wakes the loop at once.

## bd version facts

- `bd ready --brief` first appears in bd 1.2.0. Its flag is absent from
  `cmd/bd/ready.go` in the 1.0.4, 1.0.5, 1.1.0 and 1.1.2 sources and present in
  1.2.0 and 1.2.1.
- bd 1.0.4, the supported floor, from its pinned release tarball: `bd ready
  --json --limit 5000 --brief` fails with `Error: unknown flag: --brief`, the
  anchored error the fallback matches, and `bd ready --json
  --include-ephemeral --limit 5000` parses. bd 1.3.1, the current pin, parses
  the brief read.

## Re-measuring after deploy

gc-3r0anv owns the post-deploy measurement and waits on gc-due3fr. Each scan
writes `control-ready scan source=ready ready=N queue=N dur=...` to the
dispatcher's trace log, and each wake writes a `serve wake-event` or `serve
wake-sweep` line. A filtered event writes `serve ignore-unread-store-event`.
