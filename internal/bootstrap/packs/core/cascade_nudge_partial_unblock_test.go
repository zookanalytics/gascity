package core

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// cascadeFixtureGC writes a `gc` shim for the cascade order that serves canned
// responses from files under fixDir, so a test can shape the dependency graph
// per bead id rather than answering every bd call the same way:
//
//   - `events --type bead.closed ...` prints $FIX/events.jsonl (one JSON per line)
//   - `rig list --json` prints an empty rig set (single-rig city: no --rig
//     scoping and no suspended-rig skip)
//   - `bd dep list <id> ... --direction=up ...`   prints $FIX/up-<id>.json
//   - `bd dep list <id> ... --direction=down ...` prints $FIX/down-<id>.json
//   - `session nudge ...` logs the argv and exits with $FIX/nudge-exit (default 0)
//
// The id is the first positional after `dep list`; the script places any
// `--rig <name>` flags after it, so a fixed-position read is safe. A missing
// fixture answers `[]`.
func cascadeFixtureGC(t *testing.T, fixDir string) (binDir, logPath string) {
	t.Helper()
	return fakeGCBin(t, `FIX="`+fixDir+`"
case "$1" in
  events)
    [ -f "$FIX/events.jsonl" ] && cat "$FIX/events.jsonl"
    exit 0 ;;
  rig)
    echo '{"rigs":[]}'
    exit 0 ;;
  bd)
    shift
    if [ "$1" = "dep" ] && [ "$2" = "list" ]; then
      shift 2
      id="$1"
      dir=""
      for a in "$@"; do
        case "$a" in
          --direction=up) dir="up" ;;
          --direction=down) dir="down" ;;
        esac
      done
      f="$FIX/$dir-$id.json"
      if [ -f "$f" ]; then cat "$f"; else echo '[]'; fi
      exit 0
    fi
    echo '[]'
    exit 0 ;;
  session)
    if [ -f "$FIX/nudge-exit" ]; then exit "$(cat "$FIX/nudge-exit")"; fi
    exit 0 ;;
esac
exit 0
`)
}

// writeCascadeFixture writes one canned-response file under the fixtures dir.
func writeCascadeFixture(t *testing.T, fixDir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(fixDir, name), []byte(content), 0o644); err != nil {
		t.Fatalf("write fixture %s: %v", name, err)
	}
}

func requireJQForCascade(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq not available; cascade-nudge-on-blocker-close.sh requires it")
	}
}

// TestCascadeSkipsPartialUnblock is the discrimination guarantee: a dependent
// with an open sibling blocker is NOT nudged when the first of its blockers
// closes. Before the readiness re-check this order fired "may be unblocked" on
// the first close, misleading the assignee and re-injecting every turn.
func TestCascadeSkipsPartialUnblock(t *testing.T) {
	requireJQForCascade(t)
	fix := t.TempDir()
	state := t.TempDir()
	writeCascadeFixture(t, fix, "events.jsonl", `{"payload":{"bead":{"id":"blk1"}}}`+"\n")
	writeCascadeFixture(t, fix, "up-blk1.json", `[{"id":"dep1","status":"open","assignee":"rig/agent"}]`)
	// dep1 is still blocked by blk2, so closing blk1 is only a partial unblock.
	writeCascadeFixture(t, fix, "down-dep1.json", `[{"id":"blk1","status":"closed"},{"id":"blk2","status":"open"}]`)

	binDir, logPath := cascadeFixtureGC(t, fix)
	if out, err := runCascadeNudge(t, binDir, state); err != nil {
		t.Fatalf("cascade order failed: %v\n%s", err, out)
	}

	if n, logged := countNudges(t, logPath); n != 0 {
		t.Fatalf("partial unblock must send no nudge, sent %d:\n%s", n, logged)
	}
}

// TestCascadeNudgesOnFullReadiness is the other half: a dependent whose every
// blocks-dependency is closed IS nudged exactly once, and the nudge carries the
// dependent as its bead reference so repeated queued session nudges collapse by
// supersession.
func TestCascadeNudgesOnFullReadiness(t *testing.T) {
	requireJQForCascade(t)
	fix := t.TempDir()
	state := t.TempDir()
	writeCascadeFixture(t, fix, "events.jsonl", `{"payload":{"bead":{"id":"blk1"}}}`+"\n")
	writeCascadeFixture(t, fix, "up-blk1.json", `[{"id":"dep1","status":"open","assignee":"rig/agent"}]`)
	// blk1 is dep1's only blocker; closing it makes dep1 ready.
	writeCascadeFixture(t, fix, "down-dep1.json", `[{"id":"blk1","status":"closed"}]`)

	binDir, logPath := cascadeFixtureGC(t, fix)
	if out, err := runCascadeNudge(t, binDir, state); err != nil {
		t.Fatalf("cascade order failed: %v\n%s", err, out)
	}

	n, logged := countNudges(t, logPath)
	if n != 1 {
		t.Fatalf("full readiness must send exactly one nudge, sent %d:\n%s", n, logged)
	}
	if !strings.Contains(logged, "rig/agent") {
		t.Fatalf("nudge must address the dependent's assignee:\n%s", logged)
	}
	if !strings.Contains(logged, "--reference-bead dep1") {
		t.Fatalf("nudge must carry the dependent as its bead reference so repeated queued session nudges supersede:\n%s", logged)
	}
}

// TestCascadeIsIdempotentAcrossEvals pins the per-pair dedup: the same
// blocker-close event, still inside the lookback window on a later eval, does
// not re-fire the ready-transition nudge.
func TestCascadeIsIdempotentAcrossEvals(t *testing.T) {
	requireJQForCascade(t)
	fix := t.TempDir()
	state := t.TempDir()
	writeCascadeFixture(t, fix, "events.jsonl", `{"payload":{"bead":{"id":"blk1"}}}`+"\n")
	writeCascadeFixture(t, fix, "up-blk1.json", `[{"id":"dep1","status":"open","assignee":"rig/agent"}]`)
	writeCascadeFixture(t, fix, "down-dep1.json", `[{"id":"blk1","status":"closed"}]`)

	binDir, logPath := cascadeFixtureGC(t, fix)
	if out, err := runCascadeNudge(t, binDir, state); err != nil {
		t.Fatalf("first eval failed: %v\n%s", err, out)
	}
	if out, err := runCascadeNudge(t, binDir, state); err != nil {
		t.Fatalf("second eval failed: %v\n%s", err, out)
	}

	if n, logged := countNudges(t, logPath); n != 1 {
		t.Fatalf("ready-transition nudge must fire at most once per pair, fired %d:\n%s", n, logged)
	}
}
