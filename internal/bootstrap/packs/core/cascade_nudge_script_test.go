package core

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// cascadeScriptPath is the on-disk copy of the blocker-close cascade order.
// The test runs the script, so it needs a real path rather than the embedded FS
// the content-assertion tests in this package read.
const cascadeScriptPath = "assets/scripts/cascade-nudge-on-blocker-close.sh"

// cascadeFakeGC writes a `gc` shim that dispatches the four subcommands the
// cascade order calls and logs each invocation to gc.log. Canned responses live
// as files under a fixtures dir the caller populates:
//
//   - `events --type bead.closed ...` prints $FIX/events.jsonl (one JSON per line)
//   - `rig list --json` prints an empty rig set (single-rig city, no --rig scoping)
//   - `bd dep list <id> ... --direction=up ...`   prints $FIX/up-<id>.json
//   - `bd dep list <id> ... --direction=down ...` prints $FIX/down-<id>.json
//   - `session nudge ...` logs the argv and exits with $FIX/nudge-exit (default 0)
//
// The id is the first positional after `dep list`; the script places any
// `--rig <name>` flags after it, so a fixed-position read is safe.
func cascadeFakeGC(t *testing.T, fixDir string) (binDir, logPath string) {
	t.Helper()
	binDir = t.TempDir()
	logPath = filepath.Join(binDir, "gc.log")
	shim := `#!/bin/sh
printf 'gc %s\n' "$*" >> "` + logPath + `"
FIX="` + fixDir + `"
case "$1" in
  events)
    [ -f "$FIX/events.jsonl" ] && cat "$FIX/events.jsonl"
    exit 0 ;;
  rig)
    echo '{"rigs":[]}'
    exit 0 ;;
  bd)
    # bd dep list <id> [--rig X] --direction=<dir> --type=blocks --json
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
`
	if err := os.WriteFile(filepath.Join(binDir, "gc"), []byte(shim), 0o755); err != nil {
		t.Fatalf("write fake gc: %v", err)
	}
	return binDir, logPath
}

// writeFixture writes one canned-response file under the fixtures dir.
func writeFixture(t *testing.T, fixDir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(fixDir, name), []byte(content), 0o644); err != nil {
		t.Fatalf("write fixture %s: %v", name, err)
	}
}

// runCascade runs the cascade order once with the fake gc on PATH and a private
// pack-state dir so the dedup file does not leak between runs. It returns the
// combined output; assertions read gc.log for the nudge decision.
func runCascade(t *testing.T, binDir, stateDir string) (string, error) {
	t.Helper()
	cmd := exec.Command("bash", cascadeScriptPath)
	env := append(os.Environ(),
		"PATH="+binDir+string(os.PathListSeparator)+os.Getenv("PATH"),
		"GC_PACK_STATE_DIR="+stateDir,
		// The trace helper must stay quiet, and the lookback default is fine.
		"GC_BD_TRACE_JSON=",
	)
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func gcLog(t *testing.T, logPath string) string {
	t.Helper()
	data, err := os.ReadFile(logPath)
	if err != nil {
		if os.IsNotExist(err) {
			return ""
		}
		t.Fatalf("read gc log: %v", err)
	}
	return string(data)
}

func countNudges(log string) int {
	n := 0
	for _, line := range strings.Split(log, "\n") {
		if strings.HasPrefix(line, "gc session nudge") {
			n++
		}
	}
	return n
}

// TestCascadeSkipsPartialUnblock is the discrimination guarantee: a dependent
// with an open sibling blocker is NOT nudged when the first of its blockers
// closes. Before the readiness re-check this order fired "may be unblocked" on
// the first close, misleading the assignee and re-injecting every turn.
func TestCascadeSkipsPartialUnblock(t *testing.T) {
	fix := t.TempDir()
	state := t.TempDir()
	writeFixture(t, fix, "events.jsonl", `{"payload":{"bead":{"id":"blk1"}}}`+"\n")
	writeFixture(t, fix, "up-blk1.json", `[{"id":"dep1","status":"open","assignee":"rig/agent"}]`)
	// dep1 is still blocked by blk2, so closing blk1 is only a partial unblock.
	writeFixture(t, fix, "down-dep1.json", `[{"id":"blk1","status":"closed"},{"id":"blk2","status":"open"}]`)

	binDir, logPath := cascadeFakeGC(t, fix)
	if out, err := runCascade(t, binDir, state); err != nil {
		t.Fatalf("cascade order failed: %v\n%s", err, out)
	}

	if n := countNudges(gcLog(t, logPath)); n != 0 {
		t.Fatalf("partial unblock must send no nudge, sent %d:\n%s", n, gcLog(t, logPath))
	}
}

// TestCascadeNudgesOnFullReadiness is the other half: a dependent whose every
// blocks-dependency is closed IS nudged exactly once, and the nudge carries the
// dependent as its bead reference so repeated queued session nudges collapse by
// supersession.
func TestCascadeNudgesOnFullReadiness(t *testing.T) {
	fix := t.TempDir()
	state := t.TempDir()
	writeFixture(t, fix, "events.jsonl", `{"payload":{"bead":{"id":"blk1"}}}`+"\n")
	writeFixture(t, fix, "up-blk1.json", `[{"id":"dep1","status":"open","assignee":"rig/agent"}]`)
	// blk1 is dep1's only blocker; closing it makes dep1 ready.
	writeFixture(t, fix, "down-dep1.json", `[{"id":"blk1","status":"closed"}]`)

	binDir, logPath := cascadeFakeGC(t, fix)
	if out, err := runCascade(t, binDir, state); err != nil {
		t.Fatalf("cascade order failed: %v\n%s", err, out)
	}

	log := gcLog(t, logPath)
	if n := countNudges(log); n != 1 {
		t.Fatalf("full readiness must send exactly one nudge, sent %d:\n%s", n, log)
	}
	if !strings.Contains(log, "rig/agent") {
		t.Fatalf("nudge must address the dependent's assignee:\n%s", log)
	}
	if !strings.Contains(log, "--reference-bead dep1") {
		t.Fatalf("nudge must carry the dependent as its bead reference so repeated queued session nudges supersede:\n%s", log)
	}
}

// TestCascadeIsIdempotentAcrossEvals pins the per-pair dedup: the same
// blocker-close event, still inside the lookback window on a later eval, does
// not re-fire the ready-transition nudge.
func TestCascadeIsIdempotentAcrossEvals(t *testing.T) {
	fix := t.TempDir()
	state := t.TempDir()
	writeFixture(t, fix, "events.jsonl", `{"payload":{"bead":{"id":"blk1"}}}`+"\n")
	writeFixture(t, fix, "up-blk1.json", `[{"id":"dep1","status":"open","assignee":"rig/agent"}]`)
	writeFixture(t, fix, "down-dep1.json", `[{"id":"blk1","status":"closed"}]`)

	binDir, logPath := cascadeFakeGC(t, fix)
	if out, err := runCascade(t, binDir, state); err != nil {
		t.Fatalf("first eval failed: %v\n%s", err, out)
	}
	if out, err := runCascade(t, binDir, state); err != nil {
		t.Fatalf("second eval failed: %v\n%s", err, out)
	}

	if n := countNudges(gcLog(t, logPath)); n != 1 {
		t.Fatalf("ready-transition nudge must fire at most once per pair, fired %d:\n%s", n, gcLog(t, logPath))
	}
}
