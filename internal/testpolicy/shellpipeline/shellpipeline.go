// Package shellpipeline detects shell-pipeline constructs that are unsafe
// under `set -o pipefail`, so the repo's shell assets can be guarded against a
// defect class that has now recurred five times.
//
// The class: `grep -q` exits the instant it matches, without draining stdin.
// The writer feeding it then races into a SIGPIPE, and under `set -o pipefail`
// that 141 becomes the pipeline's exit status — so a PRESENT match is reported
// as ABSENT. Below the 64KiB pipe buffer the writer's single write usually
// lands before grep exits and the misread is a rare load-sensitive flake; once
// the output outgrows the buffer the misread is deterministic.
//
// Sightings: d416a0085 (reaper.sh), gc-d760o (orphan-sweep.sh), gc-atrp1
// (spawn-storm-detect/wisp-compact/escalate), gc-u2fyx (rebase-resolve-lib.sh),
// gc-01o2l (.githooks/pre-push, where it silently disabled the push test gate).
//
// Callers walk their own asset trees and report each offset; keeping the
// detector here means a new tree gets guarded by an import rather than by a
// copy of the regexes, which is what let the class recur after the first two
// fixes shipped only per-script behavioral tests.
package shellpipeline

import (
	"bytes"
	"regexp"
)

var (
	// A `set ... pipefail` declaration anywhere in the script.
	pipefailDeclPattern = regexp.MustCompile(`(?m)^[ \t]*set\b[^\n]*\bpipefail\b`)
	// A writer piped into `grep` with a `-q` (quiet) flag, in any flag
	// cluster: -q, -qF, -Fxq, -qiE, -Eq, -qsF, ... The here-string form
	// (`grep -q ... <<<"$x"`) has no pipe and is not matched.
	pipedGrepQPattern = regexp.MustCompile(`\|[[:space:]]*grep[[:space:]]+(-[A-Za-z]+[[:space:]]+)*-[A-Za-z]*q[A-Za-z]*\b`)
)

// FindPipefailGrepQPipelines returns the byte offsets of `writer | grep -q`
// pipelines in a script that runs under `set -o pipefail`. It returns nil for
// scripts that do not enable pipefail (where the SIGPIPE is harmless) and
// skips full-line comments so the guard can be documented in prose.
func FindPipefailGrepQPipelines(data []byte) []int {
	if !pipefailDeclPattern.Match(data) {
		return nil
	}
	var offsets []int
	pos := 0
	for _, line := range bytes.SplitAfter(data, []byte("\n")) {
		if trimmed := bytes.TrimLeft(line, " \t"); len(trimmed) == 0 || trimmed[0] != '#' {
			if loc := pipedGrepQPattern.FindIndex(line); loc != nil {
				offsets = append(offsets, pos+loc[0])
			}
		}
		pos += len(line)
	}
	return offsets
}

// Remedy is the guidance every caller's failure message should carry, so the
// fix is described identically wherever the class is caught.
const Remedy = "under `set -o pipefail`, piping into `grep -q` SIGPIPEs the writer on grep's early exit and reports a present match as absent — capture the output once and test it (`out=$(...)`; `[ -n \"$out\" ]`), use a here-string (`grep -q ... <<<\"$out\"`), or use list_contains_line from _list-helpers.sh"

// DescribeLine returns the 1-based line number and the trimmed source line
// containing offset, for use in a caller's failure message.
func DescribeLine(data []byte, offset int) (int, string) {
	lineNumber := bytes.Count(data[:offset], []byte{'\n'}) + 1
	lineStart := bytes.LastIndexByte(data[:offset], '\n') + 1
	lineEnd := bytes.IndexByte(data[offset:], '\n')
	if lineEnd < 0 {
		lineEnd = len(data)
	} else {
		lineEnd += offset
	}
	return lineNumber, string(bytes.TrimSpace(data[lineStart:lineEnd]))
}
