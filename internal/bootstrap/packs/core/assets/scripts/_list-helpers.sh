#!/usr/bin/env bash
# _list-helpers.sh — pipefail-safe list utilities shared across core pack
# scripts. Source it instead of re-implementing membership checks:
#
#   . "$__SCRIPT_DIR/_list-helpers.sh"
#
# Sourced, never executed, so it sets no shell options — it must not impose
# `pipefail`/`errexit` on its caller. It uses bash `[[` and `$'\n'`, so the
# sourcing script must be bash.
#
# list_contains_line LIST NEEDLE
#   Exact whole-line membership test over a newline-delimited LIST, forking
#   nothing. Returns 0 when NEEDLE is a complete line of LIST, 1 otherwise
#   (also 1 when either argument is empty).
#
# Use this in place of `printf '%s\n' "$LIST" | grep -Fxq -- "$NEEDLE"`. Under
# `set -o pipefail` that pipeline is a latent SIGPIPE false-negative: `grep -q`
# exits the instant it matches without draining stdin, so the upstream writer
# races into a SIGPIPE (141), and pipefail promotes that 141 to the pipeline's
# status — a present candidate reported as ABSENT. Below the 64KiB pipe buffer
# the writer's single write usually lands first and the misread is a rare
# load-sensitive flake; once the list outgrows the buffer the misread is
# deterministic. Fixed twice before as a forked-pipeline race: d416a0085 in
# reaper.sh, gc-d760o in orphan-sweep.sh.
#
# Beyond removing the SIGPIPE, a pure-bash test cannot confuse "candidate
# absent" with "the check never ran" (spawn failure, signal) the way a forked
# probe does — both of the latter surface as the same non-zero status.
#
# Quoting $needle inside the pattern keeps glob metacharacters literal, so this
# stays a fixed-string whole-line match exactly like grep -Fx.
list_contains_line() {
    local list="$1"
    local needle="$2"
    [ -n "$needle" ] || return 1
    [ -n "$list" ] || return 1
    [[ $'\n'"$list"$'\n' == *$'\n'"$needle"$'\n'* ]]
}
