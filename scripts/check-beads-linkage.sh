#!/usr/bin/env bash
# check-beads-linkage.sh <gc-binary> [beads-module-path]
#
# Advisory post-install guard: reports when the gc just installed links a
# PINNED beads library instead of the local beads source the bd on PATH was
# built from.
#
# Why this exists (gc-ykvko): gc runs the beads library in-process, so the
# library it links and the bd binary beside it have to agree. `make install`
# links whatever go.mod pins — for this fork, a pseudo-version naming an
# untagged beads commit. A Gas Town deployment instead builds gc through a
# wrapper that applies `replace <beads module> => <local checkout>`, so gc
# links the same source bd was built from. Nothing at runtime reports the
# difference: the native-store preflight cannot compare a pseudo-version or a
# replaced module against a bd release, so version_compat passes the check as
# unconfirmed (internal/beads/contract/preflight_checker.go) and the skew stays
# silent. Three separate incidents began with a plain `make install`, and a
# written warning prevented none of them, so the remedy lives in the build.
#
# Contract: this guard ALWAYS exits 0. It reports; it never fails an install.
# It stays silent whenever it lacks the evidence to judge — no Go toolchain, no
# bd on PATH, unreadable build info, a binary that links no beads library at
# all, or no local beads checkout to rebuild against. That last one is what
# keeps CI and upstream contributors quiet: on a machine with no checkout, a gc
# linking the pinned library is the only thing `make install` can produce, so a
# bd built from some other published version is not evidence of a mistake.
set -uo pipefail

# The module gc links for its in-process beads library. Kept equal to
# beadsModulePath in internal/beads/contract/preflight_checker.go, which
# resolves the same dependency at runtime.
readonly DEFAULT_BEADS_MODULE="github.com/steveyegge/beads"

# beads_linkage prints how a binary links the beads module, as one
# tab-separated "<kind>\t<detail>" line:
#
#   local\t<path>      linked from a filesystem checkout (a source replace)
#   pinned\t<version>  linked from a published module version
#
# It prints nothing when the binary does not link the module, or when its build
# info cannot be read. A replace onto a versioned module reports the
# replacement's version: the binary still links a published snapshot, not the
# local checkout.
beads_linkage() {
	local binary="$1" module="$2"
	go version -m "$binary" 2>/dev/null | awk -v module="$module" '
		pending {
			pending = 0
			resolved = 1
			if ($1 == "=>") {
				if ($2 ~ /^\.?\.?\//) { print "local\t" $2 }
				else { print "pinned\t" $3 }
			} else {
				print "pinned\t" version
			}
			exit
		}
		($1 == "dep" || $1 == "mod") && $2 == module {
			version = $3
			pending = 1
		}
		END { if (!resolved && pending) print "pinned\t" version }
	'
}

# is_beads_checkout reports whether dir is a source checkout of the beads
# module — a directory whose go.mod declares that module path. Reading the
# module line is what keeps an unrelated directory of the same name, or a
# checkout of something else, from reading as a replace target.
is_beads_checkout() {
	local dir="$1" module="$2" declared
	[ -n "$dir" ] && [ -f "$dir/go.mod" ] || return 1
	declared="$(awk '$1 == "module" { print $2; exit }' "$dir/go.mod" 2>/dev/null)"
	[ "$declared" = "$module" ]
}

# local_beads_checkout prints the local beads checkout a `replace` could point
# gc at, and fails when the machine has none.
#
# This is the gate on the whole warning. What the warning asks for is a rebuild
# against local beads source, so it is only worth printing where that source
# exists: on a machine without it — CI, a fresh clone, a contributor tracking
# the go.mod pin — there is no skew to fix and no build-optimized.sh to run.
#
# Two places reveal a checkout. bd's own `replace` names one directly, which is
# how a deployment that keeps its checkout somewhere unusual reveals it. Failing
# that, "$HOME/beads" is the location Gas Town's build-optimized.sh resolves,
# so it finds the town shape even when bd itself came from a published version.
local_beads_checkout() {
	local module="$1" bd_kind="$2" bd_detail="$3"
	if [ "$bd_kind" = "local" ] && is_beads_checkout "$bd_detail" "$module"; then
		printf '%s' "$bd_detail"
		return 0
	fi
	local home="${HOME:-}"
	if [ -n "$home" ] && is_beads_checkout "$home/beads" "$module"; then
		printf '%s' "$home/beads"
		return 0
	fi
	return 1
}

# describe_bd renders bd's side of the comparison for the warning.
describe_bd() {
	local kind="$1" detail="$2" module="$3"
	if [ "$kind" = "local" ]; then
		printf 'built from the local checkout %s' "$detail"
	elif [ "$detail" = "(devel)" ]; then
		printf 'built from local %s source (reported as (devel))' "$module"
	else
		printf 'built from %s %s' "$module" "$detail"
	fi
}

# report_skew prints the warning banner on stderr. It names both sides of the
# comparison so the reader can confirm the finding without re-running anything.
report_skew() {
	local binary="$1" module="$2" version="$3" bd_binary="$4" bd_summary="$5" checkout="$6"
	cat >&2 <<EOF

!! ====================================================================
!! WARNING: this gc links a PINNED beads library, not your beads source
!! ====================================================================
!!
!!   installed  $binary
!!     links    $module $version (pinned by go.mod)
!!   bd         $bd_binary
!!     $bd_summary
!!   beads      $checkout
!!     the local checkout gc should have been built against
!!
!! gc runs the beads library in-process, so a gc built this way can
!! disagree with the bd beside it. Nothing at runtime will say so: the
!! native-store preflight cannot compare an unreleased library version
!! against a bd release, so version_compat passes as unconfirmed and
!! the skew stays silent.
!!
!! The supported build for a Gas Town deployment is
!!
!!     build-optimized.sh gc
!!
!! which applies a go.mod \`replace\` onto your local checkout, so gc
!! links the same source bd was built from. \`make install\` does not.
!! ====================================================================

EOF
}

main() {
	local binary="${1:-}" module="${2:-$DEFAULT_BEADS_MODULE}"
	[ -n "$binary" ] || return 0
	command -v go >/dev/null 2>&1 || return 0

	local linkage kind version
	linkage="$(beads_linkage "$binary" "$module")"
	[ -n "$linkage" ] || return 0
	kind="${linkage%%$'\t'*}"
	version="${linkage#*$'\t'}"
	# A build against local beads source is the supported shape.
	[ "$kind" = "pinned" ] || return 0

	local bd_binary
	bd_binary="$(command -v bd 2>/dev/null)"
	[ -n "$bd_binary" ] || return 0

	local bd_linkage bd_kind bd_detail
	bd_linkage="$(beads_linkage "$bd_binary" "$module")"
	[ -n "$bd_linkage" ] || return 0
	bd_kind="${bd_linkage%%$'\t'*}"
	bd_detail="${bd_linkage#*$'\t'}"

	# Both sides name the same published version: gc and bd agree.
	if [ "$bd_kind" = "pinned" ] && [ "$bd_detail" = "$version" ]; then
		return 0
	fi

	# Nothing to rebuild against, so nothing to report.
	local checkout
	checkout="$(local_beads_checkout "$module" "$bd_kind" "$bd_detail")" || return 0

	report_skew "$binary" "$module" "$version" "$bd_binary" \
		"$(describe_bd "$bd_kind" "$bd_detail" "$module")" "$checkout"
}

main "$@"
exit 0
