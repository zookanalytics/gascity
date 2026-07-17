#!/usr/bin/env bash
# check-gomod-replace.sh [go.mod-path]
#
# Fails if go.mod contains any replace directive targeting an unreleased
# version: pseudo-version, local filesystem path, or git branch/ref.
#
# Policy: gascity is a public project. It must only pin released semver tags.
# The only override is an explicit human-operator decision (e.g. an emergency
# security fix from an unreleased commit). That override is a manual admin
# bypass of this required CI check — automated workers may NEVER self-authorize
# an unreleased dependency.
#
# Codified exception (fork allowlist): replace targets in FORK_REPLACE_ALLOWLIST
# below are permitted to carry an unreleased pin — but ONLY a commit-backed
# pseudo-version (e.g. v0.0.0-20260625154543-d05de7acf095). The allowlist
# widens exactly one shape: the pseudo-version. A mutable branch/ref token such
# as `main`, a prerelease label, or any other non-semver token targeting an
# allowlisted fork is still BLOCKED, exactly as for any other module. These are
# fork-owned, operator-approved repositories — not automated workers
# self-authorizing a third-party dep. The gascity fork permanently redirects
# github.com/steveyegge/beads to its own beads fork
# (github.com/zookanalytics/beads), a DoltLite-backed integration branch that
# tracks commits and therefore never carries a released semver tag — so `go get`
# pins it to a pseudo-version, which is immutable (it encodes a specific
# commit). The operator's approval of that pin is codified here once, so it is
# not re-litigated on every PR. Adding an entry to the allowlist is itself an
# operator-gated change (it modifies this committed guard). See gc-bvjbs.
#
# Released: exactly vX.Y.Z where X, Y, Z are integers (e.g. v1.0.5, v0.0.1).
# Blocked: pseudo-version, prerelease label, local path, git branch/ref, or
#          any non-semver version token.
#
# Handles both single-line and grouped multi-line replace blocks:
#   replace foo => bar v1.0.0-pseudo          (single-line)
#   replace (                                 (grouped block)
#       foo => bar v1.0.0-pseudo
#   )
set -euo pipefail

gomod="${1:-go.mod}"

if [[ ! -f "$gomod" ]]; then
	echo "check-gomod-replace: $gomod not found" >&2
	exit 1
fi

# Fork allowlist — replace *target* module paths permitted to carry an
# unreleased (pseudo-version) pin. See the header comment for the policy
# rationale. Each entry is matched against the replace target's module path
# exactly (never as a substring), so a local path or a look-alike suffix cannot
# slip through.
FORK_REPLACE_ALLOWLIST=(
	"github.com/zookanalytics/beads"
)

# is_fork_allowlisted PATH — exit 0 if PATH exactly matches an allowlisted fork
# target module path, exit 1 otherwise.
is_fork_allowlisted() {
	local candidate="$1" allowed
	for allowed in "${FORK_REPLACE_ALLOWLIST[@]}"; do
		[[ "$candidate" == "$allowed" ]] && return 0
	done
	return 1
}

# is_pseudo_version VERSION — exit 0 if VERSION is a Go module pseudo-version,
# exit 1 otherwise. A pseudo-version (e.g. v0.0.0-20260625154543-d05de7acf095)
# is commit-backed and immutable: it encodes a specific commit as a
# 14-digit UTC timestamp plus the commit hash. This is Go's own pseudo-version
# grammar (golang.org/x/mod/module) transcribed to POSIX ERE, covering all
# three forms — vX.0.0-, vX.Y.Z-pre.0., and vX.Y.(Z+1)-0. bases. Mutable tokens
# that name a moving target — branch/ref names like `main`, released tags like
# `v1.0.5`, and prerelease labels like `v1.0.5-rc1` — are NOT pseudo-versions
# and return 1.
is_pseudo_version() {
	local version="$1"
	local pseudo_re='^v[0-9]+\.(0\.0-|[0-9]+\.[0-9]+-([^+]*\.)?0\.)[0-9]{14}-[A-Za-z0-9]+(\+incompatible)?$'
	[[ "$version" =~ $pseudo_re ]]
}

check_replace_rhs() {
	local stripped="$1" rhs="$2"
	local version="" path_part="$rhs"

	# Strip an inline `// comment` from the RHS before splitting; otherwise the
	# trailing comment tokens defeat the two-token path/version match below and
	# an unreleased version slips through (e.g. `=> mod v1.0.5-pseudo // why`).
	if [[ "$rhs" == *"//"* ]]; then
		rhs="${rhs%%//*}"
	fi
	rhs="${rhs%"${rhs##*[![:space:]]}"}"   # trim trailing whitespace
	path_part="$rhs"

	# Split into path and optional version (last space-separated token).
	if [[ "$rhs" =~ ^([^ ]+)[[:space:]]+([^ ]+)$ ]]; then
		path_part="${BASH_REMATCH[1]}"
		version="${BASH_REMATCH[2]}"
	fi

	# Local filesystem paths are always unreleased.
	if [[ "$path_part" == ./* || "$path_part" == ../* || "$path_part" == /* ]]; then
		echo "check-gomod-replace: BLOCKED — replace directive targets a local path:" >&2
		echo "  $stripped" >&2
		echo "" >&2
		echo "  Policy: gascity is a public project that must only pin released semver deps." >&2
		echo "  Local-path replaces (./  ../  /) may not appear in committed go.mod." >&2
		echo "  Override: human operator must manually bypass this required CI check." >&2
		return 1
	fi

	# Fork allowlist: a fork-owned, operator-approved target (see header) may
	# carry an unreleased pin — but ONLY a commit-backed *pseudo-version*, the
	# single immutable unreleased shape. That is the sole additional form the
	# allowlist grants beyond the general rules below; every other token (a
	# branch/ref such as `main`, a prerelease label, an arbitrary tag) falls
	# through to the general version check and is blocked exactly as it would be
	# for a non-fork target. Checked *after* the local-path block above, so a
	# local path can never be allowlisted even if it resembles a fork module.
	# Gating on the pseudo-version shape here closes the hole where the allowlist
	# short-circuited to pass before any version validation and thus waved
	# through a mutable branch ref (gc-434qa).
	if is_fork_allowlisted "$path_part" && is_pseudo_version "$version"; then
		return 0
	fi

	# No version: path-only redirect with no version to check.
	[[ -n "$version" ]] || return 0

	# Only pure vX.Y.Z release tags are allowed. Everything else — pseudo-versions
	# (timestamp+sha suffix), prerelease labels (-rc1, -beta), and non-semver
	# tokens (branch names like "main", git refs) — is blocked.
	if [[ ! "$version" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
		echo "check-gomod-replace: BLOCKED — replace directive targets an unreleased version:" >&2
		echo "  $stripped" >&2
		echo "" >&2
		echo "  Policy: gascity is a public project that must only pin released semver deps." >&2
		echo "  Only exact vX.Y.Z release tags are allowed; pseudo-versions, prerelease" >&2
		echo "  labels, and git branch/ref tokens are not. Version seen: $version" >&2
		echo "  Override: human operator must manually bypass this required CI check." >&2
		return 1
	fi

	return 0
}

failed=0
in_replace_block=0
while IFS= read -r line; do
	stripped="${line#"${line%%[! ]*}"}"

	# Detect opening of a grouped block: replace (
	if [[ "$stripped" == "replace (" ]]; then
		in_replace_block=1
		continue
	fi

	# Detect closing of a grouped block.
	if [[ $in_replace_block -eq 1 && "$stripped" == ")" ]]; then
		in_replace_block=0
		continue
	fi

	if [[ $in_replace_block -eq 1 ]]; then
		# Inner line of a block: "old [version] => new [version]"
		[[ -z "$stripped" || "$stripped" == //* ]] && continue
		[[ "$stripped" == *"=>"* ]] || continue
		rhs="${stripped#*=>}"
		rhs="${rhs#"${rhs%%[! ]*}"}"
		check_replace_rhs "$stripped" "$rhs" || failed=1
		continue
	fi

	# Single-line replace: starts with "replace " but is not "replace ("
	[[ "$stripped" == replace\ * && "$stripped" != "replace (" ]] || continue
	[[ "$stripped" == *"=>"* ]] || continue
	rhs="${stripped#*=>}"
	rhs="${rhs#"${rhs%%[! ]*}"}"
	check_replace_rhs "$stripped" "$rhs" || failed=1
done < "$gomod"

if [[ $failed -ne 0 ]]; then
	exit 1
fi

echo "check-gomod-replace: OK (no unreleased replace directives)"
