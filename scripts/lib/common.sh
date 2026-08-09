#!/usr/bin/env bash
# Shared helpers for Gas City release scripts.
#
# Source this file in other scripts:
#   SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
#   source "$SCRIPT_DIR/lib/common.sh"

# shellcheck disable=SC2034  # colors are consumed by sourcing scripts
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

is_darwin_sed() {
    [[ "$OSTYPE" == "darwin"* && "$(command -v sed)" == "/usr/bin/sed" ]]
}

# Cross-platform `sed -i` wrapper (BSD sed on macOS needs an explicit empty backup arg).
sed_i() {
    if is_darwin_sed; then
        sed -i '' "$@"
    else
        sed -i "$@"
    fi
}

append_word_once() {
    local var_name="$1"
    local word="$2"
    local current="${!var_name:-}"

    case " $current " in
        *" $word "*)
            return 0
            ;;
    esac

    printf -v "$var_name" '%s' "${current:+$current }$word"
}

configure_linux_system_cgo_fallback() {
    [[ "${SYS_USR_CGO_FALLBACK:-1}" != "0" ]] || return 0

    local sys_usr_include="${SYS_USR_INCLUDE:-/usr/include}"
    local sys_usr_lib_root="${SYS_USR_LIB_ROOT:-/usr/lib}"
    local sys_usr_lib64_root="${SYS_USR_LIB64_ROOT:-/usr/lib64}"
    local cc_bin="${CC:-cc}"

    [[ -f "$sys_usr_include/unicode/uregex.h" ]] || return 0
    if "$cc_bin" -E -Wp,-v -x c /dev/null 2>&1 |
        sed 's/^[[:space:]]*//' |
        grep -F -x -q "$sys_usr_include"; then
        return 0
    fi

    append_word_once cgo_cppflags "-I$sys_usr_include"

    local -a candidates=()
    local arch dir seen_dirs=""
    while IFS= read -r arch; do
        [[ -n "$arch" ]] || continue
        candidates+=("$sys_usr_lib_root/$arch")
    done < <(
        {
            dpkg-architecture -q DEB_HOST_MULTIARCH 2>/dev/null || true
            "$cc_bin" -print-multiarch 2>/dev/null || true
        }
    )
    candidates+=("$sys_usr_lib64_root" "$sys_usr_lib_root")

    for dir in "${candidates[@]}"; do
        [[ -d "$dir" ]] || continue
        case " $seen_dirs " in
            *" $dir "*)
                continue
                ;;
        esac
        seen_dirs="${seen_dirs:+$seen_dirs }$dir"
        append_word_once cgo_ldflags "-L$dir"
    done
}

# gc_isolated_gitconfig_path prints the canonical path of the seeded global git
# config every test entrypoint shares. Keeping it under TMPDIR means a CI
# container, a `make` run and a direct shard invocation each get one stable file
# without any of them reaching into the host's real ~/.gitconfig.
gc_isolated_gitconfig_path() {
    printf '%s/gascity-testcfg/gitconfig' "${TMPDIR:-/var/tmp}"
}

# gc_seed_isolated_gitconfig writes the canonical isolated global git config and
# prints its path. This is the single source of that file's contents: the
# Makefile's ISOLATED_GITCONFIG and gc_resolve_isolated_gitconfig below both go
# through here so the `make` path and the direct-invocation path cannot drift.
#
# The seeded settings exist for two different failure modes:
#   - gpgsign = false neutralizes a host ~/.gitconfig with commit.gpgsign=true +
#     gpg.format=ssh. TEST_ENV allowlists HOME but deliberately not
#     SSH_AUTH_SOCK, so without this every test that execs `git commit` dies
#     with "Couldn't get agent socket?" (gc-fzl4).
#   - user.name/user.email/init.defaultBranch give tests a committer identity and
#     a stable initial branch without each one opting in.
#
# The write is atomic (temp file + rename) because concurrent shard jobs share
# this path: a truncated-then-rewritten file would be readable mid-write.
gc_seed_isolated_gitconfig() {
    local path dir tmp
    path="$(gc_isolated_gitconfig_path)"
    dir="$(dirname "$path")"
    mkdir -p "$dir" || return 1
    tmp="$(mktemp "$dir/gitconfig.XXXXXX")" || return 1
    # mktemp creates 0600; the file carries no secrets and is shared by every
    # concurrent test job, so publish it readable rather than umask-dependent.
    if ! printf '[user]\n\tname = Gas City Test\n\temail = gascity-test@example.invalid\n[commit]\n\tgpgsign = false\n[tag]\n\tgpgsign = false\n[init]\n\tdefaultBranch = main\n' > "$tmp" ||
        ! chmod 0644 "$tmp" ||
        ! mv -f "$tmp" "$path"; then
        rm -f "$tmp"
        return 1
    fi
    printf '%s' "$path"
}

# gc_resolve_isolated_gitconfig exports GIT_CONFIG_GLOBAL pointing at a real,
# writable git config, so every `env -i` allowlist below it forwards a usable
# value instead of an empty one.
#
# An empty GIT_CONFIG_GLOBAL looks fail-safe and is not: git treats it as
# /dev/null when READING (no global config at all) but resolves the WRITE
# destination to the empty path, so `git config --global <key> <value>` fails
# with "error: could not write config file :". CI invokes the shard scripts
# directly, with no Makefile TEST_ENV above them, and `${GIT_CONFIG_GLOBAL-}`
# forwarded exactly that empty value — which broke `gc init` (gc-beads-bd's
# ensure_beads_role writes beads.role into the global config) across six
# Integration shards (gc-f7wx8).
#
# A caller that already chose a config keeps it: under `make`, TEST_ENV has
# already pointed the variable at $(ISOLATED_GITCONFIG), and re-seeding there
# would clobber keys such as beads.role that earlier steps wrote into it.
gc_resolve_isolated_gitconfig() {
    if [[ -n "${GIT_CONFIG_GLOBAL:-}" ]]; then
        export GIT_CONFIG_GLOBAL
        return 0
    fi
    local path
    path="$(gc_isolated_gitconfig_path)"
    # Seed only when the file is missing. Re-seeding an existing one would drop
    # keys a concurrent job has already written into it.
    if [[ ! -f "$path" ]]; then
        path="$(gc_seed_isolated_gitconfig)" || return 1
    fi
    export GIT_CONFIG_GLOBAL="$path"
}

configure_cgo_platform_paths() {
    cgo_cppflags="${cgo_cppflags:-${CGO_CPPFLAGS:-}}"
    cgo_ldflags="${cgo_ldflags:-${CGO_LDFLAGS:-}}"

    case "$(uname)" in
        Darwin)
            local icu_prefix
            icu_prefix="$(brew --prefix icu4c 2>/dev/null || true)"
            if [[ -n "$icu_prefix" ]]; then
                append_word_once cgo_cppflags "-I${icu_prefix}/include"
                append_word_once cgo_ldflags "-L${icu_prefix}/lib"
            fi
            ;;
        Linux)
            configure_linux_system_cgo_fallback
            ;;
    esac
}
