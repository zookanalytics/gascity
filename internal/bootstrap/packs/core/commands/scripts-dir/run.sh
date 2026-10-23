#!/bin/sh
# gc core scripts-dir: print the directory that holds the core pack's scripts.
# gc registers a pack command under the binding the city imports the pack as,
# and runs it with GC_PACK_DIR set to the pack it loaded the command from, so
# the directory is the copy of core this city composes, wherever it is served
# from. A formula step calls a core script as "$(gc core scripts-dir)/<script>"
# inside the command that runs it, so no variable has to survive from an
# earlier shell.
# Exit: 0 printed · 1 GC_PACK_DIR unset or no assets/scripts under it.
set -eu

[ -n "${GC_PACK_DIR:-}" ] || {
    echo "scripts-dir: GC_PACK_DIR is unset; gc sets it for a pack command, so run this as gc core scripts-dir, with the binding the city imports the core pack as" >&2
    exit 1
}
dir="$GC_PACK_DIR/assets/scripts"
[ -d "$dir" ] || {
    echo "scripts-dir: $dir does not exist" >&2
    exit 1
}
printf '%s\n' "$dir"
