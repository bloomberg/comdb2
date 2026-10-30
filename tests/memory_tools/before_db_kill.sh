#!/usr/bin/env bash
# Run immediately before a test deliberately SIGKILLs a database server.
#
#   ${TESTSROOTDIR}/memory_tools/before_db_kill.sh <dbname> [host]
#
# SIGKILL skips LeakSanitizer's exit-time check, so in memory-tool runs the
# server first records a pre-kill checkpoint ("memdebug leakcheck": a
# recoverable LeakSanitizer check in asan-system, an intent marker otherwise).
# A failed checkpoint makes the memory result incomplete. The test's kill is
# not changed, and this always exits 0. Without MEMORY_TOOL_VARIANT it does
# nothing.

[[ -n $MEMORY_TOOL_VARIANT ]] || exit 0

db=$1
host=$2
target=(default)
[[ -n $host ]] && target=(--host "$host")

# shellcheck disable=SC2086 # CDB2_OPTIONS is a word list, as everywhere in tests/
out=$(timeout --kill-after=5s 120s "${CDB2SQL_EXE:-cdb2sql}" ${CDB2_OPTIONS} --tabs "$db" "${target[@]}" \
      'exec procedure sys.cmd.send("memdebug leakcheck")' 2>&1)

case "$out" in
*"memdebug leakcheck: ok"* | *"memdebug leakcheck: leaks found"*)
    ;;
*"memdebug leakcheck: unavailable (no LeakSanitizer)"*)
    [[ $MEMORY_TOOL_VARIANT == asan-system ]] &&
        reason="asan-system server has no LeakSanitizer"
    ;;
*)
    reason="no checkpoint: ${out//$'\n'/ }"
    ;;
esac

if [[ -n $reason ]]; then
    # shellcheck source=lifecycle.sh
    source "$(dirname "${BASH_SOURCE[0]}")/lifecycle.sh"
    memory_tool_lifecycle_note_incomplete "pre-kill checkpoint for ${db} ${host:-local}: ${reason}"
fi
exit 0
