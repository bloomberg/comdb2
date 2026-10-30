#!/usr/bin/env bash
# DEBUG_PREFIX for memory-tool test runs: run one Comdb2 process under the
# test's memory-tool variant and replace this shell with it, so the process
# keeps this PID ($! and pidfiles stay correct).
#
#   exec.sh /path/to/comdb2 <args...>
#
# lifecycle.sh copies this script, config.sh and runtime.env into
# <artifact-root>/runtime/ on every node; ssh'd server starts do not inherit
# the test's environment, so the settings are read from runtime.env.
#
# For Comdb2 it records <artifact-root>/nodes/<host>/invocations/<pid>/
#   launch.json   pid, node, variant, testcase, argv
#   stderr.log    copy of stderr (UBSan writes findings there)
#   detector/     sanitizer and Valgrind logs
# and the server itself adds clean-exit / leakcheck markers there.
# Any other program (some tests prefix cdb2sql) is executed unchanged.

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

if [[ $# -eq 0 ]]; then
    echo "memory-tools exec.sh: no command given" >&2
    exit 2
fi

# MEMORY_TOOL_VARIANT, MEMORY_TOOL_ARTIFACT_ROOT, MEMORY_TOOL_TESTCASE,
# MEMORY_TOOL_COMDB2_EXE
# shellcheck source=/dev/null
if ! source "${here}/runtime.env"; then
    echo "memory-tools exec.sh: cannot read ${here}/runtime.env" >&2
    exit 2
fi

if [[ $(basename -- "$1") != comdb2 && $1 != "$MEMORY_TOOL_COMDB2_EXE" ]]; then
    exec "$@"
fi

# shellcheck source=config.sh
source "${here}/config.sh"

json_string()
{
    local s=$1 out= c i
    for ((i = 0; i < ${#s}; i++)); do
        c=${s:i:1}
        case "$c" in
        '"') out+='\"' ;;
        '\') out+='\\' ;;
        [[:cntrl:]]) out+=$(printf '\\u%04x' "'$c") ;;
        *) out+=$c ;;
        esac
    done
    printf '"%s"' "$out"
}

node=$(hostname)
node=${node//[^A-Za-z0-9._-]/_}
dir="${MEMORY_TOOL_ARTIFACT_ROOT}/nodes/${node}/invocations/$$"

if ! mkdir -p "${dir}/detector"; then
    echo "memory-tools exec.sh: cannot create ${dir}" >&2
    exit 2
fi
memory_tool_configure "$MEMORY_TOOL_VARIANT" "${dir}/detector" server || exit 2

{
    printf '{\n  "pid": %d,\n  "node": %s,\n  "variant": %s,\n  "testcase": %s,\n' \
        $$ "$(json_string "$node")" "$(json_string "$MEMORY_TOOL_VARIANT")" \
        "$(json_string "$MEMORY_TOOL_TESTCASE")"
    printf '  "start": %s,\n  "argv": [' "$(json_string "$(date -u +%Y-%m-%dT%H:%M:%SZ)")"
    sep=
    for arg in "$@"; do
        printf '%s%s' "$sep" "$(json_string "$arg")"
        sep=', '
    done
    printf ']\n}\n'
} > "${dir}/launch.json"

export COMDB2_MEMORY_INVOCATION_DIR=$dir

if [[ -n $MEMORY_TOOL_MAX_OPEN_FILES ]]; then
    nofile=$(ulimit -S -n)
    if [[ $nofile == unlimited || $nofile -gt $MEMORY_TOOL_MAX_OPEN_FILES ]]; then
        ulimit -S -n "$MEMORY_TOOL_MAX_OPEN_FILES"
    fi
fi

# stderr still reaches the caller's log; tee keeps a copy for the scan.
exec env -u ASAN_OPTIONS -u LSAN_OPTIONS -u UBSAN_OPTIONS -u TSAN_OPTIONS \
    "${MEMORY_TOOL_ENV[@]}" "${MEMORY_TOOL_COMMAND_PREFIX[@]}" "$@" \
    2> >(tee -a "${dir}/stderr.log" >&2)
