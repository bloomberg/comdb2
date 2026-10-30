#!/usr/bin/env bash
# Run one memory-tool probe under one variant and check the detector caught it.
#
#   run_probe.sh --variant asan-system --probe heap-use-after-free \
#                --binary <build-dir>/tests/memory_tools/memprobe_heap_use_after_free \
#                --artifact-dir <variant-artifact-dir>
#
# Writes <artifact-dir>/probes/<probe>/{command.txt,stdout.log,stderr.log,
# exit-code.txt,report.json,detector/*}. Exits 0 only if the expectation from
# config.sh holds (faulty probe caught with the right signature, or clean
# probe exited 0 with no findings).

set -o errexit -o nounset -o pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=config.sh
source "${here}/config.sh"

usage()
{
    local IFS='|'
    echo "usage: $0 --variant <${MEMORY_TOOL_VARIANTS[*]}> --probe <name> --binary <path> --artifact-dir <dir> [--timeout <secs>]" >&2
    exit 2
}

variant= probe= binary= artifact_dir= timeout_secs=300
while [[ $# -gt 0 ]]; do
    case "$1" in
    --variant) variant=${2-}; shift 2 ;;
    --probe) probe=${2-}; shift 2 ;;
    --binary) binary=${2-}; shift 2 ;;
    --artifact-dir) artifact_dir=${2-}; shift 2 ;;
    --timeout) timeout_secs=${2-}; shift 2 ;;
    *) usage ;;
    esac
done
[[ -n $variant && -n $probe && -n $binary && -n $artifact_dir ]] || usage

expect=$(memory_tool_probe_expectation "$probe") || {
    echo "run_probe: unknown probe '$probe'" >&2
    exit 2
}

mkdir -p "$artifact_dir"
artifact_dir=$(cd "$artifact_dir" && pwd)
probe_dir="${artifact_dir}/probes/${probe}"
detector_dir="${probe_dir}/detector"

memory_tool_configure "$variant" "$detector_dir"

found=0
for p in "${MEMORY_TOOL_EXPECTED_PROBES[@]}"; do
    [[ $p == "$probe" ]] && found=1
done
if [[ $found -ne 1 ]]; then
    echo "run_probe: probe '$probe' is not part of variant $variant" \
         "(${MEMORY_TOOL_EXPECTED_PROBES[*]})" >&2
    exit 2
fi
if [[ ! -x $binary ]]; then
    echo "run_probe: probe binary missing or not executable: $binary" >&2
    exit 1
fi

# Only this probe's previous results are removed.
rm -rf "$probe_dir"
mkdir -p "$detector_dir"

# Start from a known detector environment: callers' own settings must not
# weaken or redirect the canonical ones.
cmd=(env -u ASAN_OPTIONS -u LSAN_OPTIONS -u UBSAN_OPTIONS -u TSAN_OPTIONS
     "${MEMORY_TOOL_ENV[@]}" "${MEMORY_TOOL_COMMAND_PREFIX[@]}" "$binary")
printf '%q ' timeout --kill-after=10 "$timeout_secs" "${cmd[@]}" > "${probe_dir}/command.txt"
echo >> "${probe_dir}/command.txt"

set +o errexit
# The outer group only discards bash's own "Aborted" job notice when a probe
# dies by signal; the probe's output goes to its log files.
{ (
    if [[ -n $MEMORY_TOOL_MAX_OPEN_FILES ]]; then
        nofile=$(ulimit -S -n)
        if [[ $nofile == unlimited || $nofile -gt $MEMORY_TOOL_MAX_OPEN_FILES ]]; then
            ulimit -S -n "$MEMORY_TOOL_MAX_OPEN_FILES"
        fi
    fi
    exec timeout --kill-after=10 "$timeout_secs" "${cmd[@]}"
) > "${probe_dir}/stdout.log" 2> "${probe_dir}/stderr.log"; } 2> /dev/null
rc=$?
set -o errexit
echo "$rc" > "${probe_dir}/exit-code.txt"

check=(python3 "${here}/check_report.py" check
       --variant "$variant" --probe "$probe" --expect "$expect"
       --kind "$MEMORY_TOOL_REPORT_KIND" --exit-code "$rc"
       --stream "${probe_dir}/stdout.log" --stream "${probe_dir}/stderr.log"
       --output "${probe_dir}/report.json")
for code in "${MEMORY_TOOL_FAILURE_EXIT_CODES[@]}"; do
    check+=(--failure-exit-code "$code")
done
shopt -s nullglob
for log in "$detector_dir"/*; do
    check+=(--report "$log")
done

"${check[@]}"
