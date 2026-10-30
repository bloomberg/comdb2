#!/usr/bin/env bash
# Memory-tool self-test: the one command local users and CI run.
#
#   tests/memory_tools/selftest.sh --variant <asan-system|asan-native|tsan-system|valgrind-system>
#       [--build-dir <dir>] [--artifact-dir <dir>] [--jobs <n>]
#
# Configures a clean build of the variant, builds the full comdb2 target and
# the probes, checks the sanitizer runtime is (or is not) linked, runs every
# probe the variant expects and writes <artifact-dir>/summary.{json,txt}.
# Exits 0 only if every step and every probe passed. Artifacts are kept on
# failure. Set CMAKE_GENERATOR=Ninja to use ninja, and COMDB2_SOURCE_SHA to
# record the source revision when building a tree without .git.

set -o nounset -o pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source_dir=$(cd "${here}/../.." && pwd)
# shellcheck source=config.sh
source "${here}/config.sh"

usage()
{
    local IFS='|'
    echo "usage: $0 --variant <${MEMORY_TOOL_VARIANTS[*]}> [--build-dir <dir>] [--artifact-dir <dir>] [--jobs <n>]" >&2
    exit 2
}

variant= build_dir= artifact_dir= jobs=
while [[ $# -gt 0 ]]; do
    case "$1" in
    --variant) variant=${2-}; shift 2 ;;
    --build-dir) build_dir=${2-}; shift 2 ;;
    --artifact-dir) artifact_dir=${2-}; shift 2 ;;
    --jobs) jobs=${2-}; shift 2 ;;
    -h|--help) usage ;;
    *) echo "selftest: unknown argument '$1'" >&2; usage ;;
    esac
done
[[ -n $variant ]] || usage
[[ -n $jobs ]] || jobs=$(nproc 2>/dev/null || echo 4)
[[ -n $build_dir ]] || build_dir="${TMPDIR:-/tmp}/comdb2-memory/${variant}/build"
[[ -n $artifact_dir ]] || artifact_dir="${TMPDIR:-/tmp}/comdb2-memory/${variant}/artifacts"

marker=.comdb2-memory-tools

# Empty a directory we own. Refuses directories that are not empty and were
# not created by this script, so a mistyped path cannot wipe unrelated files.
reset_dir()
{
    local dir=$1
    if [[ -e $dir ]]; then
        if [[ ! -d $dir ]]; then
            echo "selftest: $dir exists and is not a directory" >&2
            return 1
        fi
        if [[ -n $(ls -A "$dir") && ! -e $dir/$marker ]]; then
            echo "selftest: refusing to clear $dir: not empty and not created by selftest.sh" >&2
            return 1
        fi
        rm -rf "$dir"
    fi
    mkdir -p "$dir" && touch "$dir/$marker"
}

[[ $artifact_dir == /* ]] || artifact_dir="${PWD}/${artifact_dir}"
[[ $build_dir == /* ]] || build_dir="${PWD}/${build_dir}"

# Validates the variant name and the artifact path before touching anything.
memory_tool_configure "$variant" "${artifact_dir}/probes" || exit 2

reset_dir "$artifact_dir" || exit 1

# COMDB2_SOURCE_SHA identifies exported trees (e.g. git archive) that have no .git.
source_sha=${COMDB2_SOURCE_SHA:-$(git -C "$source_dir" rev-parse HEAD 2>/dev/null || echo unknown)}

summarize()
{
    local args=(--variant "$variant" --artifact-dir "$artifact_dir"
                --leak-checking "$MEMORY_TOOL_LEAK_CHECKING" --source-sha "$source_sha")
    local p
    for p in "${MEMORY_TOOL_EXPECTED_PROBES[@]}"; do
        args+=(--expected-probe "$p")
    done
    [[ $# -gt 0 ]] && args+=(--stage-failure "$1")
    python3 "${here}/check_report.py" summarize "${args[@]}"
}

# Record why the run stopped early, write the summary and exit nonzero.
fail_stage()
{
    echo "selftest: FAILED: $1" >&2
    if command -v python3 >/dev/null 2>&1; then
        summarize "$1" >/dev/null || true
    fi
    echo "selftest: artifacts in ${artifact_dir}" >&2
    exit 1
}

echo "variant:      $variant"
echo "source:       $source_dir ($source_sha)"
echo "build dir:    $build_dir"
echo "artifact dir: $artifact_dir"
echo "jobs:         $jobs"

memory_tool_check_tools || fail_stage "missing required tools for ${variant}"

{
    echo "date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "host: $(uname -n)"
    echo "kernel: $(uname -srm)"
    [[ -r /etc/os-release ]] && echo "os: $(. /etc/os-release && echo "${PRETTY_NAME-}")"
    echo "source: $source_sha"
    echo "cc: ${CC:-cc}: $("${CC:-cc}" --version 2>&1 | head -n 1)"
    echo "cxx: ${CXX:-c++}: $("${CXX:-c++}" --version 2>&1 | head -n 1)"
    echo "gcc: $(gcc --version 2>&1 | head -n 1)"
    echo "cmake: $(cmake --version 2>&1 | head -n 1)"
    echo "python3: $(python3 --version 2>&1)"
    echo "bash: ${BASH_VERSION}"
    echo "glibc: $(ldd --version 2>&1 | head -n 1)"
    if command -v valgrind >/dev/null 2>&1; then
        echo "valgrind: $(valgrind --version 2>&1)"
    else
        echo "valgrind: not installed"
    fi
    if command -v llvm-symbolizer >/dev/null 2>&1; then
        echo "llvm-symbolizer: $(command -v llvm-symbolizer)"
    fi
    echo
    echo "ulimit -a:"
    ulimit -a
    for knob in overcommit_memory mmap_rnd_bits; do
        if [[ -r /proc/sys/vm/$knob ]]; then
            echo "vm.${knob}: $(cat /proc/sys/vm/$knob)"
        fi
    done
} > "${artifact_dir}/tool-versions.txt" 2>&1
cat "${artifact_dir}/tool-versions.txt"

# ASan and TSan reserve terabytes of virtual address space and cannot start
# under a virtual-memory limit. Fail as an infrastructure problem instead of
# letting every probe die for an unrelated reason.
vlimit=$(ulimit -v)
if [[ $MEMORY_TOOL_SANITIZER != none && $vlimit != unlimited ]]; then
    fail_stage "infrastructure: ulimit -v is ${vlimit} KiB; ${MEMORY_TOOL_SANITIZER} sanitizers need an unlimited virtual address space (run 'ulimit -v unlimited')"
fi

echo "== configure"
reset_dir "$build_dir" || fail_stage "cannot prepare build directory ${build_dir}"
if ! cmake -S "$source_dir" -B "$build_dir" "${MEMORY_TOOL_CMAKE_ARGS[@]}" \
        > "${artifact_dir}/configure.log" 2>&1; then
    tail -n 30 "${artifact_dir}/configure.log" >&2
    fail_stage "cmake configure failed (configure.log)"
fi
cache="${build_dir}/CMakeCache.txt"
if ! command grep -qx "COMDB2_SANITIZER:STRING=${MEMORY_TOOL_SANITIZER}" "$cache" ||
   ! command grep -qx "COMDB2_PER_THREAD_MALLOC:BOOL=${MEMORY_TOOL_PER_THREAD_MALLOC}" "$cache"; then
    fail_stage "configured cache does not match variant ${variant} (expected COMDB2_SANITIZER=${MEMORY_TOOL_SANITIZER}, COMDB2_PER_THREAD_MALLOC=${MEMORY_TOOL_PER_THREAD_MALLOC})"
fi

echo "== build comdb2 memory-tool-probes"
if ! cmake --build "$build_dir" -j "$jobs" --target comdb2 memory-tool-probes \
        > "${artifact_dir}/build.log" 2>&1; then
    tail -n 40 "${artifact_dir}/build.log" >&2
    fail_stage "build failed (build.log)"
fi

echo "== linkage"
binaries=("${build_dir}/db/comdb2")
for probe in "${MEMORY_TOOL_EXPECTED_PROBES[@]}"; do
    binaries+=("$(memory_tool_probe_binary "$build_dir" "$probe")")
done
# Runtime library -> symbol that only instrumented code references.
declare -A runtime_symbol=([libasan]=__asan_init [libubsan]=__ubsan_handle_ [libtsan]=__tsan_init)
linkage_errors=()
for bin in "${binaries[@]}"; do
    if [[ ! -x $bin ]]; then
        linkage_errors+=("missing binary $bin")
        continue
    fi
    libs=$(ldd "$bin" 2>&1)
    syms=$(readelf -sW "$bin" 2>/dev/null)
    {
        echo "## $bin"
        echo "$libs"
        echo
    } >> "${artifact_dir}/linkage.txt"
    for lib in libasan libubsan libtsan; do
        expected=0
        for want in "${MEMORY_TOOL_EXPECTED_RUNTIME[@]}"; do
            [[ $want == "$lib" ]] && expected=1
        done
        linked=0 instrumented=0
        command grep -qE "^[[:space:]]*${lib}\.so" <<< "$libs" && linked=1
        command grep -qF "${runtime_symbol[$lib]}" <<< "$syms" && instrumented=1
        if [[ $expected -eq 1 && $linked -ne 1 ]]; then
            linkage_errors+=("$bin is not linked against $lib")
        elif [[ $expected -eq 1 && $instrumented -ne 1 ]]; then
            linkage_errors+=("$bin links $lib but has no ${runtime_symbol[$lib]} references")
        elif [[ $expected -ne 1 && ( $linked -eq 1 || $instrumented -eq 1 ) ]]; then
            linkage_errors+=("$bin unexpectedly uses $lib")
        fi
    done
done
if [[ ${#linkage_errors[@]} -ne 0 ]]; then
    printf '%s\n' "${linkage_errors[@]}" | tee -a "${artifact_dir}/linkage.txt" >&2
    fail_stage "linkage check failed (linkage.txt)"
fi
if [[ ${#MEMORY_TOOL_EXPECTED_RUNTIME[@]} -eq 0 ]]; then
    echo "OK: no sanitizer runtime linked (${#binaries[@]} binaries)" | tee -a "${artifact_dir}/linkage.txt"
else
    echo "OK: ${MEMORY_TOOL_EXPECTED_RUNTIME[*]} linked and referenced by ${#binaries[@]} binaries" |
        tee -a "${artifact_dir}/linkage.txt"
fi

echo "== probes"
for probe in "${MEMORY_TOOL_EXPECTED_PROBES[@]}"; do
    "${here}/run_probe.sh" --variant "$variant" --probe "$probe" \
        --binary "$(memory_tool_probe_binary "$build_dir" "$probe")" \
        --artifact-dir "$artifact_dir"
done

echo "== summary"
summarize
rc=$?
echo "selftest: artifacts in ${artifact_dir}"
exit $rc
