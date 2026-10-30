# Canonical memory-tool variants. Source this file from bash; do not execute it.
#
# This is the only place that maps a variant name to CMake options, runtime
# detector settings and the probes that must be caught. Compiler and linker
# flags for each COMDB2_SANITIZER mode live in cmake/Comdb2Sanitizers.cmake.
#
#   source tests/memory_tools/config.sh
#   memory_tool_configure asan-system /path/to/detector-logs || exit 1
#   cmake -S . -B build "${MEMORY_TOOL_CMAKE_ARGS[@]}"
#   env "${MEMORY_TOOL_ENV[@]}" "${MEMORY_TOOL_COMMAND_PREFIX[@]}" ./program
#
# After a successful call these are set:
#   MEMORY_TOOL_VARIANT           variant name
#   MEMORY_TOOL_SANITIZER         COMDB2_SANITIZER value
#   MEMORY_TOOL_PER_THREAD_MALLOC COMDB2_PER_THREAD_MALLOC value
#   MEMORY_TOOL_CMAKE_ARGS        array of CMake -D arguments
#   MEMORY_TOOL_ENV               array of NAME=value runtime settings
#   MEMORY_TOOL_COMMAND_PREFIX    array to put in front of the program (may be empty)
#   MEMORY_TOOL_EXPECTED_PROBES   array of probes this variant must pass
#   MEMORY_TOOL_EXPECTED_RUNTIME  array of shared libraries every binary must link
#   MEMORY_TOOL_REPORT_KIND       "sanitizer" or "valgrind" (for check_report.py)
#   MEMORY_TOOL_FAILURE_EXIT_CODES array of exit codes the detector uses on a finding
#   MEMORY_TOOL_LEAK_CHECKING     "authoritative" or "disabled"
#   MEMORY_TOOL_REQUIRED_TOOLS    array of commands that must be on PATH
#   MEMORY_TOOL_MAX_OPEN_FILES    if set, lower the soft open-files limit to this
#                                 before running a program (Valgrind)

MEMORY_TOOL_VARIANTS=(asan-system asan-native tsan-system valgrind-system)

# Every probe the harness knows about, and the finding category it must produce.
memory_tool_probe_expectation()
{
    case "$1" in
    clean)                 echo clean ;;
    heap-use-after-free)   echo heap-use-after-free ;;
    leak)                  echo leak ;;
    comdb2-allocator-leak) echo leak ;;
    signed-overflow)       echo undefined-behavior ;;
    data-race)             echo data-race ;;
    invalid-read)          echo invalid-read ;;
    uninitialized-read)    echo uninitialized-read ;;
    *) return 1 ;;
    esac
}

# Probe binary path inside a configured build directory.
memory_tool_probe_binary()
{
    local build_dir=$1 probe=$2
    echo "${build_dir}/tests/memory_tools/memprobe_${probe//-/_}"
}

memory_tool_configure()
{
    local variant=${1-} logdir=${2-}

    MEMORY_TOOL_VARIANT=
    MEMORY_TOOL_SANITIZER=
    MEMORY_TOOL_PER_THREAD_MALLOC=
    MEMORY_TOOL_CMAKE_ARGS=()
    MEMORY_TOOL_ENV=()
    MEMORY_TOOL_COMMAND_PREFIX=()
    MEMORY_TOOL_EXPECTED_PROBES=()
    MEMORY_TOOL_EXPECTED_RUNTIME=()
    MEMORY_TOOL_REPORT_KIND=
    MEMORY_TOOL_FAILURE_EXIT_CODES=()
    MEMORY_TOOL_LEAK_CHECKING=
    MEMORY_TOOL_REQUIRED_TOOLS=(gcc cmake python3 readelf ldd timeout)
    MEMORY_TOOL_MAX_OPEN_FILES=

    if [[ $# -ne 2 || -z $variant || -z $logdir ]]; then
        echo "usage: memory_tool_configure <variant> <detector-log-dir>" >&2
        return 2
    fi
    # Sanitizer option strings are split on whitespace and ':'.
    if [[ $logdir != /* || $logdir == *[[:space:]:]* ]]; then
        echo "memory_tool_configure: detector log dir must be an absolute path" \
             "without whitespace or ':': '$logdir'" >&2
        return 2
    fi

    # Shared strict settings. ASan aborts on the first error; UBSan is built
    # recoverable, and halt_on_error=1 below makes its first finding fatal.
    local asan_common="halt_on_error=1:abort_on_error=1:detect_stack_use_after_return=1"
    asan_common+=":strict_string_checks=1:fast_unwind_on_malloc=0:malloc_context_size=50"
    asan_common+=":symbolize=1:log_path=${logdir}/asan"
    # With GCC's combined ASan+UBSan runtime UBSan ignores log_path and writes to
    # stderr, so callers must also scan the program's stderr.
    local ubsan="UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1:log_path=${logdir}/ubsan"

    case "$variant" in
    asan-system)
        MEMORY_TOOL_SANITIZER=address-undefined
        MEMORY_TOOL_PER_THREAD_MALLOC=OFF
        MEMORY_TOOL_ENV=(
            "ASAN_OPTIONS=detect_leaks=1:${asan_common}"
            "LSAN_OPTIONS=exitcode=23:report_objects=1"
            "$ubsan"
        )
        MEMORY_TOOL_EXPECTED_PROBES=(heap-use-after-free leak comdb2-allocator-leak signed-overflow clean)
        MEMORY_TOOL_EXPECTED_RUNTIME=(libasan libubsan)
        MEMORY_TOOL_REPORT_KIND=sanitizer
        # 134: SIGABRT from abort_on_error (ASan and LSan); 23: LSan exitcode
        # if the runtime does not abort; 1: fatal UBSan finding.
        MEMORY_TOOL_FAILURE_EXIT_CODES=(134 23 1)
        MEMORY_TOOL_LEAK_CHECKING=authoritative
        ;;
    asan-native)
        # dlmalloc mspaces hide individual Comdb2 allocations from LSan, so
        # leak checking is switched off rather than reported as incomplete.
        MEMORY_TOOL_SANITIZER=address-undefined
        MEMORY_TOOL_PER_THREAD_MALLOC=ON
        MEMORY_TOOL_ENV=(
            "ASAN_OPTIONS=detect_leaks=0:${asan_common}"
            "$ubsan"
        )
        MEMORY_TOOL_EXPECTED_PROBES=(heap-use-after-free signed-overflow clean)
        MEMORY_TOOL_EXPECTED_RUNTIME=(libasan libubsan)
        MEMORY_TOOL_REPORT_KIND=sanitizer
        MEMORY_TOOL_FAILURE_EXIT_CODES=(134 1)
        MEMORY_TOOL_LEAK_CHECKING=disabled
        ;;
    tsan-system)
        MEMORY_TOOL_SANITIZER=thread
        MEMORY_TOOL_PER_THREAD_MALLOC=OFF
        MEMORY_TOOL_ENV=(
            "TSAN_OPTIONS=halt_on_error=1:exitcode=66:second_deadlock_stack=1:history_size=7:log_path=${logdir}/tsan"
        )
        MEMORY_TOOL_EXPECTED_PROBES=(data-race clean)
        MEMORY_TOOL_EXPECTED_RUNTIME=(libtsan)
        MEMORY_TOOL_REPORT_KIND=sanitizer
        MEMORY_TOOL_FAILURE_EXIT_CODES=(66)
        MEMORY_TOOL_LEAK_CHECKING=disabled
        ;;
    valgrind-system)
        MEMORY_TOOL_SANITIZER=none
        MEMORY_TOOL_PER_THREAD_MALLOC=OFF
        MEMORY_TOOL_COMMAND_PREFIX=(
            valgrind
            --tool=memcheck
            --leak-check=full
            --show-leak-kinds=all
            --errors-for-leak-kinds=definite,indirect,possible
            --track-origins=yes
            --num-callers=50
            --error-exitcode=97
            "--log-file=${logdir}/valgrind.%p.log"
        )
        MEMORY_TOOL_EXPECTED_PROBES=(invalid-read uninitialized-read leak comdb2-allocator-leak clean)
        MEMORY_TOOL_REPORT_KIND=valgrind
        MEMORY_TOOL_FAILURE_EXIT_CODES=(97)
        MEMORY_TOOL_LEAK_CHECKING=authoritative
        MEMORY_TOOL_REQUIRED_TOOLS+=(valgrind)
        # Valgrind refuses to start ("Private file creation failed") when the
        # open-files limit is huge, as in some containers.
        MEMORY_TOOL_MAX_OPEN_FILES=1048576
        ;;
    *)
        echo "memory_tool_configure: unknown variant '$variant'." \
             "Allowed values: ${MEMORY_TOOL_VARIANTS[*]}" >&2
        return 2
        ;;
    esac

    MEMORY_TOOL_VARIANT=$variant
    MEMORY_TOOL_CMAKE_ARGS=(
        "-DCOMDB2_SANITIZER=${MEMORY_TOOL_SANITIZER}"
        "-DCOMDB2_PER_THREAD_MALLOC=${MEMORY_TOOL_PER_THREAD_MALLOC}"
        -DCOMDB2_TEST=ON
        -DCOMDB2_MEMORY_PROBES=ON
    )
    return 0
}

# Fails, listing what is missing, if a tool the configured variant needs is
# not on PATH. Call after memory_tool_configure.
memory_tool_check_tools()
{
    local tool missing=()
    for tool in "${MEMORY_TOOL_REQUIRED_TOOLS[@]}"; do
        command -v "$tool" >/dev/null 2>&1 || missing+=("$tool")
    done
    if [[ ${#missing[@]} -ne 0 ]]; then
        echo "memory-tools: variant ${MEMORY_TOOL_VARIANT} requires missing tool(s): ${missing[*]}" >&2
        return 1
    fi
    return 0
}
