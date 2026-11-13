#!/usr/bin/env bash
# The build settings behind each variant name (cmake/Comdb2MemoryVariant.cmake)
# must match what the runtime side expects of a binary (config.sh), and a
# variant must refuse settings that contradict it.
# Run: bash tests/memory_tools/test_variant_mapping.sh   (needs cmake, no compiler)

set -o nounset -o pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source_dir=$(cd "${here}/../.." && pwd)
# shellcheck source=config.sh
source "${here}/config.sh"

failures=0
fail() { echo "FAIL: $*"; failures=$((failures + 1)); }
pass() { echo "ok:   $*"; }

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

# configure <build-dir> [cmake args...]: configure the mapping project
configure()
{
    local dir=$1
    shift
    cmake -S "${here}/variant_mapping" -B "$dir" "$@" > "${dir}.log" 2>&1
}

cached() # cached <build-dir> <name>
{
    sed -n "s/^${2}:[A-Z]*=//p" "${1}/CMakeCache.txt"
}

for variant in "${MEMORY_TOOL_VARIANTS[@]}"; do
    memory_tool_configure "$variant" /tmp/memory-tools-test || { fail "$variant: config.sh"; continue; }
    dir="${tmp}/${variant}"
    if ! configure "$dir" "${MEMORY_TOOL_CMAKE_ARGS[@]}"; then
        fail "$variant: configure failed: $(tail -n 5 "${dir}.log")"
        continue
    fi
    [[ $(cached "$dir" COMDB2_MEMORY_VARIANT) == "$variant" ]] || fail "$variant: COMDB2_MEMORY_VARIANT=$(cached "$dir" COMDB2_MEMORY_VARIANT)"
    [[ $(cached "$dir" COMDB2_SANITIZER) == "$MEMORY_TOOL_SANITIZER" ]] ||
        fail "$variant: CMake COMDB2_SANITIZER=$(cached "$dir" COMDB2_SANITIZER), config.sh MEMORY_TOOL_SANITIZER=$MEMORY_TOOL_SANITIZER"
    [[ $(cached "$dir" COMDB2_PER_THREAD_MALLOC) == "$MEMORY_TOOL_PER_THREAD_MALLOC" ]] ||
        fail "$variant: CMake COMDB2_PER_THREAD_MALLOC=$(cached "$dir" COMDB2_PER_THREAD_MALLOC), config.sh MEMORY_TOOL_PER_THREAD_MALLOC=$MEMORY_TOOL_PER_THREAD_MALLOC"
    [[ $(cached "$dir" COMDB2_MEMORY_TEST_HOOKS) == ON ]] || fail "$variant: hooks not on"
    [[ $(cached "$dir" COMDB2_MEMORY_FAULT_INJECTION) == OFF ]] || fail "$variant: fault injection on"
    # Reconfiguring the same directory with the same variant is not a conflict.
    configure "$dir" "${MEMORY_TOOL_CMAKE_ARGS[@]}" || fail "$variant: reconfigure failed: $(tail -n 5 "${dir}.log")"
    pass "$variant: CMake and config.sh agree"
done

# No variant: the ordinary defaults, no hooks.
configure "${tmp}/none" || fail "no variant: configure failed"
[[ $(cached "${tmp}/none" COMDB2_SANITIZER) == none &&
   $(cached "${tmp}/none" COMDB2_PER_THREAD_MALLOC) == ON &&
   $(cached "${tmp}/none" COMDB2_MEMORY_TEST_HOOKS) == OFF &&
   $(cached "${tmp}/none" COMDB2_MEMORY_FAULT_INJECTION) == OFF ]] ||
    fail "no variant: defaults changed: $(command grep '^COMDB2_' "${tmp}/none/CMakeCache.txt" | tr '\n' ' ')"
pass "no variant: defaults unchanged"

# expect_error <name> <message> [cmake args...]
expect_error()
{
    local name=$1 message=$2
    shift 2
    if configure "${tmp}/${name}" "$@"; then
        fail "${name}: accepted $*"
    # CMake rewraps messages
    elif [[ $(tr -s "[:space:]" " " < "${tmp}/${name}.log") != *"$message"* ]]; then
        fail "${name}: message missing '$message': $(tail -n 8 "${tmp}/${name}.log")"
    else
        pass "${name}: rejected"
    fi
}

expect_error unknown "Unknown COMDB2_MEMORY_VARIANT value 'asan-sytem'. Allowed values: asan-system, asan-native, tsan-system, valgrind-system" \
    -DCOMDB2_MEMORY_VARIANT=asan-sytem
expect_error conflict-sanitizer "COMDB2_SANITIZER=none (asan-system needs address-undefined)" \
    -DCOMDB2_MEMORY_VARIANT=asan-system -DCOMDB2_SANITIZER=none
expect_error conflict-allocator "COMDB2_PER_THREAD_MALLOC=ON (valgrind-system needs OFF)" \
    -DCOMDB2_MEMORY_VARIANT=valgrind-system -DCOMDB2_PER_THREAD_MALLOC=ON
expect_error conflict-hooks "COMDB2_MEMORY_TEST_HOOKS=OFF (tsan-system needs ON)" \
    -DCOMDB2_MEMORY_VARIANT=tsan-system -DCOMDB2_MEMORY_TEST_HOOKS=OFF
expect_error fault-injection-needs-hooks "COMDB2_MEMORY_FAULT_INJECTION requires COMDB2_MEMORY_TEST_HOOKS=ON" \
    -DCOMDB2_MEMORY_FAULT_INJECTION=ON

# A build directory configured for one variant is not silently switched.
configure "${tmp}/switch" -DCOMDB2_MEMORY_VARIANT=asan-system || fail "switch: first configure failed"
expect_error switch "COMDB2_SANITIZER=address-undefined (valgrind-system needs none)" \
    -DCOMDB2_MEMORY_VARIANT=valgrind-system

# Equal values are not a conflict; fault injection may be added for harness tests.
if configure "${tmp}/equal" -DCOMDB2_MEMORY_VARIANT=asan-native -DCOMDB2_SANITIZER=address-undefined \
        -DCOMDB2_PER_THREAD_MALLOC=1 -DCOMDB2_MEMORY_FAULT_INJECTION=ON &&
   [[ $(cached "${tmp}/equal" COMDB2_MEMORY_FAULT_INJECTION) == ON ]]; then
    pass "matching low-level values and fault injection accepted"
else
    fail "matching low-level values rejected: $(tail -n 8 "${tmp}/equal.log")"
fi

# The top-level CMakeLists.txt must apply the variant before it declares the
# settings the variant sets (option() would otherwise fix the default first).
line() { command grep -n -m1 -F "$1" "${source_dir}/CMakeLists.txt" | cut -d: -f1; }
variant_line=$(line 'include(Comdb2MemoryVariant)')
ptm_line=$(line 'option(COMDB2_PER_THREAD_MALLOC')
san_line=$(line 'include(Comdb2Sanitizers)')
if [[ -n $variant_line && -n $ptm_line && -n $san_line ]] &&
   ((variant_line < ptm_line && variant_line < san_line)); then
    pass "CMakeLists.txt applies the variant first"
else
    fail "CMakeLists.txt order: variant ${variant_line:-missing}, allocator ${ptm_line:-missing}, sanitizer ${san_line:-missing}"
fi

if [[ $failures -ne 0 ]]; then
    echo "$failures failure(s)"
    exit 1
fi
echo "all variant mapping tests passed"
