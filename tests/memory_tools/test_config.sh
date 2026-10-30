#!/usr/bin/env bash
# Tests for config.sh. Run: bash tests/memory_tools/test_config.sh

set -o nounset -o pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=config.sh
source "${here}/config.sh"

failures=0
fail() { echo "FAIL: $*"; failures=$((failures + 1)); }
pass() { echo "ok:   $*"; }

has() # has <needle> <items...>
{
    local needle=$1 x
    shift
    for x in "$@"; do [[ $x == "$needle" ]] && return 0; done
    return 1
}

expect_variant()
{
    local variant=$1 sanitizer=$2 malloc=$3 kind=$4
    if ! memory_tool_configure "$variant" /tmp/memory-tools-test 2>/dev/null; then
        fail "$variant: configure failed"
        return
    fi
    [[ $MEMORY_TOOL_VARIANT == "$variant" ]] || fail "$variant: MEMORY_TOOL_VARIANT=$MEMORY_TOOL_VARIANT"
    has "-DCOMDB2_SANITIZER=${sanitizer}" "${MEMORY_TOOL_CMAKE_ARGS[@]}" ||
        fail "$variant: sanitizer mapping (${MEMORY_TOOL_CMAKE_ARGS[*]})"
    has "-DCOMDB2_PER_THREAD_MALLOC=${malloc}" "${MEMORY_TOOL_CMAKE_ARGS[@]}" ||
        fail "$variant: allocator mapping (${MEMORY_TOOL_CMAKE_ARGS[*]})"
    has -DCOMDB2_TEST=ON "${MEMORY_TOOL_CMAKE_ARGS[@]}" || fail "$variant: COMDB2_TEST missing"
    has -DCOMDB2_MEMORY_PROBES=ON "${MEMORY_TOOL_CMAKE_ARGS[@]}" || fail "$variant: COMDB2_MEMORY_PROBES missing"
    [[ ${#MEMORY_TOOL_CMAKE_ARGS[@]} -eq 4 ]] || fail "$variant: expected 4 CMake args, got ${#MEMORY_TOOL_CMAKE_ARGS[@]}"
    [[ $MEMORY_TOOL_REPORT_KIND == "$kind" ]] || fail "$variant: report kind $MEMORY_TOOL_REPORT_KIND"
    has clean "${MEMORY_TOOL_EXPECTED_PROBES[@]}" || fail "$variant: clean control missing"
    [[ ${#MEMORY_TOOL_FAILURE_EXIT_CODES[@]} -gt 0 ]] || fail "$variant: no failure exit codes"

    local p
    for p in "${MEMORY_TOOL_EXPECTED_PROBES[@]}"; do
        memory_tool_probe_expectation "$p" >/dev/null || fail "$variant: probe $p has no expectation"
    done
    # Every runtime setting is one NAME=value word; nothing needs eval.
    local e
    for e in "${MEMORY_TOOL_ENV[@]}"; do
        [[ $e =~ ^[A-Z_]+=[^[:space:]]+$ ]] || fail "$variant: malformed env entry '$e'"
    done
    pass "$variant"
}

for v in "${MEMORY_TOOL_VARIANTS[@]}"; do
    memory_tool_configure "$v" /tmp/x >/dev/null 2>&1 || fail "listed variant $v does not configure"
done
[[ ${MEMORY_TOOL_VARIANTS[*]} == "asan-system asan-native tsan-system valgrind-system" ]] ||
    fail "variant list changed: ${MEMORY_TOOL_VARIANTS[*]}"

expect_variant asan-system address-undefined OFF sanitizer
expect_variant asan-native address-undefined ON sanitizer
expect_variant tsan-system thread OFF sanitizer
expect_variant valgrind-system none OFF valgrind

# asan-system: LSan on and authoritative, both leak probes required.
memory_tool_configure asan-system /tmp/memory-tools-test
[[ ${MEMORY_TOOL_ENV[0]} == ASAN_OPTIONS=detect_leaks=1:* ]] || fail "asan-system: detect_leaks=1 missing"
has leak "${MEMORY_TOOL_EXPECTED_PROBES[@]}" || fail "asan-system: leak probe missing"
has comdb2-allocator-leak "${MEMORY_TOOL_EXPECTED_PROBES[@]}" || fail "asan-system: allocator leak probe missing"
[[ $MEMORY_TOOL_LEAK_CHECKING == authoritative ]] || fail "asan-system: leak checking $MEMORY_TOOL_LEAK_CHECKING"
[[ ${MEMORY_TOOL_EXPECTED_RUNTIME[*]} == "libasan libubsan" ]] || fail "asan-system: runtime ${MEMORY_TOOL_EXPECTED_RUNTIME[*]}"
pass "asan-system leak checking"

# asan-native: must not claim leak authority.
memory_tool_configure asan-native /tmp/memory-tools-test
if has leak "${MEMORY_TOOL_EXPECTED_PROBES[@]}" || has comdb2-allocator-leak "${MEMORY_TOOL_EXPECTED_PROBES[@]}"; then
    fail "asan-native claims a leak probe"
fi
[[ ${MEMORY_TOOL_ENV[0]} == ASAN_OPTIONS=detect_leaks=0:* ]] || fail "asan-native: detect_leaks=0 missing"
[[ $MEMORY_TOOL_LEAK_CHECKING == disabled ]] || fail "asan-native: leak checking $MEMORY_TOOL_LEAK_CHECKING"
pass "asan-native does not claim leak checking"

# tsan-system
memory_tool_configure tsan-system /tmp/memory-tools-test
[[ ${MEMORY_TOOL_EXPECTED_PROBES[*]} == "data-race clean" ]] || fail "tsan-system probes ${MEMORY_TOOL_EXPECTED_PROBES[*]}"
[[ ${MEMORY_TOOL_EXPECTED_RUNTIME[*]} == libtsan ]] || fail "tsan-system runtime"
pass "tsan-system probes"

# valgrind-system: no sanitizer anywhere, Valgrind prefix, Valgrind required.
memory_tool_configure valgrind-system "/tmp/memory-tools-test"
[[ ${#MEMORY_TOOL_ENV[@]} -eq 0 ]] || fail "valgrind-system sets sanitizer env: ${MEMORY_TOOL_ENV[*]}"
[[ ${#MEMORY_TOOL_EXPECTED_RUNTIME[@]} -eq 0 ]] || fail "valgrind-system expects a sanitizer runtime"
[[ ${MEMORY_TOOL_COMMAND_PREFIX[0]} == valgrind ]] || fail "valgrind-system prefix"
has --error-exitcode=97 "${MEMORY_TOOL_COMMAND_PREFIX[@]}" || fail "valgrind-system error exit code"
has "--log-file=/tmp/memory-tools-test/valgrind.%p.log" "${MEMORY_TOOL_COMMAND_PREFIX[@]}" ||
    fail "valgrind-system log file is not one array element"
has valgrind "${MEMORY_TOOL_REQUIRED_TOOLS[@]}" || fail "valgrind-system does not require valgrind"
for t in gcc cmake python3 readelf ldd timeout; do
    has "$t" "${MEMORY_TOOL_REQUIRED_TOOLS[@]}" || fail "valgrind-system does not require $t"
done
[[ $MEMORY_TOOL_MAX_OPEN_FILES -gt 0 ]] || fail "valgrind-system has no open-files ceiling"
pass "valgrind-system"

# Server flavour (exec.sh): findings never change the exit status or stop the
# server early, except ASan memory errors; the probe flavour stays strict.
memory_tool_configure asan-system /tmp/memory-tools-test server || fail "server flavour rejected"
[[ ${MEMORY_TOOL_ENV[*]} == *"LSAN_OPTIONS=exitcode=0:"* ]] || fail "server: LSan changes exit status"
[[ ${MEMORY_TOOL_ENV[*]} == *"UBSAN_OPTIONS=halt_on_error=0:"* ]] || fail "server: UBSan halts"
[[ ${MEMORY_TOOL_ENV[0]} == *":halt_on_error=1:abort_on_error=1:"* ]] || fail "server: ASan errors must still stop"
memory_tool_configure asan-native /tmp/memory-tools-test server
[[ ${MEMORY_TOOL_ENV[*]} == *"UBSAN_OPTIONS=halt_on_error=0:"* ]] || fail "server asan-native: UBSan halts"
memory_tool_configure tsan-system /tmp/memory-tools-test server
[[ ${MEMORY_TOOL_ENV[0]} == TSAN_OPTIONS=halt_on_error=0:exitcode=0:* ]] || fail "server: TSan halts or sets exit code"
memory_tool_configure valgrind-system /tmp/memory-tools-test server
has --error-exitcode=97 "${MEMORY_TOOL_COMMAND_PREFIX[@]}" && fail "server: Valgrind sets an error exit code"
[[ ${MEMORY_TOOL_COMMAND_PREFIX[-1]} == "--log-file=/tmp/memory-tools-test/valgrind.%p.log" ]] ||
    fail "server: Valgrind prefix broken: ${MEMORY_TOOL_COMMAND_PREFIX[*]}"
memory_tool_configure asan-system /tmp/memory-tools-test probe
[[ ${MEMORY_TOOL_ENV[*]} == *"LSAN_OPTIONS=exitcode=23:"* && ${MEMORY_TOOL_ENV[*]} == *"UBSAN_OPTIONS=halt_on_error=1:"* ]] ||
    fail "probe flavour not strict"
memory_tool_configure asan-system /tmp/memory-tools-test wat 2>/dev/null && fail "unknown flavour accepted"
pass "server and probe runtime flavours"

# Unknown variant and bad arguments fail with a clear message.
errfile=$(mktemp)
# Not in $(...): the stale-state check needs the variables of this shell.
if memory_tool_configure wat /tmp/out 2> "$errfile"; then
    fail "unknown variant accepted"
elif err=$(cat "$errfile"); [[ $err != *"Allowed values: asan-system asan-native tsan-system valgrind-system"* ]]; then
    fail "unknown variant message does not list allowed values: $err"
elif [[ -n $MEMORY_TOOL_VARIANT || ${#MEMORY_TOOL_CMAKE_ARGS[@]} -ne 0 ]]; then
    fail "unknown variant left stale configuration"
else
    pass "unknown variant rejected"
fi
rm -f "$errfile"
memory_tool_configure asan-system 2>/dev/null && fail "missing log dir accepted"
memory_tool_configure asan-system relative/dir 2>/dev/null && fail "relative log dir accepted"
memory_tool_configure asan-system "/tmp/with space" 2>/dev/null && fail "log dir with whitespace accepted"
memory_tool_configure asan-system "/tmp/with:colon" 2>/dev/null && fail "log dir with ':' accepted"
pass "bad arguments rejected"

# Probe binary location follows the CMake target naming.
[[ $(memory_tool_probe_binary /b heap-use-after-free) == /b/tests/memory_tools/memprobe_heap_use_after_free ]] ||
    fail "probe binary path"
memory_tool_probe_expectation no-such-probe >/dev/null && fail "unknown probe has an expectation"
[[ $(memory_tool_probe_expectation signed-overflow) == undefined-behavior ]] || fail "signed-overflow expectation"
pass "probe helpers"

# Tool check reports what is missing.
memory_tool_configure valgrind-system /tmp/memory-tools-test
MEMORY_TOOL_REQUIRED_TOOLS=(definitely-not-a-real-tool-xyz)
if err=$(memory_tool_check_tools 2>&1); then
    fail "missing tool not detected"
elif [[ $err != *definitely-not-a-real-tool-xyz* ]]; then
    fail "missing tool not named: $err"
else
    pass "missing tool detected"
fi

if [[ $failures -ne 0 ]]; then
    echo "$failures failure(s)"
    exit 1
fi
echo "all config tests passed"
