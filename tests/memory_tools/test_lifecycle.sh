#!/usr/bin/env bash
# Tests for exec.sh, lifecycle.sh and before_db_kill.sh with fake executables.
# Run: bash tests/memory_tools/test_lifecycle.sh

set -o nounset -o pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
work=$(mktemp -d "${TMPDIR:-/tmp}/memory-tools-lifecycle.XXXXXX")
trap 'rm -rf "$work"' EXIT

failures=0
fail() { echo "FAIL: $*"; failures=$((failures + 1)); }
pass() { echo "ok:   $*"; }

for f in exec.sh lifecycle.sh before_db_kill.sh; do
    bash -n "${here}/$f" || fail "bash -n $f"
done
pass "bash -n"

# Fake comdb2: reports its pid and environment, writes to stderr, then waits.
mkdir -p "$work/bin" "$work/shim"
cat > "$work/bin/comdb2" <<'EOF'
#!/usr/bin/env bash
echo "pid=$$ invdir=${COMDB2_MEMORY_INVOCATION_DIR-} asan=${ASAN_OPTIONS-}"
echo "fake comdb2 stderr line" >&2
[[ ${1-} == --exit-now ]] && exit 0
exec sleep 60
EOF
chmod +x "$work/bin/comdb2"
# hostname with characters that must not reach a path
printf '#!/bin/sh\necho "we!rd:host"\n' > "$work/shim/hostname"
chmod +x "$work/shim/hostname"

# Runs in a subshell so each case starts from a clean environment.
init_case()
{
    unset DEBUGGER MEMORY_TOOL_ARTIFACT_ROOT MEMORY_TOOL_INJECT_LEAK
    export TESTDIR="$work/testdir" TESTCASE=$1 TESTID=1 HOSTNAME="we!rd:host" CLUSTER=
    export COMDB2_EXE="$work/bin/comdb2" DEBUG_PREFIX=
    # shellcheck source=lifecycle.sh
    source "${here}/lifecycle.sh"
}

# 1. no-op without MEMORY_TOOL_VARIANT
(
    failures=0  # this case only; reported to the parent by its exit status
    init_case noop
    unset MEMORY_TOOL_VARIANT
    memory_tool_lifecycle_init || fail "no-op init returned nonzero"
    [[ -z $DEBUG_PREFIX ]] || fail "no-op init set DEBUG_PREFIX=$DEBUG_PREFIX"
    [[ ! -e $TESTDIR/.memory-tools ]] || fail "no-op init created artifacts"
    memory_tool_lifecycle_finalize pass || fail "no-op finalize returned nonzero"
    [[ $(memory_tool_lifecycle_scale_timeout 5m) == 5m ]] || fail "no-op scaled a timeout"
    MEMORY_TOOL_VARIANT= "${here}/before_db_kill.sh" db || fail "no-op before_db_kill"
    exit $((failures > 0))
) && pass "no-op without MEMORY_TOOL_VARIANT" || fail "no-op without MEMORY_TOOL_VARIANT"

# 2. unknown variant and DEBUGGER conflict
(
    failures=0  # this case only; reported to the parent by its exit status
    init_case badvariant
    export MEMORY_TOOL_VARIANT=wat
    memory_tool_lifecycle_init 2>/dev/null && fail "unknown variant accepted"
    export MEMORY_TOOL_VARIANT=asan-system DEBUGGER=gdb
    err=$(memory_tool_lifecycle_init 2>&1) && fail "DEBUGGER conflict accepted"
    [[ $err == *DEBUGGER* ]] || fail "DEBUGGER conflict message: $err"
    true
    exit $((failures > 0))
) && pass "unknown variant and DEBUGGER conflict rejected" || fail "unknown variant and DEBUGGER conflict rejected"

# 3. runtime creation, exec.sh pid preservation, metadata, stderr copy
(
    failures=0  # this case only; reported to the parent by its exit status
    init_case pidcase
    export MEMORY_TOOL_VARIANT=asan-system PATH="$work/shim:$PATH"
    memory_tool_lifecycle_init || { fail "init"; exit 1; }
    root=$MEMORY_TOOL_ARTIFACT_ROOT
    [[ $root == "$TESTDIR/.memory-tools/pidcase" ]] || fail "artifact root $root"
    [[ $DEBUG_PREFIX == "$root/runtime/exec.sh" && -x $DEBUG_PREFIX ]] || fail "DEBUG_PREFIX $DEBUG_PREFIX"
    for f in exec.sh config.sh runtime.env; do
        [[ -f $root/runtime/$f ]] || fail "runtime file $f missing"
    done
    [[ $MEMORY_TOOL_EXITALARMSEC -gt 10 && $MEMORY_TOOL_CLEAN_EXIT_TIMEOUT -gt 10 ]] ||
        fail "memory-mode exit timeouts not raised"
    [[ $(memory_tool_lifecycle_scale_timeout 5m) == 16m ]] || fail "asan timeout scale"

    cd "$work" || exit 1
    ${DEBUG_PREFIX} "$COMDB2_EXE" mydb --lrl "a b.lrl" > out.log 2> err.log &
    launched=$!
    for _ in 1 2 3 4 5 6 7 8 9 10; do [[ -s out.log ]] && break; sleep 0.2; done
    [[ $(cut -d' ' -f1 out.log) == "pid=$launched" ]] || fail "target pid differs from \$!: $(cat out.log)"
    [[ $(cat out.log) == *"asan=detect_leaks=1"* ]] || fail "ASAN_OPTIONS not applied"
    inv="$root/nodes/we_rd_host/invocations/$launched"
    [[ -d $inv/detector ]] || fail "invocation dir $inv (node name not sanitized?)"
    python3 -c 'import json,sys; d=json.load(open(sys.argv[1]));
assert d["pid"] == int(sys.argv[2]) and d["argv"][-1] == "a b.lrl" and d["variant"] == "asan-system", d' \
        "$inv/launch.json" "$launched" || fail "launch.json"
    kill -9 "$launched"
    wait "$launched" 2>/dev/null
    kill -0 "$launched" 2>/dev/null && fail "kill -9 \$! did not kill the target"
    [[ $(cat err.log) == "fake comdb2 stderr line" ]] || fail "stderr not forwarded: $(cat err.log)"
    [[ $(cat "$inv/stderr.log") == "fake comdb2 stderr line" ]] || fail "stderr not copied"

    # anything other than comdb2 runs unchanged and unrecorded
    [[ $(${DEBUG_PREFIX} /bin/echo passthrough) == passthrough ]] || fail "passthrough"
    [[ $(ls "$root/nodes/we_rd_host/invocations" | wc -l) -eq 1 ]] || fail "passthrough was recorded"
    exit $((failures > 0))
) && pass "runtime files, pid preservation, metadata, stderr copy, passthrough" || fail "runtime files, pid preservation, metadata, stderr copy, passthrough"

# 4. finalize: clean process passes; killed without checkpoint is incomplete;
#    result is computed once
(
    failures=0  # this case only; reported to the parent by its exit status
    init_case finalize
    export MEMORY_TOOL_VARIANT=asan-system PATH="$work/shim:$PATH"
    memory_tool_lifecycle_init || exit 1
    cd "$work" || exit 1
    COMDB2_MEMORY_INVOCATION_DIR= ${DEBUG_PREFIX} "$COMDB2_EXE" --exit-now > /dev/null 2>&1
    inv=$(ls -d "$MEMORY_TOOL_ARTIFACT_ROOT"/nodes/*/invocations/*)
    touch "$inv/clean-exit"           # what the real server writes
    out=$(memory_tool_lifecycle_finalize pass) || fail "clean finalize: $out"
    [[ $out == *"memory pass"* ]] || fail "clean summary: $out"

    init_case killed
    export MEMORY_TOOL_VARIANT=asan-system PATH="$work/shim:$PATH"
    memory_tool_lifecycle_init || exit 1
    ${DEBUG_PREFIX} "$COMDB2_EXE" --exit-now > /dev/null 2>&1
    out=$(memory_tool_lifecycle_finalize pass) && fail "no clean exit accepted: $out"
    [[ $out == *"memory incomplete"* ]] || fail "incomplete summary: $out"
    memory_tool_lifecycle_finalize pass > /dev/null && fail "second finalize changed result"
    true
    exit $((failures > 0))
) && pass "finalize pass / incomplete / once" || fail "finalize pass / incomplete / once"

# 5. before_db_kill: checkpoint outcomes
(
    failures=0  # this case only; reported to the parent by its exit status
    init_case prekill
    export MEMORY_TOOL_VARIANT=asan-system
    memory_tool_lifecycle_init || exit 1
    fake="$work/fake-cdb2sql"
    for reply in "memdebug leakcheck: ok" "memdebug leakcheck: leaks found"; do
        printf '#!/bin/sh\necho "%s"\n' "$reply" > "$fake"; chmod +x "$fake"
        CDB2SQL_EXE=$fake "${here}/before_db_kill.sh" db node1 || fail "exit code"
    done
    [[ ! -e $MEMORY_TOOL_ARTIFACT_ROOT/incomplete.txt ]] || fail "good checkpoint recorded as incomplete"
    printf '#!/bin/sh\necho "memdebug leakcheck: unavailable (no LeakSanitizer)"\n' > "$fake"
    CDB2SQL_EXE=$fake "${here}/before_db_kill.sh" db node1
    command grep -q "has no LeakSanitizer" "$MEMORY_TOOL_ARTIFACT_ROOT/incomplete.txt" ||
        fail "asan-system without LSan not recorded"
    printf '#!/bin/sh\necho "connection refused" >&2; exit 1\n' > "$fake"
    CDB2SQL_EXE=$fake "${here}/before_db_kill.sh" db || fail "failed checkpoint changed exit code"
    command grep -q "no checkpoint: connection refused" "$MEMORY_TOOL_ARTIFACT_ROOT/incomplete.txt" ||
        fail "failed checkpoint not recorded"

    init_case prekill-tsan
    export MEMORY_TOOL_VARIANT=tsan-system
    memory_tool_lifecycle_init || exit 1
    printf '#!/bin/sh\necho "memdebug leakcheck: unavailable (no LeakSanitizer)"\n' > "$fake"
    CDB2SQL_EXE=$fake "${here}/before_db_kill.sh" db
    [[ ! -e $MEMORY_TOOL_ARTIFACT_ROOT/incomplete.txt ]] || fail "tsan intent marker recorded as incomplete"
    exit $((failures > 0))
) && pass "before_db_kill outcomes" || fail "before_db_kill outcomes"

if [[ $failures -ne 0 ]]; then
    echo "$failures failure(s)"
    exit 1
fi
echo "all lifecycle tests passed"
