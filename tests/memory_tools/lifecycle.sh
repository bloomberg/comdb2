# Memory-tool lifecycle for one database test. Sourced by tests/runtestcase.
#
#   MEMORY_TOOL_VARIANT=asan-system make -C tests <test>
#
# With MEMORY_TOOL_VARIANT unset every function is a no-op. Otherwise:
#
#   memory_tool_lifecycle_init             validate, create the artifact root and
#                                          runtime files, export DEBUG_PREFIX
#   memory_tool_lifecycle_prepare_cluster  copy the runtime files to cluster nodes
#   memory_tool_lifecycle_after_setup      MEMORY_TOOL_INJECT_LEAK=1 (testing the
#                                          harness): make the server leak once
#   memory_tool_lifecycle_note_incomplete  record why coverage is incomplete
#   memory_tool_lifecycle_wait_exit        after a test asked servers to exit,
#                                          wait for them before its fallback kill
#   memory_tool_lifecycle_finalize         collect remote artifacts, scan every
#                                          server process, write summary.json
#
# Artifacts: ${TESTDIR-}/.memory-tools/${TESTCASE-}/ (TESTDIR is shared by every test
# of a make run). Runtime detector settings come only from config.sh.

MEMORY_TOOL_LIFECYCLE_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

memory_tool_lifecycle_active()
{
    [[ -n ${MEMORY_TOOL_VARIANT-} ]]
}

memory_tool_lifecycle_init()
{
    memory_tool_lifecycle_active || return 0

    if [[ -n ${DEBUGGER-} ]]; then
        echo "memory-tools: DEBUGGER=${DEBUGGER-} cannot be combined with MEMORY_TOOL_VARIANT" >&2
        return 1
    fi
    if [[ -z ${TESTDIR-} || -z ${TESTCASE-} ]]; then
        echo "memory-tools: TESTDIR and TESTCASE must be set" >&2
        return 1
    fi

    # shellcheck source=config.sh
    source "${MEMORY_TOOL_LIFECYCLE_DIR}/config.sh"
    MEMORY_TOOL_ARTIFACT_ROOT="${TESTDIR-}/.memory-tools/${TESTCASE-}"
    memory_tool_configure "${MEMORY_TOOL_VARIANT-}" "${MEMORY_TOOL_ARTIFACT_ROOT-}/probe" || return 1

    local runtime="${MEMORY_TOOL_ARTIFACT_ROOT-}/runtime"
    rm -rf "${MEMORY_TOOL_ARTIFACT_ROOT-}"
    mkdir -p "$runtime" "${MEMORY_TOOL_ARTIFACT_ROOT-}/nodes" || return 1
    cp -p "${MEMORY_TOOL_LIFECYCLE_DIR}/exec.sh" "${MEMORY_TOOL_LIFECYCLE_DIR}/config.sh" "$runtime/" || return 1
    {
        printf 'MEMORY_TOOL_VARIANT=%q\n' "${MEMORY_TOOL_VARIANT-}"
        printf 'MEMORY_TOOL_ARTIFACT_ROOT=%q\n' "${MEMORY_TOOL_ARTIFACT_ROOT-}"
        printf 'MEMORY_TOOL_TESTCASE=%q\n' "${TESTCASE-}"
        printf 'MEMORY_TOOL_COMDB2_EXE=%q\n' "${COMDB2_EXE-}"
    } > "${runtime}/runtime.env" || return 1

    local sha=${COMDB2_SOURCE_SHA:-$(git -C "$MEMORY_TOOL_LIFECYCLE_DIR" rev-parse HEAD 2>/dev/null)}
    {
        echo "variant=${MEMORY_TOOL_VARIANT-}"
        echo "testcase=${TESTCASE-}"
        echo "testid=${TESTID-}"
        echo "source_sha=${sha:-unknown}"
        echo "nodes=${CLUSTER:-${HOSTNAME-}}"
        echo "start=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    } > "${MEMORY_TOOL_ARTIFACT_ROOT-}/run.txt"

    export MEMORY_TOOL_ARTIFACT_ROOT
    export DEBUG_PREFIX="${runtime}/exec.sh"
    # Instrumented servers need longer to start, run and finish cleanly; the
    # clean exit must complete for LeakSanitizer and Valgrind to report.
    export MEMORY_TOOL_EXITALARMSEC=${MEMORY_TOOL_EXITALARMSEC:-300}
    export MEMORY_TOOL_CLEAN_EXIT_TIMEOUT=${MEMORY_TOOL_CLEAN_EXIT_TIMEOUT:-$MEMORY_TOOL_DEFAULT_CLEAN_EXIT_TIMEOUT}
    return 0
}

# Scale a timeout like "5m" by the variant's slowdown.
memory_tool_lifecycle_scale_timeout()
{
    local t=$1
    if ! memory_tool_lifecycle_active || [[ $t != *m ]]; then
        echo "$t"
        return
    fi
    echo "$(( ${t%m} * MEMORY_TOOL_SLOWDOWN + 1 ))m"
}

memory_tool_lifecycle_after_setup()
{
    memory_tool_lifecycle_active || return 0
    [[ ${MEMORY_TOOL_INJECT_LEAK-} == 1 ]] || return 0
    echo "inject_leak=1" >> "${MEMORY_TOOL_ARTIFACT_ROOT-}/run.txt"
    # shellcheck disable=SC2086
    "${CDB2SQL_EXE:-cdb2sql}" ${CDB2_OPTIONS-} --tabs "${DBNAME-}" default \
        'exec procedure sys.cmd.send("memdebug testleak")'
}

memory_tool_lifecycle_wait_exit()
{
    memory_tool_lifecycle_active || return 0
    local pid deadline=$((SECONDS + ${MEMORY_TOOL_CLEAN_EXIT_TIMEOUT:-120}))
    for pid in "$@"; do
        while kill -0 "$pid" 2>/dev/null && ((SECONDS < deadline)); do
            sleep 1
        done
    done
}

memory_tool_lifecycle_note_incomplete()
{
    memory_tool_lifecycle_active || return 0
    [[ -n ${MEMORY_TOOL_ARTIFACT_ROOT-} ]] || return 0
    echo "$*" >> "${MEMORY_TOOL_ARTIFACT_ROOT-}/incomplete.txt"
}

memory_tool_lifecycle_prepare_cluster()
{
    memory_tool_lifecycle_active || return 0
    local node runtime="${MEMORY_TOOL_ARTIFACT_ROOT-}/runtime" rc=0
    for node in ${CLUSTER-}; do
        [[ $node == "${HOSTNAME-}" ]] && continue
        if ! ssh -n -o StrictHostKeyChecking=no "$node" "mkdir -p '$runtime'" ||
           ! scp -q -p -o StrictHostKeyChecking=no "$runtime/exec.sh" "$runtime/config.sh" \
                 "$runtime/runtime.env" "${node}:${runtime}/"; then
            memory_tool_lifecycle_note_incomplete "cannot install memory runtime on ${node}"
            rc=1
        fi
    done
    return $rc
}

# Copy each remote node's records home. A TESTDIR shared between nodes already
# has them; copying onto itself is skipped.
memory_tool_lifecycle_collect()
{
    local node root=${MEMORY_TOOL_ARTIFACT_ROOT-}
    for node in ${CLUSTER-}; do
        [[ $node == "${HOSTNAME-}" ]] && continue
        if ssh -n -o StrictHostKeyChecking=no "$node" "test -e '$root/nodes/.local-${HOSTNAME-}'"; then
            continue
        fi
        mkdir -p "${root}/collected/${node}"
        if ! scp -q -r -o StrictHostKeyChecking=no "${node}:${root}/nodes/." "${root}/collected/${node}/"; then
            memory_tool_lifecycle_note_incomplete "cannot collect memory artifacts from ${node}"
            continue
        fi
        cp -R "${root}/collected/${node}/." "${root}/nodes/" ||
            memory_tool_lifecycle_note_incomplete "cannot merge memory artifacts from ${node}"
    done
}

# Servers whose pid the test does not track (restarts without --pidfile) may
# still be finishing the clean exit unsetup asked for. Wait for every recorded,
# still-running server; one that does not exit is recorded and killed.
memory_tool_lifecycle_wait_for_servers()
{
    local root=${MEMORY_TOOL_ARTIFACT_ROOT-} here node inv pid pids=() deadline
    here=$(hostname)
    here=${here//[^A-Za-z0-9._-]/_}
    for inv in "${root}/nodes/${here}"/invocations/*; do
        [[ -d $inv && ! -e $inv/clean-exit ]] || continue
        pid=${inv##*/}
        [[ -r /proc/$pid/cmdline ]] && tr '\0' ' ' < "/proc/$pid/cmdline" | command grep -q comdb2 &&
            pids+=("$pid")
    done
    deadline=$((SECONDS + ${MEMORY_TOOL_CLEAN_EXIT_TIMEOUT:-120}))
    for pid in "${pids[@]}"; do
        while kill -0 "$pid" 2>/dev/null && ((SECONDS < deadline)); do
            sleep 1
        done
        if kill -0 "$pid" 2>/dev/null; then
            memory_tool_lifecycle_note_incomplete "${here} pid ${pid} still running at the end of the test; killed"
            kill -9 "$pid"
        fi
    done
    for node in ${CLUSTER-}; do
        [[ $node == "${HOSTNAME-}" ]] && continue
        # same wait on the node, over one ssh session; it prints what it killed
        local killed
        killed=$(ssh -n -o StrictHostKeyChecking=no "$node" "
            for inv in '${root}'/nodes/*/invocations/*; do
                [ -d \"\$inv\" ] && [ ! -e \"\$inv/clean-exit\" ] || continue
                pid=\${inv##*/}
                grep -q comdb2 /proc/\$pid/cmdline 2>/dev/null || continue
                t=0
                while kill -0 \$pid 2>/dev/null && [ \$t -lt ${MEMORY_TOOL_CLEAN_EXIT_TIMEOUT:-120} ]; do sleep 1; t=\$((t+1)); done
                if kill -0 \$pid 2>/dev/null; then
                    echo \"$node pid \$pid still running at the end of the test; killed\"
                    kill -9 \$pid
                fi
            done") || memory_tool_lifecycle_note_incomplete "cannot check servers on ${node}"
        [[ -n $killed ]] && memory_tool_lifecycle_note_incomplete "$killed"
    done
}

# memory_tool_lifecycle_finalize <functional-status>
#   functional-status: pass | fail | not-run
# Prints one summary line. Returns 0 only if memory status is pass (or the test
# never started a server and memory status is not-run). Runs once per test.
memory_tool_lifecycle_finalize()
{
    memory_tool_lifecycle_active || return 0
    local root=${MEMORY_TOOL_ARTIFACT_ROOT-}
    [[ -n $root && -d $root ]] || return 1
    [[ -e ${root}/summary.json ]] && return "$(cat "${root}/finalize-rc" 2>/dev/null || echo 1)"

    memory_tool_lifecycle_wait_for_servers
    if [[ -n ${CLUSTER-} ]]; then
        touch "${root}/nodes/.local-${HOSTNAME-}"
        memory_tool_lifecycle_collect
    fi

    local nodes=${CLUSTER:-${HOSTNAME-}} args=() rc
    args=(scan --root "$root" --variant "${MEMORY_TOOL_VARIANT-}" --testcase "${TESTCASE-}"
          --kind "$MEMORY_TOOL_REPORT_KIND" --leak-checking "$MEMORY_TOOL_LEAK_CHECKING"
          --functional-status "$1")
    local n
    for n in $nodes; do
        args+=(--expected-node "$n")
    done
    python3 "${MEMORY_TOOL_LIFECYCLE_DIR}/check_report.py" "${args[@]}"
    rc=$?
    echo "$rc" > "${root}/finalize-rc"
    return $rc
}
