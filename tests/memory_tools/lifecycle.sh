# Memory-tool lifecycle for one database test. Sourced by tests/runtestcase.
#
#   MEMORY_TOOL_VARIANT=asan-system make -C tests <test>
#
# With MEMORY_TOOL_VARIANT unset every function is a no-op. Otherwise:
#
#   memory_tool_lifecycle_init             validate the variant and COMDB2_EXE,
#                                          create this run's artifact root and
#                                          runtime files, export DEBUG_PREFIX
#   memory_tool_lifecycle_prepare_cluster  copy the runtime files to cluster nodes
#   memory_tool_lifecycle_after_setup      MEMORY_TOOL_INJECT_LEAK=1 (testing the
#                                          harness): make the server leak once
#   memory_tool_lifecycle_note_incomplete  record why coverage is incomplete
#   memory_tool_lifecycle_wait_exit        after a test asked servers to exit,
#                                          wait for them before its fallback kill
#   memory_tool_lifecycle_finalize         wait for recorded servers, collect
#                                          remote records, scan every server
#                                          process, write summary.json
#
# Artifacts: $TESTDIR/.memory-tools/$TESTCASE/<run-id>/, new for every run
# (TESTDIR is shared by the tests of a make run and survives on remote nodes),
# and $TESTDIR/.memory-tools/$TESTCASE/latest points to it. Runtime detector
# settings come only from config.sh.

MEMORY_TOOL_LIFECYCLE_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=config.sh
source "${MEMORY_TOOL_LIFECYCLE_DIR}/config.sh"

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

    MEMORY_TOOL_RUN_ID="${TESTID:-0}.$(date -u +%Y%m%dT%H%M%S).$$"
    local testroot="${TESTDIR}/.memory-tools/${TESTCASE}"
    MEMORY_TOOL_ARTIFACT_ROOT="${testroot}/${MEMORY_TOOL_RUN_ID}"
    memory_tool_configure "$MEMORY_TOOL_VARIANT" "${MEMORY_TOOL_ARTIFACT_ROOT}/probe" || return 1
    memory_tool_check_tools || return 1
    # a server not built for the variant would run uninstrumented and pass
    local problems
    if ! problems=$(memory_tool_check_binary "${COMDB2_EXE-}" server); then
        echo "memory-tools: COMDB2_EXE does not match ${MEMORY_TOOL_VARIANT}:" >&2
        echo "$problems" >&2
        return 1
    fi

    local runtime="${MEMORY_TOOL_ARTIFACT_ROOT}/runtime"
    mkdir -p "$runtime" "${MEMORY_TOOL_ARTIFACT_ROOT}/nodes" || return 1
    ln -sfn "$MEMORY_TOOL_RUN_ID" "${testroot}/latest"
    # a node that sees this file shares TESTDIR with us: nothing to copy there
    touch "${MEMORY_TOOL_ARTIFACT_ROOT}/nodes/.local-${HOSTNAME-}"
    cp -p "${MEMORY_TOOL_LIFECYCLE_DIR}/exec.sh" "${MEMORY_TOOL_LIFECYCLE_DIR}/config.sh" "$runtime/" || return 1
    {
        printf 'MEMORY_TOOL_VARIANT=%q\n' "${MEMORY_TOOL_VARIANT-}"
        printf 'MEMORY_TOOL_ARTIFACT_ROOT=%q\n' "${MEMORY_TOOL_ARTIFACT_ROOT-}"
        printf 'MEMORY_TOOL_RUN_ID=%q\n' "$MEMORY_TOOL_RUN_ID"
        printf 'MEMORY_TOOL_TESTCASE=%q\n' "${TESTCASE-}"
        printf 'MEMORY_TOOL_COMDB2_EXE=%q\n' "${COMDB2_EXE-}"
    } > "${runtime}/runtime.env" || return 1

    local sha=${COMDB2_SOURCE_SHA:-$(git -C "$MEMORY_TOOL_LIFECYCLE_DIR" rev-parse HEAD 2>/dev/null)}
    {
        echo "variant=${MEMORY_TOOL_VARIANT-}"
        echo "testcase=${TESTCASE-}"
        echo "testid=${TESTID-}"
        echo "run_id=${MEMORY_TOOL_RUN_ID}"
        echo "comdb2_exe=${COMDB2_EXE-}"
        echo "source_sha=${sha:-unknown}"
        echo "nodes=${CLUSTER:-${HOSTNAME-}}"
        echo "start=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    } > "${MEMORY_TOOL_ARTIFACT_ROOT-}/run.txt"

    export MEMORY_TOOL_ARTIFACT_ROOT MEMORY_TOOL_RUN_ID
    export DEBUG_PREFIX="${runtime}/exec.sh"
    # Instrumented servers need longer to start, run and finish cleanly; the
    # clean exit must complete for LeakSanitizer and Valgrind to report.
    export MEMORY_TOOL_EXITALARMSEC=${MEMORY_TOOL_EXITALARMSEC:-300}
    export MEMORY_TOOL_CLEAN_EXIT_TIMEOUT=${MEMORY_TOOL_CLEAN_EXIT_TIMEOUT:-$MEMORY_TOOL_DEFAULT_CLEAN_EXIT_TIMEOUT}
    export MEMORY_TOOL_EXIT_REQUEST_TIMEOUT=${MEMORY_TOOL_EXIT_REQUEST_TIMEOUT:-60}
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
    timeout --kill-after=5s 120s "${CDB2SQL_EXE:-cdb2sql}" ${CDB2_OPTIONS-} --tabs "${DBNAME-}" default \
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

memory_tool_lifecycle_shares_testdir()
{
    ssh -n -o StrictHostKeyChecking=no "$1" \
        "test -e '${MEMORY_TOOL_ARTIFACT_ROOT}/nodes/.local-${HOSTNAME-}'"
}

memory_tool_lifecycle_prepare_cluster()
{
    memory_tool_lifecycle_active || return 0
    local node runtime="${MEMORY_TOOL_ARTIFACT_ROOT-}/runtime" rc=0
    for node in ${CLUSTER-}; do
        [[ $node == "${HOSTNAME-}" ]] && continue
        memory_tool_lifecycle_shares_testdir "$node" && continue
        if ! ssh -n -o StrictHostKeyChecking=no "$node" "mkdir -p '$runtime'" ||
           ! scp -q -p -o StrictHostKeyChecking=no "$runtime/exec.sh" "$runtime/config.sh" \
                 "$runtime/runtime.env" "${node}:${runtime}/"; then
            memory_tool_lifecycle_note_incomplete "cannot install memory runtime on ${node}"
            rc=1
        fi
    done
    return $rc
}

# Copy each remote node's records (nodes/ and uncovered/) home. A TESTDIR shared
# between nodes already has them; copying onto itself is skipped.
memory_tool_lifecycle_collect()
{
    local node root=${MEMORY_TOOL_ARTIFACT_ROOT-} d
    for node in ${CLUSTER-}; do
        [[ $node == "${HOSTNAME-}" ]] && continue
        memory_tool_lifecycle_shares_testdir "$node" && continue
        mkdir -p "${root}/collected/${node}"
        if ! scp -q -r -o StrictHostKeyChecking=no "${node}:${root}/." "${root}/collected/${node}/"; then
            memory_tool_lifecycle_note_incomplete "cannot collect memory artifacts from ${node}"
            continue
        fi
        for d in nodes uncovered; do
            [[ -d ${root}/collected/${node}/$d ]] || continue
            mkdir -p "${root}/$d"
            cp -R "${root}/collected/${node}/$d/." "${root}/$d/" ||
                memory_tool_lifecycle_note_incomplete "cannot merge memory artifacts from ${node}"
        done
    done
}

# Servers whose pid the test does not track (e.g. restarts without --pidfile)
# may still be exiting when the test ends, and a server that wrote clean-exit is
# still running its exit-time checks. Wait, up to the clean-exit timeout, for
# every recorded process that is still alive, then for its stderr copy. A pid
# is only ours while its command line still ends with the recorded argv (pids
# are reused). Nothing is killed here; a server still running is recorded.
memory_tool_lifecycle_wait_for_servers()
{
    local root=${MEMORY_TOOL_ARTIFACT_ROOT-} limit=${MEMORY_TOOL_CLEAN_EXIT_TIMEOUT:-120}
    local here node inv pid deadline running
    here=$(hostname)
    here=${here//[^A-Za-z0-9._-]/_}
    deadline=$((SECONDS + limit))
    for inv in "${root}/nodes/${here}"/invocations/*; do
        [[ -d $inv && -r $inv/cmdline ]] || continue
        pid=${inv##*/}
        while memory_tool_lifecycle_is_recorded_process "$pid" "$inv/cmdline" && ((SECONDS < deadline)); do
            sleep 1
        done
        if memory_tool_lifecycle_is_recorded_process "$pid" "$inv/cmdline"; then
            memory_tool_lifecycle_note_incomplete "${here} pid ${pid} still running ${limit}s after the test ended"
            continue
        fi
        while [[ ! -e $inv/stderr-complete ]] && ((SECONDS < deadline)); do
            sleep 1
        done
    done
    for node in ${CLUSTER-}; do
        [[ $node == "${HOSTNAME-}" ]] && continue
        # the same wait on the node, in one ssh session; it prints what is still running
        running=$(ssh -n -o StrictHostKeyChecking=no "$node" "
            t=0
            for inv in '${root}'/nodes/*/invocations/*; do
                [ -r \"\$inv/cmdline\" ] || continue
                pid=\${inv##*/}
                want=\$(tr '\0' '\n' < \"\$inv/cmdline\")
                while :; do
                    have=\$(tr '\0' '\n' 2>/dev/null < /proc/\$pid/cmdline) || break
                    case \"\$have\" in *\"\$want\") ;; *) break ;; esac
                    [ \$t -lt $limit ] || { echo \"$node pid \$pid still running ${limit}s after the test ended\"; break; }
                    sleep 1; t=\$((t+1))
                done
                while [ ! -e \"\$inv/stderr-complete\" ] && [ \$t -lt $limit ]; do sleep 1; t=\$((t+1)); done
            done") || memory_tool_lifecycle_note_incomplete "cannot check servers on ${node}"
        [[ -n $running ]] && memory_tool_lifecycle_note_incomplete "$running"
    done
}

# memory_tool_lifecycle_is_recorded_process <pid> <recorded-cmdline-file>
memory_tool_lifecycle_is_recorded_process()
{
    local have want
    have=$(tr '\0' '\n' 2>/dev/null < "/proc/$1/cmdline") || return 1
    want=$(tr '\0' '\n' < "$2")
    [[ -n $want && $have == *"$want" ]]
}

# memory_tool_lifecycle_finalize <functional-status>
#   functional-status: pass | fail | not-run
# Prints one summary line. Returns 0 only if the memory status is pass, or
# not-run for a test that starts no server (COMDB2_UNITTEST). Runs once.
memory_tool_lifecycle_finalize()
{
    memory_tool_lifecycle_active || return 0
    local root=${MEMORY_TOOL_ARTIFACT_ROOT-}
    [[ -n $root && -d $root ]] || return 1
    [[ -e ${root}/summary.json ]] && return "$(cat "${root}/finalize-rc" 2>/dev/null || echo 1)"

    memory_tool_lifecycle_wait_for_servers
    [[ -n ${CLUSTER-} ]] && memory_tool_lifecycle_collect

    local expect=yes nodes=${CLUSTER:-${HOSTNAME-}} args=() n rc
    [[ ${COMDB2_UNITTEST-} == 1 ]] && expect=no
    args=(scan --root "$root" --variant "$MEMORY_TOOL_VARIANT" --testcase "${TESTCASE-}"
          --run-id "$MEMORY_TOOL_RUN_ID" --expect-server "$expect"
          --kind "$MEMORY_TOOL_REPORT_KIND" --leak-checking "$MEMORY_TOOL_LEAK_CHECKING"
          --functional-status "$1")
    for n in $nodes; do
        args+=(--expected-node "$n")
    done
    python3 "${MEMORY_TOOL_LIFECYCLE_DIR}/check_report.py" "${args[@]}"
    rc=$?
    echo "$rc" > "${root}/finalize-rc"
    return $rc
}
