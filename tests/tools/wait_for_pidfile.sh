# wait_for_pidfile <pidfile> <pid> <timeout-seconds>
#
# Waits for the server started as <pid> to write <pidfile>. Returns 0 as soon
# as it has, and 1 with a message if <pid> exits first or the timeout passes.
# Instrumented servers (memory-tool runs) start much more slowly than normal
# ones, so callers scale the timeout instead of sleeping a fixed time.
wait_for_pidfile()
{
    local pidfile=$1 pid=$2 timeout=$3
    local deadline=$((SECONDS + timeout))

    while [[ ! -s $pidfile ]]; do
        if ! kill -0 "$pid" 2>/dev/null; then
            echo "server pid $pid exited before writing $pidfile"
            return 1
        fi
        if ((SECONDS >= deadline)); then
            echo "server pid $pid did not write $pidfile within ${timeout}s"
            return 1
        fi
        sleep 0.25
    done
    return 0
}
