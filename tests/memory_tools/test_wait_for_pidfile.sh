#!/usr/bin/env bash
# Tests for tests/tools/wait_for_pidfile.sh, the bounded wait tests/setup uses
# for a single-node server's pidfile.
# Run: bash tests/memory_tools/test_wait_for_pidfile.sh

set -o nounset -o pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../tools/wait_for_pidfile.sh
source "${here}/../tools/wait_for_pidfile.sh"
work=$(mktemp -d "${TMPDIR:-/tmp}/wait-for-pidfile.XXXXXX")
trap 'kill $(jobs -p) 2>/dev/null; rm -rf "$work"' EXIT

failures=0
fail() { echo "FAIL: $*"; failures=$((failures + 1)); }
pass() { echo "ok:   $*"; }

# server <pidfile> <delay> <then>: a fake server that writes its pidfile after
# <delay> seconds (never, if "never"), then keeps running or exits ("die" exits
# instead of writing it)
server()
{
    local pidfile=$1 delay=$2 then=$3
    (
        [[ $delay == never ]] && exec sleep 30
        sleep "$delay"
        [[ $then == die ]] && exit 3
        echo $BASHPID > "$pidfile"
        exec sleep 30
    ) &
}

# timed <out-var> <command...>: run the command, leave elapsed whole seconds in
# <out-var> and return its status
timed()
{
    local -n elapsed=$1
    shift
    local start=$SECONDS rc
    "$@"
    rc=$?
    elapsed=$((SECONDS - start))
    return $rc
}

# Fast normal server: returns as soon as the pidfile is there, well before the
# fixed 2 s the old setup slept.
server "$work/fast.pid" 0.3 run
if timed t wait_for_pidfile "$work/fast.pid" $! 10 > "$work/out" && ((t <= 1)); then
    pass "fast server: returned after ${t}s"
else
    fail "fast server: rc/elapsed ${t}s: $(cat "$work/out")"
fi

# Slow server (an instrumented one): pidfile after more than 2 s, before the
# scaled deadline.
server "$work/slow.pid" 3 run
if timed t wait_for_pidfile "$work/slow.pid" $! $((10 * 3)) > "$work/out" && ((t >= 2 && t <= 5)); then
    pass "slow server: waited ${t}s"
else
    fail "slow server: rc/elapsed ${t}s: $(cat "$work/out")"
fi

# Never writes it: fails at the deadline, not before and not much after.
server "$work/never.pid" never run
if timed t wait_for_pidfile "$work/never.pid" $! 2 > "$work/out"; then
    fail "server without a pidfile accepted"
elif ((t < 2 || t > 4)) || ! command grep -q "did not write .* within 2s" "$work/out"; then
    fail "never: elapsed ${t}s, message: $(cat "$work/out")"
else
    pass "server that never writes its pidfile: failed after ${t}s"
fi

# Dies before writing it: fails at once, long before the deadline.
server "$work/dies.pid" 0.3 die
if timed t wait_for_pidfile "$work/dies.pid" $! 60 > "$work/out"; then
    fail "server that died accepted"
elif ((t > 2)) || ! command grep -q "exited before writing" "$work/out"; then
    fail "dies: elapsed ${t}s, message: $(cat "$work/out")"
else
    pass "server that dies early: failed after ${t}s"
fi

# An empty pidfile (being written) is not yet a pidfile.
: > "$work/empty.pid"
server "$work/unused.pid" never run
if timed t wait_for_pidfile "$work/empty.pid" $! 1 > "$work/out"; then
    fail "empty pidfile accepted"
else
    pass "empty pidfile is not accepted"
fi

if [[ $failures -ne 0 ]]; then
    echo "$failures failure(s)"
    exit 1
fi
echo "all wait_for_pidfile tests passed"
