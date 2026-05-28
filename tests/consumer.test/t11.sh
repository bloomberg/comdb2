#!/usr/bin/env bash
set -e

# Test: consumer SP downgrades from snapshot isolation.
# Proves the SP is NOT running under snapshot by starting the consumer
# BEFORE inserting rows, then showing it consumes events that were
# added after invocation.

cdb2sql="${CDB2SQL_EXE} -tabs -s ${CDB2_OPTIONS} ${DBNAME} default"

set_tunable_all_nodes() {
    local tunable="$1"
    local val="$2"
    if [ -n "${CLUSTER}" ]; then
        for node in ${CLUSTER}; do
            ${CDB2SQL_EXE} -tabs -s ${CDB2_OPTIONS} --host ${node} ${DBNAME} "PUT TUNABLE ${tunable} '${val}'" 2>/dev/null || true
        done
    else
        ${cdb2sql} "PUT TUNABLE ${tunable} '${val}'"
    fi
}

orig=$(${cdb2sql} "select value from comdb2_tunables where name='sql_tranlevel_default'")

# Map display value back to tunable token
case "${orig}" in
    BLOCKSQL)          orig_tok="blocksock" ;;
    "READ COMMITTED")  orig_tok="recom" ;;
    SNAPSHOT)          orig_tok="snapshot" ;;
    SERIALIZABLE)      orig_tok="serial" ;;
    *)                 echo "unknown level: ${orig}"; exit 1 ;;
esac

cleanup() {
    for q in $(${cdb2sql} "select name from comdb2_triggers where name='snapconsumer'" 2>/dev/null); do
        ${cdb2sql} "drop lua trigger ${q}" 2>/dev/null || true
    done
    ${cdb2sql} "drop table if exists snap_t" 2>/dev/null || true
    ${cdb2sql} "drop table if exists snap_results" 2>/dev/null || true
    set_tunable_all_nodes sql_tranlevel_default "${orig_tok}"
}
trap cleanup EXIT

set_tunable_all_nodes sql_tranlevel_default snapshot
lvl=$(${cdb2sql} "select value from comdb2_tunables where name='sql_tranlevel_default'")
if [ "${lvl}" != "SNAPSHOT" ]; then
    echo "FAIL: could not set sql_tranlevel_default to SNAPSHOT"
    exit 1
fi
echo "default isolation set to: ${lvl}"

${cdb2sql} 'drop table if exists snap_t'
${cdb2sql} 'drop table if exists snap_results'
for q in $(${cdb2sql} "select name from comdb2_triggers where name='snapconsumer'"); do
    ${cdb2sql} "drop lua trigger ${q}"
done

${cdb2sql} 'create table snap_t(i int)' > /dev/null
${cdb2sql} 'create table snap_results(i int)' > /dev/null

# Consumer SP: loops consuming events and writing each value to snap_results.
# Under snapshot isolation this would fail at db:consumer().
# Even if it somehow got past that, a frozen snapshot would not see
# events queued after the SP was invoked.
${cdb2sql} <<'EOF' > /dev/null
CREATE PROCEDURE snapconsumer VERSION 'test' {
local function main()
    local consumer = db:consumer()
    while true do
        local event = consumer:get()
        db:begin()
        consumer:next()
        db:exec("insert into snap_results values(" .. tostring(event.new.i) .. ")")
        db:commit()
    end
end
}$$
CREATE LUA CONSUMER snapconsumer ON (TABLE snap_t FOR INSERT)
EOF

# Start the consumer BEFORE any rows exist.
# It will block on consumer:get() waiting for events.
${cdb2sql} "exec procedure snapconsumer()" > /dev/null 2>&1 &
CONSUMER_PID=$!
sleep 1

# Insert rows AFTER the consumer was already invoked.
${cdb2sql} "insert into snap_t values(1),(2),(3)" > /dev/null

# Wait for the queue to drain
for i in $(seq 1 20); do
    depth=$(${cdb2sql} "select depth from comdb2_queues where spname='snapconsumer'")
    if [ "${depth}" = "0" ]; then
        break
    fi
    sleep 1
done

# Insert more rows — still after original invocation
${cdb2sql} "insert into snap_t values(4),(5)" > /dev/null

for i in $(seq 1 20); do
    depth=$(${cdb2sql} "select depth from comdb2_queues where spname='snapconsumer'")
    if [ "${depth}" = "0" ]; then
        break
    fi
    sleep 1
done

kill ${CONSUMER_PID} 2>/dev/null
wait ${CONSUMER_PID} 2>/dev/null || true

# Verify all 5 rows were consumed — including ones added after invocation
cnt=$(${cdb2sql} "select count(*) from snap_results")
echo "consumed count: ${cnt}"
if [ "${cnt}" != "5" ]; then
    echo "FAIL: expected 5 consumed events, got ${cnt}"
    ${cdb2sql} "select * from snap_results order by i"
    exit 1
fi

vals=$(${cdb2sql} "select i from snap_results order by i" | tr '\n' ',')
echo "consumed values: ${vals}"
expected="1,2,3,4,5,"
if [ "${vals}" != "${expected}" ]; then
    echo "FAIL: expected ${expected} got ${vals}"
    exit 1
fi

echo "passed t11 - snapshot consumer downgrade"
