#!/usr/bin/env bash

# make sure analyze can bypass sqlthd pool

# first wait for lingering threads to exit
sleep 30
host=$(${CDB2SQL_EXE} -s -tabs ${CDB2_OPTIONS} ${DBNAME} default "select host from comdb2_cluster order by is_master limit 1")
# allow only one thread
${CDB2SQL_EXE} -s -tabs --host $host ${CDB2_OPTIONS} ${DBNAME} - <<'EOF'
create table t15(a int unique)$$
exec procedure sys.cmd.send('sqlenginepool maxt 1')
EOF

${CDB2SQL_EXE} -s -tabs --host $host ${CDB2_OPTIONS} ${DBNAME} "select sleep(10)" >/dev/null &
sleep 3

start=$(date +%s)
${CDB2SQL_EXE} -s -tabs --host $host ${CDB2_OPTIONS} ${DBNAME} "analyze t15"
end=$(date +%s)
time=$((end-start))

${CDB2SQL_EXE} -s -tabs --host $host ${CDB2_OPTIONS} ${DBNAME} "exec procedure sys.cmd.send('sqlenginepool maxt 48')"
if [[ $time > 3 ]]; then
    echo "FAILED"
    exit 1
fi

echo "SUCCESS"
sleep 10
exit 0
