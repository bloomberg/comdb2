#!/usr/bin/env bash

[[ -n "$3" ]] && exec >$3 2>&1
cdb2sql $SP_OPTIONS - <<EOF
create table t1 {$(cat t.csc2)}\$\$
create table t2 {$(cat t.csc2)}\$\$
create table foraudit {$(cat foraudit.csc2)}\$\$
create procedure nop0 version 'noptest' {$(cat nop_consumer.lua)}\$\$
create procedure log1 version 'logtest' {$(cat log_consumer.lua)}\$\$
create procedure dml2 version 'dmltest' {$(cat dml_consumer.lua)}\$\$
create lua consumer nop0 on (table foraudit for insert)
create lua consumer log1 on (table foraudit for insert)
create lua consumer dml2 on (table foraudit for insert)
EOF

cdb2sql $SP_OPTIONS "insert into t1 values('outer t1');"
cdb2sql $SP_OPTIONS "insert into t2 values('outer t2');"

for ((i=1;i<9600;++i)); do
    echo "insert into foraudit values(${i})"
done | cdb2sql $SP_OPTIONS - >/dev/null

for ((i=1;i<9600;++i)); do
    echo "exec procedure nop0()"
done | cdb2sql $SP_OPTIONS - >/dev/null

for ((i=1;i<9600;++i)); do
    echo "exec procedure log1()"
done | cdb2sql --host $SP_HOST $SP_OPTIONS - >/dev/null

for ((i=1;i<96;++i)); do
    echo "exec procedure dml2(${i})"
done | cdb2sql $SP_OPTIONS

if [ $SP_HOST == `hostname` ]; then
    cp ${TESTDIR}/logs/${DBNAME}.db qdb1-log1.log
else
    scp ${SP_HOST}:${TESTDIR}/${DBNAME}.db qdb1-log1.log
fi

cdb2sql $SP_OPTIONS "select queuename, depth from comdb2_queues order by queuename;"

cdb2sql $SP_OPTIONS "select count(*) from t1;"
cdb2sql $SP_OPTIONS "select count(*) from t2;"

added_to_log=$(cat qdb1-log1.log | egrep "add, <nil>, [0123456789]{1,4}$" | uniq | wc -l)

if [ $added_to_log -ne 9599 ] ; then
    echo "bad queuedb side-effects, need 9599 entries, got $added_to_log"
    exit 1
fi
