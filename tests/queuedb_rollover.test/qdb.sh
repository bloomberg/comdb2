#!/usr/bin/env bash

[[ -n "$3" ]] && exec >$3 2>&1
cdb2sql --host $SP_HOST $SP_OPTIONS - <<EOF
create table foraudit {$(cat foraudit.csc2)}\$\$
create procedure nop0 version 'noptest' {$(cat nop_consumer.lua)}\$\$
create procedure log1 version 'logtest' {$(cat log_consumer.lua)}\$\$
create lua consumer nop0 on (table foraudit for insert)
create lua consumer log1 on (table foraudit for insert)
EOF

cdb2sql --host $SP_HOST $SP_OPTIONS "put tunable test_log_file 'XXX.comdb2_dedicated_test.log'" > /dev/null

for ((i=1;i<9600;++i)); do
    echo "insert into foraudit values(${i})"
done | cdb2sql --host $SP_HOST $SP_OPTIONS - > /dev/null

for ((i=1;i<9600;++i)); do
    echo "exec procedure nop0()"
done | cdb2sql --host $SP_HOST $SP_OPTIONS - > /dev/null

for ((i=1;i<9600;++i)); do
    echo "exec procedure log1()"
done | cdb2sql --host $SP_HOST $SP_OPTIONS - > /dev/null

cdb2sql --host $SP_HOST $SP_OPTIONS "select queuename, depth from comdb2_queues order by queuename;"

if [ $SP_HOST != `hostname` ]; then
  scp $SP_HOST:$DBDIR/comdb2_dedicated_test.log $DBDIR/comdb2_dedicated_test.log
fi

added_to_log=$(cat $DBDIR/comdb2_dedicated_test.log | egrep "^add, <nil>, [0123456789]{1,4}$" | uniq | wc -l)

if [ $added_to_log -ne 9599 ] ; then
    echo "bad queuedb log, need 9599 entries, got $added_to_log"
    exit 1
fi
