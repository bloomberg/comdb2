#!/usr/bin/env bash

[[ -n "$3" ]] && exec >$3 2>&1
cdb2sql $SP_OPTIONS - <<EOF
create table foraudit4 {$(cat foraudit.csc2)}\$\$
create procedure nop2 version 'noptest' {$(cat nop_consumer.lua)}\$\$
create lua consumer nop2 on (table foraudit4 for insert)
EOF

for ((i=0;i<50;++i)); do
    ./qdb4_adds.sh 96 &
    ./qdb_cons.sh nop2 0 $((96 * 2)) &
    wait
    cdb2sql $SP_OPTIONS "select depth from comdb2_queues where queuename = '__qnop2';"
done

cdb2sql $SP_OPTIONS "select depth from comdb2_queues where queuename = '__qnop2';"
cdb2sql $SP_OPTIONS "select count(*) as row_count from foraudit4;"
