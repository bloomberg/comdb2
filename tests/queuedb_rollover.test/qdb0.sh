#!/usr/bin/env bash

[[ -n "$3" ]] && exec >$3 2>&1
cdb2sql $SP_OPTIONS - <<EOF
create table foraudit0 {$(cat foraudit.csc2)}\$\$
create procedure nopZ version 'noptest' {$(cat nop_consumer.lua)}\$\$
create lua consumer nopZ on (table foraudit0 for insert)
EOF

for ((i=0;i<2;++i)); do
    ./qdb0_adds.sh 96 &
    ./qdb0_cons.sh 96 &
    wait
    cdb2sql $SP_OPTIONS "select depth from comdb2_queues where queuename = '__qnopZ';"
done

cdb2sql $SP_OPTIONS "select depth from comdb2_queues where queuename = '__qnopZ';"
cdb2sql $SP_OPTIONS "select count(*) as row_count from foraudit0;"
