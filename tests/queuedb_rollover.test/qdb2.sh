#!/usr/bin/env bash

[[ -n "$3" ]] && exec >$3 2>&1
cdb2sql $SP_OPTIONS - <<EOF
create table foraudit2 {$(cat foraudit.csc2)}\$\$
create procedure nop1 version 'noptest' {$(cat nop_consumer.lua)}\$\$
create lua consumer nop1 on (table foraudit2 for insert and update and delete)
EOF

./qdb2_adds.sh 96000 &
sleep 2

cdb2sql $SP_OPTIONS "select depth > 0 from comdb2_queues where queuename = '__qnop1';"

./qdb2_cons.sh 96000 &
sleep 2

cdb2sql $SP_OPTIONS "select depth > 0 from comdb2_queues where queuename = '__qnop1';"

wait
cdb2sql $SP_OPTIONS "select depth from comdb2_queues where queuename = '__qnop1';"
