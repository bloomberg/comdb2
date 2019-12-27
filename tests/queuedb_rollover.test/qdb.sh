#!/usr/bin/env bash

[[ -n "$3" ]] && exec >$3 2>&1
cdb2sql $SP_OPTIONS - <<EOF
create table foraudit {$(cat foraudit.csc2)}\$\$
create procedure nop0 version 'noptest' {$(cat nop_consumer.lua)}\$\$
create procedure log1 version 'logtest' {$(cat log_consumer.lua)}\$\$
create lua consumer nop0 on (table foraudit for insert and update and delete)
create lua consumer log1 on (table foraudit for insert and update and delete)
EOF

for ((i=1;i<9600;++i)); do
    echo "insert into foraudit values(${i})"
done | cdb2sql $SP_OPTIONS - > /dev/null

for ((i=1;i<9600;++i)); do
    echo "exec procedure nop0()"
    echo "exec procedure log1()"
done | cdb2sql $SP_OPTIONS - > /dev/null
