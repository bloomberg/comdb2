#!/usr/bin/env bash

[[ -n "$3" ]] && exec >$3 2>&1
cdb2sql $SP_OPTIONS - <<EOF
create table foraudit3 {$(cat foraudit.csc2)}\$\$
create procedure const3 version 'const_test' {$(cat const_consumer.lua)}\$\$
create lua consumer const3 on (table foraudit3 for insert)
EOF

for ((i=0;i<5;++i)); do
    ./qdb3_dml_adds.sh 1000 2>&1 >/dev/null &
    ./qdb3_dml_cons.sh 1000 2>&1 >/dev/null &
    ./qdb3_ddl_cons.sh 10 2>&1 >/dev/null &
    ./qdb3_ddl_proc.sh 10 2>&1 >/dev/null &
done
wait
