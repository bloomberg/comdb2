#!/usr/bin/env bash

[[ -n "$3" ]] && exec >$3 2>&1
cdb2sql $SP_OPTIONS - <<EOF
create table foraudit3 {$(cat foraudit.csc2)}\$\$
create procedure const3 version 'const_test' {$(cat const_consumer.lua)}\$\$
create lua consumer const3 on (table foraudit3 for insert)
EOF

for ((i=0;i<5;++i)); do
    ./qdb3_dml_adds.sh 1000 $i &
    ./qdb3_dml_cons.sh 1000 $i &
    ./qdb3_ddl_cons.sh 10 &
    ./qdb3_ddl_proc.sh 10 &
    wait
done
wait

cdb2sql $SP_OPTIONS "select count(*) as row_count from foraudit3;"
