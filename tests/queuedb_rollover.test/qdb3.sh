#!/usr/bin/env bash

[[ -n "$3" ]] && exec >$3 2>&1
cdb2sql $SP_OPTIONS - <<EOF
create table foraudit3 {$(cat foraudit.csc2)}\$\$
create procedure const3 version 'const_test' {$(cat const_consumer.lua)}\$\$
create lua consumer const3 on (table foraudit3 for insert)
EOF

tot_had_depth=0

for ((i=0;i<5;++i)); do
    ./qdb3_dml_adds.sh 1000 &
    ./qdb3_dml_cons.sh 1000 &

    cur_had_depth=$(cdb2sql $SP_OPTIONS "select (depth IS NOT NULL) as hadDepth from comdb2_queues where queuename = '__qconst3'")
    if [[ "$cur_had_depth" == "(hadDepth=1)" ]]; then
        tot_had_depth=1
    fi

    ./qdb3_ddl_cons.sh 10 &
    ./qdb3_ddl_proc.sh 10 &
done
wait

echo $tot_had_depth
cdb2sql $SP_OPTIONS "select (count(*) > 0) as hasRows from foraudit3;"
