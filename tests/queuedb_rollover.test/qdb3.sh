#!/usr/bin/env bash

[[ -n "$3" ]] && exec >$3 2>&1
cdb2sql $SP_OPTIONS - <<EOF
create table foraudit3 {$(cat foraudit.csc2)}\$\$
create procedure const3 version 'const_test' {$(cat const_consumer.lua)}\$\$
create lua consumer const3 on (table foraudit3 for insert)
EOF

something_was_enqueued=0
something_was_dequeued=0

for ((i=0;i<5;++i)); do
    ./qdb3_dml_adds.sh 1000 &
    ./qdb3_dml_cons.sh 1000 &

    had_enq=$(cdb2sql $SP_OPTIONS "select (total_enqueued > 0) as hadEnq from comdb2_queues where queuename = '__qconst3'")

    if [[ "$had_enq" == "(hadEnq=1)" ]]; then
        something_was_enqueued=1
    fi

    had_deq=$(cdb2sql $SP_OPTIONS "select (total_dequeued > 0) as hadDeq from comdb2_queues where queuename = '__qconst3'")

    if [[ "$had_deq" == "(hadDeq=1)" ]]; then
        something_was_enqueued=1
    fi

    ./qdb3_ddl_cons.sh 10 &
    ./qdb3_ddl_proc.sh 10 &
done
wait

echo something_was_enqueued=$something_was_enqueued
echo something_was_dequeued=$something_was_dequeued
cdb2sql $SP_OPTIONS "select (count(*) > 0) as hasRows from foraudit3;"
