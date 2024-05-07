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

./qdb_cons.sh nop0 1 9600
./qdb_cons.sh log1 1 9600 $SP_HOST

for ((i=1;i<96;++i)); do
    cdb2sql ${SP_OPTIONS} "exec procedure dml2($i)" 2> /dev/null
    if [[ $? -ne 0 ]]; then
        cdb2sql ${SP_OPTIONS} "exec procedure dml2($i)"
    fi
done

sleep 60

if [ -n "$CLUSTER" ] ; then
    if [ $SP_HOST == `hostname` ]; then
	cp ${TESTDIR}/logs/${DBNAME}.db qdb1-log1.log
    else
	scp ${SP_HOST}:${TESTDIR}/${DBNAME}.db qdb1-log1.log
    fi

    master=$(cdb2sql --tabs $SP_OPTIONS "select comdb2_sysinfo('master')")

    if [ $master == `hostname` ]; then
        cp ${TESTDIR}/logs/${DBNAME}.db qdb1-master-log1.log
    else
        scp ${master}:${TESTDIR}/${DBNAME}.db qdb1-master-log1.log
    fi
else
    cp ${TESTDIR}/logs/${DBNAME}.db qdb1-log1.log
    cp ${TESTDIR}/logs/${DBNAME}.db qdb1-master-log1.log
fi

cdb2sql $SP_OPTIONS "select queuename, depth from comdb2_queues order by queuename;"

cdb2sql $SP_OPTIONS "select count(*) from t1;"
cdb2sql $SP_OPTIONS "select count(*) from t2;"

added_to_log=$(cat qdb1-log1.log | egrep "add, <nil>, [0123456789]{1,4}$" | uniq | wc -l)

if [ $added_to_log -ne 9599 ] ; then
    echo "bad queuedb side-effects, need 9599 entries, got $added_to_log"
    exit 1
fi

if [[ $DBNAME != *"noroll1generated"* ]] ; then
    add_0=$(cat qdb1-master-log1.log | egrep " add_qdb_file: __qnop0 \(0x[0123456789abcdef]{1,}\) ==> SUCCESS \(0\)$" | wc -l)

    if [ $add_0 -ne 1 ] ; then
        echo "missing __qnop0 add_qdb_file, need 1 entries, got $add_0"
        exit 1
    fi

    add_1=$(cat qdb1-master-log1.log | egrep " add_qdb_file: __qlog1 \(0x[0123456789abcdef]{1,}\) ==> SUCCESS \(0\)$" | wc -l)

    if [ $add_1 -ne 1 ] ; then
        echo "missing __qlog1 add_qdb_file, need 1 entries, got $add_1"
        exit 1
    fi

    add_2=$(cat qdb1-master-log1.log | egrep " add_qdb_file: __qdml2 \(0x[0123456789abcdef]{1,}\) ==> SUCCESS \(0\)$" | wc -l)

    if [ $add_2 -ne 1 ] ; then
        echo "missing __qdml2 add_qdb_file, need 1 entries, got $add_2"
        exit 1
    fi

    add_final=$(cat qdb1-master-log1.log | egrep " finalize_add_qdb_file SUCCESS" | wc -l)

    if [ $add_final -ne 3 ] ; then
        echo "missing final add_qdb_file, need 3 entries, got $add_final"
        exit 1
    fi

    del_0=$(cat qdb1-master-log1.log | egrep " del_qdb_file: __qnop0 \(0x[0123456789abcdef]{1,}\) ==> SUCCESS \(0\)$" | wc -l)

    if [ $del_0 -ne 1 ] ; then
        echo "missing __qnop0 del_qdb_file, need 1 entries, got $del_0"
        exit 1
    fi

    del_1=$(cat qdb1-master-log1.log | egrep " del_qdb_file: __qlog1 \(0x[0123456789abcdef]{1,}\) ==> SUCCESS \(0\)$" | wc -l)

    if [ $del_1 -ne 1 ] ; then
        echo "missing __qlog1 del_qdb_file, need 1 entries, got $del_1"
        exit 1
    fi

    del_2=$(cat qdb1-master-log1.log | egrep " del_qdb_file: __qdml2 \(0x[0123456789abcdef]{1,}\) ==> SUCCESS \(0\)$" | wc -l)

    if [ $del_2 -ne 0 ] ; then
        echo "extra __qdml2 del_qdb_file, need 0 entries, got $del_2"
        exit 1
    fi

    del_final=$(cat qdb1-master-log1.log | egrep " finalize_del_qdb_file SUCCESS" | wc -l)

    if [ $del_final -ne 2 ] ; then
        echo "missing final del_qdb_file, need 2 entries, got $del_final"
        exit 1
    fi

    echo "verified that all queuedb rollover entries are correct"
fi
