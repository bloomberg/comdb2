#!/bin/bash

cdb2sql="${CDB2SQL_EXE} -r 1 ${CDB2_OPTIONS} $DBNAME default"
c=8
echo > insert.out
echo > insertq

insert_generated() {
    local table=$1
    local pk=$2
    if [[ -z "$j" ]]; then
        c=$(($c+1))
        j=$c
    fi
    echo "insert into $table (alltypes_short, alltypes_u_short, alltypes_int, alltypes_u_int, alltypes_longlong, alltypes_float, alltypes_double, alltypes_byte, alltypes_cstring, alltypes_pstring, alltypes_blob, alltypes_datetime, alltypes_datetimeus, alltypes_vutf8, alltypes_intervalym, alltypes_intervalds, alltypes_intervaldsus, alltypes_decimal32, alltypes_decimal64, alltypes_decimal128) values ( $pk ,$j ,$((1-2*(j%2)))0000$j ,10000$j ,$((1-2*(j%2)))000000000$j ,$((1-2*(j%2)))00.00$j ,$((1-2*(j%2)))0000$j.0000$j ,x'aabbccddeeffaabb$((j%2))$((j%3))$((j%4))$((j%5))$((j%2))$((j%3))$((j%4))$((j%5))$((j%2))$((j%3))$((j%4))$((j%5))$((j%2))$((j%3))$((j%4))$((j%5))' ,'mycstring$j' ,'mypstring$j' ,x'$((j%2))$((j%3))$((j%4))$((j%5))' ,'2024-01-15T12:30:00' ,'2024-01-15T12:30:00' ,'myvutf8$j' ,$((1-2*(j%2)))$j ,$((1-2*(j%2)))0000$j , $((1-2*(j%2)))0000$j , $((1-2*(j%2)))0000$j , $((1-2*(j%2)))00000000$j , $((1-2*(j%2)))000000000000000$j )"
}

fields=$(awk '/alltypes_/ && !/PK/ {print $2}' alltypes.csc2 | sed 's/\[.*\]//g')
comma_fields=$(echo $fields | sed 's/ /, /g')
bind_values=$(echo $fields | sed 's/^/@/; s/ /, @/g')
insert_generated "<TBL>" "<FLD>" >> insertq 2>&1
values=$(tr '\n' ' ' < insertq | sed 's/.*values[[:space:]]*([[:space:]]*//; s/[[:space:]]*)[[:space:]]*$//')

run_trigger_test() {
    local bind_or_plain=$1
    $cdb2sql "drop table if exists t_trig"
    $cdb2sql "drop table if exists t1"
    $cdb2sql "create table t_trig { $(cat alltypes.csc2) }"
    $cdb2sql "create table t1 { $(cat alltypes.csc2) }"
    $cdb2sql "create procedure ins_trig version 'instrig' { $(cat trigger.lua) }"
    $cdb2sql "create lua trigger ins_trig for (table t1 on insert on update on delete)"
    if [[ "$bind_or_plain" == "bind" ]]; then
        insert_with_bind_alltypes t1
    else
        run_alltypes_insert t1
    fi
    # run_alltypes_insert t1
    res=$($cdb2sql "select * from t1 where alltypes_short=$c")
    res_trig=$($cdb2sql "select * from t_trig where alltypes_short=$c")
    if [[ "$res" != "$res_trig" ]]; then
        echo "Trigger test failed: t1 contents do not match t_trig: $res vs $res_trig"
        exit 1
    else
        echo "Trigger test passed!!"
    fi
    $cdb2sql "drop procedure ins_trig version 'instrig'"
    $cdb2sql "drop lua trigger ins_trig"
}

run_consumer_test() {
    local bind_or_plain=$1
    $cdb2sql "drop table if exists t1"
    $cdb2sql "create table t1 { $(cat alltypes.csc2) }"
    $cdb2sql "create procedure watch version 'basic' { $(cat watch.lua) }"
    $cdb2sql "create lua consumer watch for (table t1 on insert)"

    $cdb2sql "exec procedure watch()" &
    watch_pid=$!
    sleep 1  
    
    if [[ "$bind_or_plain" == "bind" ]]; then
        insert_with_bind_alltypes t1
    else
        run_alltypes_insert t1
    fi

    sleep 5  
    kill $watch_pid 2>/dev/null
    wait $watch_pid 2>/dev/null
    $cdb2sql "drop procedure watch version 'basic'"
    $cdb2sql "drop lua consumer watch"
}

run_alltypes_insert() {
    local table=$1
    c=$(($c+1))
    $cdb2sql "$(cat insertq | sed "s/<TBL>/$table/g" | sed "s/<FLD>/$c/g")"
}

insert_with_bind_alltypes() {
    local table=$1
    c=$(($c+1))
    echo "$(cat insert_bind | sed "s/<TBL>/$1/g" | sed "s/<FLD>/$c/g")" > insert_bind_final
    $cdb2sql >> insert.out <<EOF
$(cat insert_bind_final)
EOF
    if [ $? -ne 0 ]; then
        echo "Insert with bind failed"
        exit 1
    fi
}

cleanup () {
    rm -rf insert_bind_final
}

run_sp_cstr_test() {
    local bind_or_plain=$1
    $cdb2sql "drop table if exists t1"
    $cdb2sql "drop table if exists t1_cstr"
    $cdb2sql "create table t1 { $(cat alltypes.csc2) }"
    $cdb2sql "create table t1_cstr { $(cat alltypes_cstr.csc2) }"
    
    $cdb2sql "create procedure sp_cstr version 'spcstr' { $(cat sp.lua) }"
    
    if [[ "$bind_or_plain" == "bind" ]]; then
        insert_with_bind_alltypes t1
    else
        run_alltypes_insert t1
    fi

    $cdb2sql "exec procedure sp_cstr()"
    
    
    echo "t1 rows:"
    $cdb2sql "select alltypes_short from t1"
    echo "t1_cstr rows:"
    $cdb2sql "select alltypes_short from t1_cstr"

    $cdb2sql "drop procedure sp_cstr version 'spcstr'"
}

check_pk_and_type() {
    out=$(grep -oE "type='[^']+', alltypes_short=[0-9]+" consumer.out | sort)
    exp=$(grep -oE "type='[^']+', alltypes_short=[0-9]+" consumer.exp | sort)
    
    if [[ "$out" == "$exp" ]]; then
        return 0
    else
        echo "Mismatch in type/alltypes_short:"
        diff <(echo "$out") <(echo "$exp")
        exit 1
    fi
}

run_trigger_test
run_consumer_test > consumer.out 2>&1
run_trigger_test bind
run_consumer_test bind >> consumer.out 2>&1

if check_pk_and_type &>/dev/null; then
    echo "Consumer test passed!"
else
    echo "Consumer test failed: see diff, "
    check_pk_and_type
    exit 1
fi


run_sp_cstr_test
run_sp_cstr_test bind

cleanup
