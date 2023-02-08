#!/usr/bin/env bash

insert_records()
{
    j=$1
    nstop=$2
    let nout=nout+1
    insfl=insert${nout}.out
    retries=0
    echo "Inserting $((nstop-j+1)) records ($j to $nstop)."
    echo "" > $insfl

    while [[ $j -le $nstop ]]; do
        ${CDB2SQL_EXE} ${CDB2_OPTIONS} $dbname default "insert into t2(alltypes_short, alltypes_u_short, alltypes_int, alltypes_u_int, alltypes_longlong, alltypes_float, alltypes_double, alltypes_byte, alltypes_cstring, alltypes_pstring, alltypes_blob, alltypes_datetime, alltypes_datetimeus, alltypes_vutf8, alltypes_intervalym, alltypes_intervalds, alltypes_intervaldsus, alltypes_decimal32, alltypes_decimal64, alltypes_decimal128) values ( $((1-2*(j%2)))$j ,$j ,$((1-2*(j%2)))0000$j ,10000$j ,$((1-2*(j%2)))000000000$j ,$((1-2*(j%2)))00.00$j ,$((1-2*(j%2)))0000$j.0000$j ,x'aabbccddeeffaabb$((j%2))$((j%3))$((j%4))$((j%5))$((j%2))$((j%3))$((j%4))$((j%5))$((j%2))$((j%3))$((j%4))$((j%5))$((j%2))$((j%3))$((j%4))$((j%5))' ,'mycstring$j' ,'mypstring$j' ,x'$((j%2))$((j%3))$((j%4))$((j%5))' ,'$(date +'%Y-%m-%dT%H:%M:%S')' ,'$(date +'%Y-%m-%dT%H:%M:%S')' ,'myvutf8$j' ,$((1-2*(j%2)))$j ,$((1-2*(j%2)))0000$j , $((1-2*(j%2)))0000$j , $((1-2*(j%2)))0000$j , $((1-2*(j%2)))00000000$j , $((1-2*(j%2)))000000000000000$j )"  &>> $insfl

        if [ $? -ne 0 ]; then
            sleep 1
            let retries=retries+1

            if [ $retries -eq 5 ]; then
                return 1
            fi

            continue
        fi

        let j=j+1
        let retries=0
    done
    echo "done inserting round $nout"
}

${CDB2SQL_EXE} ${CDB2_OPTIONS} $dbname default "create table t2 { \
    $(cat alltypes.csc2) }"

insert_records 1 $NRECS
