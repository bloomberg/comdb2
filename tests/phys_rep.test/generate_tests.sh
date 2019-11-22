#!/usr/bin/env bash

# This should be used under runit's context
# NRECS=1000
# NRUNS=10000

# TEST 1: Table generation

# TEST 2: Insert, Update, Delete
set -x

for i in $(seq 1 $NRECS); do
    echo "insert into t1 (id, a, b, c, d, e, f, g, h, i, j) values ($i, 1, 2, 3, 4, 5, 6.000000, 7.000000, 'eight', x'99', now());"
done > 2-1-insert.src.sql

for i in $(seq 1 $NRUNS); do
    what=$(($RANDOM % 3))
    id=$(($RANDOM % $NRECS))
    case $what in
        0)  echo "insert into t1 (id, a, b, c, d, e, f, g, h, i, j) values ($id, 1, 2, 3, 4, 5, 6.000000, 7.000000, 'eight', x'99', now());"
            ;;
        1)  echo "delete from t1 where id = $id limit 1"
            ;;
        2)  echo "update t1 set a=a+1, b=b+2 where id=$id"
            ;;
    esac
done > 2-update.src.sql

# TEST 3: Truncate
echo 'truncate t1' > 3-2-truncate.src.sql

# TEST 4: Lua testing
printf "create procedure foo version '1' { local function main(x) db:emit(x) end }" > 4-1-sp-create.src.sql
printf "create procedure foo version '2' { local function main(x) db:dmit(x) db:emit(x) end }" > 4-2-sp-add.src.sql
printf "put default procedure foo '2'" > 4-3-sp-version.src.sql

# TEST 5: Schema changes
printf "alter table t2 options rec none, blobfield none, ipu off, isc off,\
    odh off, rebuild { $(cat alltypes_full.csc2) } \$\$\n" > 5-3-alter-opts.src.sql
let j=$NRECS+1
printf "insert into t2(alltypes_short, alltypes_u_short, alltypes_int, alltypes_u_int, alltypes_longlong, alltypes_float, alltypes_double, alltypes_byte, alltypes_cstring, alltypes_pstring, alltypes_blob, alltypes_datetime, alltypes_datetimeus, alltypes_vutf8, alltypes_intervalym, alltypes_intervalds, alltypes_intervaldsus, alltypes_decimal32, alltypes_decimal64, alltypes_decimal128) values ( $((1-2*(j%2)))$j ,$j ,$((1-2*(j%2)))0000$j ,10000$j ,$((1-2*(j%2)))000000000$j ,$((1-2*(j%2)))00.00$j ,$((1-2*(j%2)))0000$j.0000$j ,x'aabbccddeeffaabb$((j%2))$((j%3))$((j%4))$((j%5))$((j%2))$((j%3))$((j%4))$((j%5))$((j%2))$((j%3))$((j%4))$((j%5))$((j%2))$((j%3))$((j%4))$((j%5))' ,'mycstring$j' ,'mypstring$j' ,x'$((j%2))$((j%3))$((j%4))$((j%5))' ,'$(date +'%Y-%m-%dT%H:%M:%S')' ,'$(date +'%Y-%m-%dT%H:%M:%S')' ,'myvutf8$j' ,$((1-2*(j%2)))$j ,$((1-2*(j%2)))0000$j , $((1-2*(j%2)))0000$j , $((1-2*(j%2)))0000$j , $((1-2*(j%2)))00000000$j , $((1-2*(j%2)))000000000000000$j )" >> 5-3-alter-opts.src.sql

let j=j+1
printf "alter table t2 options rec none, blobfield none, ipu off, isc off, \
    rebuild { $(cat alltypes_full.csc2) } \$\$\n" > 5-4-alter-opts.src.sql
printf "insert into t2(alltypes_short, alltypes_u_short, alltypes_int, alltypes_u_int, alltypes_longlong, alltypes_float, alltypes_double, alltypes_byte, alltypes_cstring, alltypes_pstring, alltypes_blob, alltypes_datetime, alltypes_datetimeus, alltypes_vutf8, alltypes_intervalym, alltypes_intervalds, alltypes_intervaldsus, alltypes_decimal32, alltypes_decimal64, alltypes_decimal128) values ( $((1-2*(j%2)))$j ,$j ,$((1-2*(j%2)))0000$j ,10000$j ,$((1-2*(j%2)))000000000$j ,$((1-2*(j%2)))00.00$j ,$((1-2*(j%2)))0000$j.0000$j ,x'aabbccddeeffaabb$((j%2))$((j%3))$((j%4))$((j%5))$((j%2))$((j%3))$((j%4))$((j%5))$((j%2))$((j%3))$((j%4))$((j%5))$((j%2))$((j%3))$((j%4))$((j%5))' ,'mycstring$j' ,'mypstring$j' ,x'$((j%2))$((j%3))$((j%4))$((j%5))' ,'$(date +'%Y-%m-%dT%H:%M:%S')' ,'$(date +'%Y-%m-%dT%H:%M:%S')' ,'myvutf8$j' ,$((1-2*(j%2)))$j ,$((1-2*(j%2)))0000$j , $((1-2*(j%2)))0000$j , $((1-2*(j%2)))0000$j , $((1-2*(j%2)))00000000$j , $((1-2*(j%2)))000000000000000$j )" >> 5-4-alter-opts.src.sql

let j=j+1
printf "alter table t2 options rebuild { $(cat alltypes_full.csc2) } \$\$\n" > 5-5-alter-opts.src.sql
printf "insert into t2(alltypes_short, alltypes_u_short, alltypes_int, alltypes_u_int, alltypes_longlong, alltypes_float, alltypes_double, alltypes_byte, alltypes_cstring, alltypes_pstring, alltypes_blob, alltypes_datetime, alltypes_datetimeus, alltypes_vutf8, alltypes_intervalym, alltypes_intervalds, alltypes_intervaldsus, alltypes_decimal32, alltypes_decimal64, alltypes_decimal128) values ( $((1-2*(j%2)))$j ,$j ,$((1-2*(j%2)))0000$j ,10000$j ,$((1-2*(j%2)))000000000$j ,$((1-2*(j%2)))00.00$j ,$((1-2*(j%2)))0000$j.0000$j ,x'aabbccddeeffaabb$((j%2))$((j%3))$((j%4))$((j%5))$((j%2))$((j%3))$((j%4))$((j%5))$((j%2))$((j%3))$((j%4))$((j%5))$((j%2))$((j%3))$((j%4))$((j%5))' ,'mycstring$j' ,'mypstring$j' ,x'$((j%2))$((j%3))$((j%4))$((j%5))' ,'$(date +'%Y-%m-%dT%H:%M:%S')' ,'$(date +'%Y-%m-%dT%H:%M:%S')' ,'myvutf8$j' ,$((1-2*(j%2)))$j ,$((1-2*(j%2)))0000$j , $((1-2*(j%2)))0000$j , $((1-2*(j%2)))0000$j , $((1-2*(j%2)))00000000$j , $((1-2*(j%2)))000000000000000$j )" >> 5-5-alter-opts.src.sql

printf "drop table t1\ndrop table t2" > 6-1-drop.src.sql

#
# create queries
#

# Tables shared across tests
echo 'select * from t1' | tee 1-create-table.query.sql 2-1-insert.query.sql \
    2-2-update.query.sql 3-1-truncate.query.sql 3-2-truncate.query.sql 

printf 'select * from t2' | tee 5-1-create-alltypes.query.sql 5-2-alter.query.sql \
    5-3-alter-opts.query.sql 5-4-alter-opts.query.sql 5-5-alter-opts.src.sql

print "select * from t1\nselect * from t2" > 6-1-drop.query.sql

# TEST 4
printf 'select * from comdb2_procedures' | tee 4-1-sp-create.query.sql 4-2-sp-version.query.sql
printf "exec procedure foo('hi')" >> 4-2-sp-version.query.sql 
printf "exec procedure foo('hello!')" > 4-3-sp-version.query.sql

