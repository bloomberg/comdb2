#!/usr/bin/env bash
set -e
function setup {
cdb2sql="${CDB2SQL_EXE} -tabs -s ${CDB2_OPTIONS} ${DBNAME} default"
for q in $($cdb2sql 'select distinct name from comdb2_triggers'); do
    ${cdb2sql} "drop lua trigger $q"
done
${cdb2sql} 'create table t03(i cstring(32))' > /dev/null
${cdb2sql} "create procedure t03consumer version 'test' {}" > /dev/null
${cdb2sql} 'create lua consumer t03consumer on (table t03 for insert)' > /dev/null

pre=$(cat <<EOF
local function main(sql)
local c = db:consumer()
local e = c:get()
e.id = nil
e.sql = sql
local j = db:table_to_json(e)
EOF
)
}

post=$(cat <<EOF
c:emit(j)
end
EOF
)

function generate_sp {
sp=$(cat <<EOF
${pre}
local s = db:prepare(sql)
${begin}
s:${func}()
c:consume()
${commit}
${post}
EOF
)
sps+=("${sp}")

sp=$(cat <<EOF
${pre}
${begin}
local s = db:prepare(sql)
s:${func}()
c:consume()
${commit}
${post}
EOF
)
sps+=("${sp}")

sp=$(cat <<EOF
${pre}
${begin}
c:consume()
local s = db:prepare(sql)
s:${func}()
${commit}
${post}
EOF
)
sps+=("${sp}")

sp=$(cat <<EOF
${pre}
${begin}
local s = db:prepare(sql)
c:consume()
s:${func}()
${commit}
${post}
EOF
)
sps+=("${sp}")
}

function run_test {
    sql="${1}"
    $cdb2sql 'truncate table t03'
    i=0
    for sp in "${sps[@]}"; do
        i=$(($i + 1))
        insert="${2} ${func} ${i}"
        echo "${sp}"
        $cdb2sql "create procedure t03consumer version 'test' {${sp}}" >> /dev/null
        $cdb2sql "insert into t03 values('${insert}')" >> /dev/null
        $cdb2sql "exec procedure t03consumer('$sql')"
    done
}

function test_read {
    run_test 'select * from t03' 'read test'
    j=$($cdb2sql 'select count(*) from t03')
    k=$($cdb2sql 'select depth from comdb2_queues')
    echo "run:$i rows:$j depth:$k"
    [[ $i -ne $sp_count ]] && echo 'Fail to run all procedures' && exit 1
    [[ $j -ne $sp_count ]] && echo 'Unexpected row count' && exit 1
    [[ $k -ne 0 ]] && echo 'Unexpected queue depth' && exit 1
    echo 'pass read'
    echo
}

function test_write {
    run_test 'delete from t03' 'write test'
    j=$($cdb2sql 'select count(*) from t03')
    k=$($cdb2sql 'select depth from comdb2_queues')
    echo "run:$i rows:$j depth:$k"
    [[ $i -ne $sp_count ]] && echo 'Fail to run all procedures' && exit 1
    [[ $j -ne 0 ]] && echo 'Unexpected row count' && exit 1
    [[ $k -ne 0 ]] && echo 'Unexpected queue depth' && exit 1
    echo "pass write"
    echo
}

function generate_sps {
sps=()
begin='--db:begin()'
commit='--db:commit()'
generate_sp

begin='db:begin()'
commit='db:commit()'
generate_sp
}

declare -a sps
setup

func='exec'
generate_sps
sp_count=${#sps[@]}
test_read
test_write

func='fetch'
generate_sps
sp_count=${#sps[@]}
test_read
#Can't fetch 'write' stmts - they don't return a row
#test_write

echo 'passed t03'
