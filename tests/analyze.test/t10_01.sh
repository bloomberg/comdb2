#!/usr/bin/env bash
# This script runs analyze and sends analyze abort to cancel it 
# The analyse abort command needs to be sent to the same node 
# which is performing analyze.

args=$1
dbname=$2
maxt10=200
runtime=180
wpid=-1
rpid=-1
rrpid=-1
apid=-1

$CDB2SQL_EXE ${CDB2_OPTIONS} $dbname default - <<EOF > /dev/null 2>&1
drop table if exists t10
create table t10 {
schema
{
    int  id
    blob b1 null=yes
}
keys
{
    dup "ID" = id
}
}\$\$
insert into t10 values (0, NULL)
insert into t10 values (1, x'aa')
insert into t10 values (2, NULL)
insert into t10 values (3, x'aa')
insert into t10 values (4, NULL)
insert into t10 values (5, x'aa')
insert into t10 values (6, NULL)
insert into t10 values (7, x'aa')
insert into t10 values (8, NULL)
insert into t10 values (9, x'aa')
create procedure test version 'sptest' {
local function read()
    db:begin()
    db:exec("selectv * from t10 where id > 20 order by id")
    local rc = db:commit()
    if rc ~= 0 then
        return -1
    end
    return 0
end
local function write()
    local stmts = {
        "insert into t10(id, b1) values (ABS(RANDOM() % 200), RANDOMBLOB(16))",
        "delete from t10 where id = ABS(RANDOM() % 200)",
        "update t10 set id = ABS((RANDOM()+id) % 200), b1 = RANDOMBLOB(16) where id = ABS(RANDOM() % 200)"
    }
    for _, s in ipairs(stmts) do
        db:begin()
        db:exec(s)
        if db:commit() ~= 0 then
            return -1
        end
    end
    return 0
end
local function main(t)
    local rc1 = read()
    local rc2 = write()
    if rc1 ~= 0 then
        db:emit("sp read failed")
        return -1
    end
    if rc2 ~= 0 then
        db:emit("sp write failed")
        return -1
    end
    db:emit("sp passed")
    return 0
end
}\$\$
put default procedure test 'sptest'
EOF

hexarr=(0 1 2 3 4 5 6 7 8 9 A B C D E F)

# Create a random blob 
function randbl
{
    typeset sz=$1
    local i=0
    res=""
    while [ $i -lt $sz ] ; do
        res="$res${hexarr[$((RANDOM % 16))]}"
        let i=i+1
    done
    echo $res

    return 0
}

function insert_rand_t10
{
    typeset db=$1
    typeset id
    typeset bl
    typeset out
    typeset bsz

    # Create a random id for t10
    id=$(( RANDOM % (maxt10 * 2) ))
    # Create a random blob for t10
    bsz=$(( (RANDOM % 17) * 2 ))
    bl=$(randbl $bsz)

    out=$($CDB2SQL_EXE ${CDB2_OPTIONS} $db default "insert into t10(id, b1) values ($id, x'$bl')" 2>&1)
    if [[ $? != 0 ]]; then
        echo "insert_rand_t10 failed, $out"
        exit 1
    fi

    return 0
}

function update_rand_t10
{
    typeset db=$1
    typeset id
    typeset upid
    typeset nullblob=0
    typeset out
    typeset bsz

    # Create a random id for t10
    id=$(( RANDOM % (maxt10 * 2) )) ; upid=$(( RANDOM % (maxt10 * 2) ))
    # Create a random blob for t10
    bsz=$(( (RANDOM % 17) * 2 ))
    bl=$(randbl $bsz)
    # Make the blob a NULL once every 5 times
    if [[ "0" == $(( RANDOM % 5 )) ]]; then
        nullblob=1
    fi

    if [[ "1" == "$nullblob" ]]; then
        out=$($CDB2SQL_EXE ${CDB2_OPTIONS} $db default "update t10 set id=$upid, b1=NULL where id=$id" 2>&1)
    else
        out=$($CDB2SQL_EXE ${CDB2_OPTIONS} $db default "update t10 set id=$upid, b1=x'$bl' where id=$id" 2>&1)
    fi

    if [[ $? != 0 ]]; then
        echo "update_rand_t10 failed, $out"
        exit 1
    fi

    return 0
}

function delete_rand_t10
{
    typeset db=$1
    typeset id
    typeset x
    typeset out

    # Create a random id for t10
    x=$RANDOM ; id=$(( x % (maxt10 * 2) ))

    out=$($CDB2SQL_EXE ${CDB2_OPTIONS} $db default "delete from t10 where id=$id" 2>&1)

    if [[ $? != 0 ]]; then
        echo "delete_rand_t10 failed, $out"
        exit 1
    fi

    return 0
}

function sp_rand_t10
{
    typeset db=$1

    out=$($CDB2SQL_EXE ${CDB2_OPTIONS} $db default "exec procedure test()" 2>&1)

    if [[ $? != 0 ]]; then
        echo "sp_rand_t10 failed, $out"
        exit 1
    fi

    return 0
}

function writer
{
    while true; do
        insert_rand_t10 $dbname
        update_rand_t10 $dbname
        delete_rand_t10 $dbname
        sp_rand_t10 $dbname
    done

    return 0
}

function reader
{
    while true; do
        $CDB2SQL_EXE ${CDB2_OPTIONS} $dbname default - <<'EOF' > /dev/null
begin
select * from t10 where id > 20 order by id
commit
EOF
        if [[ $? != 0 ]]; then
            echo "reader failed"
            exit 1
        fi
    done

    return 0
}

function reader_recom
{
    while true; do
        $CDB2SQL_EXE ${CDB2_OPTIONS} $dbname default - <<'EOF' > /dev/null
set transaction read committed
begin
select * from t10 where id > 20 order by id
commit
EOF
        if [[ $? != 0 ]]; then
            echo "reader failed"
            exit 1
        fi
    done

    return 0
}

function analyzer 
{
    while true; do
        $CDB2SQL_EXE ${CDB2_OPTIONS} $dbname default "analyze t10 100" > /dev/null
        if [[ $? != 0 ]]; then
            echo "analyzer failed"
            exit 1
        fi
    done

    return 0
}

writer &
wpid=$!

reader &
rpid=$!

reader_recom &
rrpid=$!

analyzer &
apid=$!

# let background run for $runtime seconds
# sleep $runtime
ii=0
while [[ $ii -lt $runtime ]]; do
    if [[ "-1" != "$wpid" ]]; then
        ps -p $wpid > /dev/null 2>&1
        if [[ $? != 0 ]]; then
            break
        fi
    fi
    if [[ "-1" != "$rpid" ]]; then
        ps -p $rpid > /dev/null 2>&1
        if [[ $? != 0 ]]; then
            break
        fi
    fi
    if [[ "-1" != "$rrpid" ]]; then
        ps -p $rrpid > /dev/null 2>&1
        if [[ $? != 0 ]]; then
            break
        fi
    fi
    if [[ "-1" != "$apid" ]]; then
        ps -p $apid > /dev/null 2>&1
        if [[ $? != 0 ]]; then
            break
        fi
    fi
    let ii=ii+1
    sleep 1
done

failed=0
if [[ "-1" != "$wpid" ]]; then
    ps -p $wpid > /dev/null 2>&1
    if [[ $? != 0 ]]; then
        failed=1
    fi
    { kill -9 $wpid && wait $wpid; } 2>/dev/null
fi
if [[ "-1" != "$rpid" ]]; then
    ps -p $rpid > /dev/null 2>&1
    if [[ $? != 0 ]]; then
        failed=1
    fi
    { kill -9 $rpid && wait $rpid; } 2>/dev/null
fi
if [[ "-1" != "$rrpid" ]]; then
    ps -p $rrpid > /dev/null 2>&1
    if [[ $? != 0 ]]; then
        failed=1
    fi
    { kill -9 $rrpid && wait $rrpid; } 2>/dev/null
fi
if [[ "-1" != "$apid" ]]; then
    ps -p $apid > /dev/null 2>&1
    if [[ $? != 0 ]]; then
        failed=1
    fi
    { kill -9 $apid && wait $apid; } 2>/dev/null
fi

if [[ $failed != 0 ]]; then
    echo "FAILED"
    exit 1
fi

echo SUCCESS
exit 0
