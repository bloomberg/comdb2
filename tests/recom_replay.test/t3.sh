dbname=$1
runtime=180
xpid=-1
ypid=-1

cdb2sql ${CDB2_OPTIONS} $dbname default - <<EOF > /dev/null 2>&1
drop table if exists t3
create table t3 {
    tag ondisk {
        int x
        int y
    }
    keys {
        "KEY" = x
    }
}\$\$
insert into t3 values (0, 0)
EOF

function writer
{
    typeset out=$1
    while true; do
        cdb2sql --showeffects ${CDB2_OPTIONS} -f t3.sql $dbname default > $out 2>&1
        diff $out t3.out > /dev/null 2>&1
        if [ $? != 0 ]; then
            diff $out t3.out
            break
        fi
    done
}

cdb2sql ${CDB2_OPTIONS} $dbname default "exec procedure sys.cmd.send('on early_verify')"

writer t3x.res &
xpid=$!

writer t3y.res &
ypid=$!

ii=0
while [[ $ii -lt $runtime ]]; do
    if [[ "-1" != "$xpid" ]]; then
        ps -p $xpid > /dev/null 2>&1
        if [[ $? != 0 ]]; then
            break
        fi
    fi
    if [[ "-1" != "$ypid" ]]; then
        ps -p $ypid > /dev/null 2>&1
        if [[ $? != 0 ]]; then
            break
        fi
    fi
    let ii=ii+1
    sleep 1
done

failed=0
if [[ "-1" != "$xpid" ]]; then
    ps -p $xpid > /dev/null 2>&1
    if [[ $? != 0 ]]; then
        failed=1
    fi
    { kill -9 $xpid && wait $xpid; } 2>/dev/null
fi
if [[ "-1" != "$ypid" ]]; then
    ps -p $ypid > /dev/null 2>&1
    if [[ $? != 0 ]]; then
        failed=1
    fi
    { kill -9 $ypid && wait $ypid; } 2>/dev/null
fi

if [[ $failed != 0 ]]; then
    echo "on early_verify FAILED"
    exit 1
fi
echo "on early_verify succeeded"

echo "turning off early_verify, expect test to fail"
cdb2sql ${CDB2_OPTIONS} $dbname default "exec procedure sys.cmd.send('off early_verify')"

writer t3x.res &
xpid=$!

writer t3y.res &
ypid=$!

ii=0
while [[ $ii -lt $runtime ]]; do
    if [[ "-1" != "$xpid" ]]; then
        ps -p $xpid > /dev/null 2>&1
        if [[ $? != 0 ]]; then
            break
        fi
    fi
    if [[ "-1" != "$ypid" ]]; then
        ps -p $ypid > /dev/null 2>&1
        if [[ $? != 0 ]]; then
            break
        fi
    fi
    let ii=ii+1
    sleep 1
done

failed=0
if [[ "-1" != "$xpid" ]]; then
    ps -p $xpid > /dev/null 2>&1
    if [[ $? != 0 ]]; then
        failed=1
    fi
    { kill -9 $xpid && wait $xpid; } 2>/dev/null
fi
if [[ "-1" != "$ypid" ]]; then
    ps -p $ypid > /dev/null 2>&1
    if [[ $? != 0 ]]; then
        failed=1
    fi
    { kill -9 $ypid && wait $ypid; } 2>/dev/null
fi

if [[ $failed == 0 ]]; then
    echo "off early_verify SUCCEEDED, but expected FAILED"
    exit 1
fi
echo "off early_verify failed as expected"

echo "TEST SUCCESS"
exit 0
