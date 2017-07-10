#!/bin/bash

select_test=0
clock_test=0
debug_trace_flags=""
remove_good_test=0

while [[ -n "$1" ]]; do
    case $1 in
        -s)
            select_test=1
            shift
            ;;
        -c)
            echo "THIS WILL RUN THE CLOCK TEST PERIODICALLY"
            clock_test=1
            shift
            ;;
        -d)
            echo "ENABLING DEBUG TRACE"
            debug_trace_flags="-D"
            shift
            ;;
        -r)
            echo "REMOVING GOOD TESTS"
            remove_good_test=1
            shift
            ;;

        *)
            db=$1
            shift
            ;;
    esac
done

if [[ -z "$db" ]]; then
    echo "the db variable is not set .. i cannot add to comdb2db"
    exit 1
fi

pathbase=${COMDB2_PATHBASE:-/home/ubuntu/comdb2}
runbase=$pathbase/linearizable/ctest/
scripts=$pathbase/linearizable/scripts
. $scripts/setvars
outbase=${COMDB2_OUTBASE:-/db/testout}

# add to comdb2db
$scripts/addmach_comdb2db $db


iter=0
insper=0
select_test_flag=""
if [[ "$select_test" == "1" ]]; then
    select_test_flag="-s"
    echo "Adding initial records for select-test (this could take a while)"
    $runbase/insert -d $db -Y
fi

while :; do 
    outfile=$outbase/insert.$db.$iter.out
    if [[ $(( iter % 2 )) == 0 ]]; then
        mflag="-M"
    else
        mflag=""
    fi

    if [[ "$clock_test" == "1" ]]; then
        count_test=4
    else
        count_test=3
    fi

    # i can run two of the tests without affecting others

    if [[ $(( iter % count_test )) == 0 ]]; then
        descr="RUNNING PARTITION TEST"
        runtest="-G partition"
    fi

    if [[ $(( iter % count_test )) == 1 ]]; then
        descr="RUNNING SIGSTOP TEST"
        runtest="-G sigstop"
    fi

    if [[ $(( iter % count_test )) == 2 ]]; then
        descr="RUNNING SIGSTOP AND PARTITION TEST"
        runtest="-G sigstop -G partition"
    fi

    # this will only be true if clock_test is 1 (& count_test is 4)
    if [[ $(( iter % count_test )) == 3 ]]; then
        descr="RUNNING SIGSTOP, PARTITION AND CLOCK TEST"
        runtest="-G sigstop -G partition -G clock"
        echo "RUNNING THE CLOCK TEST THIS ITERATION"
    fi

    # Do each test with each value
    modval=$(( (iter / count_test) % 4 ))
    insper=$(( 10 ** modval ))

    echo "$(date) running test iteration $iter for $db using $insper records per insert"
    echo "$descr"

    # -D = enable debug-trace flags, -i <#> = number of inserts per txn
    echo "$runbase/insert $debug_trace_flags -i $insper $runtest $select_test_flag -d $db $mflag"
    $runbase/insert $debug_trace_flags -i $insper $runtest $select_test_flag -d $db $mflag > $outfile 2>&1
    grep "lost value" $outfile 
    if [[ $? == 0 ]]; then
        echo "!!! LOST VALUE IN ITERATION $iter !!!"
        break 2
    fi

    grep "^XXX " $outfile
    if [[ $? == 0 ]]; then
        echo "XXX FAILURE IN ITERATION $iter !!!"
        break 2
    fi

    grep "THIS SHOULD HAVE RETURNED DUP" $outfile
    if [[ $? == 0 ]]; then
        echo "DUP BLKSEQ FAILURE IN ITERATION $iter !!!"
        break 2
    fi
 
    grep "FAIL THIS TEST" $outfile
    if [[ $? == 0 ]]; then
        echo "!!! DUP VALUE IN ITERATION $iter !!!"
        break 2
    fi

    # Make sure that the lsn appears only once per cnonce
    cnonce=$(egrep cnonce $outfile | egrep cdb2_ | awk '{print $7,$8}' | egrep -v "\[0]\[0\]" | sort -u | awk '{print $1}' | uniq -c | egrep -v "^      1" | egrep -v '2 -s' | wc -l)
    if [[ "$cnonce" != 0 ]]; then
        echo "!!! CNONCE HAS MORE THAN ONE LSN !!!"
        egrep cnonce $outfile | egrep cdb2_ | awk '{print $7,$8}' | egrep -v "\[0]\[0\]" | sort -u | awk '{print $1}' | uniq -c | egrep -v "^      1"  | egrep -v '2 -s'
        break 2
    fi

    grep "handle state" $outfile
    if [[ $? == 0 ]]; then
        echo "!!! HANDLE STATE ERROR IN ITERATION $iter !!!"
        break 2
    fi

    let iter=iter+1

    $scripts/heal $db
    ma=$(cdb2sql -tabs $db dev "exec procedure sys.cmd.send('bdb cluster')" | egrep MASTER | egrep lsn) ; m=${ma%%:*}
    c=$(ssh $m "/opt/bb/bin/cdb2sql -tabs $db @localhost \"exec procedure sys.cmd.send('bdb cluster')\"")
    echo "$c"

    echo "$c" | egrep COHERENT
    r=$?
    while [[ $r == 0 ]] ; do
        echo "$(date) waiting for $db cluster to become coherent"
        $scripts/heal $db
        sleep 1
        ma=$(cdb2sql -tabs $db dev "exec procedure sys.cmd.send('bdb cluster')" | egrep MASTER | egrep lsn) ; m=${ma%%:*}
        c=$(ssh $m "/opt/bb/bin/cdb2sql -tabs $db @localhost \"exec procedure sys.cmd.send('bdb cluster')\"")
        echo "$c"
        echo "$c" | egrep COHERENT
        r=$?
    done

    sleep 1

    if [[ "$remove_good_test" = "1" ]]; then
        echo "removing good test run"
        rm -f $outfile
    fi

done

