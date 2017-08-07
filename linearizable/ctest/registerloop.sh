#!/bin/bash

clock_test=0
debug_trace_flags=""
remove_good_test=0

while [[ -n "$1" ]]; do
    case $1 in
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
knossos=$pathbase/linearizable/filetest
. $scripts/setvars
outbase=${COMDB2_OUTBASE:-/db/testout}

# add to comdb2db
$scripts/addmach_comdb2db $db

iter=0
select_test_flag=""

while :; do 
    outfile=$outbase/register.$db.$iter.out
    clojout=$outbase/register.$db.$iter.cloj
    knosout=$outbase/register.$db.$iter.knossos

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

    echo "$(date) running test iteration $iter for $db"
    echo "$descr"

    # -D = enable debug-trace flags, -i <#> = number of inserts per txn
    echo "$runbase/register $debug_trace_flags -r 20 $runtest -d $db $mflag -j $clojout"
    $runbase/register $debug_trace_flags -r 20 $runtest -d $db $mflag -j $clojout > $outfile 2>&1

    (
        cd $knossos
        lein run $clojout > $knosout 2>&1
    )

    egrep "^{:valid\? false" $knosout >/dev/null 2>&1 
    if [[ $? == 0 ]]; then
        echo "!!! INVALID ANALYSIS FOR REGISTER TEST ITERATION $iter !!!"
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
        rm -f $clojout
        rm -f $knosout
    fi

done

