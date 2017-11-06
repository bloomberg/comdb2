#!/bin/bash

# Unlike the other linearizable tests, this must be run by itself on a machine

remove_good_test=0

while [[ -n "$1" ]]; do
    case $1 in

        -d)
            echo "ENABLED DEBUG TRACE"
            export COMDB2_DEBUG=1
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
scripts=$pathbase/linearizable/scripts
. $scripts/setvars
outbase=${COMDB2_OUTBASE:-/db/testout}

$scripts/heallall

# add to comdb2db
$scripts/addmach_comdb2db $db


iter=0
while :; do 

    let iter=iter+1

    outfile=$outbase/jepsen_test.$db.$iter.out

    $scripts/heal $db

    ma=$(cdb2sql -tabs $db dev "exec procedure sys.cmd.send('bdb cluster')" | egrep MASTER | egrep lsn) ; m=${ma%%:*}
    c=$(ssh $m "/opt/bb/bin/cdb2sql -tabs $db @localhost \"exec procedure sys.cmd.send('bdb cluster')\"")
    echo "$c"

    echo "$c" | egrep COHERENT
    r=$?
    while [[ $r == 0 ]] ; do
        echo "$(date) waiting for $db cluster to become coherent"
        $pathbase/linearizable/scripts/heal $db
        sleep 1
        ma=$(cdb2sql -tabs $db dev "exec procedure sys.cmd.send('bdb cluster')" | egrep MASTER | egrep lsn) ; m=${ma%%:*}
        c=$(ssh $m "/opt/bb/bin/cdb2sql -tabs $db @localhost \"exec procedure sys.cmd.send('bdb cluster')\"")
        echo "$c" | egrep COHERENT
        r=$?
    done

    sleep 1

    cdb2sql $db dev "delete from jepsen where 1 = 1"

    export COMDB2_DBNAME=$db
    lein test 2>&1 | tee $outfile 2>&1 
    if [[ $? != 0 ]]; then 
        echo "!!! JEPSEN TEST FAILED ON ITERATION $iter !!!" 
        break 2
    fi

    egrep "Analysis invalid!" $outfile >/dev/null 2>&1 
    if [[ $? == 0 ]]; then
        echo "!!! JEPSEN ANALYSIS INVALID $iter !!!" 
        break 2
    fi

    egrep -v "closeNo|gnuplot|IOException|OutOfMemoryError|Uncaught" $outfile | egrep exception
    if [[ $? == 0 ]]; then
        echo "!!! JEPSEN TEST FAILED WITH EXCEPTION ITERATION $iter !!!" 
        break 2
    fi

#    egrep -i "handle state" $outfile > /dev/null 2>&1
#    if [[ $? == 0 ]]; then
#        echo "!!! JEPSEN TEST FAILED WITH WRONG HANDLE STATE ITERATION $iter !!!" 
#        break 2
#    fi

    sleep 1

    if [[ "$remove_good_test" = "1" ]]; then
        echo "removing good test run"
        rm -f $outfile
    fi

done

