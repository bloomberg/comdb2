#!/usr/bin/env bash

# deadlock policy tester for the cdb2tcm testsuite

# arguments 
args=$1
dbnm=$2


# generator and updater tasks
gen=gen
upd=upd

keys[0]="a"
keys[1]="b"
keys[2]="c"

function gen {
    db=$1
    count=$2
    i=1
    while [[ $i -le $count ]]; do
        echo "insert into t1(a, b, c) values($i, $i, $i)"
        i=$(($i+1))
    done | cdb2sql -s ${CDB2_OPTIONS} $db default - >/dev/null
}

function upd {
    db=$1
    count=$(cdb2sql --tabs ${CDB2_OPTIONS} $db default 'select max(a) from t1')
    while :; do
        val=$(($RANDOM % $count))
        key=$(($RANDOM % 3))
        echo "select * from t1 where "${keys[$key]}" = $val limit 1"
        echo "update t1 set a=$val where "${keys[$key]}" = $val"
    done | cdb2sql -s ${CDB2_OPTIONS} $db default - >/dev/null
}



# fork & timeout an executable after a period of time
function run_timeout
{
    # local vars
    typeset st
    typeset et
    typeset elapsed=0
    typeset cpid=0

    # arguments
    exe=$1
    args=$2
    tmout=$3

    # SECONDS returns the number of seconds since the shell was invoked
    st=$SECONDS
    
    # run command and grab pid
    $exe $args > /dev/null 2>&1 &
    cpid=$!

    # verify pid
    ps -p $cpid >/dev/null 2>&1

    while [[ $? == 0 && $elapsed -lt $tmout ]] ; do

        sleep 1

        # check to see if it's time to break out of the loop
        et=$SECONDS
        elapsed=$(( et - st ))

        # verify pid
        ps -p $cpid >/dev/null 2>&1

    done

    # kill the task
    kill -9 $cpid >/dev/null 2>&1 
    return 0
}


# run several instances of a program for a period of time
function spawn_instances
{
    # local vars
    typeset cnt=0

    # arguments
    exe=$1
    args=$2
    tmout=$3
    inst=$4

    # spawn several instances 
    while [[ $cnt -lt $inst ]]; do

        run_timeout $exe $args $tmout &
        let cnt=cnt+1

    done

    return 0
}

# run a select statement- verify that the output is correct
function test_select
{
    # local vars
    typeset fil
    typeset cnt=0
    typeset err=0

    # arguments
    dbnm=$1
    iter=$2

    # select 
    while [[ $cnt -lt $iter ]] ; do 

        fil=${TMPDIR}/tsel.$cnt
        cdb2sql -s ${CDB2_OPTIONS} $dbnm default "select * from t1 where a>0" >/dev/null 2>$fil &
        let cnt=cnt+1

    done

    # wait for selects to complete
    sleep 20

    # zap
    cnt=0

    # print the output
    while [[ $cnt -lt $iter ]] ; do 
        [[ -s $fil ]] && err=1
        cat $fil
        let cnt=cnt+1

    done

    if [[ 0 == "$err" ]]; then
        echo "SUCCESS!"
    fi

}


# generate 10000 records of input data
$gen $dbnm 10000


# start 40 updaters - allow them to run for a minute
spawn_instances $upd $dbnm 60 40


# sleep for a few seconds
sleep 10


# spawn 40 selects against an index
test_select $dbnm 10
