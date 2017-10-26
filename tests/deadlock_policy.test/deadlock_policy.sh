#!/usr/bin/env bash

# deadlock policy tester for the cdb2tcm testsuite

#set -x

# arguments 
args=$1
dbnm=$2
log=$3

# local variables
isrmt=
mch=

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
    echo $exe $args
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

    if [[ $elapsed -ge $tmout ]]; then

        echo " killing $exe because we timed out $elapsed -ge $tmout"
        kill -9 $cpid >/dev/null 2>&1 
        # this is a kludge - also kill the cdb2sql processes
        ps | grep cdb2sql | awk '{print $1}' | xargs kill -9 >/dev/null 2>&1 || true
        return 1

    fi

    # normal end
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


# validate the logfile output of this test
function validscon
{
    # local vars
    typeset maxr=0
    typeset cmd
    typeset failcnt=0
    typeset ln
    typeset kpln
    typeset cntscn=0

    # arguments
    dbname=$1
    log=$2
    failpct=$3
    failmx=$4
    minscn=$5

    # tail the logfile
    while read ln ; do 

        # check for an n_reqs line - verify the retries
        if [[ "$ln" == *"nnewsql"* ]]; then

            xxx=${ln#*nnewsql}
            reqs=$(set - $xxx ; echo $1)

            #echo nreqs $reqs

            xxx=${ln#*n_retries}
            if [[ "$xxx" != "$ln" ]]; then
                retries=$(set - $xxx ; echo $1)
            else
                retries=0
            fi

            let cntscn=cntscn+1

            # if retries is more than half requests fail the test
            flmxx=$(echo "$reqs * $failpct" | bc)
            flmx=${flmxx%%.*}

            if [[ $retries -gt $flmx ]]; then
                let failcnt=failcnt+1
                kpln="$kpln$ln
"
            fi
        fi

        # check for long request trace if we're failing on that
        if [[ "$ln" == *"LONG REQUEST"* ]] ; then

            let failcnt=failcnt+1
            kpln="$kpln$ln
"
        fi


    done < $log 
    # make sure that we have the minimal number of scon lines
    if [[ $cntscn -lt $minscn ]] ; then

        echo "TEST FAILED"
        echo " "
        echo "> I needed to parse at least $minscn 'scon' lines"
        echo "> I only saw $cntscn 'scon' lines"
        echo "> Am I reading the correct logfile?"
        echo " "
        return 1

    fi

    # chk tolerance threshold & fail the test
    if [[ $failcnt -gt $failmx ]] ; then

        echo "TEST FAILED"
        echo "$kpln"
        return 1

    else

        echo "SUCCESS!"
        return 0

    fi

} 

cdb2sql ${CDB2_OPTIONS} $dbnm default "exec procedure sys.cmd.send('scon')"

# check to see if database is running remotely
if [[ "$rmt" == 0 ]]; then

    isrmt=0
    mch=$(hostname)
    echo "scon" >> /dev/tty

fi


# generate 10000 records of input data
$gen $dbnm 10000


# start 40 updaters - allow them to run for a minute
spawn_instances $upd $dbnm 60 40

# sleep for a bit
sleep 60

# copy the logfile to local machine
# TODO:NOENV

# validate the log
validscon $dbnm $log .80 20 30
exit $?
