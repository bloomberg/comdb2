#!/usr/bin/env bash

db=$1
n=$2
sleeptime=${3:-1}
ccreq=${4:-1}
cntfile=$db.$n
totcnt=0
whoami=$(whoami)


/home/$whoami/comdb2/cdb2sql $db @$n "create table t1 { schema { int a } }" >/dev/null 2>&1

function dbreq
{
    x=$(( RANDOM % 2 ))
    if [[ "$x" == "1" ]]; then
        /home/$whoami/comdb2/cdb2sql -r 1000000 -R 1000000 $db @$n - < readsql.txt 2>&1
    else
        /home/$whoami/comdb2/cdb2sql -r 1000000 -R 1000000 $db @$n - < writesql.txt 2>&1
    fi
}

while :; do 
    echo "$(date)"
    cnt=0
    while [[ $cnt -lt $ccreq ]]; do
        ( dbreq ) &
        let cnt=cnt+1
        let totcnt=totcnt+1
    done
    wait
    echo $totcnt > $cntfile
    sleep $sleeptime
done | gawk '{ print strftime("%H:%M:%S>"), $0; fflush(); }'
