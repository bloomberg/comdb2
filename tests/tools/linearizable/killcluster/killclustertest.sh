#!/usr/bin/env bash

db=$1
shift

if [[ -z "$db" ]]; then
    echo "Requires a dbname"
    exit 1
fi

sigstop=0
if [[ "$1" == "-p" ]]; then
    sigstop=1
    shift
fi

debug=0
[[ $debug == 1 ]] && set -x
pathbase=${COMDB2_PATHBASE:-/home/ubuntu/comdb2}
scripts=$pathbase/linearizable/scripts
. $scripts/setvars
clustertest=$pathbase/linearizable/killcluster

$scripts/addmach_comdb2db $db

# echo 'Heal network on cluster'
# $scripts/heal $db

dbout=/db/testout/killcluster.$db.out
dberr=/db/testout/killcluster.$db.err

# If these are already present this will be a simple select
$pathbase/linearizable/ctest/test -d $db -S 2000000 -Y

cdb2sql -maxretries 1000000 -debugtrace $db dev - < $clustertest/delete_sql >$dbout.delete 2>$dberr.delete

cdb2sql -maxretries 1000000 -debugtrace $db dev - < $clustertest/sql >$dbout 2>$dberr &
sqlpid=$!
sleep 8

outfile=$db.out.$(date +%Y%m%d%H%M%S)

# Remove the current tracefile
for x in $machines ; do
    ssh $x "export first=1 ; ls -t /db/comdb2/$db/$db.out.*.$x | while read fl ; do if [[ \$first == "0" ]]; then rm \$fl ; else echo "keep \$fl" ; first=0 ; fi ; done" 

done

for x in $machines ; do
    if [[ "$sigstop" == "1" ]]; then

        sleeptime=$(( (RANDOM % 20) + 1 ))
        echo "pid=\$(ps -ef | egrep comdb2 | egrep lrl | egrep -v bash | egrep $db | awk '{print \$2}') ; kill -SIGSTOP \$pid ; sleep $sleeptime ; kill -SIGCONT \$pid"
        ssh $x "pid=\$(ps -ef | egrep comdb2 | egrep lrl | egrep -v bash | egrep $db | awk '{print \$2}') ; kill -SIGSTOP \$pid ; sleep $sleeptime ; kill -SIGCONT \$pid" < /dev/null &

    else

        echo "kill -9 \$(ps -ef | egrep comdb2 | egrep lrl | egrep -v bash | egrep $db | awk '{print \$2}') ; sleep 10 ; export MALLOC_CHECK_=2 ; /opt/bb/bin/comdb2 $db -lrl /db/comdb2/$db/$db.lrl -pidfile /tmp/$db.pid 2>&1 | ts '[%Y-%m-%d %H:%M:%S]' > /db/comdb2/$db/$outfile.$x 2>&1"
        ssh $x "kill -9 \$(ps -ef | egrep comdb2 | egrep lrl | egrep -v bash | egrep $db | awk '{print \$2}') ; sleep 10 ; export MALLOC_CHECK_=2 ; /opt/bb/bin/comdb2 $db -lrl /db/comdb2/$db/$db.lrl -pidfile /tmp/$db.pid 2>&1 | ts '[%Y-%m-%d %H:%M:%S]' > /db/comdb2/$db/$outfile.$x 2>&1" < /dev/null &

    fi
    #sleep 2
done

kill -0 $sqlpid 
while [[ $? == 0 ]]; do
    sleep 1
    kill -0 $sqlpid
done

egrep -v comdb2_host $dbout > $dbout.normalized
python generate_correct_out.py > $clustertest/correct.out.normalized

diff $dbout.normalized $clustertest/correct.out.normalized
if [[ $? -ne 0 ]] ; then
    echo "Error in test!  See diff $dbout.normalized $clustertest/correct.out.normalized"
    exit 1
else
    echo "Successful test"
fi
