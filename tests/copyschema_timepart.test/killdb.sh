# bring down new db
newdb=$1
pidfile=$2
if [[ -f $pidfile ]]; then
    pid=$(cat $pidfile)
    pstr=$(ps -p $pid -o args | grep comdb2)
    echo $pstr | grep -q "comdb2 ${newdb}"
    if [[ $? -eq 0 ]]; then
        echo "${TESTCASE}: killing $pid"
        if [ "`echo $pstr | awk '{ print $1 }' | xargs basename`" = "comdb2" ] ; then
            kill -9 $pid
        else
            kill $pid
        fi
    fi
    rm -f $pidfile
else
    echo "kill_by_pidfile: pidfile $pidfile does not exist"
fi
