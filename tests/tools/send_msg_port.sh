#!/usr/bin/env bash
#
#send message to port 

host=localhost

[ "$1" == "-h" ] && shift && host=$1 && shift 
msg=$1
port=$2

failexit() {
    echo $1
    exit 1
}

if [ "x$msg" == "x" ] ; then
    failexit "message empty"
fi
if [ "x$port" == "x" ] ; then
    failexit "port empty"
fi

exec 3<>/dev/tcp/$host/$port || failexit "no listener in port $port"

echo $msg >&3

read -t 0.2 resp <&3
rc=$?
while [ $rc -eq 0 ] ; do
    echo "$resp"
    read -t 0.2 resp <&3
    rc=$?
done

exec 3>&- #close output connection
exec 3<&- #close input connection
