#!/usr/bin/env bash

S="runtest.sh"

if (( $? >= 2 )) ; then
    S=$1
fi

dbname=`grep " DBNAME=" $S | awk -F\" '{ print $2 ;}'`
dbdir=`grep " DBDIR=" $S | awk -F\" '{ print $2 }'`

echo "Dbname \"$dbname\""
echo "Dbdir \"$dbdir\""


echo "Run:"
echo "cdb2sql ${dbname} local \"exec procedure sys.cmd.send('exit')\""
echo "d=1; while [[ ! -z \"\$d\" ]] ;  do d=\`/bb/bin/psef ${dbname} | grep comdb2\` ; done"
echo "LD_PRELOAD=/lib64/libpthread.so.0 gdb --args comdb2 ${dbname} -lrl ${dbdir}/${dbname}.lrl"
