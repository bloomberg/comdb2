#!/bin/bash

shift
dbname=$1
shift

wait
leader=$(cdb2sql --tabs ${CDB2_OPTIONS} $dbname default "select host from comdb2_cluster where is_master='Y'")
cdb2sql --host ${leader} ${CDB2_OPTIONS} $dbname default "exec procedure t13()" > t13_01.req.out 2>&1 &
sleep 1
cdb2sql --host ${leader} ${CDB2_OPTIONS} $dbname default "analyze" 
wait
