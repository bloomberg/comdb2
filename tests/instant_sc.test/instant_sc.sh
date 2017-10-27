#!/usr/bin/env bash

table=$1
db=$2
sc=$3

cdb2sql ${CDB2_OPTIONS} $db default "alter table $table { `cat $sc ` }"

cdb2sql --tabs ${CDB2_OPTIONS} $db default 'exec procedure sys.cmd.send("stat csc2vers")' | grep "table $table is at csc2 version" | awk '{print $7}'
