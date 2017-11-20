#!/usr/bin/env bash

table=$1
db=$2

#comdb2sc -m $db send stat csc2vers | grep "table $table is at csc2 version" | awk '{print $7}'
cdb2sql --tabs ${CDB2_OPTIONS} $db default 'exec procedure sys.cmd.send("stat csc2vers")' | grep "table $table is at csc2 version" | awk '{print $7}'
