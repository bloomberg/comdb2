#!/usr/bin/env bash

exec 2>&1
LC_ALL=C
DB="$2"
SED="s/rows inserted/.n_writeops_done/g;s/'//g"

echo "insert into t1 values (0,0,0)
insert into t1 values (1,1,2)
insert into t1 values (2,1,2)
insert into t1 values (2,1,2)
insert into t1 values (3,1,2)
insert into t1 values (3,1,2)
insert into t1 values (3,1,2)
insert into t1 values (4,1,2)
insert into t1 values (4,1,2)
insert into t1 values (4,1,2)
insert into t1 values (4,1,2)
insert into t1 values (5,1,2)
insert into t1 values (5,1,2)
insert into t1 values (5,1,2)
insert into t1 values (5,1,2)
insert into t1 values (5,1,2)" | cdb2sql -s ${CDB2_OPTIONS} $DB default - | sed "$SED"
