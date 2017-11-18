#!/usr/bin/env bash

exec 2>&1
LC_ALL=C
DB="$2"
SED="s/rows inserted/.n_writeops_done/g;s/'//g"

echo "insert into t5 values(1,2,3,4,5)
insert into t5 values(1,1,3,4,5)
insert into t5 values(1,2,1,4,5)
insert into t5 values(1,2,3,1,5)
insert into t5 values(1,2,3,4,1)" | cdb2sql -s ${CDB2_OPTIONS} $DB default - | sed "$SED"
echo "select * from t5 where a1=1 and a3>=1 and a2=2 order by a3 desc, a4 desc" | cdb2sql -s ${CDB2_OPTIONS} $DB default -
