#!/usr/bin/env bash

exec 2>&1
LC_ALL=C
DB="$2"
SED="s/rows inserted/.n_writeops_done/g;s/'//g"

echo "insert into t2 values(1,x'ff','comdb2')
insert into t2 values(2,x'00','oracle')
insert into t2 values(3,x'0f','mysql')
insert into t2 values(4,x'f0','postgres')" | cdb2sql -s ${CDB2_OPTIONS} $DB default - | sed "$SED"

echo "select * from t2 order by i
select * from t2 order by i desc
select * from t2 where i=1
select * from t2 where i>=0
select * from t2 where i<=99
select * from t2 where i<0
select * from t2 where i>99" | cdb2sql -s ${CDB2_OPTIONS} $DB default - | sort
