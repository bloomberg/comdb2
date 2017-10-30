#!/usr/bin/env bash

exec 2>&1
LC_ALL=C
export LANG=C
db="$2"
cdb2sql -s ${CDB2_OPTIONS} $db default "select a from t" | sort
cdb2sql -s ${CDB2_OPTIONS} $db default "select a from t order by i"
echo "insert into t(i, j) values(38, 38)
insert into t(i, j, a) values(39, 39, 39)" | cdb2sql -s ${CDB2_OPTIONS} $db default -
cdb2sql -s ${CDB2_OPTIONS} $db default "select a from t order by i"
cdb2sql -s ${CDB2_OPTIONS} $db default "select a from t" | sort
