#!/usr/bin/env bash

exec 2>&1
LC_ALL=C
DB="$2"
SED="s/rows inserted/.n_writeops_done/g;s/'//g"

echo "insert into t4(i, j, p, q) values(0, 0, 0, 0)
insert into t4(i, j, p, q) values(1, 1, 1, 1)
insert into t4(i, j, p, q) values(2147483647, 4294967295, 2147483647, 4294967295)
insert into t4(i, j, p, q) values(-2147483648, 0, -2147483648, 0)" | cdb2sql -s ${CDB2_OPTIONS} $DB default - | sed "$SED"
echo "select * from t4 order by i
select * from t4 order by i desc" | cdb2sql -s ${CDB2_OPTIONS} $DB default -
