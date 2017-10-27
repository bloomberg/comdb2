#!/usr/bin/env bash

exec 2>&1
LC_ALL=C
DB="$2"
SED="s/rows inserted/.n_writeops_done/g;s/'//g"

echo "insert into t3 values(7179847, 9001,  8, 0, 0) 
insert into t3 values(7179847, 9001,  9, 0, 0)
insert into t3 values(7179847, 9001, 10, 0, 0) 
insert into t3 values(7179847, 9001, 11, 0, 0)" | cdb2sql -s ${CDB2_OPTIONS} $DB default - | sed "$SED"
echo "select line_number from t3 where owner_id=7179847 and firm_id=9001 and line_number > 10 order by line_number
select line_number from t3 where owner_id=7179847 and firm_id=9001 and line_number < 10 order by line_number" | cdb2sql -s ${CDB2_OPTIONS} $DB default -
