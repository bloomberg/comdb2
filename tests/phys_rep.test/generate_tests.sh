#!/bin/bash

# This should be used under runit's context
# NRECS=1000
# NRUNS=10000

if [[ ! -e 2-insert.src.sql ]]; then
    for i in $(seq 1 $NRECS); do
        echo "insert into t1 (id, a, b, c, d, e, f, g, h, i, j) values ($i, 1, 2, 3, 4, 5, 6.000000, 7.000000, 'eight', x'99', now());"
    done > 2-insert.src.sql
fi

for i in $(seq 1 $NRUNS); do
    what=$(($RANDOM % 3))
    id=$(($RANDOM % $NRECS))
    case $what in
        0)  echo "insert into t1 (id, a, b, c, d, e, f, g, h, i, j) values ($id, 1, 2, 3, 4, 5, 6.000000, 7.000000, 'eight', x'99', now());"
    ;;
    1)  echo "delete from t1 where id = $id limit 1"
;;
2)  echo "update t1 set a=a+1, b=b+2 where id=$id"
;;
esac
done > 2-update.src.sql
