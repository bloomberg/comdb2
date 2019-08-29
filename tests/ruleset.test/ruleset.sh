#!/usr/bin/env bash

cdb2sql --host $SP_HOST $SP_OPTIONS "EXEC PROCEDURE sys.cmd.send('free_ruleset')"
cdb2sql --host $SP_HOST $SP_OPTIONS "EXEC PROCEDURE sys.cmd.send('free_ruleset')"

cdb2sql --host $SP_HOST $SP_OPTIONS "EXEC PROCEDURE sys.cmd.send('dump_ruleset')"

cdb2sql --host $SP_HOST $SP_OPTIONS "SELECT 1;"
cdb2sql --host $SP_HOST $SP_OPTIONS "SELECT * FROM t1;"
cdb2sql --host $SP_HOST $SP_OPTIONS "SELECT x FROM t1;"
cdb2sql --host $SP_HOST $SP_OPTIONS "SELECT x FROM t1 ORDER BY x;"

cdb2sql --host $SP_HOST $SP_OPTIONS "EXEC PROCEDURE sys.cmd.send('reload_ruleset t1.ruleset')"

cdb2sql --host $SP_HOST $SP_OPTIONS "SELECT 2;"
cdb2sql --host $SP_HOST $SP_OPTIONS "SELECT * FROM t1;"
cdb2sql --host $SP_HOST $SP_OPTIONS "SELECT x FROM t1;"
cdb2sql --host $SP_HOST $SP_OPTIONS "SELECT x FROM t1 ORDER BY x;"

cdb2sql --host $SP_HOST $SP_OPTIONS "EXEC PROCEDURE sys.cmd.send('save_ruleset t1_saved.ruleset')"

if ! diff t1.ruleset t1_saved.ruleset ; then
  echo output is different from expected
  exit 1
fi

cdb2sql --host $SP_HOST $SP_OPTIONS "EXEC PROCEDURE sys.cmd.send('evaluate_ruleset')"
cdb2sql --host $SP_HOST $SP_OPTIONS "EXEC PROCEDURE sys.cmd.send('free_ruleset')"

cdb2sql --host $SP_HOST $SP_OPTIONS "SELECT 3;"
cdb2sql --host $SP_HOST $SP_OPTIONS "SELECT * FROM t1;"
cdb2sql --host $SP_HOST $SP_OPTIONS "SELECT x FROM t1;"
cdb2sql --host $SP_HOST $SP_OPTIONS "SELECT x FROM t1 ORDER BY x;"

cdb2sql --host $SP_HOST $SP_OPTIONS "EXEC PROCEDURE sys.cmd.send('evaluate_ruleset')"

# debug.random_sql_work_delayed
# debug.random_sql_work_rejected
