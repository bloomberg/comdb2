#!/usr/bin/env bash

cdb2sql --host $SP_HOST $SP_OPTIONS "SELECT '------- PHASE 1 -------'" 2>&1
cdb2sql --host $SP_HOST $SP_OPTIONS "PUT TUNABLE strict_double_quotes 0" 2>&1
cdb2sql --host $SP_HOST $SP_OPTIONS "SELECT \"1\"" 2>&1
cdb2sql --host $SP_HOST $SP_OPTIONS "SELECT \"x\"" 2>&1
cdb2sql --host $SP_HOST $SP_OPTIONS "SELECT \"x\" FROM t1 WHERE x = 1" 2>&1
cdb2sql --host $SP_HOST $SP_OPTIONS "SELECT \"y\" FROM t1 WHERE x = 1" 2>&1
cdb2sql --host $SP_HOST $SP_OPTIONS "SELECT \"x\" FROM t2 WHERE \"t2.y\" = 1" 2>&1
cdb2sql --host $SP_HOST $SP_OPTIONS "SELECT \"y\" FROM t2 WHERE \"t2.x\" = 1" 2>&1
cdb2sql --host $SP_HOST $SP_OPTIONS "SELECT \"x\" FROM t2 WHERE \"t2\".\"y\" = 1" 2>&1
cdb2sql --host $SP_HOST $SP_OPTIONS "SELECT \"y\" FROM t2 WHERE \"t2\".\"x\" = 1" 2>&1

cdb2sql --host $SP_HOST $SP_OPTIONS "SELECT '------- PHASE 2 -------'" 2>&1
cdb2sql --host $SP_HOST $SP_OPTIONS "PUT TUNABLE strict_double_quotes 1" 2>&1
cdb2sql --host $SP_HOST $SP_OPTIONS "SELECT \"1\"" 2>&1
cdb2sql --host $SP_HOST $SP_OPTIONS "SELECT \"x\"" 2>&1
cdb2sql --host $SP_HOST $SP_OPTIONS "SELECT \"x\" FROM t1 WHERE x = 1" 2>&1
cdb2sql --host $SP_HOST $SP_OPTIONS "SELECT \"x\" FROM t1 WHERE \"t1\".\"x\" = 1" 2>&1
cdb2sql --host $SP_HOST $SP_OPTIONS "SELECT \"y\" FROM t2 WHERE \"t2\".\"y\" = 1" 2>&1
cdb2sql --host $SP_HOST $SP_OPTIONS "SELECT \"t2\".\"y\" FROM t2 WHERE \"t2\".\"y\" = 1" 2>&1
cdb2sql --host $SP_HOST $SP_OPTIONS "SELECT \"y\" FROM t1 WHERE x = 1" 2>&1
cdb2sql --host $SP_HOST $SP_OPTIONS "SELECT \"x\" FROM t2 WHERE \"t2.y\" = 1" 2>&1
cdb2sql --host $SP_HOST $SP_OPTIONS "SELECT \"y\" FROM t2 WHERE \"t2.x\" = 1" 2>&1
cdb2sql --host $SP_HOST $SP_OPTIONS "SELECT \"x\" FROM t2 WHERE \"t2\".\"y\" = 1" 2>&1
cdb2sql --host $SP_HOST $SP_OPTIONS "SELECT \"y\" FROM t2 WHERE \"t2\".\"x\" = 1" 2>&1
cdb2sql --host $SP_HOST $SP_OPTIONS "INSERT INTO t1(x) VALUES(\"9\")" 2>&1
cdb2sql --host $SP_HOST $SP_OPTIONS "UPDATE t1 SET x = \"8\" WHERE x = '1'" 2>&1
cdb2sql --host $SP_HOST $SP_OPTIONS "DELETE FROM t1 WHERE x = \"9\"" 2>&1

cdb2sql --host $SP_HOST $SP_OPTIONS "SELECT '------- PHASE 3 -------'" 2>&1
cdb2sql --host $SP_HOST $SP_OPTIONS "PUT TUNABLE strict_double_quotes 0" 2>&1
cdb2sql --host $SP_HOST $SP_OPTIONS "INSERT INTO t2(y) VALUES(\"2\")" 2>&1
cdb2sql --host $SP_HOST $SP_OPTIONS "INSERT INTO t2(y) VALUES('4')" 2>&1
cdb2sql --host $SP_HOST $SP_OPTIONS "INSERT INTO t2(y) VALUES('7')" 2>&1
cdb2sql --host $SP_HOST $SP_OPTIONS "UPDATE t2 SET y = \"3\" WHERE y = '7'" 2>&1
cdb2sql --host $SP_HOST $SP_OPTIONS "DELETE FROM t2 WHERE y = \"4\"" 2>&1
