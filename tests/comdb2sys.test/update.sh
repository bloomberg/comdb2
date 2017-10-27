#!/usr/bin/env bash

DB=$2

cdb2sql -s ${CDB2_OPTIONS} $DB default "drop table pk" &> /dev/null
cdb2sql -s ${CDB2_OPTIONS} $DB default "drop table fk" &> /dev/null

cdb2sql -s ${CDB2_OPTIONS} $DB default "create table pk { `cat pk.csc2 ` }" &> /dev/null
cdb2sql -s ${CDB2_OPTIONS} $DB default "create table fk { `cat fk.csc2 ` }" &> /dev/null

cdb2sql ${CDB2_OPTIONS} $DB default "insert into fk values(1)" #FAIL
cdb2sql ${CDB2_OPTIONS} $DB default "insert into pk values(1)"
cdb2sql ${CDB2_OPTIONS} $DB default "insert into fk values(1)"
cdb2sql ${CDB2_OPTIONS} $DB default "update fk set i = 2" #FAIL
cdb2sql ${CDB2_OPTIONS} $DB default "insert into pk values(2)"
cdb2sql ${CDB2_OPTIONS} $DB default "update fk set i = 2"
