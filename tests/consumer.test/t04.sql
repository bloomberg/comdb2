-- start with bad tz
-- if these records are inserted after creating lua consumer nothing bad will happen until you read from consumer
-- server will read garbage datetime from queue and abort
-- this is because garbage datetime will be inserted into queue in append_field() function
-- where server datetime is being attempted to convert to client datetime with invalid tz (returns rc), which is what is inserted into queue

SET TIMEZONE zzz
DROP TABLE IF EXISTS t4;
CREATE TABLE t4(a DATETIME); $$
INSERT INTO t4 VALUES (NOW()); -- this and next stmt are ok even with bad tz set as long as no queue
INSERT INTO t4 VALUES ("2023-12-12 America/New_York");
SELECT * FROM t4; -- data will be inserted but can't select bc have bad tz
CREATE DEFAULT LUA CONSUMER test4 ON (TABLE t4 FOR INSERT);
INSERT INTO t4 VALUES (NOW());
INSERT INTO t4 VALUES ("2023-12-12 America/New_York");
DROP LUA CONSUMER test4
DROP PROCEDURE test4 VERSION 'comdb2 default consumer 1.0'
DROP TABLE t4;

-- Do the same thing with datetimeus
CREATE TABLE t4(a DATETIMEUS); $$
INSERT INTO t4 VALUES (NOW()); -- this and next stmt are ok even with bad tz set as long as no queue
INSERT INTO t4 VALUES ("2023-12-12 America/New_York");
SELECT * FROM t4; -- data will be inserted but can't select bc have bad tz
CREATE DEFAULT LUA CONSUMER test4 ON (TABLE t4 FOR INSERT);
INSERT INTO t4 VALUES (NOW());
INSERT INTO t4 VALUES ("2023-12-12 America/New_York");
DROP LUA CONSUMER test4
DROP PROCEDURE test4 VERSION 'comdb2 default consumer 1.0'
DROP TABLE t4;
