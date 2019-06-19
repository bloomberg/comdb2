SELECT 'Crash in sql_dump_running_statements()' AS test;
DROP TABLE IF EXISTS t1;
CREATE TABLE t1(i INT UNIQUE)$$
INSERT INTO t1 VALUES(1),(2);
PUT TUNABLE 'debug.tmptbl_corrupt_mem' 1;
SELECT * FROM t1 WHERE i IN (1,2) ORDER BY i;
EXEC PROCEDURE sys.cmd.send('sql dump');
DROP TABLE t1;
