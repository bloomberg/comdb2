SELECT '---- test#1 ----' as test;

# https://www.sqlite.org/src/info/5981a8c041a3c2f3
CREATE TABLE t1(id INTEGER PRIMARY KEY, a INT, b INT, c INT)$$
INSERT INTO t1 VALUES(10,1,2,5);
INSERT INTO t1 VALUES(20,1,3,5);
INSERT INTO t1 VALUES(30,1,2,4);
INSERT INTO t1 VALUES(40,1,3,4);
CREATE INDEX t1x ON t1(a,b,c);

ANALYZE t1;

UPDATE sqlite_stat1 SET stat = '84000 3 2 1' WHERE idx LIKE '$T1X%';
UPDATE sqlite_stat1 set stat = '84000 1' WHERE idx LIKE '$COMDB2_PK_%';

# the following DDL forces new sql engines so that new stats get picked up
CREATE TABLE dummy(i INT)$$

SELECT * FROM t1 WHERE a=1 AND b IN (2,3) AND c BETWEEN 4 AND 5 ORDER BY +id;

DROP TABLE t1;
DROP TABLE dummy;
