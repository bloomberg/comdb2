SELECT '---------------------------------- PART #01 ----------------------------------' AS part;
# Simple cases
CREATE TABLE t1(i INT) $$
INSERT INTO t1 VALUES(1), (1);
SELECT * FROM t1;
DROP TABLE t1;

SELECT '---------------------------------- PART #02 ----------------------------------' AS part;
CREATE TABLE t1(i INT PRIMARY KEY) $$
INSERT INTO t1 VALUES(1), (1);
SELECT * FROM t1;
DROP TABLE t1;

SELECT '---------------------------------- PART #03 ----------------------------------' AS part;
# Test with non-unique (dup) key
CREATE TABLE t1(i INT PRIMARY KEY, j INT) $$
CREATE INDEX t1_j ON t1(j)
INSERT INTO t1 VALUES(0,1);
INSERT INTO t1 VALUES(1,1);
SELECT * FROM t1 ORDER BY i;
DROP TABLE t1;

SELECT '---------------------------------- PART #04 ----------------------------------' AS part;
# ON CONFLICT DO NOTHING
CREATE TABLE t1(i INT PRIMARY KEY, j INT UNIQUE) $$
INSERT INTO t1 VALUES(0,1);
INSERT INTO t1 VALUES(1,1) ON CONFLICT DO NOTHING;
SELECT * FROM t1 ORDER BY i;
DROP TABLE t1;

SELECT '---------------------------------- PART #05 ----------------------------------' AS part;
# ON CONFLICT DO REPLACE
CREATE TABLE t1(i INT PRIMARY KEY, j INT UNIQUE) $$
INSERT INTO t1 VALUES(0,1);
REPLACE INTO t1 VALUES(1,1);
SELECT * FROM t1 ORDER BY i;

SELECT '---------------------------------- PART #06 ----------------------------------' AS part;
CREATE TABLE t2(i INT PRIMARY KEY, j INT UNIQUE) $$
INSERT INTO t2 VALUES(0,1);
INSERT INTO t2 VALUES(1,0);
INSERT INTO t2 VALUES(1,1);
# Replaces both the existing records
REPLACE INTO t2 VALUES(1,1), (2,2), (3,3);
SELECT * FROM t2 ORDER BY i,j;

SELECT '---------------------------------- PART #07 ----------------------------------' AS part;
CREATE TABLE t3(i INT PRIMARY KEY, j INT, k INT UNIQUE) $$
INSERT INTO t3 VALUES(0,1,0);
INSERT INTO t3 VALUES(1,0,1);
# Another syntax for .. ON CONFLICT DO REPLACE ..
REPLACE INTO t3 VALUES(1,2,0);
SELECT * FROM t3 ORDER BY i,j,k;

SELECT '---------------------------------- PART #08 ----------------------------------' AS part;
# A case where the same record is up for deletion twice.
REPLACE INTO t3 VALUES(1,3,0);
SET TRANSACTION READ COMMITTED;
REPLACE INTO t3 VALUES(1,3,0);
SET TRANSACTION BLOCKSQL;

SELECT * FROM t3 ORDER BY i;

SELECT '---------------------------------- PART #09 ----------------------------------' AS part;
DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;

SELECT '---------------------------------- PART #10 ----------------------------------' AS part;
# ON CONFLICT DO UPDATE
CREATE TABLE t1(i INT PRIMARY KEY, j INT, k INT UNIQUE) $$
INSERT INTO t1 VALUES(0,1,0);
INSERT INTO t1 VALUES(1,0,1);
INSERT INTO t1 VALUEs(0,1,2);
INSERT INTO t1 VALUEs(0,1,2) ON CONFLICT (i) DO UPDATE SET z=z+1
INSERT INTO t1 VALUEs(0,1,2) ON CONFLICT (i) DO UPDATE SET k=z+1
INSERT INTO t1 VALUEs(0,1,2) ON CONFLICT (i) DO UPDATE SET k=k+1
INSERT INTO t1 VALUEs(0,1,2) ON CONFLICT (i) DO UPDATE SET j=j+1
INSERT INTO t1 VALUEs(2,1,3) ON CONFLICT (i) DO UPDATE SET i=1
SELECT * FROM t1 ORDER BY i, j, k;
DROP TABLE t1;

SELECT '---------------------------------- PART #11 ----------------------------------' AS part;
# EXCLUDED (sqlite/test/upsert2.test)
CREATE TABLE t1(a INTEGER PRIMARY KEY, b int, c INT DEFAULT 0)$$
INSERT INTO t1(a,b) VALUES(1,2),(3,4);
INSERT INTO t1(a,b) VALUES(1,8),(2,11),(3,1) ON CONFLICT(a) DO UPDATE SET b=excluded.b, c=c+1 WHERE t1.b<excluded.b;
SELECT * FROM t1 ORDER BY a, b;
DROP TABLE t1;

SELECT '---------------------------------- PART #12 ----------------------------------' AS part;
CREATE TABLE t1(x INTEGER PRIMARY KEY, y INT UNIQUE)$$
INSERT INTO t1(x,y) SELECT 1,2 WHERE 1 ON CONFLICT(x) DO UPDATE SET y=max(t1.y,excluded.y) AND 1;
DROP TABLE t1;

SELECT '---------------------------------- PART #13 ----------------------------------' AS part;
CREATE TABLE t1(x INTEGER PRIMARY KEY, y INT UNIQUE) $$
INSERT INTO t1(x,y) SELECT 1,2 WHERE 1 ON CONFLICT(x) DO UPDATE SET y=max(t1.y,excluded.y) AND 1;
SELECT * FROM t1 ORDER BY x, y;
INSERT INTO t1(x,y) SELECT 1,1 WHERE 1 ON CONFLICT(x) DO UPDATE SET y=max(t1.y,excluded.y) AND 1;
SELECT * FROM t1 ORDER BY x, y;
DROP TABLE t1;

SELECT '---------------------------------- PART #14 ----------------------------------' AS part;
# INDEX ON EXPRESSION (sqlite/test/upsert1.test)
CREATE TABLE t1 {
schema
    {
		int a
		int b null = yes
		int c dbstore = 0 null = yes
    }
keys
    {
		"COMDB2_PK" = a
		"idx1" = (int)"a+b"
    }
} $$
INSERT INTO t1(a,b) VALUES(7,8) ON CONFLICT(a+b) DO NOTHING;
INSERT INTO t1(a,b) VALUES(8,7),(9,6) ON CONFLICT(a+b) DO NOTHING;
SELECT * FROM t1 ORDER BY a, b;

SELECT '---------------------------------- PART #15 ----------------------------------' AS part;
INSERT INTO t1(a,b) VALUES(8,7),(9,6) ON CONFLICT(a) DO NOTHING;
SELECT * FROM t1 ORDER BY a, b;
DROP TABLE t1;

SELECT '---------------------------------- PART #16 ----------------------------------' AS part;
CREATE TABLE t1(i INT PRIMARY KEY, j INT UNIQUE, k INT, l INT UNIQUE)$$
CREATE INDEX t1_k ON t1(k);
INSERT INTO t1 VALUES(1,1,1,1);
INSERT INTO t1 VALUES(1,1,1,1);
INSERT INTO t1 VALUES(1,1,1,1) ON CONFLICT DO NOTHING;
INSERT INTO t1 VALUES(1,1,1,1) ON CONFLICT(i) DO NOTHING;
INSERT INTO t1 VALUES(1,1,1,1) ON CONFLICT(j) DO NOTHING;
INSERT INTO t1 VALUES(1,1,1,1) ON CONFLICT(k) DO NOTHING;
INSERT INTO t1 VALUES(1,1,1,1) ON CONFLICT(l) DO NOTHING;
INSERT INTO t1 VALUES(1,1,1,1) ON CONFLICT(m) DO NOTHING;
SELECT * FROM t1 ORDER BY i, j, k, l;

SELECT '---------------------------------- PART #17 ----------------------------------' AS part;
INSERT INTO t1 VALUES(2,1,1,1) ON CONFLICT(j) DO NOTHING;
INSERT INTO t1 VALUES(2,1,1,2) ON CONFLICT(j) DO NOTHING;
SELECT * FROM t1 ORDER BY i, j, k, l;

SELECT '---------------------------------- PART #18 ----------------------------------' AS part;
INSERT INTO t1 VALUES(2,2,1,1) ON CONFLICT(l) DO NOTHING;
SELECT * FROM t1 ORDER BY i, j, k, l;

SELECT '---------------------------------- PART #19 ----------------------------------' AS part;
INSERT INTO t1 VALUES(99, 99, 1, 99) ON CONFLICT DO NOTHING;
SELECT * FROM t1 ORDER BY i, j, k, l;
DROP TABLE t1;

SELECT '---------------------------------- PART #20 ----------------------------------' AS part;
CREATE TABLE t1 (i INT, j INT) $$
CREATE UNIQUE INDEX idx1 ON t1(i) WHERE j > 6;
INSERT INTO t1 VALUES(1, 10);
INSERT INTO t1 VALUES(1, 11);
SELECT * FROM t1 ORDER BY i, j;

SELECT '---------------------------------- PART #21 ----------------------------------' AS part;
INSERT INTO t1 VALUES(1, 4);
SELECT * FROM t1 ORDER BY i, j;

SELECT '---------------------------------- PART #22 ----------------------------------' AS part;
INSERT INTO t1 VALUES(1, 4) ON CONFLICT(i) WHERE j > 6 DO NOTHING;
SELECT * FROM t1 ORDER BY i, j;
DROP TABLE t1;

SELECT '---------------------------------- PART #23 ----------------------------------' AS part;
# Test uniqnulls
CREATE TABLE t1(i INT UNIQUE)$$
INSERT INTO t1 VALUES(1);
INSERT INTO t1 VALUES(NULL);
INSERT INTO t1 VALUES(NULL);
INSERT INTO t1 VALUES(NULL) ON CONFLICT(i) DO NOTHING;
SELECT COUNT(*)=4 FROM t1;
SELECT * FROM t1 ORDER BY i;
DROP TABLE t1;

SELECT '---------------------------------- PART #24 ----------------------------------' AS part;
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
CREATE TABLE t1(i INT UNIQUE, j INT)$$
CREATE TABLE t2(i INT, j INT)$$
INSERT INTO t2 VALUES(1, 1);
SET TRANSACTION READ COMMITTED
BEGIN;
INSERT INTO t1 VALUES(1, 1) ON CONFLICT DO NOTHING;
DELETE FROM t2;
INSERT INTO t1 VALUES(1, 2) ON CONFLICT DO NOTHING;
COMMIT;
SELECT 't1' AS tbl, * FROM t1;
SELECT 't2' AS tbl, * FROM t2;
DROP TABLE t1;
DROP TABLE t2;

SELECT '---------------------------------- PART #25 ----------------------------------' AS part;
DROP TABLE IF EXISTS p;
DROP TABLE IF EXISTS c;
CREATE TABLE p(i INT UNIQUE)$$
CREATE TABLE c(i INT UNIQUE, FOREIGN KEY(i) REFERENCES p(i) ON UPDATE CASCADE ON DELETE CASCADE)$$
INSERT INTO p VALUES(1)
INSERT INTO c VALUES(1)
BEGIN
UPDATE p SET i = 2 WHERE i = 1
INSERT INTO c VALUES(2) ON CONFLICT DO NOTHING
COMMIT
SELECT 'p' AS tbl, * FROM p;
SELECT 'c' AS tbl, * FROM c;
DROP TABLE c;
DROP TABLE p;

SELECT '-- DRQS 143875590 --';
CREATE TABLE t1(c1 VARCHAR(10), c2 INT)$$
CREATE UNIQUE INDEX idx ON t1(c1, c2);
INSERT INTO t1 VALUES('aaa', 7);
INSERT INTO t1 VALUES('aaa', 3) ON CONFLICT(c1, c2) DO NOTHING;
SELECT * FROM t1;
INSERT INTO t1 VALUES('aaa', 3);
INSERT INTO t1 VALUES('aaa', 3) ON CONFLICT(c1, c2) DO NOTHING;
DROP TABLE t1;

SELECT '---- CHECK CONSTRAINT & UPSERT ----' as test;
CREATE TABLE t1(i INT UNIQUE, CHECK(i>10))$$
INSERT INTO t1 VALUES(1);
INSERT INTO t1 VALUES(11);
INSERT INTO t1 VALUES(11);
INSERT INTO t1 VALUES(11) ON CONFLICT(i) DO NOTHING;
SELECT * FROM t1;
INSERT INTO t1 VALUES(11) ON CONFLICT(i) DO UPDATE SET i = 1;
SELECT * FROM t1;
INSERT INTO t1 VALUES(11) ON CONFLICT(i) DO UPDATE SET i = i+1;
SELECT * FROM t1;
DROP TABLE t1;
