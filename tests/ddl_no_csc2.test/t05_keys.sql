CREATE TABLE t1(i INT, j INT) $$
SELECT csc2 FROM sqlite_master WHERE name NOT LIKE 'sqlite_stat%';
INSERT INTO t1 values(1,1);
INSERT INTO t1 values(1,1);
INSERT INTO t1 values(NULL, NULL);
INSERT INTO t1 values(NULL, NULL);
SELECT COUNT(*)=4 AS result FROM t1
DROP TABLE t1;

CREATE TABLE t1(i INT PRIMARY KEY, j INT) $$
SELECT csc2 FROM sqlite_master WHERE name NOT LIKE 'sqlite_stat%';
INSERT INTO t1 values(1,1);
INSERT INTO t1 values(1,1);
INSERT INTO t1 values(NULL, NULL);
INSERT INTO t1 values(NULL, NULL);
SELECT COUNT(*)=1 AS result FROM t1
DROP TABLE t1;

# NULL columns not allowed in PK
CREATE TABLE t1(i INT PRIMARY KEY NULL, j INT) $$

# Explicit NOT NULL in PK
CREATE TABLE t1(i INT PRIMARY KEY NOT NULL, j INT) $$
SELECT csc2 FROM sqlite_master WHERE name NOT LIKE 'sqlite_stat%';
INSERT INTO t1 values(1,1);
INSERT INTO t1 values(1,1);
INSERT INTO t1 values(NULL, NULL);
INSERT INTO t1 values(NULL, NULL);
SELECT COUNT(*)=1 AS result FROM t1
DROP TABLE t1;

CREATE TABLE t1(i INT UNIQUE NULL, j INT) $$
SELECT csc2 FROM sqlite_master WHERE name NOT LIKE 'sqlite_stat%';
INSERT INTO t1 values(1,1);
INSERT INTO t1 values(1,1);
INSERT INTO t1 values(NULL, NULL);
INSERT INTO t1 values(NULL, NULL);
SELECT COUNT(*)=3 AS result FROM t1
DROP TABLE t1;

# Should behave like PK
CREATE TABLE t1(i INT UNIQUE NOT NULL, j INT) $$
SELECT csc2 FROM sqlite_master WHERE name NOT LIKE 'sqlite_stat%';
INSERT INTO t1 values(1,1);
INSERT INTO t1 values(1,1);
INSERT INTO t1 values(NULL, NULL);
INSERT INTO t1 values(NULL, NULL);
SELECT COUNT(*)=1 AS result FROM t1
DROP TABLE t1;

CREATE TABLE t1(i INT NULL, j INT) $$
CREATE INDEX t1_i ON t1(i) $$
SELECT csc2 FROM sqlite_master WHERE name NOT LIKE 'sqlite_stat%';
INSERT INTO t1 values(1,1);
INSERT INTO t1 values(1,1);
INSERT INTO t1 values(NULL, NULL);
INSERT INTO t1 values(NULL, NULL);
SELECT COUNT(*)=4 AS result FROM t1
DROP TABLE t1;

CREATE TABLE t1(i INT NOT NULL, j INT) $$
CREATE INDEX t1_i ON t1(i) $$
SELECT csc2 FROM sqlite_master WHERE name NOT LIKE 'sqlite_stat%';
INSERT INTO t1 values(1,1);
INSERT INTO t1 values(1,1);
INSERT INTO t1 values(NULL, NULL);
INSERT INTO t1 values(NULL, NULL);
SELECT COUNT(*)=2 AS result FROM t1
DROP TABLE t1;
