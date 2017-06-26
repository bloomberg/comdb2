CREATE TABLE t1(i INT) $$
CREATE INDEX idx1 ON t1(i);
CREATE INDEX idx1 ON t1(i);
CREATE INDEX IF NOT EXISTS idx1 ON t1(i);
CREATE UNIQUE INDEX idx1 ON t1(i);
CREATE UNIQUE INDEX idx2 ON t1(i);
CREATE INDEX 'idx3' ON t1(i);
CREATE INDEX "idx4" ON t1(i);
CREATE INDEX `idx5` ON t1(i);
CREATE INDEX idx6 ON t1('i');
CREATE INDEX IF NOT EXISTS idx7 ON t1('i');

CREATE TABLE t2(i INT, j INT) $$
CREATE INDEX idx1 ON t2 (i) WHERE (j > 10);
CREATE INDEX idx3 ON t2(i,j);
CREATE INDEX idx3 ON t2(i,j);
CREATE INDEX idx3 ON t2(i);
CREATE INDEX idx4 ON t2(i,j);

CREATE TABLE t3(i INT, j INT) $$
CREATE INDEX idx1 ON t3(i) WITH DATACOPY;
CREATE INDEX idx2 ON t3(i) WITH DATACOPY WHERE (j > 10);
CREATE INDEX idx3 ON t3(i COLLATE DATACOPY);

CREATE TABLE t4(i INT) $$
CREATE INDEX 'uniqueidxname' ON t4(i);
CREATE INDEX 'sameidxname' ON t4(i);
CREATE TABLE t5(i INT) $$
CREATE INDEX 'sameidxname' ON t5(i);
DROP INDEX 'uniqueidxnameXXX';
DROP INDEX IF EXISTS 'uniqueidxnameXXX';
DROP INDEX 'uniqueidxname';
DROP INDEX 'sameidxname';
DROP INDEX 'sameidxname' ON t5;
DROP INDEX IF EXISTS 'sameidxname';

SELECT * FROM comdb2_tables WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_columns WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_keys WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_constraints WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM sqlite_master WHERE name NOT LIKE 'sqlite_stat%';
DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
DROP TABLE t4;
DROP TABLE t5;

