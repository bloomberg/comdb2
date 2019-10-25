SELECT '---------------------------------- PART #00 ----------------------------------' AS part;
CREATE TABLE t1(i INT) $$
DROP TABLE t1;
# a quick test to check selecting from comdb2_sc_status does not hang
SELECT COUNT(*) > 0 FROM comdb2_sc_status;

SELECT '---------------------------------- PART #01 ----------------------------------' AS part;
CREATE TABLE t1(i INT) $$
CREATE TABLE t1(i INT) $$
CREATE TABLE T1(i INT) $$
CREATE TABLE 't1'(i INT) $$
CREATE TABLE "t1"(i INT) $$
CREATE TABLE `t1`(i INT) $$
CREATE TABLE t2(i INT, i INT) $$
CREATE TABLE t3('i' INT, `j` INT, "k" INT) $$
CREATE TABLE t4(i INT NULL) $$
CREATE TABLE t5(i INT NULL, j INT NULL) $$
CREATE TABLE t6(i INT NOT NULL) $$
CREATE TABLE t7(i INT NOT NULL, j INT NOT NULL) $$
CREATE TABLE t8(i INT DEFAULT 1) $$
CREATE TABLE t9(i INT NOT NULL DEFAULT 0) $$
CREATE TABLE t10(i INT NULL DEFAULT 0) $$

SELECT '---------------------------------- PART #02 ----------------------------------' AS part;
SELECT * FROM comdb2_tables WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_columns WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_keys WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_constraints WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM sqlite_master WHERE name NOT LIKE 'sqlite_stat%';

SELECT '---------------------------------- PART #03 ----------------------------------' AS part;
DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
DROP TABLE t4;
DROP TABLE t5;
DROP TABLE t6;
DROP TABLE t7;
DROP TABLE t8;
DROP TABLE t9;
DROP TABLE t10;

SELECT '---------------------------------- PART #04 ----------------------------------' AS part;
CREATE TABLE t1(i INT PRIMARY KEY) $$
CREATE TABLE t2(i INT PRIMARY KEY, j INT) $$
CREATE TABLE t3(i INT PRIMARY KEY, j INT PRIMARY KEY) $$
CREATE TABLE t4(i INT, i INT) $$
CREATE TABLE t5(i INT UNIQUE) $$
CREATE TABLE t6(i INT PRIMARY KEY, j INT UNIQUE) $$
CREATE TABLE t7(i INTt) $$
CREATE TABLE t8(INT i) $$
CREATE TABLE t9(i INT , j INT, PRIMARY KEY(i, j)) $$
CREATE TABLE t10(i INT, j INT, UNIQUE(i, j)) $$

SELECT '---------------------------------- PART #05 ----------------------------------' AS part;
SELECT * FROM comdb2_tables WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_columns WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_keys WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_constraints WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM sqlite_master WHERE name NOT LIKE 'sqlite_stat%';

SELECT '---------------------------------- PART #06 ----------------------------------' AS part;
DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
DROP TABLE t4;
DROP TABLE t5;
DROP TABLE t6;
DROP TABLE t7;
DROP TABLE t8;
DROP TABLE t9;
DROP TABLE t10;

SELECT '---------------------------------- PART #07 ----------------------------------' AS part;
CREATE TABLE t1(i INT, j INT, UNIQUE(i, j), UNIQUE(i, j)) $$
CREATE TABLE t2(i INT, j INT, UNIQUE(i, j), UNIQUE(j, i)) $$
CREATE TABLE IF NOT EXISTS t3(i INT) $$
CREATE TABLE IF NOT EXISTS t3(i INT) $$
CREATE TABLE t4(i INT, j INT, UNIQUE(i DESC)) $$
CREATE TABLE t5(i INT, j INT, UNIQUE(i ASC)) $$
CREATE TABLE t6(i INT, j INT, UNIQUE(i ASC, j DESC)) $$
CREATE TABLE t7(i INT, j INT, UNIQUE(i DESC, j DESC)) $$
CREATE TABLE t8(i INT, j INT, UNIQUE(i DESC, j), UNIQUE(i, j), UNIQUE(i, j DESC)) $$

SELECT '---------------------------------- PART #08 ----------------------------------' AS part;
SELECT * FROM comdb2_tables WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_columns WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_keys WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_constraints WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM sqlite_master WHERE name NOT LIKE 'sqlite_stat%';

SELECT '---------------------------------- PART #09 ----------------------------------' AS part;
DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
DROP TABLE t4;
DROP TABLE t5;
DROP TABLE t6;
DROP TABLE t7;
DROP TABLE t8;

SELECT '---------------------------------- PART #10 ----------------------------------' AS part;
CREATE TABLE t1(i INT PRIMARY KEY) $$
CREATE TABLE t2(i INT PRIMARY KEY, FOREIGN KEY (i) REFERENCES t1(i)) $$
CREATE TABLE t3(i INT, FOREIGN KEY (i) REFERENCES t1(i)) $$
CREATE TABLE t4(i INT) $$
CREATE TABLE t5(i INT PRIMARY KEY, FOREIGN KEY (i) REFERENCES t4(i)) $$
CREATE TABLE t6(i INT PRIMARY KEY REFERENCES t1(i)) $$
CREATE TABLE t7(i INT, j INT, PRIMARY KEY(i, j)) $$
CREATE TABLE t8(i INT, j INT, PRIMARY KEY(i, j), FOREIGN KEY (i, j) REFERENCES t7(i, j)) $$
CREATE TABLE t9(i INT, j INT, PRIMARY KEY(i), FOREIGN KEY (i, j) REFERENCES t7(i, j)) $$

SELECT '---------------------------------- PART #11 ----------------------------------' AS part;
SELECT * FROM comdb2_columns WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_keys WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_constraints WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM sqlite_master WHERE name NOT LIKE 'sqlite_stat%';

SELECT '---------------------------------- PART #12 ----------------------------------' AS part;
DROP TABLE t2;
DROP TABLE t3;
DROP TABLE t4;
DROP TABLE t5;
DROP TABLE t6;
DROP TABLE t8;
DROP TABLE t9;
DROP TABLE t1;
DROP TABLE t7;

SELECT '---------------------------------- PART #13 ----------------------------------' AS part;
# Table options
CREATE TABLE t1(i INT) OPTIONS REC ZLIB, BLOBFIELD ZLIB $$
CREATE TABLE t2(i INT) OPTIONS REC ZLIB, REBUILD $$
CREATE TABLE t3(i INT) OPTIONS REC LZ4, BLOBFIELD LZ4 $$

SELECT '---------------------------------- PART #14 ----------------------------------' AS part;
SELECT * FROM comdb2_tables WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_columns WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_keys WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_constraints WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM sqlite_master WHERE name NOT LIKE 'sqlite_stat%';

SELECT '---------------------------------- PART #15 ----------------------------------' AS part;
DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;

SELECT '---------------------------------- PART #16 ----------------------------------' AS part;
# Types
CREATE TABLE t1(v VUTF8(100)) $$
CREATE TABLE t2('d' DATETIME) $$
CREATE TABLE t3("t" TEXT(100)) $$
CREATE TABLE t4(`t` U_SHORT) $$
CREATE TABLE t5(c CHAR(100)) $$
CREATE TABLE t6(a INT(100)) $$
CREATE TABLE t7(v VARCHAR(    100)) $$
CREATE TABLE t8(v VARCHAR(    100  )) $$
CREATE TABLE t9(d DECIMAL64) $$
CREATE TABLE t10(f FLOAT, d DOUBLE, r REAL) $$
CREATE TABLE t11(i INTEGER, j SMALLINT, k BIGINT) $$

SELECT '---------------------------------- PART #17 ----------------------------------' AS part;
SELECT * FROM comdb2_columns WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_keys WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_constraints WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM sqlite_master WHERE name NOT LIKE 'sqlite_stat%';

SELECT '---------------------------------- PART #18 ----------------------------------' AS part;
DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
DROP TABLE t4;
DROP TABLE t5;
DROP TABLE t6;
DROP TABLE t7;
DROP TABLE t8;
DROP TABLE t9;
DROP TABLE t10;
DROP TABLE t11;

SELECT '---------------------------------- PART #19 ----------------------------------' AS part;
CREATE TABLE main.t1(i INT) $$
CREATE TABLE remotedb.t1(i INT) $$

SELECT '---------------------------------- PART #20 ----------------------------------' AS part;
DROP TABLE t1;

SELECT '---------------------------------- PART #21 ----------------------------------' AS part;
CREATE TABLE t1(b blob) $$
CREATE TABLE t2(b blob[1]) $$
CREATE TABLE t3(b blob(1)) $$
CREATE TABLE t4(b blob(100)) $$
CREATE TABLE t5(b blob(0)) $$
CREATE TABLE t6(b blob(-100)) $$

SELECT '---------------------------------- PART #22 ----------------------------------' AS part;
SELECT * FROM comdb2_columns WHERE tablename NOT LIKE 'sqlite_stat%';

SELECT '---------------------------------- PART #23 ----------------------------------' AS part;
CREATE TABLE t4(b blob(0)) $$
CREATE TABLE t5(b blob(-100)) $$

SELECT '---------------------------------- PART #24 ----------------------------------' AS part;
SELECT * FROM comdb2_columns WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_keys WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_constraints WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM sqlite_master WHERE name NOT LIKE 'sqlite_stat%';

SELECT '---------------------------------- PART #25 ----------------------------------' AS part;
DROP TABLE t1;
DROP TABLE t3;
DROP TABLE t4;
DROP TABLE t5;

SELECT '---------------------------------- PART #26 ----------------------------------' AS part;
CREATE TABLE t1(i INT PRIMARY KEY) $$
CREATE TABLE t2(i INT PRIMARY KEY, FOREIGN KEY (i) REFERENCES t1) $$
CREATE TABLE t2(i INT PRIMARY KEY, FOREIGN KEY (i) REFERENCES t1(i)) $$
CREATE TABLE t3(i INT PRIMARY KEY, FOREIGN KEY (i) REFERENCES t1(i) ON DELETE CASCADE) $$
CREATE TABLE t4(i INT PRIMARY KEY, FOREIGN KEY (i) REFERENCES t1(i) ON UPDATE CASCADE ON DELETE CASCADE) $$
CREATE TABLE t5(i INT PRIMARY KEY, FOREIGN KEY (i) REFERENCES t1(i) ON DELETE NO ACTION) $$
CREATE TABLE t6(i INT PRIMARY KEY, FOREIGN KEY (i) REFERENCES t1(i) ON DELETE SET NULL) $$
CREATE TABLE t6(i INT PRIMARY KEY, FOREIGN KEY (i) REFERENCES t1(i) ON DELETE SET DEFAULT) $$
CREATE TABLE t6(i INT PRIMARY KEY, FOREIGN KEY (i) REFERENCES t1(i) ON DELETE RESTRICT) $$
CREATE TABLE t6(i INT PRIMARY KEY, FOREIGN KEY (i) REFERENCES t1(i) ON DELETE junk) $$

SELECT '---------------------------------- PART #27 ----------------------------------' AS part;
SELECT * FROM comdb2_columns WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_keys WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_constraints WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM sqlite_master WHERE name NOT LIKE 'sqlite_stat%';

SELECT '---------------------------------- PART #28 ----------------------------------' AS part;
DROP TABLE t5;
DROP TABLE t4;
DROP TABLE t3;
DROP TABLE t2;
DROP TABLE t1;

SELECT '---------------------------------- PART #29 ----------------------------------' AS part;
CREATE TABLE t1(i INT NULL, PRIMARY KEY(i)) $$
CREATE TABLE t2(i INT NULL PRIMARY KEY) $$
CREATE TABLE t3(i INT NULL, j INT NOT NULL, PRIMARY KEY(i, j)) $$

SELECT '---------------------------------- PART #30 ----------------------------------' AS part;
INSERT INTO t1 VALUES(NULL);
INSERT INTO t2 VALUES(NULL);
INSERT INTO t3 VALUES(1, NULL);
INSERT INTO t3 VALUES(NULL, 1);
INSERT INTO t3 VALUES(NULL, NULL);

SELECT '---------------------------------- PART #31 ----------------------------------' AS part;
SELECT COUNT(*)=0 FROM t1;
SELECT COUNT(*)=0 FROM t2;
SELECT COUNT(*)=0 FROM t3;

SELECT '---------------------------------- PART #32 ----------------------------------' AS part;
SELECT * FROM comdb2_columns WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_keys WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_constraints WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM sqlite_master WHERE name NOT LIKE 'sqlite_stat%';

SELECT '---------------------------------- PART #33 ----------------------------------' AS part;
DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;

SELECT '---------------------------------- PART #34 ----------------------------------' AS part;
# Some tests for quoted default values for columns.
CREATE TABLE t1(i INT, j INT DEFAULT '1') $$
INSERT INTO t1(i) VALUES (1);
SELECT * FROM t1;
DROP TABLE t1;

SELECT '---------------------------------- PART #35 ----------------------------------' AS part;
CREATE TABLE t1(i INT, j INT DEFAULT "1") $$
INSERT INTO t1(i) VALUES (1);
SELECT * FROM t1;
DROP TABLE t1;

SELECT '---------------------------------- PART #36 ----------------------------------' AS part;
CREATE TABLE t1(i INT, c CHAR(100) DEFAULT 0) $$
INSERT INTO t1(i) VALUES (1);
SELECT * FROM t1;
DROP TABLE t1;

SELECT '---------------------------------- PART #37 ----------------------------------' AS part;
CREATE TABLE t1(i INT, c CHAR(100) DEFAULT 'foo') $$
INSERT INTO t1(i) VALUES (1);
SELECT * FROM t1;
DROP TABLE t1;

SELECT '---------------------------------- PART #38 ----------------------------------' AS part;
CREATE TABLE t1(i INT, d DATETIME DEFAULT 'CURRENT_TIMESTAMP') $$
INSERT INTO t1(i) VALUES (1);
INSERT INTO t1(i) VALUES (2);
SELECT COUNT(*) = 2 FROM t1;
DROP TABLE t1;

SELECT '---------------------------------- PART #39 ----------------------------------' AS part;
CREATE TABLE t1(i INT, d INTERVALDS DEFAULT '1 11:11:11.111') $$
CREATE TABLE t1(i INT, b BLOB(100) DEFAULT 'xxxxx') $$

SELECT '---------------------------------- PART #40 ----------------------------------' AS part;
CREATE TABLE t1(i INT, f1 FLOAT DEFAULT '1.1', f2 FLOAT DEFAULT "1.1" ,f3 FLOAT DEFAULT 1.1) $$
INSERT INTO t1(i) VALUES (1);
SELECT * FROM t1;
DROP TABLE t1;

SELECT '---------------------------------- PART #41 ----------------------------------' AS part;
CREATE TABLE t1(i INT, j INT)$$
CREATE INDEX t1_ij ON t1(i, j)
CREATE INDEX t1_ji ON t1(j, i)
CREATE TABLE t2(i INT, j INT)$$
CREATE INDEX 'dup1' ON t2(i, j)
CREATE INDEX dup2 ON t2(j, i)
CREATE TABLE t3(i INT, j INT, UNIQUE(i,j), UNIQUE(j,i))$$
CREATE TABLE t4(i INT, j INT, UNIQUE 'uniq1'(i,j), UNIQUE 'uniq2'(j,i))$$
CREATE TABLE t5(i INT UNIQUE, j INT, UNIQUE(i,j), UNIQUE 'unique_key'(j,i))$$
CREATE INDEX t5_ij ON t5(i, j)
CREATE INDEX dup_key ON t5(i, j)
CREATE TABLE t6(i INT) $$
CREATE INDEX COMDB2_PK ON t6(i)
CREATE TABLE t7(i INT, j INT) $$
CREATE INDEX 'dup' ON t7(i)
CREATE INDEX 'dup' ON t7(j)
CREATE INDEX 'xxxx' ON t7(i)
CREATE UNIQUE INDEX 'xxxx' ON t7(j)

SELECT '---------------------------------- PART #42 ----------------------------------' AS part;
SELECT * FROM comdb2_columns WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_keys WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_constraints WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM sqlite_master WHERE name NOT LIKE 'sqlite_stat%';

SELECT '---------------------------------- PART #43 ----------------------------------' AS part;
DROP TABLE t1;
DROP TABLE t2
DROP TABLE t3;
DROP TABLE t4;
DROP TABLE t5;
DROP TABLE t6;
DROP TABLE t7;

SELECT '---------------------------------- PART #44 ----------------------------------' AS part;
CREATE TABLE t1(c1 CHAR(2), c2 CSTRING(2), c3 VARCHAR(2), c4 TEXT) $$
SELECT * FROM comdb2_columns WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM sqlite_master WHERE name NOT LIKE 'sqlite_stat%';
DROP TABLE t1;

SELECT '---------------------------------- PART #45 ----------------------------------' AS part;
CREATE TABLE t1(i INT UNIQUE ASC) $$
CREATE TABLE t1(i INT UNIQUE DESC) $$
CREATE TABLE t1(i INT PRIMARY KEY ASC) $$
CREATE TABLE t2(i INT PRIMARY KEY DESC) $$
CREATE TABLE t3(i INT UNIQUE) $$
CREATE TABLE t4(i INT) $$
CREATE INDEX t4_i ON t4(i)
CREATE TABLE t5(i INT, j INT) $$
CREATE INDEX t5_ji ON t5(j DESC, i ASC)

SELECT '---------------------------------- PART #46 ----------------------------------' AS part;
SELECT * FROM comdb2_columns WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_keys WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_constraints WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM sqlite_master WHERE name NOT LIKE 'sqlite_stat%';

SELECT '---------------------------------- PART #47 ----------------------------------' AS part;
DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
DROP TABLE t4;
DROP TABLE t5;

SELECT '---------------------------------- PART #48 ----------------------------------' AS part;
CREATE TABLE t1(unique INT UNIQUE) $$
CREATE TABLE t1('unique' INT UNIQUE) $$

SELECT '---------------------------------- PART #49 ----------------------------------' AS part;
SELECT * FROM comdb2_columns WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_keys WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_constraints WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM sqlite_master WHERE name NOT LIKE 'sqlite_stat%';

SELECT '---------------------------------- PART #50 ----------------------------------' AS part;
DROP TABLE t1;

SELECT '---------------------------------- PART #51 ----------------------------------' AS part;
CREATE TABLE t1(i INT, j INT)$$
CREATE INDEX t1_ij1 on t1(i, j);
CREATE INDEX t1_ij2 on t1(i DESC, j ASC);
CREATE INDEX t1_ij3 on t1(i ASC, j DESC);
CREATE INDEX t1_ij4 on t1(i DESC, j DESC);
CREATE TABLE t2(i INT REFERENCES t1(i)) $$
CREATE TABLE t3(i INT REFERENCES t1(i DESC)) $$
CREATE TABLE t4(i INT, j INT, FOREIGN KEY (i, j) REFERENCES t1(i, j)) $$
CREATE TABLE t5(i INT, j INT, FOREIGN KEY (i DESC, j) REFERENCES t1(i DESC, j)) $$
CREATE TABLE t6(i INT, j INT, FOREIGN KEY (i, j DESC) REFERENCES t1(i, j DESC)) $$
CREATE TABLE t7(i INT, j INT, FOREIGN KEY (i DESC, j DESC) REFERENCES t1(i DESC, j DESC)) $$
CREATE TABLE t8(i INT, FOREIGN KEY (i DESC) REFERENCES t1(i DESC)) $$

SELECT '---------------------------------- PART #52 ----------------------------------' AS part;
SELECT * FROM comdb2_columns WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_keys WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_constraints WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM sqlite_master WHERE name NOT LIKE 'sqlite_stat%';

SELECT '---------------------------------- PART #53 ----------------------------------' AS part;
DROP TABLE t2;
DROP TABLE t4;
DROP TABLE t1;

SELECT '---------------------------------- PART #54 ----------------------------------' AS part;
CREATE TABLE t1(i INT PRIMARY KEY)$$
CREATE TABLE t2(i INT CONSTRAINT mycons1 REFERENCES t1(i)) $$
CREATE TABLE t3(i INT, CONSTRAINT 'mycons2' FOREIGN KEY (i) REFERENCES t1(i)) $$
CREATE TABLE t4(i INT, FOREIGN KEY (i) REFERENCES t1(i) CONSTRAINT "mycons3") $$

SELECT '---------------------------------- PART #55 ----------------------------------' AS part;
SELECT * FROM comdb2_constraints WHERE tablename NOT LIKE 'sqlite_stat%';

SELECT '---------------------------------- PART #56 ----------------------------------' AS part;
DROP TABLE t2;
DROP TABLE t3;
DROP TABLE t4;
DROP TABLE t1;

SELECT '---------------------------------- PART #57 ----------------------------------' AS part;
# CTAS is currently not supported.
CREATE TABLE t1(i INT) $$
INSERT INTO t1 VALUES(1);
CREATE TABLE t2 AS SELECT * FROM t1 $$
DROP TABLE t1;

SELECT '---------------------------------- PART #58 ----------------------------------' AS part;
CREATE TABLE t1(i INT (100))$$
CREATE TABLE t1(v VARCHAR (100))$$
CREATE TABLE t2(v VARCHAR      (100)    )$$
SELECT csc2 FROM sqlite_master WHERE name NOT LIKE 'sqlite_stat%';
DROP TABLE t1;
DROP TABLE t2;

SELECT '---------------------------------- PART #59 ----------------------------------' AS part;
CREATE TABLE t1(i INT, j DEFAULT 1) $$

CREATE TABLE t1(v CSTRING(10), UNIQUE(CAST(v || 'aaa' AS CSTRING(10))))$$
SELECT csc2 FROM sqlite_master WHERE tbl_name = 't1' AND type = 'table';
DROP TABLE t1;

# Test an invalid case of providing expression without CAST()
CREATE TABLE t1(json vutf8(128), UNIQUE (json_extract(json, '$.a')))$$

CREATE TABLE t1(json vutf8(128), UNIQUE (CAST(json_extract(json, '$.a') AS int)), UNIQUE (CAST(json_extract(json, '$.b') AS cstring(10)))) $$
SELECT csc2 FROM sqlite_master WHERE tbl_name = 't1' AND type = 'table';
INSERT INTO t1 VALUES ('{"a":0,"b":"zero"}'), ('{"a":1,"b":"one"}');
INSERT INTO t1 VALUES ('{"a":0,"b":"zero"}'), ('{"a":1,"b":"one"}');
SELECT * FROM t1;
DROP TABLE t1;

SELECT '---------------------------------- PART #60 ----------------------------------' AS part;
CREATE TABLE t1(i INT PRIMARY KEY)$$
CREATE TABLE t2(i INT CONSTRAINT mycons1 REFERENCES t1(i) ON UPDATE CASCADE ON DELETE CASCADE)$$
SELECT csc2 FROM sqlite_master WHERE tbl_name = 't2' AND type = 'table';
DROP TABLE t2;
DROP TABLE t1;
