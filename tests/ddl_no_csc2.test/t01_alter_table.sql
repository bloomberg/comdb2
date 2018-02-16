CREATE TABLE t1(i INT) $$
ALTER TABLE t1 $$
ALTER TABLE t1 ADD COLUMN j INT $$
ALTER TABLE t1 ADD COLUMN k INT NOT NULL $$
ALTER TABLE t1 ADD COLUMN l INT NULL $$
ALTER TABLE t1 ADD COLUMN m INT DEFAULT 1 $$
ALTER TABLE t1 ADD COLUMN n INT DEFAULT 1, ADD COLUMN o INT $$
ALTER TABLE t1 ADD COLUMN p INT DEFAULT 1, DROP COLUMN p $$

CREATE TABLE t2(i INT, j INT, k INT) $$
ALTER TABLE t2 DROP COLUMN doesnotexist $$
ALTER TABLE t2 DROP COLUMN 'k' $$
ALTER TABLE t2 DROP COLUMN "i" $$
ALTER TABLE t2 DROP COLUMN `j` $$

CREATE TABLE t3(i INT, j INT, UNIQUE (i)) $$
ALTER TABLE t3 DROP COLUMN i $$
CREATE TABLE t4(i INT, j INT, k INT, UNIQUE (i,j)) $$
ALTER TABLE t4 DROP COLUMN i $$

CREATE TABLE t5(i INT) $$
ALTER TABLE t5 ADD COLUMN j INT DEFAULT 1 $$

CREATE TABLE t6(i INT) OPTIONS REC ZLIB, BLOBFIELD ZLIB $$
ALTER TABLE t6 ADD COLUMN j INT NULL $$
CREATE TABLE t7(i INT) OPTIONS REC ZLIB, REBUILD $$
ALTER TABLE t7 ADD COLUMN j INT NULL $$

CREATE TABLE t8(i BLOB) $$
ALTER TABLE t8 ADD COLUMN j BLOB(100) $$

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
DROP TABLE t6;
DROP TABLE t7;
DROP TABLE t8;

CREATE TABLE t1(v VARCHAR(10) DEFAULT 'foo', d DATETIME DEFAULT 'CURRENT_TIMESTAMP', i INT DEFAULT '10') $$
CREATE INDEX IDX ON t1(i);
CREATE TABLE t2(v VARCHAR(10) DEFAULT foo, d DATETIME DEFAULT CURRENT_TIMESTAMP, i INT DEFAULT 10) $$
CREATE INDEX IDX ON t2(i);

SELECT * FROM comdb2_tables WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_columns WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_keys WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_constraints WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM sqlite_master WHERE name NOT LIKE 'sqlite_stat%';

DROP INDEX IDX on t1;
DROP INDEX IDX on t2;

SELECT * FROM comdb2_tables WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_columns WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_keys WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_constraints WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM sqlite_master WHERE name NOT LIKE 'sqlite_stat%';

DROP TABLE t1;
DROP TABLE t2;

CREATE TABLE t1(i INT, j INT, k int) $$
CREATE TABLE t2(i INT, j INT, k int) $$
ALTER TABLE t1 ADD UNIQUE INDEX idx1 (i,j) $$
ALTER TABLE t1 ADD UNIQUE INDEX idx2 (i) $$
ALTER TABLE t1 ADD INDEX idx3(j,i) $$
ALTER TABLE t1 ADD INDEX idx4(i) $$
ALTER TABLE t1 ADD PRIMARY KEY (k) $$
ALTER TABLE t2 ADD UNIQUE INDEX idx1 (i,j) $$
ALTER TABLE t2 ADD FOREIGN KEY (i,j) REFERENCES t1(i,j) $$

SELECT * FROM comdb2_tables WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_columns WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_keys WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_constraints WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM sqlite_master WHERE name NOT LIKE 'sqlite_stat%';

DROP TABLE t2;
DROP TABLE t1;

CREATE TABLE t1(i INT, j INT) $$
CREATE TABLE t2(i INT, j INT) $$
CREATE TABLE t3(i INT, j INT) $$
ALTER TABLE t1 ADD INDEX idx (i,j) $$
ALTER TABLE t2 ADD INDEX idx (i,j) $$
ALTER TABLE t2 ADD FOREIGN KEY (i,j) REFERENCES t1(i,j) $$
ALTER TABLE t2 ADD FOREIGN KEY (i,j) REFERENCES t1(i,j) $$
ALTER TABLE t3 ADD PRIMARY KEY (i,j) $$

SELECT * FROM comdb2_tables WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_columns WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_keys WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_constraints WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM sqlite_master WHERE name NOT LIKE 'sqlite_stat%';

ALTER TABLE t1 DROP INDEX 'idx' $$
ALTER TABLE t2 DROP FOREIGN KEY '$CONSTRAINT_95177019' $$
ALTER TABLE t1 DROP INDEX 'idx' $$
ALTER TABLE t2 DROP INDEX 'idx' $$
ALTER TABLE t3 DROP PRIMARY KEY $$

SELECT * FROM comdb2_tables WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_columns WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_keys WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_constraints WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM sqlite_master WHERE name NOT LIKE 'sqlite_stat%';

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;

# Test the cascading effect of deleting a column.
CREATE TABLE t1(i INT, j INT, k int, PRIMARY KEY (i,j,k)) $$
CREATE TABLE t2(i INT, j INT, k int, PRIMARY KEY (j,k), UNIQUE idx (j)) $$
ALTER TABLE t2 ADD FOREIGN KEY (i,j) REFERENCES t1(i,j) $$
SELECT * FROM comdb2_tables WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_columns WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_keys WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_constraints WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM sqlite_master WHERE name NOT LIKE 'sqlite_stat%';
ALTER TABLE t2 DROP COLUMN j $$
SELECT * FROM comdb2_tables WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_columns WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_keys WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_constraints WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM sqlite_master WHERE name NOT LIKE 'sqlite_stat%';
DROP TABLE t1;
DROP TABLE t2;

CREATE TABLE t1(i INT, j INT, k INT, UNIQUE idx1 (i,j,k), UNIQUE idx2(i DESC, j DESC, k DESC)) $$
CREATE TABLE t2(i INT, j INT, k INT, FOREIGN KEY (i,j) REFERENCES t1(i,j), FOREIGN KEY (i DESC, j DESC) REFERENCES t1(i DESC, j DESC)) $$
SELECT * FROM sqlite_master;
SELECT * FROM comdb2_constraints WHERE tablename NOT LIKE 'sqlite_stat%';
ALTER TABLE t2 DROP FOREIGN KEY '$CONSTRAINT_9C9BDEA4' $$
SELECT * FROM comdb2_tables WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_columns WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_keys WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM comdb2_constraints WHERE tablename NOT LIKE 'sqlite_stat%';
SELECT * FROM sqlite_master WHERE name NOT LIKE 'sqlite_stat%';
DROP TABLE t2;
DROP TABLE t1;

CREATE TABLE t1(i INT, j INT, k INT, UNIQUE idx1 (i,j,k), UNIQUE idx2(i DESC, j DESC, k DESC)) $$
CREATE TABLE t2(i INT, j INT, k INT, CONSTRAINT "mycons1" FOREIGN KEY (i,j) REFERENCES t1(i,j), FOREIGN KEY (i DESC, j DESC) REFERENCES t1(i DESC, j DESC)) $$
SELECT * FROM comdb2_constraints WHERE tablename NOT LIKE 'sqlite_stat%';
ALTER TABLE t2 ADD CONSTRAINT "mycons2" FOREIGN KEY (i) REFERENCES t1(i) $$
SELECT * FROM comdb2_constraints WHERE tablename NOT LIKE 'sqlite_stat%';
ALTER TABLE t2 DROP FOREIGN KEY "mycons1", DROP FOREIGN KEY "mycons2" $$
SELECT * FROM comdb2_constraints WHERE tablename NOT LIKE 'sqlite_stat%';
DROP TABLE t2;
DROP TABLE t1;

CREATE TABLE t1 {
	tag ondisk {
		int i null = yes
		u_int j null = yes
		u_longlong k null = yes
	}
	keys {
		"idx1" = i
		"idx2" = i + j
	}
	tag "tag1" {
		int i
		longlong j
	}
	tag "tag2" {
		u_longlong j
	}
} $$
SELECT csc2 FROM sqlite_master WHERE name LIKE 't1';
ALTER TABLE t1 ADD COLUMN l INT $$
SELECT csc2 FROM sqlite_master WHERE name LIKE 't1';
ALTER TABLE t1 DROP COLUMN j $$
ALTER TABLE t1 DROP COLUMN l $$
SELECT csc2 FROM sqlite_master WHERE name LIKE 't1';
DROP TABLE t1;
