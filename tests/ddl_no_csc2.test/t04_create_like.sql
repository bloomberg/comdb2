CREATE TABLE t1(i INT, j INT, key idx1 (i, j), key idx2 (i DESC, j ASC), key idx3 (i ASC, j DESC), key idx4 (i DESC, j DESC))$$
CREATE TABLE t2(i INT PRIMARY KEY)$$
CREATE TABLE t3(i INT REFERENCES t1(i DESC)) $$
CREATE TABLE t4(i INT, j INT, FOREIGN KEY (i, j) REFERENCES t1(i, j)) $$
CREATE TABLE t5(i INT UNIQUE, j INT, KEY(i,j), UNIQUE(i,j), KEY dup_key(i,j), UNIQUE 'unique_key'(j,i))$$
CREATE TABLE t1_copy like t1 $$
CREATE TABLE t2_copy like t2 $$
CREATE TABLE t3_copy like t3 $$
CREATE TABLE t4_copy like t4 $$
CREATE TABLE t5_copy like t5 $$
# t6 does not exist
CREATE TABLE t6_copy like t6 $$
SELECT name, COUNT(*) FROM sqlite_master WHERE type = 'table' GROUP BY lower(csc2) HAVING COUNT(*) > 1 ORDER BY name;
DROP TABLE t2;
DROP TABLE t3;
DROP TABLE t4;
DROP TABLE t5;
DROP TABLE t1_copy;
DROP TABLE t2_copy;
DROP TABLE t3_copy;
DROP TABLE t4_copy;
DROP TABLE t5_copy;
DROP TABLE t1;
