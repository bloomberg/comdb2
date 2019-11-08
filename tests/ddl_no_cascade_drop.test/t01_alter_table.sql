SELECT * FROM comdb2_tunables WHERE name = 'ddl_cascade_drop';

# https://github.com/bloomberg/comdb2/issues/857
CREATE TABLE t1(i INT PRIMARY KEY) $$
CREATE TABLE t2(i INT PRIMARY KEY, j INT, FOREIGN KEY (i) REFERENCES t1(i)) $$
DROP INDEX 'comdb2_pk' ON t2;
ALTER TABLE t2 DROP COLUMN 'i' $$
ALTER TABLE t2 DROP FOREIGN KEY '$CONSTRAINT_C6A17957', DROP INDEX 'comdb2_pk', DROP COLUMN 'i' $$
SELECT csc2 FROM sqlite_master WHERE name LIKE 't2';
DROP TABLE t1;
DROP TABLE t2;
