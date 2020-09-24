SELECT * FROM comdb2_tunables WHERE name = 'ddl_cascade_drop';

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
CREATE TABLE t2(i INT PRIMARY KEY, j INT, FOREIGN KEY (i) REFERENCES t1(i)) $$
CREATE TABLE t1(i INT PRIMARY KEY) $$
CREATE TABLE t2(i INT PRIMARY KEY, j INT, FOREIGN KEY (i) REFERENCES t1(i)) $$

SELECT csc2 FROM sqlite_master WHERE name LIKE 't2';
# this is how one can find the name of a constraint
SELECT name FROM comdb2_constraints WHERE tablename='t2' AND keyname='COMDB2_PK'

DROP TABLE t1;
ALTER TABLE t1 DROP COLUMN 'i' $$

# drop column will succeed and drop the key, and foreign key too
ALTER TABLE t2 DROP COLUMN 'i' $$
SELECT csc2 FROM sqlite_master WHERE name LIKE 't2';
DROP TABLE t1;
DROP TABLE t2;
