SELECT '1. Test CHECK CONSTRAINT behavior' as test;
CREATE TABLE t1(color VARCHAR(10), CONSTRAINT valid_colors CHECK (color IN ('red', 'green', 'blue')))$$
INSERT INTO t1 VALUES('red');
INSERT INTO t1 VALUES('greenish');
INSERT INTO t1 VALUES('black');
SELECT * FROM t1;
DROP TABLE t1;

SELECT '2. ALTER TABLE' as test;
CREATE TABLE t1(color VARCHAR(10))$$
SELECT csc2 FROM sqlite_master WHERE name = 't1';
INSERT INTO t1 VALUES('blue');
INSERT INTO t1 VALUES('black');
# This ALTER must fail because 'black' is not a valid color.
ALTER TABLE t1 ADD CONSTRAINT valid_colors CHECK (color IN ('red', 'green', 'blue'))$$
SELECT csc2 FROM sqlite_master WHERE name = 't1';

DELETE FROM t1 WHERE color = 'black';
# This ALTER should now succeed.
ALTER TABLE t1 ADD CONSTRAINT valid_colors CHECK (color IN ('red', 'green', 'blue'))$$
SELECT csc2 FROM sqlite_master WHERE name = 't1';
SELECT * FROM t1;

INSERT INTO t1 VALUES('white');
ALTER TABLE t1 DROP CONSTRAINT valid_colors$$
SELECT csc2 FROM sqlite_master WHERE name = 't1';
INSERT INTO t1 VALUES('white');
SELECT * FROM t1;
DROP TABLE t1;

SELECT '3. Invalid use cases' as test;
CREATE TABLE t1(i INT, CHECK ())$$
CREATE TABLE t1(i INT, CHECK (SELECT 1))$$
CREATE TABLE t1(i INT, CHECK (i > MAX(i)))$$
CREATE TABLE t1(i INT, CHECK (i > (SELECT MAX(i))))$$
