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

SELECT '4. Test for updates' as test;
CREATE TABLE t1(i INT, CHECK (i < 10))$$
INSERT INTO t1 VALUES(1);
UPDATE t1 SET i = 10 WHERE i = 1;
SELECT * FROM t1;
DROP TABLE t1;

SELECT '5. Test for some obscure expressions' as test;
CREATE TABLE t1(i INT, CHECK("i>10"))$$
INSERT INTO t1 VALUES(1);
INSERT INTO t1 VALUES(11);
DROP TABLE t1;

CREATE TABLE t1(i INT, CHECK("i"))$$
INSERT INTO t1 VALUES(1);
INSERT INTO t1 VALUES(11);
DROP TABLE t1;

CREATE TABLE t1(i INT, CHECK('i'))$$
INSERT INTO t1 VALUES(1);
INSERT INTO t1 VALUES(11);
DROP TABLE t1;

CREATE TABLE t1(v VARCHAR(10), CHECK("v"))$$
INSERT INTO t1 VALUES("aaa");
DROP TABLE t1;

CREATE TABLE t1(v VARCHAR(10), CHECK('v'))$$
INSERT INTO t1 VALUES("aaa");
DROP TABLE t1;

SELECT '6. Test for strings' as test;
CREATE TABLE t1(v VARCHAR(10), CHECK(v NOT LIKE 'foo'))$$
INSERT INTO t1 VALUES('foo');
INSERT INTO t1 VALUES('bar');
SELECT * FROM t1;
DROP TABLE t1;

CREATE TABLE t1(v VARCHAR(10), CHECK(v NOT LIKE "foo"))$$
INSERT INTO t1 VALUES('foo');
INSERT INTO t1 VALUES('bar');
SELECT * FROM t1;
DROP TABLE t1;
