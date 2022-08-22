SELECT '---------------------------------- PART #00 ----------------------------------' AS part;
CREATE TABLE t2(c CSTRING(37))$$
INSERT INTO t2 VALUES('77de9161-f2f7-4b93-b8ae-75a20c3be2d0');
SELECT COUNT(*) FROM t2 WHERE c=guid_str(guid('77de9161-f2f7-4b93-b8ae-75a20c3be2d0'));
INSERT INTO t2 VALUES(guid_str());
INSERT INTO t2 VALUES(guid_str(guid()));
SELECT LENGTH(c) FROM t2;
DROP TABLE t2;

SELECT '---------------------------------- PART #01 ----------------------------------' AS part;
CREATE TABLE t2(c TEXT)$$
INSERT INTO t2 VALUES('77de9161-f2f7-4b93-b8ae-75a20c3be2d0');
SELECT COUNT(*) FROM t2 WHERE c=guid_str(guid('77de9161-f2f7-4b93-b8ae-75a20c3be2d0'));
INSERT INTO t2 VALUES(guid_str());
INSERT INTO t2 VALUES(guid_str(guid()));
SELECT LENGTH(c) FROM t2;
DROP TABLE t2;

SELECT '---------------------------------- PART #02 ----------------------------------' AS part;
CREATE TABLE t2(c BLOB(24))$$
INSERT INTO t2 VALUES(guid());
INSERT INTO t2 VALUES(guid('77de9161-f2f7-4b93-b8ae-75a20c3be2d0'));
#INSERT INTO t2 VALUES(CAST (guid_str() as BLOB));
SELECT LENGTH(c) FROM t2;
DROP TABLE t2;

