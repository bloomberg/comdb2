CREATE TABLE t4(a INT)$$
INSERT INTO t4 VALUES(1);
CREATE VIEW v4 AS SELECT a FROM t4;
SELECT a FROM v4;
SELECT name, rootpage, tbl_name FROM sqlite_master WHERE type = 'view';
DROP VIEW v4;
DROP TABLE t4;
