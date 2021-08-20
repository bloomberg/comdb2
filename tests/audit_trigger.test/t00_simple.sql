CREATE TABLE t(num int)$$

CREATE LUA AUDIT TRIGGER t_audit ON (TABLE t FOR INSERT)

SELECT "------table and procedure creation------"
SELECT * FROM comdb2_tables ORDER BY tablename

INSERT INTO t VALUES(5)
INSERT INTO t VALUES(10)
DELETE FROM t WHERE num=10

SELECT sleep(1)

SELECT "------table filling------"
SELECT type, tbl, old_num, new_num from "$audit_t" ORDER BY new_num

DROP TABLE t
DROP LUA TRIGGER t_audit
DROP TABLE "$audit_t"