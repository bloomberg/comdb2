CREATE TABLE t(i int)$$
CREATE LUA AUDIT TRIGGER bloop ON (TABLE t FOR INSERT)
INSERT INTO t VALUES(4)
ALTER TABLE t ADD COLUMN b int$$
SELECT * FROM t
SELECT new_i, old_i, new_b, old_b FROM "$audit_t" ORDER BY new_i, old_i
ALTER TABLE t { schema { int i null=yes int b null=yes int c null=yes } }$$
SELECT new_i, old_i, new_b, old_b, new_c, old_c FROM "$audit_t" ORDER BY new_i, old_i
INSERT INTO t VALUES(1,2,3)

SELECT sleep(1)

SELECT new_i, old_i, new_b, old_b, new_c, old_c FROM "$audit_t"

DROP TABLE t 
DROP TABLE "$audit_t"
DROP LUA TRIGGER bloop