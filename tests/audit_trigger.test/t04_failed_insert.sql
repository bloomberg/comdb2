CREATE TABLE t(i int, s char(65))$$

CREATE LUA AUDIT TRIGGER heyo ON (TABLE t FOR INSERT)  

INSERT INTO t values(4)
INSERT INTO t values(5, "abc")

SELECT sleep(1)

SELECT new_i, new_s FROM "$audit_t" ORDER BY new_i

DROP TABLE t
DROP TABLE "$audit_t"
DROP LUA TRIGGER heyo