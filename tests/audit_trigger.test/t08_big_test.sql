CREATE TABLE t1(i int, c char(5), b int)$$
CREATE TABLE t2(a int, bloop char(100))$$

INSERT INTO t1 VALUES(4, "abc", 1232)
CREATE LUA AUDIT TRIGGER hello ON (TABLE t1 FOR UPDATE AND DELETE)
CREATE LUA AUDIT TRIGGER world ON (TABLE t2 FOR UPDATE AND INSERT)

INSERT INTO t1 VALUES(10, "fsad", -5)
DELETE FROM t1 WHERE b=1232

INSERT INTO t2 VALUES(1, "a duck walked up to a lemonade stand")

SELECT sleep(1)
SELECT "-------------first selection------------"
SELECT old_i, new_i, old_c, new_c, old_b, new_b FROM "$audit_t1" ORDER BY old_i, new_i
SELECT old_a, new_a, old_bloop, new_bloop FROM "$audit_t2" ORDER BY old_a, new_a

CREATE LUA AUDIT TRIGGER world ON (TABLE t1 FOR INSERT AND UPDATE AND DELETE)
CREATE LUA AUDIT TRIGGER world2 ON (TABLE t1 FOR INSERT AND UPDATE AND DELETE)

INSERT INTO t1 VALUES(0, "doop", 4)
DROP TABLE "$audit_t1"
DROP LUA TRIGGER hello

CREATE LUA AUDIT TRIGGER hello2 ON (TABLE t1 FOR INSERT AND UPDATE AND DELETE)
DELETE FROM t1 WHERE c="doop"
INSERT INTO t1 VALUES(100, "doop3", 4)

SELECT sleep(1)
SELECT "-------------second selection------------"
SELECT * FROM comdb2_tables
SELECT * FROM comdb2_triggers ORDER BY tbl_name, event, col
SELECT old_i, new_i, old_c, new_c, old_b, new_b FROM "$audit_t1" ORDER BY old_i, new_i
SELECT old_i, new_i, old_c, new_c, old_b, new_b FROM "$audit_t1$2" ORDER BY old_i, new_i

DROP LUA TRIGGER world
DROP LUA TRIGGER world2
DROP LUA TRIGGER hello2 
DROP TABLE t1 
DROP TABLE t2 
DROP TABLE "$audit_t1"
DROP TABLE "$audit_t1$2"
DROP TABLE "$audit_t2"