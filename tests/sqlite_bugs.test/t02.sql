SELECT '---- test#3 ----' as test;

# View/CTE ALTER handling:
# - https://github.com/sqlite/sqlite/commit/38096961c7cd109110ac21d3ed7dad7e0cb0ae06
# - https://github.com/sqlite/sqlite/commit/a6c1a71cde082e09750465d5675699062922e387
CREATE TABLE t1(a int)$$
CREATE VIEW v2(b) AS WITH t3(b) AS (SELECT 1) SELECT b FROM t3$$
ALTER TABLE t1 RENAME TO t4$$
SELECT name FROM sqlite_master WHERE type='table' AND name IN ('t1','t4') ORDER BY name;

DROP VIEW v2;
DROP TABLE t4;
