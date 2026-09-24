SELECT '---- test#4 ----' as test;

# DISTINCT + window-function crash path:
# - https://github.com/sqlite/sqlite/commit/e59c562b3f6894f84c715772c4b116d7b5c01348
# - https://github.com/sqlite/sqlite/commit/8428b3b437569338a9d1e10c4cd8154acbe33089
# - https://github.com/sqlite/sqlite/commit/0934d640456bb168a8888ae388643c5160afe501
CREATE TABLE w1(aa int, bb int)$$
INSERT INTO w1 VALUES(1,2);
INSERT INTO w1 VALUES(5,6);
CREATE TABLE w2(x int)$$
INSERT INTO w2 VALUES(1);
SELECT (SELECT DISTINCT sum(aa) OVER() FROM w1 ORDER BY 1) AS s, x FROM w2 ORDER BY 1;

DROP TABLE w1;
DROP TABLE w2;
