SELECT '---- test#5 ----' as test;

# DISTINCT + LEFT JOIN flattening guard:
# - https://github.com/sqlite/sqlite/commit/396afe6f6aa90a31303c183e11b2b2d4b7956b35
CREATE TABLE j0(a int, b int)$$
CREATE INDEX j0a ON j0(a);
INSERT INTO j0 VALUES(10,10);
INSERT INTO j0 VALUES(10,11);
INSERT INTO j0 VALUES(10,12);
SELECT DISTINCT c FROM j0 LEFT JOIN (SELECT a+1 AS c FROM j0) ORDER BY c;

DROP TABLE j0;
