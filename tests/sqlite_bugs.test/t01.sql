SELECT '---- test#2 ----' as test;

# Generated-column integrity_check and colUsed regressions:
# - https://github.com/sqlite/sqlite/commit/ebd70eedd5d6e6a890a670b5ee874a5eae86b4dd
# - https://github.com/sqlite/sqlite/commit/522ebfa7cee96fb325a22ea3a2464a63485886a8
# - https://github.com/sqlite/sqlite/commit/926f796e8feec15f3836aa0a060ed906f8ae04d3
CREATE TABLE gc0(c0, c1 NOT NULL AS (c0==0))$$
INSERT INTO gc0(c0) VALUES(0);
PRAGMA integrity_check;

CREATE TABLE gc1(x, y AS(x+1))$$
INSERT INTO gc1 VALUES(10);
SELECT y FROM gc1 JOIN gc1 USING (y,y);
CREATE INDEX gc1y ON gc1(y);
SELECT y FROM gc1 JOIN gc1 USING (y,y);

CREATE TABLE gc3(aa INT PRIMARY KEY, bb UNIQUE AS(aa))$$
INSERT INTO gc3 VALUES(1);
SELECT 100 AS tag, aa, bb FROM gc3;
DELETE FROM gc3 WHERE (SELECT bb FROM gc3);
SELECT 200 AS tag, aa, bb FROM gc3;

DROP TABLE gc0;
DROP TABLE gc1;
DROP TABLE gc3;
