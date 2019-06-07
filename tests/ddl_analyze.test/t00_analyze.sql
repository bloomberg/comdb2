SELECT '-- check allowed ANALYZE syntax --' AS test;

TRUNCATE sqlite_stat1;
TRUNCATE sqlite_stat4;

CREATE TABLE t1(i INT) $$
CREATE UNIQUE INDEX idx1 ON t1(i);
INSERT INTO t1 SELECT * FROM generate_series(1,1000);

CREATE TABLE t2(i INT) $$
CREATE UNIQUE INDEX idx1 ON t2(i);
INSERT INTO t2 SELECT * FROM generate_series(1,1000);

SELECT * FROM sqlite_stat1;
SELECT * FROM sqlite_stat4;

# ANALYZE a specific table
ANALYZE t1;
SELECT DISTINCT tbl FROM sqlite_stat1;
SELECT DISTINCT tbl FROM sqlite_stat4;
TRUNCATE sqlite_stat1;
TRUNCATE sqlite_stat4;

# ANALYZE all tables
ANALYZE ALL;
SELECT DISTINCT tbl FROM sqlite_stat1 ORDER BY tbl;
SELECT DISTINCT tbl FROM sqlite_stat4 ORDER BY tbl;
TRUNCATE sqlite_stat1;
TRUNCATE sqlite_stat4;

# Test more ANALYZE options
ANALYZE ALL OPTIONS THREADS 2, SUMMARIZE 2;
SELECT DISTINCT tbl FROM sqlite_stat1 ORDER BY tbl;
SELECT DISTINCT tbl FROM sqlite_stat4 ORDER BY tbl;
TRUNCATE sqlite_stat1;
TRUNCATE sqlite_stat4;

DROP TABLE t1;
DROP TABLE t2;

SELECT '-- ANALYZE table "all" --' AS test;

TRUNCATE sqlite_stat1;
TRUNCATE sqlite_stat4;

CREATE TABLE 'all'(i INT) $$
CREATE UNIQUE INDEX idx1 ON 'all'(i);
INSERT INTO 'all' SELECT * FROM generate_series(1,1000);

CREATE TABLE other_tab(i INT) $$
CREATE UNIQUE INDEX idx1 ON other_tab(i);
INSERT INTO other_tab SELECT * FROM generate_series(1,1000);

SELECT * FROM sqlite_stat1;
SELECT * FROM sqlite_stat4;

# ANALYZE a specific table
ANALYZE 'all'
SELECT DISTINCT tbl FROM sqlite_stat1 ORDER BY tbl;
SELECT DISTINCT tbl FROM sqlite_stat4 ORDER BY tbl;

DROP TABLE 'all';
DROP TABLE other_tab;
