SELECT '---- test#6 ----' as test;

# CREATE TABLE AS smoke path (related to schema handling around CTAS entries):
# - https://github.com/sqlite/sqlite/commit/1e9c47be1e81e94a67f788c98fd70e8bf70e3746
CREATE TABLE ctas1 AS SELECT 1 AS x, 2 AS y$$
SELECT x, y FROM ctas1;
DROP TABLE ctas1;
