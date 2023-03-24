SELECT "== UPSERT isn't supported, expect meaningful error ==" as test;
CREATE TABLE t1 options blobfield none, rec none { schema { int i } keys { "idx1" = i } }$$
INSERT INTO t1 VALUES(1);
INSERT INTO t1 VALUES(1);
INSERT INTO t1 VALUES(1) ON CONFLICT DO NOTHING;
INSERT INTO t1 VALUES(1) ON CONFLICT(i) DO UPDATE SET i = i+1;
INSERT OR IGNORE INTO t1 VALUES(1);
INSERT OR REPLACE INTO t1 VALUES(1);
REPLACE INTO t1 VALUES(1);
DROP TABLE t1;
