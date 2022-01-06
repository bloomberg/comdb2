SELECT '---------------------------------- PART #01 ----------------------------------' AS part;
DROP TABLE IF EXISTS t1;
CREATE TABLE t1(i INT UNIQUE)$$
CREATE TIME PARTITION ON t1 AS t1_tp PERIOD 'yearly' RETENTION 2 START '2018-01-01';

SELECT SLEEP(5);

INSERT INTO t1_tp VALUES(1);
# UNIQUE-constraints are not fully supported on TPTs.
# Also, new-shard names could show up in the dup-error message
#INSERT INTO t1_tp VALUES(1);
INSERT INTO t1_tp VALUES(1) ON CONFLICT DO NOTHING;
INSERT INTO t1_tp VALUES(1) ON CONFLICT(i) DO UPDATE SET i = i+1;
SELECT * FROM t1_tp;

DROP TIME PARTITION t1_tp;
# Each new year will add a new shard here, better leave it commented
#SELECT * FROM comdb2_tables ORDER BY 1;
