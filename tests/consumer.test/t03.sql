DROP TABLE IF EXISTS t
CREATE TABLE t (i INTEGER, d double, c char(32), v varchar(64), dt datetime, dtus datetimeus, b0 blob, b1 blob(256))$$
CREATE DEFAULT LUA CONSUMER watcher ON (TABLE t FOR INSERT AND UPDATE AND DELETE)
INSERT INTO t VALUES(1, 2, 3, 4, 5, 6, x'600d', x'f00d')
UPDATE t SET i = i + 1, d = d + 1, c = c || '3', v = v || '4', dt = dt + CAST(1 AS DAY), dtus = dtus + CAST(5.000123 AS DAY), b0 = x'baad', b1 = x'cafe'
DELETE FROM t
EXEC PROCEDURE watcher('{"with_id":false, "consume_count":3}')
