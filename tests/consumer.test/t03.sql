DROP TABLE IF EXISTS t
DROP TABLE IF EXISTS u
CREATE TABLE t (i INTEGER, d DOUBLE, c CHAR(32), v VARCHAR(64), dt DATETIME, dtus DATETIMEUS, b0 BLOB, b1 BLOB(256), ym INTERVALYM, ds INTERVALDS, dsus INTERVALDSUS)$$
CREATE TABLE u (foobar int)$$
CREATE DEFAULT LUA CONSUMER watcher ON (TABLE t FOR INSERT AND UPDATE AND DELETE), (TABLE u FOR INSERT)
CREATE DEFAULT LUA CONSUMER watcher ON (TABLE t FOR INSERT AND UPDATE AND DELETE)

INSERT INTO t VALUES(1, 2, 3, 4, 5, 6, x'600d', x'f00d', 10 - 5, 100 - 50, 1000.123 - 500.456)
SELECT * FROM t

UPDATE t SET i = i + 1, d = d + 1, c = c || '3', v = v || '4', dt = dt + CAST(1 AS DAY), dtus = dtus + CAST(5.000123 AS DAY), b0 = x'baad', b1 = x'cafe', ym = ym + 1, ds = ds + 2, dsus = dsus + 3.3
SELECT * FROM t

DELETE FROM t
SELECT * FROM t

-- consume and stop after 3 events (ins, upd, del)
EXEC PROCEDURE watcher('{"with_id":false, "consume_count":3}')

-- legacy default version
CREATE PROCEDURE foo {}$$
SELECT name, version, "default" FROM comdb2_procedures WHERE name='foo'
-- should overwrite legacy default version
CREATE DEFAULT LUA CONSUMER foo ON (TABLE t FOR INSERT)
SELECT name, version, "default" FROM comdb2_procedures WHERE name='foo'

-- client specified version
CREATE PROCEDURE bar VERSION 'baz' {}$$
SELECT name, version, "default" FROM comdb2_procedures WHERE name='bar'
-- should overwrite default version
CREATE DEFAULT LUA CONSUMER bar ON (TABLE t FOR INSERT)
SELECT name, version, "default" FROM comdb2_procedures WHERE name='bar'

DROP TABLE t
DROP TABLE u
DROP LUA CONSUMER watcher
DROP LUA CONSUMER foo
DROP LUA CONSUMER bar
