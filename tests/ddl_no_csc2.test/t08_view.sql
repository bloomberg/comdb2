SELECT 'create table using name of an existing view and vice versa' as test;
CREATE TABLE t1(i INT)$$
CREATE VIEW t1 AS SELECT 1;

CREATE VIEW v1 AS SELECT 1;
CREATE TABLE v1(i INT)$$

SELECT * FROM comdb2_tables;
SELECT * FROM comdb2_views;

DROP TABLE t1;
DROP VIEW v1;

SELECT 'create views using same prefixed names' as test;
CREATE VIEW aa AS SELECT 1;
CREATE VIEW aaa AS SELECT 1;

SELECT * FROM comdb2_tables;
SELECT * FROM comdb2_views;

DROP VIEW a;
DROP VIEW aa;

SELECT * FROM comdb2_tables;
SELECT * FROM comdb2_views;

DROP VIEW aaa;

SELECT * FROM comdb2_tables;
SELECT * FROM comdb2_views;

SELECT 'SC did not work properly with sc_done_same_tran disabled' as test;
# sc_done_same_tran should be enabled by default
SELECT value FROM comdb2_tunables WHERE name = 'sc_done_same_tran';
# disable sc_done_same_tran
PUT TUNABLE sc_done_same_tran 0
CREATE VIEW v1 AS SELECT 1;
SELECT * FROM v1;
DROP VIEW v1;
SELECT * FROM v1;
# re-enable sc_done_same_tran
PUT TUNABLE sc_done_same_tran 1
