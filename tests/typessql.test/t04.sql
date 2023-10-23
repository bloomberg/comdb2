INSERT INTO t2 VALUES (1, 1.0, 'hi', 'hello', x'deadbeef', x'deadbeef', '20230913T', '2023-09-13T00:00:00.000001');
SELECT NULL AS i, NULL as r, NULL as s, NULL as v, NULL as byt, NULL as b, NULL as d, NULL as d2 UNION ALL SELECT * FROM t2 ORDER BY i; -- test returning all types from queue
SELECT NULL AS interval UNION ALL SELECT d2 - d FROM t2;
set timezone Asia/Tokyo;
SELECT NULL AS i, NULL as r, NULL as s, NULL as v, NULL as byt, NULL as b, NULL as d, NULL as d2 UNION ALL SELECT * FROM t2 ORDER BY i; -- make sure timezone is correct
