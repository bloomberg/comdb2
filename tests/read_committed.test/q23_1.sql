DROP TABLE IF EXISTS recom;
CREATE TABLE recom(a int primary key, b int)$$
SET TRANSACTION READ COMMITTED;
BEGIN;
INSERT INTO recom values (1,2),(2,3),(3,4),(4,5),(5,6),(6,7),(7,8);
UPDATE recom SET b=99 where b%2=0;
DELETE FROM recom where b=99;
SELECT count(*) FROM recom where b=99;
COMMIT;
