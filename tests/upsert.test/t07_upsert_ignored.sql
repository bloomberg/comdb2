SELECT '---- ignored upsert skips constraints ----' AS test;
DROP TABLE IF EXISTS c;
DROP TABLE IF EXISTS p;
CREATE TABLE p(i INT UNIQUE)$$
CREATE TABLE c(i INT UNIQUE, j INT, FOREIGN KEY(j) REFERENCES p(i))$$
INSERT INTO p VALUES(1);
INSERT INTO c VALUES(1, 1);

SELECT '-- 1. conflict on all indexes, child points at missing parent --' AS test;
INSERT INTO c VALUES(1, 99) ON CONFLICT DO NOTHING;
SELECT * FROM c ORDER BY i;

SELECT '-- 2. conflict on target index, child points at missing parent --' AS test;
INSERT INTO c VALUES(1, 99) ON CONFLICT(i) DO NOTHING;
SELECT * FROM c ORDER BY i;

SELECT '-- 3. multiple ignored upserts in one txn --' AS test;
BEGIN;
INSERT INTO c VALUES(1, 99) ON CONFLICT DO NOTHING;
INSERT INTO c VALUES(1, 98) ON CONFLICT(i) DO NOTHING;
INSERT INTO c VALUES(1, 1) ON CONFLICT DO NOTHING;
COMMIT;
SELECT * FROM c ORDER BY i;

SELECT '-- 4. ignored upsert + fk-violating insert must fail --' AS test;
BEGIN;
INSERT INTO c VALUES(1, 99) ON CONFLICT DO NOTHING;
INSERT INTO c VALUES(2, 99);
COMMIT;
SELECT * FROM c ORDER BY i;

SELECT '-- 5. fk-violating insert + ignored upsert must fail --' AS test;
BEGIN;
INSERT INTO c VALUES(2, 99);
INSERT INTO c VALUES(1, 99) ON CONFLICT DO NOTHING;
COMMIT;
SELECT * FROM c ORDER BY i;

SELECT '-- 6. ignored upsert + valid insert succeeds --' AS test;
BEGIN;
INSERT INTO c VALUES(1, 99) ON CONFLICT DO NOTHING;
INSERT INTO c VALUES(2, 1);
COMMIT;
SELECT * FROM c ORDER BY i;

SELECT '-- 7. ignored upsert + delete of referenced parent must fail --' AS test;
BEGIN;
INSERT INTO c VALUES(1, 99) ON CONFLICT DO NOTHING;
DELETE FROM p WHERE i = 1;
COMMIT;
SELECT * FROM p ORDER BY i;

SELECT '-- 8. ignored upsert + dup inserts in same txn must fail --' AS test;
BEGIN;
INSERT INTO c VALUES(1, 99) ON CONFLICT DO NOTHING;
INSERT INTO c VALUES(3, 1);
INSERT INTO c VALUES(3, 1);
COMMIT;
SELECT * FROM c ORDER BY i;

SELECT '-- 9. ignored upsert + update and delete succeed --' AS test;
BEGIN;
INSERT INTO c VALUES(1, 99) ON CONFLICT DO NOTHING;
UPDATE c SET i = 3 WHERE i = 2;
DELETE FROM c WHERE i = 1;
COMMIT;
SELECT * FROM c ORDER BY i;

SELECT '-- 10. ignored upsert on target index, conflict on other index must fail --' AS test;
CREATE UNIQUE INDEX c_j ON c(j);
INSERT INTO c VALUES(4, 1) ON CONFLICT(i) DO NOTHING;
SELECT * FROM c ORDER BY i;

SELECT '-- 11. ignored upsert + upsert that inserts --' AS test;
INSERT INTO p VALUES(2);
BEGIN;
INSERT INTO c VALUES(3, 99) ON CONFLICT DO NOTHING;
INSERT INTO c VALUES(5, 2) ON CONFLICT DO NOTHING;
COMMIT;
SELECT * FROM c ORDER BY i;

SELECT '-- 12. ignored upsert + upsert that inserts with missing parent must fail --' AS test;
BEGIN;
INSERT INTO c VALUES(3, 99) ON CONFLICT DO NOTHING;
INSERT INTO c VALUES(6, 99) ON CONFLICT DO NOTHING;
COMMIT;
SELECT * FROM c ORDER BY i;

DROP TABLE c;
DROP TABLE p;
