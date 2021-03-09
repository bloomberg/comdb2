SET TIMEZONE UTC
CREATE TABLE lhs (i INTEGER)$$
CREATE TABLE rhs (i INTEGER, dt DATETIME)$$
INSERT INTO lhs VALUES (1), (2), (3)
INSERT INTO rhs VALUES (2, '2021-03-05')
SELECT CASE WHEN dt IS NULL THEN '1970-01-01' ELSE dt END AS dt FROM lhs LEFT JOIN rhs ON lhs.i = rhs.i ORDER BY dt DESC
SELECT CASE WHEN dt IS NULL THEN '1970-01-01' ELSE dt END AS dt FROM lhs LEFT JOIN rhs ON lhs.i = rhs.i ORDER BY dt DESC
SELECT CASE WHEN dt IS NULL THEN '1970-01-01' ELSE dt END AS dt FROM lhs LEFT JOIN rhs ON lhs.i = rhs.i ORDER BY dt DESC
SELECT CASE WHEN dt IS NULL THEN '1970-01-01' ELSE dt END AS dt FROM lhs LEFT JOIN rhs ON lhs.i = rhs.i ORDER BY dt DESC
SELECT CASE WHEN dt IS NULL THEN '1970-01-01' ELSE dt END AS dt FROM lhs LEFT JOIN rhs ON lhs.i = rhs.i ORDER BY dt DESC
SELECT CASE WHEN dt IS NULL THEN '1970-01-01' ELSE dt END AS dt FROM lhs LEFT JOIN rhs ON lhs.i = rhs.i ORDER BY dt DESC
SELECT CASE WHEN dt IS NULL THEN '1970-01-01' ELSE dt END AS dt FROM lhs LEFT JOIN rhs ON lhs.i = rhs.i ORDER BY dt DESC
SELECT CASE WHEN dt IS NULL THEN '1970-01-01' ELSE dt END AS dt FROM lhs LEFT JOIN rhs ON lhs.i = rhs.i ORDER BY dt DESC
SELECT CASE WHEN dt IS NULL THEN '1970-01-01' ELSE dt END AS dt FROM lhs LEFT JOIN rhs ON lhs.i = rhs.i ORDER BY dt DESC
SELECT CASE WHEN dt IS NULL THEN '1970-01-01' ELSE dt END AS dt FROM lhs LEFT JOIN rhs ON lhs.i = rhs.i ORDER BY dt DESC
SELECT dt AS equal_plus FROM rhs WHERE dt = "+" -- incompatible string
SELECT dt AS notequal_plus FROM rhs WHERE dt != "+"
SELECT dt AS greaterthan_plus FROM rhs WHERE dt > "+"
SELECT dt AS lessthan_plus FROM rhs WHERE dt < "+"
SELECT dt AS equal_1trillion FROM rhs WHERE dt = 1000000000000 -- overflow integer
SELECT dt AS notequal_1trillion FROM rhs WHERE dt != 1000000000000
SELECT dt AS greaterthan_1trillion FROM rhs WHERE dt > 1000000000000
SELECT dt AS lessthan_1trillion FROM rhs WHERE dt < 1000000000000
SELECT dt AS equal_negative1trillionpoint123 FROM rhs WHERE dt = -1000000000000.123 -- underflow real
SELECT dt AS notequal_negative1trillionpoint123 FROM rhs WHERE dt != -1000000000000.123
SELECT dt AS greaterthan_negative1trillionpoint123 FROM rhs WHERE dt > -1000000000000.123
SELECT dt AS lessthan_negative1trillionpoint123 FROM rhs WHERE dt < -1000000000000.123
