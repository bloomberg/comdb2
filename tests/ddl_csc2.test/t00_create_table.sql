DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;

# Negative array size not allowed
CREATE TABLE t1 { schema { cstring c[-1] } } $$
CREATE TABLE t1 { schema { blob c[-1] } } $$
CREATE TABLE t1 { schema { byte c[-1] } } $$
CREATE TABLE t1 { schema { vutf8 c[-1] } } $$
CREATE TABLE t1 { schema { cstring c[-100] } } $$
CREATE TABLE t1 { schema { blob c[-100] } } $$
CREATE TABLE t1 { schema { byte c[-100] } } $$
CREATE TABLE t1 { schema { vutf8 c[-100] } } $$
CREATE TABLE t1 { constants { SIZE = -1 } schema { cstring c[SIZE] } } $$
CREATE TABLE t1 { constants { SIZE = -100 } schema { cstring c[SIZE] } } $$
# Special cases
CREATE TABLE t1 { schema { cstring c[1] } } $$
CREATE TABLE t1 { schema { byte c[0] } } $$

# Check for zero array size (allowed)
CREATE TABLE t1 { schema { blob c[0] } } $$
CREATE TABLE t2 { schema { vutf8 c[0] } } $$
CREATE TABLE t3 { constants { SIZE = 0 } schema { vutf8 c[SIZE] } } $$

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
