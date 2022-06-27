CREATE TABLE t(a INT, b INT, c INT, d INT); $$
CREATE INDEX a ON t(a) INCLUDE (b, b, c, c, b, d);

drop table t;
