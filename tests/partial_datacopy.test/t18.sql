CREATE TABLE t(a INT, b INT, c INT, d INT); $$
CREATE INDEX a ON t(a) INCLUDE (e);
CREATE INDEX a ON t(a) INCLUDE (b, e, c);
CREATE INDEX a ON t(a) INCLUDE (b, e, c, a, f, g);

drop table t;
