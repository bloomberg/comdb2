CREATE TABLE t(a INT, b INT, c INT, d INT); $$
CREATE INDEX a ON t(a) INCLUDE ALL (b, c);
CREATE INDEX a ON t(a) INCLUDE (b, c) ALL;
CREATE INDEX a ON t(a) INCLUDE (b, c) (d);

drop table t;
