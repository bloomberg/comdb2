CREATE TABLE t(a INT, b INT, c INT, d INT); $$
CREATE INDEX a ON t(a) INCLUDE (b, b, b, b, c, c, c, c, b, c, b, c, b, d);

select * from comdb2_keys where tablename='t';
select * from comdb2_partial_datacopies where tablename='t';

drop table t;
