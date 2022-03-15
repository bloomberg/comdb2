CREATE TABLE t(a INT, b INT, c INT, d INT); $$
CREATE INDEX a ON t(a) INCLUDE (a);

CREATE TABLE t2(a INT, b INT, c INT, d INT); $$
CREATE INDEX a ON t2(a) INCLUDE (a, b, c, d);

select * from comdb2_keys where tablename='t';
select * from comdb2_partial_datacopies where tablename='t';

select * from comdb2_keys where tablename='t2';
select * from comdb2_partial_datacopies where tablename='t2';

drop table t;
drop table t2;
