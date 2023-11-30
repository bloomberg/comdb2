create table t1(a int, b int); $$
create index t1_1 on t1(b);
UPDATE t1 SET a=-1 WHERE (b, 0) IN (SELECT F.b, SUM(ABS(F.a)) OVER() FROM t1 F);
