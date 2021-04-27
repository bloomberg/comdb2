drop table if exists t12
create table t12 (i integer)$$
create index "" on t12(i)
create index "s p a c e" on t12(i)
create index "a_loooooooooooooooooooooooooooooooooooooooooooooooooong_name" on t12(i)
