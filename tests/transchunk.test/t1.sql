-- DRQS-172914411

drop table if exists t1;

create table t1 {
    schema {
        int c1
        int c2
    }
    keys {
        "key" = c1 + c2 {where c1 = 1}
    }
}$$

set transaction blocksql
set transaction chunk 1
begin;
delete from t1;
insert into t1 values (1, 1);
insert into t1 values (1, 1);
commit;

-- Since the above transaction ran in chunk-mode, only the first INSERT
-- should have succeeded.
select count(*) = 1 from t1;

exec procedure sys.cmd.verify('t1')

delete from t1;

# Cleanup
drop table t1

# Additionally, we also test that chunked-deletes work fine with
# partial-index.

create table t1 {
    schema {
        int c1
        int c2
    }
    keys {
        "key" = c1 + c2 {where c1 = 1}
    }
}$$

insert into t1 select 1, value from generate_series(1, 10000);

set transaction chunk 100
begin;
delete from t1;
commit;

select count(*) = 0 from t1;

exec procedure sys.cmd.verify('t1')

delete from t1;

# Cleanup
drop table t1
