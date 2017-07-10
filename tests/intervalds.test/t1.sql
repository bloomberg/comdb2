drop table if exists t1
create table t1 {
    tag ondisk {
        intervalds intds
        intervaldsus intdsus
    }
}$$
insert into t1 values (cast(1 as days), cast(1 as days))
insert into t1 values (cast(1 as hours), cast(1 as hours))
insert into t1 values (cast(1 as mins), cast(1 as mins))
insert into t1 values (cast(1 as secs), cast(1 as secs))

select 0 * intds, intds * 0, 0 * intdsus, intdsus * 0 from t1 order by intdsus
select 1 * intds, intds * 1, 1 * intdsus, intdsus * 1 from t1 order by intdsus
select intds / 1, intdsus / 1 from t1 order by intdsus
select 2 * intds, intds * 2, 2 * intdsus, intdsus * 2 from t1 order by intdsus
select intds / 2, intdsus / 2 from t1 order by intdsus
select 1000 * intds, intds * 1000, 1000 * intdsus, intdsus * 1000 from t1 order by intdsus
select intds / 1000, intdsus / 1000 from t1 order by intdsus
select 1000000 * intds, intds * 1000000, 1000000 * intdsus, intdsus * 1000000 from t1 order by intdsus
select 1000 * intds * 1000, intds * 1000 * 1000, 1000 * intdsus * 1000, intdsus * 1000 * 1000 from t1 order by intdsus
select intds / 1000000, intdsus / 1000000 from t1 order by intdsus
select intds / 1000 / 1000, intdsus / 1000 / 1000 from t1 order by intdsus

select 0 + intds, intds + 0, 0 + intdsus, intdsus + 0 from t1 order by intdsus
select 1 + intds, intds + 1, 1 + intdsus, intdsus + 1 from t1 order by intdsus
select intds + intdsus * 0, intds + intds * 0, intdsus + intdsus * 0 from t1 order by intdsus
select intds + intdsus * 1, intds + intds * 1, intdsus + intdsus * 1 from t1 order by intdsus
select intds + intdsus / 1, intds + intds / 1, intdsus + intdsus / 1 from t1 order by intdsus
select intds + intdsus * 2, intds + intds * 2, intdsus + intdsus * 2 from t1 order by intdsus
select intds + intdsus / 2, intds + intds / 2, intdsus + intdsus / 2 from t1 order by intdsus
select intds + intdsus * 1000, intds + intds * 1000, intdsus + intdsus * 1000 from t1 order by intdsus
select intds + intdsus / 1000, intds + intds / 1000, intdsus + intdsus / 1000 from t1 order by intdsus
select intds + intdsus * 1000000, intds + intds * 1000000, intdsus + intdsus * 1000000 from t1 order by intdsus
select intds + intdsus / 1000000, intds + intds / 1000000, intdsus + intdsus / 1000000 from t1 order by intdsus

select intds - 0, intdsus - 0 from t1 order by intdsus
select intds - intdsus * 0, intds - intds * 0, intdsus - intdsus * 0 from t1 order by intdsus
select intdsus * 1 - intds, intds * 1 - intds, intdsus * 1 - intdsus from t1 order by intdsus
select intds - intdsus / 1, intds - intds / 1, intdsus - intdsus / 1 from t1 order by intdsus
select intdsus * 2 - intds, intds * 2 - intds, intdsus * 2 - intdsus from t1 order by intdsus
select intds - intdsus / 2, intds - intds / 2, intdsus - intdsus / 2 from t1 order by intdsus
select intdsus * 1000 - intds, intds * 1000 - intds, intdsus * 1000 - intdsus from t1 order by intdsus
select intds - intdsus / 1000, intds - intds / 1000, intdsus - intdsus / 1000 from t1 order by intdsus
select intdsus * 1000000 - intds, intds * 1000000 - intds, intdsus * 1000000 - intdsus from t1 order by intdsus
select intds - intdsus / 1000000, intds - intds / 1000000, intdsus - intdsus / 1000000 from t1 order by intdsus
