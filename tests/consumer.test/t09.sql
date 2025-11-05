drop table if exists t
create table t(i int)$$

set transaction read committed
create default lua consumer recom_test with sequence on (table t for insert)
select version from comdb2_procedures where name='recom_test'

set transaction snapshot
create default lua consumer snap_test with sequence on (table t for insert)
select version from comdb2_procedures where name='snap_test'

set transaction serializable
create default lua consumer serial_test with sequence on (table t for insert)
select version from comdb2_procedures where name='serial_test'
