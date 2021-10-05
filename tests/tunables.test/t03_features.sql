put tunable check_constraint_feature 'off';
create table t1(color varchar(10), constraint valid_colors check (color IN ('red', 'green', 'blue')))$$
put tunable check_constraint_feature 'on';
create table t1(color varchar(10), constraint valid_colors check (color IN ('red', 'green', 'blue')))$$

put tunable default_function_feature 'off';
create table t2 { schema { byte id[16] dbstore={hex(guid())} } }$$
put tunable default_function_feature 'on';
create table t2 { schema { byte id[16] dbstore={hex(guid())} } }$$

put tunable on_del_set_null_feature 'off';
create table p { schema{ int i } keys { "pk" = i } } $$
create table c { schema{ int i null=yes } keys { dup "pk" = i } constraints { "pk" -> <"p":"pk"> on delete set null on update cascade } }$$
put tunable on_del_set_null_feature 'on';
create table c { schema{ int i null=yes } keys { dup "pk" = i } constraints { "pk" -> <"p":"pk"> on delete set null on update cascade } }$$

put tunable sequence_feature 'off';
create table t3 { schema { longlong i null=yes dbstore=nextsequence } }$$
put tunable sequence_feature 'on';
create table t3 { schema { longlong i null=yes dbstore=nextsequence } }$$

put tunable view_feature 'off';
create view v1 as select 1;
put tunable view_feature 'on';
create view v1 as select 1;

drop table t1;
drop table t2;
drop table c;
drop table p;
drop view v1;
drop table t3;
