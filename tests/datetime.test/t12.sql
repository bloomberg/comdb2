create table t12 {
schema
{
    int i
    datetime     dt dbstore="2019-13-30T25:59:59.987 US/Eastern"
}
}$$

insert into t12(i) values(2),(3)
select * from t12
