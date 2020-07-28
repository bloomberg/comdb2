create table t15 {
schema
{
    int i
    datetime     dt dbstore="2019-13-30T25:59:59.987 UTC"
}
}$$

insert into t15(i) values(2),(3)
select * from t15
