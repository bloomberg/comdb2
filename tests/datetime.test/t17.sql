set timezone UTC
with t(a) as (values (cast('2021-03-11T13:00:00 America/New_York' as datetime)), (cast('2021-03-11T13:00:00 Europe/London' as datetime)), (cast('2021-03-11T13:00:00 Asia/Tokyo' as datetime)), ('1')) select cast(a as datetime) from t order by a -- Mike's query to reproduce wrong sort order
with t(a) as (values (cast('2021-03-11T13:00:00 America/New_York' as datetime)), (cast('2021-03-11T13:00:00 Europe/London' as datetime)), (cast('2021-03-11T13:00:00 Asia/Tokyo' as datetime)), ('qqq')) select cast(a as datetime) from t order by a
