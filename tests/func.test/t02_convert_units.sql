select '== test for convert_units ==' as t;
select convert_units('hi', 2, 3)
select convert_units(1, 2, 3)
select convert_units(1, 'hi', 3)
select convert_units(1, 'hi', 'hi')
select convert_units(1, null, 'bytes')
select convert_units(1000, null, 'bytes') as bytes
select convert_units(2000, null, 'bytes') as bytes
select convert_units(10000, null, 'bytes') as bytes
select convert_units(10*1024*1024, null, 'bytes') as bytes
select convert_units(cast(10*1024*1024*1024 as real), null, 'bytes') as bytes
select convert_units(cast(10*1024*1024*1024*1024 as real), null, 'bytes') as bytes
select convert_units(cast(10*1024*1024*1024*1024*1024 as real), null, 'bytes') as bytes
