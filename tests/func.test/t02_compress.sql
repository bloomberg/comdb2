-- Tests for compress()/uncompress()
select cast(uncompress(compress('hello world!')) as text);
select cast(uncompress(compress('hello world!')) as text) = 'hello world!';
select length(cast(uncompress(compress('hello world!')) as text)) = length('hello world!');

select uncompress(compress(zeroblob(1000000))) = zeroblob(1000000);
select length(compress(zeroblob(1000000)));

select compress(NULL);
select uncompress(NULL);
select uncompress(compress(NULL));

select compress(10);
select compress(10.5);
select uncompress('foo');
select uncompress(x'cdb2');

-- Tests for compress_zlib()/uncompress_zlib()
select cast(uncompress_zlib(compress_zlib('hello world!')) as text);
select cast(uncompress_zlib(compress_zlib('hello world!')) as text) = 'hello world!';
select length(cast(uncompress_zlib(compress_zlib('hello world!')) as text)) = length('hello world!');

select uncompress_zlib(compress_zlib(zeroblob(1000000))) = zeroblob(1000000);
select length(compress_zlib(zeroblob(1000000)));

select compress_zlib(NULL);
select uncompress_zlib(NULL);
select uncompress_zlib(compress_zlib(NULL));

select compress_zlib(10);
select compress_zlib(10.5);
select uncompress_zlib('foo');
select uncompress_zlib(x'cdb2');
