select cast(uncompress_gzip(compress_gzip('hello world!')) as text);
select cast(uncompress_gzip(compress_gzip('hello world!')) as text) = 'hello world!';
select length(cast(uncompress_gzip(compress_gzip('hello world!')) as text)) = length('hello world!');

select uncompress_gzip(compress_gzip(zeroblob(1000000))) = zeroblob(1000000);
select length(compress_gzip(zeroblob(1000000)));

select compress_gzip(NULL);
select uncompress_gzip(NULL);
select uncompress_gzip(compress_gzip(NULL));

select compress_gzip(10);
select compress_gzip(10.5);
select uncompress_gzip('foo');
select uncompress_gzip(x'cdb2');
