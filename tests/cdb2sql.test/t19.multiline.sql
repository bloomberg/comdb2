-- test that line is skipped after a syntax error is encountered
select 'see me'; oompa loompa; select 'dont see me';
select 'see me, too';
