set transaction mode read committed;

select * from comdb2_set_commands;
set transaction mode blocksql;

select * from comdb2_set_commands;
set transaction chunk blah;

select * from comdb2_set_commands;
set transaction chunk 10;
set timeout 200;

select * from comdb2_set_commands;
set maxquerytime 200;
set timezone UTC;
set datetime precision m;
set user 'mohit';

select * from comdb2_set_commands;
set password 'blahblah';
set spversion sp1 1;
set prepare_only on;
set readonly on;
set sptrace on;
set spdebug on;

select * from comdb2_set_commands;
set hasql on;
set cursordebug all;
set verifyretry on;
set queryeffects statement;
set getcost on;
set maxtransize 10;

select * from comdb2_set_commands;
set groupconcatmemlimit 100;
set intransresults on;
set admin on;
set querylimit warn;

set rowbuffer on;


set explain verbose 2;
set plannereffort 9;
set explain on;
