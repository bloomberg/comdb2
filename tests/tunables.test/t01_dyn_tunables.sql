SELECT name, read_only FROM comdb2_tunables WHERE name = 'dir';
PUT TUNABLE dir '/tmp';

PUT TUNABLE nonexistent 100;

SELECT name, value, read_only FROM comdb2_tunables WHERE name = 'latch_max_poll';

PUT TUNABLE latch_max_poll 100;
SELECT value FROM comdb2_tunables WHERE name = 'latch_max_poll';

PUT TUNABLE latch_max_poll 'xx';
PUT TUNABLE latch_max_poll xx;
SELECT value FROM comdb2_tunables WHERE name = 'latch_max_poll';

PUT TUNABLE latch_max_poll '10';
SELECT value FROM comdb2_tunables WHERE name = 'latch_max_poll';

# Test dynamic tunable using 'exec procedure' & 'put tunable'.
SELECT value AS 'debug_rowlocks' FROM comdb2_tunables WHERE name = 'debug_rowlocks';
PUT TUNABLE debug_rowlocks 0;
SELECT value AS 'debug_rowlocks' FROM comdb2_tunables WHERE name = 'debug_rowlocks';
exec procedure sys.cmd.send('debug_rowlocks')
SELECT value AS 'debug_rowlocks' FROM comdb2_tunables WHERE name = 'debug_rowlocks';
exec procedure sys.cmd.send('nodebug_rowlocks')
SELECT value AS 'debug_rowlocks' FROM comdb2_tunables WHERE name = 'debug_rowlocks';
SELECT value AS 'nodebug_rowlocks' FROM comdb2_tunables WHERE name = 'nodebug_rowlocks';

