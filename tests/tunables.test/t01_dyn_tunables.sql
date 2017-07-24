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
