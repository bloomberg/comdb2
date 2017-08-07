SELECT COUNT(*) AS TUNABLES_COUNT FROM comdb2_tunables;
SELECT * FROM comdb2_tunables WHERE name NOT IN ('dir', 'hostname', 'name', 'next_genid') ORDER BY name;
