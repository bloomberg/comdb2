SELECT COUNT(*) AS TUNABLES_COUNT FROM comdb2_tunables;
SELECT name, description, type, CASE WHEN name IN ('dir', 'hostname', 'appsockpool.stacksz') THEN '***' ELSE value END AS value, read_only FROM comdb2_tunables WHERE name NOT IN ('name') ORDER BY name;
