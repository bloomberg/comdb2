put tunable typessql_records_max 1
SELECT NULL AS a UNION ALL SELECT 1 ORDER BY a;
