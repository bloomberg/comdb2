# Verify that setting a string tunable via LRL does not crash.
SELECT value FROM comdb2_tunables WHERE name = 'iam_metrics_namespace';
PUT TUNABLE mask_internal_tunables 0
SELECT value FROM comdb2_tunables WHERE name = 'debug_default_string_update';
