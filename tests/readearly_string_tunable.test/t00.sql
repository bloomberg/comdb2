# Verify that setting a READEARLY string tunable via LRL does not crash.
SELECT value FROM comdb2_tunables WHERE name = 'iam_metrics_namespace';
