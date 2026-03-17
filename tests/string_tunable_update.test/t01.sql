# Verify that updating a string tunable with initial value
# but no update handler at runtime does not crash.
PUT TUNABLE debug_default_string_update 'comdb2-string-tunable-test-runtime';
SELECT value FROM comdb2_tunables WHERE name = 'debug_default_string_update';
