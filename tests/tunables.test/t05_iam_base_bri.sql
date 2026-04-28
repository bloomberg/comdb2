# Test iam_base_bri tunable: should be settable at runtime once, and not changeable after
SELECT name, value, read_only FROM comdb2_tunables WHERE name = 'iam_base_bri';

# Set iam_base_bri at runtime (should succeed)
PUT TUNABLE iam_base_bri 'test_bri_value';
SELECT name, value FROM comdb2_tunables WHERE name = 'iam_base_bri';

# Try to set again (should fail - already set)
PUT TUNABLE iam_base_bri 'another_value';
SELECT name, value FROM comdb2_tunables WHERE name = 'iam_base_bri';
