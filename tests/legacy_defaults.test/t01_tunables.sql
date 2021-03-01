SELECT "== Tunables in legacy_defaults mode ==" as test;

SELECT * FROM comdb2_tunables where name = 'legacy_defaults';

# In legacy_defaults mode:
# * some tunables are automatically set/enabled by default
# * all 'internal' tunables are made visible via comdb2_tunables
SELECT * FROM comdb2_tunables where name = 'mask_internal_tunables';

# Querying the 'plugin' tunable used to trigger a segfault as
# the its internal structure does not have a variable to
# store the value (see db/plugin_handle.c for the rationale).
SELECT * FROM comdb2_tunables where name LIKE '%plugin%';
