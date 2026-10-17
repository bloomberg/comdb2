# Tunables System

## Summary

Comdb2 implements a **runtime tunables system** that allows database behavior to be configured without recompilation. These tunables control query optimization, execution strategies, and feature enablement.

## Files Modified

- `sqlite/src/sqliteInt.h` - Tunables structure definition

## New Files

- `sqlite/src/sqlite_tunables.c` - Tunables implementation
- `sqlite/src/sqlite_tunables.h` - Tunables declarations

## How To

### Viewing Tunables

```sql
-- View all tunables
SELECT * FROM comdb2_tunables;

-- View specific tunable
SELECT * FROM comdb2_tunables WHERE name = 'analyze_empty_tables';
```

### Setting Tunables

```sql
-- Set at runtime
PUT TUNABLE analyze_empty_tables 1;

-- Set via admin command
cdb2sql mydb "exec procedure sys.cmd.send('analyze_empty_tables 1')"
```

### Common Tunables

| Tunable | Default | Description |
|---------|---------|-------------|
| `stat4_samples_multiplier` | 1 | Multiplier for ANALYZE sample count |
| `stat4_extra_samples` | 0 | Additional samples for ANALYZE |
| `analyze_empty_tables` | 0 | Record stats for empty tables |
| `seekscan_maxsteps` | -1 | Control seek scan optimization |
| `alternate_normalize` | 1 | Use alternate SQL normalization |
| `old_column_names` | 0 | Use cached column names |

## Why

### Operational Flexibility
- Production databases need runtime tuning without restarts
- Different workloads benefit from different settings
- A/B testing of optimization strategies

### Performance Tuning
- Control query optimizer behavior
- Adjust statistics collection
- Enable/disable specific optimizations

### Debugging
- Enable verbose logging
- Force specific code paths
- Trace execution behavior

## Implementation Details

### Tunables Structure
```c
typedef struct {
    // ANALYZE tunables
    int stat4_samples_multiplier;
    int stat4_extra_samples;
    int analyze_empty_tables;

    // Query optimization
    int seekscan_maxsteps;
    int disable_skipscan_optimization;

    // SQL processing
    int alternate_normalize;
    int old_column_names;

    // ... many more ...
} sqlite3_tunables_t;

extern sqlite3_tunables_t sqlite3_gbl_tunables;
```

### Usage in Code
```c
// In analyze.c
mxSample = numRowsToNumSamplesEst(nRows)
         * sqlite3_gbl_tunables.stat4_samples_multiplier;
mxSample += sqlite3_gbl_tunables.stat4_extra_samples;

// In wherecode.c
if (gbl_seekscan_maxsteps == 0) {
    sqlite3VdbeAddOp0(v, OP_Noop);
} else if (gbl_seekscan_maxsteps > 0) {
    addrSeekScan = sqlite3VdbeAddOp1(v, OP_SeekScan, gbl_seekscan_maxsteps);
}

// In tokenize.c
if( gbl_alternate_normalize ){
    p->zNormSql = sqlite3Normalize_alternate(p, p->zSql, 0);
}
```

### System Table Integration
Tunables are exposed via `comdb2_tunables` system table:
```c
// In comdb2systbl.c
static int tunablesColumn(sqlite3_vtab_cursor *cur,
                          sqlite3_context *ctx, int i) {
    switch(i) {
        case 0: sqlite3_result_text(ctx, tunable->name, -1, SQLITE_STATIC);
        case 1: sqlite3_result_int(ctx, *tunable->value);
        // ...
    }
}
```

## Tunables Reference

### ANALYZE Tunables

| Tunable | Type | Default | Description |
|---------|------|---------|-------------|
| `stat4_samples_multiplier` | int | 1 | Multiply sample count by this |
| `stat4_extra_samples` | int | 0 | Add this many extra samples |
| `analyze_empty_tables` | bool | 0 | Analyze tables with 0 rows |

### Query Optimization Tunables

| Tunable | Type | Default | Description |
|---------|------|---------|-------------|
| `seekscan_maxsteps` | int | -1 | -1=auto, 0=off, >0=fixed |
| `disable_skipscan_optimization` | bool | 0 | Disable skip-scan |
| `use_raw_data` | bool | 0 | Use raw data optimization |

### SQL Processing Tunables

| Tunable | Type | Default | Description |
|---------|------|---------|-------------|
| `alternate_normalize` | bool | 1 | Use Comdb2 SQL normalization |
| `old_column_names` | bool | 0 | Cache column metadata |

## Upgrade Considerations

When upgrading SQLite:
1. **New Optimizations**: New SQLite optimizations may need corresponding tunables
2. **Removed Features**: Tunables for removed features become obsolete
3. **Changed Defaults**: SQLite default changes may warrant tunable additions
4. **Structure Changes**: sqlite3_tunables_t may need new fields
