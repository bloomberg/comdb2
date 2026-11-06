# System Tables

## Summary

Comdb2 provides **virtual system tables** that expose database metadata, runtime statistics, configuration, and operational information via SQL queries.

## Files Modified

- `sqlite/src/main.c` - System table registration

## New Files

- External: `db/comdb2systbl.c`, `db/comdb2systbl.h` - System table implementations

## How To

### Querying System Tables

```sql
-- List all tables
SELECT * FROM comdb2_tables;

-- List columns for a table
SELECT * FROM comdb2_columns WHERE tablename = 'users';

-- View indexes
SELECT * FROM comdb2_keys WHERE tablename = 'orders';

-- Check constraints
SELECT * FROM comdb2_constraints WHERE tablename = 'orders';

-- View tunables
SELECT * FROM comdb2_tunables;

-- Check replication status
SELECT * FROM comdb2_replication_stats;

-- View active connections
SELECT * FROM comdb2_connections;

-- View running queries
SELECT * FROM comdb2_active_queries;

-- Check locks
SELECT * FROM comdb2_locks;
```

## System Tables Reference

### Schema Tables

| Table | Description |
|-------|-------------|
| `comdb2_tables` | All tables in database |
| `comdb2_columns` | Column definitions |
| `comdb2_keys` | Index definitions |
| `comdb2_constraints` | Constraint definitions |
| `comdb2_keycomponents` | Index column components |
| `comdb2_tablesizes` | Table size information |

### Configuration Tables

| Table | Description |
|-------|-------------|
| `comdb2_tunables` | Runtime tunable settings |
| `comdb2_plugins` | Loaded plugins |
| `comdb2_appsock_handlers` | Application socket handlers |
| `comdb2_opcode_handlers` | Opcode handlers |

### Runtime Tables

| Table | Description |
|-------|-------------|
| `comdb2_connections` | Active connections |
| `comdb2_active_queries` | Currently running queries |
| `comdb2_locks` | Lock information |
| `comdb2_transaction_state` | Transaction states |
| `comdb2_threadpools` | Thread pool statistics |
| `comdb2_memstats` | Memory statistics |

### Replication Tables

| Table | Description |
|-------|-------------|
| `comdb2_replication_stats` | Replication statistics |
| `comdb2_cluster` | Cluster node information |

### Function Tables

| Table | Description |
|-------|-------------|
| `comdb2_procedures` | Stored procedures |
| `comdb2_lua_sfuncs` | Lua scalar functions |
| `comdb2_lua_afuncs` | Lua aggregate functions |

## Why

### Introspection
- Query schema using SQL (not separate APIs)
- Programmatic access to metadata
- Standard SQL tools work for administration

### Monitoring
- Real-time statistics via SQL
- Integration with monitoring tools
- Historical analysis with queries

### Debugging
- Lock contention analysis
- Query performance investigation
- Resource usage tracking

## Implementation Details

### Registration
```c
// In main.c - openDatabase()
if( !db->mallocFailed && rc==SQLITE_OK ){
    rc = comdb2SystblInit(db);
}
```

### Virtual Table Pattern
```c
// Each system table implements sqlite3_module
static sqlite3_module comdb2_tables_module = {
    0,                         /* iVersion */
    0,                         /* xCreate */
    systblTablesConnect,       /* xConnect */
    systblTablesBestIndex,     /* xBestIndex */
    systblTablesDisconnect,    /* xDisconnect */
    0,                         /* xDestroy */
    systblTablesOpen,          /* xOpen */
    systblTablesClose,         /* xClose */
    systblTablesFilter,        /* xFilter */
    systblTablesNext,          /* xNext */
    systblTablesEof,           /* xEof */
    systblTablesColumn,        /* xColumn */
    systblTablesRowid,         /* xRowid */
    0,                         /* xUpdate */
    0,                         /* xBegin */
    0,                         /* xSync */
    0,                         /* xCommit */
    0,                         /* xRollback */
    0,                         /* xFindFunction */
    0,                         /* xRename */
    0,                         /* xSavepoint */
    0,                         /* xRelease */
    0,                         /* xRollbackTo */
    0                          /* xShadowName */
};
```

### Column Access
```c
static int systblTablesColumn(
    sqlite3_vtab_cursor *cur,
    sqlite3_context *ctx,
    int i
){
    systbl_tables_cursor *pCur = (systbl_tables_cursor*)cur;
    switch(i){
        case 0:  // tablename
            sqlite3_result_text(ctx, pCur->table->tablename, -1, SQLITE_STATIC);
            break;
        case 1:  // type
            sqlite3_result_text(ctx, table_type_str(pCur->table), -1, SQLITE_STATIC);
            break;
        // ...
    }
    return SQLITE_OK;
}
```

## Series Extension

Comdb2 also includes the `generate_series` table-valued function:

```sql
-- Generate a series of numbers
SELECT value FROM generate_series(1, 10);

-- With step
SELECT value FROM generate_series(0, 100, 10);
```

Registration:
```c
#ifdef SQLITE_ENABLE_SERIES
if( !db->mallocFailed && rc==SQLITE_OK ){
    rc = sqlite3SeriesInit(db);
}
#endif
```

## Upgrade Considerations

When upgrading SQLite:
1. **Virtual Table API**: Changes to sqlite3_module may require updates
2. **Registration Order**: System table init happens after core SQLite init
3. **Virtual Table Features**: New vtab capabilities may be useful
