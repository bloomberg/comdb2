# callback.c Modifications

## Summary

The callback.c file has modifications related to **schema management** in comdb2. The changes disable SQLite's schema generation tracking and add a function to clear schema entries by table name.

## Modifications

### 1. Disable Schema Generation Counter (Lines 462-471)
```c
if( pSchema->schemaFlags & DB_SchemaLoaded ){
#if defined(SQLITE_BUILDING_FOR_COMDB2)
    /*
    ** we clear the schema when things change and we detect that
    ** comdb2 uses different mechanisms to protect against that, and
    ** sqlite3BtreeUpdateMeta is not supported (sqlite3BtreeGetMeta
    ** returns a constant 0)
    **/
#else
    pSchema->iGeneration++;
#endif
}
```

SQLite normally increments `iGeneration` when schema changes to detect stale prepared statements. Comdb2 has its own mechanism for detecting schema changes, and since `sqlite3BtreeGetMeta` returns a constant 0 in comdb2, this counter-based approach doesn't work.

### 2. New Function: sqlite3SchemaClearByName() (Lines 478-510)
```c
void sqlite3SchemaClearByName(void *p, const char *tblname);
```

This function clears schema information for a specific table by name, rather than clearing the entire schema. This is useful for comdb2's schema change operations where only a single table needs to be refreshed.

## Why These Changes

1. **Schema Generation**: Comdb2 uses its own distributed schema versioning system rather than SQLite's internal generation counter. Since comdb2 doesn't support BTree metadata operations, the standard approach doesn't work.

2. **Per-Table Schema Clear**: When a table's schema changes in a distributed database, it's more efficient to clear just that table's cached schema rather than invalidating everything. This supports comdb2's online schema change feature.
