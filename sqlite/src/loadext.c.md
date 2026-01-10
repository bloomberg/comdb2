# loadext.c Modifications

## Summary

Modifications to **disable certain extension APIs** that don't apply to comdb2's architecture.

## Modifications

### 1. Disable sqlite3_last_insert_rowid (Lines 207-211)
```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
  0,
#else
  sqlite3_last_insert_rowid,
#endif
```

The `sqlite3_last_insert_rowid` function is disabled (set to NULL in the extension API table).

### 2. Disable Backup APIs (Lines 345-357)
```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
  0,
  0,
  0,
  0,
  0,
#else
  sqlite3_backup_finish,
  sqlite3_backup_init,
  sqlite3_backup_pagecount,
  sqlite3_backup_remaining,
  sqlite3_backup_step,
#endif
```

All SQLite backup APIs are disabled:
- `sqlite3_backup_finish`
- `sqlite3_backup_init`
- `sqlite3_backup_pagecount`
- `sqlite3_backup_remaining`
- `sqlite3_backup_step`

## Why These Changes

1. **Last Insert Rowid**: Comdb2 uses its own rowid system (likely based on genids/rrns) that doesn't map directly to SQLite's autoincrement rowid concept. The standard function would return incorrect results.

2. **Backup APIs**: SQLite's backup APIs work with SQLite's file-based database format. Since comdb2 uses BerkeleyDB as its storage layer and has its own replication/backup mechanisms, the SQLite backup APIs are not applicable.

These APIs are set to NULL to prevent extensions from accidentally using them and getting incorrect results.
