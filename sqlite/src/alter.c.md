# alter.c Modifications

## Summary

The alter.c file has been **almost entirely disabled** for comdb2. The vast majority of SQLite's ALTER TABLE implementation is wrapped in `#if !defined(SQLITE_BUILDING_FOR_COMDB2)` blocks, meaning comdb2 provides its own ALTER TABLE implementation elsewhere.

## Modifications

### 1. Disable Main ALTER TABLE Implementation (Lines 22-473)
```c
#if !defined(SQLITE_BUILDING_FOR_COMDB2)
// ... entire ALTER TABLE implementation ...
#endif
```
The core ALTER TABLE logic including:
- `sqlite3AlterRenameTable()` - table renaming
- `sqlite3AlterFinishAddColumn()` - adding columns
- `sqlite3AlterBeginAddColumn()` - column addition preparation

### 2. Disable RENAME COLUMN and Other ALTER Operations (Lines 509-1642)
```c
#if !defined(SQLITE_BUILDING_FOR_COMDB2)
// ... RENAME COLUMN, built-in functions, etc. ...
#endif
```
This disables:
- `sqlite3AlterRenameColumn()` - column renaming
- ALTER TABLE related built-in functions
- Table constraint modifications

## Why These Changes

Comdb2 has its own schema management system that differs fundamentally from SQLite's file-based approach. Since comdb2 uses BerkeleyDB as its storage layer and has distributed/clustered table management, the standard SQLite ALTER TABLE operations don't apply. Comdb2 implements its own DDL processing in `comdb2build.c`.
