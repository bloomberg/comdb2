# fwd_types.h

## Summary

**Forward declarations** of SQLite internal types for use by comdb2 code.

## Contents

```c
typedef struct UnpackedRecord UnpackedRecord;
typedef struct Vdbe Vdbe;
typedef struct Btree Btree;
typedef struct BtCursor BtCursor;
typedef struct BtShared BtShared;
typedef struct BtreePayload BtreePayload;
typedef struct sqlite3_value Mem;
typedef struct Schema Schema;
```

## Forward Declared Types

| Type | Description |
|------|-------------|
| `UnpackedRecord` | Unpacked database record for comparison |
| `Vdbe` | Virtual Database Engine - compiled SQL statement |
| `Btree` | B-tree handle for database files |
| `BtCursor` | Cursor for navigating B-tree |
| `BtShared` | Shared B-tree state |
| `BtreePayload` | Payload for B-tree operations |
| `Mem` | Memory cell (aka sqlite3_value) |
| `Schema` | Database schema representation |

## Purpose

This file allows comdb2 code to:
1. Reference SQLite internal types without including full headers
2. Break circular include dependencies
3. Improve compilation speed by avoiding unnecessary header includes

## Why This File Exists

Comdb2's code outside of the sqlite directory often needs to work with SQLite internal types for:
- BerkeleyDB B-tree integration
- Custom cursor implementations
- Schema management
- Value handling in the storage layer

Forward declarations allow these references without pulling in all of sqliteInt.h.
