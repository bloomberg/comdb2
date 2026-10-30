# Custom Storage Engine Integration

## Summary

Comdb2 replaces SQLite's built-in btree/pager storage layer with **BerkeleyDB** as its storage engine. This requires disabling SQLite's page cache and providing custom btree implementations.

## Files Modified

- `sqlite/src/main.c` - Page cache function stubs
- `sqlite/src/vdbe.c` - Custom btree cursor operations
- `sqlite/src/vdbeaux.c` - VDBE preparation for custom storage
- `sqlite/src/build.c` - Schema operations for custom storage
- `sqlite/src/sqliteInt.h` - BtCursor and Btree structure adjustments

## New Files

- `sqlite/src/sqlite_btree.h` - Btree interface definitions

## How To

### Understanding the Storage Layer

Comdb2's storage engine provides:
- ACID transactions with distributed commit
- MVCC (Multi-Version Concurrency Control)
- Replication across cluster nodes
- Custom page layouts optimized for Comdb2's access patterns

### Configuration

The storage engine is configured at database creation time, not at runtime. Comdb2 users don't directly interact with storage engine settings through SQL.

## Why

### Enterprise Requirements
- **Replication**: BerkeleyDB provides the foundation for Comdb2's synchronous replication
- **MVCC**: Proper snapshot isolation requires storage-layer support
- **Durability**: Enterprise-grade durability guarantees beyond SQLite's defaults

### Architecture
- SQLite's btree is designed for single-file embedded databases
- Comdb2 needs networked, replicated storage
- Separation of concerns: SQLite handles SQL, BerkeleyDB handles storage

## Implementation Details

### Page Cache Stubs
SQLite's page cache is completely disabled via stub functions:

```c
// In main.c
void sqlite3PCacheBufferSetup(void *p, int sz, int n){ return; }
int sqlite3PcacheInitialize(void){ return 0; }
void sqlite3PcacheShutdown(void){ return; }
void sqlite3PCacheSetDefault(void){ return; }
int sqlite3HeaderSizeBtree(void){ return 0; }
int sqlite3HeaderSizePcache(void){ return 0; }
int sqlite3HeaderSizePcache1(void){ return 0; }
```

### Null Pager Safety
Since Comdb2 may not have a pager in certain contexts:

```c
// In main.c - sqlite3_file_control()
if( pPager ){
    // ... pager operations ...
} else {
    rc = SQLITE_NOTFOUND;
}
```

### Custom Record Format
Comdb2 uses its own record format, not SQLite's:

```c
// In insert.c
if( gbl_sqlite_makerecord_for_comdb2 ){
    sqlite3VdbeAddOp2(v, OP_Integer, iDataCur, regRec);
    // Set flag for Comdb2 format conversion
    sqlite3VdbeChangeP5(v, OPFLAG_MKREC_COMDB2);
}
```

### Constraint Delegation
NOT NULL and CHECK constraints are handled by the storage layer:

```c
// In insert.c - constraint checks are skipped
#if !defined(SQLITE_BUILDING_FOR_COMDB2)
    /* Test all NOT NULL constraints. */
    /* Test all CHECK constraints */
#endif
```

### DATACOPY Collation
For indexes that store data copies without ordering:

```c
// In main.c
static int datacopyCollatingFunc(
    void *NotUsed,
    int nKey1, const void *pKey1,
    int nKey2, const void *pKey2
){
    return 0;  // All keys equal - no ordering
}
```

## Btree Interface

Comdb2 implements the SQLite btree interface but routes to BerkeleyDB:

| SQLite Function | Comdb2 Behavior |
|-----------------|-----------------|
| `sqlite3BtreeOpen` | Opens BerkeleyDB environment |
| `sqlite3BtreeCursor` | Creates BerkeleyDB cursor |
| `sqlite3BtreeInsert` | BerkeleyDB put operation |
| `sqlite3BtreeDelete` | BerkeleyDB delete operation |
| `sqlite3BtreeNext` | BerkeleyDB cursor advance |

## Upgrade Considerations

When upgrading SQLite:
1. **Btree API Changes**: Any changes to the btree interface require corresponding BerkeleyDB adapter updates
2. **Page Cache**: Ensure stub functions remain compatible with any new page cache APIs
3. **Record Format**: Changes to OP_MakeRecord may affect Comdb2's record conversion
4. **Pager References**: New code paths may assume pager exists - add null checks
5. **Constraint Handling**: Ensure constraint bypass logic remains in place
