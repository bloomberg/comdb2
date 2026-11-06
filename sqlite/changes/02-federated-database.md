# Federated Database (FDB)

## Summary

Comdb2 supports **federated queries** that can access tables across multiple Comdb2 database clusters. This allows queries to JOIN local tables with tables from remote databases transparently.

## Files Modified

- `sqlite/src/build.c` - Remote table discovery and schema loading
- `sqlite/src/attach.c` - Dynamic database attachment (`comdb2_dynamic_attach`)
- `sqlite/src/insert.c` - Remote INSERT handling, temporary table requirements
- `sqlite/src/select.c` - Remote SELECT optimization
- `sqlite/src/wherecode.c` - Cursor hints for remote queries
- `sqlite/src/vdbeapi.c` - Remote database detection (`sqlite3_stmt_has_remotes`)
- `sqlite/src/sqliteInt.h` - Db structure extensions for remote databases

## How To

### Accessing Remote Tables

```sql
-- Attach a remote database
ATTACH 'remote_cluster' AS remote;

-- Query remote table
SELECT * FROM remote.customers WHERE region = 'US';

-- Join local and remote tables
SELECT o.id, c.name
FROM orders o
JOIN remote.customers c ON o.customer_id = c.id;

-- Insert from remote to local
INSERT INTO local_customers
SELECT * FROM remote.customers WHERE active = 1;
```

### Dynamic Attachment

```c
// In application code
comdb2_dynamic_attach(db, "remote_cluster", "remote");

// ... use remote tables ...

comdb2_dynamic_detach(db, "remote");
```

## Why

### Distributed Architecture
- Comdb2 clusters may span multiple physical deployments
- Applications need to query data across clusters without manual data movement
- Enables microservices architectures where each service owns its database

### Transparency
- SQL queries work the same whether tables are local or remote
- The optimizer handles predicate pushdown to remote nodes
- Transaction semantics are preserved across remote queries

## Implementation Details

### Database Index Convention
- `iDb=0` - main database (local)
- `iDb=1` - temp database
- `iDb>1` - attached databases (potentially remote)

### Remote Detection
```c
// In vdbeapi.c
int sqlite3_stmt_has_remotes(sqlite3_stmt *pStmt) {
    Vdbe *p = (Vdbe*)pStmt;
    // Check btreeMask for databases beyond main (0) and temp (1)
    // Bit 2+ indicates remote databases
    return (p->btreeMask & ~3) != 0;
}
```

### Cursor Hints for Remote Queries
The `codeCursorHint()` function in wherecode.c has special handling:
```c
// Only generate cursor hints for remote cursors
if( pWInfo->pTabList->a[iLevel].zDatabase==NULL )
    return;  // Skip local tables
```

### Temporary Table Requirement for Remote INSERT
```c
// In insert.c
// Remote INSERT SELECT always uses temp table to comply with FDB protocol
if( iDb>1 ){
    return 1;  // Force temporary table
}
```

### AST Tracking
```c
// Track INSERT operations to remote tables
ast_t *ast = ast_init(pParse, __func__);
if( ast ) ast_push(ast, AST_TYPE_INSERT, v, (iDb>1) ? pTab : NULL);
```

## FDB Protocol Requirements

1. **Cursor Closure**: All cursors must be closed before committing a transaction chunk
2. **Predicate Pushdown**: WHERE conditions should be sent to remote nodes
3. **Schema Verification**: Remote schema version must be validated
4. **Transaction Coordination**: Distributed transactions require special handling

## Upgrade Considerations

When upgrading SQLite:
1. Preserve `zDatabase` field usage in SrcList_item for remote table detection
2. Maintain cursor hint modifications in wherecode.c
3. Ensure temporary table logic in insert.c remains intact
4. Preserve AST tracking for remote operations
5. Check for changes to database attachment logic in attach.c
