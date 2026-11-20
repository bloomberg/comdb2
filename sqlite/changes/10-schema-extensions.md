# Schema Extensions

## Summary

Comdb2 extends SQLite's schema handling with **CSC2 schema format**, **table versioning**, **partial indexes**, **DATACOPY columns**, and enhanced index features.

## Files Modified

- `sqlite/src/sqliteInt.h` - Extended Table, Index, Db structures
- `sqlite/src/build.c` - Schema building with CSC2 support
- `sqlite/src/analyze.c` - DATACOPY column handling
- `sqlite/src/parse.y` - Schema-related grammar

## New Files

- `sqlite/src/comdb2build.c` - CSC2 schema operations
- `sqlite/src/comdb2build.h` - Schema declarations

## How To

### CSC2 Schema Format

```sql
-- Put a CSC2 schema
PUT SCHEMA tablename 'schema
    int id
    cstring name[64]
    datetime created
keys
    "PK" = id
';

-- Get a CSC2 schema
GET SCHEMA tablename;
```

### Extended Index Features

```sql
-- Partial index
CREATE INDEX idx_active ON users(name) WHERE active = 1;

-- DATACOPY (covering) index
CREATE INDEX idx_covering ON orders(customer_id) DATACOPY;

-- Index with included columns
CREATE INDEX idx_include ON orders(id) INCLUDE (total, date);

-- Expression index
CREATE INDEX idx_expr ON users(LOWER(email));

-- Unique with nulls allowed
CREATE UNIQUE INDEX idx_nullable ON users(ssn) WHERE ssn IS NOT NULL;
```

### Table Options

```sql
-- Create with options
CREATE TABLE logs (
    id INT,
    message TEXT
) OPTIONS (
    REC ZLIB,      -- Record compression
    BLOB LZ4,      -- Blob compression
    ODH OFF        -- On-disk header off
);
```

## Why

### CSC2 Schema Format
- Native Comdb2 schema definition language
- Precise control over field types and sizes
- Compatibility with existing Comdb2 tooling

### Partial Indexes
- Smaller index size
- Faster index scans for common queries
- Reduced maintenance overhead

### DATACOPY Columns
- Covering indexes without rowid lookup
- Improved query performance
- Reduced I/O for common access patterns

### Table Versioning
- Track schema changes across replication
- Detect stale queries after schema changes
- Support online schema changes

## Implementation Details

### Extended Table Structure
```c
struct Table {
    // ... standard SQLite fields ...
#ifdef SQLITE_BUILDING_FOR_COMDB2
    int version;      // Schema version for change detection
    int iDb;          // Database index (for remote tables)
    int hasPartIdx;   // Has partial indexes
    int hasExprIdx;   // Has expression indexes
#endif
};
```

### Extended Index Structure
```c
struct Index {
    // ... standard SQLite fields ...
#ifdef SQLITE_BUILDING_FOR_COMDB2
    // DATACOPY marked by "DATACOPY" collation name
    // Partial index has pPartIdxWhere set
#endif
};
```

### Extended Db Structure
```c
struct Db {
    // ... standard SQLite fields ...
#ifdef SQLITE_BUILDING_FOR_COMDB2
    int class;           // Database class
    int class_override;  // Override class
    int local;           // Is local database
    int version;         // Schema version
#endif
};
```

### DATACOPY Handling in ANALYZE
```c
// In analyze.c - stop at DATACOPY marker
nCol = pIdx->nKeyCol;
for(i=0; i < nCol; ++i){
    if( strcmp(pIdx->azColl[i], "DATACOPY")==0 ){
        nCol = i;
        break;
    }
}
```

### Skip Scan Control
```c
// Check if skip scan is disabled for index
if( is_comdb2_index_disableskipscan(pIdx->zName) ){
    // Don't use skip scan optimization
}
```

### Schema Version Verification
```c
// Track tables for verification
sqlite3VdbeAddTable(v, pTab);

// At execution time
if( tableVersionChanged(pTab) ){
    return SQLITE_COMDB2SCHEMA;
}
```

## CSC2 vs SQLite Types

| CSC2 Type | SQLite Affinity |
|-----------|-----------------|
| `int` | INTEGER |
| `u_int` | INTEGER |
| `short` | INTEGER |
| `longlong` | INTEGER |
| `float` | REAL |
| `double` | REAL |
| `cstring` | TEXT |
| `vutf8` | TEXT |
| `byte` | BLOB |
| `blob` | BLOB |
| `datetime` | DATETIME |
| `datetimeus` | DATETIMEUS |
| `intervalym` | INTERVAL |
| `intervalds` | INTERVAL |
| `decimal` | DECIMAL |

## sqlite_master Extensions

```sql
-- CSC2 column added to sqlite_master
SELECT type, name, tbl_name, rootpage, sql, csc2
FROM sqlite_master;
```

## Upgrade Considerations

When upgrading SQLite:
1. **Table Structure**: Preserve version, iDb fields
2. **Index Structure**: Maintain partial index support
3. **Schema Parsing**: CSC2 integration in build.c
4. **Master Table**: Extra CSC2 column in INSERT statements
5. **Collation Names**: DATACOPY collation handling
