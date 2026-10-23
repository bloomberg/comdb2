# SQL Syntax Extensions

## Summary

Comdb2 extends SQLite's SQL grammar with **80+ new keywords** and syntax constructs for DDL operations, transaction control, system administration, and Comdb2-specific features.

## Files Modified

- `sqlite/src/parse.y` - Grammar rules for new syntax
- `sqlite/src/tokenize.c` - New token types and lexer rules
- `sqlite/src/build.c` - DDL statement handling

## New Files

- `sqlite/src/comdb2build.c` - Comdb2 DDL implementation
- `sqlite/src/comdb2build.h` - DDL declarations

## How To

### Transaction Control

```sql
-- Set transaction isolation level
SET TRANSACTION READ COMMITTED;
SET TRANSACTION SNAPSHOT ISOLATION;

-- Control transaction behavior
SET TRANSACTION CHUNK 1000;  -- Chunk large transactions
```

### Table Operations

```sql
-- Create table with Comdb2 options
CREATE TABLE users (
    id INT PRIMARY KEY,
    name TEXT
) OPTIONS (ODH OFF, BLOBFIELD NONE);

-- Alter table with specific options
ALTER TABLE users OPTIONS (REC ZLIB, BLOB LZ4);

-- Truncate with options
TRUNCATE TABLE users;
```

### Index Operations

```sql
-- Create partial index
CREATE INDEX idx_active ON users(status) WHERE status = 'active';

-- Create index with data copy
CREATE INDEX idx_covering ON orders(customer_id) INCLUDE (order_date, total);

-- Rebuild index
REBUILD INDEX idx_active;
```

### Schema Operations

```sql
-- Create time partition
CREATE TIME PARTITION ON events
PERIOD 'daily'
RETENTION 90
START '2024-01-01';

-- Manage schema
PUT SCHEMA table_name 'csc2_schema_definition';
GET SCHEMA table_name;
```

### Administrative Commands

```sql
-- Analyze database
ANALYZE;
ANALYZE table_name;

-- Get execution plan
EXPLAIN QUERY PLAN SELECT * FROM users WHERE id = 1;

-- DRYRUN mode (validate without executing)
SET DRYRUN ON;
```

## New Keywords

### DDL Keywords
`PARTITION`, `PERIOD`, `RETENTION`, `SHARD`, `CONSTRAINT`, `OPTIONS`, `ODH`, `IPU`, `ISC`, `REC`, `BLOB`, `REBUILD`, `TRUNCATE`, `PUT`, `GET`, `SCHEMA`, `ANALYZE`

### Transaction Keywords
`CHUNK`, `SNAPSHOT`, `SERIALIZABLE`, `COMMITTED`, `UNCOMMITTED`, `REPEATABLE`, `ISOLATION`

### Type Keywords
`DATETIME`, `DATETIMEUS`, `INTERVAL`, `DECIMAL`, `CSTRING`, `VUTF8`

### Index Keywords
`DATACOPY`, `INCLUDE`, `COVERING`, `PARTIAL`, `UNIQNULLS`, `SKIPSCAN`

### System Keywords
`GENID48`, `LZ4`, `ZLIB`, `RLE`, `CRLE`

## Why

### Enterprise DDL
- Schema operations need fine-grained control (compression, on-disk format)
- Time partitioning for data lifecycle management
- Index options for query optimization

### Transaction Management
- Chunked transactions for large data operations
- Snapshot isolation for consistent reads
- Distributed transaction semantics

### Operational Control
- DRYRUN mode for DDL validation
- ANALYZE integration with Comdb2's statistics
- Schema introspection commands

## Implementation Details

### Token Types
New token types in tokenize.c:
```c
TK_GENID48   // 48-bit generation ID keyword
TK_LZ4       // LZ4 compression keyword
TK_NOSQL     // NoSQL query block {json}
```

### NoSQL Token Handling
```c
case CC_LB: {  // Left brace '{'
    // Scan to end of string, expect closing '}'
    for(i=1; (c=z[i])!=0; i++){}
    *tokenType = z[i-1]=='}' ? TK_NOSQL : TK_ILLEGAL;
    return i;
}
```

### Grammar Rules
Parser rules in parse.y route to comdb2 functions:
```
cmd ::= PUT SCHEMA nm(X) BLOBVAL(Y). {
    comdb2PutSchema(pParse, &X, &Y);
}

cmd ::= CREATE TIME PARTITION ON nm(X) ... {
    comdb2CreateTimePartition(pParse, &X, ...);
}
```

### DDL Routing
DDL statements are routed to Comdb2's schema engine:
```c
// Standard SQLite DDL is intercepted
if( comdb2IsDryrun(pParse) ){
    sqlite3ErrorMsg(pParse, "DRYRUN not supported for this operation");
    return;
}
// Otherwise route to comdb2 DDL handler
comdb2AlterTable(pParse, pSrc, pColumns);
```

## Upgrade Considerations

When upgrading SQLite:
1. **Grammar Conflicts**: New SQLite keywords may conflict with Comdb2 keywords
2. **Parser Generator**: Lemon parser changes may require parse.y adjustments
3. **Token Values**: New token types must not conflict with TK_GENID48, TK_LZ4, TK_NOSQL
4. **Tokenizer Changes**: Character class modifications need to preserve CC_LB/CC_RB
5. **Statement Types**: New SQLite statement types may need Comdb2 routing
