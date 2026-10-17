# Query Modes

## Summary

Comdb2 introduces special query execution modes: **DRYRUN** for DDL validation, **SELECTV** for query recording, and **source list extraction** for dependency analysis.

## Files Modified

- `sqlite/src/parse.y` - DRYRUN syntax
- `sqlite/src/tokenize.c` - Source list extraction mode
- `sqlite/src/prepare.c` - Prepare flags
- `sqlite/src/insert.c` - Recording cursor support
- `sqlite/src/trigger.c` - DRYRUN blocking

## How To

### DRYRUN Mode

```sql
-- Enable DRYRUN mode
SET DRYRUN ON;

-- Validate DDL without executing
ALTER TABLE users ADD COLUMN email TEXT;
-- Returns success/error without actually modifying schema

-- Disable DRYRUN mode
SET DRYRUN OFF;
```

### SELECTV (Query Recording)

```sql
-- Execute with recording
SELECTV * FROM orders WHERE customer_id = ?;
-- Statement execution is recorded for auditing
```

### Source List Extraction

```c
// Programmatic use - extract table references
sqlite3_prepare_v3(db, sql, -1,
    SQLITE_PREPARE_SRCLIST_ONLY, &stmt, NULL);
// Parse succeeds even with syntax errors
// Extracts table/view references only
```

## Why

### DRYRUN Mode
- **Validation**: Test DDL statements before deploying
- **CI/CD**: Validate schema migrations in pipelines
- **Safety**: Catch errors before impacting production

### SELECTV (Query Recording)
- **Auditing**: Track what queries were executed
- **Debugging**: Reproduce query behavior
- **Stored Procedures**: Context tracking in procedures

### Source List Extraction
- **Dependencies**: Find which tables a query references
- **Access Control**: Determine required permissions
- **Query Routing**: Route queries to appropriate shards

## Implementation Details

### DRYRUN Mode
```c
// Check if DRYRUN is enabled
if( comdb2IsDryrun(pParse) ){
    sqlite3ErrorMsg(pParse, "DRYRUN not supported for this operation");
    goto cleanup;
}

// Parser flag
struct Parse {
    // ...
    u8 dryrun;  // Non-zero if in DRYRUN mode
};
```

### DRYRUN Blocking Points
DRYRUN is blocked for operations that can't be simulated:
- `BeginTrigger` - trigger.c:106-110
- `FinishTrigger` - trigger.c:296-301
- `DropTrigger` - trigger.c:580-584
- Various DDL operations in build.c

### Recording Cursors
```c
// In insert.c
if( GET_CURSOR_RECORDING(pParse, iCur) ){
    opcode = OP_OpenRead_Record;
}

// Macro to check recording flag
#define GET_CURSOR_RECORDING(pParse, iCur) \
    ((pParse->recording[iCur/32] >> (iCur%32)) & 1)
```

### Source List Extraction
```c
// In tokenize.c - runParser()
if( pParse->prepFlags & SQLITE_PREPARE_SRCLIST_ONLY ){
    if( pParse->rc != SQLITE_OK ){
        // Clear error and continue
        if( pParse->zErrMsg ){
            sqlite3DbFree(db, pParse->zErrMsg);
            pParse->zErrMsg = 0;
        }
        pParse->rc = SQLITE_OK;
        break;
    }
}
```

### Semicolon Requirement
```c
// In tokenize.c
int need_to_check_for_semi =
    pParse->prepFlags & SQLITE_PREPARE_REQUIRE_SEMI;

if( need_to_check_for_semi && lastTokenParsed != TK_SEMI ){
    pParse->rc = SQLITE_MISSING_SEMI;
    break;
}
```

## Prepare Flags

| Flag | Purpose |
|------|---------|
| `SQLITE_PREPARE_SRCLIST_ONLY` | Extract source tables only |
| `SQLITE_PREPARE_REQUIRE_SEMI` | Require terminating semicolon |
| `SQLITE_PREPARE_PERSISTENT` | Statement persists past schema change |

## Recording Cursor Structure
```c
struct Parse {
    // Bitmap of cursors that need recording
    int recording[MAX_CURSOR_IDS / sizeof(int)];
};
```

## Upgrade Considerations

When upgrading SQLite:
1. **Prepare Flags**: New SQLite prepare flags may conflict
2. **Parser State**: Parse structure extensions must be preserved
3. **Error Handling**: Source list mode error suppression must work
4. **New DDL**: New DDL statements need DRYRUN blocking
