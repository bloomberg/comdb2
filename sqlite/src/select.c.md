# SQLite select.c Modifications for Comdb2

This document analyzes the modifications made to SQLite's `select.c` file for the Comdb2 project. All modifications are wrapped in `#if defined(SQLITE_BUILDING_FOR_COMDB2)` blocks.

## Summary of Modifications

The modifications to select.c primarily focus on:

1. **Recording Mode Support** - Infrastructure for tracking and recording query execution
2. **Parallel Query Execution** - LIMIT/OFFSET handling for parallel query processing
3. **Remote Database Access** - Schema verification for remote database tables
4. **Special Column Handling** - Support for Comdb2-specific columns like `comdb2_rowtimestamp`
5. **Source List Extraction** - Ability to extract table lists without full query execution
6. **Optimization Controls** - Comdb2-specific query optimization flags
7. **Index Preferences** - Special handling for parallel count operations

## 1. Recording Mode Infrastructure

### Location: Lines 91-100, 186-188

**Purpose**: Enable query execution tracking by setting a "recording" flag on SELECT statements and their cursors.

```c
static void _set_src_recording(Parse *pParse, Select *pSub) {
  int tbl;
  pSub->recording = 1;
  for(tbl=0; tbl<pSub->pSrc->nSrc; tbl++){
    SET_CURSOR_RECORDING(pParse, pSub->pSrc->a[tbl].iCursor);
  }
}
```

**Why**: This allows Comdb2 to track which tables and cursors are involved in a query for monitoring, debugging, or special execution modes. The recording flag is propagated through subqueries during flattening operations.

**Usage Points**:
- Lines 3961, 3985: Called during subquery flattening
- Line 186: Initialized to 0 in `sqlite3SelectNew()`
- Lines 4983, 6755: Used in `OP_OpenRead_Record` operations

---

## 2. External Function Declarations

### Location: Lines 86-89

**Declarations**:
```c
extern int comdb2_register_limit(int, int);
extern void comdb2_register_offset(int, int, int);
extern const char *comdb2_get_dbname(void);
extern void comdb2_set_verify_remote_schemas(void);
```

**Purpose**: Interface with Comdb2-specific functionality:
- `comdb2_register_limit()`: Register LIMIT clause for parallel processing
- `comdb2_register_offset()`: Register OFFSET clause for parallel processing
- `comdb2_get_dbname()`: Get current database name for remote table detection
- `comdb2_set_verify_remote_schemas()`: Trigger schema verification for remote databases

---

## 3. Parallel Query Support - LIMIT/OFFSET Processing

### Location: Lines 2245-2264

**Purpose**: Enable parallel query execution by registering LIMIT and OFFSET values.

```c
int is_parallel;
if( (is_parallel = comdb2_register_limit(iLimit, ++pParse->nMem))!=0 ){
  sqlite3VdbeAddOp2(v, OP_IntCopy, iLimit, pParse->nMem);
}

if( pLimit->pRight ){
  // ... offset handling ...
  if( is_parallel ){
    comdb2_register_offset(iOffset, iOffset+1, ++pParse->nMem);
    sqlite3VdbeAddOp2(v, OP_IntCopy, iOffset, pParse->nMem);
  }
}
```

**Why**: Parallel query execution requires copying LIMIT/OFFSET values to additional registers so multiple parallel workers can independently track their progress without interference.

**Impact**: When `comdb2_register_limit()` returns non-zero (indicating parallel mode is enabled), extra `OP_IntCopy` operations preserve these values for parallel execution.

---

## 4. setJoinExpr Function Visibility Change

### Location: Lines 78, 408

**Change**: Made `setJoinExpr()` a non-static function renamed to `sqlite3SetJoinExpr()`

```c
// Before: static void setJoinExpr(Expr *p, int iTable)
// After:  void sqlite3SetJoinExpr(Expr *p, int iTable)
```

**Why**: Comdb2 needs to call this function from external code to properly mark expressions as part of JOIN conditions. This is crucial for LEFT JOIN handling where expressions need the `EP_FromJoin` property set.

**Usage**: Called when processing ON/USING clauses (lines 512, 4114) and in comment updates.

---

## 5. Special Column Type Handling

### Location: Lines 1742-1757, 1931-1942

**Purpose**: Support Comdb2-specific pseudo-columns, particularly `comdb2_rowtimestamp`.

```c
if( iCol<0 ){
  switch( iCol ){
    default:
      zType = "INTEGER";
      zOrigCol = "rowid";
      break;
    case -3:
      zType = "DATETIME";
      zOrigCol = "comdb2_rowtimestamp";
      break;
  }
}
```

**Why**: Comdb2 extends SQLite with special columns:
- `iCol == -1` (default): Standard ROWID (INTEGER)
- `iCol == -3`: Comdb2's row timestamp (DATETIME)

These columns don't exist in the table schema but are computed at runtime. The type system needs to return appropriate types for these virtual columns.

---

## 6. Source List Extraction Mode

### Location: Lines 5808-5827, 5829-5838

**Purpose**: Extract table names from a SELECT without executing the query.

```c
if( !db->mallocFailed && pParse->prepFlags&SQLITE_PREPARE_SRCLIST_ONLY ){
  SrcList *pSrc = p->pSrc;
  int iSrc;

  pParse->azSrcListOnly = sqlite3DbMallocZero(db, pSrc->nSrc * sizeof(char*));

  for(iSrc=0; iSrc<pSrc->nSrc; iSrc++){
    const char *zSrcDatabase = pSrc->a[iSrc].zDatabase;
    if( !zSrcDatabase ) zSrcDatabase = "main";
    pParse->azSrcListOnly[iSrc] = sqlite3MPrintf(db,
        "%s.%s", zSrcDatabase, pSrc->a[iSrc].zName
    );
  }
  pParse->nSrcListOnly = pSrc->nSrc;
  rc = 0;
  goto select_end;
}
```

**Why**: Comdb2 needs to:
1. Analyze queries to determine which tables are accessed
2. Perform authorization checks before execution
3. Check if remote tables are involved without executing the query
4. Build query plans without full execution

This optimization allows query preparation to stop after identifying tables, saving significant processing time.

---

## 7. Remote Table Schema Verification

### Location: Lines 5541-5556, 5830-5838

**Purpose**: Detect and handle queries involving remote databases.

```c
static int sql_has_remotes(Parse *pParse, SrcList *pList) {
  int i;
  for(i=0;i<pList->nSrc; i++){
    char *dbname = pList->a[i].zDatabase;
    if(dbname && strcasecmp(dbname,"main") && strcasecmp(dbname, "temp") &&
            strcasecmp(dbname, comdb2_get_dbname())) {
      return 1;
    }
  }
  return 0;
}
```

**Usage**:
```c
if( pParse->checkSchema == 1 &&
    pParse->zErrMsg && strncasecmp(pParse->zErrMsg, "no such column",
        strlen("no such column")) == 0){
  if (sql_has_remotes(pParse, p->pSrc)) {
    comdb2_set_verify_remote_schemas();
  }
}
```

**Why**: When a "no such column" error occurs and remote tables are involved, it might indicate that:
1. The remote schema has changed
2. Local cached schema is out of date
3. Schema verification is needed

Calling `comdb2_set_verify_remote_schemas()` triggers a schema refresh for remote databases.

---

## 8. Optimization Control - Subquery Flattening

### Location: Lines 3841-3846

**Purpose**: Disable subquery flattening optimization for LEFT JOIN with subqueries.

```c
if( (pSubitem->fg.jointype & JT_OUTER)!=0 ){
  extern int gbl_enable_sq_flattening_optimization;
  if (!gbl_enable_sq_flattening_optimization) {
    return 0;
  }
  isLeftJoin = 1;
  // ...
}
```

**Why**: Subquery flattening can change query semantics for LEFT JOINs. Example:

```sql
-- Original
SELECT * FROM t1 LEFT JOIN (SELECT x FROM t2 WHERE y > 10) ON t1.a = x

-- If flattened incorrectly, could become
SELECT * FROM t1 LEFT JOIN t2 ON t1.a = t2.x WHERE t2.y > 10
```

The second form is semantically different - the WHERE clause filters after the join, while the original filters before.

The global flag `gbl_enable_sq_flattening_optimization` allows runtime control of this optimization.

---

## 9. Compound SELECT Ephemeral Table Optimization

### Location: Lines 2808-2810

**Purpose**: Mark ephemeral tables as unordered for better performance.

```c
addr = sqlite3VdbeAddOp2(v, OP_OpenEphemeral, tab1, 0);
sqlite3VdbeChangeP5(v, BTREE_UNORDERED);
```

**Why**: For INTERSECT operations, the ephemeral table doesn't need to maintain order. The `BTREE_UNORDERED` flag allows the storage engine to use more efficient data structures (hash tables instead of B-trees).

---

## 10. SRT_Set Comment - Known Issue

### Location: Lines 1575-1579

**Purpose**: Document a potential compatibility issue.

```c
/* FIXME TODO XXX
 * This might be incorrect. prod has some hack to make the following
 * use numeric type. The opcodes here have changed. Need to verify */
```

**Why**: This indicates:
1. Production Comdb2 has a type coercion hack for set membership operations
2. SQLite opcodes changed between versions
3. The current implementation may not correctly handle type conversions
4. Needs verification/testing

This is for `IN` subquery operations: `WHERE x IN (SELECT ...)`.

---

## 11. Assert Removals

### Location: Lines 123, 1906-1908, 2024

**Purpose**: Remove assertions that don't hold in Comdb2's execution model.

```c
// Line 123: Added assertion about pWin
assert( p->pWin==0 );

// Lines 1906-1908: Made assertion conditional
#if !defined(SQLITE_BUILDING_FOR_COMDB2)
  assert( pTabList!=0 );
#endif

// Line 2024: Comment removed assertion about TK_AGG_COLUMN
// assert( pColExpr->op!=TK_AGG_COLUMN );
```

**Why**:
- Window functions (`pWin`) may be present in Comdb2's extended SQL
- `pTabList` can legitimately be NULL in some Comdb2 query patterns
- Aggregate column operations may occur in different order in Comdb2

---

## 12. Cursor Assignment for Recording

### Location: Lines 4983, 4985-4987

**Purpose**: Assign cursors differently when in recording mode.

```c
sqlite3SrcListAssignCursors(pParse, pTabList, p->recording);
```

**Why**: Recording mode needs to track cursor usage for query analysis. The additional parameter tells the cursor assignment routine to mark cursors as being recorded.

---

## 13. AST (Abstract Syntax Tree) Integration

### Location: Lines 5953-5954

**Purpose**: Hook into Comdb2's AST system for query analysis.

```c
ast_t *ast = ast_init(pParse, __func__);
if( ast ) ast_push(ast, AST_TYPE_SELECT, v, p);
```

**Why**: Comdb2 maintains an AST representation for:
- Query plan visualization
- Query optimization analysis
- Debugging and profiling
- Query rewriting

---

## 14. Count Optimization with Parallel Execution

### Location: Lines 6735-6749

**Purpose**: Choose between index-based and table-based counting based on parallel execution settings.

```c
extern int gbl_direct_count;
extern int gbl_parallel_count;
int use_data = gbl_direct_count && gbl_parallel_count;

if( pBest ){
  if( use_data==0 ){
    iRoot = pBest->tnum;
    pKeyInfo = sqlite3KeyInfoOfIndex(pParse, pBest);
  }
}
```

**Why**: For `SELECT count(*)` queries:
- **Normal mode**: Use smallest index (faster for single-threaded)
- **Parallel mode**: Use data table directly (better for parallel workers)

Parallel counting on the data table allows:
- Multiple workers to scan different ranges simultaneously
- Better parallelization than index scanning
- Avoids index lock contention

---

## 15. Recording Mode for Count Operations

### Location: Lines 6753-6759

**Purpose**: Use special opcode for count operations in recording mode.

```c
if( p->recording ){
    sqlite3VdbeAddOp4Int(v, OP_OpenRead_Record, iCsr, iRoot, iDb, 1);
}else
    sqlite3VdbeAddOp4Int(v, OP_OpenRead, iCsr, iRoot, iDb, 1);
```

**Why**: `OP_OpenRead_Record` is a Comdb2-specific opcode that:
- Records the tables being read
- May apply different locking strategies
- Enables query execution tracking
- Supports explain/analyze functionality

---

## 16. Table Registration

### Location: Line 6712

**Purpose**: Register table access with the VDBE.

```c
sqlite3VdbeAddTable(v, pTab);
```

**Why**: Comdb2 needs to track all tables accessed by a query for:
- Lock management
- Transaction tracking
- Access control enforcement
- Query auditing

---

## 17. Count Optimization Control

### Location: Lines 6184-6193

**Purpose**: Make count-of-view optimization controllable.

```c
#ifdef SQLITE_COUNTOFVIEW_OPTIMIZATION
  if( OptimizationEnabled(db, SQLITE_QueryFlattener|SQLITE_CountOfView)
   && countOfViewOptimization(pParse, p)
  ){
    if( db->mallocFailed ) goto select_end;
    pEList = p->pEList;
    pTabList = p->pSrc;
  }
#endif
```

**Why**: This optimization transforms queries like:
```sql
SELECT count(*) FROM (SELECT ... UNION ALL SELECT ...)
```
into:
```sql
SELECT (SELECT count(*) FROM ...) + (SELECT count(*) FROM ...)
```

Comdb2 enables this conditionally because it may not always be beneficial, especially with:
- Complex WHERE clauses
- Remote tables
- Distributed queries

---

## Key Design Principles

### 1. **Minimal Invasiveness**
All modifications are conditionally compiled. Standard SQLite builds are unaffected.

### 2. **Parallel Execution Support**
Multiple modifications support parallel query execution:
- LIMIT/OFFSET register copying
- Direct table access for counts
- Unordered ephemeral tables

### 3. **Distributed Database Support**
Features for multi-database operations:
- Remote schema verification
- Database name checking
- Source list extraction

### 4. **Extended Type System**
Support for Comdb2-specific types:
- DATETIME for timestamps
- Special pseudo-columns

### 5. **Query Analysis**
Infrastructure for query introspection:
- Recording mode
- AST integration
- Table tracking

---

## Potential Issues and TODOs

### 1. SRT_Set Type Handling (Line 1575)
**Status**: Needs verification
**Issue**: Type conversion in set membership operations may be incorrect
**Risk**: Query results could be wrong for `IN` subqueries with type mismatches

### 2. Assert Removals
**Status**: Documented but not fully justified
**Issue**: Removing asserts may hide bugs
**Risk**: Logic errors might go undetected

### 3. Global Variables
**Status**: Used extensively
**Issue**: Global variables for optimization flags
**Risk**: Thread safety concerns, testing complexity

---

## Testing Recommendations

1. **Parallel Execution**: Test LIMIT/OFFSET with parallel queries
2. **Remote Tables**: Verify schema refresh on "no such column" errors
3. **Recording Mode**: Ensure proper cursor tracking
4. **Special Columns**: Test `comdb2_rowtimestamp` in various contexts
5. **Count Optimization**: Compare results with/without parallel counting
6. **Type System**: Test SRT_Set operations with type conversions

---

## Performance Implications

### Positive Impacts
- **Parallel counting**: Significant speedup for large tables
- **Unordered ephemeral**: Faster INTERSECT operations
- **Source list extraction**: Faster query preparation

### Potential Concerns
- **Extra register copies**: Small overhead for LIMIT/OFFSET in parallel mode
- **AST overhead**: Additional memory and processing for query trees
- **Recording mode**: Extra bookkeeping during execution

---

## Summary

The modifications to `select.c` extend SQLite with Comdb2-specific features while maintaining compatibility with standard SQLite. The changes primarily support:

1. **Distributed query execution** across multiple databases
2. **Parallel query processing** for better performance
3. **Query introspection** for debugging and optimization
4. **Extended data types** specific to Comdb2

All modifications are well-isolated using preprocessor directives and generally follow SQLite's coding patterns. The most critical features are the parallel execution support and remote database handling, which are core to Comdb2's distributed database architecture.
