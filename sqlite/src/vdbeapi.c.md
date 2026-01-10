# SQLite VDBE API Modifications for Comdb2

This document describes the modifications made to SQLite's `vdbeapi.c` file for integration with the Comdb2 database system.

## Summary of All Modifications

The modifications to `vdbeapi.c` add Comdb2-specific functionality while maintaining SQLite compatibility. All changes are wrapped in `#if defined(SQLITE_BUILDING_FOR_COMDB2)` preprocessor directives. The key areas modified include:

1. **Column name caching** - Caching and validation of column names and types
2. **Virtual table locks** - Managing locks for virtual tables
3. **Extended data types** - Support for datetime, interval, and decimal types
4. **Remote database detection** - Detecting queries involving remote databases
5. **Timezone and precision handling** - Statement-level timezone and datetime precision
6. **Schema change handling** - Custom schema change error codes
7. **SQL normalization** - Alternative normalization strategy

## VDBE API Changes

### 1. Column Name and Metadata Management

#### Added Functions

**`stmt_column_name(sqlite3_stmt *pStmt, int index)`**
- Retrieves the column name at the specified index from a statement
- Uses the VDBE's `aColName` array
- Returns NULL and logs error if statement handle is invalid

**`stmt_cached_column_name(sqlite3_stmt *pStmt, int index)`**
- Retrieves cached column names from `oldColNames` field
- Used for performance optimization to avoid recomputing column names
- Returns cached string directly

**`stmt_column_decltype(sqlite3_stmt *pStmt, int index)`**
- Retrieves the declared type of a column
- Defaults to "text" if no type is declared
- Uses `COLNAME_DECLTYPE` offset calculation

**`stmt_cached_column_decltype(sqlite3_stmt *pStmt, int index)`**
- Retrieves cached column declaration types
- Accesses `oldColDeclTypes` array

**`stmt_cached_column_count(sqlite3_stmt *pStmt)`**
- Returns the cached column count from `oldColCount`
- Returns 0 if statement is NULL

**`stmt_set_cached_columns(sqlite3_stmt *pStmt, char **column_names, char **column_decltypes, int column_count)`**
- Sets cached column metadata
- Frees existing cached data before setting new values
- Used for statement caching/reuse optimization

#### Column Matching Functions

**`stmt_do_column_names_match(sqlite3_stmt *pStmt)`**
- Compares current column names with cached names
- Returns 1 if they match, 0 if different
- Controlled by `gbl_old_column_names` global flag
- Ensures schema compatibility when reusing prepared statements

**`stmt_do_column_decltypes_match(sqlite3_stmt *pStmt)`**
- Compares current column types with cached types
- Returns 1 if they match, 0 if different
- Validates type compatibility for cached statements

**Why these were added:**
- Comdb2 needs to cache and reuse prepared statements across multiple executions
- Column metadata caching improves performance by avoiding repeated metadata lookups
- Schema validation ensures cached statements remain valid after schema changes
- The `gbl_old_column_names` flag allows toggling between cached and dynamic metadata

### 2. Virtual Table Management

**`stmt_free_vtable_locks(sqlite3_stmt *pStmt)`**
- Frees virtual table lock resources
- Clears `vTableLocks` array and resets counts
- Called during statement finalization

**`stmt_set_vlock_tables(sqlite3_stmt *pStmt, char **vTableLocks, int numVTableLocks, int hasVTables, int flags)`**
- Sets virtual table lock information on a statement
- Frees existing locks before setting new ones
- Stores lock count, lock array, and flags

**`stmt_set_has_scalar_func(sqlite3_stmt *pStmt, int hasScalarFunc)`**
- Records whether the statement contains scalar functions
- Used for query optimization and execution planning

**Why these were added:**
- Comdb2 has a distributed architecture requiring explicit virtual table locking
- Virtual tables may reference remote databases that need coordination
- Lock management prevents race conditions and ensures data consistency

### 3. Extended Data Type Support

#### Datetime Support

**`sqlite3_value_datetime(sqlite3_value *pVal)`**
- Extracts datetime value with timezone information
- Converts value to datetime if not already in that format
- Uses `MEM_Datetime` flag and `du.dt` structure
- Returns pointer to `dttz_t` structure

**`sqlite3_column_datetime(sqlite3_stmt *pStmt, int i)`**
- Column accessor for datetime values
- Wraps `sqlite3_value_datetime` for result set access

**`sqlite3_bind_datetime(sqlite3_stmt *pStmt, int i, dttz_t *dt, char *tz)`**
- Binds datetime parameter with timezone
- Sets `MEM_Datetime` flag and stores timezone string

**`sqlite3_result_datetime(sqlite3_context *pCtx, dttz_t *dt, const char *tz)`**
- Sets function result to datetime value
- Used in user-defined functions

#### Interval Support

**`sqlite3_value_interval(sqlite3_value *pVal, int type)`**
- Extracts interval value (year-month, day-second, or decimal)
- Handles conversion for different interval types:
  - `INTV_YM_TYPE` - Year-Month intervals
  - `INTV_DS_TYPE` - Day-Second intervals
  - `INTV_DSUS_TYPE` - Day-Second with microseconds
  - `SQLITE_DECIMAL` - Decimal intervals

**`sqlite3_column_interval(sqlite3_stmt *pStmt, int i, int type)`**
- Column accessor for interval values

**`sqlite3_bind_interval(sqlite3_stmt *pStmt, int i, intv_t *it)`**
- Binds interval parameter
- Sets `MEM_Interval` flag

**`sqlite3_result_interval(sqlite3_context *pCtx, intv_t *pValue)`**
- Sets function result to interval value

**`sqlite3_result_decimal(sqlite3_context *pCtx, decQuad *dec)`**
- Sets function result to decimal value

#### Type Detection Enhancement

Modified `sqlite3_value_type()` to detect Comdb2-specific types:
- Returns `SQLITE_INTERVAL_YM`, `SQLITE_INTERVAL_DS`, `SQLITE_INTERVAL_DSUS`, or `SQLITE_DECIMAL` for interval types
- Returns `SQLITE_DATETIMEUS` or `SQLITE_DATETIME` based on precision
- Returns `SQLITE_NEXTSEQ` for master sequence values

**Why these were added:**
- Comdb2 extends SQLite with business-oriented temporal types
- Datetime with timezone support is critical for distributed databases across time zones
- Interval types enable temporal arithmetic (e.g., "add 3 months")
- These types map to Comdb2's native CSON protocol

### 4. Statement Inspection Functions

**`sqlite3_hasResultSet(sqlite3_stmt *pStmt)`**
- Checks if statement has a result set
- Returns 1 if `pResultSet` is not NULL

**`sqlite3_hasNColumns(sqlite3_stmt *pStmt, int n)`**
- Checks if result set has exactly n columns
- Used for result validation

**`sqlite3_isColumnNullType(sqlite3_stmt *pStmt, int i)`**
- Checks if specific column is NULL
- Uses bit flag check (`flags & 0x1`)

**Why these were added:**
- Comdb2's stored procedures and triggers need to introspect statement results
- Fast NULL checking optimizes conditional logic in procedures
- Column count validation ensures API contract compliance

### 5. Timezone and Precision Management

**`sqlite3_resetclock(sqlite3_stmt *pStmt)`**
- Resets the statement's time specification
- Calls internal `sqlite3VdbeResetClock()`
- Used to update statement timestamps

**`stmt_tzname(sqlite3_stmt *pStmt)`**
- Returns the timezone name associated with the statement
- Accesses VDBE's `tzname` field

**`stmt_set_dtprec(sqlite3_stmt *pStmt, int precision)`**
- Sets datetime precision for the statement
- Allows caller (e.g., Lua) to control precision
- Stores in VDBE's `dtprec` field

**Modified `columnMem()` function:**
- Associates timezone (`pOut->tz = pVm->tzname`)
- Associates datetime precision (`pOut->dtprec = pVm->dtprec`)
- Associates database handle for error reporting (`pOut->db`)

**Why these were added:**
- Comdb2 supports per-connection timezone settings
- Datetime precision (second vs. microsecond) affects storage and display
- Timezone context must flow through result columns for correct formatting
- Database handle association enables better error messages

### 6. Remote Database Detection

**`sqlite3_stmt_has_remotes(sqlite3_stmt *pStmt)`**
- Detects if statement references remote databases
- Checks `btreeMask` to identify non-local databases
- Default databases are "main" (index 0) and "temp" (index 1)
- Any database with index >= 4 (mask bit 2) is considered remote
- Handles both array and scalar `btreeMask` based on `SQLITE_MAX_ATTACHED`

**Why this was added:**
- Comdb2 is a distributed database system
- Queries spanning multiple nodes require different execution strategies
- Transaction coordination differs for local vs. remote queries
- Enables query routing and optimization decisions

### 7. Statement Lifecycle Modifications

**Modified `sqlite3_finalize()`:**
- Frees cached column names if `gbl_old_column_names` is enabled
- Calls `stmt_free_column_names()`
- Frees virtual table locks via `stmt_free_vtable_locks()`

**Modified `columnName()` function:**
- Returns cached column names/types when available
- Checks `gbl_old_column_names` flag
- Falls back to standard SQLite behavior if caching disabled
- Only applies to UTF-8 names (not UTF-16)
- Handles both `COLNAME_NAME` and `COLNAME_DECLTYPE`

**Why these were modified:**
- Proper resource cleanup prevents memory leaks
- Cached metadata improves performance for repeated executions
- Conditional caching allows backward compatibility

## Statement Execution Modifications

### 1. Schema Change Handling

**Modified `sqlite3Step()` function:**
```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
  if( rc==SQLITE_COMDB2SCHEMA ) return rc;
#endif
```
- Returns immediately on `SQLITE_COMDB2SCHEMA` error
- Skips profile callback for Comdb2 schema errors
- Prevents standard SQLite schema retry logic

**Modified `sqlite3_step()` function:**
```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
    if( rc==SQLITE_COMDB2SCHEMA ){
      sqlite3_mutex_leave(db->mutex);
      return rc;
    }
#endif
```
- Exits schema retry loop on `SQLITE_COMDB2SCHEMA`
- Allows Comdb2-specific schema change handling

**Removed assertion:**
```c
#if !defined(SQLITE_BUILDING_FOR_COMDB2)
  assert( rc==SQLITE_ROW  || rc==SQLITE_DONE   || rc==SQLITE_ERROR
       || (rc&0xff)==SQLITE_BUSY || rc==SQLITE_MISUSE
  );
#endif
```
- Comdb2 introduces additional return codes
- Standard assertion would fail for Comdb2-specific codes

**Why these were modified:**
- Comdb2 uses distributed schema management
- Schema changes may originate from other nodes
- `SQLITE_COMDB2SCHEMA` signals need to propagate immediately
- Custom retry logic handles distributed schema synchronization

### 2. Memory Cell Initialization

**Modified `columnNullValue()` static structure:**
```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
    /* .du         = */ {{0}},
    /* .tz         = */ (char*)0,
    /* .dtprec     = */ (int)0,
    /* .flags      = */ (u32)MEM_Null,
#else
    /* .flags      = */ (u16)MEM_Null,
#endif
```
- Initializes Comdb2-specific Mem fields
- `du` - datetime/interval union
- `tz` - timezone string
- `dtprec` - datetime precision
- `flags` becomes `u32` instead of `u16` (expanded for new type flags)

**Why this was modified:**
- Extended Mem structure requires proper initialization
- Prevents undefined behavior when accessing Comdb2 fields
- Ensures NULL values have consistent representation

### 3. Zero Blob Handling

**Modified `sqlite3_result_zeroblob64()` and `sqlite3_bind_zeroblob()`:**
```c
#ifndef SQLITE_OMIT_INCRBLOB
  sqlite3VdbeMemSetZeroBlob(pCtx->pOut, (int)n);
  return SQLITE_OK;
#else
  return sqlite3VdbeMemSetZeroBlob(pCtx->pOut, (int)n);
#endif
```

**Why this was modified:**
- Comdb2 may be built with `SQLITE_OMIT_INCRBLOB`
- Conditional compilation ensures proper error handling
- Zero blobs still work even without incremental blob I/O

### 4. SQL Normalization

**Modified `sqlite3_normalized_sql()` function:**
```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
    if( gbl_alternate_normalize ){
      p->zNormSql = sqlite3Normalize_alternate(p, p->zSql, 0);
    }else{
      p->zNormSql = sqlite3Normalize(p, p->zSql, 0);
    }
#else
    p->zNormSql = sqlite3Normalize(p, p->zSql);
#endif
```
- Introduces alternate normalization function
- Controlled by `gbl_alternate_normalize` global flag (defaults to 1)
- Allows Comdb2-specific SQL normalization strategies

**Why this was modified:**
- Comdb2 has different SQL fingerprinting requirements
- Statement caching and monitoring need consistent normalization
- Alternate normalization may handle Comdb2 extensions differently

## Rationale for Modifications

### Performance Optimization
- **Column name caching** reduces repeated metadata lookups
- **Cached column validation** enables statement reuse
- **Fast NULL checks** optimize conditional logic in procedures

### Distributed Database Support
- **Remote database detection** enables query routing
- **Virtual table locks** coordinate distributed resources
- **Schema change handling** manages distributed schema synchronization

### Extended Type System
- **Datetime/interval types** support business logic requirements
- **Timezone support** handles global distributed deployments
- **Precision control** balances storage and accuracy needs

### Integration Requirements
- **Global configuration flags** allow runtime behavior changes
- **Logging integration** uses Comdb2's `logmsg()` facility
- **Error handling** propagates Comdb2-specific error codes

### Memory Management
- **Proper cleanup** in finalization prevents leaks
- **Lock management** ensures resource release
- **Extended Mem initialization** prevents undefined behavior

### API Compatibility
- All modifications are conditionally compiled
- Standard SQLite behavior preserved when building without Comdb2
- Minimal changes to core execution paths
- Extensions follow SQLite API patterns

## Design Patterns Used

1. **Wrapper Functions**: Many new functions wrap VDBE internals for safer access
2. **Global Flags**: Runtime configuration via `gbl_*` variables
3. **Conditional Compilation**: `#if defined(SQLITE_BUILDING_FOR_COMDB2)` isolates changes
4. **Resource Management**: Paired allocation/deallocation functions
5. **Error Logging**: Consistent error reporting via `logmsg()`
6. **Defensive Programming**: NULL checks and validation throughout

## Impact on SQLite Core

The modifications maintain SQLite's architecture while extending functionality:

- **No breaking changes** to standard SQLite API
- **Minimal performance overhead** when features not used
- **Isolated modifications** don't affect core algorithms
- **Compatible evolution** allows future SQLite upgrades
- **Clear separation** via preprocessor directives

These modifications transform SQLite into a capable SQL execution engine for Comdb2's distributed database system while preserving the option to build standard SQLite.
