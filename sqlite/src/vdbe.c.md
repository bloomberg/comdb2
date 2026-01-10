# vdbe.c Modifications for Comdb2

## Summary

This document analyzes the modifications made to SQLite's `vdbe.c` (Virtual Database Engine) for the Comdb2 project. The file contains approximately 289 conditional compilation blocks wrapped in `SQLITE_BUILDING_FOR_COMDB2` preprocessor directives. These modifications adapt SQLite's bytecode execution engine to work with Comdb2's distributed database architecture, custom data types, and storage layer.

**Key Areas of Modification:**
1. Custom data type support (datetime, interval, decimal types)
2. Raw/cooked data access optimization
3. Cursor and record handling for Comdb2's storage engine
4. Opcode extensions and custom operations
5. Transaction and locking integration
6. Logging and debugging infrastructure
7. Remote schema and distributed query support

---

## Modifications Grouped by Purpose/Feature

### 1. Custom Data Types and Affinity Handling

**Location:** Lines 438-516, 1925-2102, 2295-2349

**Modifications:**
- Added `SQLITE_AFF_DECIMAL` support for arbitrary precision decimal numbers
- Added `SQLITE_AFF_SMALL` support for single-precision floating-point (float vs double)
- Added `SQLITE_AFF_DATETIME` for datetime types with timezone and precision
- Added interval types: `SQLITE_AFF_INTV_YE`, `SQLITE_AFF_INTV_MO`, `SQLITE_AFF_INTV_DY`, `SQLITE_AFF_INTV_HO`, `SQLITE_AFF_INTV_MI`, `SQLITE_AFF_INTV_SE`
- Implemented type conversion functions: `sqlite3VdbeMemDecimalfy()`, `sqlite3VdbeMemDatetimefy()`, `sqlite3VdbeMemIntervalfy()`

**Purpose:**
Comdb2 extends SQLite's type system to support financial/scientific precision (decimal), datetime operations with timezone awareness, and interval arithmetic. These are essential for enterprise database applications that require:
- Exact decimal arithmetic for financial calculations
- Proper timezone handling for global applications
- Date/time interval operations (e.g., adding months to a date)

**Code Example:**
```c
if( affinity==SQLITE_AFF_DECIMAL ){
  sqlite3VdbeMemDecimalfy(pRec);
  return;
}
```

---

### 2. Arithmetic Operations with Custom Types

**Location:** Lines 1555-2102 (OP_Add, OP_Subtract, OP_Multiply, OP_Divide)

**Modifications:**
- Extended arithmetic opcodes to handle decimal, datetime, and interval types
- Implemented operations:
  - Datetime + Interval → Datetime
  - Datetime - Datetime → Interval
  - Interval + Interval → Interval
  - Interval * Integer → Interval
  - Decimal arithmetic with proper precision handling
- Added memory management for dynamically allocated results

**Purpose:**
Enables SQL expressions like:
- `SELECT order_date + INTERVAL '30 days' FROM orders`
- `SELECT end_time - start_time FROM events`
- `SELECT price * quantity` (with decimal precision)

**Why This Was Made:**
Standard SQLite only supports integer/real/text/blob types. Enterprise databases need:
- Date arithmetic for reporting and analytics
- Exact decimal math to avoid floating-point errors in financial data
- Type-safe operations that preserve precision

---

### 3. Raw vs Cooked Data Access Optimization

**Location:** Lines 44-50, 549-554, 2850-2894, 4323-4379

**Modifications:**
- Added `cur_is_raw()` macro to identify raw data cursors
- Implemented `setCookCol()` function to optimize field access
- Added `nCookFields` tracking in VdbeCursor
- Direct data access bypass for raw table/index cursors
- Integrated with Comdb2's `get_data()` and `get_datacopy()` functions

**Purpose:**
Comdb2 stores data in its own wire format ("raw") but SQLite expects a specific record format ("cooked"). This optimization:
- Bypasses unnecessary format conversions when possible
- Directly accesses Comdb2's native storage format
- Reduces CPU overhead for data retrieval

**Code Flow:**
```
OP_Column:
  if (cur_is_raw(cursor) && !nullRow) {
    // Direct access to Comdb2 storage format
    get_data(cursor, schema, data, column_index, output_mem)
  } else {
    // Fall through to standard SQLite cooked access
    goto cooked_access;
  }
```

**Why This Was Made:**
- Performance: Avoids double-conversion (Comdb2→SQLite→Comdb2)
- Memory efficiency: Reduces temporary buffer allocations
- Direct integration with Comdb2's storage layer

---

### 4. Rowid and GenId Handling

**Location:** Lines 69-107, 4019-6025, 5460-5566

**Modifications:**
- Added `getRowid()` helper function (replaces duplicate code in OP_Rowid/OP_IdxRowid)
- Extended P3 parameter to support different rowid formats:
  - P3=0: Integer rowid (standard SQLite)
  - P3=1: String format "rrn:genid" for R6 compatibility
  - P3=2: Timestamp extracted from genid
- Integration with `sqlite3BtreeGetGenId()` for distributed row identification

**Purpose:**
Comdb2 uses "genids" (generation IDs) for distributed row identification across cluster nodes:
- Globally unique row identifiers
- Include timestamp information for MVCC
- Backward compatibility with R6 (legacy version)

**Why This Was Made:**
In a distributed database:
- Standard SQLite rowids aren't unique across nodes
- Need globally unique identifiers for replication
- Genids encode both location and version information

---

### 5. Cursor Operations and Index Access

**Location:** Lines 3949-4294, 4699-4893

**Modifications:**
- Modified `OP_OpenRead`/`OP_OpenWrite` to pass VDBE pointer
- Added `OP_OpenRead_Record` opcode for recording operations
- Added cursor flags: `BTREE_CUR_RD`, `BTREE_CUR_WR`
- Implemented `setCookCol()` optimization during seeks
- Modified `OP_SeekGE`/`OP_SeekGT`/`OP_SeekLT`/`OP_SeekLE` for raw cursor handling
- Added field usage tracking: `sqlite3BtreeCursorSetFieldUsed()`

**Purpose:**
Adapts SQLite's cursor model to Comdb2's storage engine:
- Track which columns are actually accessed (projection pushdown)
- Optimize index scans for partial column access
- Support Comdb2's cursor classes (table, index, remote, sampled)

**Why This Was Made:**
- **Projection pushdown**: Only fetch columns that are needed
- **Network optimization**: In distributed mode, reduce data transfer
- **Performance**: Comdb2's storage layer can skip unused columns

---

### 6. Record Creation and MakeRecord Optimization

**Location:** Lines 2845-3054

**Modifications:**
- Added `OPFLAG_MKREC_COMDB2` flag for direct Comdb2 serialization
- Integration with `sqlite3MakeRecordForComdb2()`
- Added `MEM_Comdb2` flag for records in native format
- Bypass standard SQLite record encoding when possible

**Purpose:**
Optimize INSERT/UPDATE operations:
- Serialize directly to Comdb2's ondisk format
- Skip SQLite→Comdb2 conversion step
- Reduce memory allocations

**Code Flow:**
```
OP_MakeRecord:
  if (OPFLAG_MKREC_COMDB2) {
    sqlite3MakeRecordForComdb2(cursor, data, num_fields, &optimized)
    if (optimized) {
      // Data is already in Comdb2 format in the cursor
      mark_record_as_MEM_Comdb2
      break;
    }
  }
  // Standard SQLite record creation
```

---

### 7. Transaction and Write Operations

**Location:** Lines 3074-3082, 4633-4812

**Modifications:**
- Modified `OP_Transaction` to call `comdb2SetWriteFlag()`
- Added deferred moveto handling for table cursors on `OP_Delete`
- Extended `OP_Insert` to handle `MEM_Comdb2` records
- Pass `OPFLAG_ISUPDATE` flag to distinguish INSERT vs UPDATE
- Integration with UPDATE column tracking (`sqlite3CreateUpdCols`)

**Purpose:**
Integrate with Comdb2's transaction management:
- Track read vs write transactions
- Coordinate with distributed locking
- Optimize UPDATE operations (only update changed columns)

**Why This Was Made:**
Comdb2's transaction model differs from SQLite:
- Multi-version concurrency control (MVCC)
- Distributed transactions across cluster
- Need to track which columns are being updated

---

### 8. Verification and Constraint Checking

**Location:** Lines 5138-5251, 5318-5392

**Modifications:**
- Extended `OP_NotFound`/`OP_NotExists` with P5 parameter for early verification
- Added unique nulls handling: `comdb2_is_idx_uniqnulls()`
- Modified `OP_IfNoHope` to update `seekHit` for IN-clause optimization
- Pass opcode type to `sqlite3BtreeMovetoUnpacked()` for verification context

**Purpose:**
Enhanced constraint validation:
- Early verification for foreign key checks
- Support for unique indexes that allow multiple NULLs (SQL standard behavior)
- Optimize IN-clause searches with seekHit tracking

**Why This Was Made:**
- **SQL compliance**: SQLite's unique index treats NULL as a value; SQL standard allows multiple NULLs
- **Performance**: Early verification avoids unnecessary work
- **Distributed integrity**: Coordinate constraint checks across nodes

---

### 9. Seek and Scan Optimizations

**Location:** Lines 4893-5089

**Modifications:**
- Complete rewrite of `OP_SeekScan` (was `OP_SeekHit` in SQLite)
- Scan-ahead optimization: try up to P1 steps before falling through to full seek
- Modified `OP_SeekHit` to use range (P2 to P3) instead of boolean
- Added three-outcome logic: found/not-found/needs-full-seek

**Purpose:**
Optimize multi-column IN clauses:
- If cursor is already near target, step forward instead of seeking
- Reduces B-tree seek overhead for sequential access patterns
- Particularly effective for `WHERE (a,b) IN ((1,2), (1,3), (1,4))`

**Why This Was Made:**
Common query pattern in OLTP workloads:
- Batch operations on related records
- Range queries on composite indexes
- Can save significant I/O by stepping vs seeking

---

### 10. Logging and Debugging Infrastructure

**Location:** Lines 599-904, 683-752

**Modifications:**
- Replaced `printf()` with `logmsg(LOGMSG_USER, ...)` and `logmsgf()`
- Added `dump_sqlite_mem()` and `dump_vdbe_mem()` functions
- Global debug flag: `gbl_debug_sql_opcodes`
- Comdb2-specific datetime/interval display in trace output
- Added thread ID to opcode tracing

**Purpose:**
Production debugging in multi-threaded environment:
- Thread-safe logging (printf is not thread-safe)
- Route logs to Comdb2's logging infrastructure
- Display Comdb2-specific types in traces
- Enable/disable debug output at runtime

**Why This Was Made:**
- **Production safety**: Can't use printf in production
- **Observability**: Need to debug live systems
- **Integration**: Logs must go through central logging system

---

### 11. Virtual Table and System Table Access

**Location:** Lines 8044-8104

**Modifications:**
- Added access control: `comdb2_check_vtab_access()`
- Schema lock management for system tables
- Release view locks after opening `comdb2_tables` virtual table
- Track lock count with `p->crtPartitionLocks`

**Purpose:**
Secure access to system catalogs:
- Permission checks before opening system tables
- Prevent deadlocks during schema introspection
- Coordinate with Comdb2's schema change protocol

**Why This Was Made:**
Comdb2's system tables expose cluster state:
- Need access control for security
- Schema locks prevent concurrent DDL
- Virtual tables can trigger remote queries

---

### 12. Custom Opcodes (OpFunc Series)

**Location:** Lines 8162-8273

**Modifications:**
- New opcodes: `OP_OpFuncLoad`, `OP_OpFuncExec`, `OP_OpFuncInteger`, `OP_OpFuncReal`, `OP_OpFuncString`, `OP_OpFuncNext`
- Support for custom function execution within VDBE
- Buffer-based result iteration

**Purpose:**
Execute custom Comdb2 functions that return multiple rows/values:
- Stored procedures
- System functions that generate data
- Integration with Comdb2's Lua scripting

**Code Flow:**
```
OP_OpFuncLoad:   Load function pointer into register
OP_OpFuncExec:   Execute the function
Loop:
  OP_OpFuncInteger/Real/String: Fetch next value from buffer
  OP_OpFuncNext:  Check if more results exist
  (jump back to Loop if more data)
```

---

### 13. Error Handling and Return Codes

**Location:** Lines 7852-8330

**Modifications:**
- Preserve Comdb2-specific error codes through abort path
- Added error codes:
  - `SQLITE_DEADLOCK`
  - `SQLITE_TIMEDOUT`
  - `SQLITE_COST_TOO_HIGH`
  - `SQLITE_NO_TEMPTABLES`
  - `SQLITE_TRAN_CANCELLED`
  - `SQLITE_SCHEMA_REMOTE`
  - And 10+ more
- Don't clobber specific errors with generic `SQLITE_ERROR`

**Purpose:**
Provide detailed error information for distributed operations:
- Distinguish between different failure modes
- Enable proper error handling in applications
- Support query cost limits and resource constraints

**Why This Was Made:**
Standard SQLite error codes don't cover:
- Network failures in distributed queries
- Query cost limits (governor)
- Cross-node schema mismatches
- Distributed deadlock detection

---

### 14. FinishSeek Opcode

**Location:** Lines 6009-6025

**Modifications:**
- New opcode `OP_FinishSeek` to complete deferred seeks
- Ensures cursor is positioned before subsequent operations

**Purpose:**
Explicit control over deferred seek completion:
- Coordinate with Comdb2's cursor positioning
- Ensure data is available before access
- Support for lazy cursor movement

---

### 15. Memory and Resource Management

**Location:** Lines 1595-1602, 2712-2713

**Modifications:**
- Explicit memory freeing in arithmetic operations
- Clear `szMalloc` and `zMalloc` to prevent leaks
- Set `db` pointer after operations that might wipe it
- Timezone and precision tracking in Mem structures

**Purpose:**
Prevent memory leaks with Comdb2's type system:
- Datetime/interval allocate dynamic memory
- Decimal types have variable-length storage
- Need to free before reusing Mem objects

---

### 16. Limit Handling

**Location:** Lines 1580-1582

**Modifications:**
- Added `comdb2_handle_limit()` call in `OP_OffsetLimit`
- Integration with query governor

**Purpose:**
Enforce query result limits:
- Coordinate with Comdb2's resource governor
- Track rows returned
- Abort queries that exceed limits

---

### 17. Remote Query Support

**Location:** Various, includes schema change codes

**Modifications:**
- Error codes for remote schema operations
- `cur_is_remote()` cursor detection
- Schema push operations: `SQLITE_SCHEMA_PUSH_REMOTE`, `SQLITE_SCHEMA_PUSH_REMOTE_WRITE`

**Purpose:**
Support distributed query execution:
- Route operations to remote nodes
- Detect schema mismatches across cluster
- Coordinate cross-node operations

---

## Why These Modifications Were Made

### Architecture Differences

**SQLite:**
- Embedded, single-process database
- Local file storage
- Simple type system (5 types)
- No distributed transactions

**Comdb2:**
- Clustered, multi-node database
- Custom storage engine with replication
- Rich type system (datetime, interval, decimal)
- Distributed MVCC and consensus

### Key Motivations

1. **Type System Extensions**
   - Financial applications need exact decimal arithmetic
   - Enterprise apps need proper datetime/timezone handling
   - SQL standard compliance (intervals)

2. **Performance Optimizations**
   - Avoid format conversions between SQLite and Comdb2
   - Direct access to native storage format
   - Projection pushdown and column pruning
   - Seek-scan optimization for bulk operations

3. **Distributed Database Features**
   - Globally unique row identifiers (genids)
   - Remote query execution
   - Cross-node constraint validation
   - Distributed deadlock detection

4. **Production Operations**
   - Thread-safe logging
   - Runtime debugging controls
   - Resource governance (query limits)
   - Security (access control)

5. **Schema Management**
   - Online schema changes
   - Schema versioning
   - Backward compatibility (R6)

6. **SQL Compatibility**
   - Standard datetime arithmetic
   - Interval types
   - Unique indexes with multiple NULLs
   - Decimal precision

### Integration Philosophy

The modifications follow a pattern of **minimal invasiveness**:
- Most changes are wrapped in `#if defined(SQLITE_BUILDING_FOR_COMDB2)` blocks
- Preserve SQLite semantics where possible
- Extend rather than replace functionality
- Enable/disable optimizations based on cursor type

This allows Comdb2 to:
- Track upstream SQLite releases
- Merge security fixes easily
- Contribute optimizations back to SQLite
- Maintain a clean separation of concerns

---

## Summary Statistics

- **Total conditional blocks:** ~289
- **New opcodes added:** 7 (OpFunc series + OP_FinishSeek + OP_IfNotOpen + OP_OpenRead_Record)
- **Modified opcodes:** 40+ (all major cursor and data operations)
- **New data types:** 9+ (decimal, datetime, 6 interval types, small float)
- **New error codes:** 15+
- **Helper functions added:** 10+ (getRowid, setCookCol, dump functions, etc.)

---

## Conclusion

The modifications to `vdbe.c` transform SQLite from a single-process embedded database into the execution engine for a distributed, enterprise-grade database system. The changes maintain SQLite's core architecture while adding:

1. **Rich type system** for enterprise applications
2. **Performance optimizations** for Comdb2's storage layer
3. **Distributed operation support** for cluster coordination
4. **Production-grade tooling** for debugging and operations
5. **SQL standard compliance** for datetime and decimal operations

The design preserves SQLite's bytecode-based execution model while seamlessly integrating with Comdb2's storage engine, transaction manager, and distributed query processing. This allows Comdb2 to leverage SQLite's mature query optimizer while providing enterprise database features expected in OLTP workloads.
