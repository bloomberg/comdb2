# Comdb2 Modifications to SQLite build.c

## Overview

This document analyzes the modifications made to SQLite's `build.c` file for the comdb2 project. The modifications are wrapped in `SQLITE_BUILDING_FOR_COMDB2` conditional compilation blocks and primarily focus on schema management, distributed database support, DDL operations, and transaction handling.

## Summary of All Modifications

The comdb2 modifications to build.c can be categorized into the following areas:

### 1. **Foreign Database (FDB) Support**
- Dynamic table discovery and attachment from remote databases
- Table alias resolution for distributed queries
- Foreign database connection tracking and validation
- Schema synchronization across distributed databases

### 2. **Enhanced Schema Management**
- Custom schema reset functionality by name
- View creation/deletion hooks for comdb2
- Table version tracking
- CSC2 schema format support

### 3. **Transaction Management**
- Write transaction enforcement for DDL operations
- Transaction mode detection (parallel, remote push, remote write)
- DRYRUN mode support for certain operations

### 4. **Index Enhancements**
- Expression index tracking via `hasExprIdx` flag
- Partial index tracking via `hasPartIdx` flag
- Index order description for query planning
- Skip-scan index control for remote databases
- Unique index detection through comdb2 callbacks

### 5. **Data Type Extensions**
- DateTime types (`SQLITE_AFF_DATETIME`, `SQLITE_AFF_DATETIMEUS`)
- Interval types (year, month, day, hour, minute, second)
- Decimal type (`SQLITE_AFF_DECIMAL`)
- Small integer type (`SQLITE_AFF_SMALL`)

### 6. **Cursor Management**
- Recording cursor tracking for selectv operations
- Cursor limits enforcement (`MAX_CURSOR_IDS`)

---

## DDL (CREATE/ALTER/DROP) Related Changes

### CREATE TABLE Modifications

**Location**: `sqlite3StartTable()` and `sqlite3EndTable()`

**Changes**:
1. **Table Flags Initialization**:
   ```c
   pTable->hasPartIdx = 0;
   pTable->hasExprIdx = 0;
   ```
   - Tracks whether table has partial indexes or expression indexes
   - Used for query optimization decisions

2. **View Creation Hook**:
   ```c
   if (p->pSelect && db->isTimepartView == 0 && iDb != 1) {
     comdb2_create_view(pParse, pParse->sNameToken.z,
                        pParse->sNameToken.n, zStmt, 0);
   }
   ```
   - Delegates view creation to comdb2-specific handler
   - Excludes time partition views and temp tables

3. **CSC2 Schema Integration**:
   ```c
   if( iDb==1 ){
     // temp tables have no csc2
     sqlite3NestedParse(pParse, "UPDATE %Q.%s SET ... sql=%Q ...");
   } else {
     // Add csc2 for comdb2
     sqlite3NestedParse(pParse, "UPDATE %Q.%s SET ... sql=%Q, csc2=NULL ...");
   }
   ```
   - Adds CSC2 column to sqlite_master for non-temp tables
   - CSC2 is comdb2's native schema format

4. **Table Version Tracking**:
   ```c
   struct dbtable *dbtable = get_dbtable_by_name(p->zName);
   if (dbtable) {
     p->version = (int)dbtable->tableversion;
   }
   ```
   - Stores schema version from comdb2's internal structures
   - Used for distributed schema consistency checks

5. **Database Index Assignment**:
   ```c
   p->iDb = iDb;
   ```
   - Explicitly sets which database contains the table
   - Critical for sqlite_stat* tables and foreign tables

**Why**: Comdb2 needs to integrate its CSC2 schema format with SQLite's schema system and track additional metadata for distributed database features and query optimization.

---

### CREATE VIEW Modifications

**Location**: `sqlite3CreateView()`

**Changes**:
1. **DRYRUN Mode Check**:
   ```c
   if(comdb2IsDryrun(pParse)){
     sqlite3ErrorMsg(pParse, "DRYRUN not supported for this operation");
     goto create_view_fail;
   }
   ```
   - Prevents DRYRUN mode for view creation
   - DRYRUN is comdb2's validation-only mode

**Why**: Views involve complex metadata changes that cannot be safely simulated in DRYRUN mode without risking inconsistent state.

---

### DROP TABLE/VIEW Modifications

**Location**: `sqlite3DropTable()`

**Changes**:
1. **Write Transaction Enforcement**:
   ```c
   comdb2WriteTransaction(pParse);
   ```
   - Ensures DDL operations run in write mode
   - Critical for distributed transaction consistency

2. **View Handling**:
   ```c
   if( isView && db->isTimepartView==0 ){
     comdb2_drop_view(pParse, pName);
   } else if( isView || (iDb==1 && !bDropTable) ) {
     // Standard SQLite view/temp table drop
   } else {
     // Comdb2 table drop
     comdb2DropTable(pParse, pName);
   }
   ```
   - Routes view drops to comdb2-specific handler
   - Time partition views bypass normal view drop logic
   - Regular table drops go through comdb2's table drop mechanism

3. **Time Partition View Exception**:
   ```c
   if( !isView && pTab->pSelect ){
     if (timepart_allow_drop(pTab->zName)) {
       // Allow drop even though it looks like a view
     }
   }
   ```
   - Time partition views appear as tables with SELECT but can be dropped with DROP TABLE
   - Special handling for comdb2's time partitioning feature

**Why**: Comdb2 manages views and tables differently than vanilla SQLite, requiring custom drop logic that coordinates with the distributed system's schema management.

---

### CREATE INDEX Modifications

**Location**: `sqlite3CreateIndex()`

**Changes**:
1. **Expression Index Tracking**:
   ```c
   if( pCExpr->op!=TK_COLUMN ){
     pTab->hasExprIdx = 1;
     pIndex->bHasExpr = 1;
   }

   if (is_comdb2_index_expression(pTab->zName))
     pTab->hasExprIdx = 1;
   ```
   - Marks tables that have indexes on expressions (not just columns)
   - Used to determine if special index handling is needed

2. **Partial Index Tracking**:
   ```c
   if( pPIWhere ){
     pIndex->pTable->hasPartIdx = 1;
   }
   ```
   - Tracks partial indexes (indexes with WHERE clause)
   - Affects query planning decisions

3. **Remote Index Limitations**:
   ```c
   if( db->init.iDb>1 ){
     // remote indexes don't use skip-scan indexes
     pIndex->noSkipScan = 1;
   } else {
     pIndex->noSkipScan = 0;
   }
   ```
   - Disables skip-scan optimization for remote database indexes
   - Skip-scan may not work correctly across network boundaries

4. **CSC2 Integration**:
   ```c
   sqlite3NestedParse(pParse,
     "INSERT INTO %Q.%s VALUES('index',%Q,%Q,#%d,%Q,NULL);",
     db->aDb[iDb].zDbSName, MASTER_NAME, ...);
   ```
   - Adds NULL CSC2 column when creating index metadata
   - Keeps schema table structure consistent

**Why**: Comdb2 needs to track index characteristics that affect distributed query optimization and ensure remote indexes don't use optimizations that may fail across network boundaries.

---

### DROP INDEX Modifications

**Location**: `sqlite3DropIndex()`

**Changes**:
1. **Write Transaction Enforcement**:
   ```c
   comdb2WriteTransaction(pParse);
   ```
   - Same pattern as DROP TABLE
   - Ensures proper distributed transaction handling

**Why**: Consistent transaction handling for all DDL operations in distributed environment.

---

## Schema Handling Changes

### Foreign Database Table Discovery

**Location**: `sqlite3FindTable_int()`

**Changes**: Complete rewrite of table lookup logic with the following flow:

1. **Database Name Parsing**:
   ```c
   dbName = fdb_parse_comdb2_remote_dbname(zDatabase, &fqDbname);
   ```
   - Parses database names that may include remote database specifications
   - Supports formats like `LOCAL_dbname`, `tier_dbname`, or `dbname`

2. **Alias Resolution**:
   ```c
   if( !zDatabase && !db->init.busy && !skipAlias ){
     dbAlias = fdb_get_alias(&zName);
     zDatabase = dbAlias;
     if( zDatabase ){
       fdb_disable_push();  // Aliases are local
       goto retry_alias;
     }
   }
   ```
   - Checks if table name is actually an alias for a remote table
   - Prevents using push-remote optimization for aliased tables
   - Retries lookup with resolved database name

3. **Remote Table Discovery**:
   ```c
   if( !already_searched_fdb && (db->flags & SQLITE_PrepareOnly)==0 ){
     rc = sqlite3AddAndLockTable(db, fqDbname, zName, &version, ...);
     if( !rc ){
       rc = comdb2_dynamic_attach(db, NULL, 0, NULL, uri,
                                  dbName, &zErrDyn, version, ...);
       sqlite3UnlockTable(dbName, zName);
     }
   }
   ```
   - Contacts remote database to check if table exists
   - Locks table to get consistent schema version
   - Dynamically attaches remote table to local schema
   - Unlocks table after schema is loaded
   - Sets `already_searched_fdb` flag to prevent retry loops

4. **Class/Tier Validation**:
   ```c
   if( unlikely(zDatabase) && !db->init.busy ){
     if( fdb_validate_existing_table(zDatabase) ){
       logmsg(LOGMSG_USER, "Remote db table exists and class mismatches");
       p = NULL;
     }
   }
   ```
   - Validates that remote table's tier/class matches expectations
   - Prevents accessing wrong database in multi-tier environments

5. **Additional Lookup Variants**:
   - `sqlite3FindTableCheckOnly()`: Lookup without creating remote tables
   - `sqlite3FindTableCheckOnlyNoAlias()`: Skip alias resolution
   - `sqlite3FindTableByAnalysisLoad()`: Special handling for statistics loading

**Why**: Comdb2 needs to transparently discover and access tables across multiple database instances, requiring complex lookup logic that can handle remote databases, aliases, schema versioning, and tier-based routing.

---

### Schema Reset Enhancements

**Location**: `sqlite3ResetOneSchemaByName()`

**Changes**: New function added:
```c
void sqlite3ResetOneSchemaByName(sqlite3 *db, const char *zName,
                                  const char *zDatabase){
  // Find database by name (not index)
  for(i=0; i<db->nDb; i++){
    Db *pDb = &db->aDb[i];
    if( len == len2 && strncmp(zDatabase, pDb->zDbSName, len) == 0 ){
      if(pDb->pSchema ){
        sqlite3SchemaClearByName(pDb->pSchema, zName);
      }
      return;
    }
  }
}
```

**Why**: Standard SQLite resets schema by database index, but comdb2 needs to reset by name to handle dynamically attached databases where indices may change.

---

### View Management

**Location**: `sqlite3PredicatedClearViews()`

**Changes**: New function added:
```c
int sqlite3PredicatedClearViews(
  sqlite3 *db,
  int (*predicated_delete)(const char *name, sqlite3 *db, void *arg),
  void *arg
){
  // Iterate views
  for( k=sqliteHashFirst(&pDb->pSchema->tblHash); k; k=sqliteHashNext(k) ){
    pTab = (Table*)sqliteHashData(k);
    if( pTab->pSelect ){
      if (!get_view_by_name(pTab->zName)) {  // Skip user views
        if( (*predicated_delete)(pTab->zName, db, arg) ){
          goto restart;  // View was deleted, restart iteration
        }
      }
    }
  }
}
```

**Why**: Time partition views need to be cleaned up based on custom predicates (e.g., expired partitions). This allows comdb2 to selectively remove system-generated views while preserving user-created ones.

---

### Foreign Database Schema Management

**Location**: `sqlite3ResetFdbSchemas()`

**Changes**: New function added:
```c
void sqlite3ResetFdbSchemas(sqlite3 *db){
  sqlite3_mutex_enter(sqlite3_db_mutex(db));
  for( i=2; i<db->nDb; i++ ){  // Skip main (0) and temp (1)
    comdb2_dynamic_detach(db, i);
  }
  sqlite3_mutex_leave(sqlite3_db_mutex(db));
}
```

**Why**: When schema changes occur on remote databases, all cached foreign database schemas must be cleared and reloaded. This prevents stale schema information from causing errors.

---

## Why Modifications Were Likely Made

### 1. **Distributed Database Architecture**
Comdb2 is a distributed OLTP database that needs to:
- Discover tables across multiple database instances
- Route queries to appropriate database nodes based on tier/class
- Maintain schema consistency across distributed nodes
- Handle schema versions to detect incompatibilities

**Relevant Changes**:
- Foreign database table discovery in `sqlite3FindTable_int()`
- Table alias support for cross-database references
- Class/tier validation in table lookup
- Schema version tracking in `sqlite3EndTable()`

### 2. **CSC2 Schema Format Integration**
Comdb2 uses CSC2 (Comdb2 Schema Definition) format alongside SQLite's DDL:
- CSC2 provides richer type system and constraints
- Needs to be stored in sqlite_master for persistence
- Must be synchronized with SQLite schema

**Relevant Changes**:
- Adding CSC2 column to sqlite_master updates
- Setting `csc2=NULL` as placeholder
- Tracking table versions from CSC2 definitions

### 3. **Transaction Semantics**
Comdb2 has different transaction requirements:
- DDL operations must run in write transactions
- Need to detect and handle different transaction modes
- Support for DRYRUN validation mode
- Support for parallel and remote query execution

**Relevant Changes**:
- `comdb2WriteTransaction()` calls in DDL operations
- Transaction mode detection in `sqlite3FinishCoding()`
- DRYRUN checks in DDL operations
- Cookie mask and write mask modifications

### 4. **Query Optimization for Distributed Queries**
Remote queries have different performance characteristics:
- Skip-scan optimization may not work well over network
- Expression and partial indexes need special handling
- Need index order description for query planning

**Relevant Changes**:
- Disabling skip-scan for remote indexes
- Tracking expression and partial indexes
- `sqlite3DescribeIndexOrder()` function for query planning
- Unique index detection through callbacks

### 5. **Extended Data Types**
Comdb2 supports types beyond standard SQLite:
- DateTime types with precision
- Interval types (year/month/day/hour/minute/second)
- Decimal type for financial applications
- Small integer type

**Relevant Changes**:
- Extended affinity types in `sqlite3AffinityType()`
- Type name mappings in `createTableStmt()`
- Type serialization in `getIndexCond()`

### 6. **Time Partitioning**
Comdb2 supports automatic time-based table partitioning:
- Creates views over partitioned tables
- Needs special handling for partition drops
- Requires partition view cleanup

**Relevant Changes**:
- Time partition view handling in `sqlite3DropTable()`
- `timepart_allow_drop()` checks
- `sqlite3PredicatedClearViews()` for partition cleanup
- `isTimepartView` flag checks

### 7. **Selectv (Recording Cursor) Support**
Comdb2 has a "selectv" feature for versioned reads:
- Requires tracking which cursors are recording
- Needs cursor count limits to prevent resource exhaustion

**Relevant Changes**:
- Recording cursor tracking in `sqlite3SrcListAssignCursors()`
- `SET_CURSOR_RECORDING()` / `CLR_CURSOR_RECORDING()` macros
- `MAX_CURSOR_IDS` enforcement
- `comdb2SetRecording()` on vdbe

### 8. **Nested Parse Enhancements**
Need to preserve parse state for complex DDL:
- Some operations need to maintain parse flags
- Error messages should be captured and returned

**Relevant Changes**:
- `sqlite3NestedParse_int()` split from `sqlite3NestedParse()`
- `sqlite3NestedParsePreserveFlags()` variant
- `preserve_update` flag handling
- Error message capture in nested parse

### 9. **Table Rename Compatibility**
Comdb2 disabled SQLite's rename object tracking:
- All `IN_RENAME_OBJECT` code blocks are skipped
- Prevents conflicts with comdb2's own rename handling

**Relevant Changes**:
- All `#if !defined(SQLITE_BUILDING_FOR_COMDB2)` around rename code
- Skips token remapping during rename operations

### 10. **User Schema Support**
Comdb2 allows custom schema modifications:
- Users can define table names with special resolution
- Need to resolve table names through custom logic

**Relevant Changes**:
- `gbl_allow_user_schema` flag checks
- `resolveTableName()` calls in `sqlite3LocateTableItem()`

---

## Key Technical Patterns

### 1. **Retry Loops for Remote Discovery**
```c
retry_alias:
  dbName = fdb_parse_comdb2_remote_dbname(zDatabase, &fqDbname);
  already_searched_fdb = 0;
retry_after_fdb_creation:
  // ... lookup logic ...
  if (found_alias) goto retry_alias;
  if (created_remote) goto retry_after_fdb_creation;
```
**Purpose**: Handle cases where table name is actually an alias, or where remote table needs to be attached before use.

### 2. **Conditional CSC2 Injection**
```c
if( iDb==1 ){
  // Temp table - no CSC2
  sqlite3NestedParse(pParse, "UPDATE ... sql=%Q ...");
} else {
  // Regular table - include CSC2
  sqlite3NestedParse(pParse, "UPDATE ... sql=%Q, csc2=NULL ...");
}
```
**Purpose**: Maintain schema table compatibility while adding comdb2-specific columns.

### 3. **Delegate Pattern for DDL**
```c
if( isView && db->isTimepartView==0 ){
  comdb2_drop_view(pParse, pName);
} else if( regular_sqlite_case ) {
  // Standard SQLite code
} else {
  comdb2DropTable(pParse, pName);
}
```
**Purpose**: Route operations to comdb2-specific handlers while preserving SQLite code paths for certain cases.

### 4. **Flag Tracking for Optimization**
```c
pTable->hasPartIdx = 0;
pTable->hasExprIdx = 0;
// ... later ...
if( pPIWhere ) pIndex->pTable->hasPartIdx = 1;
if( expression_index ) pTab->hasExprIdx = 1;
```
**Purpose**: Cache expensive-to-compute properties for query optimizer decisions.

---

## Conclusion

The comdb2 modifications to SQLite's build.c represent a sophisticated integration layer that:

1. **Extends SQLite** with distributed database capabilities
2. **Integrates CSC2** schema format alongside SQL DDL
3. **Optimizes queries** for both local and remote execution
4. **Supports advanced features** like time partitioning and versioned reads
5. **Maintains compatibility** with core SQLite while adding comdb2-specific behavior

The modifications are carefully designed to minimize invasiveness (using conditional compilation) while providing the necessary hooks for comdb2's distributed architecture, extended type system, and advanced features.
