# SQLite main.c Modifications for Comdb2

This document explains all modifications made to SQLite's `main.c` file for integration with the Comdb2 database system.

## Summary of All Modifications

The modifications to `main.c` fall into several categories:
1. **Custom header includes** - Integration with Comdb2-specific subsystems
2. **Function stubs** - Disabling SQLite's page cache subsystem
3. **Custom collation sequences** - Support for Comdb2's datacopy index mechanism
4. **Enhanced error handling** - Debug logging and custom error codes
5. **Database initialization extensions** - Registration of Comdb2 system tables, functions, and Lua integration
6. **API signature changes** - Threading state integration
7. **Helper functions** - Index function introspection
8. **Null pointer safety checks** - Additional defensive programming

## Detailed Analysis

### 1. Header Includes

**Location:** Lines 18-38

```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
#include <sqlglue.h>
#include "comdb2systbl.h"
#include "series.h"
#include <logmsg.h>
#include <stddef.h>
#endif
```

**Purpose:**
- `sqlglue.h`: Comdb2's SQL integration layer for bridging SQLite with Comdb2's storage engine
- `comdb2systbl.h`: System tables that expose Comdb2 metadata and runtime information
- `series.h`: Support for the series table-valued function extension
- `logmsg.h`: Comdb2's logging infrastructure
- `stddef.h`: Standard definitions needed for offset calculations

**Why:** These headers provide access to Comdb2-specific functionality that needs to be registered during database initialization.

---

### 2. Function Declarations

**Location:** Lines 48-50

```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
int sqlite3RegexpInit(sqlite3*);
#endif
```

**Purpose:** Forward declaration for Comdb2's regular expression support initialization.

**Why:** Comdb2 provides custom regex functionality that needs to be registered with each database connection.

---

### 3. Page Cache Function Stubs

**Location:** Lines 123-131

```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
void sqlite3PCacheBufferSetup(void *p, int sz, int n){ return; }
int sqlite3PcacheInitialize(void){ return 0; }
void sqlite3PcacheShutdown(void){ return; }
void sqlite3PCacheSetDefault(void){ return; }
int sqlite3HeaderSizeBtree(void){ return 0; }
int sqlite3HeaderSizePcache(void){ return 0; }
int sqlite3HeaderSizePcache1(void){ return 0; }
#endif
```

**Purpose:** These are no-op implementations of SQLite's page cache functions.

**Why:** Comdb2 uses its own page cache mechanism instead of SQLite's built-in page cache. By stubbing these functions, Comdb2 disables SQLite's page cache while maintaining compatibility with code that expects these functions to exist. This prevents double-caching and allows Comdb2 to have full control over memory management and page lifecycle.

---

### 4. Datacopy Collating Function

**Location:** Lines 979-990

```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
/*
** Dummy collating sequence used by datacopy index
*/
static int datacopyCollatingFunc(
  void *NotUsed,
  int nKey1, const void *pKey1,
  int nKey2, const void *pKey2
){
  return 0;
}
#endif
```

**Purpose:** A collation function that always returns 0 (indicating equality).

**Why:** This is used for "datacopy" indexes in Comdb2. These are special indexes where the comparison order doesn't matter - the index is used purely for storing a copy of data, not for ordering. By always returning 0, all keys are considered equal from a collation perspective, which allows the index to function as a pure data storage mechanism without enforcing any particular sort order.

---

### 5. Enhanced Error Logging for SQLITE_BUSY

**Location:** Multiple locations (lines 1159-1172, 1847-1849, 2629-2630)

```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
    logmsg(LOGMSG_ERROR, "%s:%d SQLITE_BUSY\n", __FILE__, __LINE__);
    cheap_stack_trace();
#endif
```

**Purpose:** Enhanced debugging when SQLITE_BUSY errors occur.

**Why:** SQLITE_BUSY indicates that a database operation cannot proceed because another operation is in progress. These errors can be difficult to debug in production systems. By logging the exact location and stack trace when SQLITE_BUSY occurs, Comdb2 developers can:
1. Identify which operations are causing blocking
2. Understand the call chain that led to the busy condition
3. Diagnose race conditions and locking issues in distributed environments

This appears in three critical locations:
- Database close operations (when unfinalized statements exist)
- User-defined function modifications
- Collation sequence modifications

---

### 6. Unfinalized Statement Debugging

**Location:** Lines 1165-1171

```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
    {
      sqlite3_stmt *pStmt = 0;
      while( (pStmt = sqlite3_next_stmt(db,pStmt))!=0 ){
        logmsg(LOGMSG_DEBUG, "%s:%d NOT FINALIZED: %p ==> %p {%s}\n",
               __FILE__, __LINE__, db, pStmt, sqlite3_sql(pStmt));
      }
    }
#endif
```

**Purpose:** Log all unfinalized prepared statements when database close fails.

**Why:** When closing a database connection fails due to unfinalized statements, this code helps identify which SQL statements weren't properly cleaned up. This is critical for:
1. Debugging memory leaks
2. Identifying code paths that don't properly finalize statements
3. Understanding why database connections can't be closed in production

---

### 7. Custom Error Messages

**Location:** Lines 1532-1580

```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
  static const char* const aComdb2Msg[] = {
    /* SQLITE_DEADLOCK          */ "transaction aborted due to deadlock",
    /* SQLITE_ACCESS            */ "access denied",
    /* SQLITE_LIMIT_DEPRECATED  */ 0,
    /* 203, unused              */ 0,
    /* SQLITE_TRANTOOCOMPLEX    */ "Transaction rollback too large",
    /* SQLITE_TRAN_CANCELLED    */ "Unable to maintain snapshot, too many resources blocked",
    /* SQLITE_TRAN_NOLOG        */ "Unable to maintain snapshot, too many log files",
    /* SQLITE_TRAN_NOUNDO       */ "Database changed due to sc or fastinit; snapshot failure",
    /* SQLITE_CONV_ERROR        */ "type conversion failure",
    /* SQLITE_COMDB2SCHEMA      */ "table schema changed",
    /* SQLITE_CLIENT_CHANGENODE */ "Non-durable write",
    /* SQLITE_DDL_MISUSE        */ "overlapping tables detected in transactional DDL",
    /* SQLITE_TIMEDOUT          */ "query timed out",
    /* SQLITE_COST_TOO_HIGH     */ "query cost too high",
    /* SQLITE_NO_TEMPTABLES     */ "temporary tables disallowed",
    /* SQLITE_NO_TABLESCANS     */ "table scans disallowed",
  };
  static const int comdb2MinErr = SQLITE_DEADLOCK;
#endif
```

**Purpose:** Define human-readable error messages for Comdb2-specific error codes.

**Why:** Comdb2 extends SQLite with additional error codes to handle distributed database scenarios:
- **Deadlock handling:** Important in distributed transactions
- **Snapshot failures:** When MVCC (Multi-Version Concurrency Control) can't maintain a consistent view
- **Schema changes:** When tables are altered while queries are running
- **Query governance:** Timeout and cost controls for resource management
- **Policy enforcement:** Restrictions on certain operations (table scans, temp tables)

These are integrated into SQLite's `sqlite3ErrStr()` function to provide clear error messages to clients.

---

### 8. Lua Function Registration

**Location:** Lines 3037-3065

```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
static void register_lua_sfuncs(sqlite3 *db, struct sqlthdstate *thd)
{
    listc_t sfuncs;
    get_sfuncs(&sfuncs);
    struct lua_func_t *sfunc;
    LISTC_FOR_EACH(&sfuncs, sfunc, lnk)
    {
        lua_func_arg_t *arg = malloc(sizeof(lua_func_arg_t));
        arg->thd = thd;
        arg->name = sfunc->name;
        sqlite3_create_function_v2(db, sfunc->name, -1, SQLITE_UTF8 | sfunc->flags,
                                   arg, lua_func, NULL, NULL, free);
    }
}

static void register_lua_afuncs(sqlite3 *db, struct sqlthdstate *thd)
{
    listc_t afuncs;
    get_afuncs(&afuncs);
    struct lua_func_t *afunc;
    LISTC_FOR_EACH(&afuncs, afunc, lnk)
    {
        lua_func_arg_t *arg = malloc(sizeof(lua_func_arg_t));
        arg->thd = thd;
        arg->name = afunc->name;
        // afunc->flags are purposefully ignored here.
        sqlite3_create_function_v2(db, afunc->name, -1, SQLITE_UTF8, arg,
                                   NULL, lua_step, lua_final, free);
    }
}
#endif
```

**Purpose:** Register user-defined Lua functions (both scalar and aggregate) with SQLite.

**Why:** Comdb2 supports Lua scripting for custom SQL functions. This allows:
1. Dynamic function definitions without recompiling
2. Business logic implementation in a sandboxed scripting environment
3. Support for both scalar functions (`sfuncs`) and aggregate functions (`afuncs`)
4. Thread-local state management via `sqlthdstate`

The functions are registered with `-1` argument count, allowing variable arguments.

---

### 9. Modified Database Opening Functions

**Location:** Lines 3107-3517

**Changes:**
1. Added `struct sqlthdstate *thd` parameter to `openDatabase()` and `sqlite3_open()`
2. Modified `sqlite3_open_v2()` to pass NULL for thd
3. Modified `sqlite3_open16()` to pass 0 for thd

**Purpose:** Thread state integration throughout the database opening process.

**Why:** Comdb2 is a multi-threaded database system where each thread has its own state. The `sqlthdstate` parameter:
1. Carries thread-local configuration
2. Enables thread-specific Lua function registration
3. Provides context for logging and error handling
4. Allows per-thread resource tracking

---

### 10. Database Initialization Modifications

**Location:** Lines 3189-3192, 3268-3270, 3403-3479

**Changes:**
1. Initialize `isPreparer` field to 0 with assertion check
2. Register DATACOPY collation during database setup
3. Initialize Comdb2 extensions:
   - System tables (`comdb2SystblInit`)
   - Regular expressions (`sqlite3RegexpInit`)
   - Series table-valued function (`sqlite3SeriesInit`)
4. Register Lua functions and date functions with mutex protection

```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
  db->isPreparer = 0;
  assert(offsetof(sqlite3, isPreparer) == 0);
#endif

// ...

#if defined(SQLITE_BUILDING_FOR_COMDB2)
  createCollation(db, "DATACOPY", SQLITE_UTF8, 0, datacopyCollatingFunc, 0);
#endif

// ...

#if defined(SQLITE_BUILDING_FOR_COMDB2)
  if( !db->mallocFailed && rc==SQLITE_OK ){
    rc = comdb2SystblInit(db);
  }
  if( !db->mallocFailed && rc==SQLITE_OK){
    rc = sqlite3RegexpInit(db);
  }
#ifdef SQLITE_ENABLE_SERIES
  if( !db->mallocFailed && rc==SQLITE_OK ){
    rc = sqlite3SeriesInit(db);
  }
#endif
#endif

// ...

#if defined(SQLITE_BUILDING_FOR_COMDB2)
  static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
  pthread_mutex_lock(&mutex);
  /* these modify global structures */
  if( thd!=NULL ){
    register_lua_sfuncs(db, thd);
    register_lua_afuncs(db, thd);
  }
  register_date_functions(db);
  pthread_mutex_unlock(&mutex);
#endif
```

**Purpose:**
- **isPreparer flag:** Marks whether this database handle is used for statement preparation only
- **DATACOPY collation:** Support for datacopy indexes
- **Extension registration:** Load Comdb2-specific functionality
- **Mutex protection:** Ensure thread-safe modification of global structures

**Why:**
- The `isPreparer` field being at offset 0 is a specific optimization for fast checking
- System tables provide visibility into Comdb2's internal state
- Regex and series extensions are commonly needed features
- Mutex protection prevents race conditions when multiple threads open databases simultaneously
- The code only registers Lua functions when a thread state is provided, allowing for prepare-only database handles

---

### 11. Index Function Introspection

**Location:** Lines 3875-3966

```c
#ifdef SQLITE_BUILDING_FOR_COMDB2
int sqlite3_table_index_funcs(sqlite3 *db, const char *zDbName,
                              const char *zTableName, char ***pzFuncs, int *nFuncs)
{
  // Implementation that walks through table indexes
  // Identifies function calls in index expressions
  // Returns array of function names used in indexes
}
#endif
```

**Purpose:** Extract a list of all user-defined functions used in index expressions for a given table.

**Why:** This is critical for schema management in Comdb2:
1. **Dependency tracking:** Before dropping a function, check if it's used in any indexes
2. **Schema replication:** When replicating schema to other nodes, ensure all required functions are available
3. **Function versioning:** Track which indexes depend on which function versions
4. **Performance analysis:** Identify which functions are being called during index operations

The implementation:
1. Finds the table in the schema
2. Iterates through all indexes on the table
3. For each index column marked as `XN_EXPR` (expression, not simple column)
4. Checks if the expression is a function call
5. Collects unique function names
6. Returns them as a dynamically allocated array

---

### 12. Null Pointer Safety Check

**Location:** Lines 4014-4016, 4036-4038

```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
    if( pPager ){
#endif
    // ... pager operations ...
#if defined(SQLITE_BUILDING_FOR_COMDB2)
    } else {
      rc = SQLITE_NOTFOUND;
    }
#endif
```

**Purpose:** Add defensive null pointer check for pPager in `sqlite3_file_control()`.

**Why:** In Comdb2's architecture, certain database operations may run without a traditional pager (since Comdb2 uses its own storage engine). This check:
1. Prevents segmentation faults when pPager is NULL
2. Returns a meaningful error code (SQLITE_NOTFOUND) instead of crashing
3. Allows file control operations to gracefully fail when the underlying pager doesn't exist

---

### 13. Preparer Check API

**Location:** Lines 4659-4663

```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
int sqlite3_is_preparer(sqlite3 *db) {
    return db->isPreparer;
}
#endif
```

**Purpose:** Public API to check if a database handle is in preparer-only mode.

**Why:** Comdb2 separates statement preparation from execution. Preparer handles:
1. Only prepare SQL statements (parse, compile, optimize)
2. Don't execute statements
3. Don't need full database locking
4. Enable parallel prepare operations on multiple threads

This function allows code to determine if a handle is in this mode and adjust behavior accordingly.

---

## Configuration Modifications Summary

### Global Configuration Changes
1. **Page cache disabled:** All page cache functions return immediately
2. **Custom error messages:** Extended error code set for distributed database scenarios

### Database Connection Changes
1. **Thread state tracking:** Every connection knows its associated thread
2. **Preparer mode:** Connections can be marked as preparation-only
3. **Lua function support:** Dynamic function registration per connection
4. **Enhanced logging:** SQLITE_BUSY errors generate detailed debugging information

### Initialization Changes
1. **System tables:** Expose Comdb2 metadata via SQL
2. **Regular expressions:** Custom regex engine integration
3. **Series function:** Table-valued function support
4. **Date functions:** Comdb2-specific date/time handling
5. **Datacopy collation:** Support for non-ordered indexes

---

## Why These Modifications Were Made

### 1. Distributed Database Requirements
Comdb2 is a distributed, replicated database system. Standard SQLite is designed for single-node, embedded use. The modifications enable:
- Multi-node error scenarios (deadlocks, snapshot failures)
- Schema change handling across replicas
- Resource governance (query timeouts, cost limits)

### 2. Custom Storage Engine
Comdb2 doesn't use SQLite's default storage (btree + pager). Instead:
- Disables SQLite's page cache (has its own)
- Adds null checks where pager might not exist
- Provides alternative locking mechanisms

### 3. Extensibility
The Lua integration and system tables make Comdb2 highly extensible:
- User-defined functions without recompilation
- Runtime introspection of database state
- Custom business logic in SQL queries

### 4. Debugging and Operations
Production database systems need extensive debugging capabilities:
- Stack traces on lock contention
- Detailed statement tracking
- Index dependency analysis
- Per-thread state visibility

### 5. Performance Optimization
Several optimizations improve multi-threaded performance:
- isPreparer at offset 0 for fast checking
- Separate prepare/execute paths reduce lock contention
- Thread-local function registration enables parallel operations

---

## Maintenance Considerations

When upgrading SQLite versions, maintainers should:

1. **Review page cache changes:** Ensure stubs remain compatible
2. **Check error code additions:** Update Comdb2 error message array
3. **Verify initialization sequence:** New SQLite extensions may need Comdb2 integration
4. **Test null pager scenarios:** New code paths may assume pager exists
5. **Audit SQLITE_BUSY locations:** New blocking operations should include logging
6. **Update function registration:** New SQLite features may need thread-aware initialization

The modifications follow a consistent pattern of:
- Conditional compilation with `SQLITE_BUILDING_FOR_COMDB2`
- Minimal changes to SQLite code structure
- Clear separation between SQLite and Comdb2 functionality
- Preservation of SQLite's public API where possible
