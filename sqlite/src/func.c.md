# SQLite func.c Modifications for Comdb2

This document analyzes the modifications made to SQLite's `func.c` file for the Comdb2 database project. All modifications are wrapped in `SQLITE_BUILDING_FOR_COMDB2` preprocessor directives.

## Summary of Modifications

The comdb2 project extends SQLite's function capabilities with:
- **Additional data types**: Support for datetime, interval, and decimal types
- **Custom SQL functions**: 30+ new functions specific to comdb2 operations
- **Modified existing functions**: Enhanced behavior for standard SQL functions
- **System integration**: Functions to query server state, versioning, and metadata
- **Data utilities**: Compression, hashing, UUID generation, and type conversion

---

## 1. New Includes and Dependencies

**Location**: Lines 20-31

```c
#include <unistd.h>
#include <uuid/uuid.h>
#include <memcompare.c>
#include <zlib.h>
#include "comdb2.h"
#include "sql.h"
#include "bdb_int.h"
#include "md5.h"
#include "tohex.h"
```

**Purpose**: Adds dependencies for:
- UUID generation (RFC 4122 UUIDs)
- Compression/decompression (zlib)
- MD5 hashing
- Comdb2-specific database internals
- BerkeleyDB integration

---

## 2. Extended Type System

### 2.1 typeof() Function Extension

**Location**: Lines 93-118

**Modification**: Extends the `typeof()` function to recognize comdb2-specific types:

```c
static const char *azType[] = {
    "integer", "real", "text", "blob", "null",
    "datetime", "interval_ym", "interval_ds",
    "datetime", "interval_ds", "decimal"
};
```

**New Types**:
- `SQLITE_DATETIME` (6): Date/time values
- `SQLITE_INTERVAL_YM` (7): Year-month intervals
- `SQLITE_INTERVAL_DS` (8): Day-second intervals
- `SQLITE_DATETIMEUS` (9): Datetime with microseconds
- `SQLITE_INTERVAL_DSUS` (10): Day-second intervals with microseconds
- `SQLITE_DECIMAL` (11): Decimal fixed-point numbers
- `SQLITE_NEXTSEQ` (SQLITE_MAX_U32-2): Sequence numbers

**Why**: Comdb2 needs richer data types for financial calculations (decimal), time-based operations (datetime/intervals), and sequence generation.

### 2.2 length() Function Extension

**Location**: Lines 138-146

**Modification**: Makes `length()` work with comdb2's extended types by treating them as byte sequences.

**Why**: Ensures length calculations work consistently across all comdb2 data types.

### 2.3 abs() Function - Decimal Support

**Location**: Lines 205-224

**Modification**: Adds decimal absolute value calculation using IBM's decNumber library:

```c
case SQLITE_DECIMAL: {
    decContext ctx;
    intv_t *val = (intv_t*)sqlite3_value_interval(argv[0], SQLITE_DECIMAL);
    intv_t res_val;

    dec_ctx_init(&ctx, DEC_INIT_DECQUAD, gbl_decimal_rounding);
    decQuadAbs(&res_val.u.dec, &val->u.dec, &ctx);

    if(dfp_conv_check_status(&ctx, "quad", "abs(quad)")) {
        sqlite3_result_error(context, "decimal overflow", -1);
        break;
    }

    res_val.type = INTV_DECIMAL_TYPE;
    res_val.sign = 0;
    sqlite3_result_interval(context, &res_val);
    break;
}
```

**Why**: Financial applications require precise decimal arithmetic without floating-point errors. Uses 128-bit decimal floating-point (decQuad) for accuracy.

---

## 3. Modified Existing Functions

### 3.1 substr() - Zero Index Fix

**Location**: Lines 356-358

**Modification**: Forces 1-based indexing (SQL standard):

```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
  if (p1 == 0) p1 = 1;
#endif
```

**Why**: Ensures SQL standard compliance - `substr(x, 0, n)` behaves like `substr(x, 1, n)`.

### 3.2 round() - Type Casting Fix

**Location**: Lines 574-584

**Modification**: Adds explicit type casting for integer conversions:

```c
if( n==0 && r>=0 && r<(double)(LARGEST_INT64-1) ){
    r = (double)((sqlite_int64)(r+0.5));
}else if( n==0 && r<0 && (-r)<(double)(LARGEST_INT64-1) ){
    r = -(double)((sqlite_int64)((-r)+0.5));
}
```

**Why**: Prevents potential undefined behavior from implicit conversions in edge cases.

### 3.3 Aggregate Functions - Decimal Support

**Location**: Lines 2387-2390, 2418-2460, 2501-2556

**Modification**: Extends `sum()`, `avg()`, and `total()` to handle decimal types:

```c
struct SumCtx {
    double rSum;
    i64 iSum;
    i64 cnt;
    u8 overflow;
    u8 approx;
#if defined(SQLITE_BUILDING_FOR_COMDB2)
    u8 decs;          /* True if summing decimals */
    decQuad decSum;   /* decQuad aggregation */
#endif
};
```

**Key Features**:
- Tracks whether decimals are being summed
- Maintains separate decimal accumulator
- Uses decQuad arithmetic for precision
- Handles overflow detection
- Returns decimal results for decimal inputs

**Why**: Financial aggregations (e.g., summing account balances) require exact decimal precision.

### 3.4 group_concat() - Memory Limit Control

**Location**: Lines 2692-2720

**Modification**: Allows per-client memory limit configuration:

```c
struct sqlclntstate *clnt = get_sql_clnt();
pAccum->mxAlloc = (clnt && clnt->group_concat_mem_limit != 0) ?
    clnt->group_concat_mem_limit : db->aLimit[SQLITE_LIMIT_LENGTH];

// Custom error message
if(clnt && pAccum->accError==SQLITE_TOOBIG) {
    clnt->sqlite_errstr = "string or blob too big";
}
```

**Why**: Prevents runaway memory usage in multi-tenant environments; provides clearer error messages.

### 3.5 Pattern Matching - Export patternCompare

**Location**: Lines 1554-1558

**Modification**: Makes `patternCompare()` globally accessible:

```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
int patternCompare(
#else
static int patternCompare(
#endif
```

**Why**: Allows comdb2 code to use LIKE/GLOB pattern matching logic externally.

---

## 4. New SQL Functions Added

### 4.1 Timing and Sleep Functions

#### sleep(seconds)
**Location**: Lines 435-455

```c
static void sleepFunc(sqlite3_context *context, int argc, sqlite3_value *argv[])
```

**Purpose**: Sleep for specified seconds with interruption checking
- Calls `comdb2_sql_tick()` every second to check for query cancellation
- Returns number of seconds actually slept
- Returns -1 on error

**Use Case**: Testing, debugging, simulating delays

#### usleep(microseconds)
**Location**: Lines 457-478

**Purpose**: Microsecond-precision sleep
- Sleeps in 1-second chunks to allow interruption
- Checks for cancellation after each second
- Returns microseconds actually slept

**Use Case**: Fine-grained timing control, performance testing

---

### 4.2 SQL Analysis Functions

#### comdb2_extract_table_names(sql)
**Location**: Lines 480-526

**Purpose**: Extracts table names from SQL query without executing it
- Parses SQL using SQLite parser in PREPARE_ONLY mode
- Returns space-separated list of table names
- Returns error if SQL is invalid

**Use Case**: Security auditing, query analysis, permission checking

#### comdb2_normalize_sql(sql)
**Location**: Lines 528-550

**Purpose**: Returns normalized form of SQL query
- Replaces literals with placeholders
- Standardizes whitespace and case
- Useful for query deduplication and analysis

**Use Case**: Query fingerprinting, performance monitoring, statement caching

---

### 4.3 UUID/GUID Functions

#### guid()
**Location**: Lines 728-740

**Purpose**: Generate random UUID as binary blob (16 bytes)
- Uses system `uuid_generate()` (RFC 4122 compliant)
- Returns blob suitable for storage

**Use Case**: Primary keys, unique identifiers

#### guid_str()
**Location**: Lines 742-757

**Purpose**: Generate UUID as string (36 characters + null)
- Format: `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
- Human-readable format

**Use Case**: External APIs, logging, display

#### guid(string)
**Location**: Lines 759-785

**Purpose**: Convert UUID string to binary blob
- Validates format before conversion
- Returns NULL if invalid

**Use Case**: Storing UUIDs efficiently

#### guid_str(blob)
**Location**: Lines 787-809

**Purpose**: Convert binary UUID to string
- Validates blob is exactly 16 bytes
- Returns formatted UUID string

**Use Case**: Displaying stored UUIDs

---

### 4.4 Type Conversion Functions

#### comdb2_double_to_blob(number)
**Location**: Lines 811-828

**Purpose**: Convert double to 8-byte blob (IEEE 754 format)
- Preserves exact bit representation
- Works with integers and floats

**Use Case**: Binary storage, interoperability, bit-level operations

#### comdb2_blob_to_double(blob)
**Location**: Lines 830-846

**Purpose**: Convert 8-byte blob to double
- Must be exactly 8 bytes
- Interprets as IEEE 754 double

**Use Case**: Reading binary-stored numbers

---

### 4.5 System Information Functions

#### comdb2_sysinfo(attribute)
**Location**: Lines 855-890

**Supported Attributes**:
- `"pid"`: Process ID of database server
- `"tid"`: Thread ID (architecture-specific)
- `"master"`: Hostname of replication master node
- `"host"`: Hostname of current server
- `"class"`: Machine class/tier designation
- `"version"`: Full version info array: `[version] [codename] [semver] [git_sha] [buildtype]`

**Use Case**: Monitoring, debugging, cluster awareness

#### comdb2_ctxinfo(attribute)
**Location**: Lines 899-924

**Supported Attributes**:
- `"parallel"`: 1 if query is running in parallel mode, 0 otherwise
- `"ruleset_result"`: Result from business rules evaluation
- `"user"`: Current authenticated user

**Use Case**: Security auditing, performance analysis, debugging

#### comdb2_version()
**Location**: Lines 930-937

**Purpose**: Returns comdb2 version string

**Use Case**: Version checking, compatibility

#### comdb2_semver()
**Location**: Lines 939-946

**Purpose**: Returns semantic version string (e.g., "7.0.1")

**Use Case**: Programmatic version comparison

#### comdb2_prevquerycost()
**Location**: Lines 951-962

**Purpose**: Returns cost estimate of previous query
- Returns NULL if no previous query
- Frees cost data after retrieval

**Use Case**: Query optimization, performance monitoring

#### comdb2_host() / comdb2_node()
**Location**: Lines 964-971

**Purpose**: Returns hostname of current database node
- Alias: `comdb2_node()` is same as `comdb2_host()`

**Use Case**: Cluster-aware applications

#### comdb2_port()
**Location**: Lines 973-981

**Purpose**: Returns TCP port number of database server

**Use Case**: Connection management, cluster discovery

#### comdb2_dbname()
**Location**: Lines 984-992

**Purpose**: Returns database name

**Use Case**: Multi-database environments

#### comdb2_starttime()
**Location**: Lines 1064-1073

**Purpose**: Returns server start time as datetime

**Use Case**: Uptime calculation, monitoring

#### comdb2_user()
**Location**: Lines 1109-1118

**Purpose**: Returns current authenticated username

**Use Case**: Auditing, row-level security

#### comdb2_last_cost()
**Location**: Lines 2940-2949

**Purpose**: Returns cost of last executed statement

**Use Case**: Real-time query performance monitoring

#### comdb2_snapshot_lsn()
**Location**: Lines 1122-1147

**Purpose**: Returns LSN (Log Sequence Number) for snapshot transactions
- Returns format: `{file:offset}`
- Returns current LSN for non-snapshot mode if configured
- Returns NULL if not applicable

**Use Case**: Replication monitoring, point-in-time recovery

---

### 4.6 Table Metadata Functions

#### table_version(tablename)
**Location**: Lines 999-1020

**Purpose**: Returns schema version number for specified table
- Returns NULL if table doesn't exist
- Increments on schema changes

**Use Case**: Schema evolution tracking, cache invalidation

#### partition_info(partition_name, option)
**Location**: Lines 1027-1059

**Purpose**: Returns information about table partition
- Various options available (implementation-specific)
- Returns NULL if partition not found

**Use Case**: Partition management, query routing

---

### 4.7 Hashing and Compression

#### checksum_md5(data)
**Location**: Lines 1075-1107

**Purpose**: Computes MD5 hash of text or blob
- Returns 32-character hex string
- Works with both TEXT and BLOB inputs

**Use Case**: Data integrity, checksums, deduplication

#### compress(data) / compress_zlib(data)
**Location**: Lines 1266-1305

**Purpose**: Compress data using zlib DEFLATE
- Works with TEXT or BLOB
- Returns compressed BLOB
- Returns NULL for NULL input

**Algorithm**: zlib default compression

**Use Case**: Storage optimization, data transfer

#### uncompress(data) / uncompress_zlib(data)
**Location**: Lines 1307-1341

**Purpose**: Decompress zlib-compressed data
- Requires BLOB input
- Returns decompressed BLOB

**Use Case**: Reading compressed data

#### compress_gzip(data)
**Location**: Lines 1343-1382

**Purpose**: Compress with gzip format (includes header/trailer)
- Similar to `compress()` but gzip-compatible
- Uses deflateInit2 with windowBits = 15 | 16

**Use Case**: Web applications, HTTP compression, external tools

#### uncompress_gzip(data)
**Location**: Lines 1384-1418

**Purpose**: Decompress gzip format data
- Handles gzip header/trailer
- Uses inflateInit2 with windowBits = 15 | 16

**Use Case**: Processing gzip files, HTTP responses

**Implementation Note**: All compression functions use 32KB chunk size for streaming compression/decompression, preventing memory issues with large data.

---

### 4.8 Debug Logging Functions (Conditional)

Only available when `SQLITE_BUILDING_FOR_COMDB2_DBGLOG` is defined.

#### dbglog_cookie()
**Location**: Lines 2891-2899

**Purpose**: Generate unique cookie for debug session
- Returns 64-bit integer from fast seed generator

**Use Case**: Session tracking

#### dbglog_begin(name)
**Location**: Lines 2901-2909

**Purpose**: Begin debug logging session
- Returns status code

**Use Case**: Performance profiling, debugging

#### dbglog_end(cookie)
**Location**: Lines 2911-2934

**Purpose**: End debug session and retrieve log data
- Memory-maps debug file
- Returns log as BLOB
- Automatically unmaps file

**Use Case**: Retrieving profiling data

---

## 5. Disabled Standard Functions

**Location**: Lines 2989-2993, 3048-3052, 3116-3118

The following standard SQLite functions are **disabled** in comdb2:

1. **affinity()** (debug function)
2. **last_insert_rowid()**
3. **changes()**
4. **total_changes()**
5. **sqlite3AlterFunctions()** (ALTER TABLE support)

**Why Disabled**:
- `last_insert_rowid()`, `changes()`, `total_changes()`: Comdb2 uses different mechanisms for tracking row changes and IDs in distributed environment
- `affinity()`: Debug-only function not needed
- Alter functions: Comdb2 has its own schema change mechanism

---

## 6. Utility Functions for Function Discovery

#### sqlite3GetAllBuiltinFunctions(void **data, int *tot)
**Location**: Lines 3149-3170

**Purpose**: Returns sorted array of all available SQL functions
- Iterates function hash table
- Adds custom functions like `sys.cmd.send()`, `sys.cmd.verify()`
- Returns dynamically allocated array

**Use Case**: Auto-completion, documentation generation, introspection

#### sqlite3FreeAllBuiltinFunctions(void **data, int tot)
**Location**: Lines 3172-3179

**Purpose**: Frees memory allocated by `sqlite3GetAllBuiltinFunctions()`

---

## 7. Structural Changes

### 7.1 compareInfo Structure Export

**Location**: Lines 1472-1486

**Modification**: Moves `compareInfo` structure to header file for external use:

```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
/* Moved to "sqliteInt.h" for use by Comdb2. */
#else
struct compareInfo {
    u8 matchAll;
    u8 matchOne;
    u8 matchSet;
    u8 noCase;
};
#endif
```

**Why**: Allows comdb2 code to use LIKE/GLOB pattern matching infrastructure.

---

## Design Patterns and Best Practices

### Error Handling
- Consistent NULL return for invalid inputs
- Overflow detection for decimal operations
- Custom error messages for comdb2-specific issues
- Graceful degradation when optional features unavailable

### Memory Management
- Proper cleanup of allocated resources
- Uses `contextMalloc()` for SQLite memory tracking
- TRANSIENT or STATIC flags for result strings
- Streaming compression to limit memory usage

### Type Safety
- Explicit type checking before operations
- Validation of blob sizes for binary operations
- NULL handling throughout

### Performance
- Chunked I/O for compression (32KB chunks)
- Efficient UUID generation using OS facilities
- Minimal overhead for standard SQLite operations
- Client-configurable limits for resource control

---

## Summary of Modifications by Category

| Category | Count | Examples |
|----------|-------|----------|
| Extended Data Types | 6 | datetime, interval_ym, interval_ds, datetimeus, interval_dsus, decimal |
| Modified Functions | 7 | typeof, length, abs, substr, round, sum/avg/total, group_concat |
| System Info Functions | 13 | comdb2_version, comdb2_host, comdb2_user, etc. |
| Utility Functions | 8 | sleep, usleep, guid, md5, compress, etc. |
| SQL Analysis | 2 | comdb2_extract_table_names, comdb2_normalize_sql |
| Type Conversion | 6 | guid/guid_str conversions, comdb2_double_to_blob, etc. |
| Metadata Functions | 2 | table_version, partition_info |
| Debug Functions | 3 | dbglog_cookie, dbglog_begin, dbglog_end |
| Disabled Functions | 5 | last_insert_rowid, changes, total_changes, affinity, AlterFunctions |

**Total New Functions**: 30+

---

## Conclusion

The modifications to `func.c` transform SQLite into a enterprise-ready database system with:

1. **Financial-grade arithmetic**: Decimal type support for precise calculations
2. **Rich temporal types**: Datetime and interval support
3. **Distributed system awareness**: Cluster topology, replication, versioning
4. **Enterprise utilities**: UUID generation, compression, hashing
5. **Introspection capabilities**: Query analysis, metadata access, cost monitoring
6. **Security features**: User tracking, audit support
7. **Debugging tools**: Sleep functions, logging, query normalization

These modifications maintain backward compatibility with SQLite while adding capabilities necessary for Comdb2's distributed, transactional, and enterprise-focused architecture.
