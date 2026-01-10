# prepare.c Modifications

## Summary

Significant modifications for **per-table schema initialization**, **FDB tracking**, **CSC2 schema support**, and **encoding handling**.

## Modifications

### 1. Additional Headers and Declarations (Lines 18-29)
```c
#include "vdbeInt.h"
#include <time.h>
#include <ctype.h>
#include <datetime.h>
void comdb2SetWriteFlag(int);
#include "cdb2_constants.h"
#include <logmsg.h>
```

### 2. FDB Tracking in Schema Init (Lines 117-128, 137-140)
```c
extern int gbl_fdb_track;
if (gbl_fdb_track && iDb)
   logmsg(LOGMSG_USER, "Prep iDb=%d \"%s\"\n", iDb, argv[2]);
```

Logs schema initialization for foreign databases when tracking is enabled.

### 3. sqlite_master Schema Extension (Lines 213-216)
```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
"CREATE TABLE x(type text,name text,tbl_name text,rootpage int,sql text,csc2 text)";
#else
"CREATE TABLE x(type text,name text,tbl_name text,rootpage int,sql text)";
#endif
```

Adds a `csc2` column to sqlite_master for storing comdb2's native schema format.

### 4. Skip Duplicate sqlite_master Creation (Lines 196-230)
```c
pTab = sqlite3FindTableCheckOnly(db, zMasterName, db->aDb[iDb].zDbSName);
if( pTab==NULL ){
  // Create sqlite_master table...
}else {
  // Table exists, just set up initData
}
```

For remote databases, reuses existing sqlite_master table instead of recreating.

### 5. Transaction Begin Modification (Lines 254-258)
```c
rc = sqlite3BtreeBeginTrans(NULL, pDb->pBt, 0, 0);
comdb2SetWriteFlag(0);
```

Passes NULL for first parameter and explicitly clears write flag after read transaction.

### 6. Encoding Handling (Lines 306-311)
```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
SCHEMA_ENC(db) = SQLITE_UTF8;
#else
ENC(db) = SQLITE_UTF8;
#endif
```

Uses `SCHEMA_ENC` macro instead of `ENC` for UTF-16 omit case.

### 7. AnalysisLoad Return Value (Lines 385-389)
```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
rc = sqlite3AnalysisLoad(db, iDb);
#else
sqlite3AnalysisLoad(db, iDb);
#endif
```

Captures and checks return value from AnalysisLoad.

### 8. New sqlite3InitTable Function (Lines 431-520)
```c
int sqlite3InitTable(sqlite3 *db, char **pzErrMsg, const char *zName);
```

A variant of `sqlite3Init` that initializes only a single table by name. Used for:
- Dynamic attachment of remote tables
- Per-table schema refresh
- Supports `dbname.tablename` format with class override parsing

Key features:
- Parses `dbname.tablename` format
- Extracts class override prefix from database name
- Only initializes the specific table, not entire database
- Cleans up table name string after initialization

## Why These Changes

1. **CSC2 Column**: Stores comdb2's native schema definition alongside SQL, allowing bidirectional schema translation
2. **Per-Table Init**: Essential for FDB (federated database) where tables are attached on-demand
3. **FDB Tracking**: Debugging support for distributed query operations
4. **Encoding**: Consistent UTF-8 handling with comdb2's encoding macros
5. **Write Flag**: Ensures proper transaction mode tracking for comdb2's distributed transactions
6. **AnalysisLoad**: Better error handling for statistics loading
