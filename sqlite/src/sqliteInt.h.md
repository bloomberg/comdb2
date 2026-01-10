# SQLite Internal Header Modifications for Comdb2

This document analyzes the modifications made to SQLite's `sqliteInt.h` header file for integration with the Comdb2 database system.

## Summary

The comdb2 project extends SQLite's internal structures and functionality through conditional compilation using the `SQLITE_BUILDING_FOR_COMDB2` preprocessor directive. These modifications add support for:

- Distributed database features (foreign database support)
- Custom data types (datetime, interval types, decimal)
- Enhanced indexing capabilities
- Custom triggers and stored procedures
- Table versioning and caching
- Recording/tracking mechanisms
- Custom memory allocation strategies

---

## 1. New Includes and Dependencies

### Added Header Files (Lines 18-24)
```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
#include <cheapstack.h>
#include "tunables.h"
#include "fwd_types.h"
#include "ast.h"
#undef debug_raw
#endif
```

**Purpose**: Integrates comdb2-specific infrastructure:
- `cheapstack.h`: Stack trace utilities
- `tunables.h`: Runtime configuration parameters
- `fwd_types.h`: Forward type declarations for comdb2
- `ast.h`: Abstract syntax tree support

---

## 2. New Data Types and Affinity Extensions

### Extended Column Affinity Types (Lines 1963-1980)

Comdb2 adds several new affinity types beyond SQLite's basic types:

```c
#define SQLITE_AFF_DATETIME 'C'
#define SQLITE_AFF_INTV_YE  'D'  // Interval Year
#define SQLITE_AFF_INTV_MO  'E'  // Interval Month
#define SQLITE_AFF_INTV_DY  'F'  // Interval Day
#define SQLITE_AFF_INTV_HO  'G'  // Interval Hour
#define SQLITE_AFF_INTV_MI  'H'  // Interval Minute
#define SQLITE_AFF_INTV_SE  'I'  // Interval Second
#define SQLITE_AFF_NUMERIC  'J'  // Originally 'C'
#define SQLITE_AFF_INTEGER  'K'  // Originally 'D'
#define SQLITE_AFF_REAL     'L'  // Originally 'E'
#define SQLITE_AFF_DECIMAL  'M'
#define SQLITE_AFF_SMALL    'N'  // For float
```

**Rationale**: SQLite affinity codes were remapped to uppercase to avoid conflicts with comparison operators. The affinity mask was adjusted from `0x47` to `0x4F` to accommodate the additional types.

### Modified SQLITE_KEEPNULL Constant (Lines 2009-2026)

Changed from `0x08` to `0x30` to avoid overlap with comdb2's expanded affinity types. This is a combination of `STOREP2` and `JUMPIFNULL`, which are mutually exclusive in their usage contexts.

---

## 3. Modified Core Structures

### 3.1 Db Structure Extensions (Lines 1270-1275)

```c
struct Db {
  // ... existing fields ...
#if defined(SQLITE_BUILDING_FOR_COMDB2)
  int class;           /* what class for this cluster */
  int class_override;  /* was class explicit at the discovery time */
  int local;           /* is this a local db */
  int version;         /* which protocol it supports */
#endif
};
```

**Purpose**: Supports distributed database features, allowing databases to be categorized by class and tracked with version information for protocol compatibility.

### 3.2 sqlite3 Connection Structure (Lines 1443-1585)

```c
struct sqlite3 {
#if defined(SQLITE_BUILDING_FOR_COMDB2)
  int isPreparer;  /* Set by preparer plugin - must be first, must be an int */
#endif
  // ... existing fields ...
#if defined(SQLITE_BUILDING_FOR_COMDB2)
  char *zTblName;   /* Optional table name for attachments */
#endif
  // ... more fields ...
#if defined(SQLITE_BUILDING_FOR_COMDB2)
  void *pBestIndexCtx;  /* For sqlite3_vtab_collation() */
#endif
  // ... more fields ...
#if defined(SQLITE_BUILDING_FOR_COMDB2)
  u8 isExpert;          /* Analyze using SQLite expert */
  u8 isTimepartView;    /* Time partition view */
#endif
};
```

**Key Additions**:
- `isPreparer`: Plugin integration support
- `zTblName`: Attachment tracking
- `pBestIndexCtx`: Virtual table collation context
- `isExpert`: Query analysis mode flag
- `isTimepartView`: Time-based partitioning support

### 3.3 Table Structure Extensions (Lines 2122-2129)

```c
struct Table {
  // ... existing fields ...
#if defined(SQLITE_BUILDING_FOR_COMDB2)
  int version;      /* used for cached table schema, from remote */
  int iDb;          /* iDb version */
  int hasPartIdx;   /* Has partial index */
  int hasExprIdx;   /* Has expression index */
  int hasFuncIdx;   /* UNUSED: if the table has an index with uses a lua scalar func */
#endif
};
```

**Purpose**:
- Schema caching for remote tables
- Index capability tracking for optimization
- Partial/expression index detection

### 3.4 Index Structure Modifications (Lines 2383-2406)

```c
struct Index {
  // ... existing fields ...
#if defined(SQLITE_BUILDING_FOR_COMDB2)
  KeyInfo *pKeyInfo;  /* A KeyInfo object suitable for this index */
#endif
  // ... more fields ...
  unsigned bHasExpr:1;  /* Index contains an expression */
#ifdef SQLITE_ENABLE_STAT3_OR_STAT4
  int nSample;
#if defined(SQLITE_BUILDING_FOR_COMDB2)
  int nAlloc;  /* Number of elements allocated in aSample[] */
#endif
#endif
};
```

**Key Changes**:
- Cached `KeyInfo` for performance
- Expression index tracking via `bHasExpr` flag
- Dynamic sample allocation tracking

### 3.5 Expr Structure (Lines 2597-2601)

```c
struct Expr {
#if defined(SQLITE_BUILDING_FOR_COMDB2)
  u16 op;  /* Operation performed by this node */
#else
  u8 op;   /* Operation performed by this node */
#endif
  // ... rest of fields ...
};
```

**Rationale**: Expanded from `u8` to `u16` to support additional expression operators beyond SQLite's built-in set.

### 3.6 Parse Structure (Lines 3262-3390)

```c
struct Parse {
  // ... existing fields ...
#if defined(SQLITE_BUILDING_FOR_COMDB2)
  ast_t *ast;
  int preserve_update;  /* statement replacement, preserve flags */
#endif
  // ... more fields ...
#if defined(SQLITE_BUILDING_FOR_COMDB2)
  int recording[MAX_CURSOR_IDS/sizeof(int)];  /* which cursors are recording? */
  u8 write;                  /* Write transaction during sqlite3FinishCoding? */
  Cdb2DDL *comdb2_ddl_ctx;   /* Context for DDL commands */
  int prepFlags;             /* Prepare-only mode flags */
  int nSrcListOnly;          /* Number of table names in azSrcListOnly */
  char **azSrcListOnly;      /* Table names for SQLITE_PREPARE_SRCLIST_ONLY */
  u8 isDryrun;               /* Is a dryrun command */
#endif
};
```

**Major Additions**:
- **AST Support**: Full abstract syntax tree tracking
- **Recording Array**: Bitmap tracking which cursors are actively recording (up to 10,000 cursors)
- **DDL Context**: Schema change management
- **Prepare Flags**: Support for prepare-only mode (analyze queries without execution)
- **Source List Extraction**: Table name discovery for foreign database integration

---

## 4. New Forward Type Declarations (Lines 1159-1163)

```c
typedef struct Cdb2TrigEvent Cdb2TrigEvent;
typedef struct Cdb2TrigEvents Cdb2TrigEvents;
typedef struct Cdb2TrigTables Cdb2TrigTables;
typedef struct comdb2_ddl_context Cdb2DDL;
```

Custom trigger system that differs from SQLite's built-in triggers, providing comdb2-specific event handling.

---

## 5. New Function Declarations

### 5.1 VDBE Operation Functions (Lines 1200-1208)

```c
void comdb2SetReplace(Vdbe *v);
void comdb2SetUpdate(Vdbe *v);
void comdb2SetIgnore(Vdbe *v);
int comdb2ForceVerify(Vdbe *v);
int comdb2IgnoreFailure(Vdbe *v);
void comdb2SetUpsertIdx(Vdbe *v, int idx);
int comdb2UpsertIdx(Vdbe *v);
```

**Purpose**: Control VDBE behavior for comdb2-specific conflict resolution and upsert operations.

### 5.2 Foreign Database Integration (Lines 5039-5050)

```c
extern int sqlite3AddAndLockTable(sqlite3 *db, const char *dbname,
    const char *table, int *version, int in_analysis_load,
    int *out_class, int *out_local, int *out_class_override,
    int *proto_version);
extern int sqlite3UnlockTable(const char *dbname, const char *table);
extern int comdb2_dynamic_attach(sqlite3 *db, sqlite3_context *context,
    int argc, sqlite3_value **argv, const char *zName, const char *zFile,
    char **pzErrDyn, int version, int class, int local, int class_override,
    int proto_version);
extern void comdb2_dynamic_detach(sqlite3 *db, int idx);
```

**Purpose**: Dynamic foreign database attachment and management with versioning and locking.

### 5.3 Trigger System (Lines 5088-5100)

```c
Cdb2TrigEvents *comdb2AddTriggerEvent(Parse*,Cdb2TrigEvents*,Cdb2TrigEvent*);
void comdb2DropTrigger(Parse*,int,Token*);
Cdb2TrigTables *comdb2AddTriggerTable(Parse*,Cdb2TrigTables*,SrcList*,Cdb2TrigEvents*);
void comdb2CreateTrigger(Parse*,int,int,Token*,Cdb2TrigTables*);

void comdb2CreateScalarFunc(Parse *, Token *, int flags);
void comdb2DropScalarFunc(Parse *, Token *);
void comdb2CreateAggFunc(Parse *, Token *);
void comdb2DropAggFunc(Parse *, Token *);
int comdb2IsDryrun(Parse *);
```

**Purpose**: Custom trigger and user-defined function management.

---

## 6. Macro Definitions and Constants

### 6.1 Index Type Extension (Lines 2423-2425)

```c
#define SQLITE_IDXTYPE_DUPKEY 4  /* Is the DUPLICATE KEY for the table */
```

Support for duplicate key indexes.

### 6.2 OPFLAG Extensions (Lines 3494-3499)

```c
#define OPFLAG_FORCE_VERIFY   0x100
#define OPFLAG_IGNORE_FAILURE 0x200
#define OPFLAG_MKREC_COMDB2   0x400
#define OPFLAG_SKIPSCAN       0x800
```

Additional VDBE operation flags for comdb2-specific behaviors.

### 6.3 Cursor Recording Macros (Lines 3398-3423)

```c
#define MAX_CURSOR_IDS 10000

#define SET_CURSOR_RECORDING(p,i)
#define CLR_CURSOR_RECORDING(p,i)
#define GET_CURSOR_RECORDING(p,i)
```

Bitmap-based tracking system for identifying which cursors are actively recording data changes.

---

## 7. Tunables System (Lines 5016-5037)

```c
enum {
    SQLITE_ATTR_QUANTITY   = 1,
    SQLITE_ATTR_BOOLEAN    = 2
};

struct sqlite3_tunables_type {
#define DEF_ATTR(NAME, name, type, dflt) int name;
#include "sqlite_tunables.h"
};

extern struct sqlite3_tunables_type sqlite3_gbl_tunables;
void sqlite3_tunables_init(void);
void sqlite3_dump_tunables(void);
void sqlite3_set_tunable_by_name(char *tname, char *val);
```

**Purpose**: Runtime configuration system allowing dynamic adjustment of SQLite behavior without recompilation.

---

## 8. Significant Integration Points

### 8.1 Alternative B-tree Header
```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
#include "sqlite_btree.h"
#else
#include "btree.h"
#endif
```

Comdb2 uses a custom B-tree implementation (`sqlite_btree.h`) instead of SQLite's default.

### 8.2 Compiler Optimizations (Lines 544-557)

```c
#if defined(__GNUC__)
#define likely(x)    __builtin_expect(!!(x), 1)
#define unlikely(x)  __builtin_expect(!!(x), 0)
#endif
```

Enables GCC branch prediction hints for performance optimization.

### 8.3 PrepareOnly Flag (Lines 1646-1648)

```c
#define SQLITE_PrepareOnly HI(0x1000)  /* Pending flag for prepare_v3() */
```

Allows query analysis and planning without execution, useful for query optimization tools.

---

## 9. Statistics and Analysis

### STAT4 Support (Line 2153)
```c
#define TF_HasStat4 0x2000  /* STAT4 info available for this table */
```

Enhanced statistics tracking for query optimization.

---

## Conclusion

The comdb2 modifications to `sqliteInt.h` represent a comprehensive extension of SQLite's internal architecture to support:

1. **Distributed Systems**: Foreign database connectivity with versioning
2. **Enhanced Type System**: Additional data types for enterprise applications
3. **Advanced Indexing**: Expression indexes, partial indexes, and duplicate key indexes
4. **Schema Management**: DDL context, versioning, and caching
5. **Performance**: Cursor recording, tunable parameters, and compiler optimizations
6. **Custom Extensions**: Triggers, user-defined functions, and AST support

All modifications are cleanly isolated using `SQLITE_BUILDING_FOR_COMDB2` conditional compilation, ensuring compatibility with vanilla SQLite while enabling comdb2-specific features.
