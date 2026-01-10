# expr.c Modifications for Comdb2

## Summary of All Modifications

This document analyzes the modifications made to SQLite's `expr.c` file for the Comdb2 project. The changes are wrapped in `SQLITE_BUILDING_FOR_COMDB2` preprocessor directives and customize expression handling, type affinity, and rowid operations for Comdb2's distributed database architecture.

### Key Areas of Modification:

1. **Header Inclusions**: Added Comdb2-specific headers and external function declarations
2. **Expression Affinity Handling**: Extended type affinity checks for Comdb2-specific data types
3. **Rowid Extensions**: Added support for `COMDB2_ROWID` and `COMDB2_ROW_TIMESTAMP`
4. **Expression Evaluation**: Enhanced support for scalar functions and custom column operations
5. **Expression Description**: Added comprehensive expression-to-string conversion utilities
6. **Rename Object Support**: Disabled SQLite's rename token tracking mechanism
7. **Select Duplication**: Added recording field support during SELECT statement duplication
8. **Window Function Handling**: Updated window function gathering logic
9. **Memory Comparison**: Included custom memory comparison utilities

---

## 1. Header Inclusions and External Declarations

**Location**: Lines 7-25

### Changes:
```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
#include <alloca.h>
#endif

#if defined(SQLITE_BUILDING_FOR_COMDB2)
#include "logmsg.h"

extern int comdb2genidcontainstime(void);
extern char* fdb_table_name(int iTable);
#endif
```

### Purpose:
- **alloca.h**: Provides stack-based memory allocation for temporary buffers
- **logmsg.h**: Comdb2's logging facility for error reporting
- **comdb2genidcontainstime()**: Checks if genids (unique row identifiers) contain timestamp information
- **fdb_table_name()**: Retrieves foreign database table names

### Why This Matters:
Comdb2 uses unique row identifiers (genids) that can optionally include timestamp information. This is essential for distributed database synchronization and temporal queries.

---

## 2. Expression Handling Changes

### 2.1 Expression Affinity Check (Lines 58-60)

**Original Code**:
```c
char sqlite3ExprAffinity(Expr *pExpr){
  int op;
  pExpr = sqlite3ExprSkipCollate(pExpr);
  op = pExpr->op;
```

**Modified Code**:
```c
char sqlite3ExprAffinity(Expr *pExpr){
  int op;
  pExpr = sqlite3ExprSkipCollate(pExpr);
#if defined(SQLITE_BUILDING_FOR_COMDB2)
  if( pExpr->flags & EP_Generic ) return 0;
#endif
  op = pExpr->op;
```

**Purpose**: Returns affinity of 0 (SQLITE_AFF_BLOB) for generic expressions in Comdb2.

**Why**: Comdb2 may mark certain expressions as generic to prevent affinity-based conversions that could break compatibility with its backend storage engine.

### 2.2 Index Affinity Check (Lines 297-300)

**Modified Code**:
```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
  if (aff < SQLITE_AFF_NUMERIC) return aff==idx_affinity;
  else
#endif
  return sqlite3IsNumericAffinity(idx_affinity);
```

**Purpose**: Precise affinity matching for non-numeric types in Comdb2.

**Why**: Comdb2 has additional type affinities (datetime, intervals, decimal) that require exact matching rather than numeric grouping.

---

## 3. Type Affinity Modifications

### 3.1 Extended Affinity Array (Lines 3640-3663)

**Original Code**:
```c
static const char zAff[] = "B\000C\000D\000E";
assert( SQLITE_AFF_BLOB=='A' );
assert( SQLITE_AFF_TEXT=='B' );
```

**Modified Code**:
```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
static const char zAff[] = "B\000C\000D\000E"
                            "\000F\000G\000H"
                            "\000I\000J\000K"
                            "\000L\000M\000N";
#else
static const char zAff[] = "B\000C\000D\000E";
#endif

assert( SQLITE_AFF_BLOB=='A' );
assert( SQLITE_AFF_TEXT=='B' );
#if defined(SQLITE_BUILDING_FOR_COMDB2)
assert( SQLITE_AFF_DATETIME=='C' );
assert( SQLITE_AFF_INTV_YE=='D' );
assert( SQLITE_AFF_INTV_MO=='E' );
assert( SQLITE_AFF_INTV_DY=='F' );
assert( SQLITE_AFF_INTV_HO=='G' );
assert( SQLITE_AFF_INTV_MI=='H' );
assert( SQLITE_AFF_INTV_SE=='I' );
assert( SQLITE_AFF_NUMERIC=='J' );
assert( SQLITE_AFF_INTEGER=='K' );
assert( SQLITE_AFF_REAL=='L' );
assert( SQLITE_AFF_DECIMAL=='M' );
assert( SQLITE_AFF_SMALL=='N' );
#endif
```

**Purpose**: Extends SQLite's type affinity system to include Comdb2-specific types:
- **DATETIME**: Timestamp with timezone support
- **Intervals**: Year-Month, Day-Second intervals (6 variants)
- **DECIMAL**: Fixed-point decimal numbers
- **SMALL**: Small integer type

**Why**: Comdb2 supports SQL:2011 temporal data types and interval arithmetic that SQLite doesn't have natively.

---

## 4. Rowid Extensions

### 4.1 Comdb2 Rowid Functions (Lines 2256-2277)

**Added Functions**:
```c
int sqlite3IsComdb2Rowid(Table *pTab, const char *z){
#ifndef SQLITE_OMIT_VIRTUALTABLE
    if (IsVirtual(pTab))
        return 0;
#endif
  return (sqlite3StrICmp(z, "COMDB2_ROWID") == 0);
}

int sqlite3IsComdb2RowTimestamp(Table *pTab, const char *z){
#ifndef SQLITE_OMIT_VIRTUALTABLE
    if (IsVirtual(pTab))
        return 0;
#endif
  if (comdb2genidcontainstime()){
      return
          (sqlite3StrICmp(z, "COMDB2_ROW_TIMESTAMP") == 0 ||
           sqlite3StrICmp(z, "COMDB2_ROWTIMESTAMP") == 0);
  }
  return 0;
}
```

**Purpose**:
- Recognize `COMDB2_ROWID` as a special column name
- Recognize `COMDB2_ROW_TIMESTAMP` when genids contain time information

**Why**: Comdb2 exposes internal row identifiers and timestamps as pseudo-columns for:
- Distributed query optimization
- Replication tracking
- Temporal queries
- Debugging and diagnostics

### 4.2 Enhanced Column Retrieval (Lines 3415-3445)

**Modified Code**:
```c
if( iCol<0 || iCol==pTab->iPKey ){
#if defined(SQLITE_BUILDING_FOR_COMDB2)
    if( iCol == -1 ){
      sqlite3VdbeAddOp2(v, OP_Rowid, iTabCur, regOut);
    }else if( iCol == -2 ){
      sqlite3VdbeAddOp3(v, OP_Rowid, iTabCur, regOut, 1);
    }else if( iCol == -3 ){
      sqlite3VdbeAddOp3(v, OP_Rowid, iTabCur, regOut, 2);
    }else{
      sqlite3VdbeAddOp2(v, OP_Rowid, iTabCur, regOut);
    }
#else
    sqlite3VdbeAddOp2(v, OP_Rowid, iTabCur, regOut);
#endif
```

**Purpose**: Maps special column indices to different rowid retrieval modes:
- `iCol == -1`: Standard rowid
- `iCol == -2`: COMDB2_ROWID (with P2=1 parameter)
- `iCol == -3`: COMDB2_ROW_TIMESTAMP (with P2=2 parameter)

**Why**: Different types of rowid information require different backend operations. The P2 parameter tells the virtual machine which variant to retrieve.

### 4.3 Column Default Handling (Lines 3439-3445)

**Modified Code**:
```c
if( iCol>=0 ){
#if defined(SQLITE_BUILDING_FOR_COMDB2)
    /* register -1 will make it ignore affinity */
    sqlite3ColumnDefault(v, pTab, iCol, -1);
#else
    sqlite3ColumnDefault(v, pTab, iCol, regOut);
#endif
}
```

**Purpose**: Bypasses affinity application when retrieving column defaults.

**Why**: Comdb2's backend handles type conversions internally, and applying SQLite's affinity rules would cause double-conversion errors.

---

## 5. Expression Evaluation Customizations

### 5.1 Index Operations (Line 2588-2590)

**Added Code**:
```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
sqlite3VdbeAddTable(v, pTab);
#endif
```

**Purpose**: Associates table metadata with index operations for IN operator optimization.

**Why**: Comdb2's distributed architecture needs table context to route index lookups to the correct database node.

### 5.2 Scalar Function Tracking (Lines 4043-4045)

**Added Code**:
```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
stmt_set_has_scalar_func((sqlite3_stmt *)v, 1);
#endif
```

**Purpose**: Marks statements that use scalar functions.

**Why**: Comdb2 optimizes execution plans differently for statements with scalar functions, particularly for distributed query planning.

### 5.3 AST Tracking for IN Operator (Lines 2822-2825)

**Added Code**:
```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
ast_t *ast = ast_init(pParse, __func__);
if( ast ) ast_push(ast, AST_TYPE_IN, v, NULL);
#endif
```

**Purpose**: Builds an Abstract Syntax Tree for IN operator subqueries.

**Why**: Comdb2 can parallelize or distribute IN operator evaluation across cluster nodes when the AST is available.

### 5.4 Subroutine Flag Clearing (Line 2899)

**Modified Code**:
```c
if( addrOnce && !sqlite3ExprIsConstant(pE2) ){
  sqlite3VdbeChangeToNoop(v, addrOnce);
  ExprClearProperty(pExpr, EP_Subrtn);  // Added for Comdb2
  addrOnce = 0;
}
```

**Purpose**: Clears the subroutine flag when an expression can't be cached.

**Why**: Prevents incorrect bytecode optimization when expressions contain variable values.

---

## 6. Rename Object Support Disabled

### 6.1 Disable Token Remapping (Lines 484-486, 1701-1705, 1747-1751)

**Pattern**:
```c
#if !defined(SQLITE_BUILDING_FOR_COMDB2)
sqlite3RenameTokenRemap(pParse, pRet, pVector);
#endif
```

**Purpose**: Disables SQLite's ALTER TABLE RENAME tracking mechanism.

**Why**: Comdb2 has its own schema change mechanism that handles renaming through a different system (likely using `schemachange` operations). SQLite's rename tracking would conflict with Comdb2's distributed schema evolution.

---

## 7. Select Statement Duplication

### 7.1 Recording Field (Lines 1545-1547)

**Added Code**:
```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
pNew->recording = p->recording;
#endif
```

**Purpose**: Preserves the `recording` field when duplicating SELECT statements.

**Why**: Comdb2 uses the `recording` flag to track whether query execution should be logged or traced for debugging. This must be preserved across SELECT duplications during query optimization.

---

## 8. Window Function Handling

### 8.1 Updated Window Gathering (Lines 1368-1382)

**Modified Code**:
```c
if( pExpr->op==TK_FUNCTION && ExprHasProperty(pExpr, EP_WinFunc) ){
  Select *pSelect = pWalker->u.pSelect;
  Window *pWin = pExpr->y.pWin;
  assert( pWin );
  assert( pWin->ppThis==0 );
  if( pSelect->pWin ){
    pSelect->pWin->ppThis = &pWin->pNextWin;
  }
  pWin->pNextWin = pSelect->pWin;
  pWin->ppThis = &pSelect->pWin;
  pSelect->pWin = pWin;
}
```

**Purpose**: Enhanced window function list management with proper back-pointers.

**Why**: This is actually a bug fix from upstream SQLite that Comdb2 incorporated. It ensures proper cleanup and traversal of window function lists.

---

## 9. Expression Description Utilities

### 9.1 Large Code Addition (Lines 4913-6579)

**Major Functions Added**:

1. **`print_mem(Mem *m)`** (Lines 5630-5702)
   - Converts SQLite memory cells to human-readable strings
   - Handles all Comdb2 types: NULL, String, Integer, Real, Interval, Blob, Datetime

2. **`binary_op(int op)`** (Lines 5704-5891)
   - Converts token operators to string representations (e.g., TK_EQ → "=")

3. **`sqlite3ExprDescribe_inner()`** (Lines 5896-6558)
   - Recursively converts expression trees to SQL strings
   - Handles all expression types including:
     - Logical operators (AND, OR, NOT)
     - Comparison operators (=, <, >, <=, >=, !=)
     - Arithmetic operators (+, -, *, /, %)
     - Special constructs (BETWEEN, IN, CASE)
     - Functions and aggregates
     - Variables and parameters

4. **Public Wrappers**:
   - `sqlite3ExprDescribe()`: Compile-time expression description
   - `sqlite3ExprDescribeAtRuntime()`: Runtime expression description (evaluates values)
   - `sqlite3ExprDescribeParams()`: Expression description with parameter tracking

### Purpose:
These utilities enable:
- **Query logging**: Human-readable query representation in logs
- **Query explain**: Debugging and optimization analysis
- **Distributed query planning**: Sending SQL fragments to remote nodes
- **Parameter extraction**: Building parameterized queries for reuse

### Special Features:

**NOW() Function Handling** (Lines 6396-6431):
```c
if(strncasecmp(pExpr->u.zToken, "now", 3) == 0) {
  if(atRuntime) {
    dttz_t dt;
    int prec;
    timespec_to_dttz(&v->tspec, &dt, prec);
    return sqlite3_mprintf("cast(%lld.%*.*u as datetime)",
                           dt.dttz_sec, prec, prec, dt.dttz_frac);
  } else {
    return sqlite3_mprintf("now()");
  }
}
```

**Why**: The NOW() function needs special handling because:
- At runtime: Converts to actual timestamp for distributed query consistency
- At parse time: Keeps as function call for proper planning

**Parameter Tracking** (Lines 6298-6314):
```c
if (pParamsOut) {
    *pParamsOut = dohsql_params_append(pParamsOut, pExpr->u.zToken,
                                       pExpr->iColumn);
    if (pExpr->u.zToken[0] == '?') {
        return sqlite3_mprintf("?%d", (*pParamsOut)->nparams);
    }
    return sqlite3_mprintf("%s", pExpr->u.zToken);
}
```

**Why**: Tracks parameters for prepared statement pooling and distributed query execution. Unnamed parameters (`?`) are converted to numbered form (`?1`, `?2`, etc.).

---

## 10. Memory Comparison Utilities

### 10.1 Include memcompare.c (Line 4914)

**Added Code**:
```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
#include <memcompare.c>
#endif
```

**Purpose**: Includes Comdb2's custom memory comparison functions.

**Why**: Used in `exprCompareVariable()` function (line 4944) for comparing bound parameter values. Comdb2's comparison functions handle its extended type system correctly.

---

## 11. Integer Expression Handling

### 11.1 Enhanced sqlite3ExprIsInteger (Lines 2118-2178)

**Modified Signature**:
```c
// Before:
int sqlite3ExprIsInteger(Expr *p, int *pValue);

// After:
int sqlite3ExprIsInteger(Expr *p, int *pValue, Parse *pParse);
```

**New TK_VARIABLE Case** (Lines 2145-2174):
```c
case TK_VARIABLE: {
  sqlite3_value *pVal;
  if( pParse==0 ) break;
  if( NEVER(pParse->pVdbe==0) ) break;
  if( (pParse->db->flags & SQLITE_EnableQPSG)!=0 ) break;
  sqlite3VdbeSetVarmask(pParse->pVdbe, p->iColumn);
  pVal = sqlite3VdbeGetBoundValue(pParse->pReprepare, p->iColumn,
                                  SQLITE_AFF_BLOB);
  if( pVal ){
    if( sqlite3_value_type(pVal)==SQLITE_INTEGER ){
      sqlite3_int64 vv = sqlite3_value_int64(pVal);
      if( vv == (vv & 0x7fffffff) ){ /* non-negative numbers only */
        *pValue = (int)vv;
        rc = 1;
      }
    }
    sqlite3ValueFree(pVal);
  }
  break;
}
```

**Purpose**: Allows bound parameters to be treated as integer constants during query planning.

**Why**: Enables better optimization when parameters are bound to integer values. The check prevents usage with Query Plan Stability Guarantee (QPSG) mode to avoid plan variations.

**Updated Callers** (Lines 922, 928, 2133, 2138):
All callers now pass `0` or `pParse` as the third parameter, depending on whether parameter binding should be considered.

---

## Why These Modifications Were Made

### Distributed Database Architecture
Comdb2 is a distributed, clustered database system. Many modifications support:
- **Query routing**: Sending queries to the correct database node
- **Schema distribution**: Propagating schema changes across the cluster
- **Replication**: Tracking row changes with genids and timestamps

### Extended Type System
Comdb2 supports SQL:2011 temporal types that SQLite doesn't:
- **DATETIME** with timezone support
- **INTERVAL** types (Year-Month and Day-Second)
- **DECIMAL** fixed-point arithmetic

### Query Optimization
Expression description utilities enable:
- **Distributed query planning**: Breaking queries into sub-queries for different nodes
- **Query caching**: Generating cache keys from query structure
- **Remote execution**: Sending SQL to foreign databases

### Debugging and Monitoring
The comprehensive logging and expression printing support:
- **Query logging**: Human-readable query representation
- **Performance analysis**: Understanding query execution patterns
- **Troubleshooting**: Diagnosing issues in distributed environments

### Backend Integration
Modifications like disabling affinity on defaults and using special rowid opcodes integrate SQLite's frontend with Comdb2's custom storage engine.

---

## Conclusion

The modifications to `expr.c` transform SQLite from a standalone embedded database into the SQL frontend for a distributed database cluster. The changes are carefully isolated with preprocessor directives to maintain compatibility with upstream SQLite while adding the features necessary for Comdb2's architecture.

Key themes:
- **Type system extensions** for temporal and fixed-point arithmetic
- **Distributed query support** through expression serialization
- **Custom rowid handling** for cluster-wide unique identifiers
- **Backend integration** through specialized opcodes and affinity handling
