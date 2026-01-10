# SQLite wherecode.c Modifications for Comdb2

## Overview

This document analyzes the modifications made to SQLite's `wherecode.c` for the Comdb2 project. The `wherecode.c` file is responsible for generating VDBE (Virtual Database Engine) code that processes the WHERE clause of SQL statements. These modifications adapt SQLite's WHERE clause optimization and code generation to work with Comdb2's distributed database architecture, particularly for remote cursor handling and seek scan optimizations.

## Summary of All Modifications

The modifications to `wherecode.c` fall into several categories:

1. **LIKE Optimization Disabling** - Disables BLOB matching for LIKE operators
2. **Cursor Hints for Remote Databases** - Special handling of cursor hints for remote SQL execution
3. **Seek Scan Optimization Tuning** - Configurable seek scan behavior with `gbl_seekscan_maxsteps`
4. **Skip Scan Flagging** - Added OPFLAG_SKIPSCAN for skip-scan operations
5. **Deferred Seek Modifications** - Changes to DbMaskAllZero calls for write mask checking
6. **SeekRowid Changes** - Additional P5 parameter for non-unique row seeks
7. **Early-Out Optimization** - Modified WHERE_IN_EARLYOUT logic and SeekHit operations
8. **Index Scan Adjustments** - Removed conditional table seeking logic

---

## 1. LIKE Optimization Changes

### Location
Lines 778, 812-814, 1690, 1706, 2349, 2351, 2358

### Modified Code
```c
#if !defined(SQLITE_BUILDING_FOR_COMDB2) && !defined(SQLITE_LIKE_DOESNT_MATCH_BLOBS)
/*
** If the most recently coded instruction is a constant range constraint
** (a string literal) that originated from the LIKE optimization, then
** set P3 and P5 on the OP_String opcode so that the string will be cast
** to a BLOB at appropriate times.
*/
static void whereLikeOptimizationStringFixup(...) {
    // ... function implementation
}
#else
# define whereLikeOptimizationStringFixup(A,B,C)
#endif
```

### Purpose
The LIKE optimization in vanilla SQLite runs the range scan loop twice - once for strings and once for BLOBs. This modification disables the BLOB matching portion of LIKE optimization for Comdb2.

### Rationale
- **Performance**: Comdb2 doesn't need the dual-pass LIKE optimization since it handles LIKE operations differently
- **Consistency**: Ensures LIKE operators only match string types, not BLOBs
- **Simplification**: Reduces code complexity by using a no-op macro when BLOB matching isn't needed

---

## 2. Cursor Hint Modifications

### Location
Lines 907-911, 935-937, 950-971, 973-983, 994-996, 999-1010, 1049-1064, 1090-1094, 1491-1495, 1738-1740, 1757-1759, 2281-2285

### Modified Code

#### 2.1 Skipping Column Index Translation
```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
    /* NOTE: this breaks remote sql; discussion in progress to push this upstream */
#else
    pExpr->iColumn = sqlite3ColumnOfIndex(pHint->pIdx, pExpr->iColumn);
#endif
```

#### 2.2 Additional iLevel Parameter
```c
static void codeCursorHint(
    struct SrcList_item *pTabItem,
    WhereInfo *pWInfo,
    WhereLevel *pLevel,
    WhereTerm *pEndRange
#if defined(SQLITE_BUILDING_FOR_COMDB2)
    ,int iLevel  /* Needed to skip non-remote cursors from hinting */
#endif
)
```

#### 2.3 Remote Cursor Detection
```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
    /* Really need to run this only for remote cursors */
    /* hack, at this point only remcurs have it */
    if( pWInfo->pTabList->a[iLevel].zDatabase==NULL )
        return;
#endif
```

#### 2.4 Modified Term Filtering
```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
    /* msk makes sure I consider only terms that apply to the cursor in question */
    if( !(pTerm->prereqAll & msk)) continue;

    /* TERM_CODED commented out to still encode equality operations properly */
    if( pTerm->wtFlags & (TERM_VIRTUAL/*|TERM_CODED*/) ) continue;
#else
    if( pTerm->wtFlags & (TERM_VIRTUAL|TERM_CODED) ) continue;
#endif
```

#### 2.5 Loop Term Filtering Logic
```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
    /* All terms in pWLoop->aLTerm[] except pEndRange are used to initialize
    ** the cursor.  No need to hint initialization terms. */
    if( pTerm!=pEndRange ){
        for(j=0; j<pWLoop->nLTerm && pWLoop->aLTerm[j]!=pTerm; j++){}
        if( j<pWLoop->nLTerm ) continue;
    }
#else
    /* All terms in pWLoop->aLTerm[] except pEndRange are used to initialize
    ** the cursor.  These terms are not needed as hints for a pure range
    ** scan (that has no == terms) so omit them. */
    if( pLoop->u.btree.nEq==0 && pTerm!=pEndRange ){
        for(j=0; j<pLoop->nLTerm && pLoop->aLTerm[j]!=pTerm; j++){}
        if( j<pLoop->nLTerm ) continue;
    }
#endif
```

### Purpose
Cursor hints are optimizations that tell the storage engine what constraints to expect. Comdb2's modifications ensure cursor hints work correctly with remote database access.

### Rationale

1. **Remote SQL Compatibility**: The column index translation (`sqlite3ColumnOfIndex`) breaks remote SQL execution. Comdb2 needs direct column references for remote queries.

2. **Selective Hinting**: Only remote cursors (identified by `zDatabase != NULL`) need cursor hints. Local cursors are handled differently in Comdb2's architecture.

3. **Predicate Regeneration**: The commented-out `TERM_CODED` flag allows equality predicates to be regenerated for remote cursors, even if they've already been coded locally. This is essential for pushing predicates to remote nodes.

4. **Proper Filtering**: The bitmask (`msk`) ensures only terms relevant to the current cursor are considered, preventing cross-contamination between different query levels.

5. **Upstream Discussion**: As noted in the comment, there's ongoing discussion with the SQLite team about upstreaming these changes, indicating potential architectural differences between SQLite and Comdb2's cursor hint requirements.

---

## 3. Seek Scan Optimization

### Location
Lines 1276-1278, 1797-1838

### Modified Code

#### 3.1 Global Configuration Variable
```c
#ifdef SQLITE_BUILDING_FOR_COMDB2
int gbl_seekscan_maxsteps = -1;
#endif
```

#### 3.2 Tunable Seek Scan Logic
```c
#if defined SQLITE_BUILDING_FOR_COMDB2
    /* gbl_disable_seekscan_optimization turns off seekscans completely.  This
    ** is a bit more lenient - we can keep them on, but get rid of, or adjust,
    ** the maximum number of steps that we try to do on the index before doing
    ** the next find.  This is a three-way switch. The default setting of -1
    ** lets SQLite keep its setting that estimates the max number of steps by
    ** max number of rows it expects to traverse.  0 disables the seekscan
    ** optimization (OP_SeekScan), but still does a seekscan, doing a find for
    ** each next record.  A positive value caps the scan to that many
    ** operations. */
    if (gbl_seekscan_maxsteps == 0) {
        addrSeekScan = 0;
        sqlite3VdbeAddOp0(v, OP_Noop);
    } else if (gbl_seekscan_maxsteps > 0) {
        addrSeekScan = sqlite3VdbeAddOp1(v, OP_SeekScan, gbl_seekscan_maxsteps);
    } else {
#endif
        addrSeekScan = sqlite3VdbeAddOp1(v, OP_SeekScan,
                                         (pIdx->aiRowLogEst[0] + 9) / 10);
#if defined SQLITE_BUILDING_FOR_COMDB2
    }

    if (pRangeStart || pRangeEnd) {
        sqlite3VdbeChangeP5(v, 1);
        sqlite3VdbeChangeP2(v, addrSeekScan, sqlite3VdbeCurrentAddr(v)+1);
        addrSeekScan = 0;
    }
#endif
```

### Purpose
OP_SeekScan is an optimization that tries to step through index entries rather than doing expensive seeks. Comdb2 needs runtime control over this behavior.

### Seek Scan Modes

1. **Default Mode (`gbl_seekscan_maxsteps = -1`)**
   - Uses SQLite's heuristic: `(pIdx->aiRowLogEst[0] + 9) / 10`
   - Estimates based on expected row count

2. **Disabled Mode (`gbl_seekscan_maxsteps = 0`)**
   - Disables OP_SeekScan optimization
   - Emits OP_Noop instead
   - Still performs seek scans but does a find for each record

3. **Fixed Mode (`gbl_seekscan_maxsteps > 0`)**
   - Caps the number of step operations
   - Useful for controlling worst-case performance

### Rationale

1. **Distributed System Characteristics**: In a distributed database, seeks might have different cost characteristics than in a local SQLite database. Network latency and data distribution affect the seek vs. step trade-off.

2. **Workload Tuning**: Different workloads benefit from different seek scan strategies. The global variable allows runtime tuning without recompilation.

3. **Range Query Handling**: When range constraints exist (`pRangeStart || pRangeEnd`), the P5 flag is set and P2 is adjusted, likely to handle boundary conditions in distributed scans.

---

## 4. Skip Scan Flag

### Location
Lines 717-719

### Modified Code
```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
    sqlite3VdbeChangeP5(v, OPFLAG_SKIPSCAN);
#endif
```

### Context
This modification occurs in `codeAllEqualityTerms()` during skip-scan setup:
```c
pLevel->addrSkip = sqlite3VdbeAddOp4Int(v, (bRev?OP_SeekLT:OP_SeekGT),
                        iIdxCur, 0, regBase, nSkip);
#if defined(SQLITE_BUILDING_FOR_COMDB2)
    sqlite3VdbeChangeP5(v, OPFLAG_SKIPSCAN);
#endif
```

### Purpose
Skip-scan is an optimization where the query optimizer skips over initial columns of an index. The OPFLAG_SKIPSCAN flag signals to Comdb2's storage layer that this is a skip-scan operation.

### Rationale

1. **Storage Engine Notification**: Comdb2's storage engine needs to know when a skip-scan is being performed to handle index traversal correctly in a distributed environment.

2. **Optimization Hints**: The flag might trigger special handling in Comdb2's btree layer or remote query execution.

3. **Performance Tracking**: May be used for monitoring and statistics collection specific to skip-scan operations.

---

## 5. Deferred Seek Modifications

### Location
Lines 1130-1134

### Modified Code
```c
if( (pWInfo->wctrlFlags & WHERE_OR_SUBCLAUSE)
#if defined(SQLITE_BUILDING_FOR_COMDB2)
   && DbMaskAllZero(sqlite3ParseToplevel(pParse)->writeMask, 0)
#else
   && DbMaskAllZero(sqlite3ParseToplevel(pParse)->writeMask)
#endif
){
```

### Purpose
Deferred seeks delay rowid lookups until absolutely necessary. This modification adjusts the write mask checking logic.

### Rationale

1. **API Compatibility**: The additional `0` parameter suggests a change in the `DbMaskAllZero` function signature between SQLite versions or Comdb2's fork.

2. **Write Detection**: In OR subclauses, deferred seeks are only safe when there are no writes. The write mask check ensures this constraint.

3. **Transactional Semantics**: Comdb2's distributed transaction model may require different write mask handling to ensure consistency across nodes.

---

## 6. SeekRowid P5 Parameter

### Location
Lines 1462-1465

### Modified Code
```c
sqlite3VdbeAddOp3(v, OP_SeekRowid, iCur, addrNxt, iRowidReg);
#if defined(SQLITE_BUILDING_FOR_COMDB2)
    if( (pLoop->wsFlags & WHERE_ONEROW) == 0 )
        sqlite3VdbeChangeP5(v, 1);
#endif
```

### Purpose
The P5 parameter on OP_SeekRowid provides additional behavioral hints to the VDBE.

### Rationale

1. **Multi-Row Handling**: When `WHERE_ONEROW` is not set, the query might return multiple rows. The P5 parameter likely signals this to Comdb2's cursor management.

2. **Cursor State**: May affect cursor positioning and buffering strategies for multi-row results.

3. **Remote Execution**: Could be used to optimize remote queries that return multiple rows vs. single-row lookups.

---

## 7. Early-Out Optimization Changes

### Location
Lines 571-573, 575-579, 603-606, 615-617, 1903-1905

### Modified Code

#### 7.1 Unconditional WHERE_IN_EARLYOUT
```c
if( iEq>0 ){
    pLoop->wsFlags |= WHERE_IN_EARLYOUT;
}

#if defined(SQLITE_BUILDING_FOR_COMDB2)
    //if( iEq>0 && (pLoop->wsFlags && WHERE_IN_SEEKSCAN)==0 ){
    //  pLoop->wsFlags |= WHERE_IN_EARLYOUT;
    //}
#endif
```

In the original code (removed):
```c
if( iEq>0 && (pLoop->wsFlags & WHERE_VIRTUALTABLE)==0 ){
    pIn->iBase = iReg - i;
    pIn->nPrefix = i;
    pLoop->wsFlags |= WHERE_IN_EARLYOUT;  // <-- moved earlier
}
```

Now:
```c
if( iEq>0 ){
    pIn->iBase = iReg - i;
    pIn->nPrefix = i;
}
```

#### 7.2 SeekHit After IN Loop Setup
```c
if( iEq>0 ){
    sqlite3VdbeAddOp3(v, OP_SeekHit, pLevel->iIdxCur, 0, iEq);
}
```

#### 7.3 Modified SeekHit in Index Scan
```c
if( pLoop->wsFlags & WHERE_IN_EARLYOUT ){
    sqlite3VdbeAddOp3(v, OP_SeekHit, iIdxCur, nEq, nEq);
}
```

### Purpose
The IN early-out optimization allows the query to stop scanning when it's clear no more matches are possible in an IN(...) constraint.

### Changes Made

1. **Moved Flag Setting**: `WHERE_IN_EARLYOUT` is now set earlier and unconditionally when `iEq>0`
2. **Removed Virtual Table Check**: No longer excludes virtual tables from early-out optimization
3. **Added OP_SeekHit Instructions**: New VDBE instructions to track index hits
4. **Changed SeekHit Parameters**: Now uses `(iIdxCur, nEq, nEq)` instead of `(iIdxCur, 1)`

### Rationale

1. **Simplified Logic**: Removing the virtual table check suggests Comdb2 can use early-out optimization for all table types, possibly because it doesn't use SQLite's virtual table mechanism the same way.

2. **Seek Hit Tracking**: The OP_SeekHit instructions provide feedback to the query optimizer about index effectiveness, helping with adaptive query optimization.

3. **Equality Constraint Count**: Passing `nEq` (number of equality constraints) to OP_SeekHit provides more information for optimization decisions.

4. **Performance**: Early-out can significantly improve performance for IN(...) queries with large lists when only a few values match.

---

## 8. Index Scan Table Seeking

### Location
Lines 1755-1768 (removed), 1910-1913 (simplified)

### Original Code (Removed)
```c
if( (pWInfo->wctrlFlags & WHERE_SEEK_TABLE) || (
    (pWInfo->wctrlFlags & WHERE_SEEK_UNIQ_TABLE)
 && (pWInfo->eOnePass==ONEPASS_SINGLE)
)){
    iRowidReg = ++pParse->nMem;
    sqlite3VdbeAddOp2(v, OP_IdxRowid, iIdxCur, iRowidReg);
    sqlite3VdbeAddOp3(v, OP_NotExists, iCur, 0, iRowidReg);
    VdbeCoverage(v);
}else{
    codeDeferredSeek(pWInfo, pIdx, iCur, iIdxCur);
}
```

### Modified Code
```c
codeDeferredSeek(pWInfo, pIdx, iCur, iIdxCur);
```

### Purpose
This change always uses deferred seeks instead of immediate rowid lookups for index scans.

### Rationale

1. **Consistent Deferred Seek**: Comdb2 always prefers deferred seeks, simplifying the code path and deferring rowid lookups until data is actually needed.

2. **Covering Index Optimization**: By deferring seeks, Comdb2 can better leverage covering indexes where the table lookup might never be needed.

3. **Reduced Overhead**: Immediate seeks (OP_IdxRowid + OP_NotExists) are replaced with a single deferred seek operation, reducing VDBE instruction count.

4. **Distributed Query Efficiency**: In a distributed system, deferring seeks allows better batching and reduces round-trips to remote nodes.

---

## WHERE Clause Code Generation Changes

### Key Modifications

1. **Predicate Pushdown**: Modified cursor hints ensure predicates are properly pushed to remote nodes
2. **Early Termination**: Enhanced IN(...) early-out allows queries to stop sooner
3. **Seek Strategy**: Configurable seek scan provides workload-specific optimization

### Example: IN Clause with Index

**Original SQLite Flow:**
```
1. Set up IN loop
2. Check if virtual table → conditionally set WHERE_IN_EARLYOUT
3. Perform index seek
4. Immediate rowid lookup if needed
```

**Comdb2 Modified Flow:**
```
1. Set up IN loop
2. Unconditionally set WHERE_IN_EARLYOUT
3. Add OP_SeekHit with equality count
4. Perform index seek with configurable seek scan
5. Always defer rowid lookup
```

---

## Index Scan Code Modifications

### 1. Seek Scan Tuning
```
Old: Fixed heuristic based on row estimate
New: Three-mode switch (default/disabled/fixed)
```

### 2. Skip Scan Enhancement
```
Old: Basic OP_SeekGT/OP_SeekLT
New: OP_SeekGT/OP_SeekLT + OPFLAG_SKIPSCAN
```

### 3. Table Access
```
Old: Conditional immediate vs deferred seek
New: Always deferred seek
```

### 4. Cursor Hints
```
Old: Apply to all cursors
New: Apply only to remote cursors (zDatabase != NULL)
```

---

## Summary Table

| Modification | Lines | Purpose | Impact |
|--------------|-------|---------|--------|
| LIKE BLOB Disable | 778-814, 1690-1706, 2349-2358 | Disable BLOB matching in LIKE | Performance, Simplicity |
| Cursor Hints | 907-1064, 1491-1759, 2281-2285 | Remote SQL compatibility | Distributed Query Correctness |
| Seek Scan Tuning | 1276-1278, 1797-1838 | Configurable seek behavior | Performance Tuning |
| Skip Scan Flag | 717-719 | Storage engine notification | Optimization Tracking |
| Deferred Seek API | 1130-1134 | Write mask checking | API Compatibility |
| SeekRowid P5 | 1462-1465 | Multi-row hint | Cursor Management |
| IN Early-Out | 571-617, 1903-1905 | Enhanced early termination | Query Performance |
| Always Defer Seek | 1755-1768, 1910-1913 | Simplified seek strategy | Code Simplification, Performance |

---

## Overall Rationale

These modifications adapt SQLite's WHERE clause code generation for Comdb2's distributed database architecture:

1. **Remote Query Execution**: Cursor hints and predicate handling ensure correct execution across database nodes
2. **Performance Tuning**: Configurable seek scans and early-out optimizations allow workload-specific tuning
3. **Simplified Code Paths**: Removing conditional logic (e.g., always using deferred seeks) reduces complexity
4. **Storage Engine Integration**: Flags like OPFLAG_SKIPSCAN integrate SQLite's query layer with Comdb2's storage layer
5. **Distributed Semantics**: Write mask checking and cursor management ensure correct transactional behavior in a distributed system

The modifications maintain SQLite's query optimization framework while adapting it for Comdb2's unique requirements as a distributed, clustered database system.
