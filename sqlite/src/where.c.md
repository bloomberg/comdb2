# SQLite where.c Modifications for Comdb2

## Summary of All Modifications

This document analyzes the Comdb2-specific modifications to SQLite's `where.c` file, which is the core query optimizer and WHERE clause processor. The modifications are wrapped in `SQLITE_BUILDING_FOR_COMDB2` conditional compilation blocks.

**Total COMDB2-specific code blocks: 40**

The modifications fall into several key categories:
1. Query planner configuration and tuning
2. Index selection and disabled index handling
3. Seek-scan optimization control for distributed tables
4. Shard/partition support
5. Custom index uniqueness checking
6. Sort cost adjustments
7. Debug/tracing enhancements
8. Table metadata tracking

---

## Query Optimizer Changes

### 1. WHERE Clause Tracing (Lines 41-43)

**Modification:**
```c
#if defined(SQLITE_BUILDING_FOR_COMDB2) || defined(SQLITE_TEST) || defined(SQLITE_DEBUG)
/***/ int sqlite3WhereTrace = 0;
#endif
```

**Purpose:**
- Enables WHERE clause tracing in production builds (not just test/debug builds)
- Allows runtime debugging of query optimization in Comdb2

**Why:** Comdb2 needs to debug query planning issues in production environments without recompiling with DEBUG flags.

---

### 2. Seek-Scan Optimization Control (Lines 45-47, 2605-2616)

**Global Variables:**
```c
int gbl_disable_seekscan_optimization = 1;  // Default: disabled
int gbl_sqlite_stat4_scan = 0;             // Default: disabled
```

**In-Operator Optimization Logic (Lines 2605-2616):**
```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
/* NC: Skip SEEKSCAN for remote tables */
}else if( gbl_disable_seekscan_optimization == 0 && nInMul<2 &&
          pProbe->pTable->iDb <= 1 ){
#else /* SQLITE_BUILDING_FOR_COMDB2 */
}else if( nInMul<2 ){
#endif
```

**Purpose:**
- Controls whether IN-operator queries use seek-scan optimization
- Restricts seek-scan to local tables only (`iDb <= 1`)
- Prevents expensive seek operations on remote/distributed tables

**Why:** In Comdb2's distributed architecture, seek-scans on remote shards are prohibitively expensive. The optimization that benefits local databases can hurt performance in distributed scenarios.

**Example Impact:**
```sql
-- Query with IN clause
SELECT * FROM table WHERE id IN (1, 2, 3, 4, 5);

-- With gbl_disable_seekscan_optimization = 1:
-- Uses normal scan even if seek-scan would be marginally better

-- With gbl_disable_seekscan_optimization = 0 and local table:
-- May use seek-scan if cost model suggests it's better
```

---

### 3. Full Table Scan Cost Tuning (Lines 3084-3096)

**Modification:**
```c
#ifdef SQLITE_ENABLE_STAT4
#if defined(SQLITE_BUILDING_FOR_COMDB2)
if( gbl_sqlite_stat4_scan ){
#endif
  pNew->rRun = rSize + 16 - 2*((pTab->tabFlags & TF_HasStat4)!=0);
#if defined(SQLITE_BUILDING_FOR_COMDB2)
}else{
  pNew->rRun = rSize + 16;
}
#endif
#else
  pNew->rRun = rSize + 16;
#endif
```

**Purpose:**
- Allows runtime control over whether STAT4 statistics influence table scan cost
- `gbl_sqlite_stat4_scan = 0`: Full scan cost is `rSize + 16` (3.0*N)
- `gbl_sqlite_stat4_scan = 1`: Full scan cost reduced by 2 if STAT4 exists (2.75*N)

**Why:** Comdb2 may not always have accurate STAT4 statistics in distributed scenarios. This flag allows disabling the STAT4 bias to prevent over-optimistic cost estimates.

---

### 4. Query Planner Effort Control (Lines 4137-4157)

**Modification:**
```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
{
  int planner_effort = comdb2_get_planner_effort();
  WHERETRACE(0x002, ("Planner Effort %d\n", planner_effort));

  /* planner_effort level 1 is the default */
  if( planner_effort>1 ){
    if( planner_effort<8 ){
      mxChoice += nLoop * planner_effort;
    }else if( planner_effort<9 ){
      mxChoice += nLoop * nLoop;
    }else if( planner_effort<10 ){
      mxChoice += nLoop * nLoop * nLoop;
    }else{
      mxChoice += nLoop * nLoop * nLoop * nLoop;
    }
  }
}
#endif
```

**Default Behavior:**
- 1-table: 1 path
- 2-table: 5 paths
- 3+ table: 10 paths

**Enhanced Behavior with planner_effort:**
- Level 1: Standard (default)
- Level 2-7: `mxChoice += nLoop * planner_effort`
- Level 8: `mxChoice += nLoop²`
- Level 9: `mxChoice += nLoop³`
- Level 10+: `mxChoice += nLoop⁴`

**Purpose:**
- Allows tuning the search space for complex queries
- Higher effort = more query plans evaluated = potentially better plan but slower planning

**Why:** Complex distributed queries in Comdb2 may benefit from exploring more query plans to find optimal distributed execution strategies.

---

### 5. Sort Cost Multiplier (Lines 4076-4090, 4264-4268)

**Modifications:**
```c
extern int gbl_sqlite_sortermult;
nRow *= gbl_sqlite_sortermult;
```

**Applied in two locations:**
1. `whereSortingCost()` - calculating sort cost
2. When applying LIMIT

**Purpose:**
- Multiplies the estimated row count used for sort cost calculations
- Penalizes sorting operations more heavily

**Why:** In distributed systems, sorting is more expensive due to network transfer and merge costs. This multiplier accounts for the additional overhead.

---

### 6. Sorter Penalty Adjustment (Lines 4264-4268)

**Modification:**
```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
extern int gbl_sqlite_sorterpenalty;
rCost = sqlite3LogEstAdd(rUnsorted, aSortCost[isOrdered]) + gbl_sqlite_sorterpenalty;
#else
rCost = sqlite3LogEstAdd(rUnsorted, aSortCost[isOrdered]) + 5;
#endif
```

**Purpose:**
- Default SQLite adds fixed penalty of 5
- Comdb2 allows runtime adjustment via `gbl_sqlite_sorterpenalty`
- Encourages plans that avoid sorting by increasing sort cost

**Why:** Distributed sorting is expensive; making it more "expensive" in cost model encourages optimizer to find ordered access paths.

---

## Index Selection Modifications

### 7. Custom Index Uniqueness Check (Lines 569-574)

**Modification:**
```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
int is_unique = is_comdb2_index_unique(pTabList->a[0].zName, pIdx->zName);
if( !IsUniqueIndex(pIdx) && !is_unique ) continue;
#else
if( !IsUniqueIndex(pIdx) ) continue;
#endif
```

**Purpose:**
- Consults Comdb2's schema metadata for index uniqueness
- SQLite's index metadata may not reflect Comdb2's actual constraints

**Why:** Comdb2 has its own schema system. An index may be unique in Comdb2 but not marked as such in SQLite's metadata. This ensures correct optimization.

---

### 8. Disabled Index Filtering (Lines 3039-3050)

**Modification:**
```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
/* if index looks like COMDB2_DISABLED_xxx then skip */
if( pProbe->zName && strncmp(pProbe->zName, "$COMDB2_DISABLED_", 17)==0 ){
#ifdef WHERETRACE_ENABLED
  if( sqlite3WhereTrace&0x4 ){
    sqlite3DebugPrintf("Not using disabled index %s:%s\n",
                       pProbe->pTable->zName, pProbe->zName);
  }
#endif
  continue;
}
#endif
```

**Purpose:**
- Skips indexes with `$COMDB2_DISABLED_` prefix during query planning
- Allows runtime index disabling without schema changes

**Why:** Comdb2 needs to disable problematic indexes without dropping them (e.g., during maintenance, corruption recovery, or testing).

---

### 9. DATACOPY Collation Skip (Lines 2485-2492)

**Modification:**
```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
if(pProbe->azColl!=0
 && pProbe->azColl[saved_nEq]!=0
 && sqlite3_stricmp(pProbe->azColl[saved_nEq],"DATACOPY")==0
){
  return rc;
}
#endif
```

**Purpose:**
- Stops index term matching when encountering DATACOPY collation
- Prevents using index columns beyond the DATACOPY marker

**Why:** Comdb2 uses DATACOPY collation to mark covering index columns that are copies of data columns. These shouldn't participate in WHERE clause matching, only in covering index optimization.

---

## Shard and Distributed Table Support

### 10. Shard Table Constraints (Lines 4859-4866)

**Modification:**
```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
Expr *pNewExpr = pWhere;
if( pTabList->nSrc>0 &&
    comdb2_shard_table_constraints(pParse, pTabList->a[0].zName,
                                   pTabList->a[0].zDatabase, &pNewExpr) ){
  pWhere = pNewExpr;
}
#endif
```

**Purpose:**
- Injects additional WHERE clause constraints for sharded tables
- Adds shard key constraints before query optimization

**Why:** Comdb2's sharding requires specific shard key filters. This ensures queries are properly restricted to relevant shards before optimization begins.

---

### 11. Shard Parallelism Check (Lines 5329-5338)

**Modification:**
```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
sqlite3VdbeAddTable(v, pTab);
/* we are encoding an index access; for the outer most index scan,
   try to parallelize the access */
if( shard_check_parallelism(pIx->tnum) ){
  pParse->rc = SQLITE_SCHEMA;
  pParse->nErr++;
  goto whereBeginError;
}
#endif
```

**Purpose:**
- Checks if index access can be parallelized across shards
- Fails with SQLITE_SCHEMA if parallelization requirements aren't met

**Why:** Comdb2 can parallelize certain index scans across shards. This check ensures the query structure supports parallelization.

---

### 12. Table Metadata Tracking (Lines 5176-5181, 5329)

**Modifications:**
```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
if (!(IsVirtual(pItem->pTab) || pItem->pTab->pSelect)) {
  // add all tables referenced in query even if not needed to execute query
  sqlite3VdbeAddTable(v, pItem->pTab);
}
#endif
```

**Purpose:**
- Registers all tables referenced in query, even if optimized away
- Ensures proper locking and metadata tracking

**Why:** Comdb2 needs to track all referenced tables for:
- Distributed transaction coordination
- Schema lock management
- Query auditing/logging

---

### 13. Virtual Table Lock Tracking (Lines 5245-5250)

**Modification:**
```c
#ifdef SQLITE_BUILDING_FOR_COMDB2
const char *lockname = ((VTable*)pVTab)->pVtab->pModule->systable_lock;
if( lockname && !strncasecmp(lockname, "comdb2_tables", sizeof("comdb2_tables")) )
  v->numPartitionLocks++;
#endif
```

**Purpose:**
- Counts partition locks needed for system table access
- Special handling for `comdb2_tables` system table

**Why:** Comdb2 system tables may require special locking for consistency in distributed environments.

---

## Helper Functions and Infrastructure

### 14. Comdb2 Index Name Normalization (Lines 55-70)

**Function:**
```c
static char *comdb2IndexName(char *src, char *dest)
{
  /* remove leading $ and trailing hash value */
  if( src[0]!='$' ){
    return src;
  }
  int len = strlen(src);
  while( src[--len]!='_' )
    ;
  dest[--len] = '\0';
  while( len ){
    dest[len - 1] = src[len];
    --len;
  }
  return dest;
}
```

**Purpose:**
- Strips Comdb2's index naming prefixes/suffixes
- Format: `$name_hash` → `name`

**Why:** Comdb2 adds metadata to index names. This normalizes them for display/comparison.

---

### 15. VdbeCompare Include (Lines 1122-1124)

**Modification:**
```c
#ifdef SQLITE_ENABLE_STAT3_OR_STAT4
#if defined(SQLITE_BUILDING_FOR_COMDB2)
#include <vdbecompare.c>
#endif
```

**Purpose:**
- Includes additional comparison functions for STAT4 processing

**Why:** Comdb2 may have custom types or comparison semantics that need special handling in statistics.

---

### 16. Deferred Seek Function (Lines 177-183)

**New Function:**
```c
int sqlite3WhereUsesDeferredSeek(WhereInfo *pWInfo){
  return pWInfo->bDeferredSeek;
}
```

**Purpose:**
- Exposes whether query plan uses deferred seek optimization
- Allows code generator to know if index cursor is separate from data cursor

**Why:** Comdb2 code generator may need to know this for proper cursor management in distributed scenarios.

---

### 17. Virtual Table zTable Field (Line 3252)

**Modification:**
```c
pIdxInfo->zTable = pSrc->pTab->zName;
```

**Purpose:**
- Passes table name to virtual table xBestIndex callback
- Not in original SQLite

**Why:** Comdb2's virtual tables need table name for routing/shard determination.

---

### 18. Recording Mode Index Open (Lines 5323-5327)

**Modification:**
```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
if( op!=OP_ReopenIdx && GET_CURSOR_RECORDING(pParse, pLevel->iTabCur) ){
  op = OP_OpenRead_Record;
}
#endif
```

**Purpose:**
- Uses special `OP_OpenRead_Record` opcode when in recording mode
- Allows capturing index access for query replay/debugging

**Why:** Comdb2 has a query recording feature for debugging distributed query issues.

---

### 19. Row Estimate Fix for STAT4 (Line 1294)

**Modification:**
```c
- iUpper = sqlite3LogEstToInt(pIdx->aiRowLogEst[0]);
+ iUpper = pIdx->nRowEst0;
```

**Purpose:**
- Uses direct row estimate instead of log conversion
- More accurate for small tables

**Why:** Bug fix or precision improvement for Comdb2's table size estimates.

---

### 20. Expression Constant Check Fix (Line 2340)

**Modification:**
```c
- if( sqlite3ExprIsInteger(pRight, &k) && k>=(-1) && k<=1 ){
+ if( sqlite3ExprIsInteger(pRight, &k, 0) && k>=(-1) && k<=1 ){
```

**Purpose:**
- Adds third parameter (0) to `sqlite3ExprIsInteger` call
- API change adaptation

**Why:** Comdb2's fork may have modified this API signature for additional behavior.

---

### 21. Subset Cost Adjustment (Lines 2047-2064)

**Modified Logic:**
```c
// Old:
pTemplate->rRun = p->rRun;
pTemplate->nOut = p->nOut - 1;

// New:
pTemplate->rRun = MIN(p->rRun, pTemplate->rRun);
pTemplate->nOut = MIN(p->nOut - 1, pTemplate->nOut);
```

**Purpose:**
- More conservative cost adjustment
- Prevents cost from increasing when adjusting for proper subsets

**Why:** Prevents optimizer from making a plan artificially expensive. Only adjusts downward if needed.

---

### 22. WHERE Loop Cost Comparison (Line 2012)

**Modification:**
```c
+ if( pX->rRun>pY->rRun && pX->nOut>pY->nOut ) return 0;
```

**Purpose:**
- Additional check before determining proper subset relationship
- Prevents false subset determination

**Why:** Improves accuracy of subset detection in complex join scenarios.

---

### 23. Virtual Table Hidden Field Access (Line 4552)

**Modification:**
```c
+ whereInterstageHeuristic(pWInfo);
```

**Purpose:**
- Calls interstage heuristic between first and second path solver runs
- Part of the new whereInterstageHeuristic function (lines 4512-4585)

**Why:** Prevents optimizer from switching from index scan to full table scan just to satisfy ORDER BY.

---

### 24. WHERE End Tracking (Line 5401)

**Modification:**
```c
+ pWInfo->iEndWhere = sqlite3VdbeCurrentAddr(v);
```

**Purpose:**
- Records VDBE address where WHERE clause code generation ends
- Used by indexed expression optimization later

**Why:** Needed for precise opcode rewriting in covering index optimization.

---

### 25. Index Change Tracking (Line 5643)

**Modification:**
```c
+ if( pWInfo->eOnePass==ONEPASS_OFF || !HasRowid(pIdx->pTable) ){
+   last = iEnd;
+ }else{
+   last = pWInfo->iEndWhere;
+ }
```

**Purpose:**
- Uses different endpoint for opcode rewriting depending on query type
- More precise for ONEPASS queries

**Why:** ONEPASS optimization generates different code structure; this ensures only the right opcodes are rewritten.

---

## Summary by Category

### Query Planner Configuration (7 modifications)
1. WHERE trace enable
2. Seek-scan control
3. STAT4 scan control
4. Planner effort tuning
5. Sort cost multiplier
6. Sorter penalty
7. Expression checks

### Index Selection (5 modifications)
8. Custom uniqueness check
9. Disabled index filtering
10. DATACOPY collation
11. Index name normalization
12. Row estimate fix

### Shard/Distributed Support (4 modifications)
13. Shard constraints injection
14. Parallelism checking
15. Table metadata tracking
16. Virtual table table name

### Code Generation (4 modifications)
17. Recording mode support
18. Virtual table locks
19. Deferred seek export
20. WHERE end tracking

### Cost Model Improvements (3 modifications)
21. Subset cost logic
22. Loop cost comparison
23. Interstage heuristic

---

## Why Each Modification Was Likely Made

### Performance in Distributed Environment
Most modifications address the fundamental difference between local and distributed query execution:

- **Seek-scan restriction**: Network latency makes small seeks expensive
- **Sort penalties**: Distributed sorting requires data transfer and merging
- **Table scan costs**: Different cost profile in distributed storage

### Schema Flexibility
Comdb2's dynamic schema management requires:

- **Disabled index support**: Runtime index control without schema changes
- **Custom uniqueness checking**: Comdb2's schema may differ from SQLite's view
- **Table tracking**: All referenced tables must be locked/tracked

### Operational Requirements
Production database needs:

- **Trace in production**: Debug query issues without recompile
- **Planner effort tuning**: Balance planning time vs. plan quality
- **Recording mode**: Capture queries for debugging/replay

### Correctness in Distribution
Distributed correctness requires:

- **Shard constraints**: Ensure queries hit correct shards
- **Parallelism checks**: Validate parallel execution is safe
- **Lock tracking**: Coordinate distributed locks

---

## Configuration Tunables

The modifications expose several runtime configuration knobs:

| Variable | Default | Purpose |
|----------|---------|---------|
| `gbl_disable_seekscan_optimization` | 1 | Disable seek-scan for IN clauses |
| `gbl_sqlite_stat4_scan` | 0 | Use STAT4 for scan cost |
| `gbl_sqlite_sortermult` | ? | Sort row count multiplier |
| `gbl_sqlite_sorterpenalty` | ? | Sort cost penalty |
| `planner_effort` | 1 | Search space size |

These allow Comdb2 operators to tune the optimizer for their specific workload without code changes.
