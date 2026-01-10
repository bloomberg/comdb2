# SQLite insert.c Modifications for Comdb2

## Summary of All Modifications

This document analyzes the modifications made to SQLite's `insert.c` file for the Comdb2 distributed database system. The changes are wrapped in `SQLITE_BUILDING_FOR_COMDB2` preprocessor directives and focus on:

1. **Remote database support** for federated queries
2. **Custom constraint checking** optimizations for UPSERT operations
3. **Record format conversion** between SQLite and Comdb2 formats
4. **Transaction flag management** for distributed operations
5. **AST tracking** for remote database operations
6. **Special opcode handling** for recording cursors

## INSERT Statement Changes

### 1. Conflict Resolution Flags (Lines 653-671)

**What Changed:**
```c
if ((onError != OE_None && onError != OE_Default) || pUpsert) {
    if( onError==OE_Ignore ){       /* INSERT OR IGNORE ... */
      comdb2SetIgnore(v);
    }
    if( onError==OE_Replace ){      /* REPLACE INTO ... */
      comdb2SetReplace(v);
    }
    if( pUpsert ){
      if( pUpsert->pUpsertSet==0 ){ /* INSERT .. ON CONFLICT(..) DO NOTHING */
        comdb2SetIgnore(v);
      }else{                        /* INSERT .. ON CONFLICT(..) DO UPDATE .. */
        comdb2SetUpdate(v);
      }
    }
  }
```

**Why:**
Comdb2 is a distributed database where SQL statements are parsed on a client node but executed on a master node. These flags communicate the conflict resolution strategy to the master node, allowing it to handle:
- `INSERT OR IGNORE` → Set ignore flag
- `REPLACE INTO` → Set replace flag
- `ON CONFLICT DO NOTHING` → Set ignore flag
- `ON CONFLICT DO UPDATE` → Set update flag

### 2. AST Tracking for Remote Inserts (Lines 749-752)

**What Changed:**
```c
ast_t *ast = ast_init(pParse, __func__);
if( ast ) ast_push(ast, AST_TYPE_INSERT, v, (iDb>1) ? pTab : NULL);
```

**Why:**
The condition `(iDb>1)` identifies remote database operations. In SQLite, databases are indexed where:
- `iDb=0` → main database
- `iDb=1` → temp database
- `iDb>1` → attached databases (remote in Comdb2's federated setup)

The AST (Abstract Syntax Tree) tracking helps Comdb2 route INSERT operations to the correct remote database node.

### 3. Remote Database Temporary Table Requirement (Lines 202-216)

**What Changed:**
```c
/*
 * Make a remote insert that reads from the same remote database always use a temporary table
 * (eg INSERT INTO remote.t1 SELECT * FROM remote.t2).
 *
 * The fdb backend protocol expects all cursors to be closed before committing a transaction.
 * A chunk remote insert retains a remote read cursor across chunks. Therefore when using a non-temptable
 * plan, it clearly violates the fdb protocol (it holds onto a read cursor while committing a chunk).
 * A simple solution to overcome this is to always use a temptable to store results of the SELECT,
 * such that there will not be outstanding read cursors when tranaction chunks are committed.
 */
if( iDb>1 ){
  return 1;
}
```

**Why:**
Comdb2 uses chunked transactions for large operations to avoid holding locks too long. For statements like `INSERT INTO remote.t1 SELECT * FROM remote.t2`, forcing a temporary table ensures:
1. SELECT results are materialized locally
2. Remote read cursors can be closed before committing chunks
3. Compliance with the federated database (fdb) protocol

### 4. Recording Cursor Support (Lines 46-54)

**What Changed:**
```c
/* if we are handling a SELECTV, we generate a new opcode so that we
   know to open recording BtCursors at runtime
*/
if( GET_CURSOR_RECORDING(pParse, iCur) ){
  opcode = OP_OpenRead_Record;
}
```

**Why:**
The `OP_OpenRead_Record` opcode indicates special cursor handling, likely for Comdb2's SELECTV feature (stored procedures or versioned selects). Recording cursors may track operations for:
- Audit trails
- Replication
- Stored procedure execution context

### 5. Table Verification After Cursor Open (Lines 66-72)

**What Changed:**
```c
/*
** COMDB2: Open Cursor locks the table, verify cookie after we have
** opened the cursor
*/
sqlite3VdbeAddTable(v, pTab);
```

**Why:**
In Comdb2's distributed environment, schema changes can occur concurrently. The "cookie" is a schema version number. This modification ensures:
1. Lock the table via cursor open
2. Verify the schema hasn't changed since query compilation
3. Prevent issues with stale schema information

## Constraint Handling Modifications

### 1. Skipping Standard Constraint Checks (Lines 1410-1415, 1441-1537)

**What Changed:**
```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
  if( !need_index_checks_for_upsert(pTab, pUpsert, overrideError, 0) ){
    *pbMayReplace = 0;
    return;
  }
#endif

// NOT NULL and CHECK constraints are completely disabled:
#if !defined(SQLITE_BUILDING_FOR_COMDB2)
  /* Test all NOT NULL constraints. */
  ...
  /* Test all CHECK constraints */
  ...
#endif
```

**Why:**
Comdb2 performs constraint checking at a different layer (in the storage engine rather than the SQL layer). This modification:
- Prevents duplicate constraint checking
- Allows Comdb2's engine to handle constraints in a distributed-aware manner
- Reduces overhead by skipping unnecessary SQL-layer validation

The `need_index_checks_for_upsert()` function determines if index-level uniqueness checks are needed, which still must be performed at the SQL layer.

### 2. Selective Uniqueness Checking (Lines 1799-1822)

**What Changed:**
```c
/* At this point, we only need to perform collison detection for the
 * following cases:
 *
 * (1) INSERT .. ON CONFLICT(idx) DO UPDATE ..
 * (2) REPLACE INTO ..
 *
 */
if( (pUpsert && pUpsert->pUpsertSet!=0 && pUpIdx==pIdx) ||         /* Case 1 */
    (overrideError==OE_Replace &&
     is_comdb2_index_unique(pIdx->pTable->zName, pIdx->zName)) ) { /* Case 2 */
  /* Go-ahead and check for UNIQUENESS constraint violation */
  onError = OE_Abort;
} else {
  /* Skip UNIQUENESS constraint violation check */
  ...
  continue;
}
```

**Why:**
Standard SQLite checks all unique indexes, but Comdb2 optimizes by only checking when necessary:
- **Case 1**: UPSERT with DO UPDATE requires knowing which row conflicts
- **Case 2**: REPLACE INTO on unique indexes needs conflict detection

For simple INSERTs, Comdb2's storage layer handles uniqueness, making SQL-layer checks redundant.

### 3. UPSERT Index Tracking (Lines 917-925)

**What Changed:**
```c
if( pUpsert->pUpsertTarget ){
  sqlite3UpsertAnalyzeTarget(pParse, pTabList, pUpsert);
#if defined(SQLITE_BUILDING_FOR_COMDB2)
} else {
  comdb2SetUpsertIdx(v, MAXINDEX+1);
#endif
}
#if defined(SQLITE_BUILDING_FOR_COMDB2)
}else if( onError==OE_Ignore || onError==OE_Replace ){
  comdb2SetUpsertIdx(v, MAXINDEX+1);
#endif
```

**Why:**
The value `MAXINDEX+1` is a sentinel indicating "no specific index target". This tells the master node:
- UPSERT has no specific conflict target (matches any unique constraint)
- OR IGNORE/OR REPLACE applies to all uniqueness violations

### 4. Partition and View Validation (Lines 903-909)

**What Changed:**
```c
if( unlikely(get_dbtable_by_name(pTab->zName) == NULL) ){
  sqlite3ErrorMsg(pParse, "UPSERT not supported on partition or view \"%s\"",
          pTab->zName);
  goto insert_cleanup;
}
```

**Why:**
Comdb2 has special table types (partitions, views) that don't support UPSERT due to:
- Partitions: UPSERT logic becomes complex with data routing across partitions
- Views: No underlying storage to perform atomic upsert operations

## Record Format and Data Handling

### 1. Custom MakeRecord Handling (Lines 2094-2108)

**What Changed:**
```c
if( gbl_sqlite_makerecord_for_comdb2 ){
  /* Store the cursor in P3 of OP_MakeRecord. */
  sqlite3VdbeAddOp2(v, OP_Integer, iDataCur, regRec);
}
sqlite3VdbeAddOp3(v, OP_MakeRecord, regData, pTab->nCol, regRec);
sqlite3SetMakeRecordP5(v, pTab);
if( gbl_sqlite_makerecord_for_comdb2 ){
  /* Light the OPFLAG_MKREC_COMDB2 flag so that the VDBE knows that it needs to
     convert Mem structures to comdb2 row data of the table of cursor P3 */
  sqlite3VdbeChangeP5(v, (sqlite3VdbeGetOp(v, -1)->p5 | OPFLAG_MKREC_COMDB2));
}
```

**Why:**
Comdb2 uses a different on-disk format than SQLite. This modification:
1. Stores the cursor number in the `OP_MakeRecord` instruction
2. Sets the `OPFLAG_MKREC_COMDB2` flag
3. Signals the VDBE to convert SQLite's in-memory format to Comdb2's storage format

The conversion happens in the VDBE runtime, allowing type conversions, byte-order adjustments, and Comdb2-specific encoding.

### 2. Insert Operation Flags (Lines 2128-2135)

**What Changed:**
```c
if (comdb2ForceVerify(pParse->pVdbe)) {
  pik_flags |= OPFLAG_FORCE_VERIFY;
}
if(comdb2IgnoreFailure(pParse->pVdbe)) {
  pik_flags |= OPFLAG_IGNORE_FAILURE;
}
```

**Why:**
These flags control distributed transaction behavior:
- `OPFLAG_FORCE_VERIFY`: Forces verification step even for operations that might skip it (for debugging or strict consistency)
- `OPFLAG_IGNORE_FAILURE`: Allows operation to continue on certain failures (for OR IGNORE semantics in distributed context)

### 3. Preserve Update Flag (Lines 2112-2116)

**What Changed:**
```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
  if( pParse->nested && !pParse->preserve_update ){
#else
  if( pParse->nested ){
#endif
    pik_flags = 0;
  }else{
    pik_flags = OPFLAG_NCHANGE;
    ...
  }
```

**Why:**
The `preserve_update` flag handles nested operations (triggers, foreign keys) that should maintain update semantics. In Comdb2:
- Triggers may need to preserve the update context for distributed consistency
- Normal nested operations clear flags to avoid double-counting changes

## Function Signature Changes

### Modified Signatures for Comdb2-Specific Parameters

Several core functions have extended signatures to pass Comdb2-specific information:

1. **`sqlite3OpenTableAndIndices()`** (Lines 2164-2177)
   - Added: `int onError` and `Upsert *pUpsert`
   - Why: Determines whether to open index cursors based on UPSERT needs

2. **`sqlite3CompleteInsertion()`** (Lines 2031-2034)
   - Added: `int onError` and `Upsert *pUpsert`
   - Why: Controls index insertion based on conflict resolution strategy

3. **Flag Type Change** (Lines 2038-2042)
   - Changed `u8 pik_flags` to `u16 pik_flags`
   - Why: Comdb2 needs additional flag bits beyond SQLite's original 8

## Why These Modifications Were Made

### 1. **Distributed Architecture**
Comdb2 operates as a clustered database where:
- Clients parse SQL locally
- Master node executes operations
- Flags and metadata must be communicated across nodes

### 2. **Performance Optimization**
By delegating constraint checks to the storage layer:
- Reduces redundant checking
- Allows batch validation
- Enables distributed constraint verification

### 3. **Format Compatibility**
SQLite and Comdb2 have different:
- On-disk formats
- Type systems
- Byte ordering requirements

The record conversion layer bridges these differences.

### 4. **Transaction Chunking**
Large operations are split into chunks to:
- Avoid long-running locks
- Prevent transaction log overflow
- Enable incremental progress

Temporary tables and cursor management support this model.

### 5. **Feature Extensions**
Comdb2 adds features beyond SQLite:
- Federated database queries (remote tables)
- Stored procedures (SELECTV)
- Partition tables
- Enhanced UPSERT semantics

### 6. **Schema Safety**
In a distributed system, schema changes can propagate while queries execute. Cookie verification ensures:
- Query compilation and execution see the same schema
- Prevents data corruption from version mismatches
- Enables safe online schema changes

## Conditional Compilation Strategy

The modifications use `#if defined(SQLITE_BUILDING_FOR_COMDB2)` to:
1. Maintain SQLite compatibility (can build both versions from same source)
2. Clearly document Comdb2-specific code
3. Allow upstream SQLite upgrades with minimal merge conflicts
4. Enable debugging by toggling between behaviors

## Related Components

These modifications interact with:
- **VDBE opcodes**: Custom opcodes like `OP_OpenRead_Record`
- **Master node protocol**: Flags communicate with distributed coordinator
- **Storage engine**: Handles actual constraint checking and record format
- **FDB layer**: Manages remote database cursors
- **Schema management**: Cookie verification system
