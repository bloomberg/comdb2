# VDBE Extensions

## Summary

Comdb2 extends the **Virtual Database Engine (VDBE)** with new opcodes, modified instruction behavior, and additional execution context for distributed database operations.

## Files Modified

- `sqlite/src/vdbe.c` - Opcode implementations (~289 conditional blocks)
- `sqlite/src/vdbeaux.c` - VDBE preparation and finalization
- `sqlite/src/vdbe.h` - Opcode definitions
- `sqlite/src/vdbeInt.h` - VDBE internal structures
- `sqlite/src/vdbeapi.c` - Public VDBE API

## New Files

- `sqlite/src/comdb2vdbe.c` - Comdb2-specific VDBE helpers
- `sqlite/src/comdb2vdbe.h` - VDBE helper declarations

## How To

### Understanding VDBE Modifications

The VDBE is SQLite's execution engine. Comdb2 modifies it to:
1. Handle extended data types (datetime, interval, decimal)
2. Support distributed operations across cluster nodes
3. Integrate with Comdb2's storage layer
4. Track operations for auditing and replication

### Key Modified Opcodes

| Opcode | Modification |
|--------|-------------|
| `OP_MakeRecord` | Converts to Comdb2 record format |
| `OP_Column` | Handles extended types, raw/cooked data |
| `OP_Insert` | Distributed insert flags |
| `OP_Transaction` | Distributed transaction handling |
| `OP_OpenRead` | Recording cursor support |

## Why

### Distributed Execution
- Master node needs execution context (flags, hints)
- Distributed transactions require special commit handling
- Remote table access needs cursor management

### Extended Types
- Memory cells must handle datetime, interval, decimal
- Type conversion between SQLite and Comdb2 formats
- Precision and timezone propagation

### Performance
- Raw vs. cooked data optimization (skip deserialization)
- Skip unnecessary constraint checks
- Deferred operations for efficiency

## Implementation Details

### Vdbe Structure Extensions
```c
struct Vdbe {
    // ... standard fields ...
#ifdef SQLITE_BUILDING_FOR_COMDB2
    char *tzname;           // Statement timezone
    int dtprec;             // Datetime precision
    u16 cdb2_flags;         // Comdb2 operation flags
    int numTables;          // Table count for verification
    Table **tbls;           // Table references
    // ... more extensions ...
#endif
};
```

### New Opcodes

#### OP_OpenRead_Record
Opens a cursor with recording enabled (for SELECTV):
```c
case OP_OpenRead_Record: {
    // Opens recording BtCursor for auditing
    SET_CURSOR_RECORDING(pParse, iCur);
    // ... rest of OP_OpenRead logic ...
}
```

#### Modified OP_MakeRecord
```c
case OP_MakeRecord: {
    // Standard record creation...

#ifdef SQLITE_BUILDING_FOR_COMDB2
    if( pOp->p5 & OPFLAG_MKREC_COMDB2 ){
        // Convert to Comdb2 format using cursor info in P3
        convertToComdb2Record(pOut, cursor);
    }
#endif
}
```

### Operation Flags
```c
#define OPFLAG_MKREC_COMDB2    0x40  // Make Comdb2 format record
#define OPFLAG_FORCE_VERIFY    0x80  // Force schema verification
#define OPFLAG_IGNORE_FAILURE  0x100 // Continue on some failures
#define OPFLAG_SKIPSCAN        0x200 // Skip-scan operation
```

### Raw vs. Cooked Data
```c
// In OP_Column
if( gbl_use_raw_data && canUseRawData(pCur) ){
    // Skip deserialization - return raw bytes
    pOut->flags = MEM_Blob;
    pOut->z = rawDataPointer;
} else {
    // Standard deserialization
    deserializeColumn(pCur, pOut);
}
```

### Table Verification
```c
// Track tables for schema verification
void sqlite3VdbeAddTable(Vdbe *v, Table *pTab) {
    v->tbls[v->numTables++] = pTab;
}

// Verify at execution time
void sqlite3VdbeVerifyTables(Vdbe *v) {
    for(int i = 0; i < v->numTables; i++){
        if( tableVersionChanged(v->tbls[i]) ){
            return SQLITE_COMDB2SCHEMA;
        }
    }
}
```

### Distributed Transaction Flags
```c
void comdb2SetIgnore(Vdbe *v) { v->cdb2_flags |= CDB2_IGNORE; }
void comdb2SetReplace(Vdbe *v) { v->cdb2_flags |= CDB2_REPLACE; }
void comdb2SetUpdate(Vdbe *v) { v->cdb2_flags |= CDB2_UPDATE; }
void comdb2SetUpsertIdx(Vdbe *v, int idx) { v->upsertIdx = idx; }
```

### Statement Transfer
```c
// Transfer table references between statements (for triggers)
void sqlite3VdbeTransferTables(Vdbe *pDest, Vdbe *pSrc) {
    // Copy table references from trigger to main statement
}
```

## Error Handling

### Schema Change Error
```c
// In sqlite3Step()
if( rc == SQLITE_COMDB2SCHEMA ){
    // Schema changed during execution
    // Return immediately - don't retry
    return rc;
}
```

### Custom Error Codes
```c
#define SQLITE_DEADLOCK         200
#define SQLITE_ACCESS           201
#define SQLITE_TRANTOOCOMPLEX   204
#define SQLITE_TRAN_CANCELLED   205
#define SQLITE_CONV_ERROR       208
#define SQLITE_COMDB2SCHEMA     209
#define SQLITE_TIMEDOUT         212
```

## Upgrade Considerations

When upgrading SQLite:
1. **Opcode Numbers**: New opcodes may shift numbers - update Comdb2 opcodes
2. **VDBE Structure**: Changes to Vdbe struct affect Comdb2 extensions
3. **Execution Flow**: Changes to sqlite3Step() may affect error handling
4. **Memory Cells**: Mem structure changes require vdbemem.c updates
5. **Flags**: P5 flag usage changes may conflict with Comdb2 flags
