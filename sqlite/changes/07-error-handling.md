# Error Handling Extensions

## Summary

Comdb2 extends SQLite's error codes with **16+ additional error codes** for distributed database scenarios, including deadlocks, schema changes, timeouts, and resource limits.

## Files Modified

- `sqlite/src/main.c` - Error message definitions
- `sqlite/src/sqlite.h.in` - Error code definitions
- `sqlite/src/vdbeapi.c` - Error handling in statement execution
- `sqlite/src/vdbe.c` - Error generation during execution

## How To

### Handling Comdb2 Errors

```c
int rc = sqlite3_step(stmt);
switch(rc) {
    case SQLITE_ROW:
        // Process row
        break;
    case SQLITE_DONE:
        // Query complete
        break;
    case SQLITE_COMDB2SCHEMA:
        // Schema changed - reprepare statement
        sqlite3_reset(stmt);
        sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
        break;
    case SQLITE_DEADLOCK:
        // Deadlock - retry transaction
        sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL);
        break;
    case SQLITE_TIMEDOUT:
        // Query timed out - abort
        break;
}
```

### Error Messages

```sql
-- Get error message
SELECT sqlite3_errmsg(db);  -- Returns human-readable message
```

## Error Codes

| Code | Value | Message |
|------|-------|---------|
| `SQLITE_DEADLOCK` | 200 | Transaction aborted due to deadlock |
| `SQLITE_ACCESS` | 201 | Access denied |
| `SQLITE_TRANTOOCOMPLEX` | 204 | Transaction rollback too large |
| `SQLITE_TRAN_CANCELLED` | 205 | Unable to maintain snapshot, too many resources blocked |
| `SQLITE_TRAN_NOLOG` | 206 | Unable to maintain snapshot, too many log files |
| `SQLITE_TRAN_NOUNDO` | 207 | Database changed due to sc or fastinit; snapshot failure |
| `SQLITE_CONV_ERROR` | 208 | Type conversion failure |
| `SQLITE_COMDB2SCHEMA` | 209 | Table schema changed |
| `SQLITE_CLIENT_CHANGENODE` | 210 | Non-durable write |
| `SQLITE_DDL_MISUSE` | 211 | Overlapping tables detected in transactional DDL |
| `SQLITE_TIMEDOUT` | 212 | Query timed out |
| `SQLITE_COST_TOO_HIGH` | 213 | Query cost too high |
| `SQLITE_NO_TEMPTABLES` | 214 | Temporary tables disallowed |
| `SQLITE_NO_TABLESCANS` | 215 | Table scans disallowed |
| `SQLITE_MISSING_SEMI` | (varies) | Mandatory semicolon is missing |

## Why

### Distributed Database Scenarios

- **Deadlocks**: Multiple nodes accessing same data
- **Schema Changes**: Schema replicated across nodes
- **Snapshots**: MVCC across distributed data

### Resource Governance

- **Timeouts**: Prevent runaway queries
- **Cost Limits**: Reject expensive queries
- **Restrictions**: Enforce operational policies

### Client Communication

- **Specific Errors**: Clients can handle errors appropriately
- **Retry Logic**: Distinguish retryable from fatal errors
- **User Messages**: Meaningful error explanations

## Implementation Details

### Error Code Range
Comdb2 errors start at 200 to avoid conflicts with SQLite's range:
```c
#define SQLITE_DEADLOCK         200
#define SQLITE_ACCESS           201
// ... etc
```

### Error Messages
```c
// In main.c - sqlite3ErrStr()
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
```

### Schema Change Handling
```c
// In vdbeapi.c - sqlite3Step()
if( rc == SQLITE_COMDB2SCHEMA ){
    // Don't retry - return immediately
    sqlite3_mutex_leave(db->mutex);
    return rc;
}

// In vdbeapi.c - modified assertion
#if !defined(SQLITE_BUILDING_FOR_COMDB2)
assert( rc==SQLITE_ROW || rc==SQLITE_DONE || rc==SQLITE_ERROR
     || (rc&0xff)==SQLITE_BUSY || rc==SQLITE_MISUSE );
#endif
// Comdb2 has additional valid return codes
```

### SQLITE_BUSY Debugging
```c
// Enhanced logging for SQLITE_BUSY
if( rc == SQLITE_BUSY ){
    logmsg(LOGMSG_ERROR, "%s:%d SQLITE_BUSY\n", __FILE__, __LINE__);
    cheap_stack_trace();
}
```

## Error Categories

### Retryable Errors
- `SQLITE_DEADLOCK` - Retry transaction
- `SQLITE_COMDB2SCHEMA` - Reprepare and retry
- `SQLITE_BUSY` - Wait and retry

### Non-Retryable Errors
- `SQLITE_ACCESS` - Permission denied
- `SQLITE_CONV_ERROR` - Fix data types
- `SQLITE_DDL_MISUSE` - Fix DDL logic

### Policy Errors
- `SQLITE_TIMEDOUT` - Query took too long
- `SQLITE_COST_TOO_HIGH` - Query too expensive
- `SQLITE_NO_TEMPTABLES` - Policy violation
- `SQLITE_NO_TABLESCANS` - Policy violation

## Upgrade Considerations

When upgrading SQLite:
1. **New Error Codes**: Ensure no conflicts with Comdb2 range (200+)
2. **Error Assertions**: Remove/modify assertions that check error ranges
3. **Step Function**: Preserve Comdb2 error handling in sqlite3Step()
4. **Error String Function**: Maintain aComdb2Msg array in sqlite3ErrStr()
