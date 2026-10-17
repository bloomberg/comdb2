# Extended Data Types

## Summary

Comdb2 extends SQLite with additional data types to support enterprise database requirements: **datetime with timezone**, **interval types** (year-month and day-second), and **decimal** (128-bit IEEE 754 decimal floating-point).

## Files Modified

- `sqlite/src/vdbe.c` - VDBE execution for extended types
- `sqlite/src/vdbemem.c` - Memory cell operations for extended types
- `sqlite/src/vdbeapi.c` - Public API functions for extended types
- `sqlite/src/vdbeInt.h` - Mem structure extensions
- `sqlite/src/sqliteInt.h` - Affinity definitions for extended types
- `sqlite/src/expr.c` - Expression handling for extended types
- `sqlite/src/func.c` - Built-in functions for extended types

## New Files

- `sqlite/src/dttz.c` - Datetime with timezone implementation
- `sqlite/src/decimal.c` - Decimal type implementation
- `sqlite/src/decimal.h` - Decimal type header

## How To

### Using Datetime with Timezone

```sql
-- Create table with datetime column
CREATE TABLE events (
    id INT,
    event_time DATETIME
);

-- Insert with timezone
INSERT INTO events VALUES (1, '2024-01-15T10:30:00 America/New_York');

-- Query with timezone conversion
SELECT event_time FROM events;
```

### Using Interval Types

```sql
-- Year-Month intervals
SELECT CAST('3 years 6 months' AS INTERVAL YEAR TO MONTH);

-- Day-Second intervals
SELECT CAST('5 days 12:30:00' AS INTERVAL DAY TO SECOND);

-- Interval arithmetic
SELECT event_time + CAST('1 month' AS INTERVAL YEAR TO MONTH) FROM events;
```

### Using Decimal Type

```sql
-- Create table with decimal column
CREATE TABLE prices (
    id INT,
    amount DECIMAL
);

-- Precise decimal arithmetic
INSERT INTO prices VALUES (1, 19.99);
SELECT amount * 1.08 AS with_tax FROM prices;  -- Exact decimal math
```

## Why

### Business Requirements
- **Financial applications** require exact decimal arithmetic without floating-point rounding errors
- **Global applications** need timezone-aware datetime handling
- **Temporal calculations** require proper interval arithmetic (e.g., "add 3 months" correctly handles varying month lengths)

### SQL Standard Compliance
- DATETIME, INTERVAL, and DECIMAL are standard SQL types
- Enterprise databases (Oracle, DB2, PostgreSQL) provide similar functionality

### Comdb2 Protocol
- These types map directly to Comdb2's CSON wire protocol
- Enables efficient serialization/deserialization between client and server

## Implementation Details

### Affinity Constants
```c
#define SQLITE_AFF_DATETIME  'C'
#define SQLITE_AFF_INTV_YE   'D'  // Year-month interval (year)
#define SQLITE_AFF_INTV_MO   'E'  // Year-month interval (month)
#define SQLITE_AFF_INTV_DY   'F'  // Day-second interval (day)
#define SQLITE_AFF_INTV_HO   'G'  // Day-second interval (hour)
#define SQLITE_AFF_INTV_MI   'H'  // Day-second interval (minute)
#define SQLITE_AFF_INTV_SE   'I'  // Day-second interval (second)
#define SQLITE_AFF_DECIMAL   'M'
```

### Mem Structure Extensions
```c
struct Mem {
    // ... standard SQLite fields ...
#ifdef SQLITE_BUILDING_FOR_COMDB2
    union {
        dttz_t dt;      // Datetime with timezone
        intv_t tv;      // Interval value
        decQuad dec;    // 128-bit decimal
    } du;
    char *tz;           // Timezone name
    int dtprec;         // Datetime precision (second vs microsecond)
#endif
};
```

### Memory Flags
```c
#define MEM_Datetime  0x00010000  // Value is datetime
#define MEM_Interval  0x00020000  // Value is interval
```

## Upgrade Considerations

When upgrading SQLite, ensure:
1. Mem structure extensions are preserved in vdbeInt.h
2. Type affinity constants are maintained in sqliteInt.h
3. Type conversion functions in vdbemem.c are updated for any Mem changes
4. VDBE opcodes that handle types (OP_MakeRecord, OP_Column) preserve extended type handling
