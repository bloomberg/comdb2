# SQLite Customizations for Comdb2

This folder documents all customizations made to SQLite for integration with the Comdb2 database system.

## Overview

Comdb2 uses SQLite as its SQL parsing, optimization, and execution engine, but with significant modifications to support:

- **Distributed database** operations across cluster nodes
- **Custom storage engine** (BerkeleyDB instead of SQLite's btree)
- **Extended data types** (datetime with timezone, intervals, decimal)
- **Enterprise features** (replication, MVCC, schema management)

All modifications are wrapped in `#if defined(SQLITE_BUILDING_FOR_COMDB2)` preprocessor directives to maintain compatibility with upstream SQLite.

## Customization Categories

| # | Document | Description |
|---|----------|-------------|
| 01 | [Extended Data Types](01-extended-data-types.md) | datetime, interval, decimal types |
| 02 | [Federated Database](02-federated-database.md) | Remote table access (FDB) |
| 03 | [Storage Engine](03-storage-engine.md) | BerkeleyDB integration |
| 04 | [SQL Extensions](04-sql-extensions.md) | 80+ new SQL keywords |
| 05 | [VDBE Extensions](05-vdbe-extensions.md) | Custom opcodes and execution |
| 06 | [Tunables System](06-tunables-system.md) | Runtime configuration |
| 07 | [Error Handling](07-error-handling.md) | Distributed error codes |
| 08 | [Lua Integration](08-lua-integration.md) | User-defined functions |
| 09 | [Query Modes](09-query-modes.md) | DRYRUN, SELECTV, source list |
| 10 | [Schema Extensions](10-schema-extensions.md) | CSC2, versioning, indexes |
| 11 | [System Tables](11-system-tables.md) | Virtual tables for metadata |

## File Modifications

### Heavily Modified Files

These files have extensive modifications and are most likely to require attention during upgrades:

| File | Lines Changed | Key Changes |
|------|---------------|-------------|
| `vdbe.c` | ~289 blocks | Extended types, custom opcodes |
| `parse.y` | ~580 lines | 80+ new keywords, DDL routing |
| `sqliteInt.h` | ~200 lines | Structure extensions |
| `build.c` | ~150 blocks | CSC2, DDL handling |
| `vdbemem.c` | ~100 blocks | Type conversions |
| `func.c` | ~80 blocks | Built-in functions |
| `select.c` | ~70 blocks | Query optimization |
| `insert.c` | ~50 blocks | Distributed INSERT |

### New Files

These files are entirely Comdb2 additions:

| File | Purpose |
|------|---------|
| `comdb2build.c/h` | DDL operations |
| `comdb2vdbe.c/h` | VDBE helpers |
| `decimal.c/h` | Decimal type |
| `dttz.c` | Datetime with timezone |
| `md5.c/h` | Query fingerprinting |
| `sqlite_tunables.c/h` | Runtime configuration |
| `sqlite_btree.h` | Btree interface |

## Patches and Documentation

For each modified file, there are corresponding files in `sqlite/src/`:

- `*.patch` - Git-format patch showing exact changes
- `*.md` - Markdown documentation explaining modifications

## Conditional Compilation

All modifications use consistent patterns:

```c
#if defined(SQLITE_BUILDING_FOR_COMDB2)
    // Comdb2-specific code
#else
    // Original SQLite code
#endif

#if !defined(SQLITE_BUILDING_FOR_COMDB2)
    // Code to disable for Comdb2
#endif
```

## Upgrade Strategy

When upgrading SQLite:

1. **Review Changes**: Check SQLite changelog for areas that affect Comdb2 modifications
2. **Apply Patches**: Apply Comdb2 patches to new SQLite version
3. **Resolve Conflicts**: Manual resolution for changed areas
4. **Run Tests**: Full test suite to verify functionality
5. **Update Documentation**: Update .md files for any changes

## Key Compile Flags

```cmake
# Main Comdb2 flag
add_definitions(-DSQLITE_BUILDING_FOR_COMDB2)

# Enabled features
add_definitions(-DSQLITE_ENABLE_COLUMN_METADATA)
add_definitions(-DSQLITE_ENABLE_STAT4)
add_definitions(-DSQLITE_ENABLE_SERIES)

# Disabled features
add_definitions(-DSQLITE_OMIT_WAL)
add_definitions(-DSQLITE_OMIT_AUTOINIT)
add_definitions(-DSQLITE_OMIT_VACUUM)
```

See `sqlite/definitions.cmake` for the complete list.

## External Dependencies

Comdb2 SQLite modifications depend on:

- **BerkeleyDB** - Storage engine
- **decNumber** - Decimal type implementation
- **Lua** - User-defined functions
- **comdb2 core** - Various helper functions

## Contact

For questions about these modifications, refer to the Comdb2 project documentation or contact the development team.
