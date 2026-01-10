# SQLite Upgrade Session Context

**Last Updated:** 2026-01-09
**Branch:** `sqlite-upgrade`
**Current SQLite Version:** 3.28.0
**Target SQLite Version:** 3.51.2

## Project Context

Comdb2 is a clustered relational database that uses SQLite for:
- SQL parsing (parse.y)
- Query optimization (where*.c, select.c)
- Query execution (vdbe.c, vdbeaux.c)
- Function evaluation (func.c)

SQLite is heavily modified with ~70+ files changed and wrapped in `SQLITE_BUILDING_FOR_COMDB2` conditionals. Key customizations include:
- BerkeleyDB storage engine (replaces SQLite's btree/pager)
- Extended data types: datetime, interval, decimal
- FDB (Federated Database) for distributed queries
- 80+ new SQL keywords in parse.y
- Custom VDBE opcodes and execution hooks
- Tunables system for runtime configuration
- CSC2 schema format support

## What Was Completed

### 1. Source File Documentation (`sqlite/src/`)
- **69 `.patch` files** - Git-diff style patches showing all modifications
- **40+ `.md` files** - Explanations of why each file was modified

### 2. Customization Guide (`sqlite/changes/`)
- 11 documentation files explaining Comdb2's SQLite customizations
- README.md index linking all customization docs
- Covers: data types, FDB, storage engine, SQL extensions, VDBE, tunables, error handling, Lua, query modes, schema extensions, system tables

### 3. Upgrade Roadblocks (`sqlite/roadblocks/`)
- **66 individual release files** (3.29.0 through 3.51.2)
- Each file documents changes, risk levels, and action items
- **SUMMARY.md** - Comprehensive overview with phased upgrade path

## Critical Roadblocks Summary

| Version | Feature | Risk | Key Files |
|---------|---------|------|-----------|
| 3.35.0 | ALTER TABLE DROP COLUMN, UPSERT, RETURNING | Critical | parse.y, insert.c, build.c |
| 3.37.0 | STRICT tables | High | parse.y, build.c, vdbemem.c |
| 3.38.0 | Bloom filters, JSON operators | Critical | where.c, parse.y |
| 3.39.0 | RIGHT/FULL OUTER JOIN | Critical | select.c, where.c |
| 3.45.0 | JSONB rewrite | Critical | func.c, vdbe.c |
| 3.50.0 | sqlite3_setlk_timeout() | Medium-High | vdbeapi.c |

## Recommended Upgrade Path

1. **Phase 1 (3.29.0-3.34.1):** Foundation, lower risk changes
2. **Phase 2 (3.35.0-3.37.2):** Major features, highest complexity
3. **Phase 3 (3.38.0-3.42.0):** JOINs and JSON improvements
4. **Phase 4 (3.43.0-3.51.2):** Stabilization and refinement

## Git Commits

```
7f0013cca SQLite upgrade roadblocks analysis: 3.29.0 to 3.51.2
12d952eb1 SQLite customization analysis: patches, documentation, and changes guide
7394adc7c WIP: Initial SQLite customization analysis - patches and documentation
```

## Untracked Directories

These directories exist but are not committed (reference copies):
- `sqlite/sqlite-3.28.0/` - Upstream SQLite 3.28.0 source (for diffing)
- `sqlite/sqlite-3.51.2/` - Upstream SQLite 3.51.2 source (target version)

## Next Steps (Potential)

1. **Begin actual upgrade work** - Start with Phase 1 releases
2. **Create merge branch** - For each SQLite version increment
3. **Test incrementally** - Run Comdb2 test suite after each version
4. **Update patches** - Regenerate .patch files as conflicts are resolved
5. **Track new roadblocks** - Document any unexpected issues discovered

## Key Files Reference

Most heavily modified Comdb2 SQLite files:
- `sqlite/src/vdbe.c` - ~289 conditional blocks
- `sqlite/src/parse.y` - 80+ new keywords, grammar extensions
- `sqlite/src/sqliteInt.h` - Extended structures, new types
- `sqlite/src/build.c` - DDL routing to Comdb2 schema engine
- `sqlite/src/insert.c` - UPSERT, distributed insert handling
- `sqlite/src/select.c` - ~70 modification blocks
- `sqlite/src/func.c` - Custom functions, datetime/interval/decimal

## Important Compile Flag

All Comdb2 modifications are wrapped in:
```c
#ifdef SQLITE_BUILDING_FOR_COMDB2
// Comdb2-specific code
#endif
```

This flag is defined in `sqlite/definitions.cmake`.
