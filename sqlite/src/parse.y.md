# SQLite parse.y Modifications for Comdb2

## Summary

This document analyzes the modifications made to SQLite's `parse.y` grammar file for the comdb2 database system. The modifications add extensive SQL syntax extensions and modify core SQLite behavior to support comdb2's distributed database architecture and additional features.

The changes are wrapped in `SQLITE_BUILDING_FOR_COMDB2` preprocessor directives, allowing the same codebase to build both standard SQLite and comdb2-enhanced versions.

---

## Major Categories of Modifications

### 1. Transaction Management Restrictions

**Files Modified:** Lines 155-200

**Changes:**
- `BEGIN`, `COMMIT`, `ROLLBACK` statements are disabled and replaced with error messages
- `SAVEPOINT`, `RELEASE`, and `ROLLBACK TO SAVEPOINT` are also disabled

**Custom Error Message:**
```
"BEGIN, COMMIT, and ROLLBACK statements cannot be prepared or executed
directly against SQL engine instances (e.g. via Lua stored procedures, etc).
Instead, a custom set of Lua commands must be used, e.g. db:begin. Other than
Lua, these statements are normally handled directly by code within the subsystem
used to prepare SQL queries for worker threads."
```

**Rationale:** Comdb2 uses a distributed architecture where transaction control must be managed at a higher level than individual SQL statements. This prevents conflicts in distributed transaction coordination and ensures transaction boundaries are managed by the appropriate subsystem.

---

### 2. Enhanced Table Creation Syntax

**Files Modified:** Lines 206-282

**New Syntax Added:**

#### CSC2 Schema Definition
```sql
CREATE TABLE tablename OPTIONS(...) NOSQL 'csc2_schema_definition'
```
- Allows creating tables using comdb2's native CSC2 schema format
- Provides more control over data types and storage options

#### LIKE Clause
```sql
CREATE TABLE new_table LIKE existing_table
```
- Allows creating tables based on existing table structure

#### Table Partitioning
```sql
CREATE TABLE name (...) PARTITIONED BY {TIME PERIOD | MANUAL | COLUMNS | NONE}
```
- **TIME PERIOD:** Automatic time-based partitioning
  - `TIME PERIOD 'daily' RETENTION 7 START '2020-01-01'`
- **MANUAL:** Manual partition management
  - `MANUAL RETENTION 10 START 100`
- **COLUMNS:** Shard-based partitioning
  - `COLUMNS (col1, col2) ON (shard_key)`
- **NONE:** Remove partitioning from merged tables

#### Table Merging
```sql
CREATE TABLE name (...) MERGE tablename
```
- Allows creating views or merged tables from partitioned tables

**Rationale:** Comdb2 needs advanced partitioning for handling large datasets across distributed nodes. CSC2 format provides fine-grained control over storage layout and data types specific to comdb2's architecture.

---

### 3. Column and Constraint Extensions

**Files Modified:** Lines 299-577

**New Column Constraints:**

#### AUTOINCR Constraint
```sql
column_name type AUTOINCR
```
- Column-level auto-increment (distinct from PRIMARY KEY autoincrement)
- Handled by `comdb2AddAutoIncrement()`

#### NULL Handling
```sql
column_name type NULL
column_name type NOT NULL
```
- Explicit NULL constraint tracking via `comdb2AddNull()` and `comdb2AddNotNull()`

#### INDEX Constraint
```sql
column_name type INDEX
```
- Automatically creates a non-unique index on the column

#### FOREIGN KEY Syntax
```sql
column_name type REFERENCES table(columns)
```
- Column-level foreign key constraint

#### DBPAD Option
```sql
column_name type OPTION DBPAD = number
```
- Specifies database padding for the column
- Controls storage alignment and padding

**Rationale:** Comdb2 requires explicit tracking of column properties for schema migration and distributed index management. The DBPAD option is specific to comdb2's storage engine for optimizing data layout.

---

### 4. Enhanced Index Creation

**Files Modified:** Lines 1659-1703

**New Index Features:**

#### Partial Datacopy Indexes
```sql
CREATE INDEX name ON table(cols) OPTION DATACOPY
CREATE INDEX name ON table(cols) INCLUDE ALL
CREATE INDEX name ON table(cols) INCLUDE (col1, col2, ...)
```
- **OPTION DATACOPY:** Full datacopy index (deprecated syntax)
- **INCLUDE ALL:** Include all table columns in index
- **INCLUDE (cols):** Include specific columns (partial datacopy)

#### Temporary Indexes
```sql
CREATE TEMP INDEX name ON table(cols)
```
- Support for temporary indexes (using `temp` parameter)

**Rationale:** Datacopy indexes allow covering indexes where all query data is available from the index itself, eliminating table lookups. This is crucial for query performance in distributed environments where table access may require network hops.

---

### 5. Comprehensive ALTER TABLE Support

**Files Modified:** Lines 1990-2126

**New ALTER TABLE Operations:**

#### Column Operations
```sql
ALTER TABLE name ADD COLUMN column_def
ALTER TABLE name DROP COLUMN column_name
ALTER TABLE name ALTER COLUMN column_name SET DATA TYPE type
ALTER TABLE name ALTER COLUMN column_name SET/DROP DEFAULT
ALTER TABLE name ALTER COLUMN column_name SET/DROP AUTOINCR
ALTER TABLE name ALTER COLUMN column_name SET/DROP NOT NULL
```

#### Constraint Operations
```sql
ALTER TABLE name ADD PRIMARY KEY (cols)
ALTER TABLE name DROP PRIMARY KEY
ALTER TABLE name ADD FOREIGN KEY (cols) REFERENCES table(cols)
ALTER TABLE name DROP FOREIGN KEY constraint_name
ALTER TABLE name ADD CHECK (expression)
ALTER TABLE name DROP CONSTRAINT constraint_name
```

#### Index Operations
```sql
ALTER TABLE name ADD INDEX name (cols)
ALTER TABLE name ADD UNIQUE INDEX name (cols)
ALTER TABLE name DROP INDEX name
```

#### Schema Change Control
```sql
ALTER TABLE name SET COMMIT PENDING
ALTER TABLE name PARTITIONED BY ...
ALTER TABLE name MERGE tablename
ALTER TABLE name ALTER OPTIONS (option_list)
ALTER TABLE name DO NOTHING
```

#### CSC2 Schema Alteration
```sql
ALTER TABLE name OPTIONS(...) NOSQL 'csc2_schema'
```

**Rationale:** Comdb2 requires extensive online schema change capabilities for production systems that cannot afford downtime. The "SET COMMIT PENDING" option allows staged schema changes across a distributed cluster.

---

### 6. Comdb2-Specific DDL Commands

**Files Modified:** Lines 2192-2768

#### EXEC/EXECUTE - Stored Procedure Execution
```sql
EXEC PROCEDURE name(arg1, arg2, ...)
EXECUTE PROCEDURE name(arg1, arg2, ...)
```
- Execute Lua stored procedures

#### GET Commands - Query System State
```sql
GET ALIAS table_name
GET KW                          -- Get all keywords
GET RESERVED KW                 -- Get reserved keywords only
GET NOT RESERVED KW             -- Get non-reserved keywords
GET ANALYZE COVERAGE table
GET ANALYZE THRESHOLD table
```

#### PUT Commands - Configure System
```sql
-- Table-specific settings
PUT ANALYZE COVERAGE table value
PUT ANALYZE THRESHOLD table value
PUT SKIPSCAN ENABLE/DISABLE table
PUT ALIAS oldname newname

-- Global settings
PUT TUNABLE name = value
PUT GENID48 ENABLE/DISABLE
PUT ROWLOCKS ENABLE/DISABLE
PUT PASSWORD 'password' FOR user
PUT PASSWORD OFF FOR user
PUT AUTHENTICATION ON/OFF
PUT TIME PARTITION table RETENTION days
PUT COUNTER table INCREMENT
PUT COUNTER table SET value
PUT SCHEMACHANGE COMMITSLEEP milliseconds
PUT SCHEMACHANGE CONVERTSLEEP milliseconds
PUT DEFAULT PROCEDURE name value
```

#### REBUILD Commands - Index/Table Rebuild
```sql
REBUILD table OPTIONS(...)
REBUILD INDEX table index OPTIONS(...)
REBUILD DATA table OPTIONS(...)
REBUILD DATABLOB table OPTIONS(...)
```

**Rebuild Options:**
- `ODH ON/OFF` - Optimize data header
- `IPU ON/OFF` - In-place update
- `ISC ON/OFF` - Index skip scan
- `FORCE` - Force rebuild
- `PAGEORDER` - Rebuild in page order
- `READONLY` - Readonly mode
- `BLOBFIELD {NONE|RLE|ZLIB|LZ4}` - Blob compression
- `REC {NONE|RLE|CRLE|ZLIB|LZ4}` - Record compression

#### SCHEMACHANGE Control
```sql
SCHEMACHANGE PAUSE table
SCHEMACHANGE RESUME table
SCHEMACHANGE COMMIT table
SCHEMACHANGE ABORT table
```

#### TRUNCATE
```sql
TRUNCATE [TABLE] tablename
TRUNCOPLOG days
```

#### Partition Management
```sql
CREATE RANGE PARTITION ON table WHERE column IN (values)
CREATE [TIME] PARTITION ON table AS name PERIOD 'interval'
    RETENTION days START 'date'
DROP TIME PARTITION name
```

#### ANALYZE Extensions
```sql
ANALYZE [table] [percentage] [OPTIONS {THREADS n, SUMMARIZE n}]
ANALYZE ALL [percentage] [OPTIONS ...]
EXCLUSIVE_ANALYZE table [percentage] [OPTIONS ...]
ANALYZESQLITE [table]      -- Use SQLite analyzer
ANALYZEEXPERT [table]       -- Use expert mode
```

#### GRANT/REVOKE - Permission Management
```sql
GRANT READ|WRITE|DDL ON table TO user
GRANT OP TO user
GRANT USERSCHEMA schema_name TO user
REVOKE READ|WRITE|DDL ON table FROM user
REVOKE OP FROM user
REVOKE USERSCHEMA schema_name FROM user
```

#### Lua Function/Trigger Management
```sql
-- Scalar functions
CREATE LUA SCALAR FUNCTION name [DETERMINISTIC]
DROP LUA SCALAR FUNCTION name

-- Aggregate functions
CREATE LUA AGGREGATE FUNCTION name
DROP LUA AGGREGATE FUNCTION name

-- Triggers and consumers
CREATE LUA TRIGGER name [WITH/WITHOUT SEQUENCE]
    ON (TABLE table FOR events)
CREATE LUA CONSUMER name [WITH/WITHOUT SEQUENCE]
    ON (TABLE table FOR events)
CREATE DEFAULT LUA CONSUMER name ...
DROP LUA TRIGGER name
DROP LUA CONSUMER name
```

**Trigger Events:**
- `INSERT`, `UPDATE`, `DELETE`
- `INSERT OF (columns)` - Column-specific
- `INSERT INCLUDE (columns)` - Include specific columns
- Combinations with `AND`: `INSERT AND UPDATE`

#### Stored Procedures
```sql
CREATE PROCEDURE name NOSQL 'lua_code'
CREATE PROCEDURE name VERSION 'version' NOSQL 'lua_code'
DROP PROCEDURE name [VERSION] version_number
DROP PROCEDURE name [VERSION] 'version_string'
```

**Rationale:** These commands provide administrative and operational control specific to comdb2's architecture:
- **REBUILD** is needed for changing compression/storage options without recreating tables
- **SCHEMACHANGE** controls allow staged rollout of schema changes across distributed nodes
- **Lua extensions** enable custom business logic execution within the database
- **GRANT/REVOKE** provide row-level security and permission management
- **Partitioning** supports time-series and large dataset management

---

### 7. Query Extensions

**Files Modified:** Lines 856-878

#### SELECTV Statement
```sql
SELECTV ... FROM ... WHERE ...
```
- Special SELECT variant that enables query recording/tracing
- Sets `recording = 1` flag on the Select structure
- Used for query analysis and optimization

#### EXPLAIN DISTRIBUTION
```sql
EXPLAIN DISTRIBUTION SELECT ...
```
- Extends EXPLAIN with distribution analysis mode
- Sets `pParse->explain = 3`
- Shows how query execution is distributed across nodes

**Rationale:**
- **SELECTV** helps with query performance analysis in distributed environments
- **EXPLAIN DISTRIBUTION** is crucial for understanding query execution plans across multiple database nodes

---

### 8. DRYRUN Mode

**Files Modified:** Lines 1947-1948

```sql
DRYRUN CREATE TABLE ...
DRYRUN DROP TABLE ...
DRYRUN ALTER TABLE ...
DRYRUN REBUILD ...
```

**Implementation:**
- Sets `pParse->isDryrun = 1` flag
- Allows validation of DDL without executing it
- Available for: CREATE/DROP TABLE, VIEW, INDEX, TRIGGER, ALTER TABLE, REBUILD, PARTITION operations

**Rationale:** Critical safety feature for production systems - allows administrators to validate complex schema changes before execution, preventing costly mistakes in distributed deployments.

---

### 9. UPDATE/DELETE/INSERT Modifications

**Files Modified:** Lines 1138-1261

**Changes:**

#### Removed OR Conflict Resolution
Standard SQLite:
```sql
UPDATE OR REPLACE table SET ...
UPDATE OR IGNORE table SET ...
```

Comdb2:
```sql
UPDATE table SET ...  -- No conflict resolution clause
```
- `orconf` parameter hardcoded to 0 in `sqlite3Update()`
- Comdb2 handles conflicts at a different layer

#### CTE Support Made Optional
- INSERT/UPDATE/DELETE with CTEs only compiled when `SQLITE_OMIT_CTE` is not defined
- Default INSERT/UPDATE/DELETE don't require WITH clause support

**Rationale:** Comdb2's distributed architecture requires different conflict resolution strategies. Removing OR clauses forces applications to use comdb2's native conflict handling mechanisms designed for distributed transactions.

---

### 10. Disabled Features

**Files Modified:** Various

**Removed or Disabled:**

1. **INDEXED BY / NOT INDEXED** (Lines 1069-1072)
   - Query optimizer hints disabled
   - Comdb2 uses its own query optimization strategy

2. **WITHOUT ROWID** tables (Lines 286-295)
   - Standard SQLite syntax removed
   - Comdb2 has different internal row ID handling

3. **CREATE TABLE AS SELECT** (Lines 272-277)
   - Disabled in comdb2
   - Must use explicit CREATE TABLE then INSERT INTO SELECT

4. **Deferred Foreign Key Constraints** (Lines 605-613)
   - NOT DEFERRABLE, DEFERRABLE syntax removed
   - Comdb2 handles foreign key timing differently

5. **Rename Token Mapping** (Multiple locations)
   - `IN_RENAME_OBJECT` checks disabled
   - SQLite's rename infrastructure not used

**Rationale:** These features are either incompatible with comdb2's distributed architecture or have alternative implementations in comdb2's codebase.

---

### 11. Expert Mode Analyzer

**Files Modified:** Lines 126-132, 1976-1980

```c
static void setExpert(Parse *pParse){
  pParse->db->isExpert = 1;
}
```

```sql
ANALYZEEXPERT
ANALYZEEXPERT table
```

**Purpose:** Enables expert-mode statistics collection that provides more detailed analysis for query optimization in distributed environments.

---

### 12. Collation and Expression Extensions

**Files Modified:** Lines 1381-1392

#### COLLATE DATACOPY (Deprecated)
```sql
column COLLATE DATACOPY  -- Shows deprecation error
```
- Old syntax for datacopy indexes
- Error directs users to use `INCLUDE ALL` syntax instead
- Still parsed during schema initialization for backward compatibility

**Rationale:** Migration path from old syntax to new, cleaner INCLUDE syntax while maintaining backward compatibility for existing schemas.

---

### 13. New Token Definitions

**Files Modified:** Lines 380-393, 2893-2909

**Comdb2-Specific Keywords Added:**

**DDL/Schema Keywords:**
- ADD, AGGREGATE, ALIAS, DRYRUN, MERGE, PARTITIONED
- COLUMNS, COVERAGE, THRESHOLD, DETERMINISTIC
- NOSQL, USERSCHEMA

**Storage/Compression:**
- BLOBFIELD, DATACOPY, DBPAD, DATABLOB
- RLE, CRLE, ZLIB, LZ4, NONE
- ODH, IPU, ISC, PAGEORDER, READONLY

**Operations:**
- REBUILD, TRUNCATE, TRUNCOPLOG, BULKIMPORT
- SCHEMACHANGE, COMMITSLEEP, CONVERTSLEEP

**Security:**
- AUTHENTICATION, PASSWORD, GRANT, REVOKE
- READ, WRITE, DDL, OP

**Functions/Procedures:**
- PROCEDURE, EXEC, EXECUTE, FUNCTION, SCALAR
- LUA, TRIGGER, CONSUMER, DETERMINISTIC

**Analysis:**
- ANALYZEEXPERT, ANALYZESQLITE, DISTRIBUTION
- EXCLUSIVE_ANALYZE, SUMMARIZE, THREADS
- SKIPSCAN, TESTDEFAULT, TESTGENSHARD

**Partitioning:**
- PERIOD, RETENTION, START, MANUAL, RANGE

**Miscellaneous:**
- GENID48, ROWLOCKS, COUNTER, INCREMENT, TUNABLE
- FORCE, PENDING, RESUME, PAUSE
- GET, PUT, OPTION, OPTIONS, VERSION
- SEQUENCE, INCLUDE, RESERVED, KW

**Type Conversion Tokens:**
- TO_TEXT, TO_DATETIME, TO_BLOB, TO_NUMERIC
- TO_INT, TO_REAL, TO_DECIMAL
- TO_INTERVAL_YE, TO_INTERVAL_MO, TO_INTERVAL_DY
- TO_INTERVAL_HO, TO_INTERVAL_MI, TO_INTERVAL_SE

---

### 14. Additional Modifications

**Files Modified:** Lines 449-453

#### Type Token Changes
Standard SQLite allows:
```sql
DECIMAL(10,2)  -- precision and scale
```

Comdb2 only allows:
```sql
DECIMAL(10)    -- single parameter only
```
- Second parameter for `typetoken` production removed

**Rationale:** Simplifies type system; scale/precision handled differently in comdb2's storage layer.

---

## Summary of Design Philosophy

The modifications to `parse.y` reveal comdb2's core design principles:

### 1. **Distributed-First Architecture**
- Transaction control moved to higher levels
- Query distribution analysis built into EXPLAIN
- Schema changes designed for coordinated cluster updates

### 2. **Production-Ready Schema Evolution**
- DRYRUN mode for safe testing
- Extensive ALTER TABLE support for online changes
- Schema change pause/resume/commit for controlled rollouts
- CSC2 format for precise storage control

### 3. **Performance Optimization**
- Datacopy/INCLUDE indexes for covering queries
- Multiple compression algorithms for storage efficiency
- Rebuild operations for storage reorganization
- Manual query optimizer control via analysis tools

### 4. **Extensibility via Lua**
- Stored procedures
- User-defined scalar and aggregate functions
- Database triggers and event consumers
- Business logic execution within database

### 5. **Enterprise Features**
- Fine-grained permission system (GRANT/REVOKE)
- User authentication
- Table-level access control (READ/WRITE/DDL)
- Operational schema access (USERSCHEMA)

### 6. **Operational Control**
- Tunable parameters for performance adjustment
- Counter management for sequences
- Partition management for data lifecycle
- Comprehensive rebuild options

### 7. **Backward Compatibility**
- Deprecated syntax still parsed with warnings
- Migration paths from old to new syntax
- CSC2 format alongside SQL DDL

---

## Conclusion

The comdb2 modifications to SQLite's `parse.y` transform SQLite from a single-node embedded database into a distributed, enterprise-ready database system. The changes add:

- **~580 new lines of comdb2-specific grammar rules**
- **~80 new SQL keywords**
- **50+ new SQL command variants**
- **Extensive DDL/DML extensions**

While maintaining:
- SQLite's core SQL syntax for compatibility
- Clear separation via preprocessor directives
- Ability to build both SQLite and comdb2 from same source

The modifications demonstrate a thoughtful approach to extending SQLite while preserving the ability to track upstream SQLite changes and maintain dual build configurations.
