# attach.c Modifications

## Summary

Major modifications to support **comdb2's federated database (FDB)** feature, allowing remote database access with custom attach/detach semantics.

## Modifications

### 1. Additional Headers (Lines 14-19)
```c
#include <string.h>
#include "logmsg.h"
```

### 2. New comdb2_dynamic_attach Function (Lines 70-83)
The `attachFunc` is refactored into `comdb2_dynamic_attach` with additional parameters:
- `zName`, `zFile` - passed explicitly instead of extracted from argv
- `pzErrDyn` - error message output
- `version` - schema version
- `class` - machine class for routing
- `local` - local vs remote flag
- `class_override` - class override setting
- `proto_version` - protocol version

### 3. Extended Database Structure (Lines 102-110)
```c
char *dbName;
char *tblName;
int  iFndDb;
```

Comdb2 stores dbname and table name separately since the name includes `dbname.tablename`.

### 4. Database Name Parsing (Lines 156-182)
```c
dbName = sqlite3DbStrDup(db, zName);
char *ptr = strchr(dbName, '.');
if( ptr ){
  *ptr = '\0';
  tblName = (ptr+1);
}
```

Parses the `dbname.tablename` format used by comdb2's federated queries.

### 5. Reopen Existing Database (Lines 200-214)
```c
if( i!=db->nDb ) {
  rc = sqlite3BtreeReopen(dbName, db->aDb[iFndDb].pBt);
  // Update metadata...
  goto done_with_open;
}
```

If a database with the same name is already attached, it's reopened rather than creating a new entry.

### 6. Extended Db Structure Fields (Lines 259-265)
```c
pNew->zDbSName = dbName;
pNew->class = class;
pNew->class_override = class_override;
pNew->local = local;
pNew->version = proto_version;
```

Additional fields for federated database routing.

### 7. Single Table Initialization (Lines 351-390)
```c
rc = sqlite3InitTable(db, &zErrDyn, zTmp);
// ...
p = sqlite3HashFind(&db->aDb[iFndDb].pSchema->tblHash, tblName);
p->version = version;
p->iDb = iFndDb;
```

Comdb2 initializes only the specific table rather than the entire database, with support for LOCAL and class-override prefixes.

### 8. Wrapper attachFunc (Lines 422-450)
The standard `attachFunc` is reimplemented to call `comdb2_dynamic_attach`.

### 9. New comdb2_dynamic_detach Function (Lines 452-459)
```c
void comdb2_dynamic_detach(sqlite3 *db, int idx);
```

A public detach function for programmatic use.

### 10. Modified detachFunc (Lines 519-528)
Detach uses `comdb2_dynamic_detach` for the actual cleanup.

## Why These Changes

Comdb2's **federated database (FDB)** feature allows queries to access tables in remote comdb2 databases transparently. This requires:

1. **Name parsing**: Tables are referenced as `remote_db.table_name`
2. **Class routing**: Requests can be routed to specific machine classes (tiers)
3. **Version tracking**: Per-table schema versioning for cache invalidation
4. **Protocol versioning**: For backwards-compatible remote communication
5. **Efficient reopen**: Reusing existing connections rather than creating new ones
