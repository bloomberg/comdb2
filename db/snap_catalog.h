/*
   Copyright 2026 Bloomberg Finance L.P.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */

#ifndef INCLUDED_SNAP_CATALOG_H
#define INCLUDED_SNAP_CATALOG_H

#include <stdint.h>

struct dbtable;
struct dbview;
struct sqlclntstate;
struct sql_thread;
struct snap_catalog;
struct timepart_views;

/* A schema change is about to become visible.  Call with the schema lock
 * held exclusively, before the commit. */
void snap_catalog_schema_changed(void);

/* Make clnt->dbtran.snap_catalog the tables as of the modsnap start LSN.
 * Sets *id to the id of the catalog, or to 0 if the snapshot sees the
 * current tables.  Call with the schema lock held for read. */
void snap_catalog_prepare(struct sqlclntstate *clnt, uint64_t *id);

/* Copy the sqlite_master entries of the catalog of clnt to thd */
int snap_catalog_copy_rootpages(struct sqlclntstate *clnt, struct sql_thread *thd);

/* The time partitions of the catalog of clnt, or NULL if the snapshot sees
 * the current ones */
struct timepart_views *snap_catalog_views(struct sqlclntstate *clnt);

/* Return 1 and set *db (NULL: no such table) if the snapshot of the current
 * sql thread sees its own tables; return 0 if it sees the current tables. */
int snap_catalog_find_table(const char *name, struct dbtable **db);

/* Return 1 and set *view (NULL: no such view) if the snapshot of the current
 * sql thread sees its own views; return 0 if it sees the current views. */
int snap_catalog_find_view(const char *name, struct dbview **view);

/* 1 if db is a table as a snapshot saw it before a schema change */
int snap_catalog_is_old_table(const struct dbtable *db);

void snap_catalog_free(struct snap_catalog **cat);

#endif
