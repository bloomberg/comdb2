/*
   Copyright 2015 Bloomberg Finance L.P.

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

#ifndef _FDB_FEND_CACHE_H_
#define _FDB_FEND_CACHE_H_

#include "fdb_fend.h"

/**
 * Cache implemented as a decorator pattern
 *
 */

/* open a cursor to the sqlite_stat cache */
fdb_cursor_if_t *fdb_sqlstat_cache_cursor_open(struct sqlclntstate *clnt,
                                               fdb_t *fdb, const char *name,
                                               fdb_sqlstat_cache_t *cache);

/*
   create a cache for a table

   NOTE: this function is not reentrant and it is protected by caller mutex
 */
int fdb_sqlstat_cache_create(struct sqlclntstate *clnt, fdb_t *fdb,
                             const char *fdbname, fdb_sqlstat_cache_t **pcache);

/**
 * Destroy the local cache
 *
 */
void fdb_sqlstat_cache_destroy(fdb_sqlstat_cache_t **pcache);

#endif
