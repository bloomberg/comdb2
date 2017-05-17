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

#ifndef _BDB_SQLSTAT1
#define _BDB_SQLSTAT1

#include "bdb_api.h"

/**
 * Write the sqlite_stat1 'stat' information for table='tbl', index='idx'
 * to llmeta's backout (prev) table.  If stat is NULL, delete what's there (if
 * anything).
 *
 * Returns: 0 if successful, -1 if error.
 *
 */

int bdb_sqlite_stat1_write_prev(bdb_state_type *bdb_state,
                                tran_type *input_trans, char *tbl, char *idx,
                                char *stat, int *bdberr);

/**
 * Read the sqlite_stat1 'stat' information for table='tbl', index='idx'
 * from llmeta's backout (prev) table.
 *
 * Returns: 0 if successful, 1 if no-information, -1 if error
 *
 */

int bdb_sqlite_stat1_read_prev(bdb_state_type *bdb_state,
                               tran_type *input_trans, char *tbl, char *idx,
                               char **stat, int *bdberr);

/**
 * Temporary functions- delete the stale backup entries from llmeta.  We
 * should be able to remove these completely in a few years .. maybe.
 *
 */

int bdb_sqlite_stat1_delete_stale(bdb_state_type *bdb_state,
                                  tran_type *input_trans, char *tbl, char *idx,
                                  int *bdberr);

#endif
