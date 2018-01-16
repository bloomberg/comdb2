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

#ifndef __BDB_OSQLBCKFILL_H_
#define __BDB_OSQLBCKFILL_H_

#include <build/db.h>
#include "bdb_int.h"

/**
 * Backfill interface
 *
 */
struct bfillhndl;
typedef struct bfillhndl bfillhndl_t;

/**
 * Provide a list of "current" lsn-s for the pending
 * rowlock transactions.  It keeps the commit process
 * waiting while this list is created (the lsn for
 * current logical transactions cannot advance )
 * The handle is NULL if no lsn-s are available (no
 * logical transactions for which there is a committed
 * physical transaction).
 * If error, return -1 and set bdberr
 *
 */
int bdb_osqlbkfill_create(bdb_state_type *bdb_state, bfillhndl_t **bhndl,
                          int *bdberr, int epoch, int file, int offset,
                          tran_type *shadow_tran);

/**
 * Retrieve the first lsn for backfill
 * in the provided handle;
 * It is assumed that if there is a handle, there is a first lsn
 * Returns -1 if error and sets bdberr
 *
 */
int bdb_osqlbkfill_firstlsn(bfillhndl_t *bfillhndl, DB_LSN *lsn, int *bdberr);


/**
 * Retrieve the last (lowest) lsn for backfill
 * in the provided handle;
 * It is assumed that if there is a handle, there is a first lsn
 * Returns -1 if error and sets bdberr
 *
 */
int bdb_osqlbkfill_lastlsn(bfillhndl_t *bfillhndl, DB_LSN *lsn, int *bdberr);


/**
 * Retrieve the next lsn for backfill
 * Return 1 if there are no more lsn-s for the current handle
 * Returns -1 if error and sets bdberr
 *
 */
int bdb_osqlbkfill_next_lsn(bfillhndl_t *bfillhndl, DB_LSN *lsn, int *bdberr);

/**
 * Close handle and free the lsn entries
 * Returns -1 if error and sets bdberr
 *
 */
int bdb_osqlbkfill_close(bfillhndl_t *bfillhndl, int *bdberr);

#endif
