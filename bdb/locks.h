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

#ifndef INCLUDED_LOCKS_H
#define INCLUDED_LOCKS_H

#include <bdbglue.h>

/* Assert that we hold the bdb writelock at this point in the code */
void bdb_assert_wrlock(bdb_state_type *bdb_state, const char *funcname, int line);

/* Initialise the locking specific stuff in a parent bdb_state. */
void bdb_lock_init(bdb_state_type *bdb_state);

/* Logging functions */
void bdb_locks_dump(bdb_state_type *bdb_state, FILE *out);
void bdb_dump_my_lock_state(FILE *out);

#define FILEID_LEN 20
#define MINMAXFLUFF_LEN 10
#define KEYFLUFF_LEN 12
#define SHORT_TABLENAME_LEN 28
#define TABLE_CRC_LEN 4
#define ROWLOCK_FLUFF_LEN 2

#define ROWLOCK_KEY_SIZE (FILEID_LEN + ROWLOCK_FLUFF_LEN + 8)
#define MINMAX_KEY_SIZE (FILEID_LEN + MINMAXFLUFF_LEN + 1)
#define IXHASH_KEY_SIZE (FILEID_LEN + ROWLOCK_FLUFF_LEN + 8)
#define STRIPELOCK_KEY_SIZE (FILEID_LEN)
#define TABLELOCK_KEY_SIZE (SHORT_TABLENAME_LEN + TABLE_CRC_LEN)

/* Make sure these are different */
BB_COMPILE_TIME_ASSERT(rowlock_sizes_row_minmax, ROWLOCK_KEY_SIZE != MINMAX_KEY_SIZE);

/* Make sure that this is the same */
BB_COMPILE_TIME_ASSERT(ixhash_key_size, ROWLOCK_KEY_SIZE == IXHASH_KEY_SIZE);

void bdb_checklock(bdb_state_type *bdb_state);
int bdb_lock_table_read(bdb_state_type *, tran_type *);
int bdb_lock_table_read_fromlid(bdb_state_type *, int lid);
int bdb_lock_tablename_read_fromlid(bdb_state_type *, const char *name, int lid);
int bdb_lock_table_write_fromlid(bdb_state_type *, int lid);
int berkdb_lock_random_rowlock(bdb_state_type *bdb_state, int lid, int flags, void *lkname, int mode, void *lk);
int berkdb_lock_rowlock(bdb_state_type *bdb_state, int lid, int flags, void *lkname, int mode, void *lk);
void bdb_init_lock_key(void);

#endif
