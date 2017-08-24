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

extern pthread_key_t lock_key;

#ifdef LOCK_DEBUG

#define Pthread_mutex_lock(mutex_ptr)                                          \
    {                                                                          \
        fprintf(stderr, "pthread_mutex_lock try (%d)  %s:%d\n", mutex_ptr,     \
                __FILE__, __LINE__);                                           \
        if (pthread_mutex_lock(mutex_ptr) != 0) {                              \
            fprintf(stderr, "lock failed: %s\n", strerror(errno));             \
            abort();                                                           \
        }                                                                      \
        fprintf(stderr, "pthread_mutex_lock got (%d)  %s:%d\n", mutex_ptr,     \
                __FILE__, __LINE__);                                           \
    }

#define Pthread_mutex_unlock(mutex_ptr)                                        \
    {                                                                          \
        fprintf(stderr, "pthread_mutex_unlock(%d)  %s:%d\n", mutex_ptr,        \
                __FILE__, __LINE__);                                           \
        if (pthread_mutex_unlock(mutex_ptr) != 0) {                            \
            fprintf(stderr, "unlock failed: %s\n", strerror(errno));           \
            abort();                                                           \
        }                                                                      \
    }

#else

#define Pthread_mutex_lock(mutex_ptr)                                          \
    {                                                                          \
        int pt_mutex_rcode;                                                    \
        if ((pt_mutex_rcode = pthread_mutex_lock(mutex_ptr)) != 0) {           \
            fprintf(stderr, "pthread_mutex_lock rcode is %d\n",                \
                    pt_mutex_rcode);                                           \
            abort();                                                           \
        }                                                                      \
    }

#define Pthread_mutex_unlock(mutex_ptr)                                        \
    {                                                                          \
        int pt_mutex_rcode;                                                    \
        if ((pt_mutex_rcode = pthread_mutex_unlock(mutex_ptr)) != 0) {         \
            fprintf(stderr, "pthread_mutex_unlock rcode is %d\n",              \
                    pt_mutex_rcode);                                           \
            abort();                                                           \
        }                                                                      \
    }

#endif /* LOCK_DEBUG */

/* Acquire the write lock.  If the current thread already holds the bdb read
 * lock then it is upgraded to a write lock.  If it already holds the write
 * lock then we just increase our reference count. */
void bdb_get_writelock(bdb_state_type *bdb_state, const char *idstr,
                       const char *funcname, int line);

/* Acquire the write lock from the replication thread */
void bdb_get_writelock_abort_waiters(bdb_state_type *bdb_state,
                                     const char *idstr, const char *funcname,
                                     int line);

/* Acquire the read lock.  Multiple threads can hold the read lock
 * simultaneously.  If a thread acquires the read lock twice it is reference
 * counted.  If a thread that holds the write lock calls this then it
 * continues to hold the write lock but with a higher reference count. */
void bdb_get_readlock(bdb_state_type *bdb_state, const char *idstr,
                      const char *funcname, int line);

/* Release the lock of either type (decrements reference count, releases
 * actual lock if reference count hits zero). */
void bdb_rellock(bdb_state_type *bdb_state, const char *funcname, int line);

/* Catches thread specific lock info structs that were not released in the
 * proper way through a call to bdb_thread_event(). */
void bdb_lock_destructor(void *ptr);

/* Initialise the locking specific stuff in a parent bdb_state. */
void bdb_lock_init(bdb_state_type *bdb_state);

/* Logging functions */
void bdb_locks_dump(bdb_state_type *bdb_state, FILE *out);
void bdb_dump_my_lock_state(FILE *out);

#define FILEID_LEN 20
#define MINMAXFLUFF_LEN 10
#define KEYFLUFF_LEN 12
#define SHORT_TABLENAME_LEN 28
#define SHORT_SEQNAME_LEN 30
#define TABLE_CRC_LEN 4
#define SEQ_CRC_LEN 4
#define ROWLOCK_FLUFF_LEN 2

#define ROWLOCK_KEY_SIZE (FILEID_LEN + ROWLOCK_FLUFF_LEN + 8)
#define MINMAX_KEY_SIZE (FILEID_LEN + MINMAXFLUFF_LEN + 1)
#define KEYLOCK_KEY_SIZE (FILEID_LEN + KEYFLUFF_LEN + BDB_KEY_MAX)
#define IXHASH_KEY_SIZE (FILEID_LEN + ROWLOCK_FLUFF_LEN + 8)
#define STRIPELOCK_KEY_SIZE (FILEID_LEN)
#define TABLELOCK_KEY_SIZE (SHORT_TABLENAME_LEN + TABLE_CRC_LEN)
#define SEQLOCK_KEY_SIZE (SHORT_SEQNAME_LEN + SEQ_CRC_LEN)

/* Make sure these are different */
BB_COMPILE_TIME_ASSERT(rowlock_sizes_row_minmax,
                       ROWLOCK_KEY_SIZE != MINMAX_KEY_SIZE);

/* Make sure that this is the same */
BB_COMPILE_TIME_ASSERT(ixhash_key_size, ROWLOCK_KEY_SIZE == IXHASH_KEY_SIZE);

#define BDB_WRITELOCK(idstr)                                                   \
    bdb_get_writelock(bdb_state, (idstr), __func__, __LINE__)
#define BDB_WRITELOCK_REP(idstr)                                               \
    bdb_get_writelock_abort_waiters(bdb_state, (idstr), __func__, __LINE__)
#define BDB_READLOCK(idstr)                                                    \
    bdb_get_readlock(bdb_state, (idstr), __func__, __LINE__)
#define BDB_RELLOCK() bdb_rellock(bdb_state, __func__, __LINE__)

void bdb_checklock(bdb_state_type *bdb_state);

int bdb_lock_table_read(bdb_state_type *, tran_type *);

int bdb_lock_table_read_fromlid(bdb_state_type *, int lid);
int bdb_lock_seq_read_fromlid(bdb_state_type *, const char *seq, void *lk,
                              int lid);
int bdb_lock_seq_write_fromlid(bdb_state_type *, const char *seq, int lid);
int berkdb_lock_random_rowlock(bdb_state_type *bdb_state, int lid, int flags,
                               void *lkname, int mode, void *lk);
int berkdb_lock_rowlock(bdb_state_type *bdb_state, int lid, int flags,
                        void *lkname, int mode, void *lk);

#endif
