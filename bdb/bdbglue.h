/*
   Copyright 2024 Bloomberg Finance L.P.

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

#ifndef INCLUDED_BDBGLUE_H
#define INCLUDED_BDBGLUE_H

struct bdb_state_tag;
extern struct bdb_state_tag *gbl_bdb_state;

int bdb_recovery_timestamp_fulfills_log_age_requirement(int32_t timestamp);

/* Acquire the write lock.  If the current thread already holds the bdb read
 * lock then it is upgraded to a write lock.  If it already holds the write
 * lock then we just increase our reference count. */
void bdb_get_writelock(struct bdb_state_tag *, const char *idstr, const char *funcname, int line);

/* Acquire the write lock from the replication thread */
void bdb_get_writelock_abort_waiters(struct bdb_state_tag *, const char *idstr, const char *funcname, int line);

/* Acquire the read lock.  Multiple threads can hold the read lock
 * simultaneously.  If a thread acquires the read lock twice it is reference
 * counted.  If a thread that holds the write lock calls this then it
 * continues to hold the write lock but with a higher reference count. */
int bdb_get_readlock(struct bdb_state_tag *, int trylock, const char *idstr, const char *funcname, int line);

/* Release the lock of either type (decrements reference count, releases
 * actual lock if reference count hits zero). */
void bdb_rellock(struct bdb_state_tag *, const char *funcname, int line);

int bdb_the_lock_desired(void);

#define BDB_WRITELOCK(idstr) bdb_get_writelock(bdb_state, (idstr), __func__, __LINE__)
#define BDB_WRITELOCK_REP(idstr) bdb_get_writelock_abort_waiters(bdb_state, (idstr), __func__, __LINE__)
#define BDB_READLOCK(idstr) bdb_get_readlock(bdb_state, 0, (idstr), __func__, __LINE__)
#define BDB_TRYREADLOCK(idstr) bdb_get_readlock(bdb_state, 1, (idstr), __func__, __LINE__)
#define BDB_RELLOCK() bdb_rellock(bdb_state, __func__, __LINE__)

/*
 * Backend thread event constants.
 */
enum bdb_thr_event {
    BDBTHR_EVENT_DONE_RDONLY = 0,
    BDBTHR_EVENT_START_RDONLY = 1,
    BDBTHR_EVENT_DONE_RDWR = 2,
    BDBTHR_EVENT_START_RDWR = 3
};

void bdb_thread_event(struct bdb_state_tag *, enum bdb_thr_event);
int bdb_am_i_coherent(struct bdb_state_tag *);
int bdb_try_am_i_coherent(struct bdb_state_tag *);
int bdb_is_open(struct bdb_state_tag *);

#endif /* INCLUDED_BDBGLUE_H */
