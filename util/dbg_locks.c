/*
   Copyright 2018 Bloomberg Finance L.P.

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


#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <time.h>
#include <errno.h>
#include <pthread.h>
#include <sys/mman.h>
#include "bb_stdint.h"
#include "plhash.h"
#include "pthread_wrap_core.h"

#ifndef BUILDING_TOOLS
#include <mem_util.h>
#include <mem_override.h>
#endif

enum dbg_lock_pthread_type_t {
  DBG_LOCK_PTHREAD_TYPE_NONE = 0x0,
  DBG_LOCK_PTHREAD_TYPE_MUTEX = 0x1,
  DBG_LOCK_PTHREAD_TYPE_RWLOCK = 0x2
};

enum dbg_lock_pthread_flags_t {
  DBG_LOCK_PTHREAD_FLAG_NONE = 0x0,
  DBG_LOCK_PTHREAD_FLAG_LOCKED = 0x1,
  DBG_LOCK_PTHREAD_FLAG_READ_LOCKED = 0x2,
  DBG_LOCK_PTHREAD_FLAG_WRITE_LOCKED = 0x4
};

struct dbg_lock_pthread_outer_pair_t {
  void *obj; /* MUST BE FIRST */
  hash_t *locks;
};

struct dbg_lock_pthread_inner_key_t {
  void *obj;
  pthread_t thread;
  int type;
};

struct dbg_lock_pthread_inner_pair_t {
  struct dbg_lock_pthread_inner_key_t key; /* MUST BE FIRST */
  int nRef;
  int flags;
  const char *file;
  const char *func;
  int line;
};

typedef struct dbg_lock_pthread_outer_pair_t outer_pair_t;
typedef struct dbg_lock_pthread_inner_key_t inner_key_t;
typedef struct dbg_lock_pthread_inner_pair_t inner_pair_t;

static uint64_t dbg_locks_bytes = 0;
static uint64_t dbg_locks_peak_bytes = 0;
static pthread_mutex_t dbg_locks_lk = PTHREAD_MUTEX_INITIALIZER;
static hash_t *dbg_locks = NULL;

#define DBG_SET_NAME(a) snprintf(zBuf, nBuf, "%s", (a))
#define DBG_UNK_NAME(a) snprintf(zBuf, nBuf, "unk:%d", (a))

#define DBG_SET_IKEY(a, b, c, d) do {   \
  memset(&(a), 0, sizeof(inner_key_t)); \
  (a).obj = (b);                        \
  (a).thread = (c);                     \
  (a).type = (d);                       \
} while(0)

#define DBG_MORE_MEMORY(a) do {               \
  dbg_locks_bytes += (a);                     \
  if( dbg_locks_bytes>dbg_locks_peak_bytes ){ \
    dbg_locks_peak_bytes = dbg_locks_bytes;   \
  }                                           \
} while(0)

#define DBG_LESS_MEMORY(a) do {               \
  dbg_locks_bytes -= (a);                     \
} while(0)

/*****************************************************************************/

static void dbg_pthread_type_name(
  char *zBuf,
  size_t nBuf,
  int type
){
  switch( type ){
    case DBG_LOCK_PTHREAD_TYPE_NONE:  { DBG_SET_NAME("none");   return; }
    case DBG_LOCK_PTHREAD_TYPE_MUTEX: { DBG_SET_NAME("mutex");  return; }
    case DBG_LOCK_PTHREAD_TYPE_RWLOCK:{ DBG_SET_NAME("rwlock"); return; }
    default:                          { DBG_UNK_NAME(type);     return; }
  }
}

/*****************************************************************************/

static void dbg_pthread_flag_names(
  char *zBuf,
  size_t nBuf,
  int flags
){
  switch( flags ){
    case DBG_LOCK_PTHREAD_FLAG_NONE:        { DBG_SET_NAME("none");   return; }
    case DBG_LOCK_PTHREAD_FLAG_LOCKED:      { DBG_SET_NAME("locked"); return; }
    case DBG_LOCK_PTHREAD_FLAG_READ_LOCKED: { DBG_SET_NAME("rdlock"); return; }
    case DBG_LOCK_PTHREAD_FLAG_WRITE_LOCKED:{ DBG_SET_NAME("wrlock"); return; }
    default:                                { DBG_UNK_NAME(flags);    return; }
  }
}

/*****************************************************************************/

static int dbg_pthread_dump_inner_pair(
  void *obj,
  void *arg
){
  inner_pair_t *ipair = (inner_pair_t *)obj;
  if( ipair!=NULL ){
    FILE *out = (FILE *)arg;
    char zBuf1[64] = {0};
    char zBuf2[64] = {0};

    dbg_pthread_type_name(zBuf1, sizeof(zBuf1), ipair->key.type);
    dbg_pthread_flag_names(zBuf2, sizeof(zBuf2), ipair->flags);

    logmsgf(LOGMSG_USER,
            out, "[%s @ %s:%d] [%s / %s @ obj:%p, thd:%p] [refs:%d, pair:%p]\n",
            ipair->func, ipair->file, ipair->line, zBuf1, zBuf2, ipair->key.obj,
            (void *)ipair->key.thread, ipair->nRef, (void *)ipair);

    fflush(out);
  }
  return 0;
}

/*****************************************************************************/

static int dbg_pthread_dump_outer_pair(
  void *obj,
  void *arg
){
  outer_pair_t *opair = (outer_pair_t *)obj;
  if( opair!=NULL && opair->locks!=NULL ){
    hash_for(opair->locks, dbg_pthread_dump_inner_pair, arg);
  }
  return 0;
}

/*****************************************************************************/

static int dbg_pthread_free_inner_pair(
  void *obj,
  void *arg
){
  if( obj!=NULL ){
    free(obj);
    DBG_LESS_MEMORY(sizeof(inner_pair_t));
  }
  return 0;
}

/*****************************************************************************/

static int dbg_pthread_free_outer_pair(
  void *obj,
  void *arg
){
  outer_pair_t *opair = (outer_pair_t *)obj;
  if( opair!=NULL ){
    hash_t *locks = opair->locks;
    if( locks!=NULL ){
      hash_for(locks, dbg_pthread_free_inner_pair, NULL);
      hash_clear(locks);
      hash_free(locks);
      DBG_LESS_MEMORY(sizeof(hash_t*));
    }
    free(obj);
    DBG_LESS_MEMORY(sizeof(outer_pair_t));
  }
  return 0;
}

/*****************************************************************************/

void dbg_pthread_dump(
  FILE *out,
  const char *zDesc,
  int bSummaryOnly
){
  pthread_mutex_lock(&dbg_locks_lk);
  logmsgf(LOGMSG_USER, out, "%s (%s): used %" BBPRIu64
          ", peak %" BBPRIu64 "\n", __func__, zDesc,
          dbg_locks_bytes, dbg_locks_peak_bytes);
  if( bSummaryOnly || dbg_locks==NULL ) goto done;
  hash_for(dbg_locks, dbg_pthread_dump_outer_pair, out);
done:
  pthread_mutex_unlock(&dbg_locks_lk);
}

/*****************************************************************************/

static void dbg_pthread_check_init(void){
  pthread_mutex_lock(&dbg_locks_lk);
  if( dbg_locks==NULL ){
    dbg_locks = hash_init(sizeof(void *));
    if( dbg_locks==NULL ) abort();
    DBG_MORE_MEMORY(sizeof(hash_t*));
  }
  pthread_mutex_unlock(&dbg_locks_lk);
}

/*****************************************************************************/

void dbg_pthread_term(void){
  dbg_pthread_dump(stdout, "before cleanup", 1);
  pthread_mutex_lock(&dbg_locks_lk);
  assert( dbg_locks_bytes<=dbg_locks_peak_bytes );
  if( dbg_locks==NULL ) goto done;
  hash_for(dbg_locks, dbg_pthread_free_outer_pair, NULL);
  hash_clear(dbg_locks);
  hash_free(dbg_locks);
  DBG_LESS_MEMORY(sizeof(hash_t*));
  dbg_locks = NULL;
done:
  pthread_mutex_unlock(&dbg_locks_lk);
  dbg_pthread_dump(stdout, "after cleanup", 1);
  pthread_mutex_lock(&dbg_locks_lk);
  assert( dbg_locks_bytes==0 );
  pthread_mutex_unlock(&dbg_locks_lk);
}

/*****************************************************************************/

static void dbg_pthread_add_self(
  void *obj,
  int type,
  int flags,
  const char *file,
  const char *func,
  int line
){
  pthread_mutex_lock(&dbg_locks_lk);
  if( dbg_locks==NULL ) goto done;
  outer_pair_t *opair = hash_find(dbg_locks, &obj);
  if( opair==NULL ){
    opair = calloc(1, sizeof(outer_pair_t));
    if( opair==NULL ) abort();
    DBG_MORE_MEMORY(sizeof(outer_pair_t));
    opair->locks = hash_init(sizeof(inner_key_t));
    if( opair->locks==NULL ) abort();
    DBG_MORE_MEMORY(sizeof(hash_t*));
    opair->obj = obj;
    if( hash_add(dbg_locks, opair)!=0 ) abort();
  }
  pthread_t self = pthread_self();
  inner_key_t ikey;
  DBG_SET_IKEY(ikey, obj, self, type);
  inner_pair_t *ipair = hash_find(opair->locks, &ikey);
  if( ipair==NULL ){
    ipair = calloc(1, sizeof(inner_pair_t));
    if( ipair==NULL ) abort();
    DBG_MORE_MEMORY(sizeof(inner_pair_t));
    ipair->key.obj = obj;
    ipair->key.thread = self;
    ipair->key.type = type;
    ipair->nRef = 1;
    ipair->flags = flags;
    ipair->file = file;
    ipair->func = func;
    ipair->line = line;
    if( hash_add(opair->locks, ipair)!=0 ) abort();
  }else{
    assert( ipair->key.obj==obj );
    assert( ipair->key.thread==self );
    assert( ipair->key.type==type );
    assert( ipair->nRef>0 );
    ipair->nRef++;
    ipair->flags = flags;
    ipair->file = file;
    ipair->func = func;
    ipair->line = line;
  }
done:
  pthread_mutex_unlock(&dbg_locks_lk);
}

/*****************************************************************************/

static void dbg_pthread_remove_self(
  void *obj,
  int type,
  const char *file,
  const char *func,
  int line
){
  pthread_mutex_lock(&dbg_locks_lk);
  if( dbg_locks==NULL ) goto done;
  outer_pair_t *opair = hash_find(dbg_locks, &obj);
  if( opair==NULL ) goto done;
  pthread_t self = pthread_self();
  inner_key_t ikey;
  DBG_SET_IKEY(ikey, obj, self, type);
  inner_pair_t *ipair = hash_find(opair->locks, &ikey);
  if( ipair==NULL ) goto done;
  if( --ipair->nRef==0 ){
    if( hash_del(opair->locks, ipair)!=0 ) abort();
    free(ipair);
    DBG_LESS_MEMORY(sizeof(inner_pair_t));
    if( hash_get_num_entries(opair->locks)==0 ){
      if( hash_del(dbg_locks, &obj)!=0 ) abort();
      hash_for(opair->locks, dbg_pthread_free_inner_pair, NULL);
      hash_clear(opair->locks);
      hash_free(opair->locks);
      DBG_LESS_MEMORY(sizeof(hash_t*));
      free(opair);
      DBG_LESS_MEMORY(sizeof(outer_pair_t));
    }
  }else{
    assert( ipair->key.obj==obj );
    assert( ipair->key.thread==self );
    assert( ipair->key.type==type );
    assert( ipair->nRef>0 );
  }
done:
  pthread_mutex_unlock(&dbg_locks_lk);
}

/*****************************************************************************/

int dbg_pthread_mutex_lock(
  pthread_mutex_t *mutex,
  const char *file,
  const char *func,
  int line
){
  int rc;
  WRAP_PTHREAD_WITH_RC(rc, pthread_mutex_lock, mutex);
  if( rc==0 ){
    dbg_pthread_check_init();
    dbg_pthread_add_self(
      mutex, DBG_LOCK_PTHREAD_TYPE_MUTEX, DBG_LOCK_PTHREAD_FLAG_LOCKED,
      file, func, line
    );
  }
  return rc;
}

/*****************************************************************************/

int dbg_pthread_mutex_trylock(
  pthread_mutex_t *mutex,
  const char *file,
  const char *func,
  int line
){
  int rc;
  rc = wrap_pthread_mutex_trylock(mutex, file, func, line);
  if( rc==0 ){
    dbg_pthread_check_init();
    dbg_pthread_add_self(
      mutex, DBG_LOCK_PTHREAD_TYPE_MUTEX, DBG_LOCK_PTHREAD_FLAG_LOCKED,
      file, func, line
    );
  }
  return rc;
}

/*****************************************************************************/

int dbg_pthread_mutex_timedlock(
  pthread_mutex_t *mutex,
  const struct timespec *abs_timeout,
  const char *file,
  const char *func,
  int line
){
  int rc;
  rc = wrap_pthread_mutex_timedlock(mutex, abs_timeout, file, func, line);
  if( rc==0 ){
    dbg_pthread_check_init();
    dbg_pthread_add_self(
      mutex, DBG_LOCK_PTHREAD_TYPE_MUTEX, DBG_LOCK_PTHREAD_FLAG_LOCKED,
      file, func, line
    );
  }
  return rc;
}

/*****************************************************************************/

int dbg_pthread_mutex_unlock(
  pthread_mutex_t *mutex,
  const char *file,
  const char *func,
  int line
){
  int rc;
  WRAP_PTHREAD_WITH_RC(rc, pthread_mutex_unlock, mutex);
  if( rc==0 ){
    dbg_pthread_remove_self(
      mutex, DBG_LOCK_PTHREAD_TYPE_MUTEX, file, func, line
    );
  }
  return rc;
}

/*****************************************************************************/

int dbg_pthread_rwlock_rdlock(
  pthread_rwlock_t *rwlock,
  const char *file,
  const char *func,
  int line
){
  int rc;
  WRAP_PTHREAD_WITH_RC(rc, pthread_rwlock_rdlock, rwlock);
  if( rc==0 ){
    dbg_pthread_check_init();
    dbg_pthread_add_self(
      rwlock, DBG_LOCK_PTHREAD_TYPE_RWLOCK, DBG_LOCK_PTHREAD_FLAG_READ_LOCKED,
      file, func, line
    );
  }
  return rc;
}

/*****************************************************************************/

int dbg_pthread_rwlock_wrlock(
  pthread_rwlock_t *rwlock,
  const char *file,
  const char *func,
  int line
){
  int rc;
  WRAP_PTHREAD_WITH_RC(rc, pthread_rwlock_wrlock, rwlock);
  if( rc==0 ){
    dbg_pthread_check_init();
    dbg_pthread_add_self(
      rwlock, DBG_LOCK_PTHREAD_TYPE_RWLOCK, DBG_LOCK_PTHREAD_FLAG_WRITE_LOCKED,
      file, func, line
    );
  }
  return rc;
}

/*****************************************************************************/

int dbg_pthread_rwlock_tryrdlock(
  pthread_rwlock_t *rwlock,
  const char *file,
  const char *func,
  int line
){
  int rc;
  rc = wrap_pthread_rwlock_tryrdlock(rwlock, file, func, line);
  if( rc==0 ){
    dbg_pthread_check_init();
    dbg_pthread_add_self(
      rwlock, DBG_LOCK_PTHREAD_TYPE_RWLOCK, DBG_LOCK_PTHREAD_FLAG_READ_LOCKED,
      file, func, line
    );
  }
  return rc;
}

/*****************************************************************************/

int dbg_pthread_rwlock_trywrlock(
  pthread_rwlock_t *rwlock,
  const char *file,
  const char *func,
  int line
){
  int rc;
  rc = wrap_pthread_rwlock_trywrlock(rwlock, file, func, line);
  if( rc==0 ){
    dbg_pthread_check_init();
    dbg_pthread_add_self(
      rwlock, DBG_LOCK_PTHREAD_TYPE_RWLOCK, DBG_LOCK_PTHREAD_FLAG_WRITE_LOCKED,
      file, func, line
    );
  }
  return rc;
}

/*****************************************************************************/

int dbg_pthread_rwlock_timedrdlock(
  pthread_rwlock_t *rwlock,
  const struct timespec *abs_timeout,
  const char *file,
  const char *func,
  int line
){
  int rc;
  rc = wrap_pthread_rwlock_timedrdlock(rwlock, abs_timeout, file, func, line);
  if( rc==0 ){
    dbg_pthread_check_init();
    dbg_pthread_add_self(
      rwlock, DBG_LOCK_PTHREAD_TYPE_RWLOCK, DBG_LOCK_PTHREAD_FLAG_READ_LOCKED,
      file, func, line
    );
  }
  return rc;
}

/*****************************************************************************/

int dbg_pthread_rwlock_timedwrlock(
  pthread_rwlock_t *rwlock,
  const struct timespec *abs_timeout,
  const char *file,
  const char *func,
  int line
){
  int rc;
  rc = wrap_pthread_rwlock_timedwrlock(rwlock, abs_timeout, file, func, line);
  if( rc==0 ){
    dbg_pthread_check_init();
    dbg_pthread_add_self(
      rwlock, DBG_LOCK_PTHREAD_TYPE_RWLOCK, DBG_LOCK_PTHREAD_FLAG_WRITE_LOCKED,
      file, func, line
    );
  }
  return rc;
}

/*****************************************************************************/

int dbg_pthread_rwlock_unlock(
  pthread_rwlock_t *rwlock,
  const char *file,
  const char *func,
  int line
){
  int rc;
  WRAP_PTHREAD_WITH_RC(rc, pthread_rwlock_unlock, rwlock);
  if( rc==0 ){
    dbg_pthread_remove_self(
      rwlock, DBG_LOCK_PTHREAD_TYPE_RWLOCK, file, func, line
    );
  }
  return rc;
}

void dbg_pthread_mprotect_lock(void)
{
   int rc = mprotect(&dbg_locks_lk, sizeof(pthread_mutex_t), PROT_READ);
   if (rc != 0) {
fprintf(stderr, "%s: mprotect failed, rc=%d, errno=%d\n", __func__, rc, errno);
abort();
   }
}

