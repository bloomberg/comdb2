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

#if defined(DBG_PTHREAD_LOCKS)

#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <time.h>
#include <pthread.h>
#include "bb_stdint.h"
#include "plhash.h"
#include "pthread_wrap_core.h"

#ifndef BUILDING_TOOLS
#include <mem_util.h>
#include <mem_override.h>
#endif

enum dbg_lock_pthread_type_t {
  DBG_LOCK_PTHREAD_NONE = 0x0,
  DBG_LOCK_PTHREAD_MUTEX = 0x1,
  DBG_LOCK_PTHREAD_RDLOCK = 0x2,
  DBG_LOCK_PTHREAD_WRLOCK = 0x4,
  DBG_LOCK_PTHREAD_RWLOCK = 0x8
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
    case DBG_LOCK_PTHREAD_NONE:  { snprintf(zBuf,nBuf,"%s","none");   return; }
    case DBG_LOCK_PTHREAD_MUTEX: { snprintf(zBuf,nBuf,"%s","mutex");  return; }
    case DBG_LOCK_PTHREAD_RDLOCK:{ snprintf(zBuf,nBuf,"%s","rdlock"); return; }
    case DBG_LOCK_PTHREAD_WRLOCK:{ snprintf(zBuf,nBuf,"%s","wrlock"); return; }
    case DBG_LOCK_PTHREAD_RWLOCK:{ snprintf(zBuf,nBuf,"%s","rwlock"); return; }
    default:                     { snprintf(zBuf,nBuf,"unk:%d",type); return; }
  }
}

/*****************************************************************************/

static int dbg_pthread_dump_inner_pair(
  void *obj,
  void *arg
){
  inner_pair_t *pair = (inner_pair_t *)obj;
  if( pair!=NULL ){
    FILE *out = (FILE *)arg;
    char zBuf[64];

    dbg_pthread_type_name(zBuf, sizeof(zBuf), pair->key.type);

    fprintf(out, "%s: [refs:%4d] [type:%s @ %18p] [%s @ %s:%d] (pair:%18p)\n",
            __func__, pair->nRef, zBuf, pair->key.obj, pair->func, pair->file,
            pair->line, (void *)pair);

    fflush(out);
  }
  return 0;
}

/*****************************************************************************/

static int dbg_pthread_dump_outer_pair(
  void *obj,
  void *arg
){
  outer_pair_t *pair = (outer_pair_t *)obj;
  if( pair!=NULL && pair->locks!=NULL ){
    hash_for(pair->locks, dbg_pthread_dump_inner_pair, arg);
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
  outer_pair_t *pair = (outer_pair_t *)obj;
  if( pair!=NULL ){
    hash_t *locks = pair->locks;
    if( locks!=NULL ){
      hash_for(locks, dbg_pthread_free_inner_pair, NULL);
      hash_clear(locks);
      hash_free(locks);
      DBG_LESS_MEMORY(sizeof(hash_t*));
    }
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
  fprintf(out, "%s (%s): used %" BBPRIu64 ", peak %" BBPRIu64 "\n",
          __func__, zDesc, dbg_locks_bytes, dbg_locks_peak_bytes);
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
  if( dbg_locks==NULL ) goto done;
  hash_for(dbg_locks, dbg_pthread_free_outer_pair, NULL);
  hash_clear(dbg_locks);
  hash_free(dbg_locks);
  DBG_LESS_MEMORY(sizeof(hash_t*));
  dbg_locks = NULL;
  dbg_pthread_dump(stdout, "after cleanup", 1);
  assert( dbg_locks_bytes==0 );
done:
  pthread_mutex_unlock(&dbg_locks_lk);
}

/*****************************************************************************/

static void dbg_pthread_add_self(
  void *obj,
  int type,
  const char *file,
  const char *func,
  int line
){
  pthread_mutex_lock(&dbg_locks_lk);
  if( dbg_locks==NULL ) goto done;
  outer_pair_t *okey = hash_find(dbg_locks, obj);
  if( okey==NULL ){
    okey = calloc(1, sizeof(outer_pair_t));
    if( okey==NULL ) abort();
    DBG_MORE_MEMORY(sizeof(outer_pair_t));
    okey->locks = hash_init(sizeof(inner_key_t));
    if( okey->locks==NULL ) abort();
    DBG_MORE_MEMORY(sizeof(hash_t*));
    okey->obj = obj;
    if( hash_add(dbg_locks, okey)!=0 ) abort();
  }
  pthread_t self = pthread_self();
  inner_key_t ikey = { obj, self, type };
  inner_pair_t *pair = hash_find(okey->locks, &ikey);
  if( pair==NULL ){
    pair = calloc(1, sizeof(inner_pair_t));
    if( pair==NULL ) abort();
    DBG_MORE_MEMORY(sizeof(inner_pair_t));
    pair->key.obj = obj;
    pair->key.thread = self;
    pair->key.type = type;
    pair->nRef = 1;
    pair->file = file;
    pair->func = func;
    pair->line = line;
    if( hash_add(okey->locks, pair)!=0 ) abort();
  }else{
    assert( pair->key.obj==obj );
    assert( pair->key.thread==self );
    assert( pair->key.type==type );
    assert( pair->nRef>0 );
    pair->nRef++;
    pair->file = file;
    pair->func = func;
    pair->line = line;
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
  outer_pair_t *okey = hash_find(dbg_locks, obj);
  if( okey==NULL ) goto done;
  pthread_t self = pthread_self();
  inner_key_t ikey = { obj, self, type };
  inner_pair_t *pair = hash_find(okey->locks, &ikey);
  if( pair!=NULL && --pair->nRef==0 ){
    if( hash_del(okey->locks, pair)!=0 ) abort();
    free(pair);
    DBG_LESS_MEMORY(sizeof(inner_pair_t));
    if( hash_get_num_entries(okey->locks)==0 ){
      if( hash_del(dbg_locks, obj)!=0 ) abort();
      hash_for(okey->locks, dbg_pthread_free_inner_pair, NULL);
      hash_clear(okey->locks);
      hash_free(okey->locks);
      DBG_LESS_MEMORY(sizeof(hash_t*));
      free(okey);
      DBG_LESS_MEMORY(sizeof(outer_pair_t));
    }
  }else{
    assert( pair->key.obj==obj );
    assert( pair->key.thread==self );
    assert( pair->key.type==type );
    assert( pair->nRef>0 );
  }
done:
  pthread_mutex_unlock(&dbg_locks_lk);
}

/*****************************************************************************/

static void dbg_pthread_remove_self2(
  void *obj,
  int type,
  const char *file,
  const char *func,
  int line
){
  int type1 = type;
  int type2 = DBG_LOCK_PTHREAD_NONE;
  if( type==DBG_LOCK_PTHREAD_RWLOCK ){
    type1 = DBG_LOCK_PTHREAD_RDLOCK;
    type2 = DBG_LOCK_PTHREAD_WRLOCK;
  }
  if( type1!=DBG_LOCK_PTHREAD_NONE ){
    dbg_pthread_remove_self(obj, type1, file, func, line);
  }
  if( type2!=DBG_LOCK_PTHREAD_NONE ){
    dbg_pthread_remove_self(obj, type2, file, func, line);
  }
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
    dbg_pthread_add_self(mutex, DBG_LOCK_PTHREAD_MUTEX, file, func, line);
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
    dbg_pthread_add_self(mutex, DBG_LOCK_PTHREAD_MUTEX, file, func, line);
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
    dbg_pthread_add_self(mutex, DBG_LOCK_PTHREAD_MUTEX, file, func, line);
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
    dbg_pthread_remove_self2(mutex, DBG_LOCK_PTHREAD_MUTEX, file, func, line);
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
    dbg_pthread_add_self(rwlock, DBG_LOCK_PTHREAD_RDLOCK, file, func, line);
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
    dbg_pthread_add_self(rwlock, DBG_LOCK_PTHREAD_WRLOCK, file, func, line);
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
    dbg_pthread_add_self(rwlock, DBG_LOCK_PTHREAD_RDLOCK, file, func, line);
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
    dbg_pthread_add_self(rwlock, DBG_LOCK_PTHREAD_WRLOCK, file, func, line);
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
    dbg_pthread_add_self(rwlock, DBG_LOCK_PTHREAD_RDLOCK, file, func, line);
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
    dbg_pthread_add_self(rwlock, DBG_LOCK_PTHREAD_WRLOCK, file, func, line);
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
    dbg_pthread_remove_self2(rwlock, DBG_LOCK_PTHREAD_RWLOCK, file, func, line);
  }
  return rc;
}

#endif /* defined(DBG_PTHREAD_LOCKS) */
