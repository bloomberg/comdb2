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
#include "comdb2_atomic.h"
#include "plhash.h"

enum dbg_lock_pthread_type_t {
  DBG_LOCK_PTHREAD_NONE = 0x0,
  DBG_LOCK_PTHREAD_MUTEX = 0x1,
  DBG_LOCK_PTHREAD_RDLOCK = 0x2,
  DBG_LOCK_PTHREAD_WRLOCK = 0x4,
  DBG_LOCK_PTHREAD_RWLOCK = 0x8
};

struct dbg_lock_pthread_key_t {
  void *obj;
  pthread_t thread;
  int type;
};

struct dbg_lock_pthread_pair_t {
  struct dbg_lock_pthread_key_t key;
  int nRef;
};

static pthread_mutex_t dbg_locks_lk = PTHREAD_MUTEX_INITIALIZER;
static hash_t *dbg_locks = NULL;

/*****************************************************************************/

static void dbg_pthread_type_name(
  char *zBuf,
  size_t nBuf,
  int type
){
  switch( type ){
    case DBG_LOCK_PTHREAD_NONE:{ snprintf(zBuf,nBuf,"%s","none"); return; }
    case DBG_LOCK_PTHREAD_MUTEX:{ snprintf(zBuf,nBuf,"%s","mutex"); return; }
    case DBG_LOCK_PTHREAD_RDLOCK:{ snprintf(zBuf,nBuf,"%s","rdlock"); return; }
    case DBG_LOCK_PTHREAD_WRLOCK:{ snprintf(zBuf,nBuf,"%s","wrlock"); return; }
    case DBG_LOCK_PTHREAD_RWLOCK:{ snprintf(zBuf,nBuf,"%s","rwlock"); return; }
    default:{ snprintf(zBuf,nBuf,"unknown:%d",type);  return; }
  }
}

/*****************************************************************************/

static int dbg_pthread_dump_pair(
  void *obj,
  void *arg
){
  struct dbg_lock_pthread_pair_t *pair = (struct dbg_lock_pthread_pair_t *)obj;
  if( pair!=NULL ){
    FILE *out = (FILE *)arg;
    char zBuf[64];

    dbg_pthread_type_name(zBuf, sizeof(zBuf), pair->key.type);

    fprintf(out, "%s: [%s %p] [refs:%d] (pair:%p)\n",
            __func__, zBuf, pair->key.obj, pair->nRef, (void *)pair);
  }
  return 0;
}

/*****************************************************************************/

static int dbg_pthread_free_obj(
  void *obj,
  void *arg
){
  if( obj!=NULL ) free(obj);
  return 0;
}

/*****************************************************************************/

static int dbg_pthread_free_hash(
  void *obj,
  void *arg
){
  if( obj!=NULL ){
    hash_t *hash = (hash_t *)obj;
    hash_for(hash, dbg_pthread_free_obj, NULL);
    hash_clear(hash);
    hash_free(hash);
  }
  return 0;
}

/*****************************************************************************/

void dbg_pthread_dump(FILE *out){
  pthread_mutex_lock(&dbg_locks_lk);
  if( dbg_locks==NULL ) goto done;
  hash_for(dbg_locks, dbg_pthread_dump_pair, out);
done:
  pthread_mutex_unlock(&dbg_locks_lk);
}

/*****************************************************************************/

static void dbg_pthread_check_init(void){
  pthread_mutex_lock(&dbg_locks_lk);
  if( dbg_locks==NULL ){
    dbg_locks = hash_init(sizeof(void *));
    if( dbg_locks==NULL ) abort();
  }
  pthread_mutex_unlock(&dbg_locks_lk);
}

/*****************************************************************************/

void dbg_pthread_term(void){
  pthread_mutex_lock(&dbg_locks_lk);
  if( dbg_locks==NULL ) goto done;
  hash_for(dbg_locks, dbg_pthread_free_hash, NULL);
  hash_clear(dbg_locks);
  hash_free(dbg_locks);
  dbg_locks = NULL;
done:
  pthread_mutex_unlock(&dbg_locks_lk);
}

/*****************************************************************************/

static void dbg_pthread_add_self(
  void *obj,
  int type
){
  pthread_mutex_lock(&dbg_locks_lk);
  if( dbg_locks==NULL ) goto done;
  hash_t *objlocks = hash_find(dbg_locks, obj);
  if( objlocks==NULL ){
    objlocks = hash_init(sizeof(void *));
    if( objlocks==NULL ) abort();
  }
  pthread_t self = pthread_self();
  struct dbg_lock_pthread_key_t key = { self, type };
  struct dbg_lock_pthread_pair_t *pair = hash_find(objlocks, &key);
  if( pair==NULL ){
    pair = calloc(1, sizeof(struct dbg_lock_pthread_pair_t));
    if( pair==NULL ) abort();
    pair->key.obj = obj;
    pair->key.thread = self;
    pair->key.type = type;
    pair->nRef = 1;
    if( hash_add(objlocks, &pair->key)!=0 ) abort();
  }else{
    assert( pair->key.obj==obj );
    assert( pair->key.thread==self );
    assert( pair->key.type==type );
    assert( pair->nRef>0 );
    pair->nRef++;
  }
done:
  pthread_mutex_unlock(&dbg_locks_lk);
}

/*****************************************************************************/

static void dbg_pthread_remove_self(
  void *obj,
  int type
){
  pthread_mutex_lock(&dbg_locks_lk);
  if( dbg_locks==NULL ) goto done;
  hash_t *objlocks = hash_find(dbg_locks, obj);
  if( objlocks==NULL ) goto done;
  pthread_t self = pthread_self();
  struct dbg_lock_pthread_key_t key = { self, type };
  struct dbg_lock_pthread_pair_t *pair = hash_find(objlocks, &key);
  if( pair!=NULL && --pair->nRef==0 ){
    if( hash_del(objlocks, &pair->key)!=0 ) abort();
    free(pair);
    if( hash_get_num_entries(objlocks)==0 ){
      if( hash_del(dbg_locks, obj)!=0 ) abort();
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
  int type
){
  int type1 = type;
  int type2 = DBG_LOCK_PTHREAD_NONE;
  if( type==DBG_LOCK_PTHREAD_RWLOCK ){
    type1 = DBG_LOCK_PTHREAD_RDLOCK;
    type2 = DBG_LOCK_PTHREAD_WRLOCK;
  }
  if( type1!=DBG_LOCK_PTHREAD_NONE ) dbg_pthread_remove_self(obj, type1);
  if( type2!=DBG_LOCK_PTHREAD_NONE ) dbg_pthread_remove_self(obj, type2);
}

/*****************************************************************************/

int dbg_pthread_mutex_lock(
  pthread_mutex_t *mutex
){
  int rc = pthread_mutex_lock(mutex);
  if( rc==0 ){
    dbg_pthread_check_init();
    dbg_pthread_add_self(mutex, DBG_LOCK_PTHREAD_MUTEX);
  }
  return rc;
}

/*****************************************************************************/

int dbg_pthread_mutex_trylock(
  pthread_mutex_t *mutex
){
  int rc = pthread_mutex_trylock(mutex);
  if( rc==0 ){
    dbg_pthread_check_init();
    dbg_pthread_add_self(mutex, DBG_LOCK_PTHREAD_MUTEX);
  }
  return rc;
}

/*****************************************************************************/

int dbg_pthread_mutex_timedlock(
  pthread_mutex_t *mutex,
  const struct timespec *abs_timeout
){
  int rc = pthread_mutex_timedlock(mutex, abs_timeout);
  if( rc==0 ){
    dbg_pthread_check_init();
    dbg_pthread_add_self(mutex, DBG_LOCK_PTHREAD_MUTEX);
  }
  return rc;
}

/*****************************************************************************/

int dbg_pthread_mutex_unlock(
  pthread_mutex_t *mutex
){
  int rc = pthread_mutex_unlock(mutex);
  if( rc==0 ) dbg_pthread_remove_self2(mutex, DBG_LOCK_PTHREAD_MUTEX);
  return rc;
}

/*****************************************************************************/

int dbg_pthread_rwlock_rdlock(
  pthread_rwlock_t *rwlock
){
  int rc = pthread_rwlock_rdlock(rwlock);
  if( rc==0 ){
    dbg_pthread_check_init();
    dbg_pthread_add_self(rwlock, DBG_LOCK_PTHREAD_RDLOCK);
  }
  return rc;
}

/*****************************************************************************/

int dbg_pthread_rwlock_wrlock(
  pthread_rwlock_t *rwlock
){
  int rc = pthread_rwlock_wrlock(rwlock);
  if( rc==0 ){
    dbg_pthread_check_init();
    dbg_pthread_add_self(rwlock, DBG_LOCK_PTHREAD_WRLOCK);
  }
  return rc;
}

/*****************************************************************************/

int dbg_pthread_rwlock_tryrdlock(
  pthread_rwlock_t *rwlock
){
  int rc = pthread_rwlock_tryrdlock(rwlock);
  if( rc==0 ){
    dbg_pthread_check_init();
    dbg_pthread_add_self(rwlock, DBG_LOCK_PTHREAD_RDLOCK);
  }
  return rc;
}

/*****************************************************************************/

int dbg_pthread_rwlock_trywrlock(
  pthread_rwlock_t *rwlock
){
  int rc = pthread_rwlock_trywrlock(rwlock);
  if( rc==0 ){
    dbg_pthread_check_init();
    dbg_pthread_add_self(rwlock, DBG_LOCK_PTHREAD_WRLOCK);
  }
  return rc;
}

/*****************************************************************************/

int dbg_pthread_rwlock_timedrdlock(
  pthread_rwlock_t *rwlock,
  const struct timespec *abs_timeout
){
  int rc = pthread_rwlock_timedrdlock(rwlock, abs_timeout);
  if( rc==0 ){
    dbg_pthread_check_init();
    dbg_pthread_add_self(rwlock, DBG_LOCK_PTHREAD_RDLOCK);
  }
  return rc;
}

/*****************************************************************************/

int dbg_pthread_rwlock_timedwrlock(
  pthread_rwlock_t *rwlock,
  const struct timespec *abs_timeout
){
  int rc = pthread_rwlock_timedwrlock(rwlock, abs_timeout);
  if( rc==0 ){
    dbg_pthread_check_init();
    dbg_pthread_add_self(rwlock, DBG_LOCK_PTHREAD_WRLOCK);
  }
  return rc;
}

/*****************************************************************************/

int dbg_pthread_rwlock_unlock(
  pthread_rwlock_t *rwlock
){
  int rc = pthread_rwlock_unlock(rwlock);
  if( rc==0 ) dbg_pthread_remove_self2(rwlock, DBG_LOCK_PTHREAD_RWLOCK);
  return rc;
}

#endif /* defined(DBG_PTHREAD_LOCKS) */
