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

#include <pthread.h>
#include <logmsg.h>
#include <locks_wrap.h>
#include <schema_lk.h>

#ifndef NDEBUG
#include <stdio.h>
#include <stdlib.h>
#include "comdb2_atomic.h"
#include "plhash.h"

static pthread_mutex_t schema_rd_thds_lk = PTHREAD_MUTEX_INITIALIZER;
static hash_t *schema_rd_thds = NULL;
static pthread_t schema_wr_thd = (pthread_t)0;
#endif

static pthread_rwlock_t schema_lk = PTHREAD_RWLOCK_INITIALIZER;

#ifndef NDEBUG
inline void schema_init_held(void)
{
  Pthread_mutex_lock(&schema_rd_thds_lk);
  assert(schema_rd_thds == NULL);
  schema_rd_thds = hash_init(sizeof(pthread_t));
  Pthread_mutex_unlock(&schema_rd_thds_lk);
}

inline void schema_term_held(void)
{
  Pthread_mutex_lock(&schema_rd_thds_lk);
  assert(schema_rd_thds != NULL);
  hash_clear(schema_rd_thds);
  hash_free(schema_rd_thds);
  schema_rd_thds = NULL;
  Pthread_mutex_unlock(&schema_rd_thds_lk);
}

inline int schema_read_held_int(const char *file, const char *func, int line)
{
  int rc = 0;
  Pthread_mutex_lock(&schema_rd_thds_lk);
  if (hash_find(schema_rd_thds, pthread_self()) != NULL) {
    rc = 1;
  }
  Pthread_mutex_unlock(&schema_rd_thds_lk);
  return rc;
}

inline int schema_write_held_int(const char *file, const char *func, int line)
{
  pthread_t self = pthread_self();
  return CAS(schema_wr_thd, self, self);
}

static int schema_dump_rd_thd(void *obj, void *arg)
{
  logmsgf(LOGMSG_USER, (FILE *)arg, "[SCHEMA_LK] has reader %p\n", obj);
  fflush(out);
  return 0;
}

void dump_schema_lk(FILE *out)
{
    logmsgf(LOGMSG_USER, out, "[SCHEMA_LK] writer is %p\n",
            (void *)ATOMIC_LOAD(schema_wr_thd));
    Pthread_mutex_lock(&schema_rd_thds_lk);
    hash_for(schema_rd_thds, schema_dump_rd_thd, out);
    Pthread_mutex_unlock(&schema_rd_thds_lk);
}
#endif

inline void rdlock_schema_int(const char *file, const char *func, int line)
{
    Pthread_rwlock_rdlock(&schema_lk);
#ifndef NDEBUG
    Pthread_mutex_lock(&schema_rd_thds_lk);
    if (hash_add(schema_rd_thds, pthread_self()) != 0) {
        abort();
    }
    Pthread_mutex_unlock(&schema_rd_thds_lk);
#endif
#ifdef VERBOSE_SCHEMA_LK
    logmsg(LOGMSG_USER, "%p:RDLOCK %s:%d\n", (void *)pthread_self(), func,
           line);
#endif
}

inline int tryrdlock_schema_int(const char *file, const char *func, int line)
{
    int rc = pthread_rwlock_tryrdlock(&schema_lk);
#ifndef NDEBUG
    if (rc == 0) {
        Pthread_mutex_lock(&schema_rd_thds_lk);
        if (hash_add(schema_rd_thds, pthread_self()) != 0) {
            abort();
        }
        Pthread_mutex_unlock(&schema_rd_thds_lk);
    }
#endif
#ifdef VERBOSE_SCHEMA_LK
    logmsg(LOGMSG_USER, "%p:TRYRDLOCK RC:%d %s:%d\n", (void *)pthread_self(),
           rc, func, line);
#endif
    return rc;
}

inline void unlock_schema_int(const char *file, const char *func, int line)
{
#ifdef VERBOSE_SCHEMA_LK
    logmsg(LOGMSG_USER, "%p:UNLOCK %s:%d\n", (void *)pthread_self(), func,
           line);
#endif
#ifndef NDEBUG
    pthread_t self = pthread_self();
    pthread_t nullt = (pthread_t)NULL;
    CAS(schema_wr_thd, self, nullt);
    Pthread_mutex_lock(&schema_rd_thds_lk);
    if (hash_del(schema_rd_thds, self) != 0) {
        abort();
    }
    Pthread_mutex_unlock(&schema_rd_thds_lk);
#endif
    Pthread_rwlock_unlock(&schema_lk);
}

inline void wrlock_schema_int(const char *file, const char *func, int line)
{
    Pthread_rwlock_wrlock(&schema_lk);
#ifndef NDEBUG
    XCHANGE(schema_wr_thd, pthread_self());
#endif
#ifdef VERBOSE_SCHEMA_LK
    logmsg(LOGMSG_USER, "%p:WRLOCK %s:%d\n", (void *)pthread_self(), func,
           line);
#endif
}
