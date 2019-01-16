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
#include "comdb2_atomic.h"
#include "list.h"

static pthread_mutex_t schema_rd_thds_lk = PTHREAD_MUTEX_INITIALIZER;
static LISTC_T(pthread_t) schema_rd_thds = LISTC_T_INITIALIZER;

static pthread_t schema_wr_thd = NULL;
#endif

static pthread_rwlock_t schema_lk = PTHREAD_RWLOCK_INITIALIZER;

#ifndef NDEBUG
inline int schema_read_held_int(const char *file, const char *func, int line)
{
  int rc;
  Pthread_mutex_lock(&schema_rd_thds_lk);
  rc = listc_is_present(&schema_rd_thds, (void *)pthread_self());
  Pthread_mutex_unlock(&schema_rd_thds_lk);
  return rc;
}

inline int schema_write_held_int(const char *file, const char *func, int line)
{
  pthread_t self = pthread_self();
  return CAS64(schema_wr_thd, self, self);
}
#endif

inline void rdlock_schema_int(const char *file, const char *func, int line)
{
    Pthread_rwlock_rdlock(&schema_lk);
#ifndef NDEBUG
    Pthread_mutex_lock(&schema_rd_thds_lk);
    listc_abl(&schema_rd_thds, (void *)pthread_self());
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
        listc_abl(&schema_rd_thds, (void *)pthread_self());
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
    void *pNull = NULL;
    CAS64(schema_wr_thd, self, pNull);
    Pthread_mutex_lock(&schema_rd_thds_lk);
    if (listc_is_present(&schema_rd_thds, (void *)self)) {
        listc_rfl(&schema_rd_thds, (void *)self);
    }
    Pthread_mutex_unlock(&schema_rd_thds_lk);
#endif
    Pthread_rwlock_unlock(&schema_lk);
}

inline void wrlock_schema_int(const char *file, const char *func, int line)
{
    Pthread_rwlock_wrlock(&schema_lk);
#ifndef NDEBUG
    XCHANGE64(schema_wr_thd, pthread_self());
#endif
#ifdef VERBOSE_SCHEMA_LK
    logmsg(LOGMSG_USER, "%p:WRLOCK %s:%d\n", (void *)pthread_self(), func,
           line);
#endif
}
