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
#include "list.h"

static pthread_mutex_t schema_rd_thds_lk = PTHREAD_MUTEX_INITIALIZER;

struct pthread_t_link {
    pthread_t thread;
    LINKC_T(struct pthread_t_link) lnk;
};

static LISTC_T(struct pthread_t_link) schema_rd_thds =
    LISTC_T_INITIALIZER(offsetof(struct pthread_t_link, lnk));

static pthread_t schema_wr_thd = NULL;
#endif

static pthread_rwlock_t schema_lk = PTHREAD_RWLOCK_INITIALIZER;

#ifndef NDEBUG
inline int schema_read_held_int(const char *file, const char *func, int line)
{
  int rc = 0;
  Pthread_mutex_lock(&schema_rd_thds_lk);
  struct pthread_t_link *current, *temp;
  pthread_t self = pthread_self();
  LISTC_FOR_EACH_SAFE(&schema_rd_thds, current, temp, lnk)
  {
      if (current->thread == self) {
          rc = 1;
          break;
      }
  }
  Pthread_mutex_unlock(&schema_rd_thds_lk);
  return rc;
}

inline int schema_write_held_int(const char *file, const char *func, int line)
{
  pthread_t self = pthread_self();
  return CAS64(schema_wr_thd, self, self);
}

void dump_schema_lk(FILE *out)
{
    pthread_t self = pthread_self();
    pthread_t nullt = NULL;
    pthread_t writer = CAS64(schema_wr_thd, nullt, nullt);
    fprintf(out, "[SCHEMA_LK] %p @ %s: writer is %p\n",
            (void *)self, __func__, (void *)writer);
    Pthread_mutex_lock(&schema_rd_thds_lk);
    struct pthread_t_link *current, *temp;
    LISTC_FOR_EACH_SAFE(&schema_rd_thds, current, temp, lnk)
    {
        fprintf(out, "[SCHEMA_LK] %p @ %s: have reader %p\n",
                (void *)self, __func__, (void *)current->thread);
    }
    Pthread_mutex_unlock(&schema_rd_thds_lk);
    fflush(out);
}
#endif

inline void rdlock_schema_int(const char *file, const char *func, int line)
{
    Pthread_rwlock_rdlock(&schema_lk);
#ifndef NDEBUG
    struct pthread_t_link *newt = calloc(1, sizeof(struct pthread_t_link));
    if (newt == NULL) abort();
    newt->thread = pthread_self();
    Pthread_mutex_lock(&schema_rd_thds_lk);
    listc_abl(&schema_rd_thds, newt);
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
        struct pthread_t_link *newt = calloc(1, sizeof(struct pthread_t_link));
        if (newt == NULL) abort();
        newt->thread = pthread_self();
        Pthread_mutex_lock(&schema_rd_thds_lk);
        listc_abl(&schema_rd_thds, newt);
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
    pthread_t nullt = NULL;
    CAS64(schema_wr_thd, self, nullt);
    Pthread_mutex_lock(&schema_rd_thds_lk);
    struct pthread_t_link *current, *temp;
    LISTC_FOR_EACH_SAFE(&schema_rd_thds, current, temp, lnk)
    {
        if (current->thread == self) {
            listc_rfl(&schema_rd_thds, current);
            free(current);
            break;
        }
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
