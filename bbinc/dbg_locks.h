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
#ifndef DBG_LOCKS_H
#define DBG_LOCKS_H

#include <stdio.h>
#include <time.h>
#include <pthread.h>

/*****************************************************************************/

void dbg_pthread_dump(FILE *out);
void dbg_pthread_term(void);

/*****************************************************************************/

int dbg_pthread_mutex_lock(pthread_mutex_t *);
int dbg_pthread_mutex_trylock(pthread_mutex_t *);
int dbg_pthread_mutex_timedlock(pthread_mutex_t *, const struct timespec *);
int dbg_pthread_mutex_unlock(pthread_mutex_t *mutex);

/*****************************************************************************/

int dbg_pthread_rwlock_rdlock(pthread_rwlock_t *);
int dbg_pthread_rwlock_wrlock(pthread_rwlock_t *);
int dbg_pthread_rwlock_tryrdlock(pthread_rwlock_t *);
int dbg_pthread_rwlock_trywrlock(pthread_rwlock_t *);
int dbg_pthread_rwlock_timedrdlock(pthread_rwlock_t *, const struct timespec *);
int dbg_pthread_rwlock_timedwrlock(pthread_rwlock_t *, const struct timespec *);
int dbg_pthread_rwlock_unlock(pthread_rwlock_t *);

/*****************************************************************************/

#undef Pthread_mutex_lock
#undef Pthread_mutex_trylock
#undef Pthread_mutex_timedlock
#undef Pthread_mutex_unlock
#undef Pthread_rwlock_rdlock
#undef Pthread_rwlock_wrlock
#undef Pthread_rwlock_tryrdlock
#undef Pthread_rwlock_trywrlock
#undef Pthread_rwlock_timedrdlock
#undef Pthread_rwlock_timedwrlock
#undef Pthread_rwlock_unlock

#define Pthread_mutex_lock(a)       WRAP_PTHREAD(dbg_pthread_mutex_lock, a)
#define Pthread_mutex_trylock(a)    WRAP_PTHREAD(dbg_pthread_mutex_trylock, a)
#define Pthread_mutex_unlock(a)     WRAP_PTHREAD(dbg_pthread_mutex_unlock, a)
#define Pthread_rwlock_rdlock(a)    WRAP_PTHREAD(dbg_pthread_rwlock_rdlock, a)
#define Pthread_rwlock_wrlock(a)    WRAP_PTHREAD(dbg_pthread_rwlock_wrlock, a)
#define Pthread_rwlock_unlock(a)    WRAP_PTHREAD(dbg_pthread_rwlock_unlock, a)

#define Pthread_mutex_timedlock(a, b) \
    WRAP_PTHREAD(dbg_pthread_mutex_timedlock, a, b)

#define Pthread_rwlock_tryrdlock(a) \
    WRAP_PTHREAD(dbg_pthread_rwlock_tryrdlock, a)

#define Pthread_rwlock_trywrlock(a) \
    WRAP_PTHREAD(dbg_pthread_rwlock_trywrlock, a)

#define Pthread_rwlock_timedrdlock(a, b) \
    WRAP_PTHREAD(dbg_pthread_rwlock_timedrdlock, a, b)

#define Pthread_rwlock_timedwrlock(a, b) \
    WRAP_PTHREAD(dbg_pthread_rwlock_timedwrlock, a, b)

#endif /* DBG_LOCKS_H */
#endif /* defined(DBG_PTHREAD_LOCKS) */
