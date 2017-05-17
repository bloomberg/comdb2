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

/*#define LOCK_DEBUG*/

#ifdef LOCK_DEBUG

#include "logmsg.h"

#define Pthread_mutex_lock(mutex_ptr)                                          \
    {                                                                          \
        logmsg(LOGMSG_ERROR, "%d pthread_mutex_lock try (%d)  %s:%d\n",             \
                pthread_self(), mutex_ptr, __FILE__, __LINE__);                \
        if (pthread_mutex_lock(mutex_ptr) != 0) {                              \
            logmsg(LOGMSG_FATAL, "%d lock failed\n", pthread_self());               \
            cheap_stack_trace();                                               \
            abort();                                                           \
        }                                                                      \
        logmsg(LOGMSG_ERROR, "%d pthread_mutex_lock got (%d)  %s:%d\n",             \
                pthread_self(), mutex_ptr, __FILE__, __LINE__);                \
    }

#define Pthread_mutex_unlock(mutex_ptr)                                        \
    {                                                                          \
        logmsg(LOGMSG_ERROR, "%d pthread_mutex_unlock(%d)  %s:%d\n",                \
                pthread_self(), mutex_ptr, __FILE__, __LINE__);                \
        if (pthread_mutex_unlock(mutex_ptr) != 0) {                            \
            logmsg(LOGMSG_FATAL, "%d mutex unlock failed\n", pthread_self());       \
            cheap_stack_trace();                                               \
            abort();                                                           \
        }                                                                      \
    }

#define Pthread_rwlock_rdlock(rwlock_ptr)                                      \
    {                                                                          \
        logmsg(LOGMSG_ERROR, "%d pthread_rwlock_rdlock try (%d)  %s:%d\n",          \
                pthread_self(), rwlock_ptr, __FILE__, __LINE__);               \
        if (pthread_rwlock_rdlock(rwlock_ptr) != 0) {                          \
            logmsg(LOGMSG_FATAL, "%d rdlock lock failed\n", pthread_self());        \
            cheap_stack_trace();                                               \
            abort();                                                           \
        }                                                                      \
        logmsg(LOGMSG_ERROR, "%d Pthread_rwlock_rdlock got (%d)  %s:%d\n",          \
                pthread_self(), rwlock_ptr, __FILE__, __LINE__);               \
    }

#define Pthread_rwlock_wrlock(rwlock_ptr)                                      \
    {                                                                          \
        logmsg(LOGMSG_ERROR, "%d pthread_rwlock_rwlock try (%d)  %s:%d\n",          \
                pthread_self(), rwlock_ptr, __FILE__, __LINE__);               \
        if (pthread_rwlock_wrlock(rwlock_ptr) != 0) {                          \
            logmsg(LOGMSG_FATAL, "%d rwlock lock failed\n", pthread_self());        \
            cheap_stack_trace();                                               \
            abort();                                                           \
        }                                                                      \
        logmsg(LOGMSG_ERROR, "%d Pthread_rwlock_rwlock got (%d)  %s:%d\n",          \
                pthread_self(), rwlock_ptr, __FILE__, __LINE__);               \
    }

#define Pthread_rwlock_unlock(rwlock_ptr)                                      \
    {                                                                          \
        logmsg(LOGMSG_ERROR, "%d pthread_rwlock_unlock(%d)  %s:%d\n",               \
                pthread_self(), rwlock_ptr, __FILE__, __LINE__);               \
        if (pthread_rwlock_unlock(rwlock_ptr) != 0) {                          \
            logmsg(LOGMSG_FATAL, "%d rwlock unlock failed\n", pthread_self());      \
            cheap_stack_trace();                                               \
            abort();                                                           \
        }                                                                      \
    }

#else

#define Pthread_mutex_lock(mutex_ptr)                                          \
    {                                                                          \
        if (pthread_mutex_lock(mutex_ptr) != 0) {                              \
            abort();                                                           \
        }                                                                      \
    }

#define Pthread_mutex_unlock(mutex_ptr)                                        \
    {                                                                          \
        if (pthread_mutex_unlock(mutex_ptr) != 0) {                            \
            abort();                                                           \
        }                                                                      \
    }

#define Pthread_rwlock_rdlock(rwlock_ptr)                                      \
    {                                                                          \
        if (pthread_rwlock_rdlock(rwlock_ptr) != 0) {                          \
            abort();                                                           \
        }                                                                      \
    }

#define Pthread_rwlock_wrlock(rwlock_ptr)                                      \
    {                                                                          \
        if (pthread_rwlock_wrlock(rwlock_ptr) != 0) {                          \
            abort();                                                           \
        }                                                                      \
    }

#define Pthread_rwlock_unlock(rwlock_ptr)                                      \
    {                                                                          \
        if (pthread_rwlock_unlock(rwlock_ptr) != 0) {                          \
            abort();                                                           \
        }                                                                      \
    }

#endif
