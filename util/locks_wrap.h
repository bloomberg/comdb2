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
#ifndef _INCLUDED_LOCKS_H
#define _INCLUDED_LOCKS_H


/*#define LOCK_DEBUG*/

#include "logmsg.h"


#ifdef LOCK_DEBUG

#define Pthread_mutex_init(mutex_ptr, attr)                                     \
{                                                                          \
    logmsg(LOGMSG_USER, "%d pthread_mutex_init try (%d)  %s:%d\n",             \
           (int)pthread_self(), mutex_ptr, __FILE__, __LINE__);                \
    if (pthread_mutex_init(mutex_ptr, attr) != 0) {                            \
        logmsg(LOGMSG_FATAL, "%d:%s:%d init failed\n", (int)pthread_self(),     \
               __func__, __LINE__);                                        \
        abort();                                                           \
    }                                                                      \
    logmsg(LOGMSG_USER, "%d pthread_mutex_init got (%d)  %s:%d\n",             \
            (int)pthread_self(), mutex_ptr, __FILE__, __LINE__);                \
}

#define Pthread_mutex_destroy(mutex_ptr)                                          \
{                                                                          \
    logmsg(LOGMSG_USER, "%d pthread_mutex_destroy try (%d)  %s:%d\n",             \
           (int)pthread_self(), mutex_ptr, __FILE__, __LINE__);                \
    if (pthread_mutex_destroy(mutex_ptr) != 0) {                              \
        logmsg(LOGMSG_FATAL, "%d:%s:%d init failed\n", (int)pthread_self(),     \
               __func__, __LINE__);                                        \
        abort();                                                           \
    }                                                                      \
    logmsg(LOGMSG_USER, "%d pthread_mutex_destroy got (%d)  %s:%d\n",             \
            (int)pthread_self(), mutex_ptr, __FILE__, __LINE__);                \
}

#define Pthread_mutex_lock(mutex_ptr)                                          \
{                                                                          \
    logmsg(LOGMSG_USER, "%d pthread_mutex_lock try (%d)  %s:%d\n",             \
            (int)pthread_self(), mutex_ptr, __FILE__, __LINE__);                \
    if (pthread_mutex_lock(mutex_ptr) != 0) {                              \
        logmsg(LOGMSG_FATAL, "%d:%s:%d lock failed\n", (int)pthread_self(),     \
               __func__, __LINE__);                                        \
        abort();                                                           \
    }                                                                      \
    logmsg(LOGMSG_USER, "%d pthread_mutex_lock got (%d)  %s:%d\n",             \
            (int)pthread_self(), mutex_ptr, __FILE__, __LINE__);                \
}

#define Pthread_mutex_unlock(mutex_ptr)                                        \
{                                                                          \
    logmsg(LOGMSG_USER, "%d pthread_mutex_unlock(%d)  %s:%d\n",                \
            (int)pthread_self(), mutex_ptr, __FILE__, __LINE__);                \
    if (pthread_mutex_unlock(mutex_ptr) != 0) {                            \
        logmsg(LOGMSG_FATAL, "%d:%s:%d lock failed\n", (int)pthread_self(),     \
               __func__, __LINE__);                                        \
        abort();                                                           \
    }                                                                      \
}

#define Pthread_rwlock_init(mutex_ptr, attr)                                     \
{                                                                          \
    logmsg(LOGMSG_USER, "%d pthread_rwlock_init try (%d)  %s:%d\n",             \
           (int)pthread_self(), mutex_ptr, __FILE__, __LINE__);                \
    if (pthread_rwlock_init(mutex_ptr, attr) != 0) {                            \
        logmsg(LOGMSG_FATAL, "%d:%s:%d init failed\n", (int)pthread_self(),     \
               __func__, __LINE__);                                        \
        abort();                                                           \
    }                                                                      \
    logmsg(LOGMSG_USER, "%d pthread_rwlock_init got (%d)  %s:%d\n",             \
            (int)pthread_self(), mutex_ptr, __FILE__, __LINE__);                \
}

#define Pthread_rwlock_rdlock(rwlock_ptr)                                      \
{                                                                          \
    logmsg(LOGMSG_USER, "%d pthread_rwlock_rdlock try (%d)  %s:%d\n",          \
            (int)pthread_self(), rwlock_ptr, __FILE__, __LINE__);               \
    if (pthread_rwlock_rdlock(rwlock_ptr) != 0) {                          \
        logmsg(LOGMSG_FATAL, "%d:%s:%d rdlock lock failed\n",              \
               (int)pthread_self(), __func__, __LINE__);                        \
        abort();                                                           \
    }                                                                      \
    logmsg(LOGMSG_USER, "%d Pthread_rwlock_rdlock got (%d)  %s:%d\n",          \
            (int)pthread_self(), rwlock_ptr, __FILE__, __LINE__);               \
}

#define Pthread_rwlock_wrlock(rwlock_ptr)                                      \
{                                                                          \
    logmsg(LOGMSG_USER, "%d pthread_rwlock_wrlock try (%d)  %s:%d\n",          \
            (int)pthread_self(), rwlock_ptr, __FILE__, __LINE__);               \
    if (pthread_rwlock_wrlock(rwlock_ptr) != 0) {                          \
        logmsg(LOGMSG_FATAL, "%d:%s:%d rwlock lock failed\n",              \
               (int)pthread_self(), __func__, __LINE__);                        \
        abort();                                                           \
    }                                                                      \
    logmsg(LOGMSG_USER, "%d Pthread_rwlock_rwlock got (%d)  %s:%d\n",          \
            (int)pthread_self(), rwlock_ptr, __FILE__, __LINE__);               \
}

#define Pthread_rwlock_unlock(rwlock_ptr)                                      \
{                                                                          \
    logmsg(LOGMSG_USER, "%d pthread_rwlock_unlock(%d)  %s:%d\n",               \
            (int)pthread_self(), rwlock_ptr, __FILE__, __LINE__);               \
    if (pthread_rwlock_unlock(rwlock_ptr) != 0) {                          \
        logmsg(LOGMSG_FATAL, "%d:%s:%d rwlock unlock failed\n",            \
               (int)pthread_self(), __func__, __LINE__);                        \
        abort();                                                           \
    }                                                                      \
}

#else

#define Pthread_mutex_init(mutex_ptr, attr)                                     \
{                                                                          \
    if (pthread_mutex_init(mutex_ptr, attr) != 0) {                        \
        logmsg(LOGMSG_FATAL, "%d:%s:%d init failed\n", (int)pthread_self(),     \
               __func__, __LINE__);                                        \
        abort();                                                           \
    }                                                                      \
}

#define Pthread_mutex_destroy(mutex_ptr)                                          \
{                                                                          \
    if (pthread_mutex_destroy(mutex_ptr) != 0) {                              \
        logmsg(LOGMSG_FATAL, "%d:%s:%d init failed\n", (int)pthread_self(),     \
               __func__, __LINE__);                                        \
        abort();                                                           \
    }                                                                      \
}


#define Pthread_mutex_lock(mutex_ptr)                                          \
{                                                                          \
    if (pthread_mutex_lock(mutex_ptr) != 0) {                              \
        logmsg(LOGMSG_FATAL, "%d:%s:%d lock failed\n", (int)pthread_self(),     \
               __func__, __LINE__);                                        \
        abort();                                                           \
    }                                                                      \
}

#define Pthread_mutex_unlock(mutex_ptr)                                        \
{                                                                          \
    if (pthread_mutex_unlock(mutex_ptr) != 0) {                            \
        logmsg(LOGMSG_FATAL, "%d:%s:%d lock failed\n", (int)pthread_self(),     \
               __func__, __LINE__);                                        \
        abort();                                                           \
    }                                                                      \
}

#define Pthread_rwlock_init(mutex_ptr, attr)                                     \
{                                                                          \
    if (pthread_rwlock_init(mutex_ptr, attr) != 0) {                            \
        logmsg(LOGMSG_FATAL, "%d:%s:%d init failed\n", (int)pthread_self(),     \
               __func__, __LINE__);                                        \
        abort();                                                           \
    }                                                                      \
}

#define Pthread_rwlock_rdlock(rwlock_ptr)                                      \
{                                                                          \
    if (pthread_rwlock_rdlock(rwlock_ptr) != 0) {                          \
        logmsg(LOGMSG_FATAL, "%d:%s:%d rdlock lock failed\n",              \
               (int)pthread_self(), __func__, __LINE__);                        \
        abort();                                                           \
    }                                                                      \
}

#define Pthread_rwlock_wrlock(rwlock_ptr)                                      \
{                                                                          \
    if (pthread_rwlock_wrlock(rwlock_ptr) != 0) {                          \
        logmsg(LOGMSG_FATAL, "%d:%s:%d rwlock lock failed\n",              \
               (int)pthread_self(), __func__, __LINE__);                        \
        abort();                                                           \
    }                                                                      \
}

#define Pthread_rwlock_unlock(rwlock_ptr)                                      \
{                                                                          \
    if (pthread_rwlock_unlock(rwlock_ptr) != 0) {                          \
        logmsg(LOGMSG_FATAL, "%d:%s:%d rwlock unlock failed\n",            \
               (int)pthread_self(), __func__, __LINE__);                        \
        abort();                                                           \
    }                                                                      \
}


#endif

#endif
