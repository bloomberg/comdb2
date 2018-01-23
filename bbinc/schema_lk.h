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

#ifndef INCLUDED_SCHEMA_LK_H
#define INCLUDED_SCHEMA_LK_H

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>

#include <logmsg.h>

extern pthread_rwlock_t schema_lk;

#define rdlock_schema_lk() rdlock_schema_int(__FILE__, __func__, __LINE__)
static inline void rdlock_schema_int(const char *file, const char *func,
                                     int line)
{
    int rc = pthread_rwlock_rdlock(&schema_lk);
    if (rc) {
        logmsg(LOGMSG_FATAL, "%s:%d pthread_rwlock_rdlock failed %d\n", file,
               line, rc);
        abort();
    }
#ifdef VERBOSE_SCHEMA_LK
    fprintf(stdout, "%llx:RDLOCK %s:%d\n", pthread_self(), func, line);
#endif
}

#define tryrdlock_schema_lk() tryrdlock_schema_int(__FILE__, __func__, __LINE__)
static inline int tryrdlock_schema_int(const char *file, const char *func,
                                       int line)
{
    int rc = pthread_rwlock_tryrdlock(&schema_lk);
#ifdef VERBOSE_SCHEMA_LK
    fprintf(stdout, "%llx:TRYRDLOCK RC:%d %s:%d\n", pthread_self(), rc, func,
            line);
#endif
    return rc;
}

#define unlock_schema_lk() unlock_schema_int(__FILE__, __func__, __LINE__)
static inline void unlock_schema_int(const char *file, const char *func,
                                     int line)
{
#ifdef VERBOSE_SCHEMA_LK
    fprintf(stdout, "%llx:UNLOCK %s:%d\n", pthread_self(), func, line);
#endif
    int rc = pthread_rwlock_unlock(&schema_lk);
    if (rc) {
        logmsg(LOGMSG_FATAL, "%s:%d pthread_rwlock_unlock failed %d\n", file,
               line, rc);
        abort();
    }
}

#define wrlock_schema_lk() wrlock_schema_int(__FILE__, __func__, __LINE__)
static inline void wrlock_schema_int(const char *file, const char *func,
                                     int line)
{
    int rc = pthread_rwlock_wrlock(&schema_lk);
    if (rc) {
        logmsg(LOGMSG_FATAL, "%s:%d pthread_rwlock_wrlock failed %d\n", file,
               line, rc);
        abort();
    }
#ifdef VERBOSE_SCHEMA_LK
    fprintf(stdout, "%llx:WRLOCK %s:%d\n", pthread_self(), func, line);
#endif
}

#endif
