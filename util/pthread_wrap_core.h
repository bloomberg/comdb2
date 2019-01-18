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

#ifndef _INCLUDED_PTHREAD_WRAP_CORE_H
#define _INCLUDED_PTHREAD_WRAP_CORE_H

#include <inttypes.h>
#include <string.h>
#include <stdlib.h>
#include <logmsg.h>

#ifdef LOCK_DEBUG
#  define LKDBG_TRACE(STR, FUNC, OBJ) logmsg(LOGMSG_USER, "%s:%d " #STR " " #FUNC "(0x%"PRIxPTR") thd:%p\n", __func__, __LINE__, (uintptr_t)OBJ, (void *)pthread_self())
#else
#  define LKDBG_TRACE(...)
#endif

#define LKWRAP_FIRST_(a, ...) a
#define LKWRAP_FIRST(...) LKWRAP_FIRST_(__VA_ARGS__, 0)

#define WRAP_PTHREAD(FUNC, ...)                                                \
    do {                                                                       \
        int rc;                                                                \
        LKDBG_TRACE(TRY, FUNC, LKWRAP_FIRST(__VA_ARGS__));                     \
        if ((rc = FUNC(__VA_ARGS__)) != 0) {                                   \
            logmsg(LOGMSG_FATAL,                                               \
                   "%s:%d " #FUNC "(0x%" PRIxPTR ") rc:%d (%s) thd:%p\n",      \
                   __func__, __LINE__, (uintptr_t)LKWRAP_FIRST(__VA_ARGS__),   \
                   rc, strerror(rc), (void *)pthread_self());                  \
            abort();                                                           \
        }                                                                      \
        LKDBG_TRACE(GOT, FUNC, LKWRAP_FIRST(__VA_ARGS__));                     \
    } while (0)

#define WRAP_PTHREAD_WITH_RC(var, FUNC, ...)                                   \
    do {                                                                       \
        LKDBG_TRACE(TRY, FUNC, LKWRAP_FIRST(__VA_ARGS__));                     \
        if ((var = FUNC(__VA_ARGS__)) != 0) {                                  \
            logmsg(LOGMSG_FATAL,                                               \
                   "%s:%d " #FUNC "(0x%" PRIxPTR ") rc:%d (%s) thd:%p\n",      \
                   __func__, __LINE__, (uintptr_t)LKWRAP_FIRST(__VA_ARGS__),   \
                   var, strerror(var), (void *)pthread_self());                \
            abort();                                                           \
        }                                                                      \
        LKDBG_TRACE(GOT, FUNC, LKWRAP_FIRST(__VA_ARGS__));                     \
    } while (0)

#endif /* _INCLUDED_PTHREAD_WRAP_CORE_H */
