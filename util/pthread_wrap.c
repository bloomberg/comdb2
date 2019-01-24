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

#include <pthread.h>
#include "pthread_wrap_core.h"

int wrap_pthread_mutex_trylock(
  pthread_mutex_t *mutex,
  const char *file,
  const char *func,
  int line
){
  int rc;
  LKDBG_TRACE(TRY, __func__, mutex);
  rc = pthread_mutex_trylock(mutex);
  if( rc!=0 ){
    logmsg(LOGMSG_DEBUG,
           "%s:%d %s(0x%" PRIxPTR ") rc:%d (%s) thd:%p\n",
           func, line, __func__, (uintptr_t)mutex, rc,
           strerror(rc), (void *)pthread_self());
    LKDBG_TRACE(NOT, __func__, mutex);
  }else{
    LKDBG_TRACE(GOT, __func__, mutex);
  }
  return rc;
}

/*****************************************************************************/

int wrap_pthread_mutex_timedlock(
  pthread_mutex_t *mutex,
  const struct timespec *abs_timeout,
  const char *file,
  const char *func,
  int line
){
  int rc;
  LKDBG_TRACE(TRY, __func__, mutex);
  rc = pthread_mutex_timedlock(mutex, abs_timeout);
  if( rc!=0 ){
    logmsg(LOGMSG_DEBUG,
           "%s:%d %s(0x%" PRIxPTR ") rc:%d (%s) thd:%p\n",
           func, line, __func__, (uintptr_t)mutex, rc,
           strerror(rc), (void *)pthread_self());
    LKDBG_TRACE(NOT, __func__, mutex);
  }else{
    LKDBG_TRACE(GOT, __func__, mutex);
  }
  return rc;
}

/*****************************************************************************/

int wrap_pthread_rwlock_tryrdlock(
  pthread_rwlock_t *rwlock,
  const char *file,
  const char *func,
  int line
){
  int rc;
  LKDBG_TRACE(TRY, __func__, rwlock);
  rc = pthread_rwlock_tryrdlock(rwlock);
  if( rc!=0 ){
    logmsg(LOGMSG_DEBUG,
           "%s:%d %s(0x%" PRIxPTR ") rc:%d (%s) thd:%p\n",
           func, line, __func__, (uintptr_t)rwlock, rc,
           strerror(rc), (void *)pthread_self());
    LKDBG_TRACE(NOT, __func__, rwlock);
  }else{
    LKDBG_TRACE(GOT, __func__, rwlock);
  }
  return rc;
}

/*****************************************************************************/

int wrap_pthread_rwlock_trywrlock(
  pthread_rwlock_t *rwlock,
  const char *file,
  const char *func,
  int line
){
  int rc;
  LKDBG_TRACE(TRY, __func__, rwlock);
  rc = pthread_rwlock_trywrlock(rwlock);
  if( rc!=0 ){
    logmsg(LOGMSG_DEBUG,
           "%s:%d %s(0x%" PRIxPTR ") rc:%d (%s) thd:%p\n",
           func, line, __func__, (uintptr_t)rwlock, rc,
           strerror(rc), (void *)pthread_self());
    LKDBG_TRACE(NOT, __func__, rwlock);
  }else{
    LKDBG_TRACE(GOT, __func__, rwlock);
  }
  return rc;
}

/*****************************************************************************/

int wrap_pthread_rwlock_timedrdlock(
  pthread_rwlock_t *rwlock,
  const struct timespec *abs_timeout,
  const char *file,
  const char *func,
  int line
){
  int rc;
  LKDBG_TRACE(TRY, __func__, rwlock);
  rc = pthread_rwlock_timedrdlock(rwlock, abs_timeout);
  if( rc!=0 ){
    logmsg(LOGMSG_DEBUG,
           "%s:%d %s(0x%" PRIxPTR ") rc:%d (%s) thd:%p\n",
           func, line, __func__, (uintptr_t)rwlock, rc,
           strerror(rc), (void *)pthread_self());
    LKDBG_TRACE(NOT, __func__, rwlock);
  }else{
    LKDBG_TRACE(GOT, __func__, rwlock);
  }
  return rc;
}

/*****************************************************************************/

int wrap_pthread_rwlock_timedwrlock(
  pthread_rwlock_t *rwlock,
  const struct timespec *abs_timeout,
  const char *file,
  const char *func,
  int line
){
  int rc;
  LKDBG_TRACE(TRY, __func__, rwlock);
  rc = pthread_rwlock_timedwrlock(rwlock, abs_timeout);
  if( rc!=0 ){
    logmsg(LOGMSG_DEBUG,
           "%s:%d %s(0x%" PRIxPTR ") rc:%d (%s) thd:%p\n",
           func, line, __func__, (uintptr_t)rwlock, rc,
           strerror(rc), (void *)pthread_self());
    LKDBG_TRACE(NOT, __func__, rwlock);
  }else{
    LKDBG_TRACE(GOT, __func__, rwlock);
  }
  return rc;
}
