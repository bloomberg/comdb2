/*
   Copyright 2017, Bloomberg Finance L.P.

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

#ifndef _INCLUDED_PORT_WIN32_PTHREAD_H_
#define _INCLUDED_PORT_WIN32_PTHREAD_H_

#include <windows.h>

typedef DWORD pthread_t;
#define pthread_self() GetCurrentThreadId()

typedef DWORD pthread_mutexattr_t;
typedef HANDLE pthread_mutex_t;
#define PTHREAD_MUTEX_INITIALIZER NULL

int pthread_mutex_init(volatile pthread_mutex_t *mutex,
                       const volatile pthread_mutexattr_t *attr);
int pthread_mutex_destroy(pthread_mutex_t *mutex);
int pthread_mutex_lock(pthread_mutex_t *lk);
int pthread_mutex_unlock(pthread_mutex_t *lk);

typedef void *pthread_once_t;
#define PTHREAD_ONCE_INIT NULL
int pthread_once(pthread_once_t *once_ctrl, void (*init_rtn)(void));

#endif
