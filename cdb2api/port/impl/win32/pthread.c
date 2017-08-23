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

#include <windows.h>
#include <pthread.h>
#include <stdlib.h>
#include <malloc.h>

int pthread_mutex_lock(HANDLE *lk)
{
    HANDLE tmp;
    if (*lk == NULL) {
        tmp = CreateMutex(NULL, FALSE, NULL);
        if (InterlockedCompareExchangePointer((PVOID *)lk, (PVOID)tmp, NULL) !=
            NULL)
            CloseHandle(tmp);
    }
    return (WaitForSingleObject(*lk, INFINITE) == WAIT_FAILED);
}

int pthread_mutex_unlock(HANDLE *lk)
{
    return (ReleaseMutex(*lk) == 0);
}

int pthread_mutex_init(volatile HANDLE *lk,
                       const volatile pthread_mutexattr_t *unused)
{
    HANDLE ret = CreateMutex(NULL, FALSE, NULL);
    (void)unused;
    if (ret == NULL)
        return 1;
    *lk = ret;
    return 0;
}

int pthread_mutex_destroy(HANDLE *lk)
{
    return (CloseHandle(*lk) == 0);
}

typedef struct once_st {
    HANDLE lk;
    BOOL initd;
} once_t;

int pthread_once(pthread_once_t *once_ctrl, void (*init_rtn)(void))
{
    int rc = 0;
    once_t *once, *tmp;

    if (*once_ctrl == NULL) {
        tmp = calloc(1, sizeof(once_t));
        if (InterlockedCompareExchangePointer((PVOID *)once_ctrl, (PVOID)tmp,
                                              NULL) != NULL)
            free(tmp);
    }

    once = (once_t *)*once_ctrl;
    if (!once->initd) {
        if ((rc = pthread_mutex_lock(&once->lk)) == 0) {
            if (!once->initd) {
                init_rtn();
                once->initd = TRUE;
            }
            pthread_mutex_unlock(&once->lk);
        }
    }
    return rc;
}
