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

/* MACHINE INDEPENDENT PTHREAD MACROS */
#ifndef __MACHPTHREAD_H__
#define __MACHPTHREAD_H__

#if defined __DGUX__

#define PTHD_TESTINTR(rc) (rc) = pthread_testintr()
#define PTHD_INTR_DISABLE(rc) (rc) = pthread_setintr(PTHREAD_INTR_DISABLE)
#define PTHD_INTR_ENABLE(rc) (rc) = pthread_setintr(PTHREAD_INTR_ENABLE)
#define PTHD_ATTR_SETDETACHED(attr, rc)                                        \
    {                                                                          \
        int c = 1;                                                             \
        (rc) = pthread_attr_setdetachstate(&(attr), &c);                       \
    }
#define PTHD_ATTR_SETJOINABLE(attr, rc)                                        \
    {                                                                          \
        int c = 0;                                                             \
        (rc) = pthread_attr_setdetachstate(&(attr), &c);                       \
    }
#define PTHD_ATTR_SETSTACKSIZE(attr, stk, rc)                                  \
    (rc) = pthread_attr_setstacksize(&(attr), (stk))
#define PTHD_ATTR_SETSCOPESYSTEM(attr, rc)
#define PTHD_DETACH(tid, rc)                                                   \
    {                                                                          \
        pthread_t t = (tid);                                                   \
        (rc) = pthread_detach(&t);                                             \
        if (rc)                                                                \
            (rc) = errno;                                                      \
    }

#elif defined __linux__

#define PTHD_TESTINTR(rc) (rc) = pthread_testcancel()
#define PTHD_INTR_DISABLE(rc)                                                  \
    (rc) = pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, 0);
#define PTHD_INTR_ENABLE(rc)                                                   \
    (rc) = pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, 0);
#define PTHD_ATTR_SETDETACHED(attr, rc)                                        \
    (rc) = pthread_attr_setdetachstate(&(attr), PTHREAD_CREATE_DETACHED)
#define PTHD_ATTR_SETJOINABLE(attr, rc)                                        \
    (rc) = pthread_attr_setdetachstate(&(attr), PTHREAD_CREATE_JOINABLE)
#define PTHD_ATTR_SETSTACKSIZE(attr, stk, rc) /*NO STACKSIZE ATTR FOR LINUX*/
#define PTHD_ATTR_SETSCOPESYSTEM(attr, rc)
#define PTHD_DETACH(tid, rc)                                                   \
    {                                                                          \
        pthread_t t = (tid);                                                   \
        (rc) = pthread_detach(t);                                              \
    }

#else
/*#elif defined __sun*/

#define PTHD_TESTINTR(rc) (rc) = pthread_testcancel()
#define PTHD_INTR_DISABLE(rc)                                                  \
    (rc) = pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, 0);
#define PTHD_INTR_ENABLE(rc)                                                   \
    (rc) = pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, 0);
#define PTHD_ATTR_SETDETACHED(attr, rc)                                        \
    (rc) = pthread_attr_setdetachstate(&(attr), PTHREAD_CREATE_DETACHED)
#define PTHD_ATTR_SETJOINABLE(attr, rc)                                        \
    (rc) = pthread_attr_setdetachstate(&(attr), PTHREAD_CREATE_JOINABLE)
#define PTHD_ATTR_SETSTACKSIZE(attr, stk, rc)                                  \
    (rc) = pthread_attr_setstacksize(&(attr), (stk))
#define PTHD_ATTR_SETSCOPESYSTEM(attr, rc)                                     \
    (rc) = pthread_attr_setscope(&(attr), PTHREAD_SCOPE_SYSTEM)
#define PTHD_DETACH(tid, rc)                                                   \
    {                                                                          \
        pthread_t t = (tid);                                                   \
        (rc) = pthread_detach(t);                                              \
    }

#endif

#endif
