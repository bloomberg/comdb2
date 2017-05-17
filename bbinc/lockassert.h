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

#ifndef INCLUDED_LOCKASSERT
#define INCLUDED_LOCKASSERT

/*
 * This module provides wrapper functions for the pthread mutex lock functions
 * which will abort the program (with some diagnostic) if a mutex lock call
 * fails.
 *
 * The macros are:
 *
 * int assert_pthread_mutex_init(pthread_mutex_t *lock,
 *      const pthread_mutexattr_t *attr);
 *
 * int assert_pthread_mutex_lock(pthread_mutex_t *lock);
 *
 * int assert_pthread_mutex_trylock(pthread_mutex_t *lock);
 *
 * int assert_pthread_mutex_unlock(pthread_mutex_t *lock);
 *
 * Note that these macros all assign the outcome of the pthread call to errno
 * as well as returning it.
 *
 */

#include <errno.h>
#include <pthread.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Private function to print a diagnostic after a failed pthred call
 * and then assert().  This always returns the rc value passed in, although
 * because of the assert() it tends not to return.. */
int assert_pthread_lock_func(int rc, const char *pthread_func_name, int lineno,
                             const char *filename, const char *funcname);

#ifdef __cplusplus
}
#endif

/* The macros.  A couple of notes on the implementation.  Firstly, I provide
 * both C and C++ versions.  The only difference is that C++ does not provide
 * __func__, so we pass down 0 instead.
 *
 * The other note regards the choice of the ?: operator here.  Locking is
 * supposed to be fast and unintrusive.  Naively, I could have implemented
 * this stuff like this:
 * #define assert_pthread_mutex_lock(lock)                              \
 *  private_helper_func(pthread_mutex_lock(lock), "pthread_mutex_lock", \
 *  __LINE__, __FILE__, __func__);
 * This was actually my first implementation - the job of checking the return
 * code of pthread_mutex_lock gets delegated to private_helper_func(), which
 * will either return immediately if it gets a zero or abort with an error
 * message otherwise.  However, this seemed like a bad idea because even in the
 * expected common case (lock succeeds) the computer will end up going to the
 * trouble of packing all the arguments into registers or onto the stack only
 * to call a function which effectively does nothing.  Using the ?: operator
 * means that in the common case we just check the pthread function return code
 * and then move on.
 *
 * Yes, I'm aware that inline functions might achieve the same goal.  However
 * we tend to compile stuff with the optimizer turned off, and inline is only
 * a hint and never a contract that something will actually get inlined.
 */

#ifdef __cplusplus

#define assert_pthread_mutex_init(lock, attr)                                  \
    ((errno = pthread_mutex_init((lock), (attr)))                              \
         ? assert_pthread_lock_func(errno, "pthread_mutex_init", __LINE__,     \
                                    __FILE__, 0)                               \
         : errno)

#define assert_pthread_mutex_lock(lock)                                        \
    ((errno = pthread_mutex_lock((lock)))                                      \
         ? assert_pthread_lock_func(errno, "pthread_mutex_lock", __LINE__,     \
                                    __FILE__, 0)                               \
         : errno)

/* Note - for trylock, both 0 and EBUSY are non-error returns. */
#define assert_pthread_mutex_trylock(lock)                                     \
    ((errno = pthread_mutex_trylock((lock))) != 0 && (errno != EBUSY)          \
         ? assert_pthread_lock_func(errno, "pthread_mutex_trylock", __LINE__,  \
                                    __FILE__, 0)                               \
         : errno)

#define assert_pthread_mutex_unlock(lock)                                      \
    ((errno = pthread_mutex_unlock((lock)))                                    \
         ? assert_pthread_lock_func(errno, "pthread_mutex_unlock", __LINE__,   \
                                    __FILE__, 0)                               \
         : errno)

#else /* ! __cplusplus */

#define assert_pthread_mutex_init(lock, attr)                                  \
    ((errno = pthread_mutex_init((lock), (attr)))                              \
         ? assert_pthread_lock_func(errno, "pthread_mutex_init", __LINE__,     \
                                    __FILE__, __func__)                        \
         : errno)

#define assert_pthread_mutex_lock(lock)                                        \
    ((errno = pthread_mutex_lock((lock)))                                      \
         ? assert_pthread_lock_func(errno, "pthread_mutex_lock", __LINE__,     \
                                    __FILE__, __func__)                        \
         : errno)

/* Note - for trylock, both 0 and EBUSY are non-error returns. */
#define assert_pthread_mutex_trylock(lock)                                     \
    ((errno = pthread_mutex_trylock((lock))) != 0 && (errno != EBUSY)          \
         ? assert_pthread_lock_func(errno, "pthread_mutex_trylock", __LINE__,  \
                                    __FILE__, __func__)                        \
         : errno)

#define assert_pthread_mutex_unlock(lock)                                      \
    ((errno = pthread_mutex_unlock((lock)))                                    \
         ? assert_pthread_lock_func(errno, "pthread_mutex_unlock", __LINE__,   \
                                    __FILE__, __func__)                        \
         : errno)

#endif

#endif
