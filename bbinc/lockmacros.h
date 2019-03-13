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

#ifndef INCLUDED_LOCKMACROS
#define INCLUDED_LOCKMACROS

/*
 * LOCK MACROS FOR PTHREAD_MUTEX_LOCK
 *
 * These allow you to enclose locked sections of code within compound
 * statements.  For example:
 *
 * pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
 * LOCK(&lock) {
 *   // do stuff under lock
 * } UNLOCK(&lock);
 *
 * rather than:
 *
 * pthread_mutex_lock(&lock);
 * // do stuff under lock
 * pthread_mutex_unlock(&lock);
 *
 *
 * By enclosing the lock portion within a compound statement you gain a degree
 * of static checking (the compiler will complain if you have a LOCK() without
 * a corresponding UNLOCK()).  Also, these macros check the result of the
 * pthread mutex functions.
 *
 * If you like, these provide the poor man's C equivalent of C++ scoped lock
 * guards.
 *
 * Paul was the original author of this file.
 *
 *
 * April 2011 - Sam Jervis altered this module to use the assert_pthread_mutex_
 * (2019 using Pthread_mutex_)
 * wrappers, altering the behaviour so that if a lock acquisition fails the
 * program will abort rather than blindly carry on.  This should be ok -
 * carrying on regardless will likely just corrupt memory (as things which
 * should be accessed exclusively end up getting accessed simultaneously
 * by multiple threads), leading to a bigger mess later on.
 * Note that as a side effect of this change, using these macros will also
 * alter the errno variable.
 */

#include <pthread.h>
#include <stdio.h>

#include "locks_wrap.h"

/*
 * LOCK() {  ...  } UNLOCK()
 */

#define LOCK(lk)                                                               \
    Pthread_mutex_lock(lk);                                                    \
    do
#define UNLOCK(lk)                                                             \
    while (0)                                                                  \
        ;                                                                      \
    Pthread_mutex_unlock(lk);
#define SKIPUNLOCK(lk)                                                         \
    while (0)                                                                  \
        ;

/*
 * LOCKIFNZ() {  ...  } UNLOCKIFNZ()
 *
 * PASS IN VARIABLE TO CHECK IF LOCK BE PERFORMED.  Note that you should
 * make sure that the variable is immutable over the course of the lock, or
 * you will have a bug where the lock decision is different in UNLOCKIFNZ()
 * than it is in LOCKIFNZ().
 */

#define LOCKIFNZ(lk, a)                                                        \
    if (a)                                                                     \
        Pthread_mutex_lock(lk);                                                \
    do
#define UNLOCKIFNZ(lk, a)                                                      \
    while (0)                                                                  \
        ;                                                                      \
    if (a)                                                                     \
        Pthread_mutex_unlock(lk);

/*
 * xUNLOCK() {  ...  } xLOCK()
 *
 * Same semantics as LOCK/UNLOCK but used to temporarily unlock.
 */

#define xUNLOCK(lk)                                                            \
    Pthread_mutex_unlock(lk);                                                  \
    do
#define xLOCK(lk)                                                              \
    while (0)                                                                  \
        ;                                                                      \
    Pthread_mutex_lock(lk);

/*
 * xUNLOCKIFNZ() { ... } xLOCKIFNZ()
 */

#define xUNLOCKIFNZ(lk, a)                                                     \
    if (a)                                                                     \
        Pthread_mutex_unlock(lk);                                              \
    do
#define xLOCKIFNZ(lk, a)                                                       \
    while (0)                                                                  \
        ;                                                                      \
    if (a)                                                                     \
        Pthread_mutex_lock(lk);

/*
 * errUNLOCK() .. return
 *
 * This is commonly used to release the lock in an error case, and so does not
 * require a corresponding lock paired with it after a compound statement..
 * For example:
 *
 * LOCK(&lock) {
 *   if(disaster) {
 *     errUNLOCK(&lock);
 *     fprintf(stderr, "Stuff went wrong\n");
 *     return -1;
 *   }
 * } UNLOCK(&lock);
 */

#define errUNLOCK(lk) Pthread_mutex_unlock(lk);

/*
 * errLOCK()
 *
 * Same semantics as errUNLOCK(), but loccks instead of unlocks.
 */
#define errLOCK(lk) Pthread_mutex_lock(lk);

/*
 * errUNLOCKIFNZ() ... return
 */

#define errUNLOCKIFNZ(lk, a)                                                   \
    if (a)                                                                     \
        Pthread_mutex_unlock(lk);

/* skip out early with return: */
#define retUNLOCK(lk) errUNLOCK(lk)

/* skip out early with return if nz: */ /* dhaniff */
#define retUNLOCKIFNZ(lk, a) errUNLOCKIFNZ(lk, a)

#endif
