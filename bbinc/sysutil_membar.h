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

/**
 * sysutil_membar.h
 *
 *   macros to implement portable memory barriers
 *   (for use in lockless algorithms in reentrant, thread-safe code)
 *
 * See http://en.cppreference.com/w/c/atomic/memory_order for technical overview
 * of memory ordering.  The memory barriers below aim to provide equivalent or
 * stronger guarantees than those provided by C11/C++11 atomic_thread_fence().
 *
 * gstrauss1@bloomberg.net
 *
 * Status: 2009.08.10  alpha
 *   memory barriers might be more pessimistic than needed, but useable now.
 * Status: 2010.06.02  beta
 *   SYSUTIL_MEMBAR_LOCK_CAS() returns 0 if lock acquired, 1 if not acquired.
 *     Lock ptr must be int * and initialized to 0 (unlocked) before first use.
 * Status: 2013.05.12  prod  (have been used in prod for a number of years now)
 *   better optimized barriers providing minimal, but sufficient, memory order
 **/

#ifndef INCLUDED_SYSUTIL_MEMBAR_H
#define INCLUDED_SYSUTIL_MEMBAR_H

#if defined(__sparc) || defined(__sparc__)

#include <atomic.h>

#define SYSUTIL_MEMBAR_ccfence() asm volatile("" : : : "memory")

/* NOTE: ACQUIRE and RELEASE barriers here are stronger than needed.
 * SPARC runs in TSO (total store order) and only StoreLoad barrier must
 * be explicitly specified when needed.  (There is some older, buggy SPARC
 * chips that need stronger barriers, but those are ancient we need not
 * support them here.)  Therefore, acquire and release need only be compiler
 * fences.  To ensure compatibility with previous code that might have misused
 * acquire or release when it really needed StoreLoad, this code is not being
 * changed.  Instead, new macros are introduced (with lowercased suffix). */

#define SYSUTIL_MEMBAR_StoreStore()                                            \
    asm volatile("membar #StoreStore" : : : "memory")
#define SYSUTIL_MEMBAR_storestore() asm volatile("" : : : "memory")

#define SYSUTIL_MEMBAR_ACQUIRE()                                               \
    asm volatile("membar #StoreLoad|#StoreStore" : : : "memory")
#define SYSUTIL_MEMBAR_acquire() asm volatile("" : : : "memory")

#define SYSUTIL_MEMBAR_RELEASE()                                               \
    asm volatile("membar #LoadStore|#StoreStore" : : : "memory")
#define SYSUTIL_MEMBAR_release() asm volatile("" : : : "memory")

#define SYSUTIL_MEMBAR_FULLSYNC() asm volatile("membar #Sync" : : : "memory")
/*(might instead use #MemIssue instead of full #Sync)*/

#define SYSUTIL_MEMBAR_LOCK_CAS(ptr) atomic_cas_32((ptr), 0, 1)
/*(inline assembly would require args in registers)*/
/*(Sun Studio requires optimization for proper inline assembly cas)*/

#define SYSUTIL_MEMBAR_LOCK_RELEASE(ptr)                                       \
    SYSUTIL_MEMBAR_RELEASE();                                                  \
    *(ptr) = 0

#elif defined(__ppc__) || defined(_ARCH_PPC) || defined(_ARCH_PWR) ||          \
    defined(_ARCH_PWR2) || defined(_POWER)

/* POWER chips */

#ifdef _AIX
#include <sys/atomic_op.h>
#endif

#if !defined(__GNUC__)

#define SYSUTIL_MEMBAR_ccfence() __fence()

#define SYSUTIL_MEMBAR_StoreStore() __lwsync()

#define SYSUTIL_MEMBAR_ACQUIRE() __isync()
#define SYSUTIL_MEMBAR_acquire() __lwsync()
/* __lwsync() faster than __isync() on POWER6 and above
 * and same/negligible performance difference on POWER5 */

#define SYSUTIL_MEMBAR_RELEASE() __lwsync()

#define SYSUTIL_MEMBAR_FULLSYNC() __sync()

#define SYSUTIL_MEMBAR_LOCK_CAS(ptr) __check_lock_mp((atomic_p)(ptr), 0, 1)

#define SYSUTIL_MEMBAR_LOCK_RELEASE(ptr) __clear_lock_mp((atomic_p)(ptr), 0)

#else /* __GNUC__ */

#define SYSUTIL_MEMBAR_ccfence() asm __volatile__("" : : : "memory")

#define SYSUTIL_MEMBAR_StoreStore() asm __volatile__("lwsync" : : : "memory")

#define SYSUTIL_MEMBAR_ACQUIRE() asm __volatile__("isync" : : : "memory")
#define SYSUTIL_MEMBAR_acquire() asm __volatile__("lwsync" : : : "memory")

#define SYSUTIL_MEMBAR_RELEASE() asm __volatile__("lwsync" : : : "memory")

#define SYSUTIL_MEMBAR_FULLSYNC() asm __volatile__("sync" : : : "memory")

#ifdef _AIX

#define SYSUTIL_MEMBAR_LOCK_CAS(ptr) _check_lock((atomic_p)(ptr), 0, 1)

#define SYSUTIL_MEMBAR_LOCK_RELEASE(ptr) _clear_lock((atomic_p)(ptr), 0)

#elif defined(__linux__)

#define SYSUTIL_MEMBAR_LOCK_CAS(ptr)                                           \
    (!!__sync_val_compare_and_swap((ptr), 0, 1))
/* force bool context */

#define SYSUTIL_MEMBAR_LOCK_RELEASE(ptr) __sync_lock_release(ptr)

#endif

#endif /* __GNUC__ */

#elif(defined(__ia64) || defined(__ia64__) || defined(_M_IA64))

/* Itanium chips running HP-UX */
/* _Asm_mf(): memory fence for hardware memory sync (expensive) */

#include <machine/sys/inline.h>

#define SYSUTIL_MEMBAR_ccfence()                                               \
    _Asm_sched_fence(_UP_MEM_FENCE | _DOWN_MEM_FENCE)

#define SYSUTIL_MEMBAR_StoreStore() _Asm_mf(_UP_MEM_FENCE | _DOWN_MEM_FENCE)
/* anything better than full sync (expensive)? */

#define SYSUTIL_MEMBAR_ACQUIRE() _Asm_mf(_UP_MEM_FENCE | _DOWN_MEM_FENCE)
/* anything better than full sync (expensive)? */

#define SYSUTIL_MEMBAR_RELEASE() _Asm_mf(_UP_MEM_FENCE | _DOWN_MEM_FENCE)
/* anything better than full sync (expensive)? */

#define SYSUTIL_MEMBAR_FULLSYNC() _Asm_mf(_UP_MEM_FENCE | _DOWN_MEM_FENCE)

#define SYSUTIL_MEMBAR_LOCK_CAS(ptr) _Asm_xchg(_SZ_W, (ptr), 1, _LDHINT_NONE)
/* _Asm_xchg includes acquire semantics */

#define SYSUTIL_MEMBAR_LOCK_RELEASE(ptr)                                       \
    _Asm_st_volatile(_SZ_W, _STHINT_NONE, (ptr), 0)
/* store volatile data with 'release' memory barrier semantics
 * despite optional usage of +Ovolatile=__unordered compiler flag */
#define SYSUTIL_MEMBAR_LOCK_release(ptr)                                       \
    _Asm_st_volatile(_SZ_W, _STHINT_NONE, (ptr), 0)

#elif defined(__i386__) || defined(__x86_64__)

/* x86{,_64} chips */
/* (we currently use GCC on Linux; as of 02/11/10, we must support GCC 3)
 * (additionally, __sync_synchronize() in GCC 4.3 doesn't appear to work:
 *  http://gcc.gnu.org/bugzilla/show_bug.cgi?id=36793) */

#define SYSUTIL_MEMBAR_ccfence() __asm__ __volatile__("" : : : "memory")

#define SYSUTIL_MEMBAR_StoreStore() __asm__ __volatile__("" : : : "memory")
/* (x86 doesn't reorder stores) */
/* (exception: "temporaral move" instructions, fast string ops) */

/* NOTE: ACQUIRE and RELEASE barriers here are stronger than needed.
 * Intel clarified spec in 2010 that memory operations to typical WC memory
 * is ordered except for StoreLoad, so acquire and release need only be
 * compiler fences.  To ensure compatibility with previous code that might
 * have misused acquire or release when it really needed StoreLoad, this code
 * is not being changed.  Instead, new macros introduced (lowercased suffix)*/

#define SYSUTIL_MEMBAR_acquire() __asm__ __volatile__("" : : : "memory")

#define SYSUTIL_MEMBAR_release() __asm__ __volatile__("" : : : "memory")

#ifdef __SSE2__
#define SYSUTIL_MEMBAR_ACQUIRE() __asm__ __volatile__("mfence" : : : "memory")
#else
#define SYSUTIL_MEMBAR_ACQUIRE()                                               \
    __asm__ __volatile__("lock addl $0, 0(%%esp)" : : : "memory")
#endif

#ifdef __SSE2__
#define SYSUTIL_MEMBAR_RELEASE() __asm__ __volatile__("mfence" : : : "memory")
#else
#define SYSUTIL_MEMBAR_RELEASE()                                               \
    __asm__ __volatile__("lock addl $0, 0(%%esp)" : : : "memory")
#endif

#ifdef __SSE2__
#define SYSUTIL_MEMBAR_FULLSYNC() __asm__ __volatile__("mfence" : : : "memory")
#else
#define SYSUTIL_MEMBAR_FULLSYNC()                                              \
    __asm__ __volatile__("lock addl $0, 0(%%esp)" : : : "memory")
#endif

#define SYSUTIL_MEMBAR_LOCK_CAS(ptr)                                           \
    (__extension__({                                                           \
        int result;                                                            \
        __asm__ __volatile__("lock cmpxchgl %1,%2"                             \
                             : "=q"(result)                                    \
                             : "r"(1), "m"(*(ptr)), "0"(0)                     \
                             : "memory", "cc");                                \
        result;                                                                \
    }))

#define SYSUTIL_MEMBAR_LOCK_RELEASE(ptr)                                       \
    SYSUTIL_MEMBAR_RELEASE();                                                  \
    *(ptr) = 0

#elif defined(__GNUC__)

/* GNU C 4 builtins */

#define SYSUTIL_MEMBAR_ccfence() __asm__ __volatile__("" : : : "memory")

#define SYSUTIL_MEMBAR_StoreStore()                                            \
    __sync_synchronize() /* anything better than full sync (expensive)? */

#define SYSUTIL_MEMBAR_ACQUIRE()                                               \
    __sync_synchronize() /* anything better than full sync (expensive)? */

#define SYSUTIL_MEMBAR_RELEASE()                                               \
    __sync_synchronize() /* anything better than full sync (expensive)? */

#define SYSUTIL_MEMBAR_FULLSYNC() __sync_synchronize()

#define SYSUTIL_MEMBAR_LOCK_CAS(ptr)                                           \
    (!!__sync_val_compare_and_swap((ptr), 0, 1))
/* force bool context */

#define SYSUTIL_MEMBAR_LOCK_RELEASE(ptr) __sync_lock_release(ptr)

#else

/* not implemented */
#error sysutil membar macros not implemented for this platform

#endif

#ifndef SYSUTIL_MEMBAR_storestore
#define SYSUTIL_MEMBAR_storestore() SYSUTIL_MEMBAR_StoreStore()
#endif
#ifndef SYSUTIL_MEMBAR_acquire
#define SYSUTIL_MEMBAR_acquire() SYSUTIL_MEMBAR_ACQUIRE()
#endif
#ifndef SYSUTIL_MEMBAR_release
#define SYSUTIL_MEMBAR_release() SYSUTIL_MEMBAR_RELEASE()
#endif
#ifndef SYSUTIL_MEMBAR_fullsync
#define SYSUTIL_MEMBAR_fullsync() SYSUTIL_MEMBAR_FULLSYNC()
#endif
#ifndef SYSUTIL_MEMBAR_lock_cas
#define SYSUTIL_MEMBAR_lock_cas(ptr) SYSUTIL_MEMBAR_LOCK_CAS(ptr)
#endif
#ifndef SYSUTIL_MEMBAR_lock_release
#define SYSUTIL_MEMBAR_lock_release(ptr)                                       \
    SYSUTIL_MEMBAR_release();                                                  \
    *(ptr) = 0
#endif
#ifndef SYSUTIL_MEMBAR_ccfence
#define SYSUTIL_MEMBAR_ccfence() SYSUTIL_MEMBAR_storestore()
#endif

/*
 * atomic compare and swap function for 32-bit values
 * atomically compares *ptr with cmp and replaces *ptr with val only if *ptr ==
 * cmp
 * returns 1 if *ptr was replaced by val, 0 otherwise
 */
int sysutil_membar_cas_32(volatile unsigned int *ptr, unsigned int cmp,
                          unsigned int val);
/*
 * atomic increment of unsigned int
 * returns value of *ptr before increment
 */
unsigned int sysutil_membar_get_inc_uint(volatile unsigned int *ptr);

/* References
 *
 * https://github.com/gstrauss/mcdb and see plasma/plasma_membar.h
 *   (gstrauss@gluelogic.com is author and is me (gstrauss1@bloomberg.net))
 *
 * Intel x86 chips
 * Intel(R) 64 and IA-32 Architectures Developer's Manual: Vol. 3A
* http://www.intel.com/content/www/us/en/processors/architectures-software-developer-manuals.html
* http://www.intel.com/content/dam/www/public/us/en/documents/manuals/64-ia-32-architectures-software-developer-vol-3a-part-1-manual.pdf
 *
 * POWER chips
 * http://www.ibm.com/developerworks/systems/articles/powerpc.html
 *
 * HP-UX on Itanium
 * http://h21007.www2.hp.com/portal/download/files/unprot/Itanium/inline_assem_ERS.pdf
 * http://h21007.www2.hp.com/portal/download/files/unprot/itanium/spinlocks.pdf
 */

#endif /* INCLUDED_SYSUTIL_MEMBAR_H */
