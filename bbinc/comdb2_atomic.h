#ifndef __COMDB2_ATOMIC_H__
#define __COMDB2_ATOMIC_H__

#ifdef _SUN_SOURCE
#include <atomic.h>
#define CAS(mem, oldv, newv) (atomic_cas_32(&mem, oldv, newv) == newv)
#define XCHANGE(mem, newv) atomic_swap_32(&mem, newv)
#define ATOMIC_LOAD(mem) (atomic_add_int_nv((int *)&mem, 0))
#define ATOMIC_ADD(mem, val) (atomic_add_int_nv((int *)&mem, val))
#endif

#ifdef _LINUX_SOURCE
#define CAS(mem, oldv, newv)                                                   \
    __atomic_compare_exchange(&mem, &oldv, &newv, 0, __ATOMIC_SEQ_CST,         \
                              __ATOMIC_SEQ_CST)

#define XCHANGE(mem, newv)                                                     \
    __atomic_exchange_n((volatile int *)&mem, newv, __ATOMIC_SEQ_CST)
#define ATOMIC_LOAD(mem)                                                       \
    (__atomic_load_n((long long int *)&mem, __ATOMIC_SEQ_CST))
#define ATOMIC_ADD(mem, val) __atomic_add_fetch(&mem, val, __ATOMIC_SEQ_CST)
#endif

#ifdef _IBM_SOURCE
#define CAS(mem, oldv, newv)                                                   \
    __compare_and_swap((int *)&mem, (int *)&oldv, (*(int *)&newv))
#define XCHANGE(mem, newv) __fetch_and_swap(&mem, newv)

/* cant use    (__lwarx ((int*)&mem)); because needs POWER8 */
#define ATOMIC_LOAD(mem)                                                       \
    (__sync_fetch_and_add((volatile unsigned int *)&mem, 0))
#define ATOMIC_ADD(mem, val)                                                   \
    (__sync_add_and_fetch((volatile unsigned int *)&mem, val))
#endif

#endif
