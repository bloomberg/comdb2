#ifndef __COMDB2_ATOMIC_H__
#define __COMDB2_ATOMIC_H__

#if defined(_SUN_SOURCE)
  #include <atomic.h>
  #define CAS32(mem, oldv, newv) (atomic_cas_32(&mem, oldv, newv) == newv)
  #define XCHANGE32(mem, newv) atomic_swap_32(&mem, newv)
  #define XCHANGE64(mem, newv) atomic_swap_64(&mem, newv)
  #define ATOMIC_LOAD32(mem) atomic_add_32_nv(&mem, 0)
  #define ATOMIC_LOAD64(mem) atomic_add_64_nv(&mem, 0)
  #define ATOMIC_ADD32(mem, val) atomic_add_32_nv(&mem, val)
  #define ATOMIC_ADD64(mem, val) atomic_add_64_nv(&mem, val)
  #define ATOMIC_ADD32_PTR(mem, val) atomic_add_32_nv(mem, val)
#elif defined(_LINUX_SOURCE)
  #define CAS32(mem, oldv, newv) __atomic_compare_exchange_n(&mem, &oldv, newv, 0, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST)
  #define XCHANGE32(mem, newv) __atomic_exchange_n(&mem, newv, __ATOMIC_SEQ_CST)
  #define XCHANGE64(mem, newv) __atomic_exchange_n(&mem, newv, __ATOMIC_SEQ_CST)
  #define ATOMIC_LOAD32(mem) __atomic_load_n(&mem, __ATOMIC_SEQ_CST)
  #define ATOMIC_LOAD64(mem) __atomic_load_n(&mem, __ATOMIC_SEQ_CST)
  #define ATOMIC_ADD32(mem, val) __atomic_add_fetch(&mem, val, __ATOMIC_SEQ_CST)
  #define ATOMIC_ADD64(mem, val) __atomic_add_fetch(&mem, val, __ATOMIC_SEQ_CST)
  #define ATOMIC_ADD32_PTR(mem, val) __atomic_add_fetch(mem, val, __ATOMIC_SEQ_CST)
#elif defined(_IBM_SOURCE)
  #define CAS32(mem, oldv, newv) __compare_and_swap(&mem, &oldv, newv)
  #define XCHANGE32(mem, newv) __fetch_and_swap(&mem, newv)
  #define XCHANGE64(mem, newv) __fetch_and_swaplp(&mem, newv)
  #define ATOMIC_LOAD32(mem) __sync_fetch_and_add(&mem, 0)
  #define ATOMIC_LOAD64(mem) __sync_fetch_and_add(&mem, 0)
  #define ATOMIC_ADD32(mem, val) __sync_add_and_fetch(&mem, val)
  #define ATOMIC_ADD64(mem, val) __sync_add_and_fetch(&mem, val)
  #define ATOMIC_ADD32_PTR(mem, val) __sync_add_and_fetch(mem, val)
#else
  #error "Missing atomic primitives"
#endif

#endif
