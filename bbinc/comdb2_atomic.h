#ifndef __COMDB2_ATOMIC_H__
#define __COMDB2_ATOMIC_H__

#include <stdint.h>

/* gcc builtins accept any width, so a 32-bit op on a 64-bit word works on
 * Linux but only touches the high half on big-endian Solaris: refuse to
 * compile on either platform */
#define COMDB2_ATOMIC_SIZE(x, n) ((void)sizeof(char[sizeof(x) == (n) ? 1 : -1]))

#if defined(_SUN_SOURCE)
  #include <atomic.h>

/* Solaris runs user code under TSO, where every load is already an acquire
 * and every store a release, and all seq_cst writes here are cas/swap RMWs,
 * which are full fences. So seq_cst/acquire loads and release stores need
 * only stop the compiler from reordering; aligned 32/64-bit accesses are
 * single instructions.
 *
 * TSO is guaranteed: on SPARC the model is the privileged PSTATE.MM, which
 * Solaris sets to TSO for user code; sun4v CPUs implement no other model.
 * gcc relies on the same: on Solaris its default is -mmemory-model=tso, and
 * it emits no membar for acquire loads or release stores. */
  #define COMDB2_COMPILER_BARRIER() __asm__ __volatile__("" ::: "memory")

  /* (mem) + 0 yields the operand's type without qualifiers, matching the
   * return type of the gcc builtins */
  #define COMDB2_ATOMIC_TYPE(mem) __typeof__((mem) + 0)

  /* Statement expressions rather than static inline functions: Studio cc
   * leaves inline functions as calls below -xO3 (RelWithDebInfo is -xO2) */
  #define COMDB2_LOAD(mem, T)                                                                                          \
      ({                                                                                                               \
          T comdb2_atomic_v = *(volatile T *)&(mem);                                                                   \
          COMDB2_COMPILER_BARRIER();                                                                                   \
          (COMDB2_ATOMIC_TYPE(mem)) comdb2_atomic_v;                                                                   \
      })

  /* Same contract as __atomic_compare_exchange_n: on failure the current value
   * is written back into oldv */
  #define COMDB2_CAS(mem, oldv, newv, T, fn)                                                                           \
      ({                                                                                                               \
          T *comdb2_atomic_o = (T *)&(oldv);                                                                           \
          T comdb2_atomic_e = *comdb2_atomic_o;                                                                        \
          T comdb2_atomic_c = fn((volatile T *)&(mem), comdb2_atomic_e, (newv));                                       \
          if (comdb2_atomic_c != comdb2_atomic_e)                                                                      \
              *comdb2_atomic_o = comdb2_atomic_c;                                                                      \
          comdb2_atomic_c == comdb2_atomic_e;                                                                          \
      })

  #define CAS32(mem, oldv, newv)                                                                                       \
      (COMDB2_ATOMIC_SIZE(mem, 4), COMDB2_ATOMIC_SIZE(oldv, 4), COMDB2_CAS(mem, oldv, newv, uint32_t, atomic_cas_32))
  #define CAS64(mem, oldv, newv)                                                                                       \
      (COMDB2_ATOMIC_SIZE(mem, 8), COMDB2_ATOMIC_SIZE(oldv, 8), COMDB2_CAS(mem, oldv, newv, uint64_t, atomic_cas_64))
  #define XCHANGE32(mem, newv)                                                                                         \
      ((COMDB2_ATOMIC_TYPE(mem))(COMDB2_ATOMIC_SIZE(mem, 4), atomic_swap_32((volatile uint32_t *)&(mem), (newv))))
  #define XCHANGE64(mem, newv)                                                                                         \
      ((COMDB2_ATOMIC_TYPE(mem))(COMDB2_ATOMIC_SIZE(mem, 8), atomic_swap_64((volatile uint64_t *)&(mem), (newv))))
  #define ATOMIC_LOAD32(mem) (COMDB2_ATOMIC_SIZE(mem, 4), COMDB2_LOAD(mem, uint32_t))
  #define ATOMIC_LOAD64(mem) (COMDB2_ATOMIC_SIZE(mem, 8), COMDB2_LOAD(mem, uint64_t))
  #define ATOMIC_ADD32(mem, val)                                                                                       \
      ((COMDB2_ATOMIC_TYPE(mem))(COMDB2_ATOMIC_SIZE(mem, 4), atomic_add_32_nv((volatile uint32_t *)&(mem), (val))))
  #define ATOMIC_ADD64(mem, val)                                                                                       \
      ((COMDB2_ATOMIC_TYPE(mem))(COMDB2_ATOMIC_SIZE(mem, 8), atomic_add_64_nv((volatile uint64_t *)&(mem), (val))))
  #define ATOMIC_ADD32_PTR(mem, val)                                                                                   \
      ((COMDB2_ATOMIC_TYPE(*(mem)))(COMDB2_ATOMIC_SIZE(*(mem), 4),                                                     \
                                    atomic_add_32_nv((volatile uint32_t *)(mem), (val))))
  #define ATOMIC_LOAD32_ACQ(mem) ATOMIC_LOAD32(mem)
  #define ATOMIC_STORE32_REL(mem, val)                                                                                 \
      ({                                                                                                               \
          COMDB2_ATOMIC_SIZE(mem, 4);                                                                                  \
          COMDB2_COMPILER_BARRIER();                                                                                   \
          *(volatile uint32_t *)&(mem) = (val);                                                                        \
          (void)0;                                                                                                     \
      })
#elif defined(_LINUX_SOURCE)
  #define CAS32(mem, oldv, newv)                                                                                       \
      (COMDB2_ATOMIC_SIZE(mem, 4), COMDB2_ATOMIC_SIZE(oldv, 4),                                                        \
       __atomic_compare_exchange_n(&(mem), &(oldv), (newv), 0, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST))
  #define CAS64(mem, oldv, newv)                                                                                       \
      (COMDB2_ATOMIC_SIZE(mem, 8), COMDB2_ATOMIC_SIZE(oldv, 8),                                                        \
       __atomic_compare_exchange_n(&(mem), &(oldv), (newv), 0, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST))
  #define XCHANGE32(mem, newv) (COMDB2_ATOMIC_SIZE(mem, 4), __atomic_exchange_n(&(mem), (newv), __ATOMIC_SEQ_CST))
  #define XCHANGE64(mem, newv) (COMDB2_ATOMIC_SIZE(mem, 8), __atomic_exchange_n(&(mem), (newv), __ATOMIC_SEQ_CST))
  #define ATOMIC_LOAD32(mem) (COMDB2_ATOMIC_SIZE(mem, 4), __atomic_load_n(&(mem), __ATOMIC_SEQ_CST))
  #define ATOMIC_LOAD64(mem) (COMDB2_ATOMIC_SIZE(mem, 8), __atomic_load_n(&(mem), __ATOMIC_SEQ_CST))
  #define ATOMIC_ADD32(mem, val) (COMDB2_ATOMIC_SIZE(mem, 4), __atomic_add_fetch(&(mem), (val), __ATOMIC_SEQ_CST))
  #define ATOMIC_ADD64(mem, val) (COMDB2_ATOMIC_SIZE(mem, 8), __atomic_add_fetch(&(mem), (val), __ATOMIC_SEQ_CST))
  #define ATOMIC_ADD32_PTR(mem, val) (COMDB2_ATOMIC_SIZE(*(mem), 4), __atomic_add_fetch((mem), (val), __ATOMIC_SEQ_CST))
  #define ATOMIC_LOAD32_ACQ(mem) (COMDB2_ATOMIC_SIZE(mem, 4), __atomic_load_n(&(mem), __ATOMIC_ACQUIRE))
  #define ATOMIC_STORE32_REL(mem, val) (COMDB2_ATOMIC_SIZE(mem, 4), __atomic_store_n(&(mem), (val), __ATOMIC_RELEASE))
#else
  #error "Missing atomic primitives"
#endif

#endif
