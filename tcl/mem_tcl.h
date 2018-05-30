#ifndef INCLUDED_COMDB2MA_STATIC_TCLCDB2_H
#define INCLUDED_COMDB2MA_STATIC_TCLCDB2_H
#include <stddef.h>
#include <mem.h>
#include <mem_int.h>

#ifndef USE_SYS_ALLOC
static inline void *comdb2_malloc_tclcdb2(size_t n)
{
    return comdb2_malloc_static(COMDB2MA_STATIC_TCLCDB2, n);
}

static inline void *comdb2_malloc0_tclcdb2(size_t n)
{
    return comdb2_malloc0_static(COMDB2MA_STATIC_TCLCDB2, n);
}

static inline void *comdb2_calloc_tclcdb2(size_t n, size_t size)
{
    return comdb2_calloc_static(COMDB2MA_STATIC_TCLCDB2, n, size);
}

static inline void *comdb2_realloc_tclcdb2(void *ptr, size_t n)
{
    return comdb2_realloc_static(COMDB2MA_STATIC_TCLCDB2, ptr, n);
}

static inline void *comdb2_resize_tclcdb2(void *ptr, size_t n)
{
    return comdb2_resize_static(COMDB2MA_STATIC_TCLCDB2, ptr, n);
}

static inline char *comdb2_strdup_tclcdb2(const char *s)
{
    return comdb2_strdup_static(COMDB2MA_STATIC_TCLCDB2, s);
}

static inline char *comdb2_strndup_tclcdb2(const char *s, size_t n)
{
    return comdb2_strndup_static(COMDB2MA_STATIC_TCLCDB2, s, n);
}

static inline int  comdb2_malloc_trim_tclcdb2(size_t pad)
{
    return comdb2_malloc_trim_static(COMDB2MA_STATIC_TCLCDB2, pad);
}

static inline int  comdb2_attach_tclcdb2(comdb2ma alloc)
{
    return comdb2ma_attach_static(COMDB2MA_STATIC_TCLCDB2, alloc);
}

/* just an alias */
#  define comdb2_free_tclcdb2 comdb2_free

/*******************************************
**                                         *
** MAKE_FUNC_NAME macro                    *
**                                         *
*******************************************/
#  undef MAKE_FUNC_NAME_INT
#  undef MAKE_FUNC_NAME_EVAL
#  undef MAKE_FUNC_NAME

#  define MAKE_FUNC_NAME_INT(func, subsystem) comdb2 ## func ## subsystem
#  define MAKE_FUNC_NAME_EVAL(func, subsystem) MAKE_FUNC_NAME_INT(func, subsystem)
#  define MAKE_FUNC_NAME(func) MAKE_FUNC_NAME_EVAL(_##func##_, tclcdb2)
#  else /* !USE_SYS_ALLOC */
#  include <stdlib.h>
#  define comdb2_malloc_tclcdb2  malloc
#  define comdb2_calloc_tclcdb2  calloc
#  define comdb2_realloc_tclcdb2 realloc
#  define comdb2_free_tclcdb2    free
#  define comdb2_strdup_tclcdb2  strdup
#  define malloc_resize_tclcdb2  realloc
#endif /* USE_SYS_ALLOC */
#endif
