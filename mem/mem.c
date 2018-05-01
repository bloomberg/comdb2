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

/*
** If PER_THREAD_MALLOC is enabled, each thread will have its own
** memory areas. Per-thread memory areas are NOT lockless,
** in case that a thread frees a pointer which was allocated within
** another thread. However, the performance will still be improved,
** because a thread can expect almost 0 contention among threads when
** it locks a memory area.
*/

#include <alloca.h>
#include <errno.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <ctype.h>
#include <pthread.h>
#include <limits.h>
#include <time.h>
#include <sys/time.h>
#include <sys/resource.h>

#include <dlmalloc.h>
#include <list.h>
#include <ctrace.h>
#include "mem.h"
#include <mem_int.h>
#include <logmsg.h>

//^macros
#define COMDB2MA_SUCCESS 0
#define COMDB2MA_ROOT_NAME "MemTrack"
#define COMDB2MA_CRON_PFX "[MEMSTAT]"
#define COMDB2MA_ROUND8(size) (((size) + 7) & ~7)
#define COMDB2MA_LOCK(M)                                                       \
    (((M)->use_lock) ? pthread_mutex_lock(&((M)->lock)) : 0)
#define COMDB2MA_UNLOCK(M)                                                     \
    (((M)->use_lock) ? pthread_mutex_unlock(&((M)->lock)) : 0)
#define COMDB2MA_HAS_CAP(M) ((M)->cap != COMDB2MA_UNLIMITED)
#define COMDB2MA_CAP(M) (mspace_footprint((M)->m) > (M)->cap)
#define COMDB2MA_FULL(M)                                                       \
    (((M)->cap != COMDB2MA_UNLIMITED) && (mspace_footprint((M)->m) > (M)->cap))
#define COMDB2MA_SENTINEL_OFS (-2)
#define COMDB2MA_ALLOC_OFS (-1)
#define COMDB2MA_OVERHEAD (sizeof(void *) << 1)
#define COMDB2MA_MIN_NAME_SZ 8
#define COMDB2MA_MIN_SCOPE_SZ 8
#define COMDB2MA_MAX_MEM ((~(size_t)0) - COMDB2MA_OVERHEAD)
#define COMDB2MA_THR_UNKNOWN "unspec."

#define COMDB2MA_SENTINEL(p, m)                                                \
    (void *)(((uintptr_t)(p) + (uintptr_t)(m)) ^ 0xCDB2C00l)
#define COMDB2MA_OK_SENTINEL(p)                                                \
    ((p)[COMDB2MA_SENTINEL_OFS] ==                                             \
     COMDB2MA_SENTINEL((p) + COMDB2MA_SENTINEL_OFS, (p)[COMDB2MA_ALLOC_OFS]))

#define COMDB2MA_MALLINFO_SAFE(cm)                                             \
    ((cm)->use_lock ? mspace_mallinfo((cm)->m) : mspace_mallinfo_fast((cm)->m))

#ifdef COMDB2MA_MEMABRT
#define COMDB2MA_MEMCHK(m, sz)                                                 \
    do {                                                                       \
        if (m == NULL) {                                                       \
            logmsg(LOGMSG_FATAL, "Out of memory: %zu", sz);                    \
            abort();                                                           \
        }                                                                      \
    } while (0)
#else
#define COMDB2MA_MEMCHK(m, sz)
#endif

// macros$

//^type definitions
typedef LINKC_T(struct comdb2mspace) mspacelink;
typedef LISTC_T(struct comdb2mspace) mspacelist;

struct comdb2mspace {
    mspacelink lnk;                        /* linkedlist node */
    mspacelink sibling;                    /* sibling */
    mspacelink freelnk;                    /* freelist link */
    mspacelink busylnk;                    /* busylist link */
    mspacelist children;                   /* children */
    struct comdb2mspace *parent;           /* parent msapce. */

    struct comdb2bmspace *bm; /* points to a blocking allocator */

    mspace m; /* dlmalloc mspace */

    int use_lock;         /* use lock? */
    pthread_mutex_t lock; /* mutex */

    size_t init_sz; /* initial size */
    size_t cap;     /* capacity */

#ifdef PER_THREAD_MALLOC
    size_t refs;    /* reference counter */
    int onfreelist; /* index in freelist */
    int nthds;      /* number of threads assigned to the mspace */
    pthread_t pid;  /* pthread which is holding the mspace or was holding
                       the mspace previously if scope is `onfreelist' */
#endif

    stats_fn print_stats_fn; /* print memory statistics */
    void *arg;               /* passed to print_stats_fn as the last argument */

    const char *file; /* file */
    const char *func; /* function */
    int line;         /* line */

    const char *thr_type; /* thread type.
                             we do not write it to name because an allocator
                             may be reused by another type of thread later on */

    size_t len;   /* length of name */
    char name[1]; /* name of the mspace */
};

struct comdb2bmspace {
    LINKC_T(struct comdb2bmspace) lnk; /* linkedlist node */
    comdb2ma alloc;                    /* comdb2ma */
    pthread_mutex_t *lock;             /* lock */
    size_t cap;                        /* memory cap */
    int external;                      /* 1 if lock is an external one */
    int owned;           /* 1 if the external lock is held by a thread
                            POSIX does not have a portable "invalid" pthread_t id,
                            thus we need this additional flag */
    pthread_t owner;     /* who is holding the external lock */
    pthread_cond_t cond; /* wait condition */
    int prio;            /* 1 if there is a priority thread
                            POSIX does not have a portable "invalid" pthread_t id,
                            thus we need this additional flag */
    int inherited; /* 1 if prio_tid inherits priority from another thread */
    pthread_t prio_tid;    /* priority thread id */
    pthread_t inherit_tid; /* thread id */
    int giveback;          /* 1 if inherit_tid wants priority back */

    pthread_key_t bmakey; /* thread local flag. */

    int nblocks; /* number of blocking threads */
};

/* the pthread key is shared amongst all blocking allocators */
static pthread_once_t privileged_once = PTHREAD_ONCE_INIT;
/* 0 - undetermined; 1 - yes; -1 - no */
static pthread_key_t privileged;
#define PRIO_UNKNOWN NULL
#define PRIO_YES ((void *)1)
#define PRIO_NO ((void *)-1)

/* for sorting */
typedef struct pair {
    comdb2ma cm;
    void *key;
} pair_t;

/* for ctrace() */
typedef void (*trc_t)(const char *, ...);
// type definitions$

//^static variables and function prototypes
/* static ma range check. return `ret' and
   set errno to EINVAL upon failure */
#ifndef INSECURE
#define STATIC_RANGE_CHECK(indx, ret)
#else
#define STATIC_RANGE_CHECK(indx, ret)                                          \
    do {                                                                       \
        if ((indx) < 1 || (indx) >= COMDB2MA_COUNT) {                          \
            errno = EINVAL;                                                    \
            return ret;                                                        \
        }                                                                      \
    } while (0)
#endif

/* root mspace */
static struct {
    mspacelist list; /* list of mspaces */
    LISTC_T(struct comdb2bmspace) blist; /* list of blocking mspaces */
    mspace m;             /* mspace for managing all other mspaces */
    pthread_mutex_t lock; /* mutex */
    int use_lock; /* make me compatible with COMDB2MA_LOCK/COMDB2MA_UNLOCK.
                     always set to 1. */
    int mmap_thresh; /* user defined MMAP_THRESHOLD */
    int nice; /* mem niceness */
#ifdef PER_THREAD_MALLOC
    pthread_t main_thr_id;
    pthread_key_t zone;
    mspacelist freelist[COMDB2MA_COUNT];
    mspacelist busylist[COMDB2MA_COUNT];
    int mspace_max;
    comdb2ma pos[COMDB2MA_COUNT];
#endif

} root = {.list = 0,
          .m = 0,
          .use_lock = COMDB2MA_MT_SAFE,
          .lock = PTHREAD_MUTEX_INITIALIZER,
          .mmap_thresh = 0,
          .nice = 0};

int gbl_mem_nice = 0;

/* internal comdb2ma creation */
static comdb2ma comdb2ma_create_int(void *base, size_t init_sz, size_t max_cap,
                                    const char *name, const char *scope,
                                    int lock, stats_fn print_stats_fn,
                                    void *arg, const char *file,
                                    const char *func, int line);

/* internal comdb2ma deletion */
static int comdb2ma_destroy_int(comdb2ma cm);

#ifdef PER_THREAD_MALLOC
__thread const char *thread_type_key;
static comdb2ma get_area(int indx);
static void destroy_zone(void *);
#else
#define get_area(indx) COMDB2_STATIC_MAS[indx]
#endif

/* convert `num' to human readable format (e.g., 1024 -> 1K) */
static char *mem_to_human_readable(size_t num, char buf[], int len);

/* convert a n-digit number to human readable format.
   n bytes for digits, 1 byte for unit (G/M/K) and 1 more byte for '\0'. */
#define MEM_TO_HR(num, n) mem_to_human_readable(num, alloca(n + 2), n + 2)

/* convert a tb-level number to human readable format. */
#define TB_TO_HR(num) MEM_TO_HR(num, 13)

/* search memory allocators by pattern */
static size_t ma_pair_search(pair_t *a, char *pattern, comdb2ma_group_by grp,
                             struct mallinfo *total);

/* sort memory allocators order by `ord' */
static void ma_pair_sort(pair_t *pairs, size_t len, comdb2ma_order_by ord);

/* dump array */
static void ma_pair_dump(pair_t *pairs, size_t len, int columns, int verbose,
                         int hr, struct mallinfo *total, int all, int toctrc);

/* free array */
static void ma_pair_clean(pair_t *pairs, size_t len);

/* add statistics of `cm' to `total' */
static void ma_stats_aggregate(comdb2ma cm, struct mallinfo *total);

/* return mallinfo of a comdb2 allocator (children's mallinfos deducted). */
static struct mallinfo ma_mallinfo(const comdb2ma cm);

/* return memory footprint of a comdb2 allocator (children's footprint
 * deducted). */
static size_t ma_footprint(const comdb2ma);

/* display grand total which is pointed by `info' */
static void ma_stats_total(size_t maxnamesz, size_t maxscopesz, int columns,
                           int verbose, int hr, const struct mallinfo *info,
                           int toctrc);

/* display statistics of root. `total' is passed for percentage calculation */
static void ma_stats_root(size_t maxnamesz, size_t maxscopesz, int columns,
                          int verbose, int hr, const struct mallinfo *total,
                          int toctrc);

/* display a dash array */
static void ma_stats_dashes(int length, int toctrc);

/* print table head. if `percentage' is no-zero, a '%' column head will be added
 */
static int ma_stats_head(size_t maxnamesz, size_t maxscopesz, int columns,
                         int verbose, int hr, int percentage, int toctrc);

/* default stats display function on `cm'.
   this will be overridden if a stats callback has been defined through
   comdb2ma_create */
static int ma_stats_int(size_t maxnamesz, size_t maxscopesz, int columns,
                        comdb2ma cm, int verbose, int hr,
                        const struct mallinfo *total, int toctrc);

/* CaSe-InSeNsItIvE wildcard matching.
   ? - single character.
 * - 0 or more. */
static int wildcard_match(const char *string, const char *pattern);

/* add mallinfo of `m' to `total' */
static void mspace_stats_aggregate(mspace m, struct mallinfo *total);
/***********
 * Sorters  *
 ***********/

/* order by alphabetical order */
static int cmpr_string_asc(const void *a, const void *b);

/* order by reverse alphabetical order */
static int cmpr_string_desc(const void *a, const void *b);

/* order by number asc */
static int cmpr_number_asc(const void *a, const void *b);

/* order by number desc */
static int cmpr_number_desc(const void *a, const void *b);

/* initialize the shared 'privileged' pthread key once */
static void privileged_init(void);

/* malloc/calloc helpers which will abort upon ENOMEM */
static void *abortable_malloc(size_t);
static void *abortable_realloc(void *, size_t);
static void *abortable_calloc(size_t, size_t);

/* ctrace with prefix */
static void pfx_ctrace(const char *format, ...);
// static variables and function prototypes$

//^root
int comdb2ma_init(size_t init_sz, size_t max_cap)
{
    int i, rc;
    char *env_mspace_max, *env_mmap_thresh;

    rc = COMDB2MA_LOCK(&root);
    if (rc != 0)
        return rc;

    if (root.m != NULL)
        rc = EPERM;
    else {
        root.m = create_mspace(0, 0);
        if (root.m == NULL)
            rc = ENOMEM;
        else {
#ifdef PER_THREAD_MALLOC
            /* 
             * MALLOC_ARENA_MAX:
             * 1. Each subsytem has at most MALLOC_ARENA_MAX mspaces.
             * 2. A thread is allowed to create one if there's no mspace on
             *    the freelist and # of mspaces is less than MALLOC_ARENA_MAX.
             * 3. If a thread is not allowed to create a mspace, it will
             *    be uniformly assigned one (to minimize contention).
             */
            if ((env_mspace_max = getenv("MALLOC_ARENA_MAX")) != NULL)
                root.mspace_max = atoi(env_mspace_max);
            /* In case MALLOC_ARENA_MAX isn't a valid numeric. */
            if (root.mspace_max <= 0)
                root.mspace_max = INT_MAX;
#endif
            /* Remember MALLOC_MMAP_THRESHOLD. */
            if ((env_mmap_thresh = getenv("MALLOC_MMAP_THRESHOLD_")) != NULL)
                root.mmap_thresh = atoi(env_mmap_thresh);
            /* Be nice (finger wagging). */
            comdb2ma_nice(NICE_MODERATE);

            mspace_setmorecore(root.m, abortable_malloc);
            mspace_setmorecore0(root.m, abortable_calloc);
            mspace_setdestcore(root.m, free);
            mspace_setrecore(root.m, abortable_realloc);

            listc_init(&(root.list), offsetof(struct comdb2mspace, lnk));
            listc_init(&(root.blist), offsetof(struct comdb2bmspace, lnk));

#ifdef PER_THREAD_MALLOC
            /* create freelists for threaded allocators */
            for (i = 0; i != COMDB2MA_COUNT; ++i) {
                listc_init(&root.freelist[i],
                           offsetof(struct comdb2mspace, freelnk));
                listc_init(&root.busylist[i],
                           offsetof(struct comdb2mspace, busylnk));
            }

            root.main_thr_id = pthread_self();
            rc = pthread_key_create(&root.zone, destroy_zone);
            if (rc != 0) {
                COMDB2MA_UNLOCK(&root);
                destroy_mspace(root.m);
                root.m = NULL;
                return rc;
            }
#endif /* PER_THREAD_MALLOC */

#ifndef USE_SYS_ALLOC
            /* create static mspaces for subsystems */
            for (i = 1; i != COMDB2MA_COUNT; ++i) {
                COMDB2_STATIC_MAS[i] = comdb2ma_create_int(
                    NULL, init_sz, max_cap, COMDB2_STATIC_MA_METAS[i].name,
                    NULL, COMDB2MA_MT_SAFE, NULL, NULL, __FILE__, __func__,
                    __LINE__);

                if (COMDB2_STATIC_MAS[i] == NULL) {
                    /* oops. rollback all previous progress */
                    rc = errno;
                    for (--i; i != 0; --i) {
                        comdb2ma_destroy_int(COMDB2_STATIC_MAS[i]);
                        COMDB2_STATIC_MAS[i] = NULL;
                    }

                    destroy_mspace(root.m);
                    root.m = NULL;
                    break;
                }
            }
#endif /* !USE_SYS_ALLOC */
        }
    }

    if (rc != 0)
        COMDB2MA_UNLOCK(&root);
    else
        rc = COMDB2MA_UNLOCK(&root);

    return rc;
}

int comdb2ma_exit(void)
{
    int i, rc;
    comdb2ma curpos, tmppos;

    rc = COMDB2MA_LOCK(&root);
    if (rc != 0)
        return rc;

    if (root.m == NULL)
        rc = EPERM;
    else {
        /* destroy all mspaces */
        LISTC_FOR_EACH_SAFE(&(root.list), curpos, tmppos, lnk)
        {
            if (COMDB2MA_LOCK(curpos) == 0) {
                destroy_mspace(curpos->m);
                curpos->m = NULL;
                COMDB2MA_UNLOCK(curpos);
            }
            if (curpos->use_lock)
                pthread_mutex_destroy(&curpos->lock);
            mspace_free(root.m, curpos);
            curpos = NULL;
        }

        /* reset listc */
        listc_init(&(root.list), offsetof(struct comdb2mspace, lnk));

        /* make each static ma point to NULL */
        for (i = 1; i != COMDB2MA_COUNT; ++i)
            COMDB2_STATIC_MAS[i] = NULL;

        /* destroy my mspace */
        destroy_mspace(root.m);
        root.m = NULL;
    }

    if (rc != 0)
        COMDB2MA_UNLOCK(&root);
    else
        rc = COMDB2MA_UNLOCK(&root);

    return rc;
}

int comdb2ma_release(void)
{
    int i, rc;
    comdb2ma curpos;

    rc = COMDB2MA_LOCK(&root);
    if (rc != 0)
        return rc;

    if (root.m == NULL)
        rc = EPERM;
    else {
        /* release all lock-protected mspaces */
        LISTC_FOR_EACH(&(root.list), curpos, lnk)
        {
            if (curpos->use_lock)
                comdb2_malloc_trim(curpos, 0);
        }
    }

    if (rc != 0)
        COMDB2MA_UNLOCK(&root);
    else
        rc = COMDB2MA_UNLOCK(&root);

    return rc;
}

int comdb2ma_stats(char *pattern, int verbose, int hr, comdb2ma_order_by ord,
                   comdb2ma_group_by grp, int toctrc)
{
    size_t idx, np, sz = 0;
    comdb2ma curpos, temp;
    struct mallinfo total = {0};
    int rc;

    rc = COMDB2MA_LOCK(&root);
    if (rc != 0)
        return rc;

    if (root.m == NULL)
        rc = EPERM;
    else {
        pair_t *pairs =
            mspace_calloc(root.m, listc_size(&(root.list)), sizeof(pair_t));
        if (pairs == NULL)
            rc = ENOMEM;
        else {
            sz = ma_pair_search(pairs, pattern, grp, &total);
            ma_pair_sort(pairs, sz, ord);

            if (pattern == NULL)
                mspace_stats_aggregate(root.m, &total);

            ma_pair_dump(pairs, sz, grp, verbose, hr, &total, pattern == NULL,
                         toctrc);
            ma_pair_clean(pairs, sz);
            mspace_free(root.m, pairs);

#ifdef COMDB2MA_DEBUG
            extern void malloc_stats(void);
            malloc_stats();
#endif
        }
    }

    if (rc != 0)
        COMDB2MA_UNLOCK(&root);
    else
        rc = COMDB2MA_UNLOCK(&root);

    return rc;
}

/* dlmalloc.h and malloc.h both define struct mallinfo.
   We include malloc.h in the function to avoid the conflict. */
int comdb2ma_nice(int niceness)
{
    int rc, narena;
    rc = mspace_mallopt(M_NICE, niceness);
    if (rc == 1) { /* 1 is success */
        root.nice = niceness;
        gbl_mem_nice = niceness;

#if !defined(USE_SYS_ALLOC) && defined(PER_THREAD_MALLOC)
#  undef M_MMAP_THRESHOLD
#  undef M_TRIM_THRESHOLD
#  undef M_GRANULARITY
#  ifndef __APPLE__
#    include <malloc.h> /* for M_ARENA_MAX */
#  endif
#  ifdef M_ARENA_MAX
        /* Override glibc M_ARENA_MAX if we're told to be nicer. */
        if (niceness >= NICE_MODERATE) {
            if (root.mspace_max == INT_MAX)
                mallopt(M_ARENA_MAX, 1);
            else {
                narena = root.mspace_max >> niceness - NICE_MODERATE;
                mallopt(M_ARENA_MAX, narena == 0 ? 1 : narena);
            }
        }
#  endif /* M_ARENA_MAX */
#endif /* !USE_SYS_ALLOC && PER_THREAD_MALLOC */
    }
    return !rc;
}

int comdb2ma_niceness()
{
    return root.nice;
}

int comdb2ma_mmap_threshold(void)
{
    return root.mmap_thresh;
}

int comdb2_mallopt(int opt, int val) { return mspace_mallopt(opt, val); }

size_t comdb2_malloc_usable_size(void *ptr)
{
    return (ptr == NULL)
               ? 0
               : (dlmalloc_usable_size((void **)ptr + COMDB2MA_SENTINEL_OFS) -
                  COMDB2MA_OVERHEAD);
}
// root$

//^dynamic
comdb2ma comdb2ma_create_with_callback(void *base, size_t sz, size_t cap,
                                       const char *name, const char *scope,
                                       int lock, stats_fn fn, void *arg,
                                       const char *file, const char *func,
                                       int line)
{
    comdb2ma alloc = NULL;
    if (COMDB2MA_LOCK(&root) == 0) {
        alloc = comdb2ma_create_int(base, sz, cap, name, scope, lock, fn, arg,
                                    file, func, line);
        COMDB2MA_UNLOCK(&root);
    }
    return alloc;
}

int comdb2ma_destroy(comdb2ma cm)
{
    int rc;

    rc = COMDB2MA_LOCK(&root);
    if (rc != 0)
        return rc;

    rc = comdb2ma_destroy_int(cm);

    if (rc != 0)
        COMDB2MA_UNLOCK(&root);
    else
        rc = COMDB2MA_UNLOCK(&root);
    return rc;
}

int comdb2ma_attach(comdb2ma parent, comdb2ma child)
{
    int rc;

    rc = COMDB2MA_LOCK(child);
    if (rc == 0) {
        child->parent = parent;
        rc = COMDB2MA_UNLOCK(child);
        if (rc == 0) {
            rc = COMDB2MA_LOCK(parent);
            if (rc == 0) {
                listc_abl(&(parent->children), child);
                rc = COMDB2MA_UNLOCK(parent);
            }
        }
    }

    return rc;
}

/*
 * |<---- (2 * wordsize)-bit overhead ----->|<-------------- payload -------------->|
 * +--------------------------------------------------------------------------------+
 * |      sentinel      |     allocator     |          `size'-byte payload...       |
 * +--------------------------------------------------------------------------------+
 */
void *comdb2_malloc(comdb2ma cm, size_t size)
{
    void **out = NULL;

    if (size > COMDB2MA_MAX_MEM) {
        // force failure if integer overflow
        errno = ENOMEM;
    } else if (COMDB2MA_LOCK(cm) == 0) {
        if (!COMDB2MA_FULL(cm))
            out = mspace_malloc(cm->m, size + COMDB2MA_OVERHEAD);

        if (out != NULL) {
#ifdef PER_THREAD_MALLOC
            ++cm->refs;
#endif
            out[0] = COMDB2MA_SENTINEL(out, cm);
            out[1] = (void *)cm;
            out -= COMDB2MA_SENTINEL_OFS;
        }

        COMDB2MA_UNLOCK(cm);
    }

    return (void *)out;
}

void *comdb2_malloc0(comdb2ma cm, size_t size)
{
    void *out = comdb2_malloc(cm, size);
    if (out != NULL)
        memset(out, 0, size);

    return out;
}

void *comdb2_calloc(comdb2ma cm, size_t n, size_t size)
{
    void **out = NULL;
    size_t nb;

    if (n && size && COMDB2MA_MAX_MEM / n < size) {
        // force failure if integer overflow
        errno = ENOMEM;
    } else if (COMDB2MA_LOCK(cm) == 0) {
        nb = n * size;
        if (!COMDB2MA_FULL(cm))
            out = mspace_calloc(cm->m, 1, nb + COMDB2MA_OVERHEAD);

        if (out != NULL) {
#ifdef PER_THREAD_MALLOC
            ++cm->refs;
#endif
            out[0] = COMDB2MA_SENTINEL(out, cm);
            out[1] = (void *)cm;
            out -= COMDB2MA_SENTINEL_OFS;
        }
        COMDB2MA_UNLOCK(cm);
    }

    return (void *)out;
}

static void *comdb2_realloc_int(comdb2ma cm, void *ptr, size_t n)
{
    void **out = (void **)ptr;

    if (COMDB2MA_LOCK(cm) != 0)
        out = NULL;
    else {
        /* if no cap threshold, don't even bother to check usable size of the
         * chunk. */
        if (COMDB2MA_HAS_CAP(cm) && (n > comdb2_malloc_usable_size(out)) &&
            COMDB2MA_CAP(cm)) {
            errno = ENOMEM;
            out = NULL;
        } else {
            out = mspace_realloc(cm->m, (void *)(out + COMDB2MA_SENTINEL_OFS),
                                 n + COMDB2MA_OVERHEAD);
            if (out != NULL) {
                /* Recompute sentinel for realloc() b/c addr may be changed. */
                out[0] = COMDB2MA_SENTINEL(out, cm);
                out -= COMDB2MA_SENTINEL_OFS;
            }
        }
        COMDB2MA_UNLOCK(cm);
    }

    return (void *)out;
}

void *comdb2_realloc(comdb2ma cm, void *ptr, size_t n)
{
    void **out = NULL;

    if (ptr == NULL) {
        out = comdb2_malloc(cm, n);
    } else if (n == 0) {
        comdb2_free(ptr);
    } else {
        out = (void **)ptr;
        if (!COMDB2MA_OK_SENTINEL(out)) {
            /* sentinel does not match. ptr could be allocated by system call.
               hand it over to system realloc. */
            out = realloc(ptr, n);
        } else {
            cm = (comdb2ma)out[COMDB2MA_ALLOC_OFS];

            if (cm->bm == NULL)
                out = comdb2_realloc_int(cm, ptr, n);
            else
                out = comdb2_brealloc(cm->bm, ptr, n);
        }
    }
    return (void *)out;
}

/* This function is a clone of comdb2_realloc except it invokes mspace_resize(). */
void *comdb2_resize(comdb2ma cm, void *ptr, size_t n)
{
    void **out = NULL;

    if (ptr == NULL) {
        out = comdb2_malloc(cm, n);
    } else if (n == 0) {
        comdb2_free(ptr);
    } else {
        out = (void **)ptr;
        if (!COMDB2MA_OK_SENTINEL(out)) {
            /* sentinel does not match. ptr could be allocated by system call.
               hand it over to system realloc. */
            out = realloc(ptr, n);
        } else {
            cm = (comdb2ma)out[COMDB2MA_ALLOC_OFS];
            if (COMDB2MA_LOCK(cm) != 0)
                out = NULL;
            else {
                if (COMDB2MA_HAS_CAP(cm) && (n > comdb2_malloc_usable_size(out)) &&
                    COMDB2MA_CAP(cm)) {
                    errno = ENOMEM;
                    out = NULL;
                } else {
                    out = mspace_resize(cm->m, (void *)(out + COMDB2MA_SENTINEL_OFS),
                                        n + COMDB2MA_OVERHEAD);
                    if (out != NULL) {
                        out[0] = COMDB2MA_SENTINEL(out, cm);
                        out[1] = (void *)cm;
                        out -= COMDB2MA_SENTINEL_OFS;
                    }
                }
                COMDB2MA_UNLOCK(cm);
            }
        }
    }
    return (void *)out;
}

static void comdb2_free_int(comdb2ma cm, void *ptr)
{
    void **p = (void **)ptr;

    if (COMDB2MA_LOCK(cm) == 0) {
        mspace_free(cm->m, p + COMDB2MA_SENTINEL_OFS);
#ifdef PER_THREAD_MALLOC
        --cm->refs;

        /*
         * We must use (cm->nthds == 0) instead of (cm->nthds == 1) because
         * we can't possibly guarantee that the `1` here is the thread which
         * has allocated `ptr`. Consider the following scenario:
         * 1) A gets the mspace, allocates `ptr`. We have
         *    (cm->refs == 1 && cm->nthds == 1);
         * 2) B gets the mspace. We have
         *    (cm->refs == 1 && cm->nthds == 2);
         * 3) A hands `ptr` over to B, then exits. We have
         *    (cm->refs == 1 && cm->nthds == 1);
         * 4) B frees `ptr`. We have
         *    (cm->refs == 0 && cm->nthds == 1);
         * 5) B allocates memory.
         * Step 5) would crash if we destoryed the mspace between 4) and 5).
         * Instead, the mspace will be safely destroyed in destroy_zone() when
         * thread B exits.
         */

        if (cm->refs == 0 && cm->nthds == 0) {
            COMDB2MA_UNLOCK(cm);              // unlock cm, start over [1]
            if (COMDB2MA_LOCK(&root) == 0) {  // lock root [2]
                if (COMDB2MA_LOCK(cm) == 0) { // lock cm  [3]
                    if (cm->refs == 0 && cm->nthds == 0) { // double check [4]
                        /* (cm->onfreelist > 0) is implied. */
                        listc_rfl(&root.freelist[cm->onfreelist], cm);
                        COMDB2MA_UNLOCK(cm);
                        comdb2ma_destroy_int(cm);
                    } else { // cm has been claimed by another thread between
                             // [1] and [2]
                        COMDB2MA_UNLOCK(cm);
                    }
                }
                COMDB2MA_UNLOCK(&root);
            }
        } else
#endif
            COMDB2MA_UNLOCK(cm);
    } else {
        logmsg(LOGMSG_ERROR, "%s:%d failed to acquire allocator lock\n", __func__,
                __LINE__);
    }
}

void comdb2_free(void *ptr)
{
    comdb2ma cm;
    void **p = (void **)ptr;

    if (p != NULL) {
        if (!COMDB2MA_OK_SENTINEL(p)) {
            /* sentinel corruption. possible reasons-
               1) memory corruption. this will fail regardless which free we
               call.
               2) ptr was not malloc'd by me. hand it over to system free. */
            free(ptr);
        } else {
            cm = (comdb2ma)p[COMDB2MA_ALLOC_OFS];

            if (cm->bm == NULL)
                comdb2_free_int(cm, ptr);
            else
                comdb2_bfree(cm->bm, ptr);
        }
    }
}

char *comdb2_strdup(comdb2ma cm, const char *s)
{
    size_t len = strlen(s) + 1;
    char *copy;

    if ((copy = comdb2_malloc(cm, len)) != NULL)
        memcpy(copy, s, len);
    return copy;
}

char *comdb2_strndup(comdb2ma cm, const char *s, size_t n)
{
    /*
    ** Don't want to call strlen, Input may not be null terminated.
    ** Don't have portable strnlen.
    ** Don't want to write strnlen.
    */
    char *copy = comdb2_malloc(cm, n + 1);
    if (copy == NULL)
        return NULL;
    strncpy(copy, s, n);
    copy[n] = '\0';
    return copy;
}

struct mallinfo comdb2_mallinfo(comdb2ma cm)
{
    struct mallinfo ret = {0};
    if (COMDB2MA_LOCK(cm) == 0) {
        ret = COMDB2MA_MALLINFO_SAFE(cm);
        COMDB2MA_UNLOCK(cm);
    }
    return ret;
}

int comdb2_malloc_stats(comdb2ma cm, int verbose, int hr, int toctrc)
{
    int rc;

    rc = COMDB2MA_LOCK(cm);
    if (rc == 0) {
        if (cm->print_stats_fn != NULL) {
            struct mallinfo info = COMDB2MA_MALLINFO_SAFE(cm);
            (*(cm->print_stats_fn))(&info, verbose, hr, cm->arg);
        } else {
            ma_stats_head(cm->len, strlen(cm->thr_type), COMDB2MA_GRP_NONE,
                          verbose, hr, 0, toctrc);
            ma_stats_dashes(ma_stats_int(cm->len, strlen(cm->thr_type),
                                         COMDB2MA_GRP_NONE, cm, verbose, hr,
                                         NULL, toctrc),
                            toctrc);
        }
        rc = COMDB2MA_UNLOCK(cm);
    }
    return rc;
}

int comdb2_malloc_trim(comdb2ma cm, size_t pad)
{
    int rc;
    rc = COMDB2MA_LOCK(cm);
    if (rc == 0) {
        rc = mspace_trim(cm->m, pad);
        COMDB2MA_UNLOCK(cm);
    }
    return rc;
}
// dynamic$

//^static mspaces
int comdb2ma_attach_static(int indx, comdb2ma child)
{
    STATIC_RANGE_CHECK(indx, EINVAL);
    return comdb2ma_attach(get_area(indx), child);
}

void *comdb2_malloc_static(int indx, size_t n)
{
    STATIC_RANGE_CHECK(indx, NULL);
    return comdb2_malloc(get_area(indx), n);
}

void *comdb2_malloc0_static(int indx, size_t n)
{
    STATIC_RANGE_CHECK(indx, NULL);
    return comdb2_malloc0(get_area(indx), n);
}

void *comdb2_calloc_static(int indx, size_t n, size_t size)
{
    STATIC_RANGE_CHECK(indx, NULL);
    return comdb2_calloc(get_area(indx), n, size);
}

void *comdb2_realloc_static(int indx, void *ptr, size_t n)
{
    STATIC_RANGE_CHECK(indx, NULL);
    return comdb2_realloc(get_area(indx), ptr, n);
}

void *comdb2_resize_static(int indx, void *ptr, size_t n)
{
    STATIC_RANGE_CHECK(indx, NULL);
    return comdb2_resize(get_area(indx), ptr, n);
}

char *comdb2_strdup_static(int indx, const char *s)
{
    STATIC_RANGE_CHECK(indx, NULL);
    return comdb2_strdup(get_area(indx), s);
}

char *comdb2_strndup_static(int indx, const char *s, size_t n)
{
    STATIC_RANGE_CHECK(indx, NULL);
    return comdb2_strndup(get_area(indx), s, n);
}

struct mallinfo comdb2_mallinfo_static(int indx)
{
    struct mallinfo empty = {0};
    STATIC_RANGE_CHECK(indx, empty);
    return comdb2_mallinfo(get_area(indx));
}

int comdb2_malloc_stats_static(int indx, int verbose, int hr, int toctrc)
{
    STATIC_RANGE_CHECK(indx, EINVAL);
    return comdb2_malloc_stats(get_area(indx), verbose, hr, toctrc);
}

int comdb2_malloc_trim_static(int indx, size_t pad)
{
    STATIC_RANGE_CHECK(indx, EINVAL);
    return comdb2_malloc_trim(get_area(indx), pad);
}
// static mspaces$

//^os
/*
 ** OS malloc, calloc, realloc free and strdup.
 */
void *os_malloc(size_t size) { return malloc(size); }

void *os_calloc(size_t n, size_t size) { return calloc(n, size); }

void *os_realloc(void *ptr, size_t size) { return realloc(ptr, size); }

void os_free(void *ptr) { free(ptr); }

char *os_strdup(const char *s) { return strdup(s); }
// os$

//^static functions
static comdb2ma comdb2ma_create_int(void *base, size_t init_sz, size_t max_cap,
                                    const char *name, const char *scope,
                                    int lock, stats_fn print_stats_fn,
                                    void *arg, const char *file,
                                    const char *func, int line)
{
    static const char *UNNAMED = "Anonymous";
    comdb2ma out;

    if (max_cap != COMDB2MA_UNLIMITED && max_cap < init_sz) {
        errno = EINVAL;
        return NULL;
    }

    if (name == NULL)
        name = UNNAMED;

    out = mspace_malloc(root.m, sizeof(struct comdb2mspace) + strlen(name));
    if (out == NULL)
        return NULL;

    /* create mspace */
    if (base == NULL)
        out->m = create_mspace(init_sz, 0);
    else
        out->m = create_mspace_with_base(base, init_sz, 0);

    if (out->m == NULL) {
        mspace_free(root.m, out);
        return NULL;
    }

    if (lock == COMDB2MA_MT_SAFE && pthread_mutex_init(&out->lock, NULL) != 0) {
        destroy_mspace(out->m);
        mspace_free(root.m, out);
        return NULL;
    }

    if (max_cap == COMDB2MA_UNLIMITED || max_cap > init_sz) {
        mspace_setmorecore(out->m, abortable_malloc);
        mspace_setmorecore0(out->m, abortable_calloc);
        mspace_setdestcore(out->m, free);
        mspace_setrecore(out->m, abortable_realloc);
    }

    out->parent = NULL;
    out->bm = NULL;
    out->use_lock = lock;
    out->init_sz = init_sz;
    out->cap = max_cap;
    out->print_stats_fn = print_stats_fn;
    out->arg = arg;
    out->file = file;
    out->func = func;
    out->line = line;

#ifdef PER_THREAD_MALLOC
    out->refs = 0;
    out->onfreelist = 0;
    out->nthds = 1;
    out->pid = pthread_self();

    if (scope != NULL)
        out->thr_type = scope;
    else if (pthread_equal(root.main_thr_id, pthread_self()))
        out->thr_type = "main";
    else if (thread_type_key != NULL)
        out->thr_type = thread_type_key;
    else
        out->thr_type = COMDB2MA_THR_UNKNOWN;
#else
    out->thr_type = "/";
#endif

    out->len = strlen(name);
    strcpy(out->name, name);

    listc_init(&(out->children), offsetof(struct comdb2mspace, sibling));
    listc_abl(&(root.list), out);

    return out;
}

static int comdb2ma_destroy_int(comdb2ma cm)
{
    int rc;
    comdb2ma curpos, tmppos;

    rc = COMDB2MA_LOCK(cm);
    if (rc != 0)
        return rc;

    listc_rfl(&(root.list), cm);

    if (cm->parent != NULL && COMDB2MA_LOCK(cm->parent) == 0) {
        /* cm has a parent. remove cm from its parent's child list */
        LISTC_FOR_EACH_SAFE(&(cm->parent->children), curpos, tmppos, lnk)
        {
            if (curpos == cm) {
                listc_rfl(&(cm->parent->children), curpos);
                break;
            }
        }
        COMDB2MA_UNLOCK(cm->parent);
    }

    destroy_mspace(cm->m);
    cm->m = NULL;
    rc = COMDB2MA_UNLOCK(cm);
    if (cm->use_lock)
        pthread_mutex_destroy(&cm->lock);
    mspace_free(root.m, cm);

    return rc;
}

#ifdef PER_THREAD_MALLOC
static comdb2ma get_area(int indx)
{
    comdb2ma *zone, pos;

    if (pthread_equal(root.main_thr_id, pthread_self()))
        return COMDB2_STATIC_MAS[indx];

#ifndef THREAD_DIAGNOSIS
    if (thread_type_key == NULL)
        return COMDB2_STATIC_MAS[indx];
#endif /* !THREAD_DIAGNOSIS */

    zone = (comdb2ma *)pthread_getspecific(root.zone);
    if (zone == NULL) {
        if (COMDB2MA_LOCK(&root) == 0) {
            zone = mspace_calloc(root.m, COMDB2MA_COUNT, sizeof(comdb2ma));
            COMDB2MA_UNLOCK(&root);
        }
        pthread_setspecific(root.zone, (void *)zone);
    }

    if (zone[indx] == NULL) {
        if (COMDB2MA_LOCK(&root) == 0) {
            zone[indx] = listc_rtl(&root.freelist[indx]);

            if (zone[indx] == NULL) { /* no free mspace */
                if (listc_size(&root.busylist[indx]) < root.mspace_max) {
                    /* We're allowed to create a new mspace. */
                    zone[indx] = comdb2ma_create_int(
                        NULL, COMDB2_STATIC_MA_METAS[indx].size << 10,
                        COMDB2_STATIC_MA_METAS[indx].cap << 10,
                        COMDB2_STATIC_MA_METAS[indx].name, NULL, 1, NULL, NULL,
                        __FILE__, __func__, __LINE__);
                    zone[indx]->onfreelist = indx;
                    listc_abl(&root.busylist[indx], zone[indx]);
                } else {
                    /* Reached the limit. Grab one from busylist. */
                    pos = root.pos[indx];
                    if (pos == NULL || pos->busylnk.next == NULL) {
                        /* reset pos to top if necessary */
                        root.pos[indx] = pos = root.busylist[indx].top;
                    } else {
                        /* advance pos to return the next mspace on busylist */
                        root.pos[indx] = pos = pos->busylnk.next;
                    }

                    if (COMDB2MA_LOCK(pos) == 0) {
                        ++(pos->nthds);
                        COMDB2MA_UNLOCK(pos);
                    }
                    zone[indx] = pos;
                }
            } else if (COMDB2MA_LOCK(zone[indx]) == 0) { /* got a free mspace */
                /* increment counter */
                ++(zone[indx]->nthds);
                /* FIXME this does not make much sense
                   if MALLOC_ARENA_MAX is present as multiple threads
                   will be sharing the mspace. */
                zone[indx]->pid = pthread_self();
                if (thread_type_key != NULL)
                    zone[indx]->thr_type = thread_type_key;
                else
                    zone[indx]->thr_type = COMDB2MA_THR_UNKNOWN;
                COMDB2MA_UNLOCK(zone[indx]);
                listc_abl(&root.busylist[indx], zone[indx]);
            }

            COMDB2MA_UNLOCK(&root);
        }
    }

    return zone[indx];
}

static void destroy_zone(void *arg)
{
    comdb2ma *zone;
    size_t i;
    static const char *onfreelist = "freelist";

    zone = (comdb2ma *)arg;
    if (COMDB2MA_LOCK(&root) == 0) {
        for (i = 0; i != COMDB2MA_COUNT; ++i)
            if (zone[i] != NULL && COMDB2MA_LOCK(zone[i]) == 0) {
                /* decrement counter */
                --(zone[i]->nthds);
                if (zone[i]->nthds != 0) {
                    /* still used by other threads. simply unlock */
                    COMDB2MA_UNLOCK(zone[i]);
                } else if (zone[i]->refs == 0) {
                    /* no references, no threads.
                       remove from busylist then destroy */
                    if (root.pos[i] == zone[i]) {
                        /* move `pos' backward. we would lose
                           the reference after rfl(), so do it now. */
                        root.pos[i] = zone[i]->busylnk.prev;
                    }
                    listc_rfl(&root.busylist[i], zone[i]);
                    COMDB2MA_UNLOCK(zone[i]);
                    comdb2ma_destroy_int(zone[i]);
                } else {
                    /* The mspace is being referenced, cannot be safely
                       destroyed. Trim it and then place it on freelist
                       for future allocation. */
                    zone[i]->thr_type = onfreelist;
                    mspace_trim(zone[i]->m, 0);

                    /* remove from busy list */
                    if (root.pos[i] == zone[i]) {
                        /* move `pos' backward. we would lose
                           the reference after rfl(), so do it now. */
                        root.pos[i] = zone[i]->busylnk.prev;
                    }
                    listc_rfl(&root.busylist[i], zone[i]);

                    /* atl and rtl to achieve MRU on freelist */
                    listc_atl(&root.freelist[i], zone[i]);
                    COMDB2MA_UNLOCK(zone[i]);
                }
            }

        mspace_free(root.m, arg);
        COMDB2MA_UNLOCK(&root);
    }
}
#endif

static char *mem_to_human_readable(size_t num, char buf[], int len)
{
    if (num >> 30) /* GB should be sufficient */
        snprintf(buf, len, "%.2f%c", (double)num / (1 << 30), 'G');
    else if (num >> 20)
        snprintf(buf, len, "%.2f%c", (double)num / (1 << 20), 'M');
    else if (num >> 10)
        snprintf(buf, len, "%.2f%c", (double)num / (1 << 10), 'K');
    else if (num != 0)
        snprintf(buf, len, "%d%c", (int)num, 'B');
    else
        snprintf(buf, len, "0");

    return buf;
}

static size_t ma_pair_search(pair_t *pairs, char *pattern,
                             comdb2ma_group_by groupby, struct mallinfo *total)
{
    comdb2ma curpos, temp;
    char *trim = NULL;
    size_t idx, sz = 0;

    if (pattern != NULL) {
        trim = pattern + strlen(pattern) - 1;
        for (; trim >= pattern; --trim)
            if (isspace(*trim))
                *trim = '\0';
    }

    LISTC_FOR_EACH(&(root.list), curpos, lnk)
    {

        if (pattern == NULL || wildcard_match(curpos->name, pattern) == 0 ||
            wildcard_match(curpos->thr_type, pattern) == 0) {
            if (COMDB2MA_LOCK(curpos) == 0) {
                if (groupby == COMDB2MA_GRP_NONE)
                    pairs[sz++].cm = curpos;
                else {
                    for (idx = 0; idx != sz; ++idx) {

                        if (groupby == COMDB2MA_GRP_NAME) {
                            if (strcasecmp(pairs[idx].cm->name, curpos->name) ==
                                0) {
                                ma_stats_aggregate(
                                    curpos,
                                    (struct mallinfo *)pairs[idx].cm->arg);
                                break;
                            }
                        } else if (groupby == COMDB2MA_GRP_SCOPE) {
                            if (strcasecmp(pairs[idx].cm->thr_type,
                                           curpos->thr_type) == 0) {
                                ma_stats_aggregate(
                                    curpos,
                                    (struct mallinfo *)pairs[idx].cm->arg);
                                break;
                            }
                        } else if (groupby == COMDB2MA_GRP_NAME_SCOPE) {
                            if (strcasecmp(pairs[idx].cm->name, curpos->name) ==
                                    0 &&
                                strcasecmp(pairs[idx].cm->thr_type,
                                           curpos->thr_type) == 0) {
                                ma_stats_aggregate(
                                    curpos,
                                    (struct mallinfo *)pairs[idx].cm->arg);
                                break;
                            }
                        }
#ifdef PER_THREAD_MALLOC
                        else if (groupby == COMDB2MA_GRP_THR) {
                            if (pairs[idx].cm->pid == curpos->pid) {
                                ma_stats_aggregate(
                                    curpos,
                                    (struct mallinfo *)pairs[idx].cm->arg);
                                break;
                            }
                        }
#endif
                    }

                    if (idx == sz) {
                        temp = mspace_calloc(root.m, 1,
                                             sizeof(struct comdb2mspace) +
                                                 curpos->len);
                        temp->thr_type = curpos->thr_type;
                        temp->len = curpos->len;
                        strcpy(temp->name, curpos->name);
                        temp->arg =
                            mspace_calloc(root.m, 1, sizeof(struct mallinfo));
                        temp->file = temp->func = "/";
#ifdef PER_THREAD_MALLOC
                        temp->pid = curpos->pid;
#endif
                        ma_stats_aggregate(curpos,
                                           (struct mallinfo *)temp->arg);
                        pairs[sz++].cm = temp;
                    }
                }
                ma_stats_aggregate(curpos, total);
                COMDB2MA_UNLOCK(curpos);
            }
        }
    }

    return sz;
}

static void ma_pair_sort(pair_t *pairs, size_t len, comdb2ma_order_by orderby)
{
    comdb2ma curpos;
    struct mallinfo *ma_info;
    size_t idx;

    for (idx = 0; idx != len; ++idx) {
        curpos = pairs[idx].cm;
        switch (orderby) {
        case COMDB2MA_NAME_ASC:
        case COMDB2MA_NAME_DESC:
            pairs[idx].key = (void *)curpos->name;
            break;

        case COMDB2MA_SCOPE_ASC:
        case COMDB2MA_SCOPE_DESC:
            pairs[idx].key = (void *)curpos->thr_type;
            break;

        case COMDB2MA_TOTAL_ASC:
        case COMDB2MA_TOTAL_DESC:
            if (curpos->m == NULL) {
                ma_info = (struct mallinfo *)curpos->arg;
                pairs[idx].key =
                    (void *)(uintptr_t)(ma_info->uordblks + ma_info->fordblks);
            } else if (COMDB2MA_LOCK(curpos) == 0) {
                pairs[idx].key = (void *)(uintptr_t)ma_footprint(curpos);
                COMDB2MA_UNLOCK(curpos);
            }
            break;

        case COMDB2MA_USED_ASC:
        case COMDB2MA_USED_DESC:
            if (curpos->m == NULL) {
                ma_info = (struct mallinfo *)curpos->arg;
                pairs[idx].key = (void *)(uintptr_t)(ma_info->uordblks);
            } else if (COMDB2MA_LOCK(curpos) == 0) {
                pairs[idx].key =
                    (void *)(uintptr_t)(ma_mallinfo(curpos).uordblks);
                COMDB2MA_UNLOCK(curpos);
            }
            break;

#ifdef PER_THREAD_MALLOC
        case COMDB2MA_THR_ASC:
        case COMDB2MA_THR_DESC:
            pairs[idx].key = (void *)curpos->pid;
            break;
#endif

        default:
            break;
        }
    }

    switch (orderby) {
    case COMDB2MA_NAME_ASC:
    case COMDB2MA_SCOPE_ASC:
        qsort(pairs, len, sizeof(pair_t), cmpr_string_asc);
        break;
    case COMDB2MA_NAME_DESC:
    case COMDB2MA_SCOPE_DESC:
        qsort(pairs, len, sizeof(pair_t), cmpr_string_desc);
        break;
    case COMDB2MA_TOTAL_ASC:
    case COMDB2MA_USED_ASC:
#ifdef PER_THREAD_MALLOC
    case COMDB2MA_THR_ASC:
#endif
        qsort(pairs, len, sizeof(pair_t), cmpr_number_asc);
        break;
    case COMDB2MA_TOTAL_DESC:
    case COMDB2MA_USED_DESC:
#ifdef PER_THREAD_MALLOC
    case COMDB2MA_THR_DESC:
#endif
        qsort(pairs, len, sizeof(pair_t), cmpr_number_desc);
        break;
    default:
        break;
    }
}

static void ma_pair_dump(pair_t *pairs, size_t len, int columns, int verbose,
                         int hr, struct mallinfo *total, int display_all,
                         int toctrc)
{
    size_t idx, np, maxnamesz, maxscopesz;
    comdb2ma curpos;

    if (len == 0)
        return;

    for (idx = 0, maxnamesz = 0, maxscopesz = 0; idx != len; ++idx) {
        if (pairs[idx].cm->len > maxnamesz)
            maxnamesz = pairs[idx].cm->len;

        if (strlen(pairs[idx].cm->thr_type) > maxscopesz)
            maxscopesz = strlen(pairs[idx].cm->thr_type);
    }

    np = ma_stats_head(maxnamesz, maxscopesz, columns, verbose, hr, 1, toctrc);
    for (idx = 0; idx != len; ++idx) {
        curpos = pairs[idx].cm;
        if (COMDB2MA_LOCK(curpos) == 0) {
            ma_stats_int(maxnamesz, maxscopesz, columns, curpos, verbose, hr,
                         total, toctrc);
            COMDB2MA_UNLOCK(curpos);
        }
    }

    ma_stats_dashes(np, toctrc);
    if (display_all) /* root's statistics */
        ma_stats_root(maxnamesz, maxscopesz, columns, verbose, hr, total,
                      toctrc);
    /* grand total */
    ma_stats_total(maxnamesz, maxscopesz, columns, verbose, hr, total, toctrc);
    ma_stats_dashes(np, toctrc);
}

static void ma_pair_clean(pair_t *pairs, size_t len)
{
    size_t idx;
    comdb2ma curpos;

    for (idx = 0; idx != len; ++idx) {
        curpos = pairs[idx].cm;
        if (curpos->m == NULL) {
            mspace_free(root.m, curpos->arg);
            mspace_free(root.m, curpos);
        }
    }
}

static struct mallinfo ma_mallinfo(const comdb2ma cm)
{
    comdb2ma curpos;
    struct mallinfo info, ret = COMDB2MA_MALLINFO_SAFE(cm);
    size_t freemem = ret.fordblks, usedmem = ret.uordblks;

    LISTC_FOR_EACH(&(cm->children), curpos, sibling)
    {
        if (COMDB2MA_LOCK(curpos) == 0) {
            info = COMDB2MA_MALLINFO_SAFE(curpos);
            ret.arena -= info.arena;
            ret.ordblks -= info.ordblks;
            ret.smblks -= info.smblks;
            ret.hblks -= info.hblks;
            ret.hblkhd -= info.hblkhd;
            ret.usmblks -= info.usmblks;
            ret.fsmblks -= info.fsmblks;
            ret.uordblks -= curpos->init_sz;
            ret.keepcost -= info.keepcost;
            COMDB2MA_UNLOCK(curpos);
        }
    }
    return ret;
}

static void ma_stats_aggregate(const comdb2ma cm, struct mallinfo *total)
{
    struct mallinfo info = ma_mallinfo(cm);
    total->arena += info.arena;
    total->ordblks += info.ordblks;
    total->smblks += info.smblks;
    total->hblks += info.hblks;
    total->hblkhd += info.hblkhd;
    total->usmblks += info.usmblks;
    total->fsmblks += info.fsmblks;
    total->uordblks += info.uordblks;
    total->fordblks += info.fordblks;
    total->keepcost += info.keepcost;
}

static void mspace_stats_aggregate(const mspace m, struct mallinfo *total)
{
    struct mallinfo info = mspace_mallinfo(m);
    total->arena += info.arena;
    total->ordblks += info.ordblks;
    total->smblks += info.smblks;
    total->hblks += info.hblks;
    total->hblkhd += info.hblkhd;
    total->usmblks += info.usmblks;
    total->fsmblks += info.fsmblks;
    total->uordblks += info.uordblks;
    total->fordblks += info.fordblks;
    total->keepcost += info.keepcost;
}

static size_t ma_footprint(const comdb2ma cm)
{
    comdb2ma curpos;
    size_t sz = mspace_footprint(cm->m);

    LISTC_FOR_EACH(&(cm->children), curpos, sibling)
    sz -= mspace_footprint(curpos->m);

    return sz;
}

static void ma_stats_total(size_t maxnamesz, size_t maxscopesz, int columns,
                           int verbose, int hr, const struct mallinfo *info,
                           int toctrc)
{
    size_t sum;

    char buf[256];
    size_t len = 256;
    int np = 0;

    trc_t trc;
    trc = toctrc ? pfx_ctrace : (trc_t)printf;
    int dolog = !toctrc;

    if (maxnamesz < COMDB2MA_MIN_NAME_SZ)
        maxnamesz = COMDB2MA_MIN_NAME_SZ;

    if (maxscopesz < COMDB2MA_MIN_SCOPE_SZ)
        maxscopesz = COMDB2MA_MIN_SCOPE_SZ;

    switch (columns) {
    case COMDB2MA_GRP_NAME:
        np += snprintf(buf + np, len - np, " %-*s |", (int) maxnamesz, "total");
        break;
    case COMDB2MA_GRP_SCOPE:
        np += snprintf(buf + np, len - np, " %-*s |", (int) maxscopesz, "total");
        break;
    default:
        np += snprintf(buf + np, len - np, " %-*s  %-*s |", (int) maxnamesz, "total",
                       (int) maxscopesz, "/");
        break;
    }

    if (verbose)
        np += snprintf(buf + np, len - np,
                       " %-3s  %-5s  %12s  %12s  %15s  %6s  %18s ", "/", "/",
                       "/", "/", "/", "/", "/");

    sum = info->uordblks + info->fordblks;
    if (hr)
        np += snprintf(buf + np, len - np, " %15s  %6d%%  %15s  %15s  %15s\n",
                       TB_TO_HR(sum), 100, TB_TO_HR(info->uordblks),
                       TB_TO_HR(info->fordblks), TB_TO_HR(info->usmblks));
    else
        np +=
            snprintf(buf + np, len - np, " %15zu  %6d%%  %15zu  %15zu  %15zu\n",
                     sum, 100, info->uordblks, info->fordblks, info->usmblks);

    if (dolog)
        logmsg(LOGMSG_USER, "%.*s", np, buf);
    else
        trc("%.*s", np, buf);
}

static void ma_stats_root(size_t maxnamesz, size_t maxscopesz, int columns,
                          int verbose, int hr, const struct mallinfo *total,
                          int toctrc)
{
    size_t sum;
    struct mallinfo info = mspace_mallinfo(root.m);

    char buf[256];
    size_t len = 256;
    int np = 0;

    trc_t trc;
    trc = toctrc ? pfx_ctrace : (trc_t)printf;
    int dolog = !toctrc;

    if (maxnamesz < COMDB2MA_MIN_NAME_SZ)
        maxnamesz = COMDB2MA_MIN_NAME_SZ;

    if (maxscopesz < COMDB2MA_MIN_SCOPE_SZ)
        maxscopesz = COMDB2MA_MIN_SCOPE_SZ;

    switch (columns) {
    case COMDB2MA_GRP_NAME:
        np += snprintf(buf + np, len - np, " %-*s |", (int) maxnamesz,
                       COMDB2MA_ROOT_NAME);
        break;
    case COMDB2MA_GRP_SCOPE:
        np += snprintf(buf + np, len - np, " %-*s |", (int) maxscopesz,
                       COMDB2MA_ROOT_NAME);
        break;
    default:
        np += snprintf(buf + np, len - np, " %-*s  %-*s |", (int) maxnamesz,
                       COMDB2MA_ROOT_NAME, (int) maxscopesz, "/");
        break;
    }

    if (verbose)
        np += snprintf(buf + np, len - np,
                       " %-3s  %-5s  %12s  %12s  %15s  %6s  %18s ", "/", "/",
                       "/", "/", "/", "/", "/");

    sum = info.uordblks + info.fordblks;
    if (hr) {
        np += snprintf(buf + np, len - np, " %15s ", TB_TO_HR(sum));
        if (total != NULL)
            np += snprintf(buf + np, len - np, " %6.2f%% ",
                           100.0f * sum /
                               (double)(total->uordblks + total->fordblks));
        np += snprintf(buf + np, len - np, " %15s  %15s  %15s\n",
                       TB_TO_HR(info.uordblks), TB_TO_HR(info.fordblks),
                       TB_TO_HR(info.usmblks));
    } else {
        np += snprintf(buf + np, len - np, " %15zu ", sum);
        if (total != NULL)
            np += snprintf(buf + np, len - np, " %6.2f%% ",
                           100.0f * sum / (total->uordblks + total->fordblks));
        np += snprintf(buf + np, len - np, " %15zu  %15zu  %15zu\n",
                       info.uordblks, info.fordblks, info.usmblks);
    }

    if (dolog)
        logmsg(LOGMSG_USER, "%.*s", np, buf);
    else
        trc("%.*s", np, buf);
}

static void ma_stats_dashes(int length, int toctrc)
{
    trc_t trc;
    int dolog = !toctrc;

    if (length <= 0)
        return;

    trc = toctrc ? pfx_ctrace : (trc_t)printf;

    char dashes[length];
    memset(dashes, '-', length);

    if (dolog)
        logmsg(LOGMSG_USER, "%.*s\n", length, dashes);
    else
        trc("%.*s\n", length, dashes);
}

static int ma_stats_head(size_t maxnamesz, size_t maxscopesz, int columns,
                         int verbose, int hr, int percentage, int toctrc)
{
    char head[256];
    size_t len = 256, ofs = 0;

    trc_t trc;
    trc = toctrc ? pfx_ctrace : (trc_t)printf;
    int dolog = !toctrc;

    if (maxnamesz < COMDB2MA_MIN_NAME_SZ)
        maxnamesz = COMDB2MA_MIN_NAME_SZ;

    if (maxscopesz < COMDB2MA_MIN_SCOPE_SZ)
        maxscopesz = COMDB2MA_MIN_SCOPE_SZ;

    switch (columns) {
    case COMDB2MA_GRP_NAME:
        ofs +=
            snprintf(head + ofs, len - ofs, " %-*s |", (int)maxnamesz, "name");
        break;
    case COMDB2MA_GRP_SCOPE:
        ofs += snprintf(head + ofs, len - ofs, " %-*s |", (int)maxscopesz,
                        "scope");
        break;
    default:
        ofs += snprintf(head + ofs, len - ofs, " %-*s  %-*s |", (int)maxnamesz,
                        "name", (int)maxscopesz, "scope");
        break;
    }

    if (verbose)
        ofs += snprintf(head + ofs, len - ofs,
                        " %-3s  %-5s  %12s  %12s  %15s  %6s  %18s ", "MT",
                        "block", "init", "cap", "file", "line", "thread");

    ofs += snprintf(head + ofs, len - ofs, " %15s ", "total");
    if (percentage)
        ofs += snprintf(head + ofs, len - ofs, " %7s ", "percent");
    ofs += snprintf(head + ofs, len - ofs, " %15s  %15s  %15s", "used", "free",
                    "peak");

    ma_stats_dashes(ofs, toctrc);
    if (dolog)
        logmsg(LOGMSG_USER, "%.*s\n", (int)ofs, head);
    else
        trc("%.*s\n", ofs, head);
    ma_stats_dashes(ofs, toctrc);
    return ofs;
}

static int ma_stats_int(size_t maxnamesz, size_t maxscopesz, int columns,
                        comdb2ma cm, int verbose, int hr,
                        const struct mallinfo *total, int toctrc)
{
    // sprintf() to a char buffer, and then printf() or ctrace() to a stream.
    // have to do it this way because ctrace() doesn't return the number
    // of characters printed.
    char buf[256];
    size_t len = 256;

    char fmt[64];
    size_t sum;
    int np = 0;
    size_t cap;

    trc_t trc;
    trc = toctrc ? pfx_ctrace : (trc_t)printf;
    int dolog = !toctrc;

    if (maxnamesz < COMDB2MA_MIN_NAME_SZ)
        maxnamesz = COMDB2MA_MIN_NAME_SZ;

    if (maxscopesz < COMDB2MA_MIN_SCOPE_SZ)
        maxscopesz = COMDB2MA_MIN_SCOPE_SZ;

    switch (columns) {
    case COMDB2MA_GRP_NAME:
        np += snprintf(buf + np, len - np, " %-*s |", (int) maxnamesz, cm->name);
        break;
    case COMDB2MA_GRP_SCOPE:
        np += snprintf(buf + np, len - np, " %-*s |", (int) maxscopesz, cm->thr_type);
        break;
    default:
        np += snprintf(buf + np, len - np, " %-*s  %-*s |", (int) maxnamesz, cm->name,
                       (int) maxscopesz, cm->thr_type);
        break;
    }

    /* construct format string */
    if (verbose) {
        cap = !cm->bm ? cm->cap : (cm->bm->cap == (size_t)-1 ? 0 : cm->bm->cap);
        fmt[0] = '\0';
        strcat(fmt, " %-3s "); /* MT-safe: YES/no */
        strcat(fmt, " %-5s "); /* Block?: YES/no */
        strcat(fmt, (cm->init_sz == COMDB2MA_DEFAULT_SIZE || hr) ? " %12s "
                                                                 : " %12u ");
        strcat(fmt, (cap == COMDB2MA_UNLIMITED || hr) ? " %12s " : " %12u ");
        strcat(fmt, (strlen(cm->file) > 15) ? " %12.12s... " : " %15.15s ");
        strcat(fmt, " %6d ");
#ifdef PER_THREAD_MALLOC
        strcat(fmt, " 0x%016llx ");
#endif
        np += snprintf(
            buf + np, len - np, fmt, (cm->use_lock || cm->bm) ? "YES" : "no",
            cm->bm ? "YES" : "no",
            (cm->init_sz == COMDB2MA_DEFAULT_SIZE)
                ? "default"
                : (hr ? TB_TO_HR(cm->init_sz) : (char *)cm->init_sz),
            (cap == COMDB2MA_UNLIMITED) ? "unlimited"
                                        : (hr ? TB_TO_HR(cap) : (char *)cap),
            cm->file, cm->line
#ifdef PER_THREAD_MALLOC
            ,
            cm->pid
#endif
            );
    }

    struct mallinfo info =
        cm->m == NULL ? *(struct mallinfo *)cm->arg : ma_mallinfo(cm);
    sum = info.uordblks + info.fordblks;
    if (hr) {
        np += snprintf(buf + np, len - np, " %15s ", TB_TO_HR(sum));
        if (total != NULL)
            np += snprintf(buf + np, len - np, " %6.2f%% ",
                           100.0f * sum /
                               (double)(total->uordblks + total->fordblks));
        np += snprintf(buf + np, len - np, " %15s  %15s  %15s\n",
                       TB_TO_HR(info.uordblks), TB_TO_HR(info.fordblks),
                       TB_TO_HR(info.usmblks));
    } else {
        np += snprintf(buf + np, len - np, " %15zu ", sum);
        if (total != NULL)
            np += snprintf(buf + np, len - np, " %6.2f%% ",
                           100.0f * sum / (total->uordblks + total->fordblks));
        np += snprintf(buf + np, len - np, " %15zu  %15zu  %15zu\n",
                       info.uordblks, info.fordblks, info.usmblks);
    }

    if (dolog)
        logmsg(LOGMSG_USER, "%.*s", np, buf);
    else
        trc("%.*s", np, buf);

    return np;
}

static int cmpr_string_asc(const void *a, const void *b)
{
    pair_t *pa = (pair_t *)a;
    pair_t *pb = (pair_t *)b;
    return strcasecmp((const char *)pa->key, (const char *)pb->key);
}

static int cmpr_string_desc(const void *a, const void *b)
{
    pair_t *pa = (pair_t *)a;
    pair_t *pb = (pair_t *)b;
    return strcasecmp((const char *)pb->key, (const char *)pa->key);
}

static int cmpr_number_asc(const void *a, const void *b)
{
    pair_t *pa = (pair_t *)a;
    pair_t *pb = (pair_t *)b;

    if (pa->key > pb->key)
        return 1;
    if (pa->key == pb->key)
        return 0;
    return -1;
}

static int cmpr_number_desc(const void *a, const void *b)
{
    pair_t *pa = (pair_t *)a;
    pair_t *pb = (pair_t *)b;

    if (pa->key > pb->key)
        return -1;
    if (pa->key == pb->key)
        return 0;
    return 1;
}

static int wildcard_match(const char *s, const char *p)
{
    const char *asterisk = NULL;
    const char *ts = s;
    while (*s) {
        if ((*p == '?') || (tolower(*p) == tolower(*s))) {
            ++s;
            ++p;
        } else if (*p == '*') {
            asterisk = p++;
            ts = s;
        } else if (asterisk) {
            p = asterisk + 1;
            s = ++ts;
        } else {
            return 1;
        }
    }
    for (; *p == '*'; ++p)
        ;
    return (*p == 0) ? 0 : 1;
}

static void *abortable_malloc(size_t sz)
{
    void *mem = malloc(sz);
    COMDB2MA_MEMCHK(mem, sz);
    return mem;
}

static void *abortable_realloc(void *ptr, size_t sz)
{
    void *mem = realloc(ptr, sz);
    COMDB2MA_MEMCHK(mem, sz);
    return mem;
}

static void *abortable_calloc(size_t n, size_t sz)
{
    void *mem = calloc(n, sz);
    COMDB2MA_MEMCHK(mem, sz);
    return mem;
}

static void pfx_ctrace(const char *format, ...)
{
    va_list ap;
    char pfxfmt[128];
    snprintf(pfxfmt, sizeof(pfxfmt), COMDB2MA_CRON_PFX "%s", format);
    va_start(ap, format);
    vctrace(pfxfmt, ap);
    va_end(ap);
}

// static functions$

/* COMDB2 BLOCKING MEMORY ALLOCATOR { */
static void privileged_init(void)
{
    if (pthread_key_create(&privileged, NULL) != 0) {
        fprintf(stderr, "failed creating privileged pthread key\n");
        abort();
    }
}

static void comdb2bma_thr_dest(void *bmakey)
{
    comdb2bma ma;
    ma = (comdb2bma)bmakey;
    if (ma != NULL && ma->prio && pthread_equal(pthread_self(), ma->prio_tid)) {
        if (pthread_mutex_lock(&ma->alloc->lock) == 0) {
            if (ma->prio && pthread_equal(pthread_self(), ma->prio_tid))
                ma->prio = 0;

            if (ma->nblocks != 0)
                pthread_cond_signal(&ma->cond);
            pthread_mutex_unlock(&ma->alloc->lock);
        }
    }
}

/* constructor and destructor { */
comdb2bma comdb2bma_create_trace(size_t init, size_t cap, const char *name,
                                 pthread_mutex_t *lock, const char *file,
                                 const char *func, int line)
{
    comdb2bma ret = NULL;
    struct rlimit lim;

    pthread_once(&privileged_once, privileged_init);

    if (COMDB2MA_LOCK(&root) == 0) {
        ret = mspace_malloc(root.m, sizeof(struct comdb2bmspace));
        if (ret != NULL) {
            // make an unlimited allocator because we need to
            // allocate out-of-bounds memory to avoid deadlocks

            if (lock != NULL) {
                ret->alloc = comdb2ma_create_int(NULL, init, 0, name, NULL, 0,
                                                 NULL, NULL, file, func, line);
                ret->external = 1;
            } else {
                ret->alloc = comdb2ma_create_int(NULL, init, 0, name, NULL, 1,
                                                 NULL, NULL, file, func, line);
                ret->external = 0;
            }

            if (ret->alloc == NULL) {
                mspace_free(root.m, ret);
                ret = NULL;
            } else if (pthread_cond_init(&ret->cond, NULL) != 0) {
                comdb2ma_destroy_int(ret->alloc);
                mspace_free(root.m, ret);
                ret = NULL;
            } else if (pthread_key_create(&ret->bmakey, comdb2bma_thr_dest) !=
                       0) {
                pthread_cond_destroy(&ret->cond);
                comdb2ma_destroy_int(ret->alloc);
                mspace_free(root.m, ret);
                ret = NULL;
            }

            if (ret != NULL) {
                // initialize fields
                ret->prio = 0;
                ret->lock = lock ? lock : (&ret->alloc->lock);
                ret->alloc->bm = ret;
                ret->alloc->use_lock = 0;
                ret->nblocks = 0;
                ret->inherited = 0;
                ret->giveback = 0;

                if (cap != 0)
                    ret->cap = cap;
                else if (getrlimit(RLIMIT_AS, &lim) ||
                         lim.rlim_max == RLIM_INFINITY)
                    ret->cap = (size_t)-1;
                else
                    ret->cap = (lim.rlim_max >> 2);

                listc_abl(&(root.blist), ret);
            }
        }
        COMDB2MA_UNLOCK(&root);
    }

    return ret;
}

int comdb2bma_destroy(comdb2bma ma)
{
    int rc;

    rc = COMDB2MA_LOCK(&root);
    if (rc != 0)
        return rc;

    rc = pthread_cond_destroy(&ma->cond);
    rc = pthread_key_delete(ma->bmakey);
    rc = comdb2ma_destroy_int(ma->alloc);

    mspace_free(root.m, ma);

    if (rc != 0)
        COMDB2MA_UNLOCK(&root);
    else
        rc = COMDB2MA_UNLOCK(&root);
    return rc;
}
/* } constructor and destructor */

/* helpers { */
/*
** lock/unlock macro
** #1 always lock an internal mutex
** #2 do not attempt to lock an external mutex if i am holding the lock
** #3 always unlock an internal mutex
** #4 do not attempt to unlock an external mutex if i am holding the lock
*/
#define COMDB2BMA_LOCK(ma)                                                     \
    ((!(ma)->external) ? pthread_mutex_lock((ma)->lock) : /* #1 */             \
         ((ma)->owned && pthread_equal((ma)->owner, pthread_self()) ? 0        \
                                                                    : /* #2 */ \
              pthread_mutex_lock((ma)->lock)))

#define COMDB2BMA_UNLOCK(bma)                                                  \
    ((!(ma)->external) ? pthread_mutex_unlock((ma)->lock) : /* #3 */           \
         ((ma)->owned && pthread_equal((ma)->owner, pthread_self()) ? 0        \
                                                                    : /* #4 */ \
              pthread_mutex_unlock((ma)->lock)))

int comdb2bma_pass_priority_back(comdb2bma ma)
{
    int rc;

    if (ma == NULL)
        return EINVAL;

    if (!ma->prio || !pthread_equal(pthread_self(), ma->prio_tid))
        return EPERM;

    if ((rc = COMDB2BMA_LOCK(ma)) == 0) {
        if (!ma->prio || !pthread_equal(pthread_self(), ma->prio_tid))
            rc = EPERM;
        else
            ma->giveback = 1;

        if (rc != 0)
            COMDB2BMA_UNLOCK(ma);
        else
            rc = COMDB2BMA_UNLOCK(ma);
    }
    return rc;
}

int comdb2bma_transfer_priority(comdb2bma ma, pthread_t tid)
{
    int rc;

    if (ma == NULL)
        return EINVAL;

    if (!ma->prio || !pthread_equal(pthread_self(), ma->prio_tid))
        return EPERM;

    if ((rc = COMDB2BMA_LOCK(ma)) == 0) {
        if (!ma->prio || !pthread_equal(pthread_self(), ma->prio_tid))
            rc = EPERM;
        else {
            /*
            ** don't override inherit_tid if I am already the heir.
            ** this way my heir can walk back to the beginning of the call chain
            *correctly.
            ** for example, appsock thread -> sql thread -> blockprocessor
            *thread-
            **              ^ |
            ** |____________walk_back_to_the_beginning_______________|
            */
            if (!ma->inherited) {
                ma->inherited = 1;
                ma->inherit_tid = ma->prio_tid;
            }
            ma->prio_tid = tid;

            if (!ma->giveback)
                pthread_setspecific(privileged, PRIO_NO);
        }

        if (rc != 0)
            COMDB2BMA_UNLOCK(ma);
        else {
            if (ma->nblocks != 0)
                pthread_cond_broadcast(&ma->cond);
            rc = COMDB2BMA_UNLOCK(ma);
        }
    }
    return rc;
}

int comdb2bma_yield(comdb2bma ma)
{
    int rc;

    if (ma == NULL)
        return EINVAL;

    if (!ma->prio || !pthread_equal(pthread_self(), ma->prio_tid))
        return EPERM;

    if ((rc = COMDB2BMA_LOCK(ma)) == 0) {
        if (!ma->prio || !pthread_equal(pthread_self(), ma->prio_tid)) {
            rc = EPERM;
        } else if (ma->inherited && ma->giveback) {
            /* if priority is inherited from my ancester
               and the ancestor wants it back, pass it over
               and then broadcast */
            ma->prio_tid = ma->inherit_tid;
            pthread_setspecific(privileged, PRIO_NO);
            if (ma->nblocks != 0)
                pthread_cond_broadcast(&ma->cond);
        } else {
            ma->prio = 0;
            pthread_setspecific(privileged, PRIO_NO);
            if (ma->nblocks != 0)
                pthread_cond_signal(&ma->cond);
        }

        if (rc == 0) {
            // reset on every successful yield
            ma->giveback = 0;
            ma->inherited = 0;
        }

        if (rc != 0)
            COMDB2BMA_UNLOCK(ma);
        else
            rc = COMDB2BMA_UNLOCK(ma);
    }
    return rc;
}

static void comdb2bma_check_prio_all(void)
{
    comdb2bma curpos;

    if (pthread_getspecific(privileged) != PRIO_UNKNOWN)
        return;

    if (COMDB2MA_LOCK(&root) == 0) {
        if (root.m != NULL) {
            pthread_setspecific(privileged, PRIO_NO);
            LISTC_FOR_EACH(&(root.blist), curpos, lnk)
            {
                // no need to lock. we're fine with dirty reads here.
                if (curpos->prio &&
                    pthread_equal(pthread_self(), curpos->prio_tid)) {
                    pthread_setspecific(privileged, PRIO_YES);
                    break;
                }
            }
        }
        COMDB2MA_UNLOCK(&root);
    }
}

int comdb2bma_yield_all(void)
{
    int rc;
    comdb2bma curpos;

    if ((rc = COMDB2MA_LOCK(&root)) == 0) {
        if (root.m == NULL)
            rc = EPERM;
        else {
            LISTC_FOR_EACH(&(root.blist), curpos, lnk)
            {
                (void)comdb2bma_yield(curpos);
            }
        }

        if (rc != 0)
            COMDB2MA_UNLOCK(&root);
        else
            rc = COMDB2MA_UNLOCK(&root);
    }

    return rc;
}

int comdb2bma_mark_locked(comdb2bma ma)
{
    if (ma == NULL)
        return EINVAL;

    if (!ma->external)
        return EPERM;

    ma->owner = pthread_self();
    ma->owned = 1;
    return 0;
}

int comdb2bma_mark_unlocked(comdb2bma ma)
{
    if (ma == NULL)
        return EINVAL;

    if (!ma->external)
        return EPERM;

    ma->owned = 0;
    return 0;
}

int comdb2bma_nblocks(comdb2bma ma)
{
    int nblocks = 0;
    if (ma == NULL || COMDB2BMA_LOCK(ma) != 0)
        return nblocks;

    nblocks = ma->nblocks;
    COMDB2BMA_UNLOCK(ma);

    return nblocks;
}

pthread_t comdb2bma_priotid(comdb2bma ma)
{
    pthread_t ret = pthread_self();
    if (ma != NULL && COMDB2BMA_LOCK(ma) == 0) {
        if (ma->prio)
            ret = ma->prio_tid;
        COMDB2BMA_UNLOCK(ma);
    }
    return ret;
}
/* } helpers */

/* static fn { */
static void *comdb2_bmalloc_int(comdb2bma ma, size_t n, int block, int lk,
                                unsigned int ms);
static void *comdb2_bcalloc_int(comdb2bma ma, size_t n, size_t size, int block,
                                int lk, unsigned int ms);
static void *comdb2_brealloc_int(comdb2bma ma, void *ptr, size_t n, int block,
                                 int lk, unsigned int ms);
static void comdb2_bfree_int(comdb2bma ma, void *ptr, int lk);
/* } static fn */

/* malloc { */
static void *comdb2_bmalloc_int(comdb2bma ma, size_t n, int block, int lk,
                                unsigned int ms)
{
    void *ret = NULL;
    int rc;
    struct timespec tm;

    /* this call will lock root allocator.  so do it before locking individual
       allocator
       to avoid possible deadlocks caused by opposite order of locking */
    comdb2bma_check_prio_all();

    if (lk && COMDB2BMA_LOCK(ma) != 0)
        return ret;

#ifdef PER_THREAD_MALLOC
    if (thread_type_key == NULL)
        ret = comdb2_malloc(ma->alloc, n);
    else
#endif

        if ((!ma->prio && ma->nblocks == 0) // don't cut in line
            || (ma->prio &&
                pthread_equal(pthread_self(), ma->prio_tid)) // privileged
            ||
            pthread_getspecific(privileged) ==
                PRIO_YES // privileged by other allocators
            ) {
        ret = comdb2_malloc(ma->alloc, n);
    } else if (block) {
        ++ma->nblocks;
        while (1) {
            clock_gettime(CLOCK_REALTIME, &tm);
            if (ms == 0)
                tm.tv_sec += 10;
            else {
                tm.tv_nsec += 1000000ULL * ms;
                while (tm.tv_nsec >= 1000000000) {
                    tm.tv_nsec -= 1000000000;
                    tm.tv_sec += 1;
                }
            }

            // zzzzzz...
            rc = pthread_cond_timedwait(&ma->cond, ma->lock, &tm);

            if (rc != 0 && rc != ETIMEDOUT) // ugh...
                break;

            if (mspace_footprint(ma->alloc->m) <= ma->cap || !ma->prio ||
                pthread_equal(pthread_self(), ma->prio_tid)) {
                ret = comdb2_malloc(ma->alloc, n);
                break;
            }

            if (ms != 0) {
                errno = ETIMEDOUT;
                break;
            }
        }
        --ma->nblocks;
    }

    if (ret != NULL && !ma->prio && mspace_footprint(ma->alloc->m) > ma->cap) {
        ma->prio_tid = pthread_self();
        ma->prio = 1;
        pthread_setspecific(privileged, PRIO_YES);
    }

    if (lk)
        COMDB2BMA_UNLOCK(ma);

    if (ret != NULL)
        pthread_setspecific(ma->bmakey, ma);

    return ret;
}

void *comdb2_bmalloc(comdb2bma ma, size_t n)
{
    return comdb2_bmalloc_int(ma, n, 1, 1, 0);
}

void *comdb2_trymalloc(comdb2bma ma, size_t n)
{
    return comdb2_bmalloc_int(ma, n, 0, 1, 0);
}

void *comdb2_timedmalloc(comdb2bma ma, size_t n, unsigned int ms)
{
    return comdb2_bmalloc_int(ma, n, 1, 1, ms);
}

void *comdb2_bmalloc_nl(comdb2bma ma, size_t n)
{
    return comdb2_bmalloc_int(ma, n, 1, 0, 0);
}

void *comdb2_trymalloc_nl(comdb2bma ma, size_t n)
{
    return comdb2_bmalloc_int(ma, n, 0, 0, 0);
}

void *comdb2_timedmalloc_nl(comdb2bma ma, size_t n, unsigned int ms)
{
    return comdb2_bmalloc_int(ma, n, 1, 0, ms);
}
/* } malloc */

/* calloc { */
static void *comdb2_bcalloc_int(comdb2bma ma, size_t n, size_t size, int block,
                                int lk, unsigned int ms)
{
    void *ret = NULL;
    int rc;
    struct timespec tm;

    comdb2bma_check_prio_all();

    if (lk && COMDB2BMA_LOCK(ma) != 0)
        return ret;

#ifdef PER_THREAD_MALLOC
    if (thread_type_key == NULL)
        ret = comdb2_calloc(ma->alloc, n, size);
    else
#endif

        if ((!ma->prio && ma->nblocks == 0) // don't cut in line
            || (ma->prio &&
                pthread_equal(pthread_self(), ma->prio_tid)) // privileged
            ||
            pthread_getspecific(privileged) ==
                PRIO_YES // privileged by other allocators
            ) {
        ret = comdb2_calloc(ma->alloc, n, size);
    } else if (block) {
        ++ma->nblocks;
        while (1) {
            clock_gettime(CLOCK_REALTIME, &tm);
            if (ms == 0)
                tm.tv_sec += 10;
            else {
                tm.tv_nsec += 1000000ULL * ms;
                while (tm.tv_nsec >= 1000000000) {
                    tm.tv_nsec -= 1000000000;
                    tm.tv_sec += 1;
                }
            }

            // zzzzzz...
            rc = pthread_cond_timedwait(&ma->cond, ma->lock, &tm);

            if (rc != 0 && rc != ETIMEDOUT) // ugh...
                break;

            if (mspace_footprint(ma->alloc->m) <= ma->cap || !ma->prio ||
                pthread_equal(pthread_self(), ma->prio_tid)) {
                ret = comdb2_calloc(ma->alloc, n, size);
                break;
            }

            if (ms != 0) {
                errno = ETIMEDOUT;
                break;
            }
        }
        --ma->nblocks;
    }

    if (ret != NULL && !ma->prio && mspace_footprint(ma->alloc->m) > ma->cap) {
        ma->prio_tid = pthread_self();
        ma->prio = 1;
        pthread_setspecific(privileged, PRIO_YES);
    }

    if (lk)
        COMDB2BMA_UNLOCK(ma);

    if (ret != NULL)
        pthread_setspecific(ma->bmakey, ma);

    return ret;
}

void *comdb2_bcalloc(comdb2bma ma, size_t n, size_t size)
{
    return comdb2_bcalloc_int(ma, n, size, 1, 1, 0);
}

void *comdb2_trycalloc(comdb2bma ma, size_t n, size_t size)
{
    return comdb2_bcalloc_int(ma, n, size, 0, 1, 0);
}

void *comdb2_timedcalloc(comdb2bma ma, size_t n, size_t size, unsigned int ms)
{
    return comdb2_bcalloc_int(ma, n, size, 1, 1, ms);
}

void *comdb2_bcalloc_nl(comdb2bma ma, size_t n, size_t size)
{
    return comdb2_bcalloc_int(ma, n, size, 1, 0, 0);
}

void *comdb2_trycalloc_nl(comdb2bma ma, size_t n, size_t size)
{
    return comdb2_bcalloc_int(ma, n, size, 0, 0, 0);
}

void *comdb2_timedcalloc_nl(comdb2bma ma, size_t n, size_t size,
                            unsigned int ms)
{
    return comdb2_bcalloc_int(ma, n, size, 1, 0, ms);
}
/* } calloc */

/* realloc { */
static void *comdb2_brealloc_int(comdb2bma ma, void *ptr, size_t n, int block,
                                 int lk, unsigned int ms)
{
    int rc;
    void *ret = NULL;
    struct timespec tm;

    if (ptr == NULL) // equivalent to malloc
        return comdb2_bmalloc_int(ma, n, block, lk, ms);

    if (n == 0) { // equivalent to free
        comdb2_bfree_int(ma, ptr, lk);
        return NULL;
    }

    comdb2bma_check_prio_all();

    if (lk && COMDB2BMA_LOCK(ma) != 0)
        return ret;

#ifdef PER_THREAD_MALLOC
    if (thread_type_key == NULL)
        ret = comdb2_realloc_int(ma->alloc, ptr, n);
    else
#endif
        // shrinking a chunk. let it proceed
        if (n <= comdb2_malloc_usable_size(ptr))
        ret = comdb2_realloc_int(ma->alloc, ptr, n);

    // extending a chunk
    else if ((!ma->prio && ma->nblocks == 0) // don't cut in line
             || (ma->prio &&
                 pthread_equal(pthread_self(), ma->prio_tid)) // privileged
             ||
             pthread_getspecific(privileged) ==
                 PRIO_YES // privileged by other allocators
             ) {
        ret = comdb2_realloc_int(ma->alloc, ptr, n);
    } else if (block) {
        ++ma->nblocks;
        while (1) {
            clock_gettime(CLOCK_REALTIME, &tm);
            if (ms == 0)
                tm.tv_sec += 10;
            else {
                tm.tv_nsec += 1000000ULL * ms;
                while (tm.tv_nsec >= 1000000000) {
                    tm.tv_nsec -= 1000000000;
                    tm.tv_sec += 1;
                }
            }

            // zzzzzz...
            rc = pthread_cond_timedwait(&ma->cond, ma->lock, &tm);

            if (rc != 0 && rc != ETIMEDOUT) // ugh...
                break;

            if (mspace_footprint(ma->alloc->m) <= ma->cap || !ma->prio ||
                pthread_equal(pthread_self(), ma->prio_tid)) {
                ret = comdb2_realloc_int(ma->alloc, ptr, n);
                break;
            }

            if (ms != 0) {
                errno = ETIMEDOUT;
                break;
            }
        }
        --ma->nblocks;
    }

    if (ret != NULL && !ma->prio && mspace_footprint(ma->alloc->m) > ma->cap) {
        ma->prio_tid = pthread_self();
        ma->prio = 1;
        pthread_setspecific(privileged, PRIO_YES);
    }

    if (lk)
        COMDB2BMA_UNLOCK(ma);

    if (ret != NULL)
        pthread_setspecific(ma->bmakey, ma);

    return ret;
}

void *comdb2_brealloc(comdb2bma ma, void *ptr, size_t n)
{
    return comdb2_brealloc_int(ma, ptr, n, 1, 1, 0);
}

void *comdb2_tryrealloc(comdb2bma ma, void *ptr, size_t n)
{
    return comdb2_brealloc_int(ma, ptr, n, 0, 1, 0);
}

void *comdb2_timedrealloc(comdb2bma ma, void *ptr, size_t n, unsigned int ms)
{
    return comdb2_brealloc_int(ma, ptr, n, 1, 1, ms);
}

void *comdb2_brealloc_nl(comdb2bma ma, void *ptr, size_t n)
{
    return comdb2_brealloc_int(ma, ptr, n, 1, 0, 0);
}

void *comdb2_tryrealloc_nl(comdb2bma ma, void *ptr, size_t n)
{
    return comdb2_brealloc_int(ma, ptr, n, 0, 0, 0);
}

void *comdb2_timedrealloc_nl(comdb2bma ma, void *ptr, size_t n, unsigned int ms)
{
    return comdb2_brealloc_int(ma, ptr, n, 1, 0, ms);
}
/* } realloc */

/* free { */
static void comdb2_bfree_int(comdb2bma ma, void *ptr, int lk)
{
    if (lk && COMDB2BMA_LOCK(ma) != 0)
        return;

    comdb2_free_int(ma->alloc, ptr);

    if (mspace_footprint(ma->alloc->m) > ma->cap)
        comdb2_malloc_trim(ma->alloc, 0);

    if (ma->nblocks != 0 && mspace_footprint(ma->alloc->m) <= ma->cap)
        pthread_cond_signal(&ma->cond);

    if (lk)
        COMDB2BMA_UNLOCK(ma->lock);
}

void comdb2_bfree(comdb2bma ma, void *ptr) { comdb2_bfree_int(ma, ptr, 1); }

void comdb2_bfree_nl(comdb2bma ma, void *ptr) { comdb2_bfree_int(ma, ptr, 0); }
/* } free */
/* } COMDB2 BLOCKING MEMORY ALLOCATOR */

/* COMDB2 GLOBAL BLOB MEMORY ALLOCATOR { */
/* blob allocator limit */
size_t gbl_blobmem_cap = -1;
/* blobs whose size are larger than the threshold are allocated by blob
 * allocator */
unsigned gbl_blob_sz_thresh_bytes = -1;
/* blob allocator */
comdb2bma blobmem;
/* } COMDB2 GLOBAL BLOB MEMORY ALLOCATOR */
