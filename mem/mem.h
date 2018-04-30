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
** Comdb2 trackable memory allocator.
*/
#ifndef INCLUDED_MEM_H
#define INCLUDED_MEM_H

#include <stddef.h>
#include <pthread.h>

#define COMDB2MA_UNLIMITED 0
#define COMDB2MA_DEFAULT_SIZE 0
#define COMDB2MA_MT_SAFE 1
#define COMDB2MA_MT_UNSAFE 0

/*
** Memory report sort.
*/
typedef enum {
    COMDB2MA_DEFAULT_ORDER,
    COMDB2MA_NAME_ASC,
    COMDB2MA_NAME_DESC,
    COMDB2MA_SCOPE_ASC,
    COMDB2MA_SCOPE_DESC,
    COMDB2MA_TOTAL_ASC,
    COMDB2MA_TOTAL_DESC,
    COMDB2MA_USED_ASC,
    COMDB2MA_USED_DESC
#ifdef PER_THREAD_MALLOC
    ,
    COMDB2MA_THR_ASC,
    COMDB2MA_THR_DESC
#endif
} comdb2ma_order_by;

/*
** Memory report group by.
** XXX Maybe a good idea to make it a systable?
*/
typedef enum {
    COMDB2MA_GRP_NONE,
    COMDB2MA_GRP_NAME,
    COMDB2MA_GRP_SCOPE,
    COMDB2MA_GRP_NAME_SCOPE
#ifdef PER_THREAD_MALLOC
    ,
    COMDB2MA_GRP_THR
#endif
} comdb2ma_group_by;

/*
** comdb2ma - COMDB2 M(emory) A(llocator)
*/
typedef struct comdb2mspace *comdb2ma;

/*
** mallinfo
**
** Different malloc implementations have different declarations of
** struct mallinfo.
** For example, on Linux, all fields are typed as int. On ibm, some
** of them are typed int and others are not. On SunOS, all fields
** are typed unsigned long. In order to get the most compatible
** version of struct mallinfo, the one defined in <dlmalloc.h> should
** always be #included.
*/
struct mallinfo;

/*
** caller-specific stats callback.
** mallinfo       - struct mallinfo of the allocator
** verbose        - verbosity
** human_readable - display in human readable format
** arg            - caller-specific argument
*/
typedef void (*stats_fn)(const struct mallinfo *mallinfo, int verbose,
                         int human_readable, void *arg);

#ifdef PER_THREAD_MALLOC
/*
** Thread type string.
*/
extern __thread const char *thread_type_key;
#endif

/*
** Initialize memory tracking module and static allocators.
**
** PARAMETERS
** sz  - initial size of static mspaces. if 0, default to page size
** cap - maximum capacity of static mspaces
**
** RETURN VALUE
** 0      - success
** EINVAL - maximum capacity is less than initial size
** EPERM  - module already initialized
** ENOMEM - failed to allocate memory for static allocators
** Others - mutex_(un)lock error
*/
int comdb2ma_init(size_t sz, size_t cap);

/*
** Destroy memory tracking module and ALL allocators.
**
** RETURN VALUE
** 0      - success
** EPERM  - module not initialized
** Others - mutex_(un)lock error
*/
int comdb2ma_exit(void);

/*
** Release free chunks of all allocators back to system.
**
** RETURN VALUE
** 0      - success
** Others - mutex_(un)lock error
*/
int comdb2ma_release(void);

/*
** Report statistics of all allocators.
**
** PARAMETERS
** pattern - CaSe-InSeNsItIvE wildcard matching.
**           ?: single character; *: 0 or more.
**           eg, pattern b*db will match both BdB and BeRkDb
** verbose - verbosity
** hr      - display in human readable format (eg, 1024B => 1K, 1024*1024B =>
*1M)
** orderby - display order
** groupby - group by
** toctrc   - redirect to ctrace() if 1
**
** RETURN VALUE
** 0      - success
** EPERM  - module not initialized
** ENOMEM - insufficient memory
** Others - mutex_(un)lock error
*/
int comdb2ma_stats(char *pattern, int verbose, int hr,
                   comdb2ma_order_by orderby, comdb2ma_group_by groupby,
                   int toctrc);

/*
** Change allocator niceness.
**
** PARAMETERS
** 0 - Expand the requested size to (the nearest power of 2 * pagesize).
** 1 - Expand the requested size to the next multiple of pagesize.
**
** RETURN VALUE
** 0      - success
** Others - error
*/
int comdb2ma_nice(int);

/*
** Get sys_alloc policy.
*/
int comdb2ma_niceness();

/*
** Return the value of mallopt(M_MMAP_THRESHOLD).
*/
int comdb2ma_mmap_threshold();

/*
** Return the mmap threshold set by comdb2ma_mmap_threshold().
**
** RETURN VALUE
** mmap threshold. Returns a negative value if MMAP has been
** completely disabled. Returns 0 if no user-defined mmap
** threshold.
*/
int comdb2ma_get_mmap_threshold(void);

/*
** Comdb2ma mallopt.
**
** PARAMETERS
** opt - option.
** val - value.
**
** RETURN VALUE
** 1      - success
** 0      - error
*/
int comdb2_mallopt(int opt, int val);

/*
** Returns the number of bytes you can actually use in an allocated chunk.
**
** PARAMETERS
** ptr - pointer
*/
size_t comdb2_malloc_usable_size(void *ptr);

#ifndef COMDB2MA_OMIT_DYNAMIC

/*
** Initialize a comdb2 memory allocator.
**
** PARAMETERS
** base    - initial base of the allocator
** init_sz - initial size. If it's less than 1 page size, pad to 1 page size
** cap     - maximum capacity. If max_cap < init_sz, return EINVAL
** name    - name of the allocator.
** scope   - scope of the allocator. if null, the thread type will be used as
*scope.
** lock    - make the allocator MT-safe
** fn      - statistics report callback
** arg     - arg is passed as the last argument of fn
** file    - __FILE__
** func    - __func__
** line    - __LINE__
**
** ERRORS
** ENOMEM - insufficient memory
** EINVAL - maximum capacity is less than initial size
** Others - mutex_(un)lock error
*/
comdb2ma comdb2ma_create_with_callback(void *base, size_t init_sz,
                                       size_t max_cap, const char *name,
                                       const char *scope, int lock, stats_fn fn,
                                       void *arg, const char *file,
                                       const char *func, int line);

/*
** Initialize an allocator less verbosely.
*/
#define comdb2ma_create(init_sz, max_cap, name, lock)                          \
    comdb2ma_create_with_callback(NULL, init_sz, max_cap, name, NULL, lock,    \
                                  NULL, NULL, __FILE__, __func__, __LINE__)

/*
** Initialize an allocator with a scope.
*/
#define comdb2ma_create_with_scope(init_sz, max_cap, name, scope, lock)        \
    comdb2ma_create_with_callback(NULL, init_sz, max_cap, name, scope, lock,   \
                                  NULL, NULL, __FILE__, __func__, __LINE__)

/*
** Initialize an allocator with a base less verbosely.
** Detroying this comdb2ma will _NOT_ deallocate the given base.
*/
#define comdb2ma_create_with_base(base, init_sz, max_cap, name, lock)          \
    comdb2ma_create_with_callback(base, init_sz, max_cap, name, NULL, lock,    \
                                  NULL, NULL, __FILE__, __func__, __LINE__)

/*
** Destroy a comdb2ma.
**
** PARAMETERS
** ma - comdb2ma.
*/
int comdb2ma_destroy(comdb2ma ma);

/*
** Attach a comdb2 allocator (probably created via comdb2ma_create_with_base) to
*its parent,
** so that memory report won't double-count the child's stats.
**
** PARAMETERS
** parent - parent allocator.
** child  - child allocator.
**
** RETURN VALUE
** 0      - success
** Others - mutex_(un)lock error
*/
int comdb2ma_attach(comdb2ma parent, comdb2ma child);

/*
** Comdb2ma malloc.
**
** PARAMETERS
** ma - memory allocator
** n  - size
**
** ERROR
** ENOMEM - insufficient memory
** Others - mutex_(un)lock error
*/
void *comdb2_malloc(comdb2ma ma, size_t n);

/*
** Comdb2ma zeroing out malloc.
**
** PARAMETERS
** ma - memory allocator
** n  - size
**
** ERROR
** ENOMEM - insufficient memory
** Others - mutex_(un)lock error
*/
void *comdb2_malloc0(comdb2ma ma, size_t n);

/*
** Comdb2ma calloc.
**
** PARAMETERS
** ma   - memory allocator
** n    - number of elements
** size - size of each element
**
** ERROR
** ENOMEM - insufficient memory
** Others - mutex_(un)lock error
*/
void *comdb2_calloc(comdb2ma ma, size_t n, size_t size);

/*
** Comdb2ma realloc.
** If ptr != NULL, ignore ma and make a realloc on the allocator where ptr was
*allocated.
** Otherwise, make a malloc on ma (equivalent to comdb2_malloc(ma, n)).
**
** PARAMETERS
** ma  - memory allocator
** ptr - pointer to be reallocated
** n   - size
**
** ERROR
** ENOMEM - insufficient memory
** Others - mutex_(un)lock error
*/
void *comdb2_realloc(comdb2ma ma, void *ptr, size_t n);

/*
** Resize a memory block.
** The function behaves as comdb2_realloc(), but it does not memcpy
** if it must reposition the memory block.
*/
void *comdb2_resize(comdb2ma cm, void *ptr, size_t n);

/*
** Comdb2ma free.
**
** comdb2_free doesn't need a comdb2ma parameter. The allocator address is kept
*in a small chunk
** ahead of `ptr'. The reason we add the overhead is that, allocation and
*deallocation may take
** place in different modules or threads.
**
** PARAMETERS
** ptr - pointer.
**
** ERROR
** Others - mutex_(un)lock error
*/
void comdb2_free(void *ptr);

/*
** Comdb2ma strdup.
**
** PARAMETERS
** ma  - memory allocator
** s   - source
**
** ERROR
** ENOMEM - insufficient memory
** Others - mutex_(un)lock error
*/
char *comdb2_strdup(comdb2ma ma, const char *s);

/*
** Comdb2ma strndup.
**
** PARAMETERS
** ma  - memory allocator
** s   - source
** n   - duplicate at most the first n bytes of s
**
** ERROR
** ENOMEM - insufficient memory
** Others - mutex_(un)lock error
*/
char *comdb2_strndup(comdb2ma ma, const char *s, size_t n);

/*
** Comdb2ma mallocinfo.
**
** PARAMETERS
** ma  - memory allocator
**
** ERROR
** Others - mutex_(un)lock error
*/
struct mallinfo comdb2_mallinfo(comdb2ma ma);

/*
** Report malloc statistics on the given allocator.
**
** PARAMETERS
** ma             - memory allocator
** verbose        - verbose
** human_readable - in human readable format
** toctrc   - redirect to ctrace() if 1
**
** RETURN VALUE
** 0      - success
** Others - mutex_(un)lock error
*/
int comdb2_malloc_stats(comdb2ma ma, int verbose, int human_readable,
                        int toctrc);

/*
** Release free memory from the top of the heap of ma.
**
** PARAMETERS
** ma  - memory allocator
** pad - padding size
**
** RETURN VALUE
** 1      - success
** 0      - impossible to release any memory
** Others - mutex_(un)lock error
*/
int comdb2_malloc_trim(comdb2ma ma, size_t pad);

#endif

#ifndef COMDB2MA_OMIT_STATIC

/*
** Attach a comdb2 allocator to a static allocator.
** The stats of a child will be deducted from its parent's on the final memory
*report.
** A comdb2 allocator will be automatically attached if its parent allocator
*information
** is found in the metadata chunk.
**
** PARAMETERS
** indx   - parent index.
** child  - child allocator.
*/
int comdb2ma_attach_static(int indx, comdb2ma child);

/*
** malloc on a static allocator
**
** PARAMETERS
** indx - allocator index
** n    - size
*/
void *comdb2_malloc_static(int indx, size_t n);

/*
** malloc0 on a static allocator
**
** PARAMETERS
** indx - allocator index
** n    - size
*/
void *comdb2_malloc0_static(int indx, size_t n);

/*
** calloc on a static allocator.
**
** PARAMETERS
** indx - allocator index
** n    - number of members
** size - size of each member
*/
void *comdb2_calloc_static(int indx, size_t n, size_t size);

/*
** realloc on a static allocator.
** If ptr != NULL, ignore indx and reallocate on the allocator where ptr was
*allocated.
** Otherwise, make a malloc on allocators[indx].
**
** PARAMETERS
** indx - allocator index
** ptr  - pointer
** n    - size
*/
void *comdb2_realloc_static(int indx, void *ptr, size_t n);

/*
** resize in a static allocator.
*/
void *comdb2_resize_static(int indx, void *ptr, size_t n);

/* Just an alias. */
#define comdb2_free_static comdb2_free

/*
** strdup on a static allocator.
*/
char *comdb2_strdup_static(int, const char *);

/*
** strndup on a static allocator.
*/
char *comdb2_strndup_static(int, const char *, size_t);

/*
** mallocinfo on a static allocator.
*/
struct mallinfo comdb2_mallinfo_static(int);

/*
** Report malloc statistics on the given static allocator.
*/
int comdb2_malloc_stats_static(int indx, int verbose, int human_readable,
                               int toctrc);

/*
** Release free memory from the top of the heap of a static memory allocator.
*/
int comdb2_malloc_trim_static(int indx, size_t pad);

#endif

/****************************************
**                                     **
** System malloc/calloc/realloc/free.  **
**                                     **
****************************************/

void *os_malloc(size_t size);
void *os_calloc(size_t n, size_t size);
void *os_realloc(void *ptr, size_t size);
void os_free(void *ptr);
char *os_strdup(const char *);

#ifndef COMDB2MA_OMIT_BMEM
/*
** comdb2bma - COMDB2 B(locking) M(emory) A(llocator)
**
** When there is no available memory in the allocator,
** a thread will be given the privilege to proceed with out-of-bounds memory.
*/
typedef struct comdb2bmspace *comdb2bma;

/* constructor/destructor */

/*
** If malloc/realloc/calloc are done while holding an external mutex,
** you can pass a pointer to the external mutex to avoid possible deadlocks
**
** if cap == 0, the actual limit would be a quarter of "ulimit -v"
*/
#define comdb2bma_create(init, cap, name, plock)                               \
    comdb2bma_create_trace(init, cap, name, plock, __FILE__, __func__, __LINE__)

comdb2bma comdb2bma_create_trace(size_t init, size_t cap, const char *name,
                                 pthread_mutex_t *lock, const char *file,
                                 const char *func, int line);

int comdb2bma_destroy(comdb2bma ma);
/* } constructor/destructor */

/* helpers { */
/* functions for resolving deadlocks externally */

/* tell the allocator that I will need to get priority back.
   must be called before comdb2bma_transfer_priority() */
int comdb2bma_pass_priority_back(comdb2bma ma);

/* transfer priority to another thread. if there is currently no priority thread
   or
   the calling thread is not priority thread, this function is no-op */
int comdb2bma_transfer_priority(comdb2bma ma, pthread_t tid);

/* yield. if there is currently no priority thread or
   the calling thread is not priority thread, this function is no-op */
int comdb2bma_yield(comdb2bma ma);

/* yield all allocators. */
int comdb2bma_yield_all(void);

/* mark an external mutex locked. allocation and deallocation after will not
   attempt to lock the external mutex. the calling thread must be holding the
   external mutex.
   if the allocator is created with an internal mutex, this function is no-op */
int comdb2bma_mark_locked(comdb2bma ma);

/* mark an external mutex unlocked. the calling thread must be holding the
   external mutex.
   if the allocator is created with an internal mutex, this function is no-op */
int comdb2bma_mark_unlocked(comdb2bma ma);

/* return the number of threads that are currently blocking on the allocator */
int comdb2bma_nblocks(comdb2bma ma);

/* return current priority thread id */
pthread_t comdb2bma_priotid(comdb2bma ma);
/* } helpers */

/*
** comdb2_{b}__     - block till memory is available
** comdb2_{try}__   - return NULL immediately if no mem
** comdb2_{timed}__ - return NULL immediately if no mem
** comdb2____{nl}   - don't lock (assuming the caller already holds the lock)
**
** if the pointer is comdb2_bmalloc'd or comdb2_bcalloc'd,
** comdb2_realloc() and comdb2_free() are equivalent to
** comdb2_brealloc() and comdb2_bfree() respectively.
*/

/* malloc { */
void *comdb2_bmalloc(comdb2bma ma, size_t n);
void *comdb2_trymalloc(comdb2bma ma, size_t n);
void *comdb2_timedmalloc(comdb2bma ma, size_t n, unsigned int ms);
void *comdb2_bmalloc_nl(comdb2bma ma, size_t n);
void *comdb2_trymalloc_nl(comdb2bma ma, size_t n);
void *comdb2_timedmalloc_nl(comdb2bma ma, size_t n, unsigned int ms);
/*  malloc */

/* calloc { */
void *comdb2_bcalloc(comdb2bma ma, size_t n, size_t size);
void *comdb2_trycalloc(comdb2bma ma, size_t n, size_t size);
void *comdb2_timedcalloc(comdb2bma ma, size_t n, size_t size, unsigned int ms);
void *comdb2_bcalloc_nl(comdb2bma ma, size_t n, size_t size);
void *comdb2_trycalloc_nl(comdb2bma ma, size_t n, size_t size);
void *comdb2_timedcalloc_nl(comdb2bma ma, size_t n, size_t size,
                            unsigned int ms);
/* } calloc */

/* realloc { */
void *comdb2_brealloc(comdb2bma ma, void *ptr, size_t n);
void *comdb2_tryrealloc(comdb2bma ma, void *ptr, size_t n);
void *comdb2_timedrealloc(comdb2bma ma, void *ptr, size_t n, unsigned int ms);
void *comdb2_brealloc_nl(comdb2bma ma, void *ptr, size_t n);
void *comdb2_tryrealloc_nl(comdb2bma ma, void *ptr, size_t n);
void *comdb2_timedrealloc_nl(comdb2bma ma, void *ptr, size_t n,
                             unsigned int ms);
/* } realloc */

/* free { */
void comdb2_bfree(comdb2bma ma, void *ptr);
void comdb2_bfree_nl(comdb2bma ma, void *ptr);
/* } free */

#endif /* COMDB2_OMIT_BMEM */
#endif /* INCLUDED_MEM_H */
