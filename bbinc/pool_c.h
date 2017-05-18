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

#ifndef INCLUDED_POOL_C
#define INCLUDED_POOL_C

/* pool.h - efficient allocate/free for fixed sized blocks */

/* This module manages a pool of fixed sized blocks.  This will allocate
   a large chunk of memory and add/del from linked list.  This is proven
   to be much more efficient than malloc/free.  These routines need to
   be surround by lock for multithreaded programs.
   2/19/03
*/

#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct pool pool_t;
typedef void *poolmalloc_t(size_t sz);
typedef void poolfree_t(void *ptr);

/* CREATE A POOL.  ENTRY_SIZE IS SIZE IN BYTES OF BLOCKS RETURNED.
   STEPSIZE IS HOW MANY ARE ALLOCATED INTERNALLY; 0 MEANS DEFAULT. */
pool_t *pool_init(int entry_size, int stepsize);

/* CREATE A POOL.  CALLER SETS THE MALLOC & FREE FUNCTIONS. */
pool_t *pool_setalloc_init(int entry_size, int stepsize,
                           poolmalloc_t *poolmalloc, poolfree_t *poolfree);

/* FREE THE POOL (RELEASE ALL RESOURCES INCLUDING POOL ITSELF) */
void pool_free(pool_t *p);

/* GET A BLOCK FROM POOL */
void *pool_getablk(pool_t *p);

/* GET A ZERO'ED BLOCK FROM POOL */
void *pool_getzblk(pool_t *p);

/* RELEASE BLOCK. */
int pool_relablk(pool_t *p, void *vblk);

/* RELEASE ALL BLKS (RECLAIM ALL BLOCKS) */
void pool_relall(pool_t *p);

/* CLEAR THE POOL. (RELEASE ALL BLOCKS) */
void pool_clear(pool_t *p);

/* DUMP INFO FOR DEBUGGING.  pool_dump() dumps the pool and all its blocks,
 * pool_dumpx() just dumps header info about the pool. */
void pool_dump(pool_t *p, const char *name);
void pool_dumpx(pool_t *p, const char *name);

/* RETURN STATS */
void pool_info(pool_t *p, int *npool, int *nused, int *nblocks);

/* OVERALL SIZE IN BYTES */
void pool_info_memsz(pool_t *p, unsigned int *memsz);

/* CREATE A POOL IN FULL MEMORY PAGES THAT COULD BE PROTECTED */
pool_t *pool_setalloc_paged_init(int entry_size, poolmalloc_t *poolmalloc,
                                 poolfree_t *poolfree);

/* PROTECT A POOL CREATED USING POOL_SETALLOC_PAGED_INIT */
void pool_set_protect(pool_t *p, int flag);

#ifdef __cplusplus
} /*extern "C"*/
#endif

#endif
