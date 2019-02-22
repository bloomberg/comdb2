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

/* pool.h - efficient allocate/free for fixed sized blocks */

/* This module manages a pool of fixed sized blocks.  This will allocate
   a large chunk of memory and add/del from linked list.  This is proven
   to be much more efficient than malloc/free.  These routines need to
   be surround by lock for multithreaded programs.
   2/19/03
*/

#ifndef __INCLUDED_POOL_H__
#define __INCLUDED_POOL_H__

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

pool_t *pool_setalloc_verify_init(int entry_size, int stepsize,
                                  poolmalloc_t *poolmalloc,
                                  poolfree_t *poolfree, int verify);

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

/* DUMP INFO FOR DEBUGGING */
void pool_dump(pool_t *p, char *name);
void pool_dumpx(pool_t *p, char *name);

/* RETURN STATS */
void pool_info(pool_t *p, int *npool, int *nused, int *nblocks);

/* SET POOL NAME */
void pool_setname(pool_t *p, char *name);

/* STATS */
void pool_stats(pool_t *p, int onoff);

#ifdef __cplusplus
} /*extern "C"*/
#endif

#endif
