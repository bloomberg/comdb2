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

/* KEEPS A POOL OF THINGS FOR ME */

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/mman.h>
#include <string.h>

#include "pool.h"
#ifndef BUILDING_TOOLS
#include "mem_util.h"
#include "mem_override.h"
#endif
#include "logmsg.h"

struct pool {
    int entsz; /* entry size */

    struct freeblk {
        struct freeblk *next;
    } * freeb; /* free list- renamed to freeb so it doesn't get pre-processed to
                  comdb2_free */

    int npool; /* number in pool */
    int nused; /* number in use */

    int nblocks;
    struct blockhdr {
        struct blockhdr *prev;
        time_t time; /* just curious */
#ifndef BB_64BIT
        int zero[2]; /* keep on a 16 byte boundary */
#endif
    } * blocks;

    int stepsz; /* number of blocks to allocate each time */
    poolmalloc_t *malloc_fn;
    poolfree_t *free_fn;
    unsigned int memsz; /* overall mem usage */
    char *pool_name;
    time_t lastpr;
    int prstats;
    int verify_pool;
};

static void pool_stepup(pool_t *p)
{
    struct blockhdr *newblk;
    struct freeblk *ifree, *rfree, *newfree;
    int stepszm1, incsz;
    newblk = (struct blockhdr *)p->malloc_fn(sizeof(struct blockhdr) +
                                             p->stepsz * p->entsz);
    if (newblk == 0)
        return;

    /* RECORD MEMORY USAGE */
    p->memsz += (sizeof(struct blockhdr) + p->stepsz * p->entsz);

    /* SET UP BLOCK HEADER */
    newblk->time = time(0);
    newblk->zero[0] = 0;
    newblk->zero[1] = 0;

    /* LINK TO BLOCKS LIST */
    newblk->prev = p->blocks;
    p->blocks = newblk;
    p->nblocks++;

    /* INTIALIZE FREE LIST IN NEW BLOCK */
    newfree = (struct freeblk *)(newblk + 1); /* pnt just past block header */
    stepszm1 = p->stepsz - 1;
    incsz = p->entsz / sizeof(struct freeblk);

    ifree = newfree;
    rfree = ifree + stepszm1 * incsz;
    for (ifree = newfree; ifree < rfree; ifree += incsz) {
        ifree->next = ifree + incsz;
    }

    /* ATTACH NEW BLOCK TO FREE LIST */
    ifree->next = p->freeb;
    p->freeb = newfree;
    p->npool += p->stepsz;
    return;
}

void pool_setname(pool_t *p, char *name) { p->pool_name = name; }

void pool_stats(pool_t *p, int onoff) { p->prstats = onoff; }

static inline void pool_prstat(pool_t *p)
{
    time_t now;
    if (!p->prstats)
        return;
    if ((now = time(NULL)) > p->lastpr) {
        int npool, nused, nblocks;
        char *name, defname[17];
        if (p->pool_name) {
            name = p->pool_name;
        } else {
            snprintf(defname, sizeof(defname), "%p", p);
            name = defname;
        }
        pool_info(p, &npool, &nused, &nblocks);
        logmsg(LOGMSG_USER, "%s: %d pools, %d used, %d blocks\n", name, npool,
                nused, nblocks);
        p->lastpr = now;
    }
}

void pool_clear(pool_t *p)
{
    struct blockhdr *bb, *nbb;
    for (bb = p->blocks; bb; bb = nbb) {
        nbb = bb->prev;
        p->free_fn(bb);
        p->memsz -= (sizeof(struct blockhdr) + p->stepsz * p->entsz);
        p->nblocks--;
        p->npool -= p->stepsz;
    }
    p->freeb = 0;
    p->nused = 0;
    p->blocks = 0;
    if (p->nblocks != 0 || p->npool != 0) {
        logmsg(LOGMSG_ERROR, "emsg:pool_clear:%p:bad nblocks after clear %d\n", p,
                p->npool);
    }
    p->nblocks = 0;
    p->npool = 0;
}

void pool_relall(pool_t *p)
{
    struct blockhdr *bb, *nbb;

    p->freeb = NULL;
    p->npool = 0;
    p->nused = 0;
    for (bb = p->blocks; bb; bb = nbb) {
        struct blockhdr *newblk = (struct blockhdr *)bb;
        struct freeblk *ifree, *rfree, *newfree;
        int stepszm1, incsz;

        nbb = bb->prev;

        /* break the block, add pieces to free list */
        /* REINTIALIZE FREE LIST IN THE BLOCK */
        newfree =
            (struct freeblk *)(newblk + 1); /* pnt just past block header */
        stepszm1 = p->stepsz - 1;
        incsz = p->entsz / sizeof(struct freeblk);

        ifree = newfree;
        rfree = ifree + stepszm1 * incsz;
        for (ifree = newfree; ifree < rfree; ifree += incsz) {
            ifree->next = ifree + incsz;
        }

        /* ATTACH REINITIALIZED BLOCK TO FREE LIST */
        ifree->next = p->freeb;
        p->freeb = newfree;
        p->npool += p->stepsz;
    }
}

void pool_free(pool_t *p)
{
    /*If pool is not Null then first clear, then free pool pointer*/
    if (p != NULL) {
        pool_clear(p);
        p->free_fn(p);
        p = NULL;
    }
}

void pool_dump(pool_t *p, char *name)
{
    char *warn;
    int ii;
    struct freeblk *fre;
    struct blockhdr *blk;

    logmsg(LOGMSG_USER, "DUMP POOL %s (%p)\n", name, p);
    logmsg(LOGMSG_USER, "Entry size = %-10d    # ents in pool = %-10d\n", p->entsz,
           p->npool);
    logmsg(LOGMSG_USER, "    # used = %-10d            # free = %-10d\n", p->nused,
            p->npool - p->nused);
    logmsg(LOGMSG_USER, " Step size = %-10d          # blocks = %-10d\n", p->stepsz,
           p->nblocks);

    for (fre = p->freeb, ii = 0; fre != 0 && ii <= p->npool + 1;
         ii++, fre = fre->next)
        ;
    if (ii > p->npool || ii != (p->npool - p->nused))
        warn = "WARNING, BAD FREE COUNT!";
    else
        warn = "";
    logmsg(LOGMSG_USER, "  COUNT FREE=%d %s\n", ii, warn);

    for (blk = p->blocks, ii = 0; blk != 0 && ii <= p->nblocks + 1;
         ii++, blk = blk->prev)
        ;
    if (ii != p->nblocks)
        warn = "WARNING, BAD BLOCK COUNT!";
    else
        warn = "";
    logmsg(LOGMSG_USER, "COUNT BLOCKS=%d %s\n", ii, warn);

    for (blk = p->blocks, ii = 0; blk != 0 && ii <= p->nblocks;
         ii++, blk = blk->prev) {
        logmsg(LOGMSG_USER, "BLOCK %-10d (%p) TIME=%s", ii, blk,
               ctime((time_t *)&blk->time));
    }
    logmsg(LOGMSG_USER, "DUMP COMPLETE\n");
}

pool_t *pool_setalloc_verify_init(int entry_size, int stepsize,
                                  poolmalloc_t *poolmalloc,
                                  poolfree_t *poolfree, int verify)
{
    pool_t *p;
#ifdef NEWSI_DEBUG_POOL
    extern int gbl_verify_all_pools;
    verify = (verify + gbl_verify_all_pools);
#endif
    if (verify)
        entry_size += sizeof(pool_t *);
    entry_size = (entry_size + 3) & (-1 ^ 3);
    if (entry_size < 4)
        entry_size = 4;
    p = (pool_t *)poolmalloc(sizeof(pool_t));
    if (p == 0)
        return 0;
    p->entsz = entry_size;
    p->npool = 0;
    p->nused = 0;
    p->freeb = 0;

    p->nblocks = 0;
    p->blocks = 0;
    if (stepsize <= 0)
        stepsize = 512;
    p->stepsz = stepsize;
    p->malloc_fn = poolmalloc;
    p->free_fn = poolfree;
    p->memsz = sizeof(pool_t);
    p->pool_name = NULL;
    p->lastpr = 0;
    p->prstats = 0;
    p->verify_pool = verify;
    return p;
}

pool_t *pool_setalloc_init(int entry_size, int stepsize,
                           poolmalloc_t *poolmalloc, poolfree_t *poolfree)
{
    return pool_setalloc_verify_init(entry_size, stepsize, poolmalloc, poolfree,
                                     0);
}

pool_t *pool_init(int entry_size, int stepsize)
{
    return pool_setalloc_init(entry_size, stepsize, malloc, free);
}

pool_t *pool_verify_init(int entry_size, int stepsize)
{
    return pool_setalloc_verify_init(entry_size, stepsize, malloc, free, 1);
}

void *pool_getablk(pool_t *p)
{
    struct freeblk *got;
    if (p->freeb == 0) {
        pool_stepup(p);
        if (p->freeb == 0)
            return 0;
    }
    got = p->freeb;
    p->freeb = (p->freeb)->next;
    p->nused++;
    pool_prstat(p);
    if (p->verify_pool) {
        char *gp = (char *)got;
        pool_t **vp = (pool_t **)got;
        (*vp) = p;
        gp += sizeof(pool_t *);
        got = (struct freeblk *)gp;
    }
    return (void *)got;
}

void *pool_getzblk(pool_t *p)
{
    void *got = pool_getablk(p);
    if (got != 0) {
        int sz = p->verify_pool ? p->entsz - sizeof(pool_t *) : p->entsz;
        memset(got, 0, sz);
    }
    return got;
}

int pool_relablk(pool_t *p, void *vblk)
{
    struct freeblk *freeme;
    if (p->verify_pool) {
        char *gp = (char *)vblk;
        pool_t **vp;
        gp -= sizeof(pool_t *);
        vp = (pool_t **)gp;
        if (p != *vp)
            abort();
        vblk = (char *)gp;
    }
    freeme = (struct freeblk *)vblk;
    freeme->next = p->freeb;
    p->freeb = freeme;
    p->nused--;
    pool_prstat(p);
    return 0;
}

void pool_info(pool_t *p, int *npool, int *nused, int *nblocks)
{
    if (npool)
        *npool = p->npool;
    if (nused)
        *nused = p->nused;
    if (nblocks)
        *nblocks = p->nblocks;
}

void pool_info_memsz(pool_t *p, unsigned int *memsz)
{
    if (memsz)
        *memsz = p->memsz;
}

void pool_dumpx(pool_t *p, char *name)
{
    char *warn;
    int ii;
    struct freeblk *fre;
    struct blockhdr *blk;

    logmsg(LOGMSG_USER, "DUMP POOL %s (%p)\n", name, p);
    logmsg(LOGMSG_USER, "Entry size = %-10d    # ents in pool = %-10d\n", p->entsz,
            p->npool);
    logmsg(LOGMSG_USER, "    # used = %-10d            # free = %-10d\n", p->nused,
            p->npool - p->nused);
    logmsg(LOGMSG_USER, " Step size = %-10d          # blocks = %-10d\n", p->stepsz,
            p->nblocks);

    for (fre = p->freeb, ii = 0; fre != 0 && ii <= p->npool + 1;
         ii++, fre = fre->next)
        ;
    if (ii > p->npool || ii != (p->npool - p->nused))
        warn = "WARNING, BAD FREE COUNT!";
    else
        warn = "";
    logmsg(LOGMSG_USER, "  COUNT FREE=%d %s\n", ii, warn);

    for (blk = p->blocks, ii = 0; blk != 0 && ii <= p->nblocks + 1;
         ii++, blk = blk->prev)
        ;
    if (ii != p->nblocks)
        warn = "WARNING, BAD BLOCK COUNT!";
    else
        warn = "";
    logmsg(LOGMSG_USER, "COUNT BLOCKS=%d %s\n", ii, warn);
    logmsg(LOGMSG_USER, "TOTAL SIZE=%d\n", p->entsz * p->npool);
}

/* 041807dh: pool interface extension
   this creates a pool that uses full pages to allocate the blocks of objects
   the page_size specify the system page size
   the rest of parameters are the same as for pool_setalloc_init

   NOTE: it works only for entry_size for which (entry_size +  sizeof(struct
   blockhdr)) <= page_size
*/
pool_t *pool_setalloc_paged_init(int entry_size, poolmalloc_t *poolmalloc,
                                 poolfree_t *poolfree)
{
    logmsg(LOGMSG_DEBUG, "dummy pool_setalloc_paged_init\n");
    return NULL;
}

/*
   041907dh: set the pool protection mode; works correctly only for pool_t
   constructed
   using pool_setalloc_paged_init and which allocates aligned, multiple pages at
   a time in poolmalloc
 */
void pool_set_protect(pool_t *p, int flag)
{
    logmsg(LOGMSG_DEBUG, "dummy pool_set_protect\n");
}

#ifdef POOL_TEST_PROGRAM

void main()
{
    int ii, zz;
    pool_t p;
    int *pp[500000];
    pool_init(&p, 3 << 2, 5000);
    for (zz = 1; zz <= 5; zz++) {
        printf("%-10d GETTING...\n", zz);
        for (ii = 0; ii < 100000 * zz; ii++) {
            pp[ii] = (int *)pool_getablk(&p);
            if (pp[ii] == 0) {
                printf("FAILED TO GET A BLOCK!");
                exit(-1);
            }
            pp[ii][0] = ii;
            pp[ii][1] = -ii;
            pp[ii][2] = ii << 1;
            if ((ii & 0xffff) == 0)
                printf("GOT %d\n", ii);
        }
        printf("%-10d FREEING...\n", zz);
        for (ii = 0; ii < 100000 * zz; ii++) {
            if (pp[ii][0] != ii || pp[ii][1] != -ii || pp[ii][2] != (ii << 1)) {
                printf("ERR: MISMATCH %d: %d %d %d\n", ii, pp[ii][0], pp[ii][1],
                       pp[ii][2]);
                break;
            }
            pool_relablk(&p, pp[ii]);
            if ((ii & 0xffff) == 0)
                printf("FREED %d\n", ii);
        }
    }
    printf("DONE.\n");
    pool_dump(&p, "TEST");
    printf("CLEAR.\n");
    pool_clear(&p);
    pool_dump(&p, "CLEARED");
}

#endif
