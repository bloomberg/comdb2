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
 * Genids
 *
 */

#include <pthread.h>

#include <arpa/inet.h>

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/socketvar.h>
#include <sys/uio.h>
#include <unistd.h>
#include <stddef.h>
#include <alloca.h>

#include <sys/poll.h>
#include <sys/select.h>
#include <sys/types.h>

#include <build/db.h>
#include <epochlib.h>
#include <ctrace.h>
#include <plbitlib.h>
#include <plhash.h>
#include <list.h>

#include <net.h>
#include "bdb_int.h"
#include "locks.h"
#include "flibc.h"

#ifndef MAXSTACKDEPTH
#define MAXSTACKDEPTH 200
#endif

#include "genid.h"
#include "logmsg.h"

int gbl_block_set_commit_genid_trace = 0;
static unsigned long long commit_genid;
static DB_LSN commit_lsn;
static uint32_t commit_generation;

unsigned long long get_lowest_genid_for_datafile(int stripe)
{
    unsigned long long g = stripe;
    g = (g << GENID_STRIPE_SHIFT) & GENID_STRIPE_MASK;
    return g;
}

int get_slotnum_from_genid(int modnum, unsigned long long genid)
{
    uint16_t slot_part;

    /* grab the 2 bytes containing slot part of the genid in net byte order */
    slot_part = (uint16_t)((genid & GENID_SLOT_MASK) >> GENID_SLOT_PRE_SHIFT);

    return (ntohs(slot_part) >> GENID_SLOT_POST_SHIFT) % modnum;
}

/* the search genid will have the updateid portion masked out */
unsigned long long get_search_genid(bdb_state_type *bdb_state,
                                    unsigned long long genid)
{
    int mask, bits, pidb = bdb_state->attr->participantid_bits;

    if (!bdb_state->ondisk_header)
        return genid;

    genid = flibc_ntohll(genid);
    bits = (12 - pidb);
    mask = (0x1 << bits) - 1;
    genid &= ~(mask << 4);

    return flibc_htonll(genid);
}

/* The rep stream needs to operate without a bdb_state */
unsigned long long get_search_genid_rowlocks(unsigned long long genid)
{
    int mask;

    genid = flibc_ntohll(genid);
    mask = (0x1 << 12) - 1;
    genid &= ~(mask << 4);

    return flibc_htonll(genid);
}

unsigned long long bdb_mask_updateid(bdb_state_type *bdb_state,
                                     unsigned long long genid)
{
    unsigned long long updateid;
    unsigned long long noupdateid;

    if (!bdb_state->ondisk_header && (genid & (~GENID_NORMALIZE_MASK))) {
        /*fprintf(stderr, "fixing genid 0x%llx genid\n", genid);*/

        /* no ondisk header, but updateid is set!  this means its an old style
           record and these are actually the rightmost bits of the previously
           larger dupcount area.  shift that area back into the new place to
           fix it */

        genid = flibc_ntohll(genid);

        /* mask out the old dupcount area, which includes the updateid area */
        noupdateid = genid & GENID_OLD_DUP_MASK;

        /* extract out the old update id, shift it over to the new position */
        updateid = genid & (~GENID_OLD_DUP_MASK);
        updateid = updateid << 12;

        /* or in the "new style" update id */
        genid = noupdateid | updateid;

        genid = flibc_htonll(genid);
    } else {
        /* Mask out the updateid field since it must be zero */
        genid = get_search_genid(bdb_state, genid);
    }

    /*fprintf(stderr, "genid masked to 0x%llx genid\n", genid);*/

    return genid;
}

/*

   To support live schema change, we need to guarantee that the
   genids will always be monotonically increasing.  So we reset the counter
   whenever the epoch time wraps.  This has the neat side effect that we know
   how many genids were allocated in a second just by inspecting the genids..

   To support ODH and the fast updates plan the new genid

   time/32 + dupecount/16 + (particip&upd)/12 + stripe/4

   the attribute variable, participantid_bits, specifies the number of bits
   allocated to the participant stripe id.  The remainder of those 12 bits
   are used for the updateid

*/

struct call {
    int ncalls;
    int nframes;
    intptr_t ip[1];
};

static u_int call_hashfunc(void *key, int len)
{
    struct call *c = (struct call *)key;
    int i;
    u_int h = 0;
    for (i = 0; i < c->nframes; i++)
        h ^= (u_int)c->ip[i];

    return h;
}

static int call_cmpfunc(void *key1, void *key2, int len)
{
    struct call *c1, *c2;
    int i;
    c1 = (struct call *)key1;
    c2 = (struct call *)key2;

    if (c1->nframes != c2->nframes)
        return -1;

    for (i = 0; i < c1->nframes; i++)
        if (c1->ip[i] != c2->ip[i])
            return -1;

    return 0;
}

static int dumpandclear(void *obj, void *arg)
{
    struct call *c;
    int i;
    c = (struct call *)obj;
    if (c->ncalls > 0) {
        printf("----------------\n");
        printf("%d: ", c->ncalls);
        for (i = 0; i < c->nframes; i++) {
            printf(" 0x%p", (void *)c->ip[i]);
        }
        printf("\n");
    }
    c->ncalls = 0;
    return 0;
}

unsigned long long get_genid_counter48(unsigned long long genid)
{
    uint32_t *iptr;
    uint16_t *sptr;
    unsigned long long counter;
    iptr = (uint32_t *)&genid;
    sptr = (uint16_t *)&genid;

#if defined(_LINUX_SOURCE)
    counter = ntohl(iptr[0]);
    counter <<= 16;
    counter |= ntohs(sptr[2]);
#else
    counter = genid >> 16;
#endif

    return counter;
}

int bdb_genid_timestamp(unsigned long long genid)
{
    int *iptr;

    iptr = (int *)&genid;

    /* genids (their components at least) are always "big-endian" */
    return ntohl(iptr[0]);
}

/* Fabricated (recno) genids are retrieved from sqlite3BtreeNewRowid-
   They will always have a timestamp value of 0 */
int bdb_genid_is_recno(bdb_state_type *bdb_state, unsigned long long genid)
{
    if (bdb_state->genid_format == LLMETA_GENID_ORIGINAL) {
        if(0 == bdb_genid_timestamp(genid))
        {
            return 1;
        }
        else
        {
            return 0;
        }
    }
    else if (bdb_state->genid_format == LLMETA_GENID_48BIT) {
        /* I don't think this is right - check. */
        return is_genid_add(genid) || is_genid_upd(genid);
    }
    else {
        logmsg(LOGMSG_FATAL, "Unknown genid format!\n");
        abort();
    }
}

/* this is for sqlite3BtreeNewRowid- I don't have to bother endianizing recno */
unsigned long long bdb_recno_to_genid(int recno)
{
    unsigned long long genid = 0;
    int *iptr = (int *)&genid;

    iptr[1] = recno;

    return genid;
}

static unsigned int get_dupecount_from_genid(unsigned long long genid)
{
    unsigned short *sptr;

    sptr = (unsigned short *)&genid;

    /* genids (their components at least) are always "big-endian" */
    return ntohs(sptr[2]);
}

static inline void set_gblcontext_int(bdb_state_type *bdb_state,
                                      unsigned long long gblcontext)
{
    if (gblcontext == -1ULL) {
        logmsg(LOGMSG_ERROR, "SETTING CONTEXT TO -1\n");
        cheap_stack_trace();
    }
    if (bdb_cmp_genids(gblcontext, bdb_state->gblcontext) < 0) {
        if (gbl_block_set_commit_genid_trace) {
            logmsg(LOGMSG_ERROR, "Blocked attempt to set lower gblcontext\n");
            cheap_stack_trace();
        }
    } else {
        bdb_state->gblcontext = gblcontext;
    }
}

/* setter/getters that grab a lock. */
void set_gblcontext(bdb_state_type *bdb_state, unsigned long long gblcontext)
{
    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    Pthread_mutex_lock(&(bdb_state->gblcontext_lock));

    set_gblcontext_int(bdb_state, gblcontext);

    Pthread_mutex_unlock(&(bdb_state->gblcontext_lock));
}

unsigned long long get_gblcontext(bdb_state_type *bdb_state)
{
    unsigned long long gblcontext;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    Pthread_mutex_lock(&(bdb_state->gblcontext_lock));
    gblcontext = bdb_state->gblcontext;
    Pthread_mutex_unlock(&(bdb_state->gblcontext_lock));

    return gblcontext;
}

unsigned long long get_id(bdb_state_type *bdb_state)
{
    unsigned int dupcount;
    unsigned long long id;
    unsigned int *iptr;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    iptr = (unsigned int *)&id;

    Pthread_mutex_lock(&(bdb_state->id_lock));
    bdb_state->id++;
    dupcount = bdb_state->id;
    Pthread_mutex_unlock(&(bdb_state->id_lock));

    iptr[0] = htonl(comdb2_time_epoch());
    iptr[1] = htonl(dupcount);

    return id;
}

int get_epoch_plusplus(bdb_state_type *bdb_state)
{
    static int x = 0;
    x++;
    return x;
}

int genid_contains_time(bdb_state_type *bdb_state)
{
    return bdb_state->genid_format == LLMETA_GENID_ORIGINAL;
}

static inline void set_commit_genid_lsn_gen(bdb_state_type *bdb_state,
                                            unsigned long long genid,
                                            const DB_LSN *lsn,
                                            const uint32_t *generation)
{
    if (bdb_state->parent)
        bdb_state = bdb_state->parent;
    if (bdb_cmp_genids(genid, commit_genid) < 0) {

        /* This can occur legitamately after a rep_verify_match */
        if (gbl_block_set_commit_genid_trace) {
            logmsg(LOGMSG_ERROR, "Blocked attempt to set lower commit_genid\n");
            cheap_stack_trace();
        }
        return;
    }
    commit_genid = genid;
    if (lsn) commit_lsn = *lsn;
    if (generation) commit_generation = *generation;

    set_gblcontext_int(bdb_state, genid);
}

static unsigned long long get_genid_48bit(bdb_state_type *bdb_state,
                                          unsigned int dtafile, DB_LSN *lsn,
                                          uint32_t generation, uint64_t seed)
{
    unsigned int *iptr;
    unsigned long long genid;
    unsigned long long seed48;
    static time_t lastwarn = 0;
    time_t now;
    uint32_t highorder = 0, loworder = 0;
    uint16_t *s48ptr;
    int prwarn = 0;

    Pthread_mutex_lock(&(bdb_state->gblcontext_lock));
    if (!seed)
        seed48 = get_genid_counter48(bdb_state->gblcontext);
    else
        seed48 = seed;

    while (seed48 >= 0x0000ffffffffffffULL) {
        /* This database needs a clean dump & load (or we need to expand our
         * genids */
        logmsg(LOGMSG_ERROR, "%s: this database has run out of genids!\n",
               __func__);
        sleep(1);
    }

    seed48++;

    if (bdb_state->attr->genid48_warn_threshold &&
            (0x0000ffffffffffffULL - seed48) <=
            bdb_state->attr->genid48_warn_threshold)
        prwarn = 1;

    s48ptr = (uint16_t *)&seed48;
    iptr = (unsigned int *)&genid;

#if defined(_LINUX_SOURCE)
    memcpy(&highorder, &s48ptr[1], 4);
    loworder = s48ptr[0] << 16;
#else
    memcpy(&highorder, &s48ptr[1], 4);
    memcpy(&loworder, &s48ptr[3], 2);
#endif
    loworder |= (dtafile & 0x0000000f);
    iptr[0] = htonl(highorder);
    iptr[1] = htonl(loworder);

    bdb_state->gblcontext = genid;

    if (lsn) {
        set_commit_genid_lsn_gen(bdb_state, genid, lsn, &generation);
    }

    Pthread_mutex_unlock(&(bdb_state->gblcontext_lock));
    if (prwarn && (now = time(NULL)) > lastwarn) {
        logmsg(LOGMSG_WARN, "%s: low-genid warning: this database has only "
                            "%llu genids remaining\n",
               __func__, 0x0000ffffffffffffULL - seed48);
        lastwarn = now;
    }
    return genid;
}

void seed_genid48(bdb_state_type *bdb_state, uint64_t seed)
{
    get_genid_48bit(bdb_state, 0, NULL, 0, seed);
}

static unsigned long long get_genid_timebased(bdb_state_type *bdb_state,
                                      unsigned int dtafile, DB_LSN *lsn,
                                      uint32_t generation)
{
    /* keep a stable copy of the gblcontext */
    unsigned int *iptr;
    unsigned long long genid;
    unsigned long long gblcontext;
    unsigned int epoch;
    unsigned int next_seed;
    unsigned int munged_seed;
    unsigned int munged_dtafile;
    int epochtime;
    int contexttime;
    gblcontext = get_gblcontext(bdb_state);

    if (!bdb_state->attr->genidplusplus) {
    stall:
        epochtime = comdb2_time_epoch();
        contexttime = bdb_genid_timestamp(gblcontext);

        if (contexttime > epochtime) {
            logmsg(LOGMSG_WARN, "context is %d epoch is %d  - stalling!!!\n",
                   contexttime, epochtime);
            poll(NULL, 0, 100);
            goto stall;
        }

        iptr = (unsigned int *)&genid;

try_again:

        Pthread_mutex_lock(&(bdb_state->gblcontext_lock));

        epoch = comdb2_time_epoch();
        gblcontext = bdb_state->gblcontext;
        contexttime = bdb_genid_timestamp(gblcontext);

        if (contexttime == epoch) {
            bdb_state->seed = get_dupecount_from_genid(gblcontext);
            bdb_state->seed++;
        } else {
            bdb_state->seed = 1;
        }

        if (bdb_state->seed >= 0x10000) {
            /* I can't conceive that this code will every execute - 64K
             * insertions
             * or updates a second would be rather good though. */
            Pthread_mutex_unlock(&(bdb_state->gblcontext_lock));

            /*fprintf(stderr, "WARNING: needed more than max genids per
             * second (%d).\n", 0x10000); */
            poll(NULL, 0, 10);
            goto try_again;
        }
    } else {
        iptr = (unsigned int *)&genid;

        gblcontext = contexttime = epoch = get_epoch_plusplus(bdb_state);
        bdb_state->seed = 1;
    }

    next_seed = bdb_state->seed;

    /* shift over the seed then or in the dtafile */
    munged_seed = next_seed << 16;

    munged_dtafile = dtafile & 0x0000000f;
    munged_seed = munged_seed | munged_dtafile;

    /* genids (their components at least) are always "big-endian" */
    iptr[0] = htonl(epoch);
    iptr[1] = htonl(munged_seed);

    bdb_state->gblcontext = genid;

    /* this limps at a different speed compare to gblcontext */
    if (lsn) {
        set_commit_genid_lsn_gen(bdb_state, genid, lsn, &generation);
    }

    Pthread_mutex_unlock(&(bdb_state->gblcontext_lock));
    return genid;
}

static inline 
unsigned long long get_genid_int(bdb_state_type *bdb_state,
                                 unsigned int dtafile, DB_LSN *lsn,
                                 uint32_t generation)
{
    /* if we were passed a child, find his parent */
    /* this means we have a single genid allocator PER DB rather than PER TABLE
       now.  the nice part about this is maintaining distributed
       "cursor contexts" becomes a much simpler problem rather than the broken
       mess it currently is. */
    extern int gbl_llmeta_open;
    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    if (gbl_llmeta_open == 0)
        return 0ULL;

#if defined FORCE_GENID48
    if (1)
#else
    if (bdb_state->genid_format == LLMETA_GENID_48BIT)
#endif
    {
        return get_genid_48bit(bdb_state, dtafile, lsn, generation, 0);
    }
    else {
        return get_genid_timebased(bdb_state, dtafile, lsn, generation);
    }
}

unsigned long long get_genid(bdb_state_type *bdb_state, unsigned int dtafile)
{
    return get_genid_int(bdb_state, dtafile, NULL, 0);
}

unsigned long long bdb_get_a_genid(bdb_state_type *bdb_state)
{
    return get_genid(bdb_state, 0);
}

/* set the genid's participant stripe value & return the new genid */
unsigned long long set_participant_stripeid(bdb_state_type *bdb_state,
                                            int stripeid,
                                            unsigned long long genid)
{
    int mask, shift, pidb = (bdb_state->attr->participantid_bits);
    if (pidb <= 0 || stripeid <= 0) {
        return genid;
    }
    if (pidb > 12)
        pidb = 12;
    mask = (0x1 << pidb) - 1;
    stripeid &= mask;
    shift = (16 - pidb);
    genid &= ~(mask << shift);
    genid |= (stripeid << shift);
    return genid;
}

/* set the genid's update id value & return the new genid */
unsigned long long set_updateid(bdb_state_type *bdb_state, int updateid,
                                unsigned long long genid)
{
    int mask, bits, pidb = bdb_state->attr->participantid_bits;
    if (pidb >= 12) {
        return genid;
    }
    genid = flibc_htonll(genid);
    if (pidb < 0)
        pidb = 0;
    bits = (12 - pidb);
    mask = (0x1 << bits) - 1;
    updateid &= mask;
    genid &= ~(mask << 4);
    genid |= (updateid << 4);
    return flibc_ntohll(genid);
}

/* return the maximum updateid for this bdb_state */
int max_updateid(bdb_state_type *bdb_state)
{
    int mask, bits, pidb = bdb_state->attr->participantid_bits;
    if (pidb >= 12) {
        return 0;
    }
    if (pidb < 0)
        pidb = 0;
    bits = (12 - pidb);
    mask = (0x1 << bits) - 1;
    return mask;
}

/* return the maximum participant stripe-id for this bdb_state */
int max_participant_stripeid(bdb_state_type *bdb_state)
{
    int mask, pidb = bdb_state->attr->participantid_bits;
    if (pidb <= 0)
        return 0;
    if (pidb > 12)
        pidb = 12;
    mask = (0x1 << pidb) - 1;
    return mask;
}

/* the bdb_state object tells how many bits are in the participant stripe */
int bdb_get_participant_stripe_from_genid(bdb_state_type *bdb_state,
                                          unsigned long long genid)
{
    int mask, pidb = bdb_state->attr->participantid_bits;
    if (pidb <= 0)
        return 0;
    if (pidb > 12)
        pidb = 12;
    mask = (0x1 << pidb) - 1;
    genid >>= (16 - pidb);
    return (genid & mask);
}

/* using the bdb_state object, return the updateid for this genid */
int get_updateid_from_genid(bdb_state_type *bdb_state, unsigned long long genid)
{
    int mask, bits, pidb = bdb_state->attr->participantid_bits;
    if (pidb >= 12)
        return 0;
    if (pidb < 0)
        pidb = 0;
    bits = (12 - pidb);
    mask = (0x1 << bits) - 1;
    genid = flibc_htonll(genid);
    genid >>= 4;
    return (genid & mask);
}

/* externally callable */
unsigned long long bdb_get_timestamp(bdb_state_type *bdb_state)
{
    return bdb_genid_to_host_order(get_genid(bdb_state, 0));
}

/* convert a genid to host byte order.  this should rarely be used, throughout
 * the codebase genids should remain in network byte order; this is really only
 * used so that genids can be properly printed to a string */
unsigned long long bdb_genid_to_host_order(unsigned long long genid)
{
    return flibc_ntohll(genid);
}

unsigned long long bdb_genid_to_net_order(unsigned long long genid)
{
    return flibc_htonll(genid);
}

/* when the database comes up, we set a master_cmpcontext value by incrementing
 * the slot-component of the maximum genid.
 */
unsigned long long bdb_increment_slot(bdb_state_type *bdb_state,
                                      unsigned long long a)
{
    uint16_t slot;

    /* grab the slot */
    slot = (uint16_t)((a & GENID_SLOT_MASK) >> GENID_SLOT_PRE_SHIFT);

    /* increment */
    slot = (ntohs(slot) >> GENID_SLOT_POST_SHIFT) + 1;

    /* mask out in argument genid */
    a &= ~(GENID_SLOT_MASK);

    /* back to big-endian */
    slot = htons(slot << GENID_SLOT_POST_SHIFT);

    /* replace slot */
    a |= (unsigned long long)(slot) << GENID_SLOT_PRE_SHIFT;

    return a;
}

unsigned long long bdb_mask_stripe(bdb_state_type *bdb_state,
                                   unsigned long long a)
{
    return (a & ~(GENID_STRIPE_MASK));
}

/* Compare two genids to determine which one would have been allocated first.
 * Return codes:
 *    -1    a < b
 *    0     a == b
 *    1     a > b
 */
int bdb_cmp_genids(unsigned long long a, unsigned long long b)
{
    unsigned int *a_iptr;
    unsigned int *b_iptr;
    unsigned int a_epoch;
    unsigned int b_epoch;
    unsigned int a_rhs;
    unsigned int b_rhs;

    a_iptr = (unsigned int *)&a;
    b_iptr = (unsigned int *)&b;

    /* first compare the epoch component in host byte order */

    a_epoch = ntohl(a_iptr[0]);
    b_epoch = ntohl(b_iptr[0]);

    if (a_epoch < b_epoch)
        return -1;

    if (a_epoch > b_epoch)
        return 1;

    /* take the stripe out then get the right hand side in host byte order */

    a &= ~GENID_STRIPE_MASK;
    b &= ~GENID_STRIPE_MASK;
    a_rhs = ntohl(a_iptr[1]);
    b_rhs = ntohl(b_iptr[1]);

    if (a_rhs < b_rhs)
        return -1;

    if (a_rhs > b_rhs)
        return 1;

    return 0;
}

/* return 0 if g1 and g2 are in-place equivalent, non-0 otherwise */
int bdb_inplace_cmp_genids(bdb_state_type *bdb_state, unsigned long long g1,
                           unsigned long long g2)
{
    unsigned long long s1, s2;

    /*
    s1=get_search_genid(bdb_state, g1);
    s2=get_search_genid(bdb_state, g2);
    */

    s1 = bdb_mask_updateid(bdb_state, g1);
    s2 = bdb_mask_updateid(bdb_state, g2);

    return bdb_cmp_genids(s1, s2);
}

unsigned long long bdb_get_cmp_context_int(bdb_state_type *bdb_state)
{
    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    /* if we're the master, get a context, otherwise use the last one we
       recieved from the master */
    if (bdb_state->repinfo->master_host ==
        net_get_mynode(bdb_state->repinfo->netinfo)) {
        return get_genid(bdb_state, 0);
    } else {
        return get_gblcontext(bdb_state);
    }
}

unsigned long long bdb_get_cmp_context(bdb_state_type *bdb_state)
{
    unsigned long long context;

    context = bdb_get_cmp_context_int(bdb_state);

    /*fprintf(stderr, "bdb_get_cmp_context %p 0x%llx\n", bdb_state, context);*/

    return context;
}

int bdb_next_dtafile(bdb_state_type *bdb_state)
{
    int stripe;
    if (!bdb_state->parent)
        return 0;

    Pthread_mutex_lock(&bdb_state->last_dta_lk);
    stripe = bdb_state->last_dta =
        (bdb_state->last_dta + 1) % bdb_state->attr->dtastripe;
    Pthread_mutex_unlock(&bdb_state->last_dta_lk);
    return stripe;
}

/* Check that the given genid is older than the compare context being given.
 * Returns: 1 genid is older, 0 genid is newer */
int bdb_check_genid_is_older(bdb_state_type *bdb_state,
                             unsigned long long genid,
                             unsigned long long context)
{
    unsigned int *genid_iptr, *context_iptr;
    unsigned int genid_epoch;
    unsigned int context_epoch;

    /*
    fprintf(stderr, "bdb_check_genid_is_older genid: %llx  context: %llx\n",
       genid, context);
    */

    if (!context || !genid)
        return 1;

    genid_iptr = (unsigned int *)&genid;
    context_iptr = (unsigned int *)&context;

    /* first compare epoch times */
    genid_epoch = ntohl(genid_iptr[0]);
    context_epoch = ntohl(context_iptr[0]);
    if (genid_epoch > context_epoch)
        return 0;
    else if (genid_epoch < context_epoch)
        return 1;
    else {
        /* We're in the same second.  compare seeds.  seeds are not reset each
         * second - they wrap.  i found that drawing circles helped me
         *understand
         * this logic.
         *
         * For live schema change the genid allocation system was changed to
         * always hand out monotonically increasing genids.  But I still keep
         * this strange circular logic.  Why?  Just in case we get scenarios
         * like old server is master, new server is replicant receiving genids
         * that have wrapped like this..  This logic is complicated but should
         * be harmless for the new scheme (unless we allocate a thumping large
         * number of records each second). */
        unsigned int genid_seed = ntohl(genid_iptr[1]) >> 4;
        unsigned int context_seed = ntohl(context_iptr[1]) >> 4;
        unsigned int normalised = (genid_seed - context_seed) & 0xfffffff;
        /* once we have normalised the genid seed against the context seed using
         * clock arithmetic, anything in the first hemisphere of the clock is
         * bad (bad = newer). */
        if (normalised < 0x7ffffff)
            return 0;
        else
            return 1;
    }
}

/* just like above, but reverse logic */
int bdb_check_genid_is_newer(bdb_state_type *bdb_state,
                             unsigned long long genid,
                             unsigned long long context)
{
    unsigned int *genid_iptr, *context_iptr;
    unsigned int genid_epoch;
    unsigned int context_epoch;

    if (!context || !genid)
        return 0;

    genid_iptr = (unsigned int *)&genid;
    context_iptr = (unsigned int *)&context;

    genid_epoch = ntohl(genid_iptr[0]);
    context_epoch = ntohl(context_iptr[0]);

    if (genid_epoch <= context_epoch)
        return 0;
    else if (genid_epoch > context_epoch)
        return 1;
    else {
        unsigned int genid_seed = ntohl(genid_iptr[1]) >> 4;
        unsigned int context_seed = ntohl(context_iptr[1]) >> 4;
        unsigned int normalized = (context_seed - genid_seed) & 0xfffffff;
        if (normalized < 0x7ffffff)
            return 1;
        else
            return 0;
    }
}

DB *get_dbp_from_genid(bdb_state_type *bdb_state, int dtanum,
                       unsigned long long genid, int *out_dtafile)
{
    int dtafile;
    if (bdb_state->attr->dtastripe) {
        if (0 == dtanum || bdb_state->attr->blobstripe) {
            dtafile = get_dtafile_from_genid(genid);
            if (dtanum > 0 && bdb_state->blobstripe_convert_genid) {
                /* kludge for records that were inserted/updated before we
                 * converted the db to blobstripe. */
                if (bdb_check_genid_is_older(
                        bdb_state, genid, bdb_state->blobstripe_convert_genid))
                    dtafile = 0;
            }
        } else
            dtafile = 0;
    } else {
        dtafile = 0;
    }
    if (out_dtafile)
        *out_dtafile = dtafile;
    return bdb_state->dbp_data[dtanum][dtafile];
}

unsigned long long bdb_normalise_genid(bdb_state_type *bdb_state,
                                       unsigned long long genid)
{
    if (!bdb_state->ondisk_header || !bdb_state->inplace_updates) {
        return bdb_mask_updateid(bdb_state, genid);
    } else {
        return genid;
    }
}

unsigned long long increment_seq(unsigned long long crt)
{
    crt = bdb_genid_to_host_order(crt);
    return bdb_genid_to_net_order(++crt);
}

/* gets the datafile/stripe that an add from this thread would write to
 */
int bdb_get_active_stripe_int(bdb_state_type *bdb_state)
{
    bdb_state_type *parent;
    size_t id;

    if (bdb_state->parent)
        parent = bdb_state->parent;
    else
        parent = bdb_state;

    if (parent->attr->round_robin_stripes)
        return bdb_next_dtafile(bdb_state);

    id = (size_t)pthread_getspecific(parent->tid_key);

    return id % bdb_state->attr->dtastripe;
}

int bdb_get_active_stripe(bdb_state_type *bdb_state)
{
    int dtafile;

    BDB_READLOCK("bdb_get_active_dtafile");
    dtafile = bdb_get_active_stripe_int(bdb_state);
    BDB_RELLOCK();

    return dtafile;
}

/* this is called by writers on the master */
unsigned long long bdb_gen_commit_genid(bdb_state_type *bdb_state,
                                        const void *plsn, uint32_t generation)
{
    unsigned long long ret;

    /* locks embedded */
    ret = get_genid_int(bdb_state, 0, (DB_LSN *)plsn, generation);

    /*
    fprintf( stderr, "%s:%d got context %llx\n", __FILE__, __LINE__, ret);
    */
    return ret;
}

/* this is called by readers that need starting context */
unsigned long long bdb_get_commit_genid_generation( bdb_state_type *bdb_state, void *plsn, uint32_t *generation)
{
    unsigned long long ret = 0;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    Pthread_mutex_lock(&(bdb_state->gblcontext_lock));

    ret = commit_genid;
    if (plsn)
        *(DB_LSN *)plsn = commit_lsn;

   if (generation)
       *generation = commit_generation;

   Pthread_mutex_unlock(&(bdb_state->gblcontext_lock));

    /*
   fprintf( stderr, "%d %s:%d retrieved context %llx\n",
         pthread_self(), __FILE__, __LINE__, ret);
    */

    return ret;
}

void bdb_set_commit_lsn_gen(bdb_state_type *bdb_state, const void *inlsn, uint32_t gen)
{
    DB_LSN *lsn = (DB_LSN *)inlsn;
    Pthread_mutex_lock(&(bdb_state->gblcontext_lock));
    commit_lsn = *lsn;
    commit_generation = gen;
    Pthread_mutex_unlock(&(bdb_state->gblcontext_lock));
}

unsigned long long bdb_get_commit_genid(bdb_state_type *bdb_state, void *plsn)
{
    return bdb_get_commit_genid_generation(bdb_state, plsn, NULL);
}

/* this is called on the replicant to set the context upon applying commits */
void bdb_set_commit_genid( bdb_state_type *bdb_state, unsigned long long context, const uint32_t *generation, 
        const void *plsn, const void *args, unsigned int rectype)
{
    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    if (plsn)
        bdb_update_ltran_lsns(bdb_state, *(const DB_LSN *)plsn, args, rectype);

    Pthread_mutex_lock(&(bdb_state->gblcontext_lock));

    set_commit_genid_lsn_gen(bdb_state, context, (const DB_LSN *)plsn,
                             (const uint32_t *)generation);

    Pthread_mutex_unlock(&(bdb_state->gblcontext_lock));
}

int bdb_genid_allow_original_format(bdb_state_type *bdb_state)
{
    unsigned long long gblcontext;
    unsigned int *iptr;
    time_t gentime;
    assert(!bdb_state->parent);
    assert(bdb_state->genid_format == LLMETA_GENID_48BIT);
    gblcontext = get_gblcontext(bdb_state);
    iptr = (unsigned int *)&gblcontext;
    gentime = ntohl(iptr[0]);
    if (gentime > time(NULL))
        return 0;
    return 1;
}

int bdb_genid_format(bdb_state_type *bdb_state)
{
    assert(!bdb_state->parent);
    return bdb_state->genid_format;
}

int bdb_genid_set_format(bdb_state_type *bdb_state, int format)
{
    assert(format == LLMETA_GENID_ORIGINAL || format == LLMETA_GENID_48BIT);
    bdb_state->genid_format = format;
    return 0;
}
