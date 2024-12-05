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
 * Ondisk queue data structure with support for variable length items
 * and multiple consumers.
 *
 * We use a single berkdb ondisk queue.  Items that are too large for the
 * record size (which is the pagesize - 32) are fragmented into multiple
 * records.
 *
 * An alternatives would be the the queue records reference randomly
 * distributed data records in supporting btree, but that method may end
 * up with the btree having high contention if the records are consumed
 * as soon as they are added.
 *
 * The queue can legally contain records in which the genid is zero.  These
 * are dummy records which get inserted with the intention that they will be
 * consumed using a DB->get(DB_CONSUME) operation, which is the only way to
 * guarantee that the queue head is advanced (SR #14652).  I refer to these
 * dummy records as "goose" records since their purpose is to make sure
 * the queue head moves forwards. 
 */

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <stddef.h>
#include <pthread.h>

#include <epochlib.h>

#include <plbitlib.h>
#include <bb_stdint.h>

#include <sys/types.h>
#include <netinet/in.h>
#include <inttypes.h>
#include "endian_core.h"
#include "bdb_cursor.h"
#include "bdb_int.h"
#include "locks.h"
#include "sys_wrap.h"

#include "bdb_queue.h"
#include "bdb_queuedb.h"
#include "logmsg.h"

/* See also Oracle support request #6353586.992.  It seems that non
 * transactional reads against a queue (at least, the way I was doing them) is
 * prone to deadlock.  By making my cursor transactional I seem to get around
 * this.
 * Update: a patch was received and applied to our berkeley db build, so this
 * hack can now be disabled. */
#define TXN_READ_HACK() 0

/*
 * Endian-friendly accessor functions for bdb-queue
 */

/*
enum
{
    QUEUE_HDR_LEN       = 2 + 2 + 8 + 4 + 4 + 4 + 4 + 4
};

BB_COMPILE_TIME_ASSERT(bdb_queue_header_size, sizeof(struct bdb_queue_header) ==
        QUEUE_HDR_LEN);

*/

const uint8_t *queue_hdr_get(struct bdb_queue_header *p_queue_hdr,
                             const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || QUEUE_HDR_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_queue_hdr->fragment_no),
                    sizeof(p_queue_hdr->fragment_no), p_buf, p_buf_end);
    p_buf = buf_get(&(p_queue_hdr->num_fragments),
                    sizeof(p_queue_hdr->num_fragments), p_buf, p_buf_end);
    p_buf = buf_get(&(p_queue_hdr->genid[0]), sizeof(p_queue_hdr->genid[0]),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_queue_hdr->genid[1]), sizeof(p_queue_hdr->genid[1]),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_queue_hdr->total_sz), sizeof(p_queue_hdr->total_sz),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_queue_hdr->fragment_sz),
                    sizeof(p_queue_hdr->fragment_sz), p_buf, p_buf_end);
    p_buf =
        buf_no_net_get(&(p_queue_hdr->consumer_mask),
                       sizeof(p_queue_hdr->consumer_mask), p_buf, p_buf_end);
    p_buf = buf_get(&(p_queue_hdr->crc32), sizeof(p_queue_hdr->crc32), p_buf,
                    p_buf_end);
    p_buf = buf_get(&(p_queue_hdr->reserved), sizeof(p_queue_hdr->reserved),
                    p_buf, p_buf_end);

    return p_buf;
}

uint8_t *queue_hdr_put(const struct bdb_queue_header *p_queue_hdr,
                       uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || QUEUE_HDR_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_queue_hdr->fragment_no),
                    sizeof(p_queue_hdr->fragment_no), p_buf, p_buf_end);
    p_buf = buf_put(&(p_queue_hdr->num_fragments),
                    sizeof(p_queue_hdr->num_fragments), p_buf, p_buf_end);
    p_buf = buf_put(&(p_queue_hdr->genid[0]), sizeof(p_queue_hdr->genid[0]),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_queue_hdr->genid[1]), sizeof(p_queue_hdr->genid[1]),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_queue_hdr->total_sz), sizeof(p_queue_hdr->total_sz),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_queue_hdr->fragment_sz),
                    sizeof(p_queue_hdr->fragment_sz), p_buf, p_buf_end);
    p_buf =
        buf_no_net_put(&(p_queue_hdr->consumer_mask),
                       sizeof(p_queue_hdr->consumer_mask), p_buf, p_buf_end);
    p_buf = buf_put(&(p_queue_hdr->crc32), sizeof(p_queue_hdr->crc32), p_buf,
                    p_buf_end);
    p_buf = buf_put(&(p_queue_hdr->reserved), sizeof(p_queue_hdr->reserved),
                    p_buf, p_buf_end);

    return p_buf;
}

enum { QUEUE_FOUND_LEN = 8 + 4 + 4 + 4 + 4 };

BB_COMPILE_TIME_ASSERT(bdb_queue_found_len,
                       sizeof(struct bdb_queue_found) == QUEUE_FOUND_LEN);

const uint8_t *queue_found_get(struct bdb_queue_found *p_queue_found,
                               const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || QUEUE_FOUND_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_queue_found->genid), sizeof(p_queue_found->genid),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_queue_found->data_len), sizeof(p_queue_found->data_len),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_queue_found->data_offset),
                    sizeof(p_queue_found->data_offset), p_buf, p_buf_end);
    p_buf =
        buf_get(&(p_queue_found->trans.num_fragments),
                sizeof(p_queue_found->trans.num_fragments), p_buf, p_buf_end);
    p_buf = buf_get(&(p_queue_found->epoch), sizeof(p_queue_found->epoch),
                    p_buf, p_buf_end);

    return p_buf;
}

uint8_t *queue_found_put(const struct bdb_queue_found *p_queue_found,
                         uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || QUEUE_FOUND_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_queue_found->genid), sizeof(p_queue_found->genid),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_queue_found->data_len), sizeof(p_queue_found->data_len),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_queue_found->data_offset),
                    sizeof(p_queue_found->data_offset), p_buf, p_buf_end);
    p_buf =
        buf_put(&(p_queue_found->trans.num_fragments),
                sizeof(p_queue_found->trans.num_fragments), p_buf, p_buf_end);
    p_buf = buf_put(&(p_queue_found->epoch), sizeof(p_queue_found->epoch),
                    p_buf, p_buf_end);

    return p_buf;
}

enum { QUEUE_FOUND_SEQ_LEN = 8 + 4 + 4 + 4 + 4 + 4 + 4 };

BB_COMPILE_TIME_ASSERT(bdb_queue_found_seq_len,
                       sizeof(struct bdb_queue_found_seq) ==
                           QUEUE_FOUND_SEQ_LEN);

const uint8_t *
queue_found_seq_get(struct bdb_queue_found_seq *p_queue_found_seq,
                    const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || QUEUE_FOUND_SEQ_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_queue_found_seq->genid),
                    sizeof(p_queue_found_seq->genid), p_buf, p_buf_end);
    p_buf = buf_get(&(p_queue_found_seq->data_len),
                    sizeof(p_queue_found_seq->data_len), p_buf, p_buf_end);
    p_buf = buf_get(&(p_queue_found_seq->data_offset),
                    sizeof(p_queue_found_seq->data_offset), p_buf, p_buf_end);
    p_buf = buf_get(&(p_queue_found_seq->trans.num_fragments),
                    sizeof(p_queue_found_seq->trans.num_fragments), p_buf,
                    p_buf_end);
    p_buf = buf_get(&(p_queue_found_seq->epoch),
                    sizeof(p_queue_found_seq->epoch), p_buf, p_buf_end);
    p_buf = buf_get(&(p_queue_found_seq->seq), sizeof(p_queue_found_seq->seq),
                    p_buf, p_buf_end);

    return p_buf;
}

uint8_t *
queue_found_seq_put(const struct bdb_queue_found_seq *p_queue_found_seq,
                    uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || QUEUE_FOUND_SEQ_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_queue_found_seq->genid),
                    sizeof(p_queue_found_seq->genid), p_buf, p_buf_end);
    p_buf = buf_put(&(p_queue_found_seq->data_len),
                    sizeof(p_queue_found_seq->data_len), p_buf, p_buf_end);
    p_buf = buf_put(&(p_queue_found_seq->data_offset),
                    sizeof(p_queue_found_seq->data_offset), p_buf, p_buf_end);
    p_buf = buf_put(&(p_queue_found_seq->trans.num_fragments),
                    sizeof(p_queue_found_seq->trans.num_fragments), p_buf,
                    p_buf_end);
    p_buf = buf_put(&(p_queue_found_seq->epoch),
                    sizeof(p_queue_found_seq->epoch), p_buf, p_buf_end);
    p_buf = buf_put(&(p_queue_found_seq->seq), sizeof(p_queue_found_seq->seq),
                    p_buf, p_buf_end);

    return p_buf;
}

struct bdb_queue_priv {
    struct bdb_queue_stats stats;

    uint32_t known_lost_consumers;
    int last_orphaned_consumers_alarm;
};

/* This is based on the old genid function but without all the extra dta file
 * munging stuff that was added for dtastripe - all we need for queues are
 * unique keys.  These genids cannot be zero. */
static unsigned long long get_queue_genid(bdb_state_type *bdb_state)
{
    unsigned long long genid;
    unsigned int *iptr;
    unsigned int next_seed;

    iptr = (unsigned int *)&genid;

    do {
        /* there is a seed_lock and a seed for each bdb_state */
        Pthread_mutex_lock(&(bdb_state->seed_lock));
        bdb_state->seed++;
        next_seed = bdb_state->seed;
        Pthread_mutex_unlock(&(bdb_state->seed_lock));

        iptr[0] = comdb2_time_epoch();
        iptr[1] = next_seed;
    } while (genid == 0);

    return genid;
}

void bdb_queue_init_priv(bdb_state_type *bdb_state)
{
    if (bdb_state->bdbtype == BDBTYPE_QUEUEDB) {
        bdb_queuedb_init_priv(bdb_state);
        return;
    }

    bdb_state->qpriv = calloc(1, sizeof(bdb_queue_priv));
    if (!bdb_state->qpriv) {
        logmsg(LOGMSG_FATAL, "%s: out of memory\n", __func__);
        exit(1);
    }
}

/* mark a consumer as active or inactive. */
int bdb_queue_consumer(bdb_state_type *bdb_state, int consumer, int active,
                       int *bdberr)
{
    *bdberr = BDBERR_NOERROR;

    if (consumer < 0 || consumer >= BDBQUEUE_MAX_CONSUMERS) {
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    BDB_READLOCK("bdb_queue_consumer");
    {
        if (active)
            bset(&bdb_state->active_consumers, consumer);
        else
            bclr(&bdb_state->active_consumers, consumer);
    }
    BDB_RELLOCK();

    return 0;
}

/* Calculate how many items you can fit per page given the expected average
 * item size, the page size and whether or not checksums are enabled.
 * This is reverse engineered from berkdb source code (ideally we'd
 * just call their macros..) */
static int items_per_page(int avg_item_sz, int pagesize, int checksums)
{
    /* Remove size of page header; this is from dbinc/db_page.h
     * QPAGE_NORMAL and QPAGE_CHKSUM. */
    if (checksums)
        pagesize -= 48;
    else
        pagesize -= 28;

    /* Each item has a 1 byte header in berkeley, and then is rounded to 32
     * bits.  Also we have our own header. */
    avg_item_sz += sizeof(struct bdb_queue_header);
    avg_item_sz++;
    avg_item_sz = (avg_item_sz + 3) & ~3;

    return pagesize / avg_item_sz;
}

static int max_item_size(int pagesize, int checksums)
{
    /* Remove size of page header; this is from dbinc/db_page.h
     * QPAGE_NORMAL and QPAGE_CHKSUM. */
    if (checksums)
        pagesize -= 48;
    else
        pagesize -= 28;

    /* Remaining pagesize must be 32 bit aligned.  Subtract one for the
     * 1 byte berkdb item header and you have the maximum possible user
     * level page size.  Also subtract our own item header. */
    return pagesize - 1 - sizeof(struct bdb_queue_header);
}

static int pagesize_wasteage(int avg_item_sz, int pagesize, int checksums)
{
    int wasteage;

    /* Remove size of page header; this is from dbinc/db_page.h
     * QPAGE_NORMAL and QPAGE_CHKSUM. */
    if (checksums)
        wasteage = pagesize - 48;
    else
        wasteage = pagesize - 28;

    /* Each item has a 1 byte header in berkeley, and then is rounded to 32
     * bits.  Also we have our own header. */
    avg_item_sz += sizeof(struct bdb_queue_header);
    avg_item_sz++;
    avg_item_sz = (avg_item_sz + 3) & ~3;

    return wasteage % avg_item_sz;
}

/* bb_filexfer transfers 100KB at a time.  don't ever specify a page size
 * that does not divide evently into that.  in fact, don't go over 8KB. */
static int pagesize_ok(int pagesize)
{
    if (pagesize <= 0)
        return -1;

    if ((100 * 1024) % pagesize != 0)
        return -1;

    if (pagesize > (8 * 1024))
        return -1;

    return 0;
}

int bdb_queue_best_pagesize(int avg_item_sz)
{
    int pagesize;
    int bestpagesize;
    int bestwasteage;
    int checksums = 1; /* assume checksums on for now */

    /* Discover the minimum page size that will cover items of this length. */
    pagesize = 512;

#if 0
    /* Don't go below this, or set_pagesize will fail */
    if (pagesize < 512 /*db_int.h/DB_MIN_PGSIZE*/)
        pagesize = 512;
#endif

    while (max_item_size(pagesize, checksums) < avg_item_sz) {
        pagesize <<= 1;
        if (pagesize_ok(pagesize) != 0) {
            /* These items are huge!  Make no suggestion. */
            return 0;
        }
    }

    /* Calculate the efficiency of this page size and higher page sizes
     * until we find an optimum. */
    bestpagesize = pagesize;
    bestwasteage = pagesize_wasteage(avg_item_sz, pagesize, checksums);
    while (pagesize <= 0x10000) {
        if (pagesize_ok(pagesize) == 0) {
            int wasteage = pagesize_wasteage(avg_item_sz, pagesize, checksums);
            if (wasteage < bestwasteage) {
                bestwasteage = wasteage;
                bestpagesize = pagesize;
            }
        }
        pagesize <<= 1;
    }

    return bestpagesize;
}

extern int gbl_rowlocks;

static int bdb_queue_add_int(bdb_state_type *bdb_state, tran_type *intran,
                             const void *dta, size_t dtalen, int *bdberr,
                             unsigned long long *out_genid)
{
    size_t numfragments;
    char *fragment;
    size_t lrl;
    size_t nfrag;
    unsigned long long genid;
    tran_type *tran;

    if (gbl_rowlocks) {
        get_physical_transaction(bdb_state, intran, &tran, 0);
    } else
        tran = intran;

    if (out_genid)
        *out_genid = 0;

    if (!bdb_state->read_write) {
        *bdberr = BDBERR_READONLY;
        return -1;
    }

    *bdberr = BDBERR_NOERROR;

    /* if no consumers are registered then don't enqueue this thing, since
     * nobody wants it. */
    if (bdb_state->active_consumers == 0)
        return 0;

    lrl = bdb_state->queue_item_sz - sizeof(struct bdb_queue_header);

    /* Work out how many fragments we need. */
    numfragments = (dtalen + (lrl - 1)) / lrl;

    fragment = malloc(bdb_state->queue_item_sz);
    if (!fragment) {
        logmsg(LOGMSG_ERROR, "bdb_queue_add_int: cannot malloc %zu bytes\n",
               bdb_state->queue_item_sz);
        *bdberr = BDBERR_MALLOC;
        return -1;
    }

    genid = get_queue_genid(bdb_state);

    /* Add each fragment. */
    for (nfrag = 0; nfrag < numfragments; nfrag++) {
        struct bdb_queue_header hdr = {0};

        /*struct bdb_queue_header *)fragment;*/

        uint8_t *p_buf = (uint8_t *)fragment,
                *p_buf_end = (p_buf + bdb_state->queue_item_sz);
        size_t offset, length;
        DBT dbt_key, dbt_data;
        db_recno_t recno;
        int rc;

        /* Construct the fragment. */
        offset = nfrag * lrl;
        length = lrl;
        if (offset + length > dtalen)
            length = dtalen - offset;

        bzero(fragment, sizeof(struct bdb_queue_header));

        memcpy(hdr.genid, &genid, sizeof(genid));
        hdr.fragment_no = nfrag;
        hdr.num_fragments = numfragments;
        hdr.total_sz = dtalen;
        hdr.fragment_sz = length;

        if (nfrag == 0)
            hdr.consumer_mask = bdb_state->active_consumers;

        if (!(p_buf = queue_hdr_put(&hdr, p_buf, p_buf_end))) {
            logmsg(LOGMSG_ERROR, "%s: queue_hdr_put failed\n", __FUNCTION__);
            *bdberr = BDBERR_MISC;
            free(fragment);
            return -1;
        }

        p_buf = buf_no_net_put(((const char *)dta) + offset, length, p_buf,
                               p_buf_end);

        /* First fragment has a consumer mask to say who will consume it. */

        /* Construct dbt_key and dbt_data */
        bzero(&dbt_key, sizeof(dbt_key));
        bzero(&dbt_data, sizeof(dbt_data));

        dbt_key.data = &recno;
        dbt_key.size = sizeof(recno);
        dbt_key.ulen = sizeof(recno);
        dbt_key.flags = DB_DBT_USERMEM;

        dbt_data.data = fragment;
        dbt_data.size = length + sizeof(struct bdb_queue_header);
        dbt_data.flags = DB_DBT_USERMEM;

        /* Add it. */
        rc =
            bdb_state->dbp_data[0][0]->put(bdb_state->dbp_data[0][0], tran->tid,
                                           &dbt_key, &dbt_data, DB_APPEND);
        if (rc != 0) {
            switch (rc) {
            case DB_LOCK_DEADLOCK:
                *bdberr = BDBERR_DEADLOCK;
                break;

            default:
                *bdberr = BDBERR_MISC;
                logmsg(LOGMSG_ERROR, "%s: put failed %d %s\n", __FUNCTION__, rc,
                        db_strerror(rc));
                break;
            }
            free(fragment);
            return -1;
        }
    }

    free(fragment);
    if (out_genid)
        *out_genid = genid;
    bdb_state->qdb_adds++;
    return 0;
}

unsigned long long bdb_queue_item_genid(const struct bdb_queue_found *dta)
{
    if (dta) {
        uint8_t *p_buf = (uint8_t *)dta;
        uint8_t *p_buf_end = (p_buf + QUEUE_FOUND_LEN);
        struct bdb_queue_found item;

        if (!queue_found_get(&item, p_buf, p_buf_end)) {
            logmsg(LOGMSG_ERROR, "%s line %d: queue_found_get returns NULL\n",
                    __func__, __LINE__);
            return 0;
        }
        return item.genid;
    }
    return 0;
}

/* add an item to the end of the queue. */
int bdb_queue_add(bdb_state_type *bdb_state, tran_type *tran, const void *dta,
                  size_t dtalen, int *bdberr, unsigned long long *out_genid)
{
    int rc = 0;

    BDB_READLOCK("bdb_queue_add");
    if (bdb_state->bdbtype == BDBTYPE_QUEUEDB) {
        rc = bdb_queuedb_add(bdb_state, tran, dta, dtalen, bdberr, out_genid);
    } else {
        bdb_lock_table_read(bdb_state, tran);
        rc = bdb_queue_add_int(bdb_state, tran, dta, dtalen, bdberr, out_genid);
    }
    BDB_RELLOCK();

    return rc;
}

static int bdb_queue_add_goose_int(bdb_state_type *bdb_state, tran_type *tran,
                                   int *bdberr)
{
    int rc;
    DBT dbt_key, dbt_data;
    struct bdb_queue_header hdr;
    db_recno_t recno;

    if (!bdb_state->read_write) {
        *bdberr = BDBERR_READONLY;
        return -1;
    }

    *bdberr = BDBERR_NOERROR;

    /* Specifically, it's the zero genid that marks this record as a goose. */
    bzero(&hdr, sizeof(hdr));

    bzero(&dbt_key, sizeof(dbt_key));
    bzero(&dbt_data, sizeof(dbt_data));

    dbt_key.data = &recno;
    dbt_key.size = sizeof(recno);
    dbt_key.ulen = sizeof(recno);
    dbt_key.flags = DB_DBT_USERMEM;

    dbt_data.data = &hdr;
    dbt_data.size = sizeof(struct bdb_queue_header);
    dbt_data.flags = DB_DBT_USERMEM;

    rc = bdb_state->dbp_data[0][0]->put(bdb_state->dbp_data[0][0], tran->tid,
                                        &dbt_key, &dbt_data, DB_APPEND);
    if (rc != 0) {
        switch (rc) {
        case DB_LOCK_DEADLOCK:
            *bdberr = BDBERR_DEADLOCK;
            break;

        default:
            *bdberr = BDBERR_MISC;
            logmsg(LOGMSG_ERROR, "bdb_queue_add_goose_int: put failed %d %s\n", rc,
                    db_strerror(errno));
            break;
        }
        return -1;
    }

    return 0;
}

int bdb_queue_add_goose(bdb_state_type *bdb_state, tran_type *tran, int *bdberr)
{
    int rc = 0;

    BDB_READLOCK("bdb_queue_add_goose");
    if (bdb_state->bdbtype == BDBTYPE_QUEUE) {
        rc = bdb_queue_add_goose_int(bdb_state, tran, bdberr);
    }
    BDB_RELLOCK();

    return rc;
}

static int bdb_queue_check_goose_int(bdb_state_type *bdb_state, tran_type *tran,
                                     int *bdberr)
{
    DB_TXN *tid;
    int tmptid;
    uint8_t hdrbuf[QUEUE_HDR_LEN];
    uint8_t *p_buf = hdrbuf, *p_buf_end = (p_buf + sizeof(hdrbuf));
    DBT dbt_key, dbt_data;
    DBC *dbcp;
    db_recno_t recno;
    struct bdb_queue_header hdr;
    int rc, outrc;

    if (tran) {
        tid = tran->tid;
        tmptid = 0;
    } else if (TXN_READ_HACK()) {
        rc = bdb_state->dbenv->txn_begin(bdb_state->dbenv, NULL, &tid, 0);
        if (rc != 0) {
            logmsg(LOGMSG_ERROR, 
                    ":bdb_queue_check_goose_int txn_begin failed %d %s\n", rc,
                    db_strerror(rc));
            *bdberr = BDBERR_MISC;
            return -1;
        }
        tmptid = 1;
    } else {
        tid = NULL;
        tmptid = 0;
    }

    rc = bdb_state->dbp_data[0][0]->cursor(bdb_state->dbp_data[0][0], tid,
                                           &dbcp, 0);
    if (rc != 0) {
        switch (rc) {
        case DB_REP_HANDLE_DEAD:
        case DB_LOCK_DEADLOCK:
            *bdberr = BDBERR_DEADLOCK;
            break;

        default:
            logmsg(LOGMSG_ERROR, "bdb_queue_check_goose_int: "
                            "cursor failed tid=%p %d %s\n",
                    tid, rc, db_strerror(rc));
            *bdberr = BDBERR_MISC;
            break;
        }
        if (tid && tmptid)
            tid->abort(tid);
        return -1;
    }

    /* Default return is 2 - not found.  If we find a goose then we'll change
     * this to 0. */
    outrc = 2;

    bzero(&dbt_key, sizeof(dbt_key));
    bzero(&dbt_data, sizeof(dbt_data));

    dbt_key.ulen = sizeof(recno);
    dbt_key.data = &recno;
    dbt_key.flags = DB_DBT_USERMEM;

    dbt_data.ulen = sizeof(hdr);
    dbt_data.doff = 0;
    dbt_data.dlen = sizeof(struct bdb_queue_header);
    dbt_data.ulen = dbt_data.dlen;
    dbt_data.data = &hdrbuf;
    dbt_data.flags = DB_DBT_USERMEM | DB_DBT_PARTIAL;

    rc = dbcp->c_get(dbcp, &dbt_key, &dbt_data, DB_FIRST);

    if (rc == 0) {
        if (!(queue_hdr_get(&hdr, p_buf, p_buf_end))) {
            logmsg(LOGMSG_ERROR, "%s: queue_hdr_get failed\n", __FUNCTION__);
            *bdberr = BDBERR_MISC;
            outrc = -1;
        } else if (hdr.genid[0] == 0 && hdr.genid[1] == 0) {
            /* Found a goose! */
            outrc = 0;
        }
    } else if (rc != DB_NOTFOUND) {
        switch (rc) {
        case DB_LOCK_DEADLOCK:
            *bdberr = BDBERR_DEADLOCK;
            break;

        default:
            logmsg(LOGMSG_ERROR, "bdb_queue_check_goose_int: c_get failed %d %s\n",
                    rc, db_strerror(rc));
            *bdberr = BDBERR_MISC;
            break;
        }
        outrc = -1;
    }

    rc = dbcp->c_close(dbcp);
    if (rc != 0) {
        switch (rc) {
        case DB_LOCK_DEADLOCK:
            *bdberr = BDBERR_DEADLOCK;
            break;

        default:
            logmsg(LOGMSG_ERROR, "bdb_queue_check_goose_int: c_close failed %d %s\n",
                    rc, db_strerror(rc));
            *bdberr = BDBERR_MISC;
            break;
        }
        outrc = -1;
    }

    if (tid && tmptid)
        tid->abort(tid);

    return outrc;
}

int bdb_queue_check_goose(bdb_state_type *bdb_state, tran_type *tran,
                          int *bdberr)
{
    int rc = 0;

    BDB_READLOCK("bdb_queue_check_goose");
    if (bdb_state->bdbtype == BDBTYPE_QUEUE) {
        rc = bdb_queue_check_goose_int(bdb_state, tran, bdberr);
    }
    BDB_RELLOCK();

    return rc;
}

/* caller is responsible for rolling back if a non-goose is consumed. */
static int bdb_queue_consume_goose_int(bdb_state_type *bdb_state,
                                       tran_type *tran, int *bdberr)
{
    DBT dbt_key, dbt_data;
    db_recno_t recno;
    uint8_t hdrbuf[QUEUE_HDR_LEN];
    uint8_t *p_buf = hdrbuf, *p_buf_end = (p_buf + sizeof(hdrbuf));
    struct bdb_queue_header hdr;
    int rc;

    if (!bdb_state->read_write) {
        *bdberr = BDBERR_READONLY;
        return -1;
    }

    *bdberr = BDBERR_NOERROR;

    /* Consume the head. */
    bzero(&dbt_key, sizeof(dbt_key));
    bzero(&dbt_data, sizeof(dbt_data));

    dbt_key.data = &recno;
    dbt_key.size = sizeof(recno);
    dbt_key.ulen = sizeof(recno);
    dbt_key.flags = DB_DBT_USERMEM;

    dbt_data.doff = 0;
    dbt_data.dlen = sizeof(struct bdb_queue_header);
    dbt_data.ulen = sizeof(struct bdb_queue_header);
    dbt_data.data = &hdrbuf;
    dbt_data.flags = DB_DBT_USERMEM | DB_DBT_PARTIAL;

    rc = bdb_state->dbp_data[0][0]->get(bdb_state->dbp_data[0][0], tran->tid,
                                        &dbt_key, &dbt_data, DB_CONSUME);

    if (rc != 0) {
        switch (rc) {
        case DB_LOCK_DEADLOCK:
            *bdberr = BDBERR_DEADLOCK;
            break;

        case DB_NOTFOUND:
            *bdberr = BDBERR_DELNOTFOUND;
            break;

        default:
            logmsg(LOGMSG_ERROR, "bdb_queue_consume_goose_int: get failed %d %s\n",
                    rc, db_strerror(rc));
            *bdberr = BDBERR_MISC;
            break;
        }
        return 0;
    } else {
        /* If the record is not a goose then set BDBERR_DELNOTFOUND.
         * The caller should detect this and roll back. */
        /*
        fprintf(stderr, "got recno=%u %08x:%08x size=%u ulen=%u to consume\n",
                recno, hdr.genid[0], hdr.genid[1], dbt_data.size,
                dbt_data.ulen);
        fsnapf(stderr, &hdr, dbt_data.ulen);
        */
        if (!(queue_hdr_get(&hdr, p_buf, p_buf_end))) {
            logmsg(LOGMSG_ERROR, "%s: queue_hdr_get failed\n", __FUNCTION__);
            *bdberr = BDBERR_MISC;
            return -1;
        }

        if (hdr.genid[0] || hdr.genid[1]) {
            *bdberr = BDBERR_DELNOTFOUND;
            return -1;
        }
    }

    return 0;
}

int bdb_queue_consume_goose(bdb_state_type *bdb_state, tran_type *tran,
                            int *bdberr)
{
    int rc = 0;

    BDB_READLOCK("bdb_queue_consume_goose");
    if (bdb_state->bdbtype == BDBTYPE_QUEUE) {
        rc = bdb_queue_consume_goose_int(bdb_state, tran, bdberr);
    }
    BDB_RELLOCK();

    return rc;
}

static int bdb_queue_walk_int(bdb_state_type *bdb_state, int flags,
                              bbuint32_t *lastitem,
                              bdb_queue_walk_callback_t callback, void *userptr,
                              int *bdberr)
{
    DBT dbt_key, dbt_data;
    DBC *dbcp;
    struct bdb_queue_header hdr;
    uint8_t hdrbuf[QUEUE_HDR_LEN];
    uint8_t *p_buf = hdrbuf, *p_buf_end = (p_buf + sizeof(hdrbuf));
    db_recno_t recno;
    bbuint32_t consumer_mask = 0xffffffff;
    int rc;
    int tmptid = 0;
    DB_TXN *tid = NULL;

#if 0
    if(flags & BDB_QUEUE_WALK_KNOWN_CONSUMERS_ONLY)
        consumer_mask &= ~bdb_state->active_consumers;
#endif

    if (TXN_READ_HACK()) {
        rc = bdb_state->dbenv->txn_begin(bdb_state->dbenv, NULL, &tid, 0);
        if (rc != 0) {
            logmsg(LOGMSG_ERROR, "%s: txn_begin failed %d %s\n", __func__, rc,
                    db_strerror(rc));
            *bdberr = BDBERR_MISC;
            return -1;
        }
        tmptid = 1;
    }

    rc = bdb_state->dbp_data[0][0]->cursor(bdb_state->dbp_data[0][0], tid,
                                           &dbcp, 0);
    if (rc != 0) {
        switch (rc) {
        case DB_REP_HANDLE_DEAD:
        case DB_LOCK_DEADLOCK:
            *bdberr = BDBERR_DEADLOCK;
            break;

        default:
            logmsg(LOGMSG_ERROR, "bdb_queue_walk_int: cursor failed %d %s\n", rc,
                    db_strerror(rc));
            *bdberr = BDBERR_MISC;
            break;
        }
        if (tid && tmptid)
            tid->abort(tid);
        return -1;
    }

    /* if doing a restart get back to where we were so that DB_NEXT will put us
     * on the next item. */
    if (flags & BDB_QUEUE_WALK_RESTART) {
        bzero(&dbt_key, sizeof(dbt_key));
        bzero(&dbt_data, sizeof(dbt_data));

        recno = (db_recno_t)(*lastitem);

        dbt_key.ulen = sizeof(recno);
        dbt_key.data = &recno;
        dbt_key.flags = DB_DBT_USERMEM;

        dbt_data.ulen = sizeof(hdr);
        dbt_data.doff = 0;
        dbt_data.dlen = sizeof(struct bdb_queue_header);
        dbt_data.data = &hdrbuf;
        dbt_data.flags = DB_DBT_USERMEM | DB_DBT_PARTIAL;

        rc = dbcp->c_get(dbcp, &dbt_key, &dbt_data, DB_SET);

        if (rc != DB_NOTFOUND && rc != DB_KEYEMPTY) {
            switch (rc) {
            case DB_REP_HANDLE_DEAD:
            case DB_LOCK_DEADLOCK:
                *bdberr = BDBERR_DEADLOCK;
                break;

            default:
                logmsg(LOGMSG_ERROR, 
                        "bdb_queue_walk_int: restart c_get failed %d %s\n", rc,
                        db_strerror(rc));
                *bdberr = BDBERR_MISC;
                break;
            }
            dbcp->c_close(dbcp);
            if (tid && tmptid)
                tid->abort(tid);
            return -1;
        }

        if (!(queue_hdr_get(&hdr, p_buf, p_buf_end))) {
            logmsg(LOGMSG_ERROR, "%s: queue_hdr_get failed\n", __FUNCTION__);
            *bdberr = BDBERR_MISC;
            dbcp->c_close(dbcp);
            if (tid && tmptid)
                tid->abort(tid);
            return -1;
        }
    }

    bzero(&dbt_key, sizeof(dbt_key));
    bzero(&dbt_data, sizeof(dbt_data));

    dbt_key.ulen = sizeof(recno);
    dbt_key.data = &recno;
    dbt_key.flags = DB_DBT_USERMEM;

    dbt_data.ulen = sizeof(hdr);
    dbt_data.doff = 0;
    dbt_data.dlen = sizeof(struct bdb_queue_header);
    dbt_data.ulen = dbt_data.dlen;
    dbt_data.data = &hdrbuf;
    dbt_data.flags = DB_DBT_USERMEM | DB_DBT_PARTIAL;

    rc = dbcp->c_get(dbcp, &dbt_key, &dbt_data, DB_NEXT);
    while (rc == 0) {
        p_buf = hdrbuf;

        if (!(queue_hdr_get(&hdr, p_buf, p_buf_end))) {
            logmsg(LOGMSG_ERROR, "%s: queue_hdr_get failed\n", __FUNCTION__);
            *bdberr = BDBERR_MISC;
            dbcp->c_close(dbcp);
            if (tid && tmptid)
                tid->abort(tid);
            return -1;
        }
        /* Skip zero genid records as those are dummies. */
        if (hdr.fragment_no == 0 && (hdr.consumer_mask & consumer_mask) &&
            (hdr.genid[0] || hdr.genid[1])) {
            int consumern;

            /* Call the callback for each consumer that hasn't yet consumed
             * this message. */
            for (consumern = 0; consumern < 32; consumern++) {
                if (btst(&consumer_mask, consumern) &&
                    btst(&hdr.consumer_mask, consumern)) {
                    int callbackrc =
                        callback(consumern, (size_t)hdr.total_sz,
                                 (unsigned int)hdr.genid[0], /* epoch */
                                 userptr);

                    if (callbackrc == BDB_QUEUE_WALK_STOP) {
                        dbcp->c_close(dbcp);
                        if (tid && tmptid)
                            tid->abort(tid);
                        return 0;
                    } else if (callbackrc == BDB_QUEUE_WALK_STOP_CONSUMER) {
                        /* no more from this consumer, thanks */
                        bclr(&consumer_mask, consumern);
                    } else if (callbackrc == BDB_QUEUE_WALK_CONTINUE) {
                        /* carry on */
                    } else {
                        /* not sure what this means? */
                    }
                }
            }

            if (flags & BDB_QUEUE_WALK_FIRST_ONLY) {
                consumer_mask &= ~hdr.consumer_mask;
                if (consumer_mask == 0)
                    break;
            }
        }

        /* save recno in case we need to restart */
        if (lastitem)
            *lastitem = (bbuint32_t)recno;

        dbt_key.ulen = sizeof(recno);
        dbt_key.data = &recno;
        dbt_key.flags = DB_DBT_USERMEM;

        dbt_data.ulen = sizeof(hdr);
        dbt_data.doff = 0;
        dbt_data.dlen = sizeof(struct bdb_queue_header);
        dbt_data.data = &hdrbuf;
        dbt_data.flags = DB_DBT_USERMEM | DB_DBT_PARTIAL;
        rc = dbcp->c_get(dbcp, &dbt_key, &dbt_data, DB_NEXT);
    }

    if (rc != DB_NOTFOUND) {
        switch (rc) {
        case DB_REP_HANDLE_DEAD:
        case DB_LOCK_DEADLOCK:
            *bdberr = BDBERR_DEADLOCK;
            break;

        default:
            logmsg(LOGMSG_ERROR, "bdb_queue_walk_int: c_get failed %d %s\n", rc,
                    db_strerror(rc));
            *bdberr = BDBERR_MISC;
            break;
        }
        dbcp->c_close(dbcp);
        if (tid && tmptid)
            tid->abort(tid);
        return -1;
    }

    dbcp->c_close(dbcp);
    if (tid && tmptid)
        tid->abort(tid);
    return 0;
}

int bdb_queue_walk(bdb_state_type *bdb_state, int flags, bbuint32_t *lastitem,
                   bdb_queue_walk_callback_t callback, tran_type *tran,
                   void *userptr, int *bdberr)
{
    int rc;
    BDB_READLOCK("bdb_queue_walk");
    /* don't pass this to bdb_queuedb_walk - it needs more than an uint32_t
     * worth of state, caller needs to call it correctly. */
    if (bdb_state->bdbtype == BDBTYPE_QUEUEDB) {
        rc = bdb_queuedb_walk(bdb_state, flags, lastitem, callback, tran, userptr, bdberr);
    } else {
        /* TODO: The "tran" parameter is not passed here.  Maybe it should be? */
        rc = 0; //bdb_queue_walk_int(bdb_state, flags, lastitem, callback, userptr, bdberr);
    }
    BDB_RELLOCK();

    return rc;
}

static int bdb_queue_dump_int(bdb_state_type *bdb_state, FILE *out, int *bdberr)
{

    uint8_t hdrbuf[QUEUE_HDR_LEN];
    uint8_t *p_buf = hdrbuf, *p_buf_end = (p_buf + sizeof(hdrbuf));
    struct bdb_queue_header hdr;
    DBT dbt_key, dbt_data;
    DBC *dbcp;
    db_recno_t recno;
    int rc;
    DB_QUEUE_STAT qst;

    /* First dump statistics for the queue */
    bzero(&qst, sizeof(qst));
    rc = bdb_state->dbp_data[0][0]->stat(bdb_state->dbp_data[0][0], &qst,
                                         DB_FAST_STAT);
    logmsgf(LOGMSG_USER, out, "Queue stats (rcode %d %s):-\n", rc, db_strerror(rc));
    logmsgf(LOGMSG_USER, out, "%16s = %u\n", "qs_magic", (unsigned)qst.qs_magic);
    logmsgf(LOGMSG_USER, out, "%16s = %u\n", "qs_version", (unsigned)qst.qs_version);
    logmsgf(LOGMSG_USER, out, "%16s = %u\n", "qs_nkeys", (unsigned)qst.qs_nkeys);
    logmsgf(LOGMSG_USER, out, "%16s = %u\n", "qs_ndata", (unsigned)qst.qs_ndata);
    logmsgf(LOGMSG_USER, out, "%16s = %u\n", "qs_pagesize", (unsigned)qst.qs_pagesize);
    logmsgf(LOGMSG_USER, out, "%16s = %u\n", "qs_extentsize", (unsigned)qst.qs_extentsize);
    logmsgf(LOGMSG_USER, out, "%16s = %u\n", "qs_pages", (unsigned)qst.qs_pages);
    logmsgf(LOGMSG_USER, out, "%16s = %u\n", "qs_re_len", (unsigned)qst.qs_re_len);
    logmsgf(LOGMSG_USER, out, "%16s = %u\n", "qs_re_pad", (unsigned)qst.qs_re_pad);
    logmsgf(LOGMSG_USER, out, "%16s = %u\n", "qs_pgfree", (unsigned)qst.qs_pgfree);
    logmsgf(LOGMSG_USER, out, "%16s = %u\n", "qs_first_recno", (unsigned)qst.qs_first_recno);
    logmsgf(LOGMSG_USER, out, "%16s = %u\n", "qs_cur_recno", (unsigned)qst.qs_cur_recno);
    logmsgf(LOGMSG_USER, out, "-----\n");

    rc = bdb_state->dbp_data[0][0]->cursor(bdb_state->dbp_data[0][0], NULL,
                                           &dbcp, 0);
    if (rc != 0) {
        switch (rc) {
        case DB_REP_HANDLE_DEAD:
        case DB_LOCK_DEADLOCK:
            *bdberr = BDBERR_DEADLOCK;
            break;

        default:
            logmsg(LOGMSG_ERROR, "bdb_queue_dump_int: cursor failed %d %s\n", rc,
                    db_strerror(rc));
            *bdberr = BDBERR_MISC;
            break;
        }
        return -1;
    }

    bzero(&dbt_key, sizeof(dbt_key));
    bzero(&dbt_data, sizeof(dbt_data));

    dbt_key.ulen = sizeof(recno);
    dbt_key.data = &recno;
    dbt_key.flags = DB_DBT_USERMEM;

    dbt_data.ulen = sizeof(hdr);
    dbt_data.doff = 0;
    dbt_data.dlen = sizeof(struct bdb_queue_header);
    dbt_data.data = &hdrbuf;
    dbt_data.flags = DB_DBT_USERMEM | DB_DBT_PARTIAL;

    rc = dbcp->c_get(dbcp, &dbt_key, &dbt_data, DB_NEXT);
    while (rc == 0) {
        p_buf = hdrbuf;

        if (!(queue_hdr_get(&hdr, p_buf, p_buf_end))) {
            logmsg(LOGMSG_ERROR, "%s: queue_hdr_get failed\n", __FUNCTION__);
            *bdberr = BDBERR_MISC;
            dbcp->c_close(dbcp);
            return -1;
        }

        /* found an item - dump it to output */
        logmsgf(LOGMSG_USER, out, "recno %10u fragno %3u nfrag %3u genid %08x:%08x "
                     "sz %6u fragsz %6u mask 0x%08x\n",
                (unsigned)recno, (unsigned)hdr.fragment_no,
                (unsigned)hdr.num_fragments, (unsigned)hdr.genid[0],
                (unsigned)hdr.genid[1], (unsigned)hdr.total_sz,
                (unsigned)hdr.fragment_sz, (unsigned)hdr.consumer_mask);

        dbt_key.ulen = sizeof(recno);
        dbt_key.data = &recno;
        dbt_key.flags = DB_DBT_USERMEM;

        dbt_data.ulen = sizeof(hdr);
        dbt_data.doff = 0;
        dbt_data.dlen = sizeof(struct bdb_queue_header);
        dbt_data.data = &hdrbuf;
        dbt_data.flags = DB_DBT_USERMEM | DB_DBT_PARTIAL;
        rc = dbcp->c_get(dbcp, &dbt_key, &dbt_data, DB_NEXT);
    }
    logmsgf(LOGMSG_USER, out, "-----\n");

    if (rc != DB_NOTFOUND) {
        switch (rc) {
        case DB_REP_HANDLE_DEAD:
        case DB_LOCK_DEADLOCK:
            *bdberr = BDBERR_DEADLOCK;
            break;

        default:
            *bdberr = BDBERR_MISC;
            break;
        }
        logmsg(LOGMSG_ERROR, "bdb_queue_dump_int: c_get failed %d %s\n", rc,
                db_strerror(rc));
        dbcp->c_close(dbcp);
        return -1;
    }

    dbcp->c_close(dbcp);

    return 0;
}

int bdb_queue_dump(bdb_state_type *bdb_state, FILE *out, int *bdberr)
{
    int rc;

    BDB_READLOCK("bdb_queue_dump");
    if (bdb_state->bdbtype == BDBTYPE_QUEUEDB)
        rc = bdb_queuedb_dump(bdb_state, out, bdberr);
    else
        rc = bdb_queue_dump_int(bdb_state, out, bdberr);
    BDB_RELLOCK();

    return rc;
}

/* There's stuff in the queue that we don't have an active
 * consumer for.  We will alarm on this as such items can lead
 * to queue extents never being freed and degraded performance.
 */
static void lost_consumers_alarm(bdb_state_type *bdb_state,
                                 uint32_t lost_consumers)
{
    static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
    uint32_t new_lost_consumers;
    int ii, now = comdb2_time_epoch();

    /* Don't repeat alarms too often */
    if (bdb_state->qpriv->known_lost_consumers == lost_consumers &&
        bdb_state->qpriv->last_orphaned_consumers_alarm == now)
        return;

    Pthread_mutex_lock(&mutex);

    if (bdb_state->qpriv->last_orphaned_consumers_alarm != now) {
        new_lost_consumers = lost_consumers;
    } else {
        new_lost_consumers =
            lost_consumers & ~bdb_state->qpriv->known_lost_consumers;
    }

    if (new_lost_consumers) {
        for (ii = 0; ii < BDBQUEUE_MAX_CONSUMERS; ii++) {
            if (btst(&new_lost_consumers, ii)) {
                logmsg(LOGMSG_WARN, "QUEUE %s CONTAINS ORPHANED MESSAGES FOR "
                                "CONSUMER NUMBER %d\n",
                        bdb_state->name, ii);
            }
        }
        bdb_state->qpriv->known_lost_consumers |= lost_consumers;
        bdb_state->qpriv->known_lost_consumers &= ~bdb_state->active_consumers;
        bdb_state->qpriv->last_orphaned_consumers_alarm = now;
    }

    Pthread_mutex_unlock(&mutex);
}

static int bdb_queue_get_int(bdb_state_type *bdb_state, int consumer,
                             const struct bdb_queue_cursor *prevcursor,
                             void **fnd, size_t *fnddtalen, size_t *fnddtaoff,
                             struct bdb_queue_cursor *fndcursor,
                             int *bdberr)
{
    DBT dbt_key, dbt_data;
    DBC *dbcp;

    uint8_t hdrbuf[QUEUE_HDR_LEN];
    uint8_t *p_buf = hdrbuf, *p_buf_end = (p_buf + sizeof(hdrbuf));
    uint8_t *p_item_buf, *p_item_buf_start, *p_item_buf_end;

    struct bdb_queue_header hdr;
    int getop;
    db_recno_t recno;
    db_recno_t *recnos;
    struct bdb_queue_found item = {0};
    size_t item_len;
    char *dtaptr;
    size_t next_frag;
    int rc;
    unsigned long long lastgenid = 0;
    int tmptid = 0;
    DB_TXN *tid = NULL;
    int scanmode = 0;

    if (consumer < 0 || consumer >= BDBQUEUE_MAX_CONSUMERS) {
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    bdb_state->qpriv->stats.n_logical_gets++;

    if (TXN_READ_HACK()) {
        rc = bdb_state->dbenv->txn_begin(bdb_state->dbenv, NULL, &tid, 0);
        if (rc != 0) {
            logmsg(LOGMSG_ERROR, "bdb_queue_get_int: txn_begin failed %d %s\n", rc,
                    db_strerror(rc));
            *bdberr = BDBERR_MISC;
            return -1;
        }
        tmptid = 1;
    }

    rc = bdb_state->dbp_data[0][0]->cursor(bdb_state->dbp_data[0][0], tid,
                                           &dbcp, 0);
    if (rc != 0) {
        switch (rc) {
        case DB_REP_HANDLE_DEAD:
        case DB_LOCK_DEADLOCK:
            bdb_state->qpriv->stats.n_get_deadlocks++;
            *bdberr = BDBERR_DEADLOCK;
            break;

        default:
            logmsg(LOGMSG_ERROR, "bdb_queue_get_int: cursor failed %d %s\n", rc,
                    db_strerror(rc));
            *bdberr = BDBERR_MISC;
            break;
        }
        if (tid && tmptid)
            tid->abort(tid);
        return -1;
    }

lookagain:

    bzero(&dbt_key, sizeof(dbt_key));
    bzero(&dbt_data, sizeof(dbt_data));

    dbt_key.ulen = sizeof(recno);
    dbt_key.data = &recno;
    dbt_key.flags = DB_DBT_USERMEM;
    /* If we found something before, look for the first key that comes after */
    if (scanmode) {
        /* Try next recno until we hit something valid */
        int tmprec = recno + 1;
        dbt_key.size = sizeof(recno);
        recno = tmprec;
        getop = DB_SET;
    } else if (prevcursor) {
        /* I had the very silly idea that if you know the previous found item
         * you can start the search from the recno of its last fragment.  This
         * is silly because its last fragment may come after the first fragment
         * of what is logically the next item queued (if they were added at the
         * same time then their fragments may interleave). */
        memcpy(&lastgenid, &prevcursor->genid, 8);
        if (lastgenid == 0) {
            prevcursor = NULL;
            getop = DB_FIRST;
        } else {
            dbt_key.size = sizeof(recno);
            recno = prevcursor->recno;
            getop = DB_SET;
        }
    }
    /* Otherwise start at the front. */
    else {
        getop = DB_FIRST;
    }

    /* Do a partial get - just get the header. */
    dbt_data.ulen = sizeof(hdr);
    dbt_data.doff = 0;
    dbt_data.dlen = sizeof(struct bdb_queue_header);
    dbt_data.data = &hdrbuf;
    dbt_data.flags = DB_DBT_USERMEM | DB_DBT_PARTIAL;

    rc = dbcp->c_get(dbcp, &dbt_key, &dbt_data, getop);
    bdb_state->qpriv->stats.n_physical_gets++;
    if (rc != 0) {
        switch (rc) {
        case DB_REP_HANDLE_DEAD:
        case DB_LOCK_DEADLOCK:
            bdb_state->qpriv->stats.n_get_deadlocks++;
            *bdberr = BDBERR_DEADLOCK;
            break;

        case DB_NOTFOUND:
            if (!prevcursor || scanmode) {
                /* Queue must be empty */
                *bdberr = BDBERR_FETCH_DTA;
                break;
            } else {
                /* Go back to the beginnining; the previous cursor was out
                 * of range for the queue. */
                prevcursor = NULL;
                goto lookagain;
            }

        case DB_KEYEMPTY:
            if (prevcursor) {
                if (bdb_state->attr->qscanmode) {
                    /* The previous recno was in range for the queue but
                     * has been deleted.  The old code just sent us back to
                     * the start of the queue.  This was expensive if the
                     * queue is very long due to some unconsumed items at
                     * the head.  Now instead we can scan forwards,
                     * incrementing recno until we hit something we can
                     * read (or we hit the end of the queue). */
                    scanmode = 1;
                } else {
                    /* Previously found item wasn't there so start again
                     * from the beginning. */
                    prevcursor = NULL;
                }
                goto lookagain;
            }
        /* Fall through - we don't expect this! */

        default:
            logmsg(LOGMSG_ERROR, "bdb_queue_get_int: first c_get failed %d %s\n", rc,
                    db_strerror(rc));
            *bdberr = BDBERR_MISC;
            break;
        }
        dbcp->c_close(dbcp);
        if (tid && tmptid)
            tid->abort(tid);
        return -1;
    }

    if (!(queue_hdr_get(&hdr, p_buf, p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s: queue_hdr_get failed\n", __FUNCTION__);
        *bdberr = BDBERR_MISC;
        dbcp->c_close(dbcp);
        if (tid && tmptid)
            tid->abort(tid);
        return -1;
    }

    /* Keep hunting until we find the first part of an unconsumed item that is
     * not the item we found before (which may not have been consumed yet) */
    while (memcmp(&lastgenid, hdr.genid, 8) == 0 ||
           !btst(&hdr.consumer_mask, consumer) || hdr.fragment_no != 0 ||
           (hdr.genid[0] == 0 && hdr.genid[1] == 0)) {
        uint32_t lost_consumers =
            hdr.consumer_mask & (~bdb_state->active_consumers);
        if (lost_consumers && hdr.fragment_no == 0 &&
            (hdr.genid[0] || hdr.genid[1])) {
            lost_consumers_alarm(bdb_state, lost_consumers);
        }

        if (bdb_lock_desired(bdb_state)) {
            *bdberr = BDBERR_LOCK_DESIRED;
            dbcp->c_close(dbcp);
            if (tid && tmptid)
                tid->abort(tid);
            return -1;
        }

        rc = dbcp->c_get(dbcp, &dbt_key, &dbt_data, DB_NEXT);
        bdb_state->qpriv->stats.n_physical_gets++;
        if (rc != 0) {
            switch (rc) {
            case DB_REP_HANDLE_DEAD:
            case DB_LOCK_DEADLOCK:
                bdb_state->qpriv->stats.n_get_deadlocks++;
                *bdberr = BDBERR_DEADLOCK;
                break;

            case DB_NOTFOUND:
                /* Queue must be empty */
                *bdberr = BDBERR_FETCH_DTA;
                break;

            default:
                logmsg(LOGMSG_ERROR, 
                        "bdb_queue_get_int: second c_get failed %d %s\n", rc,
                        db_strerror(rc));
                *bdberr = BDBERR_MISC;
                break;
            }
            dbcp->c_close(dbcp);
            if (tid && tmptid)
                tid->abort(tid);
            return -1;
        }

        p_buf = hdrbuf;

        if (!(queue_hdr_get(&hdr, p_buf, p_buf_end))) {
            logmsg(LOGMSG_ERROR, "%s: queue_hdr_get failed\n", __FUNCTION__);
            *bdberr = BDBERR_MISC;
            dbcp->c_close(dbcp);
            if (tid && tmptid)
                tid->abort(tid);
            return -1;
        }
    }

    /* Allocate memory for this item */
    item_len = hdr.total_sz + sizeof(struct bdb_queue_found) +
               sizeof(db_recno_t) * hdr.num_fragments;
    p_item_buf_start = p_item_buf = malloc(item_len);
    if (!p_item_buf) {
        logmsg(LOGMSG_ERROR, "bdb_queue_get_int: malloc %zu bytes failed\n",
               item_len);
        dbcp->c_close(dbcp);
        if (tid && tmptid)
            tid->abort(tid);
        return -1;
    }
    p_item_buf_end = p_item_buf + item_len;
    bzero(p_item_buf, item_len - hdr.total_sz);
    memcpy(&item.genid, hdr.genid, sizeof(item.genid));
    item.epoch = hdr.genid[0]; /* assume that genid starts with an epoch */
    item.data_len = hdr.total_sz;
    item.data_offset =
        sizeof(struct bdb_queue_found) + sizeof(db_recno_t) * hdr.num_fragments;
    item.trans.num_fragments = hdr.num_fragments;
    dtaptr = ((char *)p_item_buf) + item.data_offset;

    if (!(p_item_buf = queue_found_put(&item, p_item_buf, p_item_buf_end))) {

        logmsg(LOGMSG_ERROR, "%s line %d: queue_found_get returns NULL\n", __func__,
                __LINE__);
        *bdberr = BDBERR_MISC;
        dbcp->c_close(dbcp);
        if (tid && tmptid)
            tid->abort(tid);
        free(p_item_buf_start);
        return -1;
    }
    recnos = (db_recno_t *)p_item_buf;

    next_frag = 0;
    while (next_frag < item.trans.num_fragments) {
        if (bdb_lock_desired(bdb_state)) {
            *bdberr = BDBERR_LOCK_DESIRED;
            dbcp->c_close(dbcp);
            if (tid && tmptid)
                tid->abort(tid);
            free(p_item_buf_start);
            return -1;
        }

        /* We have a fragment - get its data */
        recnos[next_frag] = htonl(recno);

        bzero(&dbt_key, sizeof(dbt_key));
        dbt_key.data = &recno;
        dbt_key.size = sizeof(recno);
        dbt_key.ulen = sizeof(recno);
        dbt_key.flags = DB_DBT_USERMEM;

        bzero(&dbt_data, sizeof(dbt_data));
        dbt_data.data = dtaptr;
        dbt_data.ulen = hdr.fragment_sz;
        dbt_data.doff = sizeof(struct bdb_queue_header);
        dbt_data.dlen = hdr.fragment_sz;
        dbt_data.flags = DB_DBT_USERMEM | DB_DBT_PARTIAL;

        rc = dbcp->c_get(dbcp, &dbt_key, &dbt_data, DB_SET);
        bdb_state->qpriv->stats.n_physical_gets++;
        if (rc != 0) {
            switch (rc) {
            case DB_REP_HANDLE_DEAD:
            case DB_LOCK_DEADLOCK:
                bdb_state->qpriv->stats.n_get_deadlocks++;
            /* fall through */
            case DB_NOTFOUND:
            case DB_KEYEMPTY:
                /* We just found something for this key so if it isn't
                 * there now then it must have been pulled from under
                 * our feet. */
                *bdberr = BDBERR_DEADLOCK;
                break;

            default:
                logmsg(LOGMSG_ERROR, "bdb_queue_get_int: "
                                "data c_get failed %d %s\n",
                        rc, db_strerror(rc));
                *bdberr = BDBERR_MISC;
                break;
            }
            dbcp->c_close(dbcp);
            if (tid && tmptid)
                tid->abort(tid);
            free(p_item_buf_start);
            return -1;
        }

        dtaptr += hdr.fragment_sz;
        next_frag++;

        /* If there are more fragments then we must walk the queue until we
         * find them. */
        if (next_frag < item.trans.num_fragments) {
            do {
                bzero(&dbt_key, sizeof(dbt_key));
                bzero(&dbt_data, sizeof(dbt_data));

                dbt_key.ulen = sizeof(recno);
                dbt_key.data = &recno;
                dbt_key.flags = DB_DBT_USERMEM;

                dbt_data.ulen = sizeof(hdr);
                dbt_data.doff = 0;
                dbt_data.dlen = sizeof(struct bdb_queue_header);
                dbt_data.data = &hdrbuf;
                dbt_data.flags = DB_DBT_USERMEM | DB_DBT_PARTIAL;

                rc = dbcp->c_get(dbcp, &dbt_key, &dbt_data, DB_NEXT);
                bdb_state->qpriv->stats.n_physical_gets++;
                if (rc != 0) {
                    switch (rc) {
                        size_t fragn;

                    case DB_NOTFOUND:
                    case DB_KEYEMPTY:
                        /* We expect to find the remaining fragments for
                         * this item so if they're not there then there
                         * must have been a write (specifically, the
                         * item must have been deleted by another thread
                         * since we started the scan).  Aternatively, and
                         * much worse, the queue may be corrupt.. */
                        logmsg(LOGMSG_ERROR,
                               "%s: item 0x%llx found only %zu/%u fragments "
                               "(recnos",
                               __func__, item.genid, next_frag + 1,
                               item.trans.num_fragments);
                        for (fragn = 0; fragn <= next_frag; fragn++) {
                            if (fragn % 5 == 0)
                               logmsg(LOGMSG_ERROR, "\n");
                           logmsg(LOGMSG_ERROR, " %u", (unsigned)ntohl(recnos[fragn]));
                        }
                       logmsg(LOGMSG_ERROR, ")\n");

                    case DB_REP_HANDLE_DEAD:
                    case DB_LOCK_DEADLOCK:
                        bdb_state->qpriv->stats.n_get_deadlocks++;
                        *bdberr = BDBERR_DEADLOCK;
                        break;

                    default:
                        logmsg(LOGMSG_ERROR, "bdb_queue_get_int: "
                                        "scan c_get failed %d %s\n",
                                rc, db_strerror(rc));
                        *bdberr = BDBERR_MISC;
                        break;
                    }
                    dbcp->c_close(dbcp);
                    if (tid && tmptid)
                        tid->abort(tid);
                    free(p_item_buf_start);
                    return -1;
                }

                p_buf = hdrbuf;

                if (!(queue_hdr_get(&hdr, p_buf, p_buf_end))) {
                    logmsg(LOGMSG_ERROR, "%s: queue_hdr_get failed\n", __FUNCTION__);
                    *bdberr = BDBERR_MISC;
                    dbcp->c_close(dbcp);
                    if (tid && tmptid)
                        tid->abort(tid);
                    free(p_item_buf_start);
                    return -1;
                }
            } while (memcmp(hdr.genid, &item.genid, sizeof(item.genid)) != 0 ||
                     hdr.fragment_no != next_frag);
        }
    }

    /* For crying out loud, CLOSE THE CURSOR!! */
    rc = dbcp->c_close(dbcp);
    if (tid && tmptid) {
        tid->abort(tid);
        tid = NULL;
    }
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "bdb_queue_get_int: cursor close failed %d %s\n", rc,
                db_strerror(rc));
        *bdberr = BDBERR_MISC;
        free(p_item_buf_start);
        return -1;
    }

    /* Success - found all fragments and stitched them up! */
    if (fndcursor) {
        memcpy(&fndcursor->genid, &item.genid, 8);
        fndcursor->recno = ntohl(recnos[0]);
        fndcursor->reserved = 0;
    }
    if (fnddtalen)
        *fnddtalen = item.data_len;
    if (fnddtaoff)
        *fnddtaoff = item.data_offset;
    if (fnd)
        *fnd = p_item_buf_start;
    else
        free(p_item_buf_start);

    return 0;
}

/* get the first item unconsumed by this consumer number, AFTER the passed in
 * key (pass in a zero key to get the first unconsumed item).  the caller is
 * responsible for freeing *fnddta. */
int bdb_queue_get(bdb_state_type *bdb_state, tran_type *tran, int consumer,
                  const struct bdb_queue_cursor *prevcursor,
                  struct bdb_queue_found **fnd, size_t *fnddtalen,
                  size_t *fnddtaoff, struct bdb_queue_cursor *fndcursor,
                  long long *seq, int *bdberr)
{
    int rc;

    BDB_READLOCK("bdb_queue_get");
    if (bdb_state->bdbtype == BDBTYPE_QUEUEDB)
        rc = bdb_queuedb_get(bdb_state, tran, consumer, prevcursor, fnd,
                             fnddtalen, fnddtaoff, fndcursor, seq, bdberr);
    else
        rc = bdb_queue_get_int(bdb_state, consumer, prevcursor, (void **)fnd, fnddtalen,
                               fnddtaoff, fndcursor, bdberr);
    BDB_RELLOCK();

    return rc;
}

static int bdb_queue_consume_int(bdb_state_type *bdb_state, tran_type *intran,
                                 int consumer, const void *prevfnd, int *bdberr)
{
    struct bdb_queue_found item;
    uint8_t hdrbuf[QUEUE_HDR_LEN];
    uint8_t *p_buf = hdrbuf, *p_buf_end = (p_buf + QUEUE_HDR_LEN);
    uint8_t *p_item_buf, *p_item_buf_end;
    size_t fragn;
    int rc;
    tran_type *tran;
    DBT dbt_key, dbt_data;
    db_recno_t recno;
    db_recno_t *recnos;
    struct bdb_queue_header hdr;

    if (gbl_rowlocks) {
        get_physical_transaction(bdb_state, intran, &tran, 0);
    } else
        tran = intran;

    if (!bdb_state->read_write) {
        *bdberr = BDBERR_READONLY;
        return -1;
    }

    if (!prevfnd) {
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    if (consumer < 0 || consumer >= BDBQUEUE_MAX_CONSUMERS) {
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    *bdberr = BDBERR_NOERROR;

    p_item_buf = (uint8_t *)prevfnd;
    p_item_buf_end = (p_item_buf + QUEUE_FOUND_LEN);

    if (!(p_item_buf =
              (uint8_t *)queue_found_get(&item, p_item_buf, p_item_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s line %d: queue_found_get returns NULL\n", __func__,
                __LINE__);
        *bdberr = BDBERR_MISC;
        return -1;
    }

    /* Find the first fragment (header only). */
    recnos = (db_recno_t *)p_item_buf;
    recno = ntohl(recnos[0]);
    bzero(&dbt_key, sizeof(dbt_key));
    dbt_key.size = sizeof(db_recno_t);
    dbt_key.ulen = sizeof(db_recno_t);
    dbt_key.data = &recno;
    dbt_key.flags = DB_DBT_USERMEM;

    bzero(&dbt_data, sizeof(dbt_data));
    dbt_data.data = hdrbuf;
    dbt_data.ulen = sizeof(hdr);
    dbt_data.doff = 0;
    dbt_data.dlen = sizeof(hdr);
    dbt_data.flags = DB_DBT_USERMEM | DB_DBT_PARTIAL;

    rc = bdb_state->dbp_data[0][0]->get(bdb_state->dbp_data[0][0], tran->tid,
                                        &dbt_key, &dbt_data, DB_RMW);
    if (rc != 0) {
        switch (rc) {
        case DB_REP_HANDLE_DEAD:
        case DB_LOCK_DEADLOCK:
            *bdberr = BDBERR_DEADLOCK;
            bdb_state->qpriv->stats.n_consume_deadlocks++;
            break;

        case DB_KEYEMPTY:
        case DB_NOTFOUND:
            *bdberr = BDBERR_DELNOTFOUND;
            break;

        default:
            logmsg(LOGMSG_ERROR, "bdb_queue_consume_int: get failed %d %s\n", rc,
                    db_strerror(rc));
            *bdberr = BDBERR_MISC;
            break;
        }
        return -1;
    }

    if (!queue_hdr_get(&hdr, p_buf, p_buf_end)) {
        logmsg(LOGMSG_ERROR, "%s: queue_hdr_get failed\n", __FUNCTION__);
        *bdberr = BDBERR_MISC;
        return -1;
    }

    /* Genid must match what was found before. */
    if (memcmp(hdr.genid, &item.genid, sizeof(item.genid)) != 0) {
        *bdberr = BDBERR_DELNOTFOUND;
        return -1;
    }

    /* Consume for this consumer. */
    bclr(&hdr.consumer_mask, consumer);

    if (hdr.consumer_mask == 0) {
        /* All consumers have had this item - remove it.
         * The simple way to do this is to loop through the fragments and
         * delete them using regular berkeley delete.  However this isn't
         * guaranteed to advance the queue head - leading to the ridiculous
         * goose system hack.
         * So we try using efficient berkeley queue consume in a child
         * transaction.  If that starts consuming the wrong stuff (i.e. some
         * other item) then we fall back to old way.
         */
        int consume_from_head = bdb_state->attr->newqdelmode;
        DB_TXN *child_tid = NULL;
        DB_TXN *tid = tran->tid;

        unsigned n_consumed_new_way = 0;
        unsigned n_consumed_old_way = 0;
        unsigned n_geese_consumed = 0;

        fragn = 0;

        if (consume_from_head) {
            int wrong_item = 0;

            /* Start child txn */
            rc = bdb_state->dbenv->txn_begin(bdb_state->dbenv, tid, &child_tid,
                                             0);
            if (rc == DB_LOCK_DEADLOCK) {
                *bdberr = BDBERR_DEADLOCK;
                bdb_state->qpriv->stats.n_consume_deadlocks++;
                return -1;
            } else if (rc != 0) {
                logmsg(LOGMSG_ERROR, "%s: txn_begin %d %s\n", __func__, rc,
                        db_strerror(rc));
                *bdberr = BDBERR_MISC;
                return -1;
            }
            tid = child_tid;

            /* Loop through contigious fragments */
            while (fragn < item.trans.num_fragments) {
                if (fragn > 0 &&
                    ntohl(recnos[fragn]) != ntohl(recnos[fragn - 1] + 1)) {
                    /* This fragment is not contigious with the last so we
                     * cannot expect to get it from a head consume. */
                    break;
                }

                bzero(&dbt_key, sizeof(dbt_key));
                bzero(&dbt_data, sizeof(dbt_data));

                dbt_key.data = &recno;
                dbt_key.size = sizeof(recno);
                dbt_key.ulen = sizeof(recno);
                dbt_key.flags = DB_DBT_USERMEM;

                dbt_data.data = &hdrbuf;
                dbt_data.doff = 0;
                dbt_data.dlen = sizeof(struct bdb_queue_header);
                dbt_data.ulen = sizeof(struct bdb_queue_header);
                /*dbt_data.data = &hdr;*/
                dbt_data.flags = DB_DBT_USERMEM | DB_DBT_PARTIAL;

                /* DB_CONSUME
                 * Return the record number and data from the available
                 * record closest to the head of the queue, and delete the
                 * record. The cursor will be positioned on the deleted
                 * record. The record number will be returned in key, as
                 * described in DBT. The data will be returned in the data
                 * parameter. A record is available if it is not deleted
                 * and is not currently locked. The underlying database
                 * must be of type Queue for DB_CONSUME to be specified.
                 * --
                 * So.. if the head record is locked, we get the next one
                 * (which may be the wrong one) or if there isn't a next
                 * one then we don't get anything at all.  This is really
                 * a kind of deadlock situation.. the best approach
                 * */
                rc = bdb_state->dbp_data[0][0]->get(bdb_state->dbp_data[0][0],
                                                    tid, &dbt_key, &dbt_data,
                                                    DB_CONSUME);

                if (rc == DB_LOCK_DEADLOCK) {
                    *bdberr = BDBERR_DEADLOCK;
                    break;
                } else if (rc == DB_NOTFOUND) {
                    /* See note above - if the head record which we need is
                     * locked and there aren't any further records, then
                     * we may get DB_NOTFOUND even though there are records.
                     * We could treat this as a deadlock or fallback to old
                     * method.  Let's try old method. */
                    bdb_state->qpriv->stats.n_get_not_founds++;
                    break;
                } else if (rc != 0) {
                    logmsg(LOGMSG_ERROR, "%s: DB_CONSUME get failed %d %s\n",
                            __func__, rc, db_strerror(rc));
                    *bdberr = BDBERR_MISC;
                    break;
                }

                p_buf = hdrbuf;

                if (!queue_hdr_get(&hdr, p_buf, p_buf_end)) {
                    logmsg(LOGMSG_ERROR, "%s: queue_hdr_get failed\n", __FUNCTION__);
                    *bdberr = BDBERR_MISC;
                    return -1;
                }

                /* If we got a goose then try again.  Put a limiter on it
                 * - if this queue is nothing but geese then break out and
                 * revert to old way. */
                if (hdr.genid[0] == 0 && hdr.genid[1] == 0) {
                    n_geese_consumed++;
                    if (n_geese_consumed >= 60) {
                        logmsg(LOGMSG_ERROR, "%s:too many geese in this queue!\n",
                                __func__);
                        break;
                    }
                    continue;
                }

                /* Make sure that it is the genid and recno that we expected. */
                if (memcmp(hdr.genid, &item.genid,
                           sizeof(unsigned long long)) != 0 ||
                    recno != ntohl(recnos[fragn])) {
                    /* It's not.. we will have to retry this old way. */
                    wrong_item = 1;
                    break;
                }

                /* Success */
                fragn++;
                n_consumed_new_way++;
            }

            /* If we hit an error then abort the child.  A deadlock has to
             * be bubbled down; any other error and we revert to old way. */
            if (wrong_item || *bdberr != BDBERR_NOERROR) {
                rc = child_tid->abort(child_tid);
                if (rc == DB_LOCK_DEADLOCK) {
                    *bdberr = BDBERR_DEADLOCK;
                } else if (rc != 0) {
                    logmsg(LOGMSG_ERROR, "%s: child_tid->abort %d %s\n", __func__,
                            rc, db_strerror(rc));
                    *bdberr = BDBERR_MISC;
                    return -1;
                }

                tid = tran->tid;
                child_tid = NULL;

                if (*bdberr == BDBERR_DEADLOCK) {
                    bdb_state->qpriv->stats.n_consume_deadlocks++;
                    return -1;
                }

                /* Do all fragments the old way */
                fragn = 0;
                bdb_state->qpriv->stats.n_new_way_geese_consumed +=
                    n_geese_consumed;
                bdb_state->qpriv->stats.n_new_way_frags_aborted +=
                    n_consumed_new_way;
                n_consumed_new_way = 0;
            }
        }

        /* Do remaining (or all) fragments old way */
        for (; fragn < item.trans.num_fragments; fragn++) {
            recno = ntohl(recnos[fragn]);
            bzero(&dbt_key, sizeof(dbt_key));
            dbt_key.size = sizeof(db_recno_t);
            dbt_key.data = &recno;
            dbt_key.flags = DB_DBT_USERMEM;

            rc = bdb_state->dbp_data[0][0]->del(bdb_state->dbp_data[0][0], tid,
                                                &dbt_key, 0);
            if (rc != 0) {
                switch (rc) {
                case DB_REP_HANDLE_DEAD:
                case DB_LOCK_DEADLOCK:
                    bdb_state->qpriv->stats.n_consume_deadlocks++;
                    *bdberr = BDBERR_DEADLOCK;
                    break;

                default:
                    logmsg(LOGMSG_ERROR, "bdb_queue_consume_int: "
                                    "del failed (recno %u) %d %s\n",
                            (unsigned)recno, rc, db_strerror(rc));
                    *bdberr = BDBERR_MISC;
                    break;
                }
                return -1;
            }
            n_consumed_old_way++;
        }

        if (child_tid) {
            /* Commit child; if this fails report overall failure */
            rc = child_tid->commit(child_tid, 0);
            if (rc == DB_LOCK_DEADLOCK) {
                *bdberr = BDBERR_DEADLOCK;
                bdb_state->qpriv->stats.n_consume_deadlocks++;
                return -1;
            } else if (rc != 0) {
                logmsg(LOGMSG_ERROR, "%s: child_tid->commit %d %s\n", __func__, rc,
                        db_strerror(rc));
                *bdberr = BDBERR_MISC;
                return -1;
            }
            bdb_state->qpriv->stats.n_new_way_geese_consumed +=
                n_geese_consumed;
            bdb_state->qpriv->stats.n_new_way_frags_consumed +=
                n_consumed_new_way;
            bdb_state->qpriv->stats.n_old_way_frags_consumed +=
                n_consumed_old_way;
        }
    } else {
        /* More consumers are waiting on this guy - put the header back. */
        p_buf = hdrbuf;
        if (!queue_hdr_put(&hdr, p_buf, p_buf_end)) {
            logmsg(LOGMSG_ERROR, "%s: queue_hdr_put failed\n", __FUNCTION__);
            *bdberr = BDBERR_MISC;
            return -1;
        }

        rc = bdb_state->dbp_data[0][0]->put(bdb_state->dbp_data[0][0],
                                            tran->tid, &dbt_key, &dbt_data, 0);
        if (rc != 0) {
            switch (rc) {
            case DB_REP_HANDLE_DEAD:
            case DB_LOCK_DEADLOCK:
                bdb_state->qpriv->stats.n_consume_deadlocks++;
                *bdberr = BDBERR_DEADLOCK;
                break;

            default:
                logmsg(LOGMSG_ERROR, "bdb_queue_consume_int: put failed %d %s\n", rc,
                        db_strerror(rc));
                *bdberr = BDBERR_MISC;
                break;
            }
            return -1;
        }
    }

    bdb_state->qdb_cons++;
    return 0;
}

/* consume a queue item previously found by bdb_queue_get. */
int bdb_queue_consume(bdb_state_type *bdb_state, tran_type *tran, int consumer,
                      const struct bdb_queue_found *prevfnd, int *bdberr)
{
    int rc;
    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK("bdb_queue_consume");
    if (bdb_state->bdbtype == BDBTYPE_QUEUEDB)
        rc = bdb_queuedb_consume(bdb_state, tran, consumer, prevfnd, bdberr);
    else
        rc = bdb_queue_consume_int(bdb_state, tran, consumer, prevfnd, bdberr);
    BDB_RELLOCK();

    return rc;
}

void bdb_queue_get_found_info(const void *fnd, size_t *dtaoff, size_t *dtalen)
{
    struct bdb_queue_found found;
    if (fnd) {
        uint8_t *p_buf = (uint8_t *)fnd;
        uint8_t *p_buf_end = (p_buf + QUEUE_FOUND_LEN);

        if (!queue_found_get(&found, p_buf, p_buf_end)) {
            logmsg(LOGMSG_ERROR, "%s line %d: queue_found_get returns NULL\n",
                    __func__, __LINE__);
            return;
        }

        if (dtaoff)
            *dtaoff = found.data_offset;
        if (dtalen)
            *dtalen = found.data_len;
    }
}

const struct bdb_queue_stats *bdb_queue_get_stats(bdb_state_type *bdb_state)
{
    if (bdb_state->bdbtype == BDBTYPE_QUEUEDB)
        return bdb_queuedb_get_stats(bdb_state);

    return &bdb_state->qpriv->stats;
}
