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

#include <segstr.h>
#include <lockmacro.h>
#include <list.h>
#include <plhash.h>
#include <fsnap.h>

#include "net.h"
#include "bdb_int.h"
#include "locks.h"
#include <ctrace.h>
#include <string.h>
#include <strings.h>
#include <assert.h>
#include <errno.h>
#include <alloca.h>

/* Berkeley wants this defined, or it'll try to include varargs. */
#undef STDC_HEADERS
#define STDC_HEADERS
#include <db_int.h>
#include <db_page.h>
#include <db_shash.h>
#include <dbinc_auto/db_auto.h>
#include <dbinc_auto/txn_auto.h>
#include <txn.h>
#include <dbinc_auto/txn_ext.h>
#include <btree.h>
#include <lock.h>
#include <mp.h>
#include <db.h>
#undef STDC_HEADERS

#ifndef BERKDB_46
#include "db_int.h"
#include "dbinc/db_swap.h"
#include "llog_auto.h"
#include "llog_int.h"
#include "llog_handlers.h"
#include "db_am.h"
#endif

#include "bdb_api.h"
#include "bdb_osqllog.h"

#include "endian_core.h"
#include "printformats.h"
#include "logmsg.h"

extern int gbl_dispatch_rowlocks_bench;

/* TODO:
 * Old blkseq stuff is not handled.
 * Also I am assuming we are never turning on paulbit 67, 16
 * to enable the old blkseq scheme. */

/* For compensating log records, remember the LSN we compensated so
   we skip it (don't want to undelete a record twice) */
struct remembered_record {
    DB_LSN lsn;
};

/* Set to 1 to debug compensation records */
static int debug_comp = 0;

/* Set to 1 to debug locks */
static int debug_locks = 0;

static int release_locks_for_logical_transaction(bdb_state_type *bdb_state,
                                                 unsigned long long ltranid);

int logical_commit_replicant(bdb_state_type *bdb_state,
                             unsigned long long ltranid);

static int logical_release_transaction(bdb_state_type *bdb_state,
                                       unsigned long long ltranid,
                                       int repcommit);

static int undo_physical_transaction(bdb_state_type *bdb_state, tran_type *tran,
                                     DBT *logdta, int *did_something,
                                     DB_LSN *lsn, DB_LSN *prev_lsn_out);

static int undo_upd_dta_lk(bdb_state_type *bdb_state, tran_type *tran,
                           llog_undo_upd_dta_lk_args *upd_dta_lk,
                           DB_LSN *undolsn, DB_LSN *prev, int just_load_lsn);

static int undo_upd_ix_lk(bdb_state_type *bdb_state, tran_type *tran,
                          llog_undo_upd_ix_lk_args *upd_ix_lk, DB_LSN *undolsn,
                          DB_LSN *prev, int just_load_lsn);

extern u_int32_t gbl_rep_lockid;

/* Global debug-rowlocks flag */
extern int gbl_debug_rowlocks;

static int print_log_records(bdb_state_type *bdb_state, DB_LSN *lsn)
{
    DB_LOGC *cur = NULL;
    DBT logent;
    DB_LSN nextlsn = *lsn;
    u_int32_t rectype;
    int rc = 0;

    rc = bdb_state->dbenv->log_cursor(bdb_state->dbenv, &cur, 0);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s:%d %s log_cursor rc %d\n", __FILE__, __LINE__,
                __func__, rc);
        return rc;
    }

    bzero(&logent, sizeof(DBT));

    while (nextlsn.file != 0) {

        logent.flags = DB_DBT_MALLOC;

        rc = cur->get(cur, &nextlsn, &logent, DB_SET);
        if (rc == DB_NOTFOUND) {
            rc = BDBERR_NO_LOG;
            break;
        } else if (rc) {
            logmsg(LOGMSG_ERROR, "%s:%d %s log_cur->get(%u:%u) rc %d\n", __FILE__,
                    __LINE__, __func__, nextlsn.file, nextlsn.offset, rc);
            break;
        }
        if (logent.size < sizeof(u_int32_t)) {
            rc = -1;
            logmsg(LOGMSG_ERROR, 
                "%s:%d %s lsn %u:%u unexpected log records size %d\n",
                __FILE__, __LINE__, __func__, nextlsn.file, nextlsn.file, rc);
            break;
        }

        LOGCOPY_32(&rectype, logent.data);
        /* first 2 words are type + txnid */

        logmsg(LOGMSG_USER, "%d\n", rectype);

        LOGCOPY_TOLSN(&nextlsn,
                      (u_int8_t *)logent.data + 2 * sizeof(u_int32_t));
    }

    if (cur)
        cur->close(cur, 0);
    return rc;
}

/* These routines inspect the berkeley log starting at the LSN for a logical
   transaction record and return
   the original key and data that corresponds to the physical transaction that
   LSN logs.  We do this to avoid
   logging large payloads twice every time for the  the fairly rare case we need
   to undo them.  The rules of
   the game are:

   * berkeley logs an addrem record for the key/data DBTs - there are two
   separate log records.
   * the key is logged first, then the data
   * the key or the data key can be logged as
     a) for an add the dbt portion of the contains contains the record (key or
   data)
     b) for a delete, the dbt portion will be empty, and the hdr portion
   contains either a B_KEYDATA or a B_OVERFLOW
        structure
   * if it's a B_KEYDATA, the payload (key or data) is the dta portion of the
   BKEYDATA structure
   * if it's a B_OVERFLOW it is followed by one or more 'big' records that
   contain the payload
   * we process things in reverse, so we'll see the addrem last, and the big
   records in the reverse order
   * we should only see 2 addrems unless there's been a page split or a reverse
   split
   * if there is a split, there will be an additional addrem to insert a
   reference to a into its parent
     page into a parent, which we ignore (it comes before the "real" addrems
   that we are interested in,
     so we see it later since we are going in reverse
   * if there is a reverse split, we see it last, so we can't stop when we see 2
   addrems
   * a split/reverse split can percolate up the tree, creating more
   addrem/split/reverse split records

   So to summarize: on a delete we keep running until we hit the begin record,
   and the 2 records (key/data pair) we are looking for will be the last 2
   addrem records we see.  On an add
   (undoing_a_delete == false), we stop as soon as we find 2 addrem records.

   For every record we reconstruct, we check the size and the genid payload to
   make sure
   we have sane results.  Any error in this subsystem is fatal - if means we are
   failing recovery and the
   db is potentially corrupt.
*/
static int get_next_addrem_buffer(bdb_state_type *bdb_state, DB_LSN *lsn,
                                  void *buf, int len, int *outlen, int *outpage,
                                  int *outidx, int *have_record,
                                  DB_LSN *nextlsn)
{
    void *p = NULL;
    int off = 0;
    DBT logent;
    u_int32_t rectype;
    int rc;
    BOVERFLOW *ov;
    BKEYDATA *kd;
    DB_LOGC *cur = NULL;
    DB_LSN prevlsn;
    DB_LSN savedlsn = *lsn;
    int last_was_pgfree = 0;
    int n = 0;
    __db_addrem_args *addrem_rec;
    __db_big_args *big_rec;

    rc = bdb_state->dbenv->log_cursor(bdb_state->dbenv, &cur, 0);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s:%d %s log_cursor rc %d\n", __FILE__, __LINE__,
                __func__, rc);
        return rc;
    }

    bzero(&logent, sizeof(DBT));
    rc = 0;
    while (rc == 0 && lsn->file != 0) {
        if (p) {
            __os_free(bdb_state->dbenv, p);
            p = NULL;
        }
        /* TODO: These can't be bigger than a max page size, I think. Should be
           able to get
           away with a single buffer. */
        logent.flags = DB_DBT_MALLOC;
        p = NULL;
        if (logent.data) {
            free(logent.data);
            logent.data = NULL;
        }
        rc = cur->get(cur, lsn, &logent, DB_SET);
        if (rc == DB_NOTFOUND) {
            rc = BDBERR_NO_LOG;
            goto done;
        }
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s:%d %s %d log_cur->get(%u:%u) rc %d\n", __FILE__,
                    __LINE__, __func__, n, lsn->file, lsn->offset, rc);
            break;
        }
        if (logent.size < sizeof(u_int32_t)) {
            rc = -1;
            logmsg(LOGMSG_ERROR, 
                    "%s:%d %s lsn %u:%u unexpected log records size %d\n",
                    __FILE__, __LINE__, __func__, lsn->file, lsn->file, rc);
            break;
        }
        LOGCOPY_32(&rectype, logent.data);
        /* first 2 words are type + txnid */
        LOGCOPY_TOLSN(&prevlsn,
                      (u_int8_t *)logent.data + 2 * sizeof(u_int32_t));
        *nextlsn = prevlsn;

        if (rectype == DB___db_pg_free || rectype == DB___db_pg_freedata)
            /* pg_free is generating an extra addrem that I don't understand.
             * skip it */
            last_was_pgfree = 1;
        else if (rectype != DB___db_addrem && rectype != DB___db_debug)
            last_was_pgfree = 0;

        switch (rectype) {
        case DB___db_addrem:
            __db_addrem_read(bdb_state->dbenv, logent.data, &addrem_rec);
            p = addrem_rec;

            /* skip if last record was a pg_free - this isn't one of our addrems
             */
            if (!last_was_pgfree) {
                last_was_pgfree = 0;

                if (addrem_rec->hdr.data && addrem_rec->hdr.size > 0) {
                    kd = addrem_rec->hdr.data;
                    ov = addrem_rec->hdr.data;
                } else {
                    kd = NULL;
                    ov = NULL;
                }

                if (addrem_rec->opcode == DB_ADD_DUP &&
                    (kd == NULL || B_TYPE(kd) == 0)) {
                    /* If it was an add, the buffer is going to be in the dbt
                       section of the record.
                       otherwise it'll refer to an existing page entry and it'll
                       be in the hdr
                       section of the record. */
                    *outlen = addrem_rec->dbt.size;

                    if (buf && addrem_rec->dbt.size > len) {
                        *have_record = 0;
                    } else {
                        *have_record = 1;
                        if (buf)
                            memcpy(buf, addrem_rec->dbt.data,
                                   addrem_rec->dbt.size);
                        if (outpage)
                            *outpage = addrem_rec->pgno;
                        if (outidx)
                            *outidx = addrem_rec->indx;
                    }
                    goto done;
                } else {
                    ov = addrem_rec->hdr.data;
                    kd = addrem_rec->hdr.data;

                    if (B_TYPE(kd) == B_OVERFLOW) {
                        /* Overflows get handles by __db_big type records, but
                           we need to figure out
                           the offset here - it's then checked by the __db_big
                           handler below. */
                        if (LOG_SWAPPED()) {
                            M_16_SWAP(ov->unused1);
                            M_32_SWAP(ov->pgno);
                            M_32_SWAP(ov->tlen);
                        }
                        *outlen = ov->tlen;
                        off = ov->tlen;
                    } else if (B_TYPE(kd) == B_KEYDATA) {
                        /* we don't have off-page duplicates, so this is a real
                         * entry */
                        *outlen = kd->len;
                        if (buf && (kd->len > len)) {
                            /* huh? */
                            logmsg(LOGMSG_ERROR, "no record: len %d expected %d %d\n",
                                   kd->len, len, __LINE__);
                            fsnapf(stdout, kd->data, kd->len);
                            logmsg(LOGMSG_ERROR, "full:\n");
                            fsnapf(stdout, addrem_rec->hdr.data,
                                   addrem_rec->hdr.size);
                            *have_record = 0;
                            bdb_dump_log(bdb_state->dbenv, &savedlsn);
                            abort();
                        } else {
                            int iii;
                            *have_record = 1;
                            if (buf)
                                memcpy(buf, kd->data, kd->len);
                            if (outpage)
                                *outpage = addrem_rec->pgno;
                            if (outidx)
                                *outidx = addrem_rec->indx;
#if 0
                                printf("Retrieving %d len bytes for %d op\n",
                                      kd->len, addrem_rec->opcode);
                                
                                for (iii=0; iii<kd->len; iii++)
                                {
                                   printf("%02x ", kd->data[iii]);
                                }
                                printf("\n");
#endif
                        }
                        goto done;
                    } else {
                        logmsg(LOGMSG_ERROR, "Unexpected type %d\n", kd->type);
                        *have_record = 0;
                    }
                }
            }
            break;

        case DB___db_big:
            __db_big_read(bdb_state->dbenv, logent.data, &big_rec);
            p = big_rec;

            off -= big_rec->dbt.size;
            if (off < 0) {
                logmsg(LOGMSG_ERROR, "huh?\n");
                *have_record = 0;
            } else {
                if (buf)
                    memcpy(((char *)buf) + off, big_rec->dbt.data,
                           big_rec->dbt.size);
                if (off == 0) {
                    /* this was the last big record and we have a full buffer */
                    *have_record = 1;
                    goto done;
                }
            }
            /* Page-order cursor doesn't apply to blobs. */
            break;

        default:
            break;
        }

        *lsn = prevlsn;
        n++;
    }

done:
    if (p)
        __os_free(bdb_state->dbenv, p);
    if (logent.data) {
        free(logent.data);
        logent.data = NULL;
    }
    /* if we stopped on an addrem, move cursor to the previous lsn */
    if (cur && rc == 0 && (rectype == DB___db_addrem || rectype == DB___db_big))
        *lsn = prevlsn;
    if (cur)
        cur->close(cur, 0);
    if (!have_record)
        logmsg(LOGMSG_ERROR, "no record\n");
    return rc;
}

int bdb_reconstruct_add(bdb_state_type *bdb_state, DB_LSN *startlsn, void *key,
                        int keylen, void *data, int datalen, int *p_outlen)
{
    int rc;
    int have_record = 0;
    int outlen;
    DB_LSN nextlsn = {1, 0};
    static int nadd = 0;
    DB_LSN lsn = *startlsn;

    if (!p_outlen)
        p_outlen = &outlen;
    nadd++;

    rc = get_next_addrem_buffer(bdb_state, &lsn, data, datalen, p_outlen, NULL,
                                NULL, &have_record, &nextlsn);
    if (rc == BDBERR_NO_LOG)
        return rc;
    if (rc)
        return -1;
    rc = get_next_addrem_buffer(bdb_state, &nextlsn, key, keylen, p_outlen,
                                NULL, NULL, &have_record, &nextlsn);
    if (rc == BDBERR_NO_LOG)
        return rc;
    if (rc)
        return -1;

    return 0;
}

int bdb_dump_log(DB_ENV *dbenv, DB_LSN *startlsn)
{
    DB_LOGC *cur;
    int rc;
    DBT dbt;
    DB_LSN lsn;

    struct generic_record {
        u_int32_t type;
        DB_TXN *txnid;
        DB_LSN prev_lsn;
    } * generic_record;

    /* debug: dump all the events for this record */
   logmsg(LOGMSG_USER, "========\n");
    logmsg(LOGMSG_USER, "lsn %u:%u\n", startlsn->file, startlsn->offset);

    rc = dbenv->log_cursor(dbenv, &cur, 0);
    if (rc) {
        logmsg(LOGMSG_USER, "log_cursor rc %d\n", rc);
        return rc;
    }
    bzero(&dbt, sizeof(DBT));
    dbt.flags = DB_DBT_MALLOC;
    lsn = *startlsn;
    rc = cur->get(cur, &lsn, &dbt, DB_SET);
    while (rc == 0) {
        if (dbt.data) {
            generic_record = dbt.data;

            logmsg(LOGMSG_USER, "  %u:%u type %u\n", lsn.file, lsn.offset,
                   generic_record->type);
            lsn = generic_record->prev_lsn;

            if (generic_record->type == DB___db_addrem) {
                BKEYDATA *kd;

                __db_addrem_args *addrem;
                __db_addrem_read(dbenv, dbt.data, &addrem);
                if (addrem->hdr.data)
                    kd = addrem->hdr.data;
                else
                    kd = NULL;
                if (addrem->opcode == DB_ADD_DUP &&
                    (kd == NULL || B_TYPE(kd) == 0)) {
                    fsnapf(stdout, addrem->dbt.data, addrem->dbt.size);
                } else {
                    fsnapf(stdout, kd->data, kd->len);
                }

                free(addrem);
            } else if (generic_record->type == DB___db_debug) {
                __db_debug_args *debug;
                __db_debug_read(dbenv, dbt.data, &debug);

                logmsg(LOGMSG_USER, "debug: %s", (char *)debug->key.data);

                free(debug);
            }

            free(dbt.data);
        } else {
            logmsg(LOGMSG_FATAL, "no log record returned?\n");
            abort();
        }
        if (lsn.file == 0 && lsn.offset == 0)
            break;
        rc = cur->get(cur, &lsn, &dbt, DB_SET);
    }
    if (rc) {
        logmsg(LOGMSG_USER, "log get rc %d at lsn %u:%u\n", rc, lsn.file, lsn.offset);
        return -1;
    }
    logmsg(LOGMSG_USER, "========\n");
    return -1;
}

int bdb_reconstruct_delete(bdb_state_type *bdb_state, DB_LSN *startlsn,
                           int *page, int *index, void *key, int keylen,
                           void *data, int datalen, int *outdatalen)
{
    DB_LSN savedlsn = *startlsn;
    int outlen[2];
    int rc;
    unsigned char *buf[2];
    int haveit[2] = {0};
    int pages[2] = {0};
    int indexes[2] = {0};
    int alloclen;
    int i = 0;
    static int cnt = 0;
    DB_LSN nextlsn;
    int debugit = 0;
    DB_LSN lsn = *startlsn;

    cnt++;

    if (keylen > datalen)
        alloclen = keylen;
    else
        alloclen = datalen;

    if (alloclen <= 0) {
        buf[0] = buf[1] = NULL;
    } else if (alloclen <= 1024) {
        buf[0] = alloca(alloclen);
        buf[1] = alloca(alloclen);
    } else {
        buf[0] = malloc(alloclen);
        buf[1] = malloc(alloclen);
    }

    /* look for the last two records (ie first in log order) */
    do {
        i++;
        rc = get_next_addrem_buffer(bdb_state, &lsn, buf[i % 2], alloclen,
                                    &outlen[i % 2], &pages[i % 2],
                                    &indexes[i % 2], &haveit[i % 2], &nextlsn);
        if (rc == BDBERR_NO_LOG)
            return rc;
    } while (nextlsn.file != 0 && (!haveit[0] || !haveit[1]));

    if (haveit[0] && haveit[1]) {
        if (key) {
            if (keylen < outlen[i % 2]) {
                logmsg(LOGMSG_DEBUG, 
                       "%s: asking for different keylength=%d, replicated=%d\n",
                       __func__, keylen, outlen[i % 2]);
            }
            memcpy(key, buf[i % 2], keylen);
        }
        if (data) {
            if (datalen < outlen[(i + 1) % 2]) {
                logmsg(LOGMSG_DEBUG, 
                        "%s: asking for different datalength=%d, replicated=%d\n",
                        __func__, datalen, outlen[(i + 1) % 2]);
            }
            memcpy(data, buf[(i + 1) % 2], datalen);
            if (outdatalen) {
                *outdatalen = datalen;
            }
        }
        if (page)
            *page = pages[i % 2];
        if (index)
            *index = indexes[i % 2];

        if (datalen > 1024) {
            free(buf[0]);
            free(buf[1]);
        }
        return 0;
    } else {
        if (datalen > 1024) {
            free(buf[0]);
            free(buf[1]);
        }
        return 1;
    }
}

int bdb_reconstruct_update(bdb_state_type *bdb_state, DB_LSN *startlsn,
                           int *page, int *index, void *key, int keylen,
                           void *data, int datalen)
{
    DB_LSN savedlsn = *startlsn;
    int outlen;
    int rc;
    unsigned char *buf[4];
    int pages[4] = {0};
    int indexes[4] = {0};
    int haveit[4] = {0};
    int alloclen;
    int i = 0;
    static int cnt = 0;
    DB_LSN nextlsn;
    int debugit = 0;
    DB_LSN lsn = *startlsn;

    if (keylen > datalen)
        alloclen = keylen;
    else
        alloclen = datalen;

    if (alloclen <= 0) {
        buf[0] = buf[1] = buf[2] = buf[3] = NULL;
    } else if (alloclen <= 512) {
        buf[0] = alloca(alloclen);
        buf[1] = alloca(alloclen);
        buf[2] = alloca(alloclen);
        buf[3] = alloca(alloclen);
    } else {
        buf[0] = malloc(alloclen);
        buf[1] = malloc(alloclen);
        buf[2] = malloc(alloclen);
        buf[3] = malloc(alloclen);
    }

    cnt++;

    /* look for the last two records (ie first in log order) */
    do {
        i++;
        rc = get_next_addrem_buffer(bdb_state, &lsn, buf[i % 4], alloclen,
                                    &outlen, &pages[i % 4], &indexes[i % 4],
                                    &haveit[i % 4], &nextlsn);
        if (rc == BDBERR_NO_LOG)
            return rc;
    } while (nextlsn.file != 0 && !(haveit[i % 4] && haveit[(i + 3) % 4]));

    if (haveit[i % 4] && haveit[(i + 3) % 4]) {
        if (data)
            memcpy(data, buf[(i + 3) % 4], datalen);
        if (key)
            memcpy(key, buf[i % 4], keylen);
        if (page)
            *page = pages[i % 4];
        if (index)
            *index = indexes[i % 4];
        if (datalen > 512) {
            free(buf[0]);
            free(buf[1]);
            free(buf[2]);
            free(buf[3]);
        }
        return 0;
    } else {
        if (datalen > 512) {
            free(buf[0]);
            free(buf[1]);
            free(buf[2]);
            free(buf[3]);
        }
        return 1;
    }
}

int bdb_reconstruct_key_update(bdb_state_type *bdb_state, DB_LSN *startlsn,
                               void **diff, int *offset, int *difflen)
{
    DB_LSN lsn;
    int rc, found_repl = 0;
    DBT logent = {0};
    __bam_repl_args *repl = NULL;
    DB_LOGC *cur = NULL;
    u_int32_t rectype;
    DB_LSN prevlsn;

    /* Key updates don't use addrems unless someone manages to set up 512-byte
       key pages. We should probably
       disallow that.  So just go through the log until you see a repl. */

    logent.flags = DB_DBT_MALLOC;

    rc = bdb_state->dbenv->log_cursor(bdb_state->dbenv, &cur, 0);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s:%d %s log_cursor rc %d\n", __FILE__, __LINE__,
                __func__, rc);
        return rc;
    }
    rc = 0;
    lsn = *startlsn;
    /*    printf("%s:%d lsn=[%d:%d]\n", __FILE__,__LINE__, lsn.file,
     * lsn.offset);*/

    while (rc == 0 && lsn.file != 0) {
        if (logent.data) {
            free(logent.data);
            logent.data = NULL;
        }

        rc = cur->get(cur, &lsn, &logent, DB_SET);
        if (rc == DB_NOTFOUND) {
            rc = BDBERR_NO_LOG;
            goto done;
        }
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s:%d %s log_cur->get rc %d lsn=[%d:%d]\n",
                    __FILE__, __LINE__, __func__, rc, lsn.file, lsn.offset);
            break;
        }

        LOGCOPY_32(&rectype, logent.data);
        LOGCOPY_TOLSN(&prevlsn,
                      (u_int8_t *)logent.data + 2 * sizeof(u_int32_t));

        if (rectype == DB___bam_repl) {
            rc = __bam_repl_read(bdb_state->dbenv, logent.data, &repl);
            if (rc) {
                logmsg(LOGMSG_ERROR, "__bam_repl_read rc %d\n", rc);
                goto done;
            }
            *diff = malloc(repl->orig.size);
            memcpy(*diff, repl->orig.data, repl->orig.size);
            *difflen = repl->orig.size;
            *offset = repl->prefix;
            found_repl = 1;
            break;
        }

        lsn = prevlsn;

        /*printf("%s:%d lsn=[%d:%d]\n", __FILE__,__LINE__, lsn.file,
         * lsn.offset);*/
    }

done:
    if (logent.data) {
        free(logent.data);
        logent.data = NULL;
    }
    if (cur)
        cur->close(cur, 0);
    if (repl)
        __os_free(bdb_state->dbenv, repl);

    /* Abort if I didn't find it */
    assert(found_repl);

    return rc;
}

/* Unlike reconstruct-key update, the memory has already been allocated. */
int bdb_reconstruct_inplace_update(bdb_state_type *bdb_state, DB_LSN *startlsn,
                                   void *allcd, int allcd_sz, int *offset,
                                   int *outlen, int *outpage, int *outidx)
{
    int rc = 0, foundit = 0, off = 0;
    DB_LSN lsn = *startlsn, prevlsn;
    DBT logent = {0};
    BOVERFLOW *ov = NULL;
    BKEYDATA *kd = NULL;
    __db_addrem_args *addrem_rec;
    __db_big_args *big_rec;
    u_int32_t rectype;
    __bam_repl_args *repl = NULL;
    DB_LOGC *cur = NULL;

    logent.flags = DB_DBT_MALLOC;

    if (offset)
        *offset = 0;

    rc = bdb_state->dbenv->log_cursor(bdb_state->dbenv, &cur, 0);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s:%d %s log_cursor rc %d\n", __FILE__, __LINE__,
                __func__, rc);
        return rc;
    }

    /* Search log records loop. */
    while (rc == 0 && lsn.file != 0) {
        /* Free previous log record. */
        if (logent.data) {
            free(logent.data);
            logent.data = NULL;
        }

        /* Grab a log entry. */
        rc = cur->get(cur, &lsn, &logent, DB_SET);
        if (rc == DB_NOTFOUND) {
            rc = BDBERR_NO_LOG;
            goto done;
        }
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s:%d %s log_cur->get rc %d lsn=[%d:%d]\n",
                    __FILE__, __LINE__, __func__, rc, lsn.file, lsn.offset);
            break;
        }

        /* Copy it's type. */
        LOGCOPY_32(&rectype, logent.data);

        /* Copy it's previous LSN. */
        LOGCOPY_TOLSN(&prevlsn,
                      (u_int8_t *)logent.data + 2 * sizeof(u_int32_t));

        /* Find a btree-replace log record. */
        if (rectype == DB___bam_repl) {
            /* Invoke berkeley's 'read-repl-log' routine. */
            if (0 !=
                (rc = __bam_repl_read(bdb_state->dbenv, logent.data, &repl))) {
                logmsg(LOGMSG_ERROR, "__bam_repl_read rc %d\n", rc);
                goto done;
            }

            /* Copy the reconstructed record. */
            if (allcd) {
                /* Make sure that our sizes agree. */
                assert(allcd_sz == repl->orig.size);

                /* Copy the original data. */
                memcpy(allcd, repl->orig.data, repl->orig.size);
            }

            /* Latch the original record-size. */
            if (outlen)
                *outlen = repl->orig.size;

            /* Latch the original offset (which should be 0). */
            if (offset)
                *offset = repl->prefix;

            /* Grab the original page number- this won't change on an update. */
            if (outpage)
                *outpage = repl->pgno;

            /* Grab the original page index- this won't change on an update. */
            if (outidx)
                *outidx = repl->indx;
            foundit = 1;
            break;
        }

        /* For big-record updates, we want the addrem that is deleting the
         * payload */
        if (rectype == DB___db_addrem) {
            __db_addrem_read(bdb_state->dbenv, logent.data, &addrem_rec);
            if (IS_REM_OPCODE(addrem_rec->opcode)) {
                kd = NULL;
                ov = NULL;

                if (addrem_rec->hdr.data && addrem_rec->hdr.size > 0) {
                    kd = addrem_rec->hdr.data;
                    ov = addrem_rec->hdr.data;
                }

                if (B_TYPE(kd) == B_OVERFLOW) {
                    if (LOG_SWAPPED()) {
                        M_16_SWAP(ov->unused1);
                        M_32_SWAP(ov->pgno);
                        M_32_SWAP(ov->tlen);
                    }
                    off = ov->tlen;
                    if (outlen)
                        *outlen = ov->tlen;
                } else if (B_TYPE(kd) == B_KEYDATA) {
                    assert(allcd_sz == kd->len);
                    if (allcd)
                        memcpy(allcd, kd->data, kd->len);
                    if (outlen)
                        *outlen = kd->len;
                    if (outpage)
                        *outpage = addrem_rec->pgno;
                    if (outidx)
                        *outidx = addrem_rec->indx;
                    off = 0;
                    foundit = 1;
                    __os_free(bdb_state->dbenv, addrem_rec);
                    break;
                }
            }
        }

        if (rectype == DB___db_big) {
            __db_big_read(bdb_state->dbenv, logent.data, &big_rec);
            if (big_rec->opcode == DB_REM_BIG) {
                off -= big_rec->dbt.size;
                assert(off >= 0);

                if (allcd)
                    memcpy(((char *)allcd) + off, big_rec->dbt.data,
                           big_rec->dbt.size);

                if (off == 0) {
                    foundit = 1;
                    __os_free(bdb_state->dbenv, big_rec);
                    break;
                }
            }
            __os_free(bdb_state->dbenv, big_rec);
        }

        /* Next lsn. */
        lsn = prevlsn;
    }

done:
    /* Print an error message if this wasn't found. */
    if (0 == rc && !foundit) {
        logmsg(LOGMSG_ERROR, "%s: Unable to locate update log record.\n", __func__);
        rc = -1;
    }

    /* Free last log record. */
    if (logent.data) {
        free(logent.data);
        logent.data = NULL;
    }

    /* Close cursor. */
    if (cur)
        cur->close(cur, 0);

    /* Free the log-read structure. */
    if (repl)
        __os_free(bdb_state->dbenv, repl);

    return rc;
}

/* TODO: all the log records need to be modified to take
   an LSN of a compensating log record */
int undo_add_dta_lk(bdb_state_type *bdb_state, tran_type *tran,
                    char *table_name, llog_undo_add_dta_lk_args *add_dta_lk,
                    DB_LSN *undolsn, DB_LSN *prev, int just_load_lsn)
{
    unsigned long long tranid;
    unsigned long long genid;
    int rc;

    *prev = add_dta_lk->prevllsn;
    if (just_load_lsn)
        return 0;

    tranid = add_dta_lk->ltranid;
    genid = add_dta_lk->genid;

    if (bdb_state->in_recovery) {
        print(bdb_state, "UNDO lsn %d:%d %s for transaction %016llx deleting "
                         "genid %016llx\n",
              undolsn->file, undolsn->offset, __func__, tranid, genid);
    }

    /* ll_undo_add_dta */
    rc = ll_undo_add_dta_lk(bdb_state, tran, table_name, genid, undolsn,
                            add_dta_lk->dtafile, add_dta_lk->dtastripe);

    if (rc && debug_comp)
        abort();
    return rc;
}

int undo_add_ix_lk(bdb_state_type *bdb_state, tran_type *tran, char *table_name,
                   llog_undo_add_ix_lk_args *add_ix_lk, DB_LSN *undolsn,
                   DB_LSN *prev, int just_load_lsn)
{
    unsigned long long tranid;
    unsigned long long genid;
    int rc;
    DBT *key;
    void *dta;
    DB_LSN lsn = *undolsn;

    *prev = add_ix_lk->prevllsn;
    if (just_load_lsn)
        return 0;

    /* TODO: change this back */
    /*key = malloc(add_ix_lk->keylen);*/

    tranid = add_ix_lk->ltranid;
    genid = add_ix_lk->genid;
    key = &add_ix_lk->key;

/*
rc = bdb_reconstruct_add(bdb_state, &lsn, key, add_ix_lk->keylen, NULL, 0,
NULL);
if (rc == BDBERR_NO_LOG)
   goto done;
if (rc) {
    printf("reconstruct key add rc %d\n", rc);
    goto done;
}
*/

#if 0
   printf("undo_add_ix genid %016llx  ltranid %016llx table %s\n", genid, tranid, table_name);
   fsnapf(stdout, key, add_ix->keylen);
#endif

    rc = ll_undo_add_ix_lk(bdb_state, tran, table_name, add_ix_lk->ix,
                           key->data, key->size /*add_ix_lk->keylen*/, undolsn);

done:
    if (rc && debug_comp)
        abort();
    return rc;
}

int undo_del_ix_lk(bdb_state_type *bdb_state, tran_type *tran,
                   llog_undo_del_ix_lk_args *del_ix_lk, DB_LSN *undolsn,
                   DB_LSN *prev, int just_load_lsn)
{
    int ix;
    unsigned long long genid;
    unsigned long long ltranid;
    char *table;
    int rc;
    DB_LSN lsn;

    int keylen;
    int dtalen;
    void *keybuf, *dtabuf;

    *prev = del_ix_lk->prevllsn;
    if (just_load_lsn)
        return 0;

    keylen = del_ix_lk->keylen;
    dtalen = del_ix_lk->dtalen;
    keybuf = calloc(1, keylen);
    dtabuf = calloc(1, dtalen);

debug:
    lsn = *undolsn;
    rc = bdb_reconstruct_delete(bdb_state, undolsn, NULL, NULL, keybuf, keylen,
                                dtabuf, dtalen, NULL);
    if (rc == BDBERR_NO_LOG)
        goto done;
    if (rc) {
        bdb_state->dbenv->memp_sync(bdb_state->dbenv, NULL);
        logmsg(LOGMSG_FATAL, "Reconstruct for undo ix del for lsn %u:%u ltranid "
                        "%016llx failed rc %d\n",
                lsn.file, lsn.offset, tran->logical_tranid, rc);
        abort(); /* fatal - we can't proceed if we can't undo a transaction */
    }
    memcpy(&genid, dtabuf, sizeof(unsigned long long));

    table = del_ix_lk->table.data;
    ltranid = del_ix_lk->ltranid;
    ix = del_ix_lk->ix;

    rc = ll_undo_del_ix_lk(bdb_state, tran, table, genid, ix, undolsn, keybuf,
                           keylen, dtabuf, dtalen);

done:
    if (rc && debug_comp)
        abort();
    free(keybuf);
    free(dtabuf);

    return rc;
}

int undo_del_ix(bdb_state_type *bdb_state, tran_type *tran,
                llog_undo_del_ix_args *del_ix, DB_LSN *undolsn, DB_LSN *prev,
                int just_load_lsn)
{
    int ix;
    unsigned long long genid;
    unsigned long long ltranid;
    char *table;
    int rc;
    DB_LSN lsn;

    int keylen;
    int dtalen;
    void *keybuf, *dtabuf;
    int outdatalen = 0;

    *prev = del_ix->prevllsn;
    if (just_load_lsn)
        return 0;

    keylen = del_ix->keylen;
    dtalen = del_ix->dtalen;
    keybuf = calloc(1, keylen);
    dtabuf = calloc(1, dtalen);

debug:
    lsn = *undolsn;
    rc = bdb_reconstruct_delete(bdb_state, undolsn, NULL, NULL, keybuf, keylen,
                                dtabuf, dtalen, &outdatalen);
    if (rc == BDBERR_NO_LOG)
        goto done;
    if (rc) {
        bdb_state->dbenv->memp_sync(bdb_state->dbenv, NULL);
        logmsg(LOGMSG_FATAL, "Reconstruct for undo ix del for lsn %u:%u ltranid "
                        "%016llx failed rc %d\n",
                lsn.file, lsn.offset, tran->logical_tranid, rc);
        abort(); /* fatal - we can't proceed if we can't undo a transaction */
    }
    memcpy(&genid, dtabuf, sizeof(unsigned long long));

    table = del_ix->table.data;
    ltranid = del_ix->ltranid;
    ix = del_ix->ix;

    rc = ll_undo_del_ix_lk(bdb_state, tran, table, genid, ix, undolsn, keybuf,
                           keylen, dtabuf, dtalen);

done:
    free(keybuf);
    free(dtabuf);

    return rc;
}

int undo_del_dta_lk(bdb_state_type *bdb_state, tran_type *tran,
                    llog_undo_del_dta_lk_args *del_dta_lk, DB_LSN *undolsn,
                    DB_LSN *prev, int just_load_lsn)
{
    unsigned long long genid;
    unsigned long long ltranid;
    char *table;
    int rc;
    void *dtabuf;
    int dtalen;

    *prev = del_dta_lk->prevllsn;
    if (just_load_lsn)
        return 0;

    dtalen = del_dta_lk->dtalen;

    dtabuf = calloc(1, dtalen);

    rc = bdb_reconstruct_delete(bdb_state, &del_dta_lk->prev_lsn, NULL, NULL,
                                &genid, sizeof(unsigned long long), dtabuf,
                                dtalen, NULL);

    if (rc == BDBERR_NO_LOG)
        goto done;
    if (rc) {
        bdb_state->dbenv->memp_sync(bdb_state->dbenv, NULL);
        logmsg(LOGMSG_FATAL, "Reconstruct for undo dta del for lsn %u:%u ltranid "
                        "%016llx failed rc %d",
                undolsn->file, undolsn->offset, tran->logical_tranid, rc);
        abort();
    }

    table = del_dta_lk->table.data;
    ltranid = del_dta_lk->ltranid;

    rc = ll_undo_del_dta_lk(bdb_state, tran, table, genid, undolsn,
                            del_dta_lk->dtafile, del_dta_lk->dtastripe, dtabuf,
                            dtalen);

done:
    if (rc && debug_comp)
        abort();
    free(dtabuf);

    return rc;
}

/* This should never happen - this implies an undo for a logical
   transaction that commited.  We CAN undo it, but it's completely
   illogical so we treat it as an error */
int undo_commit(bdb_state_type *bdb_state, tran_type *tran,
                llog_ltran_commit_args *commit, DB_LSN *prev, int just_load_lsn)
{
    unsigned long long tranid;

    *prev = commit->prevllsn;
    if (just_load_lsn)
        return 0;

    tranid = commit->ltranid;

    logmsg(LOGMSG_ERROR, 
            "%s:%d ERROR: undo call for a commit record tranid %016llx\n",
            __FILE__, __LINE__, tranid);

    return EINVAL;
}

char *rectypestr(int rectype)
{
    switch (rectype) {
    case DB_llog_savegenid:
        return "savegenid";
    case DB_llog_scdone:
        return "scdone";
    case DB_llog_undo_add_dta:
        return "undo_add_dta";
    case DB_llog_undo_add_ix:
        return "undo_add_ix";
    case DB_llog_ltran_commit:
        return "ltran_commit";
    case DB_llog_ltran_start:
        return "ltran_start";
    case DB_llog_ltran_comprec:
        return "ltran_comprec";
    case DB_llog_undo_del_dta:
        return "undo_del_dta";
    case DB_llog_undo_del_ix:
        return "undo_del_ix";
    case DB_llog_undo_upd_dta:
        return "undo_upd_dta";
    case DB_llog_undo_upd_ix:
        return "undo_upd_ix";
    case DB_llog_undo_add_dta_lk:
        return "undo_add_dta_lk";
    case DB_llog_undo_add_ix_lk:
        return "undo_add_ix_lk";
    case DB_llog_undo_del_dta_lk:
        return "undo_del_dta_lk";
    case DB_llog_undo_del_ix_lk:
        return "undo_del_ix_lk";
    case DB_llog_undo_upd_dta_lk:
        return "undo_upd_dta_lk";
    case DB_llog_undo_upd_ix_lk:
        return "undo_upd_ix_lk";
    default:
        return "???";
    }
}

/* Undo a single physical transaction.  In case you are wondering why this
   doesn't just call bdb_apprec: bdb_apprec is called by berkeley at various
   points during physical recover, and we don't want to do anything at those
   points.  There's no way to differentiate the calls without kludgery.
   Sets prev_lsn_out for calling code to determine previous log record for
   the logical transaction.
*/
static int undo_physical_transaction(bdb_state_type *bdb_state, tran_type *tran,
                                     DBT *logdta, int *did_something,
                                     DB_LSN *lsn, DB_LSN *prev_lsn_out)
{
    int rc = 0;
    void *logp = NULL;
    u_int32_t rectype;

    /* Rowlock write operations */
    llog_undo_add_dta_lk_args *add_dta_lk;
    llog_undo_del_dta_lk_args *del_dta_lk;
    llog_undo_upd_dta_lk_args *upd_dta_lk;
    llog_undo_add_ix_lk_args *add_ix_lk;
    llog_undo_del_ix_lk_args *del_ix_lk;
    llog_undo_upd_ix_lk_args *upd_ix_lk;

    /* Start, commit & comprec */
    llog_ltran_start_args *start;
    llog_ltran_commit_args *commit;
    llog_ltran_comprec_args *comprec;

    struct remembered_record *rrec;

    int just_load_lsn = 0;
    int failed = 0;

    *did_something = 0;

    /* if this undo is for a record we already undid, don't undo it again */
    if (tran->compensated_records &&
        hash_find(tran->compensated_records, lsn)) {
        /* still need to load prev_lsn_out regardless whether I did anything */
        just_load_lsn = 1;
    }

    LOGCOPY_32(&rectype, logdta->data);
    switch (rectype) {
    case DB_llog_undo_add_dta_lk:
        rc = llog_undo_add_dta_lk_read(bdb_state->dbenv, logdta->data,
                                       &add_dta_lk);
        if (rc)
            return rc;
        logp = add_dta_lk;
        rc = undo_add_dta_lk(bdb_state, tran, add_dta_lk->table.data,
                             add_dta_lk, lsn, prev_lsn_out, just_load_lsn);
        *did_something = !(just_load_lsn);
        break;

    case DB_llog_undo_del_dta_lk:
        rc = llog_undo_del_dta_lk_read(bdb_state->dbenv, logdta->data,
                                       &del_dta_lk);
        if (rc)
            return rc;
        logp = del_dta_lk;
        rc = undo_del_dta_lk(bdb_state, tran, del_dta_lk, lsn, prev_lsn_out,
                             just_load_lsn);
        *did_something = !(just_load_lsn);
        break;

    case DB_llog_undo_upd_dta_lk:
        rc = llog_undo_upd_dta_lk_read(bdb_state->dbenv, logdta->data,
                                       &upd_dta_lk);
        if (rc)
            return rc;
        rc = undo_upd_dta_lk(bdb_state, tran, upd_dta_lk, lsn, prev_lsn_out,
                             just_load_lsn);
        logp = upd_dta_lk;
        *did_something = !(just_load_lsn);
        break;

    case DB_llog_undo_add_ix_lk:
        rc = llog_undo_add_ix_lk_read(bdb_state->dbenv, logdta->data,
                                      &add_ix_lk);
        if (rc)
            return rc;
        logp = add_ix_lk;
        rc = undo_add_ix_lk(bdb_state, tran, add_ix_lk->table.data, add_ix_lk,
                            lsn, prev_lsn_out, just_load_lsn);
        *did_something = !(just_load_lsn);
        break;

    case DB_llog_undo_del_ix_lk:
        rc = llog_undo_del_ix_lk_read(bdb_state->dbenv, logdta->data,
                                      &del_ix_lk);
        if (rc)
            return rc;
        rc = undo_del_ix_lk(bdb_state, tran, del_ix_lk, lsn, prev_lsn_out,
                            just_load_lsn);
        logp = del_ix_lk;
        *did_something = !(just_load_lsn);
        break;

    case DB_llog_undo_upd_ix_lk:
        rc = llog_undo_upd_ix_lk_read(bdb_state->dbenv, logdta->data,
                                      &upd_ix_lk);
        if (rc)
            return rc;
        logp = upd_ix_lk;
        rc = undo_upd_ix_lk(bdb_state, tran, upd_ix_lk, lsn, prev_lsn_out,
                            just_load_lsn);
        *did_something = !(just_load_lsn);
        break;

    case DB_llog_ltran_start:
        /* no-op, except we need the previous LSN. and there can be no previous
           LSN since it's a start record. */
        prev_lsn_out->file = 0;
        prev_lsn_out->offset = 1;
        logp = NULL;
        *did_something = 0;
        break;

    case DB_llog_ltran_commit:
        rc = llog_ltran_commit_read(bdb_state->dbenv, logdta->data, &commit);
        if (rc)
            return rc;
        logp = commit;
        rc = undo_commit(bdb_state, tran, commit, prev_lsn_out, just_load_lsn);
        *did_something = 0;
        break;

    case DB_llog_ltran_comprec:
        rc = llog_ltran_comprec_read(bdb_state->dbenv, logdta->data, &comprec);
        if (rc)
            return rc;
        logp = comprec;
        rrec = malloc(sizeof(struct remembered_record));
        rrec->lsn = comprec->complsn;
        hash_add(tran->compensated_records, rrec);
        *prev_lsn_out = comprec->prevllsn;
        *did_something = 0;
        print(bdb_state, "found compensation record for %d:%d\n",
              rrec->lsn.file, rrec->lsn.offset);
        break;

    default:
        logmsg(LOGMSG_USER, "%s:%d unknown type %d in undo_physical_transaction\n", __FILE__,
               __LINE__, rectype);
        return -1;
        break;
    }

    if (logp)
        free(logp);

    return rc;
}

static inline int logical_rectype(u_int32_t rectype)
{
    switch (rectype) {
    case DB_llog_ltran_start:
    case DB_llog_ltran_commit:
    case DB_llog_ltran_comprec:
    case DB_llog_undo_add_dta_lk:
    case DB_llog_undo_add_ix_lk:
    case DB_llog_undo_del_dta_lk:
    case DB_llog_undo_del_ix_lk:
    case DB_llog_undo_upd_dta_lk:
    case DB_llog_undo_upd_ix_lk:
        return 1;
    }
    return 0;
}

static int find_last_logical_lsn(bdb_state_type *bdb_state, DB_LSN *last_lsn,
                                 DB_LSN *ll_lsn)
{
    DB_LOGC *cur = NULL;
    u_int32_t rectype;
    u_int32_t txnid;
    u_int8_t *bp;
    DBT dta;
    __txn_regop_rowlocks_args *txn_rl_args = NULL;
    DB_LSN commit_lsn = *last_lsn, lsn;
    int ret;

    ll_lsn->file = ll_lsn->offset = 0;
    bzero(&dta, sizeof(DBT));
    dta.flags = DB_DBT_REALLOC;

    if ((ret = bdb_state->dbenv->log_cursor(bdb_state->dbenv, &cur, 0)) != 0) {
        logmsg(LOGMSG_USER, "%s: error getting log cursor, ret=%d\n", __func__,
                ret);
        goto done;
    }

    do {
        if ((ret = cur->get(cur, &commit_lsn, &dta, DB_SET)) != 0) {
            logmsg(LOGMSG_USER, "%s: error getting lsn %d:%d, rc=%d\n", __func__,
                    commit_lsn.file, commit_lsn.offset, ret);
            goto done;
        }

        LOGCOPY_32(&rectype, dta.data);
        assert(rectype == DB___txn_regop_rowlocks);

        if ((ret = __txn_regop_rowlocks_read(bdb_state->dbenv, dta.data,
                                             &txn_rl_args)) != 0) {
            logmsg(LOGMSG_USER, "%s: error reading regop_rowlocks, rc=%d\n",
                    __func__, ret);
            free(txn_rl_args);
            goto done;
        }

        commit_lsn = txn_rl_args->last_commit_lsn;
        lsn = txn_rl_args->prev_lsn;
        free(txn_rl_args);

        do {
            if ((ret = cur->get(cur, &lsn, &dta, DB_SET)) != 0) {
                logmsg(LOGMSG_USER, "%s: ln %d error getting lsn %d:%d, rc=%d\n",
                        __func__, __LINE__, lsn.file, lsn.offset, ret);
                goto done;
            }
            bp = dta.data;
            LOGCOPY_32(&rectype, bp);

            if (logical_rectype(rectype)) {
                *ll_lsn = lsn;
                goto done;
            }

            bp += sizeof(u_int32_t);
            LOGCOPY_32(&txnid, bp);

            bp += sizeof(u_int32_t);
            LOGCOPY_TOLSN(&lsn, bp);
        } while (lsn.file != 0);
    } while (commit_lsn.file != 0);

done:
    if (cur)
        cur->close(cur, 0);
    if (dta.data)
        free(dta.data);

    return ret;
}

static int get_ltranid_from_log(bdb_state_type *bdb_state, DBT *logdta,
                                unsigned long long *tranid,
                                unsigned long long *genid, void **dta,
                                DB_LSN *lsn)
{
    u_int32_t rectype;
    void *logp = NULL;
    int rc = 0;

    llog_ltran_commit_args *commit;
    llog_ltran_start_args *start;
    llog_ltran_comprec_args *comprec;

    llog_undo_add_dta_lk_args *add_dta_lk;
    llog_undo_add_ix_lk_args *add_ix_lk;
    llog_undo_del_dta_lk_args *del_dta_lk;
    llog_undo_del_ix_lk_args *del_ix_lk;
    llog_undo_upd_dta_lk_args *upd_dta_lk;
    llog_undo_upd_ix_lk_args *upd_ix_lk;

    LOGCOPY_32(&rectype, logdta->data);
    *tranid = 0;
    *genid = 0;

    switch (rectype) {
    case DB_llog_ltran_start:
        rc = llog_ltran_start_read(bdb_state->dbenv, logdta->data, &start);
        if (rc)
            return rc;
        logp = start;
        *tranid = start->ltranid;
        break;

    case DB_llog_ltran_commit:
        rc = llog_ltran_commit_read(bdb_state->dbenv, logdta->data, &commit);
        if (rc)
            return rc;
        logp = commit;
        *tranid = commit->ltranid;
        break;

    case DB_llog_ltran_comprec:
        rc = llog_ltran_comprec_read(bdb_state->dbenv, logdta->data, &comprec);
        if (rc)
            return rc;
        logp = comprec;
        *tranid = comprec->ltranid;
        break;

    case DB_llog_undo_add_dta_lk:
        rc = llog_undo_add_dta_lk_read(bdb_state->dbenv, logdta->data,
                                       &add_dta_lk);
        if (rc)
            return rc;
        logp = add_dta_lk;
        *tranid = add_dta_lk->ltranid;
        break;

    case DB_llog_undo_add_ix_lk:
        rc = llog_undo_add_ix_lk_read(bdb_state->dbenv, logdta->data,
                                      &add_ix_lk);
        if (rc)
            return rc;
        logp = add_ix_lk;
        *tranid = add_ix_lk->ltranid;
        break;

    case DB_llog_undo_del_dta_lk:
        rc = llog_undo_del_dta_lk_read(bdb_state->dbenv, logdta->data,
                                       &del_dta_lk);
        if (rc)
            return rc;
        logp = del_dta_lk;
        *tranid = del_dta_lk->ltranid;
        break;

    case DB_llog_undo_del_ix_lk:
        rc = llog_undo_del_ix_lk_read(bdb_state->dbenv, logdta->data,
                                      &del_ix_lk);
        if (rc)
            return rc;
        logp = del_ix_lk;
        *tranid = del_ix_lk->ltranid;
        break;

    case DB_llog_undo_upd_dta_lk:
        rc = llog_undo_upd_dta_lk_read(bdb_state->dbenv, logdta->data,
                                       &upd_dta_lk);
        if (rc)
            return rc;
        logp = upd_dta_lk;
        *tranid = upd_dta_lk->ltranid;
        break;

    case DB_llog_undo_upd_ix_lk:
        rc = llog_undo_upd_ix_lk_read(bdb_state->dbenv, logdta->data,
                                      &upd_ix_lk);
        if (rc)
            return rc;
        logp = upd_ix_lk;
        *tranid = upd_ix_lk->ltranid;
        break;

    /* Snapisol logical logging */
    case DB_llog_undo_add_dta:
    case DB_llog_undo_add_ix:
    case DB_llog_undo_del_dta:
    case DB_llog_undo_del_ix:
    case DB_llog_undo_upd_dta:
    case DB_llog_undo_upd_ix:
    /*
    fprintf(stderr, "Skip snapisol %s lsn %d:%d\n", rectypestr(rectype),
            lsn->file, lsn->offset);
     */

    /* Fall-through */
    default:
        *tranid = 0;
        logp = NULL;
    }
    if (logp)
        *dta = logp;
    return rc;
}

int dumptrans(void *tranp, void *bdb_statep)
{
    bdb_state_type *bdb_state = bdb_statep;
    tran_type *tran = tranp;

    logmsg(LOGMSG_USER, "%016llx\n", tran->logical_tranid);

    return 0;
}

/*
   This is the logical recovery routine.  It gets called after berkeley
   recovery completes, so all btrees are physically consistent when it
   runs.  This routine's job to to make sure transactions are logically
   consistent.  DBs shouldn't proceed on startup or upgrade/downgrade
   unless this is called and succeeds.

   Replicant recovery just grabs locks for all live transactions.
   Master recovery does actual recovery.  A master that just downgraded
   MUST call bdb_run_logical_recovery(bdb_state, 0) to reacquire locks
   before proceeding.
*/

extern int gbl_fullrecovery;

static int cancel_all_logical_transactions(bdb_state_type *bdb_state)
{
    tran_type *ltrans, *temp;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    pthread_mutex_lock(&bdb_state->translist_lk);
    LISTC_FOR_EACH_SAFE(&bdb_state->logical_transactions_list, ltrans, temp,
                        tranlist_lnk)
    {
        /* Sanity-check (recovery is holding the bdb writelock) */
        if (ltrans->got_bdb_lock)
            abort();

        /*
         * This will be done at logical commit time
         * bdb_release_ltran_locks(bdb_state, ltrans, ltrans->logical_lid);
         */
        listc_rfl(&bdb_state->logical_transactions_list, ltrans);
        hash_del(bdb_state->logical_transactions_hash, ltrans);

#if defined DEBUG_TXN_LIST
        ctrace("Removing logical_tranid 0x%llx func %s line %d\n",
               ltrans->logical_tranid, __func__, __LINE__);
#endif

        free(ltrans);
    }
    pthread_mutex_unlock(&bdb_state->translist_lk);
    return 0;
}

int llmeta_open(void);

/* XXX
 * Have to think about how logical recovery is going to look on the replicants -
 * now, all of the replicant rowlocks are acquired strictly in
 * rep_process_message
 * code
 *
 * 1. What do you do if you are the replicant & you upgrade to master?
 *    - Just purge all of the berkley level ltrans locks, and run this code
 * 2. What do you do if you are the master and are downgraded?
 *    - You have to drop your logical locks and build ltrans structures for
 *      each outstanding logical transaction
 *
 * .. this can wait until after my benchmarking
 */
int bdb_run_logical_recovery(bdb_state_type *bdb_state, int is_replicant)
{
    int rc, i;
    DB_LTRAN *ltranlist;
    DBT *data_dbt = {0};
    u_int32_t ltrancount;
    tran_type **bdb_tran = NULL;
    int bdberr;

    /* We also need to track lwm as recovery runs.  Getting it from the
       metatable
       is too conservative and prevents us from using the commitlsn trick on dbs
       that don't get writes very often.  Since this is recovery, we can set the
       lwm lsn to the lsn of the most recently committed transaction. */

    bdb_state->in_recovery = 1;

    if (rc = bdb_state->dbenv->get_ltran_list(bdb_state->dbenv, &ltranlist,
                                              &ltrancount))
        abort();

    cancel_all_logical_transactions(bdb_state);

    if (ltrancount == 0) {
        logmsg(LOGMSG_INFO, "%s: No outstanding logical txns!\n", __func__);
        return 0;
    }

    bdb_tran = malloc(sizeof(tran_type *) * ltrancount);

    for (i = 0; i < ltrancount; i++) {
        bdb_tran[i] = bdb_tran_continue_logical(bdb_state, ltranlist[i].tranid,
                                                0, &bdberr);
    }

    /* if we are a replicant, we are done */
    if (is_replicant)
        goto done;

    /* master runs phase 2: aborting transactions that didn't commit. */
    logmsg(LOGMSG_USER, "logical recovery phase 2\n");

    /* first check that for all active transactions we have a full transaction
     * log */
    logmsg(LOGMSG_USER, "Active transactions to be aborted:\n");
    for (i = 0; i < ltrancount; i++) {
        logmsg(LOGMSG_USER, "tranid %016llx, last lsn %u:%u, start lsn %u:%u\n",
               ltranlist[i].tranid, ltranlist[i].last_lsn.file,
               ltranlist[i].last_lsn.offset, ltranlist[i].begin_lsn.file,
               ltranlist[i].begin_lsn.offset);
    }

    for (i = 0; i < ltrancount; i++) {
        DB_LSN ll_lsn;
        bdb_tran[i]->wrote_begin_record = 1;
        bdb_tran[i]->committed_begin_record = 1;
        bdb_tran[i]->is_about_to_commit = 0;
        rc = find_last_logical_lsn(bdb_state, &ltranlist[i].last_lsn, &ll_lsn);
        if (rc)
            abort();

        bdb_tran[i]->last_logical_lsn = ll_lsn;
        bdb_tran[i]->last_physical_commit_lsn = ll_lsn;
        bdb_tran[i]->begin_lsn = ltranlist[i].begin_lsn;
        rc = bdb_tran_abort(bdb_state, bdb_tran[i], &bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR, "abort abort rc %d bdberr %d\n", rc, bdberr);
            goto done;
        }
    }

    bdb_state->dbenv->ltran_count(bdb_state->dbenv, &ltrancount);
    assert(0 == ltrancount);

done:
    if (bdb_tran)
        free(bdb_tran);
    if (ltranlist)
        free(ltranlist);

    bdb_state->in_recovery = 0;
    return rc;
}

static int free_it(void *obj, void *arg)
{
    free(obj);
    return 0;
}

static void free_hash(hash_t *h)
{
    hash_for(h, free_it, NULL);
    hash_clear(h);
    hash_free(h);
}

/* This is called by tran_abort when it's called on a logical transaction.
   Gets a log cursor, finds the last logical log record, and
   traverses the log in reverse lsn order for that logical
   transaction.  Doesn't actually get rid of transaction memory. This code
   is also called by bdb_run_logical_recovery (master case) to abort a
   partial transaction
*/
int abort_logical_transaction(bdb_state_type *bdb_state, tran_type *tran,
                              DB_LSN *outlsn, int about_to_commit)
{
    DB_LOGC *cur;
    int rc = 0, deadlkcnt = 0;
    int did_something = 0;
    DBT logdta;
    DB_LSN lsn, undolsn, getlsn, start_phys_txn;
    u_int32_t rectype;

    tran->aborted = 1;

    if (!outlsn)
        outlsn = &getlsn;

    /* There's nothing to do if I didn't write anything */
    if (!tran->committed_begin_record)
        goto done;

    /* get a log cursor */
    rc = bdb_state->dbenv->log_cursor(bdb_state->dbenv, &cur, 0);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s:%d log_cursor rc %d\n", __FILE__, __LINE__, rc);
        return rc;
    }

    /* Use this every time we see a compensation record.  Put the compensated
       lsn into this hash.  If we see an lsn in the hash, skip it since the
       compensating transaction already took care of it. */
    tran->compensated_records =
        hash_init_o(offsetof(struct remembered_record, lsn), sizeof(DB_LSN));

    bzero(&logdta, sizeof(DBT));
    logdta.flags = DB_DBT_REALLOC;

    lsn = tran->last_physical_commit_lsn;
    rc = cur->get(cur, &lsn, &logdta, DB_SET);

    if (tran->physical_tran)
        abort();

    LOGCOPY_32(&rectype, logdta.data);

    /* Can happen during logical recovery */
    if (rectype == DB___txn_regop_rowlocks) {
        __txn_regop_rowlocks_args *txn_rl_args = NULL;
        rc = __txn_regop_rowlocks_read(bdb_state->dbenv, logdta.data,
                                       &txn_rl_args);
        if (rc != 0)
            abort();

        rc = cur->get(cur, &txn_rl_args->prev_lsn, &logdta, DB_SET);
        free(txn_rl_args);
        LOGCOPY_32(&rectype, logdta.data);
    }

    while (rc == 0 && rectype != DB_llog_ltran_start) {

#if 0
        printf("lsn %d:%d type %d\n", lsn.file, lsn.offset, rectype);
#endif

    again:
        if (tran->physical_tran == NULL) {
            memcpy(&start_phys_txn, &lsn, sizeof(lsn));
        }

        if (lsn.file == 0 && lsn.offset == 1) {
            logmsg(LOGMSG_ERROR, "reached last lsn but not a begin record type %d?\n",
                   rectype);
            break;
        }

        if (rc) {
            logmsg(LOGMSG_ERROR, "%s:%d log get %d:%d rc %d\n", __FILE__, __LINE__, lsn.file,
                   lsn.offset, rc);
            if (logdta.data)
                free(logdta.data);
            cur->close(cur, 0);
            goto done;
        }

        undolsn = lsn;
        rc = undo_physical_transaction(bdb_state, tran, &logdta, &did_something,
                                       &undolsn, &lsn);
        if (rc == DB_LOCK_DEADLOCK) {
            assert(tran->physical_tran != NULL);
            bdb_tran_abort_phys(bdb_state, tran->physical_tran);
            memcpy(&lsn, &start_phys_txn, sizeof(lsn));
            rc = cur->get(cur, &lsn, &logdta, DB_SET);
            LOGCOPY_32(&rectype, logdta.data);
            ++deadlkcnt;
            goto again;
        }

        if (rc) {
            /* Forces a log flush */
            bdb_state->dbenv->memp_sync(bdb_state->dbenv, NULL);
            /* bdb_state->dbenv->log_flush(bdb_state->dbenv, NULL); */
            logmsg(LOGMSG_FATAL, 
                    "failed to undo physical transaction at %u:%u rc %d\n",
                    lsn.file, lsn.offset, rc);
            abort();
        }

        if (did_something) {
            /* Will be NULL if we've seen nothing but comprecs */
            assert(tran->physical_tran != NULL);
            if (tran->physical_tran)
                bdb_tran_commit_phys(bdb_state, tran->physical_tran);
        }

        rc = cur->get(cur, &lsn, &logdta, DB_SET);
        LOGCOPY_32(&rectype, logdta.data);
    }
    if (rc) {
        if (logdta.data)
            free(logdta.data);
        cur->close(cur, 0);
        goto done;
    }

    if (logdta.data)
        free(logdta.data);
    cur->close(cur, 0);

    rc = 0;
    if (tran->committed_begin_record &&
        bdb_state->repinfo->myhost == bdb_state->repinfo->master_host) {
        tran_type *physical_tran;
        DB_LSN prev;

        prev = tran->last_logical_lsn;

        if (!tran->physical_tran) {
            bdb_tran_begin_phys(bdb_state, tran);
        }

        /* write a commit (really an abort) record */
        rc = bdb_llog_commit(bdb_state, tran->physical_tran, 1);
        if (rc) {
            bdb_tran_abort_phys(bdb_state, tran->physical_tran);
            goto done;
        }
        if (about_to_commit) {
            tran->is_about_to_commit = 1;
            rc = bdb_tran_commit_phys_getlsn(bdb_state, tran->physical_tran,
                                             outlsn);
            if (rc)
                goto done;
        }
    }
done:
    if (tran->compensated_records) {
        free_hash(tran->compensated_records);
        tran->compensated_records = NULL;
    }

    return rc;
}

static inline tran_type *find_logical_transaction(bdb_state_type *bdb_state,
                                                  unsigned long long ltranid,
                                                  DB_LSN *first_lsn)
{
    bdb_state_type *parent = bdb_state->parent;
    int bdberr;
    tran_type *ltrans = NULL;

    if (parent == NULL)
        parent = bdb_state;

    if (!parent->passed_dbenv_open)
        return NULL;

    pthread_mutex_lock(&parent->translist_lk);
    ltrans = hash_find(parent->logical_transactions_hash, &ltranid);
    pthread_mutex_unlock(&parent->translist_lk);
    if (ltrans == NULL) {
        ltrans = bdb_tran_continue_logical(bdb_state, ltranid, 0, &bdberr);
        if (ltrans == NULL) {

            logmsg(LOGMSG_ERROR, 
                    "failed to continue transaction %016llx, bdberr = %d\n",
                    ltranid, bdberr);
            return NULL;
        }
        /* we process records in log order, so if this is the first
           time we see this transaction, this must be the first record
           for that transaction. */

        bdb_update_startlsn(ltrans, first_lsn);
    }
    return ltrans;
}

int rowlocks_bdb_lock_check(bdb_state_type *bdb_state)
{
    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    if (bdb_state->bdb_lock_desired &&
        bdb_state->bdb_lock_write_holder != pthread_self()) {
        return 1;
    }

    return 0;
}

int bdb_tran_commit_with_seqnum_int(bdb_state_type *bdb_state, tran_type *tran,
                                    seqnum_type *seqnum, int *bdberr,
                                    int getseqnum, uint64_t *out_txnsize,
                                    void *blkseq, int blklen, void *blkkey,
                                    int blkkeylen);

static int logical_release_transaction(bdb_state_type *bdb_state,
                                       unsigned long long ltranid,
                                       int repcommit)
{
    tran_type *ltrans;
    int lockerid;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    pthread_mutex_lock(&bdb_state->translist_lk);
    ltrans = hash_find(bdb_state->logical_transactions_hash, &ltranid);
    if (ltrans == NULL) {
        pthread_mutex_unlock(&bdb_state->translist_lk);
        logmsg(LOGMSG_DEBUG, 
                "asked to release locks for an empty transaction %016llx?\n",
                ltranid);
        return 0;
    }

    /* Remove this immediately */
    hash_del(bdb_state->logical_transactions_hash, ltrans);
    listc_rfl(&bdb_state->logical_transactions_list, ltrans);

#if defined DEBUG_TXN_LIST
    ctrace("Removing logical_tranid 0x%llx func %s line %d\n",
           ltrans->logical_tranid, __func__, __LINE__);
#endif

    if (listc_size(&bdb_state->logical_transactions_list) > 0) {
        DB_LSN *top = &bdb_state->logical_transactions_list.top->startlsn;

        if (top->file > 0) {
            bdb_state->lwm.file =
                bdb_state->logical_transactions_list.top->startlsn.file;
            bdb_state->lwm.offset =
                bdb_state->logical_transactions_list.top->startlsn.offset;
        }
    }

    /* Grab the lockerid & set it to 0 in the structure */
    lockerid = ltrans->logical_lid;
    ltrans->logical_lid = 0;

    pthread_mutex_unlock(&bdb_state->translist_lk);

    /* This shouldn't happen here */
    if (repcommit && ltrans->got_bdb_lock) {
        abort();
    }

    pthread_setspecific(bdb_state->seqnum_info->key, NULL);
    free(ltrans);

    return 0;
}

int logical_commit_replicant(bdb_state_type *bdb_state,
                             unsigned long long ltranid)
{
    return logical_release_transaction(bdb_state, ltranid, 1);
}

int release_locks_for_logical_transaction_object(bdb_state_type *bdb_state,
                                                 tran_type *tran, int *bdberr)
{
    int rc;
    seqnum_type seqnum;
    uint64_t sz;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    rc = bdb_tran_commit_with_seqnum_int(bdb_state, tran, &seqnum, bdberr, 1,
                                         &sz, NULL, 0, NULL, 0);
    if (rc) {
       logmsg(LOGMSG_ERROR, "ltranid %016llx release locks commit rc %d bdberr %d\n",
               tran->logical_tranid, rc, *bdberr);
        return rc;
    }
    return 0;
}

static int release_locks_for_logical_transaction(bdb_state_type *bdb_state,
                                                 unsigned long long ltranid)
{
    tran_type *ltrans;
    int rc = 0;
    int bdberr;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    pthread_mutex_lock(&bdb_state->translist_lk);
    ltrans = hash_find(bdb_state->logical_transactions_hash, &ltranid);
    if (ltrans == NULL) {
        pthread_mutex_unlock(&bdb_state->translist_lk);
        logmsg(LOGMSG_DEBUG, "asked to release locks for an empty transaction %016llx?\n",
                ltranid);
        return 0;
    }
    pthread_mutex_unlock(&bdb_state->translist_lk);

    rc = release_locks_for_logical_transaction_object(bdb_state, ltrans,
                                                      &bdberr);
    if (rc)
        logmsg(LOGMSG_ERROR, "release_locks_for_logical_transaction_object %016llx rc %d "
               "bdberr %d\n",
               ltrans->logical_tranid, rc, bdberr);
    return rc;
}

static char *opstr(db_recops op)
{
    switch (op) {
    case DB_TXN_BACKWARD_ROLL:
        return "backward";
    case DB_TXN_ABORT:
        return "abort";
    case DB_TXN_FORWARD_ROLL:
        return "forward";
    case DB_TXN_APPLY:
        return "apply";
    case DB_TXN_PRINT:
        return "print";
    case DB_TXN_SNAPISOL:
        return "snapisol";
    default:
        return "unknown";
    }
}

unsigned char *printmemarg1(void)
{
    static unsigned char *u = NULL;
    if (!u)
        u = malloc(MAXBLOBLENGTH + 7);
    return u;
}

unsigned char *printmemarg2(void)
{
    static unsigned char *u = NULL;
    if (!u)
        u = malloc(MAXBLOBLENGTH + 7);
    return u;
}

/* recovery routine for an "add" record */
#ifndef BERKDB_46
int handle_undo_add_dta(DB_ENV *dbenv, u_int32_t rectype,
                        llog_undo_add_dta_args *addop, DB_LSN *lsn,
                        db_recops op)
{
    int rc = 0;
    bdb_state_type *parent;
    DB_LSN *lprev;
    bdb_state_type *bdb_state;
    tran_type *ltrans = NULL;

    bdb_state = dbenv->app_private;
    parent = bdb_state->parent;

    /* comdb2_db_printlog calls this with a null state, so handle it.
       We only need to handle it for DB_TXN_PRINT */
    if (bdb_state && !bdb_state->passed_dbenv_open)
        goto done;

    lprev = &addop->prevllsn;

    switch (op) {
    /* for an UNDO record, berkeley expects us to set prev_lsn */
    case DB_TXN_BACKWARD_ROLL:
    case DB_TXN_ABORT:
        rc = 0;
        break;

    case DB_TXN_FORWARD_ROLL:
    case DB_TXN_APPLY:
        if (bdb_state->attr->shadows_nonblocking) {
            rc = update_shadows_beforecommit(bdb_state, lsn, NULL, 0);
            if (rc) {
                logmsg(LOGMSG_USER, "%s:%d update_shadows_beforecommit for "
                       "tranid %016llx rc %d\n",
                       __FILE__, __LINE__, addop->ltranid, rc);
                goto done;
            }
        }

        break;

    case DB_TXN_SNAPISOL:
    case DB_TXN_PRINT:
        lprev = &addop->prevllsn;
        /* keep format similar to berkeley - except for the raw data -
           dump that in more readable format */
        printf("[%lu][%lu] CUSTOM: add_dta: rec: %lu txnid %lx"
               " prevlsn[%lu][%lu]  prevllsn[%lu][%lu] tranid %016llx",
               (u_long)lsn->file, (u_long)lsn->offset, (u_long)rectype,
               (u_long)addop->txnid->txnid, (u_long)addop->prev_lsn.file,
               (u_long)addop->prev_lsn.offset, (u_long)lprev->file,
               (u_long)lprev->offset, addop->ltranid);
        printf("\ttable:     %.*s\n", addop->table.size,
               (char *)addop->table.data);
        printf("\tdtafile:   %d\n", addop->dtafile);
        printf("\tstripe:    %d\n", addop->dtastripe);
        printf("\tgenid:     %016llx\n", addop->genid);
        printf("\tlock:      ");
        hexdumpdbt(&addop->lock);
        printf("\n");
        printf("\n");
        {
            DB_LSN lll = addop->prev_lsn;
            unsigned long long lllgenid;
            /* Large memory allocation!  This code is only called in
             * db_printlog. */
            static char *llldta = NULL;
            int iii;
            int lllout = 0;

            if (!llldta)
                llldta = printmemarg1();
            bdb_reconstruct_add(bdb_state, &lll, &lllgenid,
                                sizeof(unsigned long long), llldta,
                                MAXBLOBLENGTH + 7, &lllout);

            printf(" --genid %16llx\n", lllgenid);
            printf(" --dta [%d]  ", lllout);
            for (iii = 0; iii < lllout; iii++) {
                printf("%02x", (unsigned char)llldta[iii]);
            }
            printf("\n\n");
        }
        break;

    default:
        __db_err(dbenv, "unknown op type %d in"
                        " handle_undo_add_dta\n",
                 (int)op);
        break;
    }
done:

    *lsn = addop->prev_lsn;
    return rc;
}

int handle_undo_add_dta_lk(DB_ENV *dbenv, u_int32_t rectype,
                           llog_undo_add_dta_lk_args *addop, DB_LSN *lsn,
                           db_recops op)
{
    int rc = 0;
    DB_LSN *lprev;
    bdb_state_type *bdb_state;

    tran_type *ltrans;

    bdb_state = dbenv->app_private;

    /* comdb2_db_printlog calls this with a null state, so handle it.
       We only need to handle it for DB_TXN_PRINT */
    if (bdb_state && !bdb_state->passed_dbenv_open)
        goto done;

    lprev = &addop->prevllsn;

    switch (op) {
    /* for an UNDO record, berkeley expects us to set prev_lsn */
    case DB_TXN_BACKWARD_ROLL:
    case DB_TXN_ABORT:
        rc = 0;
        break;

    case DB_TXN_FORWARD_ROLL:
    case DB_TXN_APPLY:
        if (bdb_state->attr->shadows_nonblocking) {
            rc = update_shadows_beforecommit(bdb_state, lsn, NULL, 0);
            if (rc) {
                logmsg(LOGMSG_ERROR, "%s:%d update_shadows_beforecommit for "
                       "tranid %016llx rc %d\n",
                       __FILE__, __LINE__, addop->ltranid, rc);
                goto done;
            }
        }

        break;

    case DB_TXN_SNAPISOL:
    case DB_TXN_PRINT:
        lprev = &addop->prevllsn;
        /* keep format similar to berkeley - except for the raw data -
           dump that in more readable format */
        printf("[%lu][%lu] CUSTOM: add_dta_lk: rec: %lu txnid %lx"
               " prevlsn[%lu][%lu]  prevllsn[%lu][%lu] tranid %016llx",
               (u_long)lsn->file, (u_long)lsn->offset, (u_long)rectype,
               (u_long)addop->txnid->txnid, (u_long)addop->prev_lsn.file,
               (u_long)addop->prev_lsn.offset, (u_long)lprev->file,
               (u_long)lprev->offset, addop->ltranid);
        printf("\ttable:     %.*s\n", addop->table.size,
               (char *)addop->table.data);
        printf("\tdtafile:   %d\n", addop->dtafile);
        printf("\tstripe:    %d\n", addop->dtastripe);
        printf("\tgenid:     %016llx\n", addop->genid);
        /*printf("\tlock:      "); hexdumpdbt(&addop->lock);*/
        printf("\n");
        printf("\n");
        {
            DB_LSN lll = addop->prev_lsn;
            unsigned long long lllgenid;
            static char *llldta = NULL;
            int iii;
            int lllout = 0;

            /* Large memory allocation!  This code is only called in
             * db_printlog. */
            if (!llldta)
                llldta = printmemarg1();
            bdb_reconstruct_add(bdb_state, &lll, &lllgenid,
                                sizeof(unsigned long long), llldta,
                                MAXBLOBLENGTH + 7, &lllout);

            printf(" --genid %16llx\n", lllgenid);
            printf(" --dta [%d]  ", lllout);
            for (iii = 0; iii < lllout; iii++) {
                printf("%02x", (unsigned char)llldta[iii]);
            }
            printf("\n");
            printf("\n");
        }
        break;

    default:
        __db_err(dbenv, "unknown op type %d in"
                        " handle_undo_add_dta\n",
                 (int)op);
        break;
    }
done:
    *lsn = addop->prev_lsn;
    return rc;
}

#endif

/*
   A mostly dummy recovery routine.
   actions:
   DB_TXN_BACKWARD_ROLL: none
   DB_TXN_FORWARD_ROLL:
   DB_TXN_ABORT: none
   DB_TXN_APPLY: grab lock
   DB_TXN_PRINT: print it
   The real version would have well-defined DB_TXN_ABORT action (start
   compensation transaction, do the logical opposite of the logged record)
*/
int handle_undo_add_ix(DB_ENV *dbenv, u_int32_t rectype,
                       llog_undo_add_ix_args *addop, DB_LSN *lsn, db_recops op)
{
    int rc = 0;
    DB_LSN *lprev;
    bdb_state_type *bdb_state;
    tran_type *ltrans = NULL;

    bdb_state = dbenv->app_private;

    /* don't do anything during berkeley recovery */
    if (bdb_state && !bdb_state->passed_dbenv_open)
        goto done;

    switch (op) {
    /* for an UNDO record, berkeley expects us to set prev_lsn */
    case DB_TXN_BACKWARD_ROLL:
    case DB_TXN_ABORT:
        rc = 0;
        break;

    case DB_TXN_FORWARD_ROLL:
    case DB_TXN_APPLY:
        if (bdb_state->attr->shadows_nonblocking) {
            rc = update_shadows_beforecommit(bdb_state, lsn, NULL, 0);
            if (rc) {
                logmsg(LOGMSG_USER, "%s:%d update_shadows_beforecommit for "
                       "tranid %016llx rc %d\n",
                       __FILE__, __LINE__, addop->ltranid, rc);
                goto done;
            }
        }

        break;

    case DB_TXN_SNAPISOL:
    case DB_TXN_PRINT:
        lprev = &addop->prevllsn;
        /* keep format similar to berkeley - except for the raw data -
           dump that in more readable format */
        printf("[%lu][%lu] CUSTOM: add_ix: %lu txnid %lx prevlsn[%lu][%lu]  "
               "prevllsn[%lu][%lu] tranid %016llx\n",
               (u_long)lsn->file, (u_long)lsn->offset, (u_long)rectype,
               (u_long)addop->txnid->txnid, (u_long)addop->prev_lsn.file,
               (u_long)addop->prev_lsn.offset, (u_long)lprev->file,
               (u_long)lprev->offset, addop->ltranid);
        printf("\ttable:  %.*s\n", addop->table.size,
               (char *)addop->table.data);
        printf("\tgenid:  %016llx\n", addop->genid);
        printf("\tix:     %d\n", addop->ix);
        printf("\tkey:    ");
        printf("\n");
        {
            DB_LSN lll = addop->prev_lsn;
            unsigned long long lllgenid;
            static char *llldta = NULL;
            int iii;
            if (!llldta)
                llldta = printmemarg1();
            bdb_reconstruct_add(bdb_state, &lll, llldta, addop->keylen,
                                &lllgenid, sizeof(unsigned long long), NULL);

            printf(" --genid %16llx\n", lllgenid);
            printf(" --dta [%d]  ", addop->keylen);
            for (iii = 0; iii < addop->keylen; iii++) {
                printf("%02x", (unsigned char)llldta[iii]);
            }
            printf("\n");
        }
        printf("\n");
        break;

    default:
        __db_err(dbenv, "unknown op type %d in handle_undo_add_ix\n", (int)op);
        break;
    }
done:
    if (DB_LOCK_NOTGRANTED == rc) {
        rc = DB_LOCK_DEADLOCK;
    }
    *lsn = addop->prev_lsn;
    return rc;
}

int handle_undo_add_ix_lk(DB_ENV *dbenv, u_int32_t rectype,
                          llog_undo_add_ix_lk_args *addop, DB_LSN *lsn,
                          db_recops op)
{
    int rc = 0;
    DB_LSN *lprev;
    bdb_state_type *bdb_state;
    tran_type *ltrans;

    bdb_state = dbenv->app_private;

    /* don't do anything during berkeley recovery */
    if (bdb_state && !bdb_state->passed_dbenv_open)
        goto done;

    lprev = &addop->prevllsn;

    switch (op) {
    /* for an UNDO record, berkeley expects us to set prev_lsn */
    case DB_TXN_BACKWARD_ROLL:
    case DB_TXN_ABORT:
        rc = 0;
        break;

    case DB_TXN_FORWARD_ROLL:
    case DB_TXN_APPLY:

        if (bdb_state->attr->shadows_nonblocking) {
            rc = update_shadows_beforecommit(bdb_state, lsn, NULL, 0);
            if (rc) {
                logmsg(LOGMSG_USER, "%s:%d update_shadows_beforecommit for "
                       "tranid %016llx rc %d\n",
                       __FILE__, __LINE__, addop->ltranid, rc);
                goto done;
            }
        }

        break;

    case DB_TXN_SNAPISOL:
    case DB_TXN_PRINT:
        lprev = &addop->prevllsn;
        /* keep format similar to berkeley - except for the raw data -
           dump that in more readable format */
        printf("[%lu][%lu] CUSTOM: add_ix_lk: %lu txnid %lx prevlsn[%lu][%lu]  "
               "prevllsn[%lu][%lu] tranid %016llx\n",
               (u_long)lsn->file, (u_long)lsn->offset, (u_long)rectype,
               (u_long)addop->txnid->txnid, (u_long)addop->prev_lsn.file,
               (u_long)addop->prev_lsn.offset, (u_long)lprev->file,
               (u_long)lprev->offset, addop->ltranid);
        printf("\ttable:    %.*s\n", addop->table.size,
               (char *)addop->table.data);
        printf("\tgenid:    %016llx\n", addop->genid);
        /*printf("\tlock:     "); hexdumpdbt(&addop->lock);*/
        printf("\tix:       %d\n", addop->ix);
        printf("\tkey:      ");
        hexdumpdbt(&addop->key);
        printf("\n");
        printf("\n");
        break;

    default:
        __db_err(dbenv, "unknown op type %d in handle_undo_add_ix\n", (int)op);
        break;
    }
done:
    *lsn = addop->prev_lsn;
    return rc;
}

#ifdef COUNT_REP_LTRANS
static pthread_mutex_t lstlk = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t lcmlk = PTHREAD_MUTEX_INITIALIZER;

static unsigned long long gbl_rep_count_logical_starts = 0;
static unsigned long long gbl_rep_count_logical_commits = 0;
#endif

void print_logical_commits_starts(FILE *f)
{
    unsigned long long lst = 0, lcm = 0;
#ifdef COUNT_REP_LTRANS
    pthread_mutex_lock(&lstlk);
    lst = gbl_rep_count_logical_starts;
    pthread_mutex_unlock(&lstlk);

    pthread_mutex_lock(&lcmlk);
    lcm = gbl_rep_count_logical_commits;
    pthread_mutex_unlock(&lcmlk);

    logmsg(LOGMSG_USER, "%llu logical starts\n", lst);
    logmsg(LOGMSG_USER, "%llu logical commits\n", lcm);
#endif
}

int handle_commit(DB_ENV *dbenv, u_int32_t rectype,
                  llog_ltran_commit_args *args, DB_LSN *lsn,
                  unsigned long long *commit_genid, db_recops op)
{

    int rc = 0;
    DB_LSN *lprev;
    bdb_state_type *bdb_state;
    DB_LSN commit_lsn = *lsn;
    uint8_t *p, *p_end;

    bdb_state = dbenv->app_private;

#ifdef COUNT_REP_LTRANS
    pthread_mutex_lock(&lcmlk);
    gbl_rep_count_logical_commits++;
    pthread_mutex_unlock(&lcmlk);
#endif

    /* don't do anything during berkeley recovery */
    if (bdb_state && !bdb_state->passed_dbenv_open)
        goto done;

    /* IMPORTANT!  Don't send a NEWSEQ here!  Early-ack already sends it for the
     * txn_regop.  Sending it here can move the seqnum backwards */
    lprev = &args->prevllsn;
    if (bdb_state) /* won't be set in comdb2_db_printlog */
        set_gblcontext(bdb_state, args->gblcontext);

    switch (op) {
    case DB_TXN_FORWARD_ROLL:
        break;
    case DB_TXN_APPLY:

        if (!bdb_state->attr->shadows_nonblocking && !args->isabort) {
            rc = update_shadows_beforecommit(bdb_state, &commit_lsn,
                                             commit_genid, 0);
            if (rc) {
                logmsg(LOGMSG_ERROR, "%s:%d update_shadows_beforecommit for "
                       "tranid %016llx rc %d\n",
                       __FILE__, __LINE__, args->ltranid, rc);
                goto done;
            }
        }

        break;

    case DB_TXN_BACKWARD_ROLL:
        break;

    case DB_TXN_ABORT:
        rc = 0;
        break;

    case DB_TXN_SNAPISOL:
        if (bdb_state->attr->snapisol) {
            if (!args->isabort) {
                rc = update_shadows_beforecommit(bdb_state, lprev, commit_genid,
                                                 0);
                if (rc) {
                    logmsg(LOGMSG_ERROR, "%s:%d update_shadows_beforecommit for "
                           "tranid %016llx rc %d\n",
                           __FILE__, __LINE__, args->ltranid, rc);
                }
            }
        }
        break;

    case DB_TXN_PRINT:
        lprev = &args->prevllsn;
        /* keep format similar to berkeley - except for the raw data -
           dump that in more readable format */
        printf("[%lu][%lu] CUSTOM: %s: %lu txnid %lx prevlsn[%lu][%lu]  "
               "prevllsn[%lu][%lu] tranid %016llx\n",
               (u_long)lsn->file, (u_long)lsn->offset,
               args->isabort ? "abort" : "commit", (u_long)rectype,
               (u_long)args->txnid->txnid, (u_long)args->prev_lsn.file,
               (u_long)args->prev_lsn.offset, (u_long)lprev->file,
               (u_long)lprev->offset, args->ltranid);
        printf("\n");
        break;

    default:
        __db_err(dbenv, "unknown op type %d in handle_commit\n", (int)op);
        break;
    }
done:
    *lsn = args->prev_lsn;
    return rc;
}

int handle_start(DB_ENV *dbenv, u_int32_t rectype, llog_ltran_start_args *args,
                 DB_LSN *lsn, db_recops op)
{

    int rc = 0;
    unsigned long long ltranid;
    tran_type *ltrans;
    bdb_state_type *bdb_state;
    int bdberr;

#ifdef COUNT_REP_LTRANS
    pthread_mutex_lock(&lstlk);
    gbl_rep_count_logical_starts++;
    pthread_mutex_unlock(&lstlk);
#endif

    bdb_state = dbenv->app_private;
    if (bdb_state && bdb_state->parent)
        bdb_state = bdb_state->parent;

    ltranid = args->ltranid;

    /* don't do anything during berkeley recovery */
    if (bdb_state && !bdb_state->passed_dbenv_open)
        goto done;

    /* this is a no-op.  we are still responsible for setting prev_lsn
       for berkeley, and for DB_TXN_PRINT, but there's nothing else to do */
    switch (op) {
    case DB_TXN_BACKWARD_ROLL:
        break;

    case DB_TXN_ABORT:
        rc = 0;
        break;

    case DB_TXN_FORWARD_ROLL:
    case DB_TXN_APPLY:
        break;

    case DB_TXN_SNAPISOL:
    case DB_TXN_PRINT:
        printf("[%lu][%lu] CUSTOM: start: %lu txnid %lx prevlsn[%lu][%lu] "
               "tranid %016llx\n",
               (u_long)lsn->file, (u_long)lsn->offset, (u_long)rectype,
               (u_long)args->txnid->txnid, (u_long)args->prev_lsn.file,
               (u_long)args->prev_lsn.offset, ltranid);
        printf("\n");
        break;

    default:
        __db_err(dbenv, "unknown op type %d in handle_start\n", (int)op);
        break;
    }
done:
    *lsn = args->prev_lsn;
    return rc;
}

#ifndef BERKDB_46

int handle_comprec(DB_ENV *dbenv, u_int32_t rectype,
                   llog_ltran_comprec_args *args, DB_LSN *lsn, db_recops op)
{

    int rc = 0;
    unsigned long long ltranid;
    bdb_state_type *bdb_state;
    DB_LSN undolsn, prevllsn;
    tran_type *ltrans = NULL;

    bdb_state = dbenv->app_private;
    ltranid = args->ltranid;

    /* don't do anything during berkeley recovery */
    if (bdb_state && !bdb_state->passed_dbenv_open)
        goto done;

    undolsn = args->complsn;
    prevllsn = args->prevllsn;

    /* another no-op */
    switch (op) {
    case DB_TXN_BACKWARD_ROLL:
    case DB_TXN_ABORT:
        rc = 0;
        break;

    case DB_TXN_APPLY:
    case DB_TXN_FORWARD_ROLL:
        break;

    case DB_TXN_SNAPISOL:
    case DB_TXN_PRINT:
        printf("[%lu][%lu] CUSTOM: comprec: %lu txnid %lx prevlsn[%lu][%lu] "
               "prevllsn[%lu][%lu] tranid %016llx\n",
               (u_long)lsn->file, (u_long)lsn->offset, (u_long)rectype,
               (u_long)args->txnid->txnid, (u_long)args->prev_lsn.file,
               (u_long)args->prev_lsn.offset, (u_long)prevllsn.file,
               (u_long)prevllsn.offset, ltranid);
        printf("\tcomplsn: %d:%d\n", undolsn.file, undolsn.offset);
        printf("\n");
        break;

    default:
        __db_err(dbenv, "unknown op type %d in handle_start\n", (int)op);
        break;
    }
done:
    *lsn = args->prev_lsn;
    return rc;
}

#endif

extern int gbl_disable_rowlocks_logging;

/* We have to pass the entire key in our logical logging.  This is because to
 * add a record, berkeley can 'addrem' the key and data, or it can 'bam_repl'
 * just the data portion if this key has been deleted.  For the replace case,
 * we cannot reconstruct the key from the logfiles. */
int bdb_llog_add_ix_lk(bdb_state_type *bdb_state, tran_type *tran, int ix,
                       unsigned long long genid, DBT *key, int dtalen)
{
    DBT dbt_tbl = {0};
    int rc;
    bdb_state_type *parent;
    DB_LSN lsn;

    parent = bdb_state->parent;

    if (gbl_disable_rowlocks_logging)
        return 0;

    /* table */
    dbt_tbl.data = bdb_state->name;
    dbt_tbl.size = strlen(bdb_state->name) + 1;

    /* chain lsn */
    lsn = tran->logical_tran->last_logical_lsn;

    rc = llog_undo_add_ix_lk_log(
        parent->dbenv, tran->tid, &tran->logical_tran->last_logical_lsn, 0,
        &dbt_tbl, ix, genid, tran->logical_tran->logical_tranid, &lsn, key,
        dtalen);
    if (rc)
        return rc;
    return 0;
}

int bdb_llog_del_dta_lk(bdb_state_type *bdb_state, tran_type *tran,
                        unsigned long long genid, DBT *dbt_data, int dtafile,
                        int dtastripe)
{
    int rc;
    bdb_state_type *parent = bdb_state->parent;

    DBT dbt_table = {0};

    dbt_table.size = strlen(bdb_state->name) + 1;
    dbt_table.data = bdb_state->name;

    rc = llog_undo_del_dta_lk_log(
        parent->dbenv, tran->tid, &tran->logical_tran->last_logical_lsn, 0,
        &dbt_table, genid, tran->logical_tran->logical_tranid,
        &tran->logical_tran->last_logical_lsn, dtafile, dtastripe,
        dbt_data->size);

    return rc;
}

int bdb_llog_del_ix_lk(bdb_state_type *bdb_state, tran_type *tran, int ixnum,
                       unsigned long long genid, DBT *dbt_key, int payloadsz)
{
    int rc;
    DBT dbt_table = {0};
    DB_LSN lsn;
    bdb_state_type *parent = bdb_state->parent;

    if (gbl_disable_rowlocks_logging)
        return 0;

    /* Last lsn */
    lsn = tran->logical_tran->last_logical_lsn;

    /* Table name */
    dbt_table.size = strlen(bdb_state->name) + 1;
    dbt_table.data = bdb_state->name;

    /* Write log */
    rc = llog_undo_del_ix_lk_log(
        parent->dbenv, tran->tid, &tran->logical_tran->last_logical_lsn, 0,
        &dbt_table, genid, ixnum, tran->logical_tran->logical_tranid, &lsn,
        dbt_key->size, payloadsz);

    return rc;
}

int bdb_llog_add_dta_lk(bdb_state_type *bdb_state, tran_type *tran,
                        unsigned long long genid, int dtafile, int dtastripe)
{
    DBT dbt_tbl = {0};
    int rc;
    DB_LSN lsn;
    bdb_state_type *parent;
    char rowname[ROWLOCK_KEY_SIZE];

    if (gbl_disable_rowlocks_logging)
        return 0;

    /* table name we are writing the record to */
    dbt_tbl.data = bdb_state->name;
    dbt_tbl.size = strlen(bdb_state->name) + 1;

    /*
       chain back to previous item for this logical transaction
       NOTE: the previous record will be in a different physical
       (ie: berkeley) transaction
    */
    lsn = tran->logical_tran->last_logical_lsn;

    parent = bdb_state->parent;
    rc = llog_undo_add_dta_lk_log(parent->dbenv, tran->tid,
                                  &tran->logical_tran->last_logical_lsn, 0,
                                  &dbt_tbl, dtafile, dtastripe, genid,
                                  tran->logical_tran->logical_tranid, &lsn);
    return rc;
}

int bdb_llog_start(bdb_state_type *bdb_state, tran_type *tran, DB_TXN *txn)
{
    DBT dbt_tranid = {0};
    int rc;

    /* Keep start and commit for last logical lsn */
    /* if(gbl_disable_rowlocks_logging) return 0; */

    rc = llog_ltran_start_log(bdb_state->dbenv, txn, &tran->last_logical_lsn, 0,
                              tran->logical_tranid, 0 /*dbnum-not yet*/);
    if (rc)
        return rc;
    memcpy(&tran->begin_lsn, &tran->last_logical_lsn, sizeof(DB_LSN));

    return 0;
}

int bdb_llog_comprec(bdb_state_type *bdb_state, tran_type *tran, DB_LSN *lsn)
{
    int rc;
    DBT dbt_prevllsn = {0};
    DBT dbt_complsn = {0};

    if (gbl_disable_rowlocks_logging)
        return 0;

    rc = llog_ltran_comprec_log(bdb_state->dbenv, tran->tid,
                                &tran->logical_tran->last_logical_lsn, 0,
                                tran->logical_tran->logical_tranid,
                                &tran->logical_tran->last_logical_lsn, lsn);

    return rc;
}

int llog_ltran_commit_log_wrap(DB_ENV *dbenv, DB_TXN *txnid, DB_LSN *ret_lsnp,
                               u_int32_t flags, u_int64_t ltranid,
                               DB_LSN *prevllsn, u_int64_t gblcontext,
                               short isabort)
{
    if (gbl_rowlocks && ltranid == 0)
        abort();

    return llog_ltran_commit_log(dbenv, txnid, ret_lsnp, flags, ltranid,
                                 prevllsn, gblcontext, isabort);
}

int bdb_llog_commit(bdb_state_type *bdb_state, tran_type *tran, int isabort)
{
    DBT dbt_tranid = {0}, dbt_prevllsn = {0};
    DB_LSN lsn;
    int rc;
    DBT dbt_gblcontext = {0};
    unsigned long long gblcontext;

    char str[100];

    if (gbl_disable_rowlocks_logging) {
        // tran->logical_tran->is_about_to_commit = 1;
        return 0;
    }

    lsn = tran->logical_tran->last_logical_lsn;
    dbt_prevllsn.data = &lsn;
    dbt_prevllsn.size = sizeof(DB_LSN);

    /* rep only marks a replication event for logical transactions perm
       if this is set */
    rc = llog_ltran_commit_log_wrap(bdb_state->dbenv, tran->tid,
                                    &tran->logical_tran->last_logical_lsn, 0,
                                    tran->logical_tran->logical_tranid, &lsn,
                                    get_gblcontext(bdb_state), isabort);

    // tran->logical_tran->is_about_to_commit = 1;

    memcpy(&tran->logical_tran->commit_lsn,
           &tran->logical_tran->last_logical_lsn, sizeof(DB_LSN));

    memcpy(&tran->last_logical_lsn, &tran->logical_tran->last_logical_lsn,
           sizeof(DB_LSN));

    if (rc)
        return rc;

    return 0;
}

int bdb_llog_upd_dta_lk(bdb_state_type *bdb_state, tran_type *tran,
                        unsigned long long oldgenid,
                        unsigned long long newgenid, int dtafile, int dtastripe,
                        DBT *dbt_olddta)
{
    int rc;
    DB_LSN lsn;
    DBT dbt_table = {0};

    if (gbl_disable_rowlocks_logging)
        return 0;

    lsn = tran->logical_tran->last_logical_lsn;
    dbt_table.data = bdb_state->name;
    dbt_table.size = strlen(bdb_state->name) + 1;

    rc = llog_undo_upd_dta_lk_log(
        bdb_state->dbenv, tran->tid, &tran->logical_tran->last_logical_lsn, 0,
        &dbt_table, oldgenid, newgenid, tran->logical_tran->logical_tranid,
        &lsn, dtafile, dtastripe, dbt_olddta->size);
    if (rc)
        return rc;

    return 0;
}

/* This is being deprecated for handle_undo_del_dta_lk. */
int handle_undo_del_dta(DB_ENV *dbenv, u_int32_t rectype,
                        llog_undo_del_dta_args *delop, DB_LSN *lsn,
                        db_recops op)
{
    unsigned long long ltranid, genid;
    bdb_state_type *bdb_state;
    DB_LSN *lprev;
    tran_type *ltrans;
    int rc = 0;

    bdb_state = dbenv->app_private;
    ltranid = delop->ltranid;

    /* don't do anything during berkeley recovery */
    if (bdb_state && !bdb_state->passed_dbenv_open)
        goto done;

    genid = delop->genid;
    switch (op) {
    case DB_TXN_BACKWARD_ROLL:
    case DB_TXN_ABORT:
        rc = 0;
        break;

    case DB_TXN_FORWARD_ROLL:
    case DB_TXN_APPLY:
        /* Snapshot databases which upgrade to rowlocks can hit this case in
         * recovery. */
        rc = 0;
        break;

    case DB_TXN_SNAPISOL:
    case DB_TXN_PRINT:
        lprev = &delop->prevllsn;
        printf("[%lu][%lu] CUSTOM: del_dta %lu txnid %lx prevlsn[%lu][%lu]  "
               "prevllsn[%lu][%lu]  tranid %016llx\n",
               (u_long)lsn->file, (u_long)lsn->offset, (u_long)rectype,
               (u_long)delop->txnid->txnid, (u_long)delop->prev_lsn.file,
               (u_long)delop->prev_lsn.offset, (u_long)lprev->file,
               (u_long)lprev->offset, ltranid);
        printf("\ttable:       %.*s\n", delop->table.size,
               (char *)delop->table.data);
        printf("\tgenid:       %016llx\n", genid);
        printf("\n");
        printf("\tdtafile:     %d\n", delop->dtafile);
        printf("\tdtastripe:   %d\n", delop->dtastripe);
        printf("\tlock:        ");
        hexdumpdbt(&delop->lock);
        printf("\n");
        {
            DB_LSN lll = delop->prev_lsn;
            unsigned long long lllgenid;
            static char *llldta = NULL;
            int iii;
            if (!llldta)
                llldta = printmemarg1();
            bdb_reconstruct_delete(bdb_state, &lll, NULL, NULL, &lllgenid,
                                   sizeof(unsigned long long), llldta,
                                   delop->dtalen, NULL);

            printf(" --genid %16llx\n", lllgenid);
            printf(" --dta   ");
            for (iii = 0; iii < delop->dtalen; iii++) {
                printf("%02x", (unsigned char)llldta[iii]);
            }
            printf("\n");
        }
        break;

    default:
        __db_err(dbenv, "unknown op type %d in handle_undo_del_dta\n", (int)op);
        break;
    }
done:
    *lsn = delop->prev_lsn;
    return rc;
}

int handle_undo_del_ix(DB_ENV *dbenv, u_int32_t rectype,
                       llog_undo_del_ix_args *delop, DB_LSN *lsn, db_recops op)
{
    unsigned long long ltranid, genid;
    bdb_state_type *bdb_state;
    DB_LSN *lprev;
    int rc = 0;
    tran_type *ltrans;

    bdb_state = dbenv->app_private;

    genid = delop->genid;
    ltranid = delop->ltranid;

    /* don't do anything during berkeley recovery */
    if (bdb_state && !bdb_state->passed_dbenv_open)
        goto done;

    switch (op) {
    case DB_TXN_BACKWARD_ROLL:
    case DB_TXN_ABORT:
        rc = 0;
        break;

    case DB_TXN_FORWARD_ROLL:
    case DB_TXN_APPLY:
        /* A fresh rowlocks database is operating on an old, snapshot logrecord.
         * Just return. */
        rc = 0;
        break;

    case DB_TXN_SNAPISOL:
    case DB_TXN_PRINT:
        lprev = &delop->prevllsn;
        printf("[%lu][%lu] CUSTOM: del_ix %lu txnid %lx prelsn[%lu][%lu]  "
               "prevllsn[%lu][%lu]  tranid %016llx\n",
               (u_long)lsn->file, (u_long)lsn->offset, (u_long)rectype,
               (u_long)delop->txnid->txnid, (u_long)delop->prev_lsn.file,
               (u_long)delop->prev_lsn.offset, (u_long)lprev->file,
               (u_long)lprev->offset, ltranid);
        printf("\ttable:   %.*s\n", delop->table.size,
               (char *)delop->table.data);
        printf("\tgenid:   %016llx\n", genid);
        printf("\tixnum:   %hd\n", delop->ix);
        printf("\n");
        {
            DB_LSN lll = delop->prev_lsn;
            static char *llldta = NULL;
            static char *lllkey = NULL;
            int iii;

            if (!llldta)
                llldta = printmemarg1();
            if (!lllkey)
                lllkey = printmemarg2();

            bdb_reconstruct_delete(bdb_state, &lll, NULL, NULL, lllkey,
                                   delop->keylen, llldta, delop->dtalen, NULL);

            printf(" --genid %16llx\n", genid);
            printf(" --dta   ");
            for (iii = 0; iii < delop->keylen; iii++) {
                printf("%02x", (unsigned char)lllkey[iii]);
            }
            printf("\n");
        }
        break;

    default:
        __db_err(dbenv, "unknown op type %d in handle_undo_del_ix\n", (int)op);
        break;
    }
done:
    *lsn = delop->prev_lsn;
    return rc;
}

int handle_undo_upd_dta(DB_ENV *dbenv, u_int32_t rectype,
                        llog_undo_upd_dta_args *updop, DB_LSN *lsn,
                        db_recops op)
{

    unsigned long long ltranid, oldgenid, newgenid;
    bdb_state_type *bdb_state;
    DB_LSN *lprev;
    int rc = 0;
    tran_type *ltrans;

    bdb_state = dbenv->app_private;
    ltranid = updop->ltranid;

    /* TODO: huh?  safe?  dropping event? */
    if (bdb_state && !bdb_state->passed_dbenv_open)
        goto done;

    oldgenid = updop->oldgenid;
    newgenid = updop->newgenid;

    switch (op) {
    case DB_TXN_BACKWARD_ROLL:
    case DB_TXN_ABORT:
        rc = 0;
        break;

    case DB_TXN_FORWARD_ROLL:
    case DB_TXN_APPLY:
        /* A fresh rowlocks database is operating on an old, snapshot logrecord.
         * Just return. */
        rc = 0;
        break;

    case DB_TXN_SNAPISOL:
    case DB_TXN_PRINT:
        lprev = &updop->prevllsn;
        printf("[%lu][%lu] CUSTOM: upd_dta %lu txnid %lx prelsn[%lu][%lu]  "
               "prevllsn[%lu][%lu]  tranid %016llx\n",
               (u_long)lsn->file, (u_long)lsn->offset, (u_long)rectype,
               (u_long)updop->txnid->txnid, (u_long)updop->prev_lsn.file,
               (u_long)updop->prev_lsn.offset, (u_long)lprev->file,
               (u_long)lprev->offset, ltranid);
        printf("\ttable:       %.*s\n", updop->table.size,
               (char *)updop->table.data);
        printf("\toldgenid:    %016llx\n", oldgenid);
        printf("\tnewenid:     %016llx\n", newgenid);
        printf("\tdtafile:     %d\n", updop->dtafile);
        printf("\tdtastripe:   %d\n", updop->dtastripe);
        printf("\tolddtalen:   %d\n", updop->old_dta_len);
        /*
        printf("\toldlock:     "); hexdumpdbt(&updop->lockold); printf("\n");
        printf("\tnewlock:     "); hexdumpdbt(&updop->locknew); printf("\n");
        */
        {
            DB_LSN lll = updop->prev_lsn;
            static char *llldta = NULL;
            unsigned long long lllgenid;
            int iii;
            int irc;
            int offset;
            if (!llldta)
                llldta = printmemarg1();
            irc = 0;
            if (0 == bdb_inplace_cmp_genids(bdb_state, updop->oldgenid,
                                            updop->newgenid)) {
                if (updop->old_dta_len > 0)
                    irc = bdb_reconstruct_inplace_update(
                        bdb_state, &lll, llldta, updop->old_dta_len, &offset,
                        NULL, NULL, NULL);
                // else
            } else {
                irc = bdb_reconstruct_update(
                    bdb_state, &lll, NULL, NULL, &lllgenid,
                    sizeof(unsigned long long), llldta, updop->old_dta_len);
            }
            if (irc)
                printf("%s %d rc=%d lsn [%d][%d]\n", __FILE__, __LINE__, irc,
                       lsn->file, lsn->offset);
            else {
                printf("--dta   ");
                for (iii = 0; iii < updop->old_dta_len; iii++) {
                    printf("02x", (unsigned char)llldta[iii]);
                }
                printf("\n");
            }
        }
        break;

    default:
        __db_err(dbenv, "unknown op type %d in handle_undo_upd_dta\n", (int)op);
        break;
    }
done:
    *lsn = updop->prev_lsn;
    return rc;
}

int handle_undo_upd_ix(DB_ENV *dbenv, u_int32_t rectype,
                       llog_undo_upd_ix_args *updop, DB_LSN *lsn, db_recops op)
{

    unsigned long long ltranid, oldgenid, newgenid;
    bdb_state_type *bdb_state;
    DB_LSN *lprev;
    tran_type *ltrans;
    int rc = 0;

    bdb_state = dbenv->app_private;
    ltranid = updop->ltranid;

    /* TODO: huh?  safe?  dropping event? */
    if (bdb_state && !bdb_state->passed_dbenv_open)
        return 0;

    oldgenid = updop->oldgenid;
    newgenid = updop->newgenid;

    switch (op) {
    case DB_TXN_BACKWARD_ROLL:
    case DB_TXN_ABORT:
        rc = 0;
        break;

    case DB_TXN_FORWARD_ROLL:
    case DB_TXN_APPLY:
        /* Just return */
        rc = 0;
        break;

    case DB_TXN_SNAPISOL:
    case DB_TXN_PRINT:
        lprev = &updop->prevllsn;
        printf("[%lu][%lu] CUSTOM: upd_ix %lu txnid %lx prelsn[%lu][%lu]  "
               "prevllsn[%lu][%lu]  tranid %016llx\n",
               (u_long)lsn->file, (u_long)lsn->offset, (u_long)rectype,
               (u_long)updop->txnid->txnid, (u_long)updop->prev_lsn.file,
               (u_long)updop->prev_lsn.offset, (u_long)lprev->file,
               (u_long)lprev->offset, ltranid);
        printf("\ttable:       %.*s\n", updop->table.size,
               (char *)updop->table.data);
        printf("oldgenid:    %016llx\n", oldgenid);
        printf("\tnewenid:     %016llx\n", newgenid);
        printf("dtalen:      %016u\n", updop->dtalen);
        printf("\tix:          %hd\n", updop->ix);
        printf("\n");
        {
            DB_LSN lll = updop->prev_lsn;
            char *llldta = NULL;
            int llloffset = 0;
            int llllen = 0;
            int iii;
            bdb_reconstruct_key_update(bdb_state, &lll, (void **)&llldta,
                                       &llloffset, &llllen);

            printf("--dta [%d:%d]  ", llloffset, llllen);
            for (iii = 0; iii < llllen; iii++) {
                printf("02x", (unsigned char)llldta[iii]);
            }
            printf("\n");
            free(llldta);
        }
        break;

    default:
        __db_err(dbenv, "unknown op type %d in handle_undo_upd_ix\n", (int)op);
        break;
    }
done:
    *lsn = updop->prev_lsn;
    return rc;
}

static int undo_upd_dta_lk(bdb_state_type *bdb_state, tran_type *tran,
                           llog_undo_upd_dta_lk_args *upd_dta_lk,
                           DB_LSN *undolsn, DB_LSN *prev, int just_load_lsn)
{
    char *tablename;
    bdb_state_type *table;
    unsigned long long oldgenid;
    unsigned long long newgenid;
    unsigned long long ltranid;
    int olddta_len;
    int rc;
    int inplace = 0;
    void *olddta = NULL;
    unsigned long long chkgenid;
    static int cnt = 0;

    cnt++;
    /* bdb_dump_log(bdb_state->dbenv, undolsn); */

    *prev = upd_dta_lk->prevllsn;
    if (just_load_lsn)
        return 0;

    tablename = upd_dta_lk->table.data;
    table = bdb_get_table_by_name(bdb_state, tablename);

    ltranid = upd_dta_lk->ltranid;
    oldgenid = upd_dta_lk->oldgenid;
    newgenid = upd_dta_lk->newgenid;
    olddta_len = upd_dta_lk->old_dta_len;

    if (olddta_len)
        olddta = malloc(olddta_len);

    if (olddta_len == 0) {
        assert(upd_dta_lk->dtafile != 0);
        rc = 0;
    } else if (0 == bdb_inplace_cmp_genids(table, oldgenid, newgenid)) {
        int offset, updlen;
        rc = bdb_reconstruct_inplace_update(bdb_state, undolsn, olddta,
                                            olddta_len, &offset, &updlen, NULL,
                                            NULL);
        if (0 == rc)
            assert(offset == 0 && updlen == olddta_len);
        inplace = 1;
    } else
        rc = bdb_reconstruct_update(bdb_state, undolsn, NULL, NULL, NULL, 0,
                                    olddta, olddta_len);

    if (rc == BDBERR_NO_LOG)
        goto done;

    if (rc) {
        logmsg(LOGMSG_ERROR, "Reconstruct for undo upd dta for lsn %u:%u ltranid "
                        "%016llx failed rc %d\n",
                undolsn->file, undolsn->offset, tran->logical_tranid, rc);
        abort();
    }

    if (inplace) {
        rc = ll_undo_inplace_upd_dta_lk(
            bdb_state, tran, tablename, oldgenid, newgenid, olddta, olddta_len,
            upd_dta_lk->dtafile, upd_dta_lk->dtastripe, undolsn);
    } else {
        rc = ll_undo_upd_dta_lk(bdb_state, tran, tablename, oldgenid, newgenid,
                                olddta, olddta_len, upd_dta_lk->dtafile,
                                upd_dta_lk->dtastripe, undolsn);
    }

done:
    if (rc && debug_comp)
        abort();
    free(olddta);

    return rc;
}

static int undo_upd_ix_lk(bdb_state_type *bdb_state, tran_type *tran,
                          llog_undo_upd_ix_lk_args *upd_ix_lk, DB_LSN *undolsn,
                          DB_LSN *prev, int just_load_lsn)
{
    char *key, *data = NULL;
    int rc;
    void *diff;
    int difflen;
    int offset;

    *prev = upd_ix_lk->prevllsn;
    if (just_load_lsn)
        return 0;

    rc = bdb_reconstruct_key_update(bdb_state, undolsn, &diff, &offset,
                                    &difflen);
    if (rc == BDBERR_NO_LOG) {
        goto done;
    }
    if (rc) {
        logmsg(LOGMSG_FATAL, "Reconstruct for undo upd ix for lsn %u:%u ltranid "
                        "%016x failed rc %d\n",
                undolsn->file, undolsn->offset, tran->logical_tran, rc);
        abort();
    }

    rc = ll_undo_upd_ix_lk(bdb_state, tran, upd_ix_lk->table.data,
                           upd_ix_lk->ix, upd_ix_lk->key.data,
                           upd_ix_lk->key.size, /*???? UNITIALIZED ????*/ data,
                           upd_ix_lk->dtalen, undolsn, diff, offset, difflen);

done:
    if (rc && debug_comp)
        abort();
    free(diff);

    return rc;
}

int bdb_oldest_outstanding_ltran(bdb_state_type *bdb_state, int *ltran_count,
                                 DB_LSN *oldest_ltran)
{
    DB_LTRAN *ltranlist;
    u_int32_t ltrancount;
    bdb_state_type *parent = bdb_state;
    DB_LSN oldest = {0};
    int idx, rc;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    if (rc = bdb_state->dbenv->get_ltran_list(bdb_state->dbenv, &ltranlist,
                                              &ltrancount))
        abort();

    if (ltrancount == 0) {
        *ltran_count = ltrancount;
        return 0;
    }

    for (idx = 0; idx < ltrancount; idx++) {
        if (oldest.file == 0 ||
            log_compare(&ltranlist[idx].begin_lsn, &oldest) < 0)
            oldest = ltranlist[idx].begin_lsn;
    }

    free(ltranlist);

    *oldest_ltran = oldest;
    *ltran_count = ltrancount;
    return 0;
}

int bdb_prepare_newsi_bkfill(bdb_state_type *bdb_state,
                             uint64_t **logical_txn_list,
                             int *logical_txn_count,
                             DB_LSN *oldest_logical_birth_lsn)
{
    DB_LTRAN *ltranlist;
    u_int32_t ltrancount;
    bdb_state_type *parent = bdb_state;
    DB_LSN oldest = {0};
    int idx, rc;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    if (rc = bdb_state->dbenv->get_ltran_list(bdb_state->dbenv, &ltranlist,
                                              &ltrancount))
        abort();

    if (ltrancount == 0) {
        *logical_txn_count = 0;
        *logical_txn_list = NULL;
        *oldest_logical_birth_lsn = (DB_LSN){.file = 0, .offset = 0};
        return 0;
    }

    *logical_txn_list = (uint64_t *)malloc(sizeof(uint64_t *) * ltrancount);

    for (idx = 0; idx < ltrancount; idx++) {
        (*logical_txn_list)[idx] = ltranlist[idx].tranid;
        if (oldest.file == 0 ||
            log_compare(&ltranlist[idx].begin_lsn, &oldest) < 0)
            oldest = ltranlist[idx].begin_lsn;
    }

    free(ltranlist);

    *oldest_logical_birth_lsn = oldest;
    *logical_txn_count = ltrancount;
    return 0;
}

int bdb_get_active_logical_transaction_lsns(bdb_state_type *bdb_state,
                                            DB_LSN **lsnout, int *numlsns,
                                            int *bdberr, tran_type *shadow_tran)
{
    tran_type *ltrans;
    bdb_state_type *parent;
    DB_LSN *lsns = NULL;
    int tran_num = 0;

    *bdberr = BDBERR_NOERROR;

    if (bdb_state->parent)
        parent = bdb_state->parent;
    else
        parent = bdb_state;

    pthread_mutex_lock(&parent->translist_lk);

    lsns = malloc(sizeof(DB_LSN) * parent->logical_transactions_list.count);
    if (!lsns) {
        *numlsns = parent->logical_transactions_list.count;
        pthread_mutex_unlock(&parent->translist_lk);
        if (*numlsns == 0)
            return 0;
        *bdberr = BDBERR_MALLOC;
        return -1;
    }

    if (shadow_tran) {
        shadow_tran->startgenid =
            bdb_get_current_lsn(bdb_state, &(shadow_tran->birth_lsn.file),
                                &(shadow_tran->birth_lsn.offset));
    }

    LISTC_FOR_EACH(&parent->logical_transactions_list, ltrans, tranlist_lnk)
    {
        if (ltrans->last_physical_commit_lsn.file != 0 &&
            ltrans->last_physical_commit_lsn.offset != 1) {
            lsns[tran_num++] = ltrans->last_physical_commit_lsn;
        }
    }
    *numlsns = tran_num;

    pthread_mutex_unlock(&parent->translist_lk);
    *lsnout = lsns;
    return 0;
}

int bdb_llog_upd_ix_lk(bdb_state_type *bdb_state, tran_type *tran,
                       char *table_name, void *key, int keylen, int ix,
                       int dtalen, unsigned long long oldgenid,
                       unsigned long long newgenid)
{
    int rc;
    DBT dbt_table = {0};
    DBT dbt_key = {0};
    DB_LSN lsn;

    if (gbl_disable_rowlocks_logging)
        return 0;

    /* Grab lsn */
    lsn = tran->logical_tran->last_logical_lsn;

    /* Set table */
    dbt_table.size = strlen(table_name) + 1;
    dbt_table.data = table_name;

    /* Set key */
    dbt_key.size = keylen;
    dbt_key.data = key;

    /* Write log message */
    rc = llog_undo_upd_ix_lk_log(
        bdb_state->dbenv, tran->tid, &tran->logical_tran->last_logical_lsn, 0,
        &dbt_table, oldgenid, newgenid, tran->logical_tran->logical_tranid,
        &lsn, ix, &dbt_key, dtalen);

    return rc;
}

int bdb_llog_rowlocks_bench(bdb_state_type *bdb_state, tran_type *tran, int op,
                            int arg1, int arg2, DBT *lock1, DBT *lock2,
                            void *payload, int paylen)
{
    int rc;
    uint64_t ltranid = 0;
    DBT pload = {0};
    DB_LSN lsn = {0}, *lsnp, fakelsn = {0};

    if (tran->logical_tran) {
        lsn = tran->logical_tran->last_logical_lsn;
        lsnp = &tran->logical_tran->last_logical_lsn;
        ltranid = tran->logical_tran->logical_tranid;
    } else {
        lsnp = &fakelsn;
    }
    pload.data = payload;
    pload.size = paylen;

    if (op == 0) {
        rc = llog_commit_log_bench_log(bdb_state->dbenv, tran->tid, lsnp, 0, op,
                                       arg1, arg2, ltranid, &pload, lock1,
                                       lock2, &lsn);
    } else {
        rc = llog_rowlocks_log_bench_log(bdb_state->dbenv, tran->tid, lsnp, 0,
                                         op, arg1, arg2, ltranid, &pload, lock1,
                                         lock2, &lsn);
    }

    return rc;
}

int handle_repblob(DB_ENV *dbenv, u_int32_t rectype, llog_repblob_args *repblob,
                   DB_LSN *lsn, db_recops op)
{

    if (op == DB_TXN_PRINT || op == DB_TXN_SNAPISOL) {
        printf("[%lu][%lu] CUSTOM: repblob: rec: %lu txnid %lx"
               " prevlsn[%lu][%lu] sessionid %d seqno %d dtasz %d\n\n",
               lsn->file, lsn->offset, rectype, repblob->txnid->txnid,
               repblob->prev_lsn.file, repblob->prev_lsn.offset,
               repblob->sessionid, repblob->seqno, repblob->data.size);
    }

    *lsn = repblob->prev_lsn;

    return 0;
}

/* Prevlk includes the lock for the previous record. */
int handle_undo_del_dta_lk(DB_ENV *dbenv, u_int32_t rectype,
                           llog_undo_del_dta_lk_args *delop, DB_LSN *lsn,
                           db_recops op)
{
    bdb_state_type *bdb_state;
    DB_LSN *lprev;
    tran_type *ltrans;

    int rc = 0;

    bdb_state = dbenv->app_private;

    if (bdb_state->parent) {
        bdb_state = bdb_state->parent;
    }

    if (bdb_state && !bdb_state->passed_dbenv_open)
        goto done;

    switch (op) {
    case DB_TXN_BACKWARD_ROLL:
    case DB_TXN_ABORT:
        rc = 0;
        break;

    case DB_TXN_FORWARD_ROLL:
    case DB_TXN_APPLY:
        if (bdb_state->attr->shadows_nonblocking) {
            rc = update_shadows_beforecommit(bdb_state, lsn, NULL, 0);
            if (rc) {
               logmsg(LOGMSG_ERROR, "%s:%d update_shadows_beforecommit for "
                       "tranid %016llx rc %d\n",
                       __FILE__, __LINE__, delop->ltranid, rc);
                goto done;
            }
        }

        break;

    case DB_TXN_SNAPISOL:
    case DB_TXN_PRINT:
        lprev = &delop->prevllsn;
        printf("[%lu][%lu] CUSTOM: del_dta_lk %lu txnid %lx prelsn[%lu][%lu]  "
               "prevllsn[%lu][%lu]  tranid %016llx\n",
               (u_long)lsn->file, (u_long)lsn->offset, (u_long)rectype,
               (u_long)delop->txnid->txnid, (u_long)delop->prev_lsn.file,
               (u_long)delop->prev_lsn.offset, (u_long)lprev->file,
               (u_long)lprev->offset, delop->ltranid);
        printf("\ttable:       %.*s\n", delop->table.size,
               (char *)delop->table.data);
        printf("\tgenid:       %016llx\n", delop->genid);
        printf("\n");
        printf("\tdtafile:     %d\n", delop->dtafile);
        printf("\tdtastripe:   %d\n", delop->dtastripe);
        /*printf("\tprevlock:    "); hexdumpdbt(&delop->prevlock);*/
        printf("\n");
        {
            DB_LSN lll = delop->prev_lsn;
            unsigned long long lllgenid;
            static char *llldta = NULL;
            int iii;
            if (!llldta)
                llldta = printmemarg1();
            bdb_reconstruct_delete(bdb_state, &lll, NULL, NULL, &lllgenid,
                                   sizeof(unsigned long long), llldta,
                                   delop->dtalen, NULL);

            printf(" --genid %16llx\n", lllgenid);
            printf(" --dta   ");
            for (iii = 0; iii < delop->dtalen; iii++) {
                printf("%02x", (unsigned char)llldta[iii]);
            }
            printf("\n");
            printf("\n");
        }
        break;

    default:
        __db_err(dbenv, "unknown op type %d in handle_undo_del_dta_lk\n",
                 (int)op);
        break;
    }
done:
    *lsn = delop->prev_lsn;
    return rc;
}

int handle_undo_del_ix_lk(DB_ENV *dbenv, u_int32_t rectype,
                          llog_undo_del_ix_lk_args *delop, DB_LSN *lsn,
                          db_recops op)
{
    bdb_state_type *bdb_state;
    DB_LSN *lprev;
    int rc = 0;

    bdb_state = dbenv->app_private;

    /* don't do anything during berkeley recovery */
    if (bdb_state && !bdb_state->passed_dbenv_open)
        goto done;

    switch (op) {
    case DB_TXN_BACKWARD_ROLL:
    case DB_TXN_ABORT:
        rc = 0;
        break;

    case DB_TXN_FORWARD_ROLL:
    case DB_TXN_APPLY:

        if (bdb_state->attr->shadows_nonblocking) {
            rc = update_shadows_beforecommit(bdb_state, lsn, NULL, 0);
            if (rc) {
                logmsg(LOGMSG_ERROR, "%s:%d update_shadows_beforecommit for "
                       "tranid %016llx rc %d\n",
                       __FILE__, __LINE__, delop->ltranid, rc);
                goto done;
            }
        }

        break;

    case DB_TXN_SNAPISOL:
    case DB_TXN_PRINT:
        lprev = &delop->prevllsn;
        printf("[%lu][%lu] CUSTOM: del_ix_lk %lu txnid %lx prelsn[%lu][%lu]  "
               "prevllsn[%lu][%lu]  tranid %016llx\n",
               (u_long)lsn->file, (u_long)lsn->offset, (u_long)rectype,
               (u_long)delop->txnid->txnid, (u_long)delop->prev_lsn.file,
               (u_long)delop->prev_lsn.offset, (u_long)lprev->file,
               (u_long)lprev->offset, delop->ltranid);
        printf("\ttable:        %.*s\n", delop->table.size,
               (char *)delop->table.data);
        printf("\tgenid:        %016llx\n", delop->genid);
        printf("\tixnum:        %hd\n", delop->ix);
        printf("\n");
        {
            DB_LSN lll = delop->prev_lsn;
            static char *llldta = NULL;
            static char *lllkey = NULL;
            unsigned long long *lllgenid;
            int iii;

            if (!llldta)
                llldta = printmemarg1();
            if (!lllkey)
                lllkey = printmemarg2();
            bdb_reconstruct_delete(bdb_state, &lll, NULL, NULL, lllkey,
                                   delop->keylen, llldta, delop->dtalen, NULL);
            lllgenid = (unsigned long long *)llldta;
            printf(" --genid %16llx\n", *lllgenid);
            printf(" --dta   ");
            for (iii = 0; iii < delop->keylen; iii++) {
                printf("%02x", (unsigned char)lllkey[iii]);
            }
            printf("\n");
            printf("\n");
        }
        break;

    default:
        __db_err(dbenv, "unknown op type %d in handle_undo_del_ix_lk\n",
                 (int)op);
        break;
    }
done:
    *lsn = delop->prev_lsn;
    return rc;
}

int handle_undo_upd_dta_lk(DB_ENV *dbenv, u_int32_t rectype,
                           llog_undo_upd_dta_lk_args *updop, DB_LSN *lsn,
                           db_recops op)
{

    bdb_state_type *bdb_state;
    DB_LSN *lprev;
    int rc = 0;

    bdb_state = dbenv->app_private;

    if (bdb_state && !bdb_state->passed_dbenv_open)
        goto done;

    switch (op) {
    case DB_TXN_BACKWARD_ROLL:
    case DB_TXN_ABORT:
        rc = 0;
        break;

    case DB_TXN_FORWARD_ROLL:
    case DB_TXN_APPLY:

        if (bdb_state->attr->shadows_nonblocking) {
            rc = update_shadows_beforecommit(bdb_state, lsn, NULL, 0);
            if (rc) {
                logmsg(LOGMSG_ERROR, "%s:%d update_shadows_beforecommit for "
                       "tranid %016llx rc %d\n",
                       __FILE__, __LINE__, updop->ltranid, rc);
                goto done;
            }
        }

        break;

    case DB_TXN_SNAPISOL:
    case DB_TXN_PRINT:
        lprev = &updop->prevllsn;
        printf("[%lu][%lu] CUSTOM: upd_dta_lk %lu txnid %lx prelsn[%lu][%lu]  "
               "prevllsn[%lu][%lu]  tranid %016llx\n",
               (u_long)lsn->file, (u_long)lsn->offset, (u_long)rectype,
               (u_long)updop->txnid->txnid, (u_long)updop->prev_lsn.file,
               (u_long)updop->prev_lsn.offset, (u_long)lprev->file,
               (u_long)lprev->offset, updop->ltranid);
        printf("\ttable:       %.*s\n", updop->table.size,
               (char *)updop->table.data);
        printf("\toldgenid:    %016llx\n", updop->oldgenid);
        printf("\tnewenid:     %016llx\n", updop->newgenid);
        printf("\tdtafile:     %d\n", updop->dtafile);
        printf("\tdtastripe:   %d\n", updop->dtastripe);
        printf("\tolddtalen:   %d\n", updop->old_dta_len);
        if (updop->old_dta_len == 0) {
            assert(updop->dtafile != 0);
        } else {
            DB_LSN lll = updop->prev_lsn;
            static char *llldta = NULL;
            unsigned long long lllgenid;
            int iii;
            int irc;
            int offset;
            if (!llldta)
                llldta = printmemarg1();
            if (0 == bdb_inplace_cmp_genids(bdb_state, updop->oldgenid,
                                            updop->newgenid)) {
                irc = bdb_reconstruct_inplace_update(bdb_state, &lll, llldta,
                                                     updop->old_dta_len,
                                                     &offset, NULL, NULL, NULL);
            } else {
                irc = bdb_reconstruct_update(
                    bdb_state, &lll, NULL, NULL, &lllgenid,
                    sizeof(unsigned long long), llldta, updop->old_dta_len);
            }
            if (irc)
                printf("%s%d rc=%d\n", __FILE__, __LINE__, irc);
            else {
                printf(" --dta   ");
                for (iii = 0; iii < updop->old_dta_len; iii++) {
                    printf("%02x", (unsigned char)llldta[iii]);
                }
                printf("\n");
                printf("\n");
            }
        }
        break;

    default:
        __db_err(dbenv, "unknown op type %d in handle_undo_upd_dta_lk\n",
                 (int)op);
        break;
    }
done:
    *lsn = updop->prev_lsn;
    return rc;
}

int handle_undo_upd_ix_lk(DB_ENV *dbenv, u_int32_t rectype,
                          llog_undo_upd_ix_lk_args *updop, DB_LSN *lsn,
                          db_recops op)
{
    bdb_state_type *bdb_state;
    DB_LSN *lprev;
    tran_type *ltrans;
    int rc = 0;

    bdb_state = dbenv->app_private;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    /* TODO: huh?  safe?  dropping event? */
    if (bdb_state && !bdb_state->passed_dbenv_open)
        return 0;

    switch (op) {
    case DB_TXN_BACKWARD_ROLL:
    case DB_TXN_ABORT:
        rc = 0;
        break;

    case DB_TXN_FORWARD_ROLL:
    case DB_TXN_APPLY:
        if (bdb_state->attr->shadows_nonblocking) {
            rc = update_shadows_beforecommit(bdb_state, lsn, NULL, 0);
            if (rc) {
                logmsg(LOGMSG_ERROR, "%s:%d update_shadows_beforecommit for "
                       "tranid %016llx rc %d\n",
                       __FILE__, __LINE__, updop->ltranid, rc);
                return rc;
            }
        }

        break;

    case DB_TXN_SNAPISOL:
    case DB_TXN_PRINT:
        lprev = &updop->prevllsn;
        printf("[%lu][%lu] CUSTOM: upd_ix_lk %lu txnid %lx prelsn[%lu][%lu]  "
               "prevllsn[%lu][%lu]  tranid %016llx\n",
               (u_long)lsn->file, (u_long)lsn->offset, (u_long)rectype,
               (u_long)updop->txnid->txnid, (u_long)updop->prev_lsn.file,
               (u_long)updop->prev_lsn.offset, (u_long)lprev->file,
               (u_long)lprev->offset, updop->ltranid);
        printf("\ttable:       %.*s\n", updop->table.size,
               (char *)updop->table.data);
        printf("\toldgenid:    %016llx\n", updop->oldgenid);
        printf("\tnewenid:     %016llx\n", updop->newgenid);
        printf("\tdtalen:      %016u\n", updop->dtalen);
        printf("\tix:          %hd\n", updop->ix);
        /*printf("\tlockold:     "); hexdumpdbt(&updop->lockold);
         * printf("\n");*/
        printf("\n");
        {
            DB_LSN lll = updop->prev_lsn;
            char *llldta = NULL;
            int llloffset = 0;
            int llllen = 0;
            int iii;
            bdb_reconstruct_key_update(bdb_state, &lll, (void **)&llldta,
                                       &llloffset, &llllen);

            printf(" --dta [%d:%d]  ", llloffset, llllen);
            for (iii = 0; iii < llllen; iii++) {
                printf("%02x", (unsigned char)llldta[iii]);
            }
            printf("\n");
            printf("\n");
            free(llldta);
        }
        break;

    default:
        __db_err(dbenv, "unknown op type %d in handle_undo_upd_ix\n", (int)op);
        break;
    }
done:
    *lsn = updop->prev_lsn;
    return rc;
}

int handle_rowlocks_log_bench(DB_ENV *dbenv, u_int32_t rectype,
                              llog_rowlocks_log_bench_args *rl_log_bench,
                              DB_LSN *lsn, db_recops op)
{
    unsigned long long ltranid;
    bdb_state_type *bdb_state;
    bdb_state = dbenv->app_private;

    DB_LOCK rowlk1 = {0}, rowlk2 = {0};
    DBT lk1 = {0}, lk2 = {0};
    char mem1[ROWLOCK_KEY_SIZE], mem2[ROWLOCK_KEY_SIZE];
    int gotrowlock1 = 0, gotrowlock2 = 0;

    lk1.data = mem1;
    lk2.data = mem2;

    int rc = 0;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;
    if (bdb_state && !bdb_state->passed_dbenv_open)
        return 0;

    if (!gbl_dispatch_rowlocks_bench) {
        logmsg(LOGMSG_FATAL, "Shouldn't be here.  Aborting.\n");
        abort();
    }

    switch (op) {
    case DB_TXN_BACKWARD_ROLL:
    case DB_TXN_ABORT:
        rc = 0;
        break;

    case DB_TXN_APPLY:
    case DB_TXN_FORWARD_ROLL:
        break;

    case DB_TXN_SNAPISOL:
    case DB_TXN_PRINT:
        printf("[%lu][%lu] CUSTOM: rowlocks_log_bench %lu txnid %lx "
               "prelsn[%lu][%lu]  prevllsn[%lu][%lu]  tranid %016llx\n",
               (u_long)lsn->file, (u_long)lsn->offset, (u_long)rectype,
               (u_long)rl_log_bench->txnid->txnid,
               (u_long)rl_log_bench->prev_lsn.file,
               (u_long)rl_log_bench->prev_lsn.offset,
               (u_long)rl_log_bench->prevllsn.file,
               (u_long)rl_log_bench->prevllsn.offset, rl_log_bench->ltranid);
        printf("\top:          %d\n", rl_log_bench->op);
        printf("\targ1:        %d\n", rl_log_bench->arg1);
        printf("\targ2:        %d\n", rl_log_bench->arg2);
        printf("\tlock1:       ");
        hexdumpdbt(&rl_log_bench->lock1);
        printf("\tlock2:       ");
        hexdumpdbt(&rl_log_bench->lock2);
        printf("\tpayload:     ");
        hexdumpdbt(&rl_log_bench->payload);
        printf("\n");
        printf("\n");
        break;

    default:
        __db_err(dbenv, "unknown op type %d in %s\n", (int)op, __func__);
        break;
    }

done:
    *lsn = rl_log_bench->prev_lsn;
    return 0;
}

int handle_commit_log_bench(DB_ENV *dbenv, u_int32_t rectype,
                            llog_commit_log_bench_args *c_log_bench,
                            DB_LSN *lsn, db_recops op)
{
    bdb_state_type *bdb_state;
    int rc;
    bdb_state = dbenv->app_private;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;
    if (bdb_state && !bdb_state->passed_dbenv_open)
        return 0;

    switch (op) {
    case DB_TXN_BACKWARD_ROLL:
    case DB_TXN_APPLY:
    case DB_TXN_ABORT:
    case DB_TXN_FORWARD_ROLL:
        rc = 0;
        break;

    case DB_TXN_SNAPISOL:
    case DB_TXN_PRINT:
        printf("[%lu][%lu] CUSTOM: commit_log_bench %lu txnid %lx "
               "prelsn[%lu][%lu]  prevllsn[%lu][%lu]  tranid %016llx\n",
               (u_long)lsn->file, (u_long)lsn->offset, (u_long)rectype,
               (u_long)c_log_bench->txnid->txnid,
               (u_long)c_log_bench->prev_lsn.file,
               (u_long)c_log_bench->prev_lsn.offset,
               (u_long)c_log_bench->prevllsn.file,
               (u_long)c_log_bench->prevllsn.offset, c_log_bench->ltranid);
        printf("\top:          %d\n", c_log_bench->op);
        printf("\targ1:        %d\n", c_log_bench->arg1);
        printf("\targ2:        %d\n", c_log_bench->arg2);
        printf("\tlock1:       ");
        hexdumpdbt(&c_log_bench->lock1);
        printf("\tlock2:       ");
        hexdumpdbt(&c_log_bench->lock2);
        printf("\tpayload:     ");
        hexdumpdbt(&c_log_bench->payload);
        printf("\n");
        printf("\n");

        break;

    default:
        abort();
        break;
    }
    *lsn = c_log_bench->prev_lsn;
    return rc;
}
