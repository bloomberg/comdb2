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

/* This module takes a table as input and produces a "summary" of it as output.
   The summary
   has every N'th record in the original table.  The intended audience is sql
   analyze code.
   The idea is that for a large enough table, looking at every N'th record for a
   reasonable
   value of N will give the relative frequency/selectivity data as the entire
   table, and this
   method of scanning will be faster than a plain walk of the table. */
#include <pthread.h>
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
#include <pthread.h>
#include <sbuf2.h>
#include <fcntl.h>

#include <ctrace.h>

#include <net.h>
#include "bdb_int.h"
#include "bdb_cursor.h"
#include "locks.h"

/* need access to berkeley innards here */
#include <build/db.h>
#include <build/db_int.h>
#include <dbinc/db_page.h>
#include <dbinc/btree.h>
#include "dbinc/db_swap.h"
#include "dbinc/hmac.h"

#include <dbinc/crypto.h>
#include <btree/bt_prefix.h>
#include <assert.h>

#include <arpa/nameser_compat.h>
#ifndef BYTE_ORDER
#error "BYTE_ORDER not defined"
#endif

#include "flibc.h"
#include "logmsg.h"
#include "analyze.h"

extern volatile int gbl_schema_change_in_progress;
static double analyze_headroom = 6;

void analyze_set_headroom(uint64_t headroom)
{
    if (headroom < 1 || 100 < headroom) {
        logmsg(LOGMSG_ERROR,
               "Invalid headroom %lu. Needs to be between 0 and 100%%\n",
               headroom);
        return;
    }
    analyze_headroom = headroom;
    logmsg(LOGMSG_USER, "Analyze headroom set to %f%%\n", analyze_headroom);
}

static inline int check_free_space(const char *path)
{
    int rc = has_low_headroom(path, 100 - analyze_headroom, 1);
    if (rc) {
        return BDBERR_MISC;
    }
    return BDBERR_NOERROR;
}

#define _64K (64 * 1024)
static DB *dbp_from_meta(DB *dbp, DBMETA *meta)
{
    uint32_t magic;
    if (FLD_ISSET(meta->metaflags, DBMETA_CHKSUM))
        F_SET(dbp, DB_AM_CHKSUM);
    if (meta->encrypt_alg)
        F_SET(dbp, DB_AM_ENCRYPT);
    if ((magic = meta->magic) == DB_BTREEMAGIC)
        F_CLR(dbp, DB_AM_SWAP);
    else if (flibc_intflip(magic) == DB_BTREEMAGIC)
        F_SET(dbp, DB_AM_SWAP);
    else {
        logmsg(LOGMSG_ERROR, "bad meta magic value at %s: %u\n", __func__, magic);
        return NULL;
    }
    dbp->pgsize = F_ISSET(dbp, DB_AM_SWAP) ? flibc_intflip(meta->pagesize)
                                           : meta->pagesize;
    dbp->offset_bias = dbp->pgsize > _64K ? dbp->pgsize / _64K : 1;
    dbp->type = DB_BTREE;
    return dbp;
}

typedef struct sampler {
    DB db;                      /* our DB handle */
    bdb_state_type *bdb_state;  /* our bdb_state */
    struct temp_table *tmptbl;  /* temptable to store sampled pages */
    struct temp_cursor *tmpcur; /* cursor on the temptable */
    int pos;                    /* to keep track of the index in the page */
    void *data;                 /* payload of the entry at `pos' */
    int len;                    /* length of the payload */
} sampler_t;

int sampler_first(sampler_t *sampler)
{
    struct temp_cursor *tmpcur = sampler->tmpcur;
    int unused;

    if (bdb_temp_table_first(sampler->bdb_state, tmpcur, &unused) != 0)
        return IX_EMPTY;

    sampler->pos = 0;
    return (sampler_next(sampler) == IX_FND) ? IX_FND : IX_EMPTY;
}

int sampler_last(sampler_t *sampler)
{
    logmsg(LOGMSG_FATAL, "%s is not implemented.\n", __func__);
    abort();
    return 0;
}

int sampler_prev(sampler_t *sampler)
{
    logmsg(LOGMSG_FATAL, "%s is not implemented.\n", __func__);
    abort();
    return 0;
}

int sampler_next(sampler_t *sampler)
{
    int rc = IX_PASTEOF;
    uint8_t pfxbuf[KEYBUF];
    DB *dbp = &sampler->db;
    PAGE *page;
    db_indx_t *inp;
    int unused;
    struct temp_cursor *tmpcur = sampler->tmpcur;
    int ii, n, minlen, memcmprc;

next_leaf:
    page = (PAGE *)bdb_temp_table_data(tmpcur);
    inp = P_INP(dbp, page);
    ii = sampler->pos;
    n = NUM_ENT(page);
#ifndef NDEBUG
    uint8_t *max = (uint8_t *)page + 4096;
#endif

    for (; ii < n; ii += 2) {
        if (F_ISSET(dbp, DB_AM_SWAP))
            inp[ii] = flibc_shortflip(inp[ii]);
        BKEYDATA *data = GET_BKEYDATA(dbp, page, ii);
        assert((uint8_t *)data < max);
        if (F_ISSET(dbp, DB_AM_SWAP))
            data->len = flibc_shortflip(data->len);
        db_indx_t len;
        ASSIGN_ALIGN(db_indx_t, len, data->len);
        assert(((uint8_t *)data + len) < max);
        if (bk_decompress(dbp, page, &data, pfxbuf, sizeof(pfxbuf)) != 0) {
            logmsg(LOGMSG_ERROR, "\ndecompress failed page:%d ii:%d total:%d\n",
                   page->pgno, ii, n);
            continue;
        }
        ASSIGN_ALIGN(db_indx_t, len, data->len);

        /* Even though we sort pages by their 1st key, out-of-order pages
           can still occurr if a leaf we have read splits.
           We ensure that strictly sorted samples are returned by keeping
           reading from the temptable till the 1st entry on current page
           is greater than or equal the last entry on previous page. */
        if (ii == 0 && sampler->data != NULL) {
            minlen = len > sampler->len ? sampler->len : len;
            memcmprc = memcmp(sampler->data, data->data, minlen);
            if (memcmprc > 0 || (memcmprc == 0 && sampler->len > len))
                break;
        }

        free(sampler->data);
        sampler->data = malloc(len);
        memcpy(sampler->data, data->data, len);
        sampler->len = len;
        sampler->pos = ii + 2;
        rc = IX_FND;
        break;
    }

    if (rc != IX_FND) {
        if (bdb_temp_table_next(sampler->bdb_state, tmpcur, &unused) != 0)
            return IX_PASTEOF;
        sampler->pos = 0;
        goto next_leaf;
    }

    return rc;
}

void *sampler_key(sampler_t *sampler)
{
    return sampler->data;
}

sampler_t *sampler_init(bdb_state_type *bdb_state, int *bdberr)
{
    sampler_t *sampler;
    sampler = calloc(1, sizeof(sampler_t));
    if (sampler == NULL)
        goto err;

    sampler->tmptbl = bdb_temp_table_create(bdb_state->parent, bdberr);
    if (sampler->tmptbl == NULL)
        goto err;

    sampler->tmpcur =
        bdb_temp_table_cursor(bdb_state->parent, sampler->tmptbl, NULL, bdberr);
    if (sampler->tmpcur == NULL)
        goto err;

    sampler->bdb_state = bdb_state;
    return sampler;
err:
    if (sampler != NULL) {
        if (sampler->tmptbl)
            bdb_temp_table_close(bdb_state->parent, sampler->tmptbl, bdberr);
        free(sampler);
    }
    return NULL;
}

int sampler_close(sampler_t *sampler)
{
    int unused;
    (void)bdb_temp_table_close(sampler->bdb_state, sampler->tmptbl, &unused);
    free(sampler->data);
    free(sampler);
    return 0;
}

int bdb_summarize_table(bdb_state_type *bdb_state, int ixnum, int comp_pct,
                        sampler_t **samplerp, unsigned long long *outrecs,
                        unsigned long long *cmprecs, int *bdberr)
{
    DB_ENV *dbenv = bdb_state->dbenv;
    int is_hmac = CRYPTO_ON(dbenv);
    uint8_t pfxbuf[KEYBUF];
    char tmpname[PATH_MAX];
    char tran_tmpname[PATH_MAX];
    int rc = 0;
    DB dbp_ = {0}, *dbp;
    PAGE *page = NULL;
    unsigned char metabuf[512];
    int pgsz = 0;
    sampler_t *sampler = *samplerp;
    unsigned long long nrecs = 0;
    unsigned long long recs_looked_at = 0;
    int fd = -1;
    int last, now;

    if (comp_pct > 100 || comp_pct < 1) {
        *bdberr = BDBERR_BADARGS;
        rc = -1;
        goto done;
    }

    if (!bdb_state->parent) {
        *bdberr = BDBERR_BADARGS;
        rc = -1;
        goto done;
    }

    rc = check_free_space(bdb_state->dir);
    if (rc != BDBERR_NOERROR) {
        *bdberr = rc;
        rc = -1;
        goto done;
    }

    sampler = sampler_init(bdb_state, bdberr);

    if (sampler == NULL) {
        rc = -1;
        goto done;
    }

    *bdberr = BDBERR_NOERROR;

    if (ixnum < 0) {
        /* we only support this for indices - no need to summarize data */
        *bdberr = BDBERR_BADARGS;
        goto done;
    }
    rc = bdb_get_index_filename(bdb_state, ixnum, tmpname, sizeof(tmpname),
                                bdberr);
    if (rc) {
        if (rc == DB_LOCK_DEADLOCK)
            rc = BDBERR_DEADLOCK;
        goto done;
    }
    bdb_trans(tmpname, tran_tmpname);
    logmsg(LOGMSG_DEBUG, "open %s\n", tran_tmpname);

    fd = open(tran_tmpname, O_RDONLY);
    if (fd == -1) {
        logmsg(LOGMSG_ERROR, "can't open input db???: %d %s\n", errno,
                strerror(errno));
        rc = -1;
        goto done;
    }

    rc = read(fd, metabuf, sizeof(metabuf));
    if (rc != sizeof(metabuf)) {
        logmsg(LOGMSG_ERROR, "can't read meta page\n");
        goto done;
    }
    if ((dbp = dbp_from_meta(&dbp_, (DBMETA *)metabuf)) == NULL) {
        rc = -1;
        goto done;
    }
    pgsz = dbp->pgsize;
    page = malloc(pgsz);
#ifndef NDEBUG
    uint8_t *max = (uint8_t *)page + pgsz;
#endif
    rc = lseek(fd, 0, SEEK_SET);
    if (rc) {
        logmsg(LOGMSG_ERROR, "can't rewind to start of file\n");
        goto done;
    }

#if defined(_IBM_SOURCE) || defined(__linux__)
    // inform kernel that we will be accessing file sequentially
    // and that we won't need the file to be cached
    fdatasync(fd);
    posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED | POSIX_FADV_SEQUENTIAL);
#endif

    last = comdb2_time_epoch();
    for (rc = read(fd, page, pgsz); rc == pgsz; rc = read(fd, page, pgsz)) {
        /* If it is not a leaf page, continue reading the file. */
        if (!ISLEAF(page))
            continue;

        int ret;
        uint8_t *chksum = NULL;
        /* If we have checksums, use them to verify we don't have
           a partial page. If the checksum doesn't match,
           just skip the page. This should be rare
           (only happen for pagesizes larger than default). */
        size_t sumlen = 0;
        if (F_ISSET(dbp, DB_AM_CHKSUM)) {
            chksum_t algo = IS_CRC32C(page) ? algo_crc32c : algo_hash4;
            switch (TYPE(page)) {
            case P_HASHMETA:
            case P_BTREEMETA:
            case P_QAMMETA:
                chksum = ((BTMETA *)page)->chksum;
                sumlen = DBMETASIZE;
                break;
            default:
                chksum = P_CHKSUM(dbp, page);
                sumlen = pgsz;
                break;
            }
            if (F_ISSET(dbp, DB_AM_SWAP))
                P_32_SWAP(chksum);
            if ((ret = __db_check_chksum_algo(dbenv, dbenv->crypto_handle,
                                              (void *)chksum, page, sumlen,
                                              is_hmac, algo)) != 0) {
                logmsg(LOGMSG_ERROR, "pgno %u invalid checksum\n",
                       F_ISSET(dbp, DB_AM_SWAP) ? flibc_intflip(page->pgno)
                                                : page->pgno);
                continue;
            }
        }

        if (is_hmac) {
            DB_CIPHER *db_cipher = dbenv->crypto_handle;
            void *iv = P_IV(dbp, page);
            size_t skip = P_OVERHEAD(dbp);
            uint8_t *ciphertext = (uint8_t *)page + skip;
            if ((ret = db_cipher->decrypt(dbenv, db_cipher->data, iv,
                                          ciphertext, sumlen - skip)) != 0) {
                logmsg(LOGMSG_ERROR, "pgno %u decryption failed\n", page->pgno);
                continue;
            }
        }

        if (IS_PREFIX(page) && F_ISSET(dbp, DB_AM_SWAP))
            prefix_tocpu(dbp, page);

        db_indx_t n = NUM_ENT(page);
        if (F_ISSET(dbp, DB_AM_SWAP))
            n = flibc_shortflip(n);

        if (n == 0)
            continue;

        /* We only care about the key so we take half entries
           on the page. We don't check the flags of every entry
           to get the count, so it's likely deleted entries are
           counted here. However this is okay as we only need these
           two counters to estimate the number of periodic stat4 samples.
           And because entries may be deleted after we check them,
           even if we did check every entry, the results wouldn't be
           100% accurate anyway. */
        recs_looked_at += (n >> 1);
        if (rand() % 100 >= comp_pct)
            continue;
        NUM_ENT(page) = n;
        nrecs += (n >> 1);

        /* Check disk space, schema changes, analyze abort request etc. */
        now = comdb2_time_epoch();
        if (now - last >= 10) {
            last = now;
            rc = check_free_space(bdb_state->dir);
            if (rc != BDBERR_NOERROR) {
                *bdberr = rc;
                rc = -1;
                goto done;
            }
        }

        if (gbl_schema_change_in_progress || get_analyze_abort_requested()) {
            if (gbl_schema_change_in_progress) 
                logmsg(LOGMSG_ERROR, "%s: Aborting Analyze because "
                        "schema_change_in_progress\n", __func__);
            if (get_analyze_abort_requested())
                logmsg(LOGMSG_ERROR, "%s: Aborting Analyze because "
                        "of send analyze abort\n", __func__);
            rc = -1;
            goto done;
        }

        db_indx_t *inp = P_INP(dbp, page);
        /* Remember the value before byteswap.
           We need to reset inp[0] before
           saving the page to the temptable. */
        db_indx_t originp = inp[0];
        if (F_ISSET(dbp, DB_AM_SWAP))
            inp[0] = flibc_shortflip(inp[0]);
        BKEYDATA *data = GET_BKEYDATA(dbp, page, 0);
        assert((uint8_t *)data < max);
        /* skip deleted */
        if (B_DISSET(data))
            continue;
        if (B_TYPE(data) != B_KEYDATA)
            continue;

        /* Remember the values before byteswap.
           We need to reset 1st entry before
           saving the page to the temptable. */
        BKEYDATA *origdta = data;
        db_indx_t origdlen = data->len;
        if (F_ISSET(dbp, DB_AM_SWAP))
            data->len = flibc_shortflip(data->len);
        db_indx_t len;
        ASSIGN_ALIGN(db_indx_t, len, data->len);
        assert(((uint8_t *)data + len) < max);
        if (bk_decompress(dbp, page, &data, pfxbuf, sizeof(pfxbuf)) != 0) {
            logmsg(LOGMSG_ERROR,
                   "\ndecompress failed page:%d indx:0 total:%d\n", page->pgno,
                   n);
            continue;
        }
        ASSIGN_ALIGN(db_indx_t, len, data->len);

        /* Reset the 1st index and entry. */
        inp[0] = originp;
        origdta->len = origdlen;

        /* Save the entire page:
           key is the 1st key on the page;
           data is the page itself. */
        rc = bdb_temp_table_put(bdb_state->parent, sampler->tmptbl, data->data,
                                len, page, pgsz, NULL, bdberr);
        if (rc)
            goto done;
    }

    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "Problem reading dta file: %d %s\n", errno,
                strerror(errno));
        rc = -1;
        goto done;
    }

    logmsg(LOGMSG_INFO, "summarize added %llu records, traversed %llu\n", nrecs,
           recs_looked_at);
done:
    if (fd != -1)
        close(fd);
    if (page)
        free(page);
    if (rc || *bdberr != BDBERR_NOERROR) {
        if (sampler && *samplerp == NULL)
            sampler_close(sampler);
        return rc;
    }

    sampler->db = dbp_;
    *outrecs = nrecs;
    *cmprecs = recs_looked_at;

    if (*samplerp == NULL)
        *samplerp = sampler;

    return rc;
}
