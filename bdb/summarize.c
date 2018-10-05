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

int bdb_summarize_table(bdb_state_type *bdb_state, int ixnum, int comp_pct,
                        struct temp_table **outtbl, unsigned long long *outrecs,
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
    int pgsz;
    int created_temp_table = 0;
    unsigned long long nrecs = 0;
    unsigned long long recs_looked_at = 0;
    unsigned int pgno = 0;
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

    if (*outtbl == NULL) {
        *outtbl = bdb_temp_table_create(bdb_state->parent, bdberr);
        if (*outtbl == NULL) {
            rc = -1;
            goto done;
        }
        created_temp_table = 1;
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
    // dbp = bdb_state->dbp_ix[ixnum];
    if ((dbp = dbp_from_meta(&dbp_, (DBMETA *)metabuf)) == NULL) {
        rc = -1;
        goto done;
    }
    pgsz = dbp->pgsize;
    page = malloc(pgsz);
    uint8_t *max = (uint8_t *)page + pgsz;
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

    rc = read(fd, page, pgsz);
    last = comdb2_time_epoch();
    while (rc == pgsz) {
        if (ISLEAF(page)) {
            int i, ret;
            uint8_t *chksum = NULL;
            /* If we have checksums, use them to verify we don't have a partial
               page.
               If the checksum doesn't match, just skip the page. This should be
               rare
               (only happen for pagesizes larger than default). */
            size_t sumlen;
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
                                              ciphertext, sumlen - skip)) !=
                    0) {
                    logmsg(LOGMSG_ERROR, "pgno %u decryption failed\n", page->pgno);
                    continue;
                }
            }

            if (IS_PREFIX(page) && F_ISSET(dbp, DB_AM_SWAP))
                prefix_tocpu(dbp, page);

            db_indx_t n = NUM_ENT(page);
            if (F_ISSET(dbp, DB_AM_SWAP))
                n = flibc_shortflip(n);

            db_indx_t *inp = P_INP(dbp, page);
            /* entries on the page are paired as (key, data).  we only want
             * keys) */
            for (i = 0; i < n; i += 2) {
                /* we have a candidate */
                unsigned long long c = 0;
                if (F_ISSET(dbp, DB_AM_SWAP))
                    inp[i] = flibc_shortflip(inp[i]);
                BKEYDATA *data = GET_BKEYDATA(dbp, page, i);
                assert((uint8_t *)data < max);
                /* skip deleted */
                if (B_DISSET(data))
                    continue;
                if (B_TYPE(data) != B_KEYDATA)
                    continue;
                /* select comp_pct / 100 records */
                if (rand() % 100 < comp_pct) {
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
                    if (F_ISSET(dbp, DB_AM_SWAP))
                        data->len = flibc_shortflip(data->len);
                    db_indx_t len;
                    ASSIGN_ALIGN(db_indx_t, len, data->len);
                    assert(((uint8_t *)data + len) < max);
                    if ((rc = bk_decompress(dbp, page, &data, pfxbuf,
                                            sizeof(pfxbuf))) != 0) {
                        logmsg(LOGMSG_ERROR, 
                                "\ndecompress failed page:%d i:%d total:%d\n",
                                page->pgno, i, n);
                        goto done;
                    }
                    ASSIGN_ALIGN(db_indx_t, len, data->len);
                    rc = bdb_temp_table_put(
                        bdb_state->parent, *outtbl, data->data, len, &c,
                        sizeof(unsigned long long), NULL, bdberr);
                    if (rc)
                        goto done;
                    nrecs++;
                }
                recs_looked_at++;
            }
        }

        int get_analyze_abort_requested();
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
        rc = read(fd, page, pgsz);
        pgno++;
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
    if (rc && *outtbl && created_temp_table) {
        int crc;
        int cbdberr;
        crc = bdb_temp_table_close(bdb_state, *outtbl, &cbdberr);
        if (crc && *bdberr == BDBERR_DEADLOCK) {
            rc = -1;
            *bdberr = cbdberr;
        }
    }
    *outrecs = nrecs;
    *cmprecs = recs_looked_at;
    return rc;
}

