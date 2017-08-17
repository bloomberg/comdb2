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
** Estimate the amount of compression that can be achieved. We sample records
** from the main data file & blobs and compress them using both zlib and rle*.
**
** send dbname testcompr: print percent of records which will be sampled.
** send dbname testcompr NN: Set percent of records which will be sampled.
**
** I also use this to test crle. If testcompr is given a special tablename:
**  $ comdb2sc.tsk dbname testcompr cdb2justcrle
** then, only clre compression in performed. Additionally, the compressed
** record is decompressed and compared with the original record.
**
*/

#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <alloca.h>
#include <pthread.h>
#include <math.h>

#include <comdb2.h>
#include <sbuf2.h>
#include <sql.h>
#include <bdb_api.h>
#include <bdb_fetch.h>

#include <compress.h>
#include <zlib.h>
#include <comdb2rle.h>
#include <lz4.h>
#include <logmsg.h>

#if LZ4_VERSION_NUMBER < 10701
#define LZ4_compress_default LZ4_compress_limitedOutput
#endif


int gbl_testcompr_percent = 10;
int gbl_testcompr_max = 300000;

static const size_t genidsz = sizeof(unsigned long long);

typedef struct {
    SBUF2 *sb;
    const char *table;
    int rc;
} CompArg;

typedef struct {
    uint64_t dtasz;
    uint64_t blobsz;
} SizeEst;

typedef struct {
    SBUF2 *sb;
    unsigned long long genid;
    int fndlen;
    char fnddta[MAXLRL];
    struct dbtable *db;
    size_t blob_len[MAXBLOBS];
    void *blob_ptrs[MAXBLOBS];

    SizeEst uncompressed;
    SizeEst rle;
    SizeEst zlib;
    SizeEst crle;
    SizeEst lz4;
    char just_crle;
} CompStruct;

static int blob_compress(CompStruct *comp)
{
    struct dbtable *db = comp->db;
    const int numblobs = db->numblobs;
    int bdberr = 0;
    int rc;
    int i;

    const int blob_offset = db->nix + 1;
    char *crledta;
    char *rledta;
    char *zlibdta;
    char *lz4dta;

    for (i = 0; i < numblobs; ++i) {
        if (comp->blob_len[i] == 0)
            continue;

        /* Uncompress blob + ODH */
        comp->uncompressed.blobsz += comp->blob_len[i];

        const size_t len = comp->blob_len[i];
        void *mallocdta = NULL;
        if (len < 4 * 1024) {
            lz4dta = crledta = rledta = zlibdta = alloca(len);
        } else {
            lz4dta = crledta = rledta = zlibdta = mallocdta = malloc(len);
        }

        /* Comdb2 RLE */
        Comdb2RLE compress = {
            .in = comp->blob_ptrs[i],
            .insz = len,
            .out = (uint8_t *)crledta,
            .outsz = comp->blob_len[i],
        };
        if (compressComdb2RLE(&compress) == 0) {
            comp->crle.blobsz += compress.outsz;
        } else {
            comp->crle.blobsz += comp->blob_len[i];
        }

        if (comp->just_crle)
            goto out;

        /* RLE 8 */
        int rlesz =
            rle8_compress(comp->blob_ptrs[i], comp->blob_len[i], rledta, len);
        if (rlesz > 0) {
            comp->rle.blobsz += rlesz;
        } else { /* No RLE compression for this blob */
            comp->rle.blobsz += comp->blob_len[i];
        }

        /* zlib */
        Bytef *dest = (Bytef *)zlibdta;
        uLongf destLen = (uLongf)len;
        Bytef *source = (Bytef *)comp->blob_ptrs[i];
        uLong sourceLen = (uLong)comp->blob_len[i];
        rc = compress2(dest, &destLen, source, sourceLen, 6);
        if (rc == Z_OK) {
            comp->zlib.blobsz += destLen;
        } else { /* No zlib compression for this blob */
            comp->zlib.blobsz += comp->blob_len[i];
        }

        /* LZ4 */
        if ((rc = LZ4_compress_default(comp->blob_ptrs[i], lz4dta, len,
                                             comp->blob_len[i])) <= 0) {
            comp->lz4.blobsz += comp->blob_len[i];
        } else {
            comp->lz4.blobsz += rc;
        }

    out:
        free(comp->blob_ptrs[i]);
        comp->blob_ptrs[i] = NULL;
        free(mallocdta);
    }
    return 0;
}

static int test_compress(CompStruct *comp)
{
    int rc = 0;
    char buf[2 * MAXLRL];

    /* Uncompressed */
    comp->uncompressed.dtasz += comp->fndlen;

    /* Comdb2 RLE */
    Comdb2RLE compress = {
        .in = (uint8_t *)comp->fnddta,
        .insz = comp->fndlen,
        .out = (uint8_t *)buf,
        .outsz = comp->fndlen,
    };
    if (compressComdb2RLE(&compress) == 0) {
        comp->crle.dtasz += compress.outsz;

        if (comp->just_crle) { /* also decompress */
            uint8_t dbuf[comp->fndlen];
            Comdb2RLE d = {.in = compress.out,
                           .insz = compress.outsz,
                           .out = dbuf,
                           .outsz = sizeof(dbuf)};
            if (decompressComdb2RLE(&d) || d.outsz != compress.insz ||
                memcmp(compress.in, d.out, d.outsz)) {
                logmsg(LOGMSG_ERROR, "unable to decompress\n");
                fsnapf(stdout, compress.in, compress.insz);
            }
        }
    } else { /* No CRLE compression for this record */
        comp->crle.dtasz += comp->fndlen;
    }

    if (comp->just_crle)
        goto blob;

    /* RLE 8 */
    int rlesz = rle8_compress(comp->fnddta, comp->fndlen, buf, comp->fndlen);
    if (rlesz > 0) {
        comp->rle.dtasz += rlesz;
    } else { /* No RLE compression for this record */
        comp->rle.dtasz += comp->fndlen;
    }

    /* zlib */
    Bytef *dest = (Bytef *)buf;
    Bytef *source = (Bytef *)comp->fnddta;
    uLong sourceLen = (uLong)comp->fndlen;
    uLongf destLen = sourceLen;
    rc = compress2(dest, &destLen, source, sourceLen, 6);
    if (rc == Z_OK) {
        comp->zlib.dtasz += destLen;
    } else { /* No zlib compression for this record */
        comp->zlib.dtasz += comp->fndlen;
    }

    /* LZ4 */
    if ((rc = LZ4_compress_default(comp->fnddta, buf, comp->fndlen,
                                         comp->fndlen)) <= 0) {
        comp->lz4.dtasz += comp->fndlen;
    } else {
        comp->lz4.dtasz += rc;
    }

blob:
    rc = 0;
    if (comp->db->numblobs) {
        rc = blob_compress(comp);
    }
    return rc;
}

static void print_compr_stat(CompStruct *comp, const char *prefix, SizeEst *est)
{
    char buf[1024];
    char szbuf[64];
    double cmp_dta, cmp_blob;
    double sav_dta, sav_blob;
    struct dbtable *db = comp->db;

    cmp_dta = (double)est->dtasz / comp->uncompressed.dtasz;
    cmp_blob =
        db->numblobs ? (double)est->blobsz / comp->uncompressed.blobsz : 1;

    sav_dta = /*1.0 -*/ cmp_dta;
    sav_blob = /*1.0 -*/ cmp_blob;

    sav_dta *= 100;
    sav_blob *= 100;

    snprintf(buf, sizeof(buf) - 1, "Using %s: Data: %.2f%% Blobs %.2f%%\n",
             prefix, sav_dta, sav_blob);
    logmsg(LOGMSG_USER, "%s", buf);
    sbuf2printf(comp->sb, ">%s", buf);
}

static void compr_stat(CompStruct *comp)
{
    char buf[128];
    snprintf(buf, sizeof(buf) - 1, "Percentage of original size for: %s\n",
             comp->db->dbname);
    logmsg(LOGMSG_USER, "%s", buf);
    sbuf2printf(comp->sb, ">%s", buf);

    print_compr_stat(comp, "CRLE", &comp->crle);
    if (comp->just_crle)
        return;
    print_compr_stat(comp, "RLE8", &comp->rle);
    print_compr_stat(comp, "zlib", &comp->zlib);
    print_compr_stat(comp, " LZ4", &comp->lz4);
}

static void *handle_comptest_thd(void *_arg)
{
    CompArg *arg = _arg;
    CompStruct comp = {0};
    int compress;
    int rc;
    int i;
    int blob_pos[MAXBLOBS];
    size_t blob_offs[MAXBLOBS];
    int skip = round(100.0 / gbl_testcompr_percent - 1.0);

    struct ireq iq;
    int ixnum = -1;
    uint64_t last;
    uint64_t fndkey;
    int lastrrn, rrn;
    unsigned long long lastgenid;
    const int maxlen = sizeof(comp.fnddta);
    int fndlen;
    unsigned long long context = 0;

    int total = 1;

    if (skip >= 100) {
        skip = 99;
    } else if (skip < 0) {
        skip = 0;
    }

    backend_thread_event(thedb, BDBTHR_EVENT_START_RDONLY);
    comp.sb = arg->sb;
    comp.just_crle = 0;
    for (i = 0; i < MAXBLOBS; ++i) {
        blob_pos[i] = i;
    }
    for (i = 0; i < thedb->num_dbs; i++) {
        struct dbtable *db = thedb->dbs[i];
        if (strcmp(arg->table, "cdb2justcrle") == 0) {
            comp.just_crle = 1;
        } else if (strcmp(arg->table, "-all") != 0) {
            if (strcmp(arg->table, db->dbname) != 0) {
                continue;
            }
        }
        if (is_sqlite_stat(db->dbname)) {
            continue;
        }

        logmsg(LOGMSG_DEBUG, "Processing table: %s\n", db->dbname);

        comp.db = db;
        bzero(&comp.uncompressed, sizeof(comp.uncompressed));
        bzero(&comp.crle, sizeof(comp.crle));
        bzero(&comp.rle, sizeof(comp.rle));
        bzero(&comp.zlib, sizeof(comp.zlib));
        bzero(&comp.lz4, sizeof(comp.lz4));

        iq.dbenv = thedb;
        iq.is_fake = 1;
        iq.usedb = db;
        iq.opcode = OP_FIND;

        rc = ix_find_blobs(&iq, ixnum, NULL, 0, &fndkey, &rrn, &comp.genid,
                           &comp.fnddta, &comp.fndlen, maxlen, db->numblobs,
                           blob_pos, comp.blob_len, blob_offs, comp.blob_ptrs,
                           NULL);

        while (rc == IX_FND || rc == IX_FNDMORE) {
            if (gbl_sc_abort) {
                logmsg(LOGMSG_ERROR, "Abort compression testing %s\n", db->dbname);
                ++arg->rc;
                break;
            }
            rc = test_compress(&comp);
            if (rc) {
               logmsg(LOGMSG_ERROR, "Failed compressing %s, rc:%d (%s:%d)\n", db->dbname, rc,
                       __FILE__, __LINE__);
                ++arg->rc;
                break;
            }

            if (gbl_testcompr_max && total > gbl_testcompr_max) {
                break;
            }

            last = fndkey;
            lastrrn = rrn;
            lastgenid = comp.genid;

            int j;
            for (j = 0; j < skip; ++j) {
                /* Don't fetch any data */
                rc = ix_next_blobs(&iq, ixnum, NULL, 0, &last, lastrrn,
                                   lastgenid, &fndkey, &rrn, &comp.genid, NULL,
                                   NULL, 0, 0, NULL, NULL, NULL, NULL, NULL,
                                   context);
                if (rc != IX_FND && rc != IX_FNDMORE) {
                    break;
                }
                last = fndkey;
                lastrrn = rrn;
                lastgenid = comp.genid;
                ++total;
            }

            if (rc != IX_FND && rc != IX_FNDMORE) {
                break;
            }

            /* Fetch all data */
            rc = ix_next_blobs(&iq, ixnum, NULL, 0, &last, lastrrn, lastgenid,
                               &fndkey, &rrn, &comp.genid, &comp.fnddta,
                               &comp.fndlen, maxlen, db->numblobs, blob_pos,
                               comp.blob_len, blob_offs, comp.blob_ptrs, NULL,
                               context);
            ++total;
        }
        compr_stat(&comp);
    }
    sbuf2flush(arg->sb);
    backend_thread_event(thedb, BDBTHR_EVENT_DONE_RDONLY);
    return NULL;
}

void handle_testcompr(SBUF2 *sb, const char *table)
{
    CompArg arg;
    pthread_t t;
    void *rc;

    if (gbl_schema_change_in_progress) {
        sbuf2printf(sb, ">Schema change already running\n");
        sbuf2printf(sb, "FAILED\n");
        return;
    }

    gbl_schema_change_in_progress = 1;
    gbl_sc_abort = 0;

    arg.sb = sb;
    arg.table = table;
    arg.rc = 0;
    pthread_create(&t, NULL, handle_comptest_thd, &arg);
    pthread_join(t, &rc);
    gbl_schema_change_in_progress = 0;

    if (arg.rc == 0) {
        sbuf2printf(sb, "SUCCESS\n");
    } else {
        sbuf2printf(sb, "FAILED\n");
    }
}

