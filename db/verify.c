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

#include <errno.h>
#include <string.h>
#include <strings.h>
#include <stdlib.h>
#include <stdio.h>
#include <netinet/in.h>

#include <epochlib.h>
#include <sbuf2.h>

#include "comdb2.h"
#include "tag.h"
#include "verify.h"
#include "segstr.h"

#include <bdb_api.h>

void dump_record_by_rrn_genid(struct dbtable *db, int rrn, unsigned long long genid)
{
    int rc;
    struct ireq iq;
    char *dta;
    char *key = NULL;
    int dtasz;
    int fndlen;
    int ix;
    char tag[MAXTAGLEN];

    init_fake_ireq(db->dbenv, &iq);
    iq.usedb = db;

    dtasz = getdatsize(db);
    dta = malloc(dtasz);
    if (dta == NULL) {
        printf("dump_record_by_rrn_genid: malloc failed\n");
        return;
    }

    rc = ix_find_by_rrn_and_genid(&iq, 2, genid, dta, &fndlen, dtasz);
    if (rc) {
        printf("rrn %d genid 0x%llx not found\n", rrn, genid);
        return;
    }
    printf("rrn %d genid 0x%016llx\n", rrn, genid);
    dump_tagged_buf(db->dbname, ".ONDISK", (unsigned char *)dta);
    for (ix = 0; ix < db->nix; ix++) {
        key = malloc(getkeysize(db, ix));
        if (key == NULL) {
            printf("dump_record_by_rrn_genid: malloc failed\n");
            free(dta);
            return;
        }
        snprintf(tag, sizeof(tag), ".ONDISK_IX_%d", ix);
        rc = stag_to_stag_buf(db->dbname, ".ONDISK", dta, tag, key, NULL);
        if (rc) {
            printf("dump_record_by_rrn:stag_to_stag_buf rrn %d genid %016llx "
                   "failed\n",
                   rrn, genid);
            free(key);
            break;
        }
        printf("ix %d:\n", ix);
        dump_tagged_buf(db->dbname, tag, (unsigned char *)key);
        free(key);
    }
    free(dta);
}

void purge_by_genid(struct dbtable *db, unsigned long long *genid)
{
    int bdberr;
    /* purge all records, index entries and blobs containing a
       given genid. Presumably these will previously have been found
       through verify.

       This is transactional, verify is not.
     */

    void *tran;
    int retries = 0;
    int rc;
    struct ireq iq = {0};
    iq.usedb = db;
    iq.is_fake = 1;

    /* genid can be NULL in which case we do an auto purge */
    if (genid)
        printf("Purging genid %016llx from table %s\n", *genid, db->dbname);
retry:
    tran = bdb_tran_begin(db->handle, NULL, &bdberr);

    if (!tran) {
        fprintf(stderr, "purge_by_genid: bdb_trans_start failed - err %d\n"
                        "Please try again.\n",
                bdberr);
        return;
    }

    {
        char fndkey[MAXKEYLEN];
        char key[MAXKEYLEN];
        int rrn;
        int maxsz = getdatsize(db);
        char *dta = malloc(maxsz);
        int sz;
        int ixnum;
        int errcode;
        int fndlen;
        unsigned long long fnd_genid;

        bzero(fndkey, MAXKEYLEN);
        bzero(dta, maxsz);
        bzero(key, MAXKEYLEN);

        /* try to find it */
        if (genid) {
            rc = ix_find_by_rrn_and_genid_tran(&iq, 0, *genid, dta, &fndlen,
                                               maxsz, tran);
            if (rc != 0 || maxsz != fndlen) {
                printf("Unable to find data record for this genid.\n");

            } else {

                /* remove data (skip blobs) */
                rc = dat_del_auxdb(AUXDB_NONE, &iq, tran, 0, *genid, 0);
                printf(" dat_del rc: %d \n", rc);
            }
        } else {
            int i;
            printf("Corrupting database...\n");
            for (i = 0; i < db->nix; i++) {
                bzero(fndkey, MAXKEYLEN);
                bzero(dta, maxsz);
                bzero(key, MAXKEYLEN);

                rc = ix_find(&iq, i, key, 0, fndkey, &rrn, &fnd_genid, dta,
                             &fndlen, maxsz);
                /* delete a data record */
                if (rc == 0 || rc == 1) {
                    printf("Genid: 0x%016llx\n", fnd_genid);
                    rc = dat_del(&iq, tran, 0, fnd_genid);
                    printf("dat_del rc: %d \n", rc);
                }
            }
        }
    }

    rc = bdb_tran_commit(db->handle, tran, &bdberr);
    if (rc != 0) {
        rc = bdb_tran_abort(db->handle, tran, &bdberr);
        if (bdberr == BDBERR_DEADLOCK && ++retries < gbl_maxretries) {
            n_retries++;
            printf("Retrying purge transaction...\n");
            goto retry;
        }
        fprintf(stderr, "purge_by_genid: trans_commit failed %d\n", bdberr);
    }
}

static int verify_blobsizes_callback(void *parm, void *dta, int blobsizes[16],
                                     int offset[16], int *nblobs)
{
    int i;
    struct dbtable *db = parm;
    struct schema *s;
    int blobix = 0;
    int rc;
    int isnull;
    int outsz;
    int sz;

    *nblobs = 0;

    for (i = 0; i < db->schema->nmembers; i++) {
        s = db->schema;
        if (s->member[i].type == SERVER_BLOB ||
            s->member[i].type == SERVER_VUTF8) {
            char *p = ((char *)dta) + s->member[i].offset;
            if (stype_is_null(p)) {
                blobsizes[blobix] = -1;
            } else {
                rc = SERVER_UINT_to_CLIENT_UINT(
                    ((char *)dta) + s->member[i].offset, 5, NULL, NULL, &sz,
                    sizeof(int), &isnull, &outsz, NULL, NULL);
                if (rc)
                    return rc;
                /* tell bdb that the record fits in the inline portion and that
                 * it should
                 * verify that the blob DOESN'T exist */
                blobsizes[blobix] = ntohl(sz);
                if (s->member[i].type == SERVER_VUTF8) {
                    if (blobsizes[blobix] <= s->member[i].len - 5)
                        blobsizes[blobix] = -2;
                }
            }
            offset[blobix] = s->member[i].offset;
            blobix++;
        }
        /* TODO: vutf8 */
    }
    *nblobs = blobix;
    return 0;
}

static int verify_formkey_callback(void *parm, void *dta, void *blob_parm,
                                   int ix, void *keyout, int *keysz)
{
    struct dbtable *db = parm;
    int rc;
    struct convert_failure reason;

    *keysz = get_size_of_schema(db->ixschema[ix]);
    /*
    rc = stag_to_stag_buf(db->dbname, ".ONDISK", (const char*) dta,
    db->ixschema[ix]->tag, keyout, &reason);
     */
    rc = create_key_from_ondisk_blobs(db, ix, NULL, NULL, NULL, ".ONDISK", dta,
                                      0 /*not needed*/, db->ixschema[ix]->tag,
                                      keyout, NULL, blob_parm, MAXBLOBS, NULL);

    return rc;
}

static int verify_add_blob_buffer_callback(void *parm, void *dta, int dtasz,
                                           int blobno)
{
    blob_buffer_t *blobs = (blob_buffer_t *)parm;
    blobs[blobno].exists = 1;
    blobs[blobno].data = malloc(dtasz);
    if (blobs[blobno].data == NULL) {
        fprintf(stderr, "%s: failed to malloc size %d\n", __func__, dtasz);
        return -1;
    }
    memcpy(blobs[blobno].data, dta, dtasz);
    blobs[blobno].length = dtasz;
    blobs[blobno].collected = dtasz;
    return 0;
}

static void verify_free_blob_buffer_callback(void *parm)
{
    free_blob_buffers((blob_buffer_t *)parm, MAXBLOBS);
}

static unsigned long long verify_indexes_callback(void *parm, void *dta,
                                                  void *blob_parm)
{
    return verify_indexes(parm, dta, blob_parm, MAXBLOBS, 0);
}

int verify_table(const char *table, SBUF2 *sb, int progress_report_seconds,
             int attempt_fix, 
             int (*lua_callback)(void *, const char *), void *lua_params)
{
    struct dbtable *db;
    blob_buffer_t blob_buf[MAXBLOBS];
    int rc = 0;

    db = get_dbtable_by_name(table);
    bzero(blob_buf, sizeof(blob_buf));
    if (db == NULL) {
        if (sb) sbuf2printf(sb, "?Unknown table %s\nFAILED\n", table);
        rc = 1;
    } else {
        rc = bdb_verify(
            sb, db->handle, verify_formkey_callback, verify_blobsizes_callback,
            (int (*)(void *, void *, int *, uint8_t))vtag_to_ondisk_vermap,
            verify_add_blob_buffer_callback, verify_free_blob_buffer_callback,
            verify_indexes_callback, 
            db, lua_callback, lua_params,
            blob_buf, progress_report_seconds,
            attempt_fix);
        if (rc) {
            printf("verify rc %d\n", rc);
            if(sb) sbuf2printf(sb, "FAILED\n");
        } else if (sb) 
            sbuf2printf(sb, "SUCCESS\n");
    }
    sbuf2flush(sb);
    return rc;
}
