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

#include <unistd.h>
#include <ctrace.h>
#include <views.h>
#include <memory_sync.h>

#include <str0.h>
#include <logmsg.h>
#include "crc32c.h"

#include "schemachange.h"
#include "sc_global.h"
#include "sc_logic.h"
#include "sc_struct.h"
#include "sc_queues.h"
#include "sc_lua.h"
#include "sc_add_table.h"
#include "sc_callbacks.h"
#include "sc_schema.h"

int start_schema_change_tran(struct ireq *iq, tran_type *trans)
{
    struct schema_change_type *s = iq->sc;
    int rc;

    int maxcancelretry = 10;

    /* if we're not the master node then we can't do schema change! */
    if (thedb->master != gbl_mynode) {
        sc_errf(s, "I am not master; master is %s\n", thedb->master);
        free_schema_change_type(s);
        return SC_NOT_MASTER;
    }

    if (!s->resume && sc_resuming &&
        (s->addonly || s->drop_table || s->fastinit || s->alteronly)) {
        struct schema_change_type *last_sc = NULL;
        struct schema_change_type *stored_sc = NULL;

        pthread_mutex_lock(&sc_resuming_mtx);
        stored_sc = sc_resuming;
        while (stored_sc) {
            if (strcasecmp(stored_sc->table, s->table) == 0) {
                uuidstr_t us;
                comdb2uuidstr(stored_sc->uuid, us);
                logmsg(LOGMSG_INFO,
                       "Found ongoing schema change: rqid [%llx %s] "
                       "table %s, add %d, drop %d, fastinit %d, alter %d\n",
                       stored_sc->rqid, us, stored_sc->table,
                       stored_sc->addonly, stored_sc->drop_table,
                       stored_sc->fastinit, stored_sc->alteronly);
                if (stored_sc->rqid == iq->sorese.rqid &&
                    comdb2uuidcmp(stored_sc->uuid, iq->sorese.uuid) == 0) {
                    if (last_sc)
                        last_sc = stored_sc->sc_next;
                    else
                        sc_resuming = NULL;
                    stored_sc->sc_next = NULL;
                } else {
                    /* TODO: found an ongoing sc with different rqid
                     * should we fail this one or override the old one?
                     *
                     * For now, I am failing this one.
                     */
                    sc_errf(s, "schema change already in progress\n");
                    free_schema_change_type(s);
                    pthread_mutex_unlock(&sc_resuming_mtx);
                    return SC_CANT_SET_RUNNING;
                }
                break;
            }

            last_sc = stored_sc;
            stored_sc = stored_sc->sc_next;
        }
        pthread_mutex_unlock(&sc_resuming_mtx);
        if (stored_sc) {
            stored_sc->tran = trans;
            stored_sc->iq = iq;
            free_schema_change_type(s);
            s = stored_sc;
            iq->sc = s;
            pthread_mutex_lock(&s->mtx);
            s->finalize_only = 1;
            s->nothrevent = 1;
            s->resume = SC_RESUME;
            pthread_mutex_unlock(&s->mtx);
            uuidstr_t us;
            comdb2uuidstr(s->uuid, us);
            logmsg(LOGMSG_INFO,
                   "Resuming schema change: rqid [%llx %s] "
                   "table %s, add %d, drop %d, fastinit %d, alter %d\n",
                   s->rqid, us, s->table, s->addonly, s->drop_table,
                   s->fastinit, s->alteronly);

        } else {
            int bdberr;
            void *packed_sc_data = NULL;
            size_t packed_sc_data_len;
            if (bdb_get_in_schema_change(s->table, &packed_sc_data,
                                         &packed_sc_data_len, &bdberr) ||
                bdberr != BDBERR_NOERROR) {
                logmsg(LOGMSG_WARN, "%s: failed to discover whether table: "
                                    "%s is in the middle of a schema change\n",
                       __func__, s->table);
            }
            if (packed_sc_data) {
                stored_sc = new_schemachange_type();
                if (!stored_sc) {
                    logmsg(LOGMSG_ERROR, "%s: ran out of memory\n", __func__);
                    free(packed_sc_data);
                    free_schema_change_type(s);
                    return -1;
                }
                if (unpack_schema_change_type(stored_sc, packed_sc_data,
                                              packed_sc_data_len)) {
                    logmsg(LOGMSG_ERROR, "%s: failed to unpack sc\n", __func__);
                    free(packed_sc_data);
                    free(stored_sc);
                    free_schema_change_type(s);
                    return -1;
                }
                free(packed_sc_data);
                packed_sc_data = NULL;
                rc = bdb_set_in_schema_change(NULL, stored_sc->table, NULL, 0,
                                              &bdberr);
                if (rc)
                    logmsg(
                        LOGMSG_ERROR,
                        "%s: failed to cancel resuming schema change %d %d\n",
                        __func__, rc, bdberr);
            }
            if (stored_sc && !stored_sc->fulluprecs &&
                !stored_sc->partialuprecs &&
                stored_sc->type == DBTYPE_TAGGED_TABLE) {
                if (stored_sc->rqid == iq->sorese.rqid &&
                    comdb2uuidcmp(stored_sc->uuid, iq->sorese.uuid) == 0) {
                    s->rqid = stored_sc->rqid;
                    comdb2uuidcpy(s->uuid, stored_sc->uuid);
                    s->resume = 1;
                    uuidstr_t us;
                    comdb2uuidstr(s->uuid, us);
                    logmsg(LOGMSG_INFO,
                           "Resuming schema change: rqid [%llx %s] "
                           "table %s, add %d, drop %d, fastinit %d, alter %d\n",
                           s->rqid, us, s->table, s->addonly, s->drop_table,
                           s->fastinit, s->alteronly);
                }
                free_schema_change_type(stored_sc);
            } else
                logmsg(LOGMSG_INFO, "No ongoing schema change of table %s\n",
                       s->table);
        }
    }

    strcpy(s->original_master_node, gbl_mynode);
    unsigned long long seed;
    if (trans && s->tran == trans && iq->sc_seed) {
        seed = iq->sc_seed;
        logmsg(LOGMSG_INFO, "Starting schema change: "
                            "transactionally reuse seed 0x%llx\n",
               seed);
    } else if (s->resume) {
        logmsg(LOGMSG_INFO, "Resuming schema change: fetching seed\n");
        if ((rc = fetch_schema_change_seed(s, thedb, &seed))) {
            logmsg(LOGMSG_ERROR, "FAILED to fetch schema change seed\n");
            free_schema_change_type(s);
            return rc;
        }
        logmsg(LOGMSG_INFO, "Resuming schema change: fetched seed 0x%llx\n",
               seed);
    } else {
        seed = get_genid(thedb->bdb_env, 0);
        unsigned int *iptr = (unsigned int *)&seed;
        iptr[1] = htonl(crc32c(gbl_mynode, strlen(gbl_mynode)));
        logmsg(LOGMSG_INFO, "Starting schema change: new seed 0x%llx\n", seed);
    }

    rc = sc_set_running(1, seed, gbl_mynode, time(NULL));
    if (rc != 0) {
        if (!doing_upgrade || s->fulluprecs || s->partialuprecs) {
            free_schema_change_type(s);
            return SC_CANT_SET_RUNNING;
        } else {
            // upgrade can be preempted by other "real" schemachanges
            logmsg(LOGMSG_WARN, "Cancelling table upgrade threads. "
                                "Will start schemachange in a moment.\n");

            gbl_sc_abort = 1;
            MEMORY_SYNC;

            // give time to let upgrade threads exit
            while (maxcancelretry-- > 0) {
                sleep(1);
                if (!doing_upgrade) break;
            }

            if (doing_upgrade) {
                sc_errf(s, "failed to cancel table upgrade threads\n");
                free_schema_change_type(s);
                return SC_CANT_SET_RUNNING;
            } else if (sc_set_running(1, get_genid(thedb->bdb_env, 0),
                                      gbl_mynode, time(NULL)) != 0) {
                free_schema_change_type(s);
                return SC_CANT_SET_RUNNING;
            }
        }
    }

    if (thedb->master == gbl_mynode && !s->resume && iq->sc_seed != sc_seed) {
        logmsg(LOGMSG_INFO, "Calling bdb_set_disable_plan_genid 0x%llx\n",
               sc_seed);
        int bdberr;
        int rc =
            bdb_set_disable_plan_genid(thedb->bdb_env, NULL, sc_seed, &bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR, "Couldn't save schema change seed\n");
        }
    }
    iq->sc_seed = sc_seed;

    sc_arg_t *arg = malloc(sizeof(sc_arg_t));
    arg->trans = trans;
    arg->iq = iq;

    if (s->resume && s->alteronly && !s->finalize_only) {
        gbl_sc_resume_start = time_epochms();
    }
    /*
    ** if s->partialuprecs, we're going radio silent from this point forward
    ** in order to produce minimal spew
    */
    if (s->nothrevent) {
        if (!s->partialuprecs) logmsg(LOGMSG_INFO, "Executing SYNCHRONOUSLY\n");
        rc = do_schema_change_tran(arg);
    } else {
        if (!s->partialuprecs)
            logmsg(LOGMSG_INFO, "Executing ASYNCHRONOUSLY\n");
        pthread_t tid;
        if (trans)
            rc = pthread_create(&tid, &gbl_pthread_attr_detached,
                                (void *(*)(void *))do_schema_change_tran, arg);
        else
            rc = pthread_create(&tid, &gbl_pthread_attr_detached,
                                (void *(*)(void *))do_schema_change, s);
        if (rc) {
            logmsg(LOGMSG_ERROR,
                   "start_schema_change:pthread_create rc %d %s\n", rc,
                   strerror(errno));
            free(arg);
            free_schema_change_type(s);
            sc_set_running(0, sc_seed, gbl_mynode, time(NULL));
            rc = SC_ASYNC_FAILED;
        } else {
            rc = SC_ASYNC;
        }
    }

    return rc;
}

int start_schema_change(struct schema_change_type *s)
{
    struct ireq iq;
    init_fake_ireq(thedb, &iq);
    iq.sc = s;
    if (s->db == NULL) {
        s->db = get_dbtable_by_name(s->table);
    }
    iq.usedb = s->db;
    return start_schema_change_tran(&iq, NULL);
}

void delay_if_sc_resuming(struct ireq *iq)
{
    if (gbl_sc_resume_start == 0) return;

    int diff;
    int printerr = 0;
    while (gbl_sc_resume_start) {
        if ((diff = time_epochms() - gbl_sc_resume_start) > 300 && !printerr) {
            logmsg(LOGMSG_WARN, "Delaying since gbl_sc_resume_start has not "
                                "been reset to 0 for %dms\n",
                   diff);
            printerr = 1; // avoid spew
        }
        usleep(10000); // 10ms
    }
}

typedef struct {
    struct ireq *iq;
    void *trans;
} finalize_t;

static void *finalize_schema_change_thd_tran(void *varg)
{
    finalize_t *arg = varg;
    void *trans = arg->trans;
    struct ireq *iq = arg->iq;
    free(arg);
    finalize_schema_change_thd(iq, trans);
    return NULL;
}

int finalize_schema_change(struct ireq *iq, tran_type *trans)
{
    struct schema_change_type *s = iq->sc;
    int rc;
    assert(iq->sc->tran == NULL || iq->sc->tran == trans);
    if (s->nothrevent) {
        logmsg(LOGMSG_DEBUG, "Executing SYNCHRONOUSLY\n");
        rc = finalize_schema_change_thd(iq, trans);
    } else {
        pthread_t tid;
        finalize_t *arg = malloc(sizeof(finalize_t));
        arg->iq = iq;
        arg->trans = trans;
        logmsg(LOGMSG_DEBUG, "Executing ASYNCHRONOUSLY\n");
        rc = pthread_create(&tid, &gbl_pthread_attr_detached,
                            finalize_schema_change_thd_tran, arg);
        if (rc) {
            logmsg(LOGMSG_ERROR,
                   "start_schema_change:pthread_create rc %d %s\n", rc,
                   strerror(errno));
            free_schema_change_type(s);
            sc_set_running(0, sc_seed, gbl_mynode, time(NULL));
            rc = SC_ASYNC_FAILED;
        } else {
            rc = SC_ASYNC;
        }
    }

    return rc;
}

/* -99 if schema change already in progress */
int change_schema(char *table, char *fname, int odh, int compress,
                  int compress_blobs)
{
    struct schema_change_type *s;

    s = new_schemachange_type();
    if (!s) {
        logmsg(LOGMSG_ERROR, "%s: malloc failed\n", __func__);
        return -1;
    }
    bzero(s, sizeof(struct schema_change_type));
    s->type = DBTYPE_TAGGED_TABLE;
    strncpy0(s->table, table, sizeof(s->table));
    strncpy0(s->fname, fname, sizeof(s->fname));

    s->headers = odh;
    s->compress = compress;
    s->compress_blobs = compress_blobs;

    return start_schema_change(s);
}

int morestripe(struct dbenv *dbenvin, int newstripe, int blobstripe)
{
    struct schema_change_type *s;

    s = new_schemachange_type();
    if (!s) {
        logmsg(LOGMSG_ERROR, "%s: malloc failed\n", __func__);
        return -1;
    }
    bzero(s, sizeof(struct schema_change_type));
    s->type = DBTYPE_MORESTRIPE;
    s->newdtastripe = newstripe;
    s->blobstripe = blobstripe;

    return start_schema_change(s);
}

int create_queue(struct dbenv *dbenvin, char *queuename, int avgitem,
                 int pagesize, int isqueuedb)
{
    struct schema_change_type *s;

    s = new_schemachange_type();
    if (!s) {
        logmsg(LOGMSG_ERROR, "%s: malloc failed\n", __func__);
        return -1;
    }
    bzero(s, sizeof(struct schema_change_type));
    s->type = isqueuedb ? DBTYPE_QUEUEDB : DBTYPE_QUEUE;
    strncpy0(s->table, queuename, sizeof(s->table));
    s->avgitemsz = avgitem;
    s->pagesize = pagesize;

    return start_schema_change(s);
}

int fastinit_table(struct dbenv *dbenvin, char *table)
{
    struct schema_change_type *s;
    struct dbtable *db;

    db = get_dbtable_by_name(table);
    if (db == NULL) {
        logmsg(LOGMSG_ERROR, "%s: invalid table %s\n", __func__, table);
        return -1;
    }

    s = new_schemachange_type();
    if (!s) {
        logmsg(LOGMSG_ERROR, "%s: malloc failed\n", __func__);
        return -1;
    }
    bzero(s, sizeof(struct schema_change_type));
    s->type = DBTYPE_TAGGED_TABLE;
    strncpy0(s->table, db->dbname, sizeof(s->table));

    if (get_csc2_file(db->dbname, -1 /*highest csc2_version*/, &s->newcsc2,
                      NULL /*csc2len*/)) {
        logmsg(LOGMSG_ERROR, "%s: could not get schema for table: %s\n",
               __func__, db->dbname);
        return -1;
    }

    s->nothrevent = 1;
    s->fastinit = 1;
    s->same_schema = 1;
    s->nothrevent = 1;
    s->headers = -1;
    s->compress = -1;
    s->compress_blobs = -1;
    s->ip_updates = -1;
    s->instant_sc = -1;

    return start_schema_change(s);
}

int do_dryrun(struct schema_change_type *s)
{
    int rc;
    struct dbtable *db = NULL;
    struct dbtable *newdb = NULL;
    struct scinfo scinfo = {0};

    db = get_dbtable_by_name(s->table);
    if (db == NULL) {
        if (s->alteronly) {
            sbuf2printf(s->sb, ">Table %s does not exists\n", s->table);
            goto fail;
        } else if (s->fastinit) {
            sbuf2printf(s->sb, ">Table %s does not exists\n", s->table);
            goto fail;
        }
    } else {
        if (s->addonly) {
            sbuf2printf(s->sb, ">Table %s already exists\n", s->table);
            goto fail;
        } else if (s->fastinit) {
            sbuf2printf(s->sb, ">Table %s will be truncated\n", s->table);
            goto succeed;
        }
    }

    if (dyns_load_schema_string(s->newcsc2, thedb->envname, s->table)) {
        char *err;
        err = csc2_get_errors();
        sc_errf(s, "%s", err);
        goto fail;
    }

    if (db == NULL) {
        sbuf2printf(s->sb, ">Table %s will be added.\n", s->table);
        goto succeed;
    }

    newdb = newdb_from_schema(thedb, s->table, NULL, 0, 0, 1);
    if (!newdb) {
        goto fail;
    }

    set_schemachange_options(s, db, &scinfo);
    set_sc_flgs(s);

    newdb->odh = s->headers;
    newdb->instant_schema_change = newdb->odh && s->instant_sc;

    if (add_cmacc_stmt(newdb, 1) != 0) {
        goto fail;
    }

    if (dryrun_int(s, db, newdb, &scinfo)) {
        goto fail;
    }

succeed:
    rc = 0;
    goto done;

fail:
    rc = -1;

done:
    if (rc == 0) {
        sbuf2printf(s->sb, "SUCCESS\n");
    } else {
        sbuf2printf(s->sb, "FAILED\n");
    }
    if (newdb) {
        backout_schemas(newdb->dbname);
        newdb->schema = NULL;
        freedb(newdb);
    }
    return rc;
}

int live_sc_post_delete(struct ireq *iq, void *trans, unsigned long long genid,
                        const void *old_dta, unsigned long long del_keys,
                        blob_buffer_t *oldblobs)
{

    if (!(sc_live && iq->usedb->sc_from == iq->usedb)) {
        return 0;
    }

    int stripe = get_dtafile_from_genid(genid);
    if (stripe < 0 || stripe >= gbl_dtastripe) {
        logmsg(LOGMSG_ERROR,
               "live_sc_post_delete: genid 0x%llx stripe %d out of range!\n",
               genid, stripe);
        return 0;
    }
    unsigned long long *sc_genids = iq->usedb->sc_to->sc_genids;
    if (!sc_genids[stripe]) {
        /* A genid of zero is invalid.  So, if the schema change cursor is at
         * genid zero it means pretty conclusively that it hasn't done anything
         * yet so we cannot possibly be behind the cursor. */
        return 0;
    }

    int is_gen_gt_scptr = is_genid_right_of_stripe_pointer(
        iq->usedb->handle, genid, sc_genids[stripe]);
    if (is_gen_gt_scptr) {
        return 0;
    }

    /* genid is older than schema change position - a delete from new
     * table will be required. */

    return live_sc_post_delete_int(iq, trans, genid, old_dta, del_keys,
                                   oldblobs);
}

int live_sc_post_add(struct ireq *iq, void *trans, unsigned long long genid,
                     uint8_t *od_dta, unsigned long long ins_keys,
                     blob_buffer_t *blobs, size_t maxblobs, int origflags,
                     int *rrn)
{
    if (!sc_live || iq->usedb->sc_from != iq->usedb) return 0;

    int stripe = get_dtafile_from_genid(genid);
    if (stripe < 0 || stripe >= gbl_dtastripe) {
        logmsg(LOGMSG_ERROR,
               "live_sc_post_add: genid 0x%llx stripe %d out of range!\n",
               genid, stripe);
        return 0;
    }
    unsigned long long *sc_genids = iq->usedb->sc_to->sc_genids;
    if (!sc_genids[stripe]) {
        /* A genid of zero is invalid.  So, if the schema change cursor is at
         * genid zero it means pretty conclusively that it hasn't done anything
         * yet so we cannot possibly be behind the cursor. */
        return 0;
    }
    if (is_genid_right_of_stripe_pointer(iq->usedb->handle, genid,
                                         sc_genids[stripe])) {
        return 0;
    }
    return live_sc_post_add_int(iq, trans, genid, od_dta, ins_keys, blobs,
                                maxblobs, origflags, rrn);
}

/* should be really called live_sc_post_update_delayed_key_adds() */
int live_sc_delayed_key_adds(struct ireq *iq, void *trans,
                             unsigned long long newgenid, const void *od_dta,
                             unsigned long long ins_keys, int od_len)
{

    return live_sc_post_update_delayed_key_adds_int(iq, trans, newgenid, od_dta,
                                                    ins_keys, od_len);
}

/* Updating of a record when schemachange is going means we have to check
       the schemachange pointer and depending on its location wrt. oldgenid and
   newgenid
       we need to perform one of the following actions:
    1) ...........oldgenid and newgenid
         ^__SC ptr
       nothing to do.

    2) oldgenid  ......  newgenid
                   ^__SC ptr
       post_delete(oldgenid)

    3) newgenid  ......  oldgenid
                   ^__SC ptr
       post_add(newgenid)

    4) newgenid and oldgenid  ......
                               ^__SC ptr
       actually_update(oldgen to newgenid)
*/
int live_sc_post_update(struct ireq *iq, void *trans,
                        unsigned long long oldgenid, const void *old_dta,
                        unsigned long long newgenid, const void *new_dta,
                        unsigned long long ins_keys,
                        unsigned long long del_keys, int od_len, int *updCols,
                        blob_buffer_t *blobs, size_t maxblobs, int origflags,
                        int rrn, int deferredAdd, blob_buffer_t *oldblobs,
                        blob_buffer_t *newblobs)
{
    if (!(sc_live && iq->usedb->sc_from == iq->usedb)) {
        return 0;
    }

    int stripe = get_dtafile_from_genid(oldgenid);
    if (stripe < 0 || stripe >= gbl_dtastripe) {
        logmsg(LOGMSG_ERROR,
               "live_sc_post_update: oldgenid 0x%llx stripe %d out of range!\n",
               oldgenid, stripe);
        return 0;
    }
    unsigned long long *sc_genids = iq->usedb->sc_to->sc_genids;
    if (!sc_genids[stripe]) {
        /* A genid of zero is invalid.  So, if the schema change cursor is at
         * genid zero it means pretty conclusively that it hasn't done anything
         * yet so we cannot possibly be behind the cursor. */
        return 0;
    }
    /*
    if(get_dtafile_from_genid(newgenid) != stripe)
        printf("WARNING: New genid in stripe %d, newgenid %llx, oldgenid
    %llx\n", newstripe, newgenid, oldgenid);
    */
    if (iq->debug) {
        reqpushprefixf(iq, "live_sc_post_update: ");
    }

    int is_oldgen_gt_scptr = is_genid_right_of_stripe_pointer(
        iq->usedb->handle, oldgenid, sc_genids[stripe]);
    int is_newgen_gt_scptr = is_genid_right_of_stripe_pointer(
        iq->usedb->handle, newgenid, sc_genids[stripe]);
    int rc = 0;

    // spelling this out for legibility, various situations:
    if (is_newgen_gt_scptr &&
        is_oldgen_gt_scptr) // case 1) ..^........oldgenid and newgenid
    {
        if (iq->debug)
            reqprintf(iq,
                      "C1: scptr 0x%llx ... oldgenid 0x%llx newgenid 0x%llx ",
                      sc_genids[stripe], oldgenid, newgenid);
    } else if (is_newgen_gt_scptr &&
               !is_oldgen_gt_scptr) // case 2) oldgenid  .^....  newgenid
    {
        if (iq->debug)
            reqprintf(
                iq, "C2: oldgenid 0x%llx ... scptr 0x%llx ... newgenid 0x%llx ",
                oldgenid, sc_genids[stripe], newgenid);
        rc = live_sc_post_delete_int(iq, trans, oldgenid, old_dta, del_keys,
                                     oldblobs);
    } else if (!is_newgen_gt_scptr &&
               is_oldgen_gt_scptr) // case 3) newgenid  ..^...  oldgenid
    {
        if (iq->debug)
            reqprintf(
                iq, "C3: newgenid 0x%llx ...scptr 0x%llx ... oldgenid 0x%llx ",
                newgenid, sc_genids[stripe], oldgenid);
        rc = live_sc_post_add_int(iq, trans, newgenid, new_dta, ins_keys, blobs,
                                  maxblobs, origflags, &rrn);
    } else if (!is_newgen_gt_scptr &&
               !is_oldgen_gt_scptr) // case 4) newgenid and oldgenid  ...^..
    {
        if (iq->debug)
            reqprintf(iq,
                      "C4: oldgenid 0x%llx newgenid 0x%llx ... scptr 0x%llx",
                      oldgenid, newgenid, sc_genids[stripe]);
        rc = live_sc_post_update_int(
            iq, trans, oldgenid, old_dta, newgenid, new_dta, ins_keys, del_keys,
            od_len, updCols, blobs, deferredAdd, oldblobs, newblobs);
    }

    if (iq->debug) reqpopprefixes(iq, 1);

    return rc;
}

/**********************************************************************/
/* I ORIGINALLY REMOVED THIS, THEN MERGING I SAW IT BACK IN COMDB2.C
    I AM LEAVING IT IN HERE FOR NOW (GOTTA ASK MARK)               */

static int add_table_for_recovery(struct ireq *iq)
{
    struct schema_change_type *s = iq->sc;
    struct dbtable *db;
    struct dbtable *newdb;
    int bdberr;
    int rc;

    db = get_dbtable_by_name(s->table);
    if (db == NULL) {
        wrlock_schema_lk();
        rc = do_add_table(iq, NULL);
        unlock_schema_lk();
        return rc;
    }

    /* Shouldn't get here */
    if (s->addonly) {
        logmsg(LOGMSG_FATAL, "table '%s' already exists\n", s->table);
        abort();
        return -1;
    }

    int retries = 0;
    int changed;
    int i;
    int olddb_compress, olddb_compress_blobs, olddb_inplace_updates;
    int olddb_instant_sc;
    char new_prefix[32];
    struct scplan theplan;
    int foundix;

    if (s->headers != db->odh) {
        s->header_change = s->force_dta_rebuild = s->force_blob_rebuild = 1;
    }

    rc = dyns_load_schema_string(s->newcsc2, thedb->envname, s->table);
    if (rc != 0) {
        char *err;
        err = csc2_get_errors();
        sc_errf(s, "%s", err);
        logmsg(LOGMSG_FATAL, "Shouldn't happen in this piece of code.\n");
        abort();
    }

    if ((foundix = getdbidxbyname(s->table)) < 0) {
        logmsg(LOGMSG_FATAL, "couldnt find table <%s>\n", s->table);
        abort();
    }

    if (s->dbnum != -1) db->dbnum = s->dbnum;

    db->sc_to = newdb =
        newdb_from_schema(thedb, s->table, NULL, db->dbnum, foundix, 0);

    if (newdb == NULL) return -1;

    newdb->dtastripe = gbl_dtastripe;
    newdb->odh = s->headers;
    /* Don't lose precious flags like this */
    newdb->inplace_updates = s->headers && s->ip_updates;
    newdb->instant_schema_change = s->headers && s->instant_sc;
    newdb->version = get_csc2_version(newdb->dbname);

    if (add_cmacc_stmt(newdb, 1) != 0) {
        backout_schemas(newdb->dbname);
        abort();
    }

    if (verify_constraints_exist(NULL, newdb, newdb, s) != 0) {
        backout_schemas(newdb->dbname);
        abort();
    }

    bdb_get_new_prefix(new_prefix, sizeof(new_prefix), &bdberr);

    rc = open_temp_db_resume(newdb, new_prefix, 1, 0, NULL);
    if (rc) {
        backout_schemas(newdb->dbname);
        abort();
    }

    return 0;
}
/* Make sure that logical recovery has tables to work with */
int add_schema_change_tables()
{
    int rc;
    int scabort = 0;

    /* if a schema change is currently running don't try to resume one */
    pthread_mutex_lock(&schema_change_in_progress_mutex);
    if (gbl_schema_change_in_progress) {
        pthread_mutex_unlock(&schema_change_in_progress_mutex);
        return 0;
    }
    pthread_mutex_unlock(&schema_change_in_progress_mutex);
    struct ireq iq;
    init_fake_ireq(thedb, &iq);

    for (int i = 0; i < thedb->num_dbs; ++i) {
        int bdberr;
        void *packed_sc_data = NULL;
        size_t packed_sc_data_len = 0;
        if (bdb_get_in_schema_change(thedb->dbs[i]->dbname, &packed_sc_data,
                                     &packed_sc_data_len, &bdberr) ||
            bdberr != BDBERR_NOERROR) {
            logmsg(LOGMSG_ERROR,
                   "%s: failed to discover "
                   "whether table: %s is in the middle of a schema change\n",
                   __func__, thedb->dbs[i]->dbname);
            continue;
        }

        /* if we got some data back, that means we were in a schema change */
        if (packed_sc_data) {
            struct schema_change_type *s;
            logmsg(LOGMSG_WARN, "%s: table: %s is in the middle of a "
                                "schema change, adding table...\n",
                   __func__, thedb->dbs[i]->dbname);

            s = new_schemachange_type();
            if (!s) {
                logmsg(LOGMSG_ERROR, "%s: ran out of memory\n", __func__);
                free(packed_sc_data);
                return -1;
            }

            if (unpack_schema_change_type(s, packed_sc_data,
                                          packed_sc_data_len)) {
                sc_errf(s, "could not unpack the schema change data retrieved "
                           "from the low level meta table\n");
                free(packed_sc_data);
                free(s);
                return -1;
            }

            /* Give operators a chance to prevent a schema change from resuming.
             */
            char *abort_filename =
                comdb2_location("marker", "%s.scabort", thedb->envname);
            if (access(abort_filename, F_OK) == 0) {
                rc = bdb_set_in_schema_change(NULL, thedb->dbs[i]->dbname, NULL,
                                              0, &bdberr);
                if (rc)
                    logmsg(LOGMSG_ERROR,
                           "Failed to cancel resuming schema change %d %d\n",
                           rc, bdberr);
                else
                    scabort = 1;
            }

            free(abort_filename);
            free(packed_sc_data);

            if (scabort) {
                return 0;
            }

            MEMORY_SYNC;

            if (s->fastinit || s->type != DBTYPE_TAGGED_TABLE) {
                free(s);
                return 0;
            }

            iq.sc = s;
            rc = add_table_for_recovery(&iq);

            free(s);
            return rc;
        }
    }

    return 0;
}

int sc_timepart_add_table(const char *existingTableName,
                          const char *newTableName, struct errstat *xerr)
{
    bdb_state_type *bdb_state = thedb->bdb_env;
    struct schema_change_type sc = {0};
    char *schemabuf = NULL;
    struct dbtable *db;

    /* prepare sc */
    sc.onstack = 1;
    sc.type = DBTYPE_TAGGED_TABLE;

    snprintf(sc.table, sizeof(sc.table), "%s", newTableName);
    sc.table[sizeof(sc.table) - 1] = '\0';

    sc.scanmode = gbl_default_sc_scanmode;

    sc.live = 1;
    sc.use_plan = 1;

    /* this is a table add */
    sc.addonly = 1;
    sc.finalize = 1;

    /* get new schema */
    db = get_dbtable_by_name(existingTableName);
    if (db == NULL) {
        xerr->errval = SC_VIEW_ERR_BUG;
        snprintf(xerr->errstr, sizeof(xerr->errstr), "table '%s' not found\n",
                 existingTableName);
        goto error_prelock;
    }
    if (get_csc2_file(db->dbname, -1 /*highest csc2_version*/, &schemabuf,
                      NULL /*csc2len*/)) {
        xerr->errval = SC_VIEW_ERR_BUG;
        snprintf(xerr->errstr, sizeof(xerr->errstr),
                 "could not get schema for table '%s'\n", existingTableName);
        goto error_prelock;
    }
    sc.newcsc2 = schemabuf;

    /* make table odh, compression, ipu, instantsc the same for the new table */
    if (db->odh) sc.headers = 1;
    if (get_db_compress(db, &sc.compress)) {
        xerr->errval = SC_VIEW_ERR_BUG;
        snprintf(xerr->errstr, sizeof(xerr->errstr),
                 "could not get compression for table '%s'\n",
                 existingTableName);
        goto error_prelock;
    }
    if (get_db_compress_blobs(db, &sc.compress_blobs)) {
        xerr->errval = SC_VIEW_ERR_BUG;
        snprintf(xerr->errstr, sizeof(xerr->errstr),
                 "could not get blob compression for table '%s'\n",
                 existingTableName);
        goto error_prelock;
    }
    if (get_db_inplace_updates(db, &sc.ip_updates)) {
        xerr->errval = SC_VIEW_ERR_BUG;
        snprintf(xerr->errstr, sizeof(xerr->errstr),
                 "could not get ipu for table '%s'\n", existingTableName);
        goto error_prelock;
    }
    if (db->instant_schema_change) sc.instant_sc = 1;

    BDB_READLOCK("view_add_table");

    /* still one schema change at a time */
    if (thedb->master != gbl_mynode) {
        xerr->errval = SC_VIEW_ERR_EXIST;
        snprintf(xerr->errstr, sizeof(xerr->errstr),
                 "I am not master; master is %s\n", thedb->master);
        goto error;
    }

    if (sc_set_running(1, get_genid(thedb->bdb_env, 0), gbl_mynode,
                       time(NULL)) != 0) {
        xerr->errval = SC_VIEW_ERR_EXIST;
        snprintf(xerr->errstr, sizeof(xerr->errstr), "schema change running");
        goto error;
    }

    /* do the dance */
    sc.nothrevent = 1;
    do_schema_change(&sc);

    xerr->errval = SC_VIEW_NOERR;

error:

    BDB_RELLOCK();

error_prelock:

    free_schema_change_type(&sc);
    return xerr->errval;
}

int sc_timepart_drop_table(const char *tableName, struct errstat *xerr)
{
    bdb_state_type *bdb_state = thedb->bdb_env;
    struct schema_change_type sc = {0};
    struct dbtable *db;
    char *schemabuf = NULL;
    int rc;

    /* prepare sc */
    sc.onstack = 1;
    sc.type = DBTYPE_TAGGED_TABLE;

    snprintf(sc.table, sizeof(sc.table), "%s", tableName);
    sc.table[sizeof(sc.table) - 1] = '\0';

    sc.scanmode = gbl_default_sc_scanmode;

    sc.live = 1;
    sc.use_plan = 1;

    /* this is a table add */
    sc.drop_table = 1;
    sc.fastinit = 1;
    sc.finalize = 1;

    /* get new schema */
    db = get_dbtable_by_name(tableName);
    if (db == NULL) {
        xerr->errval = SC_VIEW_ERR_BUG;
        snprintf(xerr->errstr, sizeof(xerr->errstr), "table '%s' not found\n",
                 tableName);
        goto error_prelock;
    }

    BDB_READLOCK("view_drop_table");

    /* still one schema change at a time */
    if (thedb->master != gbl_mynode) {
        xerr->errval = SC_VIEW_ERR_EXIST;
        snprintf(xerr->errstr, sizeof(xerr->errstr),
                 "I am not master; master is %s\n", thedb->master);
        goto error;
    }

    if (sc_set_running(1, get_genid(thedb->bdb_env, 0), gbl_mynode,
                       time(NULL)) != 0) {
        xerr->errval = SC_VIEW_ERR_EXIST;
        snprintf(xerr->errstr, sizeof(xerr->errstr), "schema change running");
        goto error;
    }

    /* do the dance */
    sc.nothrevent = 1;

    /* dropping the table is another monumental piece of 5 minute dump...
       creates a new temp table and than deletes it... need schema here */
    /*do_crap*/
    {
        /* Find the existing table and use its current schema */
        if (get_csc2_file(db->dbname, -1 /*highest csc2_version*/, &schemabuf,
                          NULL /*csc2len*/)) {
            xerr->errval = SC_VIEW_ERR_BUG;
            snprintf(xerr->errstr, sizeof(xerr->errstr),
                     "%s: could not get schema for table: %s\n", __func__,
                     db->dbname);
            cleanup_strptr(&schemabuf);
            goto error;
        }

        sc.same_schema = 1;
        sc.newcsc2 = schemabuf;
    }

    rc = do_schema_change(&sc);
    if (rc) {
        if (sc_set_running(0, get_genid(thedb->bdb_env, 0), gbl_mynode,
                           time(NULL)) != 0)

            xerr->errval = SC_VIEW_ERR_SC;
        snprintf(xerr->errstr, sizeof(xerr->errstr), "failed to drop table");
        goto error;
    }

    xerr->errval = SC_VIEW_NOERR;

error:

    BDB_RELLOCK();

error_prelock:

    free_schema_change_type(&sc);
    return xerr->errval;
}

/* wrapper handling transaction */
static int do_partition(timepart_views_t *views, const char *name,
                        const char *cmd, struct errstat *err)
{
    struct ireq iq;
    void *tran = NULL;
    int rc;
    int irc = 0;
    int bdberr;
    /*
        init_fake_ireq(thedb, &iq);
        iq.use_handle = thedb->bdb_env;

        rc = trans_start(&iq, NULL, &tran);
        if (rc)
        {
            err->errval = VIEW_ERR_GENERIC;
            snprintf(err->errstr, sizeof(err->errstr), ">%s -- trans_start
       rc:%d\n", __func__, rc);
            return rc;
        }
    */
    rc = views_do_partition(tran, views, name, cmd, err);
    /*
        if(rc)
        {
            irc = trans_abort(&iq, tran);
        }
        else
        {
            irc = trans_commit(&iq, tran, gbl_mynode);
        }
        if(irc)
        {
            err->errval = VIEW_ERR_GENERIC;
            snprintf(err->errstr, sizeof(err->errstr), ">%s -- trans_%s
       rc:%d\n",
                    __func__, (rc)?"abort":"commit", irc);
        }

        tran = NULL;
    */
    if (!rc && !irc) {
        bdberr = 0;
        irc = bdb_llog_views(thedb->bdb_env, (char *)name, 1, &bdberr);
        if (irc) {
            logmsg(LOGMSG_ERROR, "%s -- bdb_llog_views rc:%d bdberr:%d\n",
                   __func__, irc, bdberr);
        }
    }

    return rc;
}

void handle_partition(SBUF2 *sb)
{
#define CHUNK 512
    char line[CHUNK];
    char *viewname;
    char *cmd;
    int len;
    int used;
    int alloc;
    struct errstat xerr = {0};
    int rc;

    if ((rc = sbuf2gets(line, sizeof(line), sb)) < 0) {
        logmsg(LOGMSG_ERROR, "%s -- sbuf2gets rc: %d\n", __func__, rc);
        return;
    }
    viewname = strdup(line);
    if (viewname[strlen(viewname) - 1] == '\n') {
        viewname[strlen(viewname) - 1] = '\0';
    }

    /* Now, read new schema */
    used = alloc = 0;
    cmd = NULL;
    while ((rc = sbuf2gets(line, sizeof(line), sb)) > 0) {
        if (strcmp(line, ".\n") == 0) {
            /* we know we have added a newline to mark end of schema,
               delete that */
            if (cmd && used > 0 && cmd[used - 1] == '\n') {
                cmd[used - 1] = '\0';
                len--;
            }
            break;
        }

        len = strlen(line) + 1; /* include \0 terminator */
        if ((alloc - used) < len) {
            if (len < CHUNK) {
                alloc = alloc + CHUNK;
            } else {
                alloc = alloc + len;
            }
            cmd = realloc(cmd, alloc);
            if (!cmd) {
                logmsg(LOGMSG_ERROR,
                       "appsock_schema_change_inner: out of memory buf=%d\n",
                       alloc);
                sbuf2printf(sb, "!out of memory reading schema\n");
                sbuf2printf(sb, "FAILED\n");
                return;
            }
        }
        memcpy(cmd + used, line, len);
        used += len - 1; /* don't add \0 terminator */
    }

    /* do the work */
    wrlock_schema_lk();
    rc = do_partition(thedb->timepart_views, viewname, cmd, &xerr);
    unlock_schema_lk();

    if (rc == 0)
        sbuf2printf(sb, "SUCCESS\n");
    else
    out:
    sbuf2printf(sb, "FAILED rc %d err \"%s\"\n", xerr.errval, xerr.errstr);

    sbuf2flush(sb);
}

/* shortcut for running table upgrade in a schemachange shell */
int start_table_upgrade(struct dbenv *dbenv, const char *tbl,
                        unsigned long long genid, int full, int partial,
                        int sync)
{
    struct schema_change_type *sc =
        calloc(1, sizeof(struct schema_change_type));
    if (sc == NULL) return ENOMEM;

    if (full == 0 && partial == 0 || full != 0 && partial != 0) {
        free(sc);
        return EINVAL;
    }

    sc->live = 1;
    sc->finalize = 1;
    sc->scanmode = gbl_default_sc_scanmode;
    sc->headers = -1;
    sc->ip_updates = 1;
    sc->instant_sc = 1;
    sc->nothrevent = sync;
    strncpy0(sc->table, tbl, sizeof(sc->table));
    sc->fulluprecs = full;
    sc->partialuprecs = partial;
    sc->start_genid = genid;

    return start_schema_change(sc);
}

static const char *delims = " \n\r\t";
int gbl_commit_sleep;
int gbl_convert_sleep;

int appsock_schema_change(SBUF2 *sb, int *keepsocket)
{

#ifdef DEBUG
    printf("%s: entering\n", __func__);
#endif

    char line[1024];
    int rc;
    char *schemabuf = NULL;
    int buflen = 0;
    int bufpos = 0;
    char *lasts = NULL;
    char *tok = NULL;
    int noschema = 0;

    struct schema_change_type sc = {0};
    sc.type = DBTYPE_TAGGED_TABLE;
    sc.sb = sb;
    sc.nothrevent = 0;
    sc.onstack = 1;
    sc.live = 0;
    sc.drop_table = 0;
    sc.use_plan = 0;
    /* default values: no change */
    sc.headers = 1;
    sc.compress = -1;
    sc.compress_blobs = -1;
    sc.ip_updates = -1;
    sc.instant_sc = -1;
    sc.commit_sleep = gbl_commit_sleep;
    sc.convert_sleep = gbl_convert_sleep;
    sc.dbnum = -1; /* -1 = not changing, anything else = set value */
    listc_init(&sc.dests, offsetof(struct dest, lnk));

    sc.finalize = 1;
    /* DRQS 21247361 - don't allow schema change to run if kicked off from
       a machine from which we don't allow writes (eg dev to alpha) */
    if (gbl_check_schema_change_permissions && sc_request_disallowed(sb)) {
        sbuf2printf(sb, "!Schema change not allowed from this machine (writes "
                        "not allowed on remote)\n");
        sbuf2printf(sb, "FAILED\n");
        return -1;
    }
    // backend_thread_event(thedb, COMDB2_THR_EVENT_START_RDWR);
    sc.scanmode = gbl_default_sc_scanmode;

    if (gbl_default_livesc) sc.live = 1;
    if (gbl_default_plannedsc) sc.use_plan = 1;

    /* First, reject if i am not master */
    if (thedb->master != gbl_mynode) {
        sbuf2printf(sb, "!I am not master, master is %d\n", thedb->master);
        sbuf2printf(sb, "FAILED\n");
        return -1;
    }

    /* Then, read options */
    rc = sbuf2gets(line, sizeof(line), sb);
    if (rc < 0) {
        fprintf(stderr, "appsock schema change I/O error reading options\n");
        return -1;
    }

    tok = strtok_r(line, delims, &lasts);
    while (tok) {

#ifdef DEBUG
        printf("%s: parameter '%s'\n", __func__, tok);
#endif

        if (strcmp(tok, "add") == 0)
            sc.addonly = 1;
        else if (strcmp(tok, "upgrade") == 0)
            sc.fulluprecs = 1;
        else if (strcmp(tok, "addsp") == 0)
            sc.addsp = 1;
        else if (strcmp(tok, "delsp") == 0)
            sc.delsp = 1;
        else if (strcmp(tok, "defaultsp") == 0)
            sc.defaultsp = 1;
        else if (strcmp(tok, "showsp") == 0)
            sc.showsp = 1;
        else if (strcmp(tok, "alter") == 0)
            sc.alteronly = 1;
        else if (strcmp(tok, "live") == 0)
            sc.live = 1;
        else if (strcmp(tok, "nolive") == 0)
            sc.live = 0;
        else if (strcmp(tok, "parallelscan") == 0)
            sc.scanmode = SCAN_PARALLEL;
        else if (strcmp(tok, "dumpscan") == 0)
            sc.scanmode = SCAN_DUMP;
        else if (strcmp(tok, "indexscan") == 0)
            sc.scanmode = SCAN_INDEX;
        else if (strncmp(tok, "table:", 6) == 0)
            strncpy0(sc.table, tok + 6, sizeof(sc.table));
        else if (strcmp(tok, "fullrebuild") == 0)
            sc.force_rebuild = 1;
        else if (strcmp(tok, "rebuildindex") == 0)
            sc.rebuild_index = 1;
        else if (strcmp(tok, "rebuilddata") == 0)
            sc.force_dta_rebuild = 1;
        else if (strcmp(tok, "rebuildblobsdata") == 0) {
            sc.force_dta_rebuild = 1;
            sc.force_blob_rebuild = 1;
        } else if (strcmp(tok, "ignoredisk") == 0 || strcmp(tok, "force") == 0)
            sc.force = 1;
        else if (strcmp(tok, "add_headers") == 0)
            sc.headers = 1;
        else if (strcmp(tok, "remove_headers") == 0)
            sc.headers = 0;
        else if (strcmp(tok, "ipu_on") == 0)
            sc.ip_updates = 1;
        else if (strcmp(tok, "ipu_off") == 0)
            sc.ip_updates = 0;
        else if (strcmp(tok, "instant_sc_on") == 0)
            sc.instant_sc = 1;
        else if (strcmp(tok, "instant_sc_off") == 0)
            sc.instant_sc = 0;

        else if (strcmp(tok, "rec_zlib") == 0)
            sc.compress = BDB_COMPRESS_ZLIB;
        else if (strcmp(tok, "rec_rle8") == 0)
            sc.compress = BDB_COMPRESS_RLE8;
        else if (strcmp(tok, "rec_crle") == 0)
            sc.compress = BDB_COMPRESS_CRLE;
        else if (strcmp(tok, "rec_lz4") == 0)
            sc.compress = BDB_COMPRESS_LZ4;
        else if (strcmp(tok, "rec_nocompress") == 0)
            sc.compress = BDB_COMPRESS_NONE;

        else if (strcmp(tok, "blob_zlib") == 0)
            sc.compress_blobs = BDB_COMPRESS_ZLIB;
        else if (strcmp(tok, "blob_rle8") == 0)
            sc.compress_blobs = BDB_COMPRESS_RLE8;
        else if (strcmp(tok, "blob_lz4") == 0)
            sc.compress_blobs = BDB_COMPRESS_LZ4;
        else if (strcmp(tok, "blob_nocompress") == 0)
            sc.compress_blobs = BDB_COMPRESS_NONE;

        else if (strcmp(tok, "useplan") == 0)
            sc.use_plan = 1;
        else if (strcmp(tok, "noplan") == 0)
            sc.use_plan = 0;
        else if (strcmp(tok, "noschema") == 0)
            noschema = 1;
        else if (strcmp(tok, "fastinit") == 0)
            sc.fastinit = 1;
        else if (strcmp(tok, "drop") == 0) {
            sc.fastinit = 1;
            sc.drop_table = 1;
        } else if (strncmp(tok, "aname:", 6) == 0)
            strncpy0(sc.aname, tok + 6, sizeof(sc.aname));
        else if (strncmp(tok, "commitsleep:", 12) == 0)
            sc.commit_sleep = atoi(tok + 12);
        else if (strncmp(tok, "convsleep:", 10) == 0)
            sc.convert_sleep = atoi(tok + 10);
        else if (strcmp(tok, "dryrun") == 0)
            sc.dryrun = 1;
        else if (strcmp(tok, "statistics") == 0)
            sc.statistics = 1;
        else if (strncmp(tok, "dbnum:", 6) == 0)
            sc.dbnum = atoi(tok + 6);
        else if (strcmp(tok, "trigger") == 0)
            sc.is_trigger = 1;
        else if (strncmp(tok, "dest:", 5) == 0) {
            struct dest *d;
            d = malloc(sizeof(struct dest));
            /* TODO: check for dupes here? */
            if (d == NULL) {
                fprintf(stderr, "%s: malloc can't allocate %d bytes\n",
                        __func__, sizeof(struct dest));
                return -1;
            }
            d->dest = strdup(tok + 5);
            if (d->dest == NULL) {
                fprintf(
                    stderr,
                    "%s: malloc can't allocate %d bytes for destination name\n",
                    __func__, strlen(tok + 5));
                free(d);
                return -1;
            }
            listc_abl(&sc.dests, d);
        } else {
            sbuf2printf(sb, "!unknown option '%s'\n", tok);
            sbuf2printf(sb, "FAILED\n");
            return -1;
        }
        tok = strtok_r(NULL, delims, &lasts);
    }

#ifdef DEBUG
    printf("%s:  sc.table '%s'\n", __func__, sc.table);
    printf("%s:  sc.aname '%s'\n", __func__, sc.aname);
#endif

    if (sc.table[0] == '\0' && !sc.showsp) {
        sbuf2printf(sb, "!no table name specified\n");
        sbuf2printf(sb, "FAILED\n");
        return -1;
    }

    /* This gets overridden later if it's needed */
    sc.fname[0] = '\0';

    /* Now, read new schema */
    while ((rc = sbuf2gets(line, sizeof(line), sb)) > 0) {
        if (strcmp(line, ".\n") == 0) {
            rc = 0;
            break;
        } else if (!noschema) {
            void *newp;
            int len = strlen(line) + 1; /* include \0 terminator */
            if (buflen - bufpos < len) {
                int newlen = buflen + sizeof(line) * 16;
                if (schemabuf)
                    newp = realloc(schemabuf, newlen);
                else
                    newp = malloc(newlen);
                if (!newp) {
                    fprintf(stderr, "appsock_schema_change_inner: out of "
                                    "memory buflen=%d\n",
                            buflen);
                    cleanup_strptr(&schemabuf);
                    sbuf2printf(sb, "!out of memory reading schema\n");
                    sbuf2printf(sb, "FAILED\n");
                    return -1;
                }
                schemabuf = newp;
                buflen = newlen;
            }
            memcpy(schemabuf + bufpos, line, len);
            bufpos += len - 1; /* don't add \0 terminator */
        }
    }
    /* we KNOW that comdb2sc adds a new line to be able to use the .<NL> trick;
       remove it HERE
     */
    if (bufpos > 0 && schemabuf[bufpos - 1] == '\n') {
        schemabuf[bufpos - 1] = '\0';
    }

    if (rc < 0) {
        fprintf(stderr, "appsock schema change I/O error reading schema\n");
        cleanup_strptr(&schemabuf);
        return -1;
    }

    if (sc.addsp) {
        sc.newcsc2 = schemabuf;
        return do_add_sp(&sc, NULL);
    } else if (sc.delsp) {
        sc.newcsc2 = schemabuf;
        return do_del_sp(&sc, NULL);
    } else if (sc.defaultsp) {
        sc.newcsc2 = schemabuf;
        return do_default_sp(&sc, NULL);
    } else if (sc.showsp) {
        sc.newcsc2 = schemabuf;
        return do_show_sp(&sc);
    } else if (sc.is_trigger) {
        sc.newcsc2 = schemabuf;
        return perform_trigger_update(&sc);
    }

    if (noschema) {
        /* Find the existing table and use its current schema */
        struct dbtable *db;
        sc.db = db = get_dbtable_by_name(sc.table);
        if (db == NULL) {
            sbuf2printf(sb, "!table '%s' not found\n", sc.table);
            sbuf2printf(sb, "FAILED\n");
            cleanup_strptr(&schemabuf);
            return -1;
        }

        if (get_csc2_file(db->dbname, -1 /*highest csc2_version*/, &schemabuf,
                          NULL /*csc2len*/)) {
            fprintf(stderr, "%s: could not get schema for table: %s\n",
                    __func__, db->dbname);
            cleanup_strptr(&schemabuf);
            return -1;
        }

        if (sc.rebuild_index) {
            int indx;
            int rc = getidxnumbyname(sc.table, sc.aname, &indx);
            if (rc) {
                sbuf2printf(sb, "!table:index '%s:%s' not found\n", sc.table,
                            sc.aname);
                sbuf2printf(sb, "FAILED\n");
                cleanup_strptr(&schemabuf);
                return -1;
            }
            sc.index_to_rebuild = indx;
        }

        sc.same_schema = 1;
    }
    sc.newcsc2 = schemabuf;

    /* We have our options and a new schema.  Do the schema change.  If we
     * get called from an appsock pool thread (which has a small stack) then
     * make a new thread for this so that we have plenty of room to work. */
    if (sc.dryrun) {
        int rc;
        if (sc_set_running(1, get_genid(thedb->bdb_env, 0), gbl_mynode,
                           time(NULL)) == 0) {
            rc = do_dryrun(&sc);
            sc_set_running(0, sc_seed, gbl_mynode, time(NULL));
        } else {
            sbuf2printf(sb, "!schema change already in progress\n");
            sbuf2printf(sb, "FAILED\n");
            rc = -1;
        }
        free_schema_change_type(&sc);
        return rc;
    }
    /* Print status of a table */
    if (sc.statistics) {
        rc = print_status(&sc);
        cleanup_strptr(&schemabuf);
        return rc;
    } else {
        struct schema_change_type *s = malloc(sizeof(sc));
        if (!s) {
            sbuf2printf(sb, "!out of memory in database\n");
            sbuf2printf(sb, "FAILED\n");
            cleanup_strptr(&schemabuf);
            return -1;
        }
        memcpy(s, &sc, sizeof(sc));

        s->onstack = 0;
        if (sc.fastinit) {
            s->nothrevent = 1;
        } else {
            s->must_close_sb = 1;
            *keepsocket = 1; /* we now own the socket */
            s->nothrevent = 0;
        }
        rc = start_schema_change(s);
        if (rc != SC_OK && rc != SC_ASYNC) {
            return -1;
        }
    }

    return 0;
}

void handle_setcompr(SBUF2 *sb)
{
    int rc;
    struct dbtable *db;
    struct ireq iq;
    char line[256];
    char *tok, *saveptr;
    const char *tbl = NULL, *rec = NULL, *blob = NULL;

    if ((rc = sbuf2gets(line, sizeof(line), sb)) < 0) {
        fprintf(stderr, "%s -- sbuf2gets rc: %d\n", __func__, rc);
        return;
    }
    if ((tok = strtok_r(line, delims, &saveptr)) == NULL) {
        sbuf2printf(sb, ">Bad arguments\n");
        goto out;
    }
    do {
        if (strcmp(tok, "tbl") == 0)
            tbl = strtok_r(NULL, delims, &saveptr);
        else if (strcmp(tok, "rec") == 0)
            rec = strtok_r(NULL, delims, &saveptr);
        else if (strcmp(tok, "blob") == 0)
            blob = strtok_r(NULL, delims, &saveptr);
        else {
            sbuf2printf(sb, ">Bad arguments\n");
            goto out;
        }
    } while ((tok = strtok_r(NULL, delims, &saveptr)) != NULL);

    if (rec == NULL && blob == NULL) {
        sbuf2printf(sb, ">No compression operation specified\n");
        goto out;
    }
    if ((db = get_dbtable_by_name(tbl)) == NULL) {
        sbuf2printf(sb, ">Table not found: %s\n", tbl);
        goto out;
    }
    if (!db->odh) {
        sbuf2printf(sb, ">Table isn't ODH\n");
        goto out;
    }

    init_fake_ireq(thedb, &iq);
    iq.usedb = db;
    iq.sb = sb;

    wrlock_schema_lk();
    rc = do_setcompr(&iq, rec, blob);
    unlock_schema_lk();

    if (rc == 0)
        sbuf2printf(sb, "SUCCESS\n");
    else
    out:
    sbuf2printf(sb, "FAILED\n");

    sbuf2flush(sb);
}

void vsb_printf(loglvl lvl, SBUF2 *sb, const char *sb_prefix,
                const char *prefix, const char *fmt, va_list args)
{
    char line[1024];
    char *s;
    char *next;

    vsnprintf(line, sizeof(line), fmt, args);
    s = line;
    while ((next = strchr(s, '\n'))) {
        *next = 0;

        if (sb) {
            sbuf2printf(sb, "%s%s\n", sb_prefix, s);
            sbuf2flush(sb);
        }
        logmsg(lvl, "%s%s\n", prefix, s);
        ctrace("%s%s\n", prefix, s);

        s = next + 1;
    }
    if (*s) {
        if (sb) {
            sbuf2printf(sb, "%s%s", sb_prefix, s);
            sbuf2flush(sb);
        }

        printf("%s%s\n", prefix, s);
        ctrace("%s%s\n", prefix, s);
    }
}

void sb_printf(SBUF2 *sb, const char *fmt, ...)
{
    va_list args;
    va_start(args, fmt);

    vsb_printf(LOGMSG_INFO, sb, "?", "", fmt, args);

    va_end(args);
}

void sb_errf(SBUF2 *sb, const char *fmt, ...)
{
    va_list args;
    va_start(args, fmt);

    vsb_printf(LOGMSG_ERROR, sb, "!", "", fmt, args);

    va_end(args);
}

void sc_printf(struct schema_change_type *s, const char *fmt, ...)
{
    va_list args;
    va_start(args, fmt);

    if (s->partialuprecs) {
        va_end(args);
        return;
    }

    if (s && s->sb) pthread_mutex_lock(&schema_change_sbuf2_lock);

    vsb_printf(LOGMSG_INFO, (s) ? s->sb : NULL, "?", "Schema change info: ",
               fmt, args);

    if (s && s->sb) pthread_mutex_unlock(&schema_change_sbuf2_lock);

    va_end(args);
}

void sc_errf(struct schema_change_type *s, const char *fmt, ...)
{
    va_list args;
    va_start(args, fmt);

    if (s->partialuprecs) {
        va_end(args);
        return;
    }

    if (s && s->sb) pthread_mutex_lock(&schema_change_sbuf2_lock);

    vsb_printf(LOGMSG_ERROR, (s) ? s->sb : NULL, "!", "Schema change error: ",
               fmt, args);

    if (s && s->sb) pthread_mutex_unlock(&schema_change_sbuf2_lock);

    va_end(args);
}
