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

#include "schemachange.h"
#include "sc_global.h"
#include "sc_logic.h"
#include "sc_struct.h"
#include "sc_queues.h"
#include "sc_lua.h"
#include "sc_add_table.h"
#include "sc_callbacks.h"
#include "sc_schema.h"
#include "crc32c.h"
#include "comdb2_atomic.h"

const char *get_hostname_with_crc32(bdb_state_type *bdb_state,
                                    unsigned int hash);
extern int gbl_test_sc_resume_race;
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

    if (!s->resume &&
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
            logmsg(LOGMSG_INFO, "Resuming schema change: rqid [%llx %s] "
                                "table %s, add %d, drop %d, fastinit %d, alter "
                                "%d, finalize_only %d\n",
                   s->rqid, us, s->table, s->addonly, s->drop_table,
                   s->fastinit, s->alteronly, s->finalize_only);

        } else {
            int bdberr;
            void *packed_sc_data = NULL;
            size_t packed_sc_data_len;
            if (bdb_get_in_schema_change(trans, s->table, &packed_sc_data,
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
    const char *node = gbl_mynode;
    if (s->tran == trans && iq->sc_seed) {
        seed = iq->sc_seed;
        logmsg(LOGMSG_INFO, "Starting schema change: "
                            "transactionally reuse seed 0x%llx\n",
               seed);
    } else if (s->resume) {
        unsigned int host = 0;
        logmsg(LOGMSG_INFO, "Resuming schema change: fetching seed\n");
        if ((rc = fetch_schema_change_seed(s, thedb, &seed, &host))) {
            logmsg(LOGMSG_ERROR, "FAILED to fetch schema change seed\n");
            free_schema_change_type(s);
            return rc;
        }
        node = get_hostname_with_crc32(thedb->bdb_env, host);
        logmsg(
            LOGMSG_INFO,
            "Resuming schema change: fetched seed 0x%llx, original node %s\n",
            seed, node ? node : "(unknown)");
        if (stopsc) {
            errstat_set_strf(&iq->errstat, "Master node downgrading - new "
                                           "master will resume schemachange");
            free_schema_change_type(s);
            return SC_MASTER_DOWNGRADE;
        }
    } else {
        seed = bdb_get_a_genid(thedb->bdb_env);
        logmsg(LOGMSG_INFO, "Starting schema change: new seed 0x%llx\n", seed);
    }
    uuidstr_t us;
    comdb2uuidstr(s->uuid, us);
    rc = sc_set_running(s->table, 1, seed, node, time(NULL));
    if (rc != 0) {
        logmsg(LOGMSG_INFO, "Failed sc_set_running [%llx %s] rc %d\n", s->rqid,
               us, rc);
        if (!s->db->doing_upgrade || s->fulluprecs || s->partialuprecs) {
            errstat_set_strf(&iq->errstat, "Schema change already in progress");
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
                if (!s->db->doing_upgrade)
                    break;
            }

            if (s->db->doing_upgrade) {
                sc_errf(s, "failed to cancel table upgrade threads\n");
                free_schema_change_type(s);
                return SC_CANT_SET_RUNNING;
            } else if (sc_set_running(s->table, 1,
                                      bdb_get_a_genid(thedb->bdb_env),
                                      gbl_mynode, time(NULL)) != 0) {
                free_schema_change_type(s);
                return SC_CANT_SET_RUNNING;
            }
        }
    }

    logmsg(LOGMSG_INFO, "sc_set_running schemachange [%llx %s]\n", s->rqid, us);

    iq->sc_host = node ? crc32c((uint8_t *)node, strlen(node)) : 0;
    if (thedb->master == gbl_mynode && !s->resume && iq->sc_seed != seed) {
        logmsg(LOGMSG_INFO, "Calling bdb_set_disable_plan_genid 0x%llx\n", seed);
        int bdberr;
        int rc = bdb_set_sc_seed(thedb->bdb_env, NULL, s->table, seed,
                                 iq->sc_host, &bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR, "Couldn't save schema change seed\n");
        }
    }
    iq->sc_seed = seed;

    sc_arg_t *arg = malloc(sizeof(sc_arg_t));
    arg->trans = trans;
    arg->iq = iq;
    arg->sc = iq->sc;
    iq->sc->usedbtablevers = iq->usedbtablevers;

    if (s->resume && s->alteronly && !s->finalize_only) {
        if (gbl_test_sc_resume_race) {
            logmsg(LOGMSG_INFO, "%s:%d sleeping 5s for sc_resume test\n",
                   __func__, __LINE__);
            sleep(5);
        }
        ATOMIC_ADD(gbl_sc_resume_start, 1);
    }
    /*
    ** if s->partialuprecs, we're going radio silent from this point forward
    ** in order to produce minimal spew
    */
    if (s->nothrevent) {
        if (!s->partialuprecs)
            logmsg(LOGMSG_INFO, "Executing SYNCHRONOUSLY\n");
        pthread_mutex_lock(&s->mtx);
        rc = do_schema_change_tran(arg);
    } else {
        int max_threads =
            bdb_attr_get(thedb->bdb_attr, BDB_ATTR_SC_ASYNC_MAXTHREADS);
        pthread_mutex_lock(&sc_async_mtx);
        while (!s->resume && max_threads > 0 &&
               sc_async_threads >= max_threads) {
            logmsg(LOGMSG_INFO, "Waiting for avaiable schema change threads\n");
            pthread_cond_wait(&sc_async_cond, &sc_async_mtx);
        }
        sc_async_threads++;
        pthread_mutex_unlock(&sc_async_mtx);

        if (!s->partialuprecs)
            logmsg(LOGMSG_INFO, "Executing ASYNCHRONOUSLY\n");
        pthread_t tid;
        pthread_mutex_lock(&s->mtx);
        rc = pthread_create(&tid, &gbl_pthread_attr_detached,
                            (void *(*)(void *))do_schema_change_tran, arg);
        if (rc) {
            logmsg(LOGMSG_ERROR,
                   "start_schema_change:pthread_create rc %d %s\n", rc,
                   strerror(errno));

            pthread_mutex_lock(&sc_async_mtx);
            sc_async_threads--;
            pthread_mutex_unlock(&sc_async_mtx);

            free(arg);
            sc_set_running(s->table, 0, iq->sc_seed, gbl_mynode, time(NULL));
            free_schema_change_type(s);
            rc = SC_ASYNC_FAILED;
        } else {
            rc = SC_ASYNC;
        }
    }

    return rc;
}

int start_schema_change(struct schema_change_type *s)
{
    struct ireq *iq = NULL;
    iq = (struct ireq *)calloc(1, sizeof(*iq));
    if (iq == NULL) {
        logmsg(LOGMSG_ERROR, "%s: failed to malloc ireq\n", __func__);
        return -1;
    }
    init_fake_ireq(thedb, iq);
    iq->sc = s;
    s->iq = iq;
    if (s->db == NULL) {
        s->db = get_dbtable_by_name(s->table);
    }
    iq->usedb = s->db;
    iq->usedbtablevers = s->db ? s->db->tableversion : 0;
    return start_schema_change_tran(iq, NULL);
}

void delay_if_sc_resuming(struct ireq *iq)
{
    if (gbl_sc_resume_start <= 0)
        return;

    int diff;
    int printerr = 0;
    int start_time = comdb2_time_epochms();
    while (gbl_sc_resume_start > 0) {
        if ((diff = comdb2_time_epochms() - start_time) > 300 && !printerr) {
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
            sc_set_running(s->table, 0, iq->sc_seed, gbl_mynode, time(NULL));
            free_schema_change_type(s);
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
    strncpy0(s->table, db->tablename, sizeof(s->table));

    if (get_csc2_file(db->tablename, -1 /*highest csc2_version*/, &s->newcsc2,
                      NULL /*csc2len*/)) {
        logmsg(LOGMSG_ERROR, "%s: could not get schema for table: %s\n",
               __func__, db->tablename);
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
        backout_schemas(newdb->tablename);
        newdb->schema = NULL;
        freedb(newdb);
    }
    return rc;
}

int live_sc_post_delete_int(struct ireq *iq, void *trans,
                            unsigned long long genid, const void *old_dta,
                            unsigned long long del_keys,
                            blob_buffer_t *oldblobs)
{
    if (iq->usedb->sc_downgrading)
        return ERR_NOMASTER;

    if (iq->usedb->sc_from != iq->usedb) {
        return 0;
    }

    int stripe = get_dtafile_from_genid(genid);
    if (stripe < 0 || stripe >= gbl_dtastripe) {
        logmsg(LOGMSG_ERROR, "%s: genid 0x%llx stripe %d out of range!\n",
               __func__, genid, stripe);
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

    return live_sc_post_del_record(iq, trans, genid, old_dta, del_keys,
                                   oldblobs);
}

int live_sc_post_delete(struct ireq *iq, void *trans, unsigned long long genid,
                        const void *old_dta, unsigned long long del_keys,
                        blob_buffer_t *oldblobs)
{
    int rc = 0;
    pthread_rwlock_rdlock(&sc_live_rwlock);

    rc = live_sc_post_delete_int(iq, trans, genid, old_dta, del_keys, oldblobs);

    pthread_rwlock_unlock(&sc_live_rwlock);
    return rc;
}

int live_sc_post_add_int(struct ireq *iq, void *trans, unsigned long long genid,
                         uint8_t *od_dta, unsigned long long ins_keys,
                         blob_buffer_t *blobs, size_t maxblobs, int origflags,
                         int *rrn)
{
    if (iq->usedb->sc_downgrading)
        return ERR_NOMASTER;

    if (iq->usedb->sc_from != iq->usedb) {
        return 0;
    }

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
    return live_sc_post_add_record(iq, trans, genid, od_dta, ins_keys, blobs,
                                   maxblobs, origflags, rrn);
}

int live_sc_post_add(struct ireq *iq, void *trans, unsigned long long genid,
                     uint8_t *od_dta, unsigned long long ins_keys,
                     blob_buffer_t *blobs, size_t maxblobs, int origflags,
                     int *rrn)
{
    int rc = 0;

    if (gbl_test_scindex_deadlock) {
        logmsg(LOGMSG_INFO, "%s: sleeping for 30s\n", __func__);
        sleep(30);
        logmsg(LOGMSG_INFO, "%s: slept 30s\n", __func__);
    }

    pthread_rwlock_rdlock(&sc_live_rwlock);

    rc = live_sc_post_add_int(iq, trans, genid, od_dta, ins_keys, blobs,
                              maxblobs, origflags, rrn);

    pthread_rwlock_unlock(&sc_live_rwlock);
    return rc;
}

/* should be really called live_sc_post_update_delayed_key_adds() */
int live_sc_delayed_key_adds(struct ireq *iq, void *trans,
                             unsigned long long newgenid, const void *od_dta,
                             unsigned long long ins_keys, int od_len)
{
    int rc = 0;
    pthread_rwlock_rdlock(&sc_live_rwlock);

    rc = live_sc_post_update_delayed_key_adds_int(iq, trans, newgenid, od_dta,
                                                  ins_keys, od_len);

    pthread_rwlock_unlock(&sc_live_rwlock);
    return rc;
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
int live_sc_post_update_int(struct ireq *iq, void *trans,
                            unsigned long long oldgenid, const void *old_dta,
                            unsigned long long newgenid, const void *new_dta,
                            unsigned long long ins_keys,
                            unsigned long long del_keys, int od_len,
                            int *updCols, blob_buffer_t *blobs, size_t maxblobs,
                            int origflags, int rrn, int deferredAdd,
                            blob_buffer_t *oldblobs, blob_buffer_t *newblobs)
{
    if (iq->usedb->sc_downgrading)
        return ERR_NOMASTER;

    if (iq->usedb->sc_from != iq->usedb) {
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
        rc = live_sc_post_del_record(iq, trans, oldgenid, old_dta, del_keys,
                                     oldblobs);
    } else if (!is_newgen_gt_scptr &&
               is_oldgen_gt_scptr) // case 3) newgenid  ..^...  oldgenid
    {
        if (iq->debug)
            reqprintf(
                iq, "C3: newgenid 0x%llx ...scptr 0x%llx ... oldgenid 0x%llx ",
                newgenid, sc_genids[stripe], oldgenid);
        rc = live_sc_post_add_record(iq, trans, newgenid, new_dta, ins_keys,
                                     blobs, maxblobs, origflags, &rrn);
    } else if (!is_newgen_gt_scptr &&
               !is_oldgen_gt_scptr) // case 4) newgenid and oldgenid  ...^..
    {
        if (iq->debug)
            reqprintf(iq,
                      "C4: oldgenid 0x%llx newgenid 0x%llx ... scptr 0x%llx",
                      oldgenid, newgenid, sc_genids[stripe]);
        rc = live_sc_post_upd_record(
            iq, trans, oldgenid, old_dta, newgenid, new_dta, ins_keys, del_keys,
            od_len, updCols, blobs, deferredAdd, oldblobs, newblobs);
    }

    if (iq->debug) reqpopprefixes(iq, 1);

    return rc;
}

int live_sc_post_update(struct ireq *iq, void *trans,
                        unsigned long long oldgenid, const void *old_dta,
                        unsigned long long newgenid, const void *new_dta,
                        unsigned long long ins_keys,
                        unsigned long long del_keys, int od_len, int *updCols,
                        blob_buffer_t *blobs, size_t maxblobs, int origflags,
                        int rrn, int deferredAdd, blob_buffer_t *oldblobs,
                        blob_buffer_t *newblobs)
{
    int rc = 0;
    pthread_rwlock_rdlock(&sc_live_rwlock);

    rc = live_sc_post_update_int(iq, trans, oldgenid, old_dta, newgenid,
                                 new_dta, ins_keys, del_keys, od_len, updCols,
                                 blobs, maxblobs, origflags, rrn, deferredAdd,
                                 oldblobs, newblobs);

    pthread_rwlock_unlock(&sc_live_rwlock);
    return rc;
}

/**********************************************************************/
/* I ORIGINALLY REMOVED THIS, THEN MERGING I SAW IT BACK IN COMDB2.C
    I AM LEAVING IT IN HERE FOR NOW (GOTTA ASK MARK)               */

static int add_table_for_recovery(struct ireq *iq, struct schema_change_type *s)
{
    struct dbtable *db;
    struct dbtable *newdb;
    int bdberr;
    int rc;

    db = get_dbtable_by_name(s->table);
    if (db == NULL) {
        wrlock_schema_lk();
        rc = do_add_table(iq, s, NULL);
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
    newdb->version = get_csc2_version(newdb->tablename);

    if (add_cmacc_stmt(newdb, 1) != 0) {
        backout_schemas(newdb->tablename);
        abort();
    }

    if (verify_constraints_exist(NULL, newdb, newdb, s) != 0) {
        backout_schemas(newdb->tablename);
        abort();
    }

    bdb_get_new_prefix(new_prefix, sizeof(new_prefix), &bdberr);

    rc = open_temp_db_resume(newdb, new_prefix, 1, 0, NULL);
    if (rc) {
        backout_schemas(newdb->tablename);
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
        if (bdb_get_in_schema_change(NULL /*tran*/, thedb->dbs[i]->tablename,
                                     &packed_sc_data, &packed_sc_data_len,
                                     &bdberr) ||
            bdberr != BDBERR_NOERROR) {
            logmsg(LOGMSG_ERROR,
                   "%s: failed to discover "
                   "whether table: %s is in the middle of a schema change\n",
                   __func__, thedb->dbs[i]->tablename);
            continue;
        }

        /* if we got some data back, that means we were in a schema change */
        if (packed_sc_data) {
            struct schema_change_type *s;
            logmsg(LOGMSG_WARN, "%s: table: %s is in the middle of a "
                                "schema change, adding table...\n",
                   __func__, thedb->dbs[i]->tablename);

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
                rc = bdb_set_in_schema_change(NULL, thedb->dbs[i]->tablename,
                                              NULL, 0, &bdberr);
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
            rc = add_table_for_recovery(&iq, s);

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

    init_schemachange_type(&sc);
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
    if (get_csc2_file(db->tablename, -1 /*highest csc2_version*/, &schemabuf,
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

    if (sc_set_running(sc.table, 1, bdb_get_a_genid(thedb->bdb_env), gbl_mynode,
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

    init_schemachange_type(&sc);
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

    if (sc_set_running(sc.table, 1, bdb_get_a_genid(thedb->bdb_env), gbl_mynode,
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
        if (get_csc2_file(db->tablename, -1 /*highest csc2_version*/,
                          &schemabuf, NULL /*csc2len*/)) {
            xerr->errval = SC_VIEW_ERR_BUG;
            snprintf(xerr->errstr, sizeof(xerr->errstr),
                     "%s: could not get schema for table: %s\n", __func__,
                     db->tablename);
            cleanup_strptr(&schemabuf);
            goto error;
        }

        sc.same_schema = 1;
        sc.newcsc2 = schemabuf;
    }

    rc = do_schema_change(&sc);
    if (rc) {
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

/* shortcut for running table upgrade in a schemachange shell */
int start_table_upgrade(struct dbenv *dbenv, const char *tbl,
                        unsigned long long genid, int full, int partial,
                        int sync)
{
    struct schema_change_type *sc =
        calloc(1, sizeof(struct schema_change_type));
    if (sc == NULL) return ENOMEM;

    if ((full == 0 && partial == 0) || (full != 0 && partial != 0)) {
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

    if (s && s->partialuprecs) {
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

    if (s && s->partialuprecs) {
        va_end(args);
        return;
    }

    if (s && s->sb) pthread_mutex_lock(&schema_change_sbuf2_lock);

    vsb_printf(LOGMSG_ERROR, (s) ? s->sb : NULL, "!", "Schema change error: ",
               fmt, args);

    if (s && s->sb) pthread_mutex_unlock(&schema_change_sbuf2_lock);

    va_end(args);
}
