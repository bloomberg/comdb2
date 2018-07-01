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
#include <poll.h>

#include <memory_sync.h>
#include <str0.h>
#include <views.h>

#include "schemachange.h"
#include "sc_global.h"
#include "sc_logic.h"
#include "sc_util.h"
#include "sc_struct.h"
#include "sc_queues.h"
#include "sc_schema.h"
#include "sc_lua.h"
#include "sc_add_table.h"
#include "sc_alter_table.h"
#include "sc_fastinit_table.h"
#include "sc_stripes.h"
#include "sc_drop_table.h"
#include "sc_rename_table.h"
#include "logmsg.h"
#include "comdb2_atomic.h"

/**** Utility functions */

static enum thrtype prepare_sc_thread(struct schema_change_type *s)
{
    struct thr_handle *thr_self = thrman_self();
    enum thrtype oldtype = THRTYPE_UNKNOWN;

    if (!s->partialuprecs)
        logmsg(LOGMSG_DEBUG, "Starting a schemachange thread\n");

    if (!s->nothrevent) {
        if (thr_self) {
            thread_started("schema change");
            oldtype = thrman_get_type(thr_self);
            thrman_change_type(thr_self, THRTYPE_SCHEMACHANGE);
        } else
            thr_self = thrman_register(THRTYPE_SCHEMACHANGE);
        if (!s->nothrevent)
            backend_thread_event(thedb, COMDB2_THR_EVENT_START_RDWR);
    }
    return oldtype;
}

static void reset_sc_thread(enum thrtype oldtype, struct schema_change_type *s)
{
    struct thr_handle *thr_self = thrman_self();
    if (!s->nothrevent) {
        backend_thread_event(thedb, COMDB2_THR_EVENT_DONE_RDWR);

        /* restore our  thread type to what it was before */
        if (oldtype != THRTYPE_UNKNOWN) thrman_change_type(thr_self, oldtype);
    }
}

/* if we're using the low level meta table and we are doing a normal change,
 * mark the table as being in a schema change so that if we are interrupted
 * the new master knows to resume
 * We mark schemachange over in mark_schemachange_over()
 */
static int mark_sc_in_llmeta_tran(struct schema_change_type *s, void *trans)
{
    int bdberr;
    int rc = SC_OK;
    void *packed_sc_data = NULL;
    size_t packed_sc_data_len;
    uuidstr_t us;
    comdb2uuidstr(s->uuid, us);
    logmsg(LOGMSG_INFO, "%s: table '%s' rqid [%llx %s]\n", __func__, s->table,
           s->rqid, us);
    if (pack_schema_change_type(s, &packed_sc_data, &packed_sc_data_len)) {
        sc_errf(s, "could not pack the schema change data for storage in "
                   "low level meta table\n");
        return SC_LLMETA_ERR;
    } else {
        unsigned retries;
        unsigned max_retries = 10;

        /* mark the schema change in progress in the low level meta table,
         * retry several times */
        for (retries = 0;
             retries < max_retries &&
             (bdb_set_in_schema_change(trans, s->table, packed_sc_data,
                                       packed_sc_data_len, &bdberr) ||
              bdberr != BDBERR_NOERROR);
             ++retries) {
            sc_errf(s, "could not mark schema change in progress in the "
                       "low level meta table, retrying ...\n");
            sleep(1);
        }
        if (retries >= max_retries) {
            sc_errf(s, "could not mark schema change in progress in the "
                       "low level meta table, giving up after %u retries\n",
                    retries);
            rc = SC_LLMETA_ERR;
            if (s->resume) {
                sc_errf(s, "failed to resume schema change, downgrading to "
                           "give another master a shot\n");
                bdb_transfermaster(thedb->dbs[0]->handle);
            }
        }
    }

    free(packed_sc_data);
    return rc;
}

static int mark_sc_in_llmeta(struct schema_change_type *s)
{
    return mark_sc_in_llmeta_tran(s, NULL);
}

static int propose_sc(struct schema_change_type *s)
{
    /* Check that all nodes are ready to do this schema change. */
    int rc = broadcast_sc_start(s->table, s->iq->sc_seed, s->iq->sc_host,
                                time(NULL));
    if (rc != 0) {
        rc = SC_PROPOSE_FAIL;
        sc_errf(s, "unable to gain agreement from all nodes to do schema "
                   "change\n");
        sc_errf(s, "check that all nodes are connected ('send bdb "
                   "cluster')\n");
    } else {
        /* if we're not actually changing the schema then everything is fully
         * replicated so we don't actually need all the replicants online to
         * do this safely.  this helps save fastinit. */
        if (!s->same_schema) {
            if (check_sc_ok(s) != 0) {
                rc = SC_PROPOSE_FAIL;
            } else {
                rc = broadcast_sc_ok();
                if (rc != 0) {
                    sc_errf(s, "cannot perform schema change; not all nodes "
                               "acknowledged readiness\n");
                    rc = SC_PROPOSE_FAIL;
                }
            }
        }

        if (s->force) {
            sc_printf(s, "Performing schema change regardless in force "
                         "mode\n");
            rc = SC_OK;
        }
    }
    return rc;
}

static int master_downgrading(struct schema_change_type *s)
{
    if (stopsc) {
        if (!s->nothrevent)
            backend_thread_event(thedb, COMDB2_THR_EVENT_DONE_RDWR);
        if (s->sb) {
            sbuf2printf(s->sb, "!Master node downgrading - new master will "
                               "resume schemachange\n");
            sbuf2close(s->sb);
        }
        logmsg(
            LOGMSG_WARN,
            "Master node downgrading - new master will resume schemachange\n");
        gbl_schema_change_in_progress = 0;
        return SC_MASTER_DOWNGRADE;
    }
    return SC_OK;
}

static void free_sc(struct schema_change_type *s)
{
    free_schema_change_type(s);
    /* free any memory csc2 allocated when parsing schema */
    pthread_mutex_lock(&csc2_subsystem_mtx);
    csc2_free_all();
    pthread_mutex_unlock(&csc2_subsystem_mtx);
}

static void stop_and_free_sc(int rc, struct schema_change_type *s, int do_free)
{
    if (!s->partialuprecs) {
        if (rc != 0) {
            logmsg(LOGMSG_INFO, "Schema change returning FAILED\n");
            sbuf2printf(s->sb, "FAILED\n");
        } else {
            logmsg(LOGMSG_INFO, "Schema change returning SUCCESS\n");
            sbuf2printf(s->sb, "SUCCESS\n");
        }
    }
    sc_set_running(s->table, 0, s->iq->sc_seed, NULL, 0);
    if (do_free) {
        free_sc(s);
    }
}

static int set_original_tablename(struct schema_change_type *s)
{
    struct dbtable *db = get_dbtable_by_name(s->table);
    if (db) {
        strncpy0(s->table, db->tablename, sizeof(s->table));
        return 0;
    }
    return 1;
}

int do_upgrade_table(struct schema_change_type *s)
{
    int rc;

    set_original_tablename(s);

    if (!s->resume) set_sc_flgs(s);
    if ((rc = mark_sc_in_llmeta(s))) return rc;

    if (rc == SC_OK) rc = do_upgrade_table_int(s);

    if (rc) {
        mark_schemachange_over(s->table);
    } else if (s->finalize) {
        rc = finalize_upgrade_table(s);
    } else {
        rc = SC_COMMIT_PENDING;
    }

    return rc;
}

typedef int (*ddl_t)(struct ireq *, struct schema_change_type *, tran_type *);

/*
** Start transaction if not passed in (comdb2sc.tsk)
** If started transaction, then
**   1. also commit it
**   2. log scdone here
*/
static int do_finalize(ddl_t func, struct ireq *iq,
                       struct schema_change_type *s, tran_type *input_tran,
                       scdone_t type)
{
    int rc;
    tran_type *tran = input_tran;

    if (tran == NULL) {
        rc = trans_start_sc(iq, NULL, &tran);
        if (rc) {
            sc_errf(s, "Failed to start finalize transaction\n");
            return rc;
        }
    }

    rc = func(iq, s, tran);

    if (rc) {
        if (input_tran == NULL) {
            trans_abort(iq, tran);
            mark_schemachange_over(s->table);
            sc_del_unused_files(s->db);
        }
        return rc;
    }

    if (input_tran == NULL) {
        // void all_locks(void*);
        // all_locks(thedb->bdb_env);
        rc = trans_commit_adaptive(iq, tran, gbl_mynode);
        if (rc) {
            sc_errf(s, "Failed to commit finalize transaction\n");
            return rc;
        }

        int bdberr = 0;
        if ((rc = bdb_llog_scdone(s->db->handle, type, 1, &bdberr)) ||
            bdberr != BDBERR_NOERROR) {
            sc_errf(s, "Failed to send scdone rc=%d bdberr=%d\n", rc, bdberr);
            return -1;
        }
        sc_del_unused_files(s->db);
    } else if (bdb_attr_get(thedb->bdb_attr, BDB_ATTR_SC_DONE_SAME_TRAN)) {
        int bdberr = 0;
        rc = bdb_llog_scdone_tran(s->db->handle, type, input_tran, s->table,
                                  &bdberr);
        if (rc || bdberr != BDBERR_NOERROR) {
            sc_errf(s, "Failed to send scdone rc=%d bdberr=%d\n", rc, bdberr);
            return -1;
        }
    }
    return rc;
}

static int check_table_version(struct ireq *iq, struct schema_change_type *sc)
{
    if (sc->addonly || sc->resume)
        return 0;
    int rc, bdberr;
    unsigned long long version;
    rc = bdb_table_version_select(sc->table, NULL, &version, &bdberr);
    if (rc != 0) {
        errstat_set_strf(&iq->errstat,
                         "failed to get version for table:%s rc:%d", sc->table,
                         rc);
        iq->errstat.errval = ERR_SC;
        return SC_INTERNAL_ERROR;
    }
    if (sc->usedbtablevers != version) {
        errstat_set_strf(&iq->errstat,
                         "stale version for table:%s master:%d replicant:%d",
                         sc->table, version, iq->usedbtablevers);
        iq->errstat.errval = ERR_SC;
        return SC_INTERNAL_ERROR;
    }
    return 0;
}

static int do_ddl(ddl_t pre, ddl_t post, struct ireq *iq,
                  struct schema_change_type *s, tran_type *tran, scdone_t type)
{
    int rc;
    if (s->finalize_only) {
        return s->sc_rc;
    }
    set_original_tablename(s);
    if ((rc = check_table_version(iq, s)) != 0) { // non-tran ??
        goto end;
    }
    if (!s->resume)
        set_sc_flgs(s);
    if ((rc = mark_sc_in_llmeta_tran(s, NULL))) // non-tran ??
        goto end;
    broadcast_sc_start(s->table, iq->sc_seed, iq->sc_host,
                       time(NULL));                   // dont care rcode
    rc = pre(iq, s, NULL);                            // non-tran ??
    if (type == alter && master_downgrading(s)) {
        s->sc_rc = SC_MASTER_DOWNGRADE;
        errstat_set_strf(
            &iq->errstat,
            "Master node downgrading - new master will resume schemachange\n");
        return SC_MASTER_DOWNGRADE;
    }
    if (rc) {
        mark_schemachange_over_tran(s->table, NULL); // non-tran ??
        broadcast_sc_end(s->table, iq->sc_seed);
    } else if (s->finalize) {
        wrlock_schema_lk();
        rc = do_finalize(post, iq, s, tran, type);
        unlock_schema_lk();
        if (type == fastinit && gbl_replicate_local)
            local_replicant_write_clear(iq, tran, s->db);
        broadcast_sc_end(s->table, iq->sc_seed);
    } else {
        rc = SC_COMMIT_PENDING;
    }
end:
    s->sc_rc = rc;
    return rc;
}

int do_alter_queues(struct schema_change_type *s)
{
    struct dbtable *db;
    int rc, bdberr;

    set_original_tablename(s);

    if (!s->resume) set_sc_flgs(s);

    rc = propose_sc(s);

    if (rc == SC_OK) rc = do_alter_queues_int(s);

    if (master_downgrading(s)) return SC_MASTER_DOWNGRADE;

    broadcast_sc_end(s->table, s->iq->sc_seed);

    if ((s->type != DBTYPE_TAGGED_TABLE) && gbl_pushlogs_after_sc)
        push_next_log();

    return rc;
}

int do_alter_stripes(struct schema_change_type *s)
{
    struct dbtable *db;
    int rc, bdberr;

    set_original_tablename(s);

    if (!s->resume) set_sc_flgs(s);

    rc = propose_sc(s);

    if (rc == SC_OK) rc = do_alter_stripes_int(s);

    if (master_downgrading(s)) return SC_MASTER_DOWNGRADE;

    broadcast_sc_end(s->table, s->iq->sc_seed);

    /* if we did a regular schema change and we used the llmeta we don't need to
     * push locgs */
    if ((s->type != DBTYPE_TAGGED_TABLE) && gbl_pushlogs_after_sc)
        push_next_log();

    return rc;
}

int do_schema_change_tran(sc_arg_t *arg)
{
    struct ireq *iq = arg->iq;
    tran_type *trans = arg->trans;
    struct schema_change_type *s = arg->sc;
    free(arg);

    if (iq == NULL) {
        abort();
    }

    s->iq = iq;
    enum thrtype oldtype = prepare_sc_thread(s);
    int rc = SC_OK;

    if (s->addsp)
        rc = do_add_sp(s, iq);
    else if (s->delsp)
        rc = do_del_sp(s, iq);
    else if (s->defaultsp)
        rc = do_default_sp(s, iq);
    else if (s->showsp)
        rc = do_show_sp(s);
    else if (s->is_trigger)
        rc = perform_trigger_update(s);
    else if (s->is_sfunc)
        rc = do_lua_sfunc(s);
    else if (s->is_afunc)
        rc = do_lua_afunc(s);
    else if (s->fastinit && s->drop_table)
        rc = do_ddl(do_drop_table, finalize_drop_table, iq, s, trans, drop);
    else if (s->fastinit)
        rc = do_ddl(do_fastinit, finalize_fastinit_table, iq, s, trans,
                    fastinit);
    else if (s->addonly)
        rc = do_ddl(do_add_table, finalize_add_table, iq, s, trans, add);
    else if (s->rename)
        rc = do_ddl(do_rename_table, finalize_rename_table, iq, s, trans,
                    rename_table);
    else if (s->fulluprecs || s->partialuprecs)
        rc = do_upgrade_table(s);
    else if (s->type == DBTYPE_TAGGED_TABLE)
        rc = do_ddl(do_alter_table, finalize_alter_table, iq, s, trans, alter);
    else if (s->type == DBTYPE_QUEUE)
        rc = do_alter_queues(s);
    else if (s->type == DBTYPE_MORESTRIPE)
        rc = do_alter_stripes(s);

    if (rc == SC_MASTER_DOWNGRADE) {
        if (s && s->newdb && s->newdb->handle) {
            int bdberr;

            if (!trans)
                backend_thread_event(thedb, COMDB2_THR_EVENT_START_RDWR);

            /* return NOMASTER for live schemachange writes */
            start_exclusive_backend_request(thedb);
            pthread_rwlock_wrlock(&sc_live_rwlock);
            s->db->sc_to = NULL;
            s->db->sc_from = NULL;
            s->db->sc_abort = 0;
            s->db->sc_downgrading = 1;
            pthread_rwlock_unlock(&sc_live_rwlock);
            end_backend_request(thedb);

            bdb_close_only(s->newdb->handle, &bdberr);
            freedb(s->newdb);
            s->newdb = NULL;

            if (!trans)
                backend_thread_event(thedb, COMDB2_THR_EVENT_DONE_RDWR);
        }
    }
    reset_sc_thread(oldtype, s);
    if (rc && rc != SC_COMMIT_PENDING) {
        logmsg(LOGMSG_ERROR, ">>> SCHEMA CHANGE ERROR: TABLE %s, RC %d\n",
               s->table, rc);
        iq->sc_should_abort = 1;
    }
    s->sc_rc = rc;
    if (!s->nothrevent) {
        pthread_mutex_lock(&sc_async_mtx);
        sc_async_threads--;
        pthread_cond_broadcast(&sc_async_cond);
        pthread_mutex_unlock(&sc_async_mtx);
    }
    if (s->resume == SC_NEW_MASTER_RESUME || rc == SC_COMMIT_PENDING ||
        (!s->nothrevent && !s->finalize)) {
        pthread_mutex_unlock(&s->mtx);
        return rc;
    }
    pthread_mutex_unlock(&s->mtx);
    if (rc == SC_MASTER_DOWNGRADE) {
        free_sc(s);
    } else {
        stop_and_free_sc(rc, s, 1 /*do_free*/);
    }
    return rc;
}

int do_schema_change(struct schema_change_type *s)
{
    int rc = 0;
    struct ireq *iq = NULL;
    iq = (struct ireq *)calloc(1, sizeof(*iq));
    if (iq == NULL) {
        logmsg(LOGMSG_ERROR, "%s: failed to malloc ireq\n", __func__);
        return -1;
    }
    init_fake_ireq(thedb, iq);
    iq->sc = s;
    if (s->db == NULL) {
        s->db = get_dbtable_by_name(s->table);
    }
    iq->usedb = s->db;
    s->usedbtablevers = iq->usedbtablevers = s->db ? s->db->tableversion : 0;
    sc_arg_t *arg = malloc(sizeof(sc_arg_t));
    arg->iq = iq;
    arg->sc = s;
    arg->trans = NULL;
    pthread_mutex_lock(&s->mtx);
    rc = do_schema_change_tran(arg);
    free(iq);
    return rc;
}

int finalize_schema_change_thd(struct ireq *iq, tran_type *trans)
{
    if (iq == NULL || iq->sc == NULL) abort();
    struct schema_change_type *s = iq->sc;
    pthread_mutex_lock(&s->mtx);
    enum thrtype oldtype = prepare_sc_thread(s);
    int rc = SC_OK;

    if (gbl_test_scindex_deadlock) {
        logmsg(LOGMSG_INFO, "%s: sleeping for 30s\n", __func__);
        sleep(30);
        logmsg(LOGMSG_INFO, "%s: slept 30s\n", __func__);
    }
    if (s->is_trigger)
        rc = finalize_trigger(s);
    else if (s->is_sfunc)
        rc = finalize_lua_sfunc();
    else if (s->is_afunc)
        rc = finalize_lua_afunc();
    else if (s->fastinit && s->drop_table)
        rc = do_finalize(finalize_drop_table, iq, s, trans, drop);
    else if (s->fastinit)
        rc = do_finalize(finalize_fastinit_table, iq, s, trans, fastinit);
    else if (s->addonly)
        rc = do_finalize(finalize_add_table, iq, s, trans, add);
    else if (s->rename)
        rc = do_finalize(finalize_rename_table, iq, s, trans, rename_table);
    else if (s->type == DBTYPE_TAGGED_TABLE)
        rc = do_finalize(finalize_alter_table, iq, s, trans, alter);
    else if (s->fulluprecs || s->partialuprecs)
        rc = finalize_upgrade_table(s);

    reset_sc_thread(oldtype, s);
    pthread_mutex_unlock(&s->mtx);

    stop_and_free_sc(rc, s, 0 /*free_sc*/);
    return rc;
}

void *sc_resuming_watchdog(void *p)
{
    struct ireq iq;
    struct schema_change_type *stored_sc = NULL;
    int time = bdb_attr_get(thedb->bdb_attr, BDB_ATTR_SC_RESUME_WATCHDOG_TIMER);
    logmsg(LOGMSG_INFO, "%s: started, sleeping %d seconds\n", __func__, time);
    sleep(time);
    logmsg(LOGMSG_INFO, "%s: waking up\n", __func__);
    bdb_thread_event(thedb->bdb_env, BDBTHR_EVENT_START_RDWR);
    init_fake_ireq(thedb, &iq);
    pthread_mutex_lock(&sc_resuming_mtx);
    stored_sc = sc_resuming;
    while (stored_sc) {
        iq.sc = stored_sc;
        if (iq.sc->db)
            iq.sc->db->sc_abort = 1;
        pthread_mutex_lock(&(iq.sc->mtx));
        stored_sc = stored_sc->sc_next;
        logmsg(LOGMSG_INFO, "%s: aborting schema change of table '%s'\n",
               __func__, iq.sc->table);
        mark_schemachange_over(iq.sc->table);
        if (iq.sc->addonly) {
            delete_temp_table(&iq, iq.sc->db);
            if (iq.sc->addonly == SC_DONE_ADD)
                delete_db(iq.sc->table);
        }
        sc_del_unused_files(iq.sc->db);
        pthread_mutex_unlock(&(iq.sc->mtx));
        free_schema_change_type(iq.sc);
        iq.sc = NULL;
    }
    sc_resuming = NULL;
    logmsg(LOGMSG_INFO, "%s: existing\n", __func__);
    bdb_thread_event(thedb->bdb_env, BDBTHR_EVENT_DONE_RDWR);
    pthread_mutex_unlock(&sc_resuming_mtx);
    return NULL;
}

int resume_schema_change(void)
{
    int i;
    int rc;
    int scabort = 0;

    /* if we're not the master node then we can't do schema change! */
    if (thedb->master != gbl_mynode) {
        logmsg(LOGMSG_WARN,
               "resume_schema_change: not the master, cannot resume a"
               " schema change\n");
        return -1;
    }

    /* if a schema change is currently running don't try to resume one */
    sc_set_running(NULL, 0, 0, NULL, 0);

    pthread_mutex_lock(&sc_resuming_mtx);
    sc_resuming = NULL;
    for (i = 0; i < thedb->num_dbs; ++i) {
        int bdberr;
        void *packed_sc_data = NULL;
        size_t packed_sc_data_len;
        if (bdb_get_in_schema_change(NULL /*tran*/, thedb->dbs[i]->tablename,
                                     &packed_sc_data, &packed_sc_data_len,
                                     &bdberr) ||
            bdberr != BDBERR_NOERROR) {
            logmsg(LOGMSG_WARN,
                   "resume_schema_change: failed to discover "
                   "whether table: %s is in the middle of a schema change\n",
                   thedb->dbs[i]->tablename);
            continue;
        }

        /* if we got some data back, that means we were in a schema change */
        if (packed_sc_data) {
            struct schema_change_type *s;
            logmsg(LOGMSG_WARN,
                   "resume_schema_change: table: %s is in the middle of a "
                   "schema change, resuming...\n",
                   thedb->dbs[i]->tablename);

            s = new_schemachange_type();
            if (!s) {
                logmsg(LOGMSG_ERROR,
                       "resume_schema_change: ran out of memory\n");
                free(packed_sc_data);
                pthread_mutex_unlock(&sc_resuming_mtx);
                return -1;
            }

            if (unpack_schema_change_type(s, packed_sc_data,
                                          packed_sc_data_len)) {
                sc_errf(s, "could not unpack the schema change data retrieved "
                           "from the low level meta table\n");
                free(packed_sc_data);
                free(s);
                pthread_mutex_unlock(&sc_resuming_mtx);
                return -1;
            }

            free(packed_sc_data);

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

            if (scabort) {
                logmsg(LOGMSG_WARN, "Cancelling schema change\n");
                rc = unlink(abort_filename);
                if (rc)
                    logmsg(LOGMSG_ERROR, "Can't delete abort marker file %s - "
                                         "future sc may abort\n",
                           abort_filename);
                free(abort_filename);
                free(s);
                pthread_mutex_unlock(&sc_resuming_mtx);
                return 0;
            }
            free(abort_filename);

            if (s->fulluprecs || s->partialuprecs) {
                logmsg(LOGMSG_DEBUG,
                       "%s: This was a table upgrade. Skipping...\n", __func__);
                free_schema_change_type(s);
                continue;
            }
            if (s->type !=
                DBTYPE_TAGGED_TABLE) { /* see do_schema_change_thd()*/
                logmsg(LOGMSG_ERROR,
                       "%s: only type DBTYPE_TAGGED_TABLE can resume\n",
                       __func__);
                free_schema_change_type(s);
                continue;
            }

            s->nothrevent = 0;
            /* we are trying to resume this sc */

            uuidstr_t us;
            comdb2uuidstr(s->uuid, us);
            logmsg(LOGMSG_INFO,
                   "%s: resuming schema change: rqid [%llx %s] "
                   "table %s, add %d, drop %d, fastinit %d, alter %d\n",
                   __func__, s->rqid, us, s->table, s->addonly, s->drop_table,
                   s->fastinit, s->alteronly);

            if (bdb_attr_get(thedb->bdb_attr, BDB_ATTR_SC_RESUME_AUTOCOMMIT) &&
                s->rqid == 0 && comdb2uuid_is_zero(s->uuid)) {
                s->resume = SC_RESUME;
                s->finalize = 1; /* finalize at the end of resume */
            } else {
                s->resume = SC_NEW_MASTER_RESUME;
                s->finalize = 0; /* wait for resubmit of bplog */
            }

            MEMORY_SYNC;

            /* start the schema change back up */
            rc = start_schema_change(s);
            if (rc != SC_OK && rc != SC_ASYNC) {
                pthread_mutex_unlock(&sc_resuming_mtx);
                return -1;
            } else if (s->finalize == 0) {
                s->sc_next = sc_resuming;
                sc_resuming = s;
            }
        }
    }

    if (sc_resuming) {
        pthread_t tid;
        rc = pthread_create(&tid, NULL, sc_resuming_watchdog, NULL);
        if (rc)
            logmsg(LOGMSG_ERROR, "%s: failed to start sc_resuming_watchdog\n",
                   __FILE__);
    }
    pthread_mutex_unlock(&sc_resuming_mtx);
    return 0;
}

/****************** Table functions ***********************************/
/****************** Functions down here will likely be moved elsewhere *****/

/* this assumes threads are not active in db */
int open_temp_db_resume(struct dbtable *db, char *prefix, int resume, int temp,
                        tran_type *tran)
{
    char *tmpname;
    int bdberr;
    int nbytes;

    nbytes = snprintf(NULL, 0, "%s%s", prefix, db->tablename);
    if (nbytes <= 0) nbytes = 2;
    nbytes++;
    tmpname = malloc(nbytes);
    snprintf(tmpname, nbytes, "%s%s", prefix, db->tablename);

    db->handle = NULL;

    /* open existing temp db if it's there (ie we're resuming after a master
     * switch) */
    if (resume) {
        db->handle = bdb_open_more(
            tmpname, db->dbenv->basedir, db->lrl, db->nix,
            (short *)db->ix_keylen, db->ix_dupes, db->ix_recnums,
            db->ix_datacopy, db->ix_collattr, db->ix_nullsallowed,
            db->numblobs + 1, /* one main record + the blobs blobs */
            db->dbenv->bdb_env, &bdberr);

        if (db->handle)
            logmsg(LOGMSG_INFO,
                   "Found existing tempdb: %s, attempting to resume an in "
                   "progress schema change\n",
                   tmpname);
        else {
            logmsg(LOGMSG_ERROR,
                   "Didn't find existing tempdb: %s, aborting schema change\n",
                   tmpname);
            free(tmpname);
            return -1;
        }
    }

    if (!db->handle) /* did not/could not open existing one, creating new one */
    {
        db->handle = bdb_create_tran(
            tmpname, db->dbenv->basedir, db->lrl, db->nix,
            (short *)db->ix_keylen, db->ix_dupes, db->ix_recnums,
            db->ix_datacopy, db->ix_collattr, db->ix_nullsallowed,
            db->numblobs + 1, /* one main record + the blobs blobs */
            db->dbenv->bdb_env, temp, &bdberr, tran);
        if (db->handle == NULL) {
            logmsg(LOGMSG_ERROR, "%s: failed to open %s, rcode %d\n", __func__,
                   tmpname, bdberr);
            free(tmpname);
            return -1;
        }
    }

    /* clone the blobstripe genid.  this will definately be needed in the
     * future when we don't change genids on schema change, but right now
     * isn't really needed. */
    bdb_set_blobstripe_genid(db->handle, db->blobstripe_genid);
    free(tmpname);
    return 0;
}

/**
 * Verify a new schema change temporary db.  A newly created/resumed db should
 * have file versions that are all strictly greater than all of the original
 * db's file versions.
 * Schema change didn't used to delete new.tablename file versions from llmeta.
 * If a schema change failed before a newdb was created, the new master would
 * try to resume the sc and it could 'reopen' the temp db using old/stale
 * new.tablename file versions causing horrifying bugs.
 * @return returns 0 on success; !0 otherwise
 */
int verify_new_temp_sc_db(struct dbtable *p_db, struct dbtable *p_newdb, tran_type *tran)
{
    int i;
    int bdberr;
    unsigned long long db_max_file_version;
    unsigned long long newdb_min_file_version;

    /* find the db's smallest file version */

    db_max_file_version = 0;

    for (i = 0; i < (1 /*dta*/ + p_db->numblobs); ++i) {
        unsigned long long file_version;

        if (bdb_get_file_version_data(p_db->handle, tran, i, &file_version,
                                      &bdberr) ||
            bdberr != BDBERR_NOERROR) {
            logmsg(LOGMSG_ERROR, "%s: bdb_get_file_version_data failed for db "
                                 "data %d\n",
                   __func__, i);
            return -1;
        }

        if (sc_cmp_fileids(file_version, db_max_file_version) > 0)
            db_max_file_version = file_version;
    }

    for (i = 0; i < p_db->nix; ++i) {
        unsigned long long file_version;

        if (bdb_get_file_version_index(p_db->handle, tran, i, &file_version,
                                       &bdberr) ||
            bdberr != BDBERR_NOERROR) {
            logmsg(LOGMSG_ERROR, "%s: bdb_get_file_version_index failed for db "
                                 "index %d\n",
                   __func__, i);
            return -1;
        }

        if (sc_cmp_fileids(file_version, db_max_file_version) > 0)
            db_max_file_version = file_version;
    }

    /* find the newdb's smallest file version */

    newdb_min_file_version = ULLONG_MAX;

    for (i = 0; i < (1 /*dta*/ + p_newdb->numblobs); ++i) {
        unsigned long long file_version;

        if (bdb_get_file_version_data(p_newdb->handle, tran, i, &file_version,
                                      &bdberr) ||
            bdberr != BDBERR_NOERROR) {
            logmsg(LOGMSG_ERROR,
                   "%s: bdb_get_file_version_data failed for newdb "
                   "data %d\n",
                   __func__, i);
            return -1;
        }

        if (sc_cmp_fileids(file_version, newdb_min_file_version) < 0)
            newdb_min_file_version = file_version;
    }

    for (i = 0; i < p_newdb->nix; ++i) {
        unsigned long long file_version;

        if (bdb_get_file_version_index(p_newdb->handle, tran, i, &file_version,
                                       &bdberr) ||
            bdberr != BDBERR_NOERROR) {
            logmsg(LOGMSG_ERROR,
                   "%s: bdb_get_file_version_index failed for newdb "
                   "index %d\n",
                   __func__, i);
            return -1;
        }

        if (sc_cmp_fileids(file_version, newdb_min_file_version) < 0)
            newdb_min_file_version = file_version;
    }

    /* if the db has any file version >= any of newdb's file versions there has
     * been an error */
    if (sc_cmp_fileids(db_max_file_version, newdb_min_file_version) >= 0) {
        logmsg(LOGMSG_ERROR,
               "%s: db's max file version %#16llx >= newdb's min file "
               "version %#16llx\n",
               __func__, db_max_file_version, newdb_min_file_version);
        return -1;
    }

    return 0;
}

/* close and remove the temp table after a failed schema change. */
int delete_temp_table(struct ireq *iq, struct dbtable *newdb)
{
    struct schema_change_type *s = iq->sc;
    tran_type *tran = NULL;
    int i, rc, bdberr;
    struct dbtable *usedb_sav;

    usedb_sav = iq->usedb;
    iq->usedb = newdb;
    rc = trans_start(iq, NULL, &tran);
    if (rc) {
        sc_errf(s, "%d: trans_start rc %d\n", __LINE__, rc);
        iq->usedb = usedb_sav;
        return -1;
    }

    rc = bdb_close_only_sc(newdb->handle, tran, &bdberr);
    if (rc) {
        sc_errf(s, "bdb_close_only rc %d bdberr %d\n", rc, bdberr);
        return -1;
    }

    for (i = 0; i < 1000; i++) {
        if (!s->retry_bad_genids)
            sc_errf(s, "removing temp table for <%s>\n", newdb->tablename);
        if ((rc = bdb_del(newdb->handle, tran, &bdberr)) ||
            bdberr != BDBERR_NOERROR) {
            rc = -1;
            sc_errf(s, "%s: bdb_del failed with rc: %d bdberr: %d\n", __func__,
                    rc, bdberr);
        } else if ((rc = bdb_del_file_versions(newdb->handle, tran, &bdberr)) ||
                   bdberr != BDBERR_NOERROR) {
            rc = -1;
            sc_errf(s, "%s: bdb_del_file_versions failed with rc: %d bdberr: "
                       "%d\n",
                    __func__, rc, bdberr);
        }

        if (rc != 0) {
            trans_abort(iq, tran);
            poll(NULL, 0, rand() % 100 + 1);
            rc = trans_start(iq, NULL, &tran);
            if (rc) {
                sc_errf(s, "%d: trans_start rc %d\n", __LINE__, rc);
                iq->usedb = usedb_sav;
                return -1;
            }
            rc = bdb_close_only_sc(newdb->handle, tran, &bdberr);
            if (rc) {
                sc_errf(s, "bdb_close_only rc %d bdberr %d\n", rc, bdberr);
                return -1;
            }
        } else
            break;
    }
    if (rc != 0) {
        sc_errf(s, "Still failed to delete temp table for %s.  I am giving up "
                   "and going home.",
                newdb->tablename);
        iq->usedb = usedb_sav;
        return -1;
    }

    rc = trans_commit(iq, tran, gbl_mynode);
    if (rc) {
        sc_errf(s, "%d: trans_commit rc %d\n", __LINE__, rc);
        iq->usedb = usedb_sav;
        return -1;
    }

    iq->usedb = usedb_sav;
    return 0;
}

int do_setcompr(struct ireq *iq, const char *rec, const char *blob)
{
    int rc;
    tran_type *tran = NULL;
    if ((rc = trans_start(iq, NULL, &tran)) != 0) {
        sbuf2printf(iq->sb, ">%s -- trans_start rc:%d\n", __func__, rc);
        return rc;
    }

    struct dbtable *db = iq->usedb;
    bdb_lock_table_write(db->handle, tran);
    int ra, ba;
    if ((rc = get_db_compress(db, &ra)) != 0) goto out;
    if ((rc = get_db_compress_blobs(db, &ba)) != 0) goto out;

    if (rec) ra = bdb_compr2algo(rec);
    if (blob) ba = bdb_compr2algo(blob);
    bdb_set_odh_options(db->handle, db->odh, ra, ba);
    if ((rc = put_db_compress(db, tran, ra)) != 0) goto out;
    if ((rc = put_db_compress_blobs(db, tran, ba)) != 0) goto out;
    if ((rc = trans_commit(iq, tran, gbl_mynode)) == 0) {
        logmsg(LOGMSG_USER, "%s -- TABLE:%s  REC COMP:%s  BLOB COMP:%s\n",
               __func__, db->tablename, bdb_algo2compr(ra), bdb_algo2compr(ba));
    } else {
        sbuf2printf(iq->sb, ">%s -- trans_commit rc:%d\n", __func__, rc);
    }
    tran = NULL;

    int bdberr = 0;
    if ((rc = bdb_llog_scdone(db->handle, setcompr, 1, &bdberr)) != 0) {
        logmsg(LOGMSG_ERROR, "%s -- bdb_llog_scdone rc:%d bdberr:%d\n",
               __func__, rc, bdberr);
    }

out:
    if (tran) {
        trans_abort(iq, tran);
    }
    return rc;
}

int dryrun_int(struct schema_change_type *s, struct dbtable *db, struct dbtable *newdb,
               struct scinfo *scinfo)
{
    int changed;
    struct scplan plan;

    if (s->headers != db->odh)
        s->header_change = s->force_dta_rebuild = s->force_blob_rebuild = 1;

    if (scinfo->olddb_inplace_updates && !s->ip_updates && !s->force_rebuild) {
        sbuf2printf(s->sb,
                    ">Cannot remove inplace updates without rebuilding.\n");
        return -1;
    }

    if (scinfo->olddb_instant_sc && !s->instant_sc) {
        sbuf2printf(
            s->sb,
            ">Cannot remove instant schema-change without rebuilding.\n");
        return -1;
    }

    if (s->force_rebuild) {
        sbuf2printf(s->sb, ">Forcing table rebuild\n");
        goto out;
    }

    if (s->force_dta_rebuild) {
        sbuf2printf(s->sb, ">Forcing data file rebuild\n");
    }

    if (s->force_blob_rebuild) {
        sbuf2printf(s->sb, ">Forcing blob file rebuild\n");
    }

    if (verify_constraints_exist(NULL, newdb, newdb, s)) {
        return -1;
    }

    if (s->compress != scinfo->olddb_compress) {
        s->force_dta_rebuild = 1;
    }

    if (s->compress_blobs != scinfo->olddb_compress_blobs) {
        s->force_blob_rebuild = 1;
    }

    changed = ondisk_schema_changed(s->table, newdb, NULL, s);
    if (changed < 0) {
        if (changed == SC_BAD_NEW_FIELD) {
            sbuf2printf(s->sb,
                        ">Cannot add new field without dbstore or null\n");
            return -1;
        } else if (changed == SC_BAD_INDEX_CHANGE) {
            sbuf2printf(s->sb,
                        ">Cannot change index referenced by other tables\n");
            return -1;
        } else {
            sbuf2printf(s->sb, ">Failed to process schema!\n");
            return -1;
        }
    }

    if (create_schema_change_plan(s, db, newdb, &plan) != 0) {
        sbuf2printf(s->sb, ">Error in plan module.\n");
        sbuf2printf(s->sb, ">Will need to rebuild table\n");
        return 0;
    }

    if (changed == SC_NO_CHANGE) {
        if (db->n_constraints && newdb->n_constraints == 0) {
            sbuf2printf(s->sb, ">All table constraints will be dropped\n");
        } else {
            sbuf2printf(s->sb, ">There is no change in the schema\n");
        }
    } else if (db->version >= MAXVER && newdb->instant_schema_change) {
        sbuf2printf(s->sb, ">Table is at version: %d MAXVER: %d\n", db->version,
                    MAXVER);
        sbuf2printf(s->sb, ">Will need to rebuild table\n");
    }

out:
    print_schemachange_info(s, db, newdb);
    return 0;
}

int backout_schema_changes(struct ireq *iq, tran_type *tran)
{
    struct schema_change_type *s = NULL;

    if (iq->sc_pending && !iq->sc_locked) {
        wrlock_schema_lk();
        iq->sc_locked = 1;
    }
    s = iq->sc = iq->sc_pending;
    while (s != NULL) {
        if (s->addonly) {
            if (s->addonly == SC_DONE_ADD)
                delete_db(s->table);
            if (s->newdb) {
                backout_schemas(s->newdb->tablename);
                cleanup_newdb(s->newdb);
            }
        } else if (s->db) {
            if (s->already_finalized)
                reload_db_tran(s->db, tran);
            else if (s->newdb) {
                backout_constraint_pointers(s->newdb, s->db);
            }
            change_schemas_recover(s->db->tablename);
        }
        sc_del_unused_files_tran(s->db, tran);
        s = iq->sc = s->sc_next;
    }
    if (iq->sc_pending) {
        // createmastertbls only once
        create_sqlmaster_records(NULL);
        create_sqlite_master();
    }
    return 0;
}

int scdone_abort_cleanup(struct ireq *iq)
{
    struct schema_change_type *s = iq->sc;
    mark_schemachange_over(s->table);
    sc_set_running(s->table, 0, iq->sc_seed, gbl_mynode, time(NULL));
    if (s->addonly) {
        delete_temp_table(iq, s->db);
    } else if (s->db) {
        sc_del_unused_files(s->db);
    }
    broadcast_sc_end(s->table, iq->sc_seed);
    return 0;
}
