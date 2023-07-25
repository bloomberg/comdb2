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
#include "sc_view.h"
#include "logmsg.h"
#include "comdb2_atomic.h"
#include "sc_callbacks.h"
#include "views.h"
#include <debug_switches.h>

void comdb2_cheapstack_sym(FILE *f, char *fmt, ...);

extern int gbl_is_physical_replicant;

/**** Utility functions */

static enum thrtype prepare_sc_thread(struct schema_change_type *s)
{
    struct thr_handle *thr_self = thrman_self();
    enum thrtype oldtype = THRTYPE_UNKNOWN;

    if (s->kind != SC_PARTIALUPRECS)
        logmsg(LOGMSG_DEBUG, "Starting a schemachange thread\n");

    if (!s->nothrevent) {
        if (thr_self) {
            thread_started("schema change");
            oldtype = thrman_get_type(thr_self);
            thrman_change_type(thr_self, THRTYPE_SCHEMACHANGE);
        } else
            thr_self = thrman_register(THRTYPE_SCHEMACHANGE);
        if (!s->nothrevent) {
            backend_thread_event(thedb, COMDB2_THR_EVENT_START_RDWR);
            logmsg(LOGMSG_INFO, "Preparing schema change read write thread\n");
        }
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
    logmsg(LOGMSG_INFO, "%s: table '%s' rqid [%llx %s]\n", __func__,
           s->tablename, s->rqid, us);
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
             (bdb_set_in_schema_change(trans, s->tablename, packed_sc_data,
                                       packed_sc_data_len, &bdberr) ||
              bdb_set_schema_change_status(
                  trans, s->tablename, s->iq->sc_seed, 0, packed_sc_data,
                  packed_sc_data_len, BDB_SC_RUNNING, NULL, &bdberr) ||
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
                bdb_transfermaster(thedb->static_table.handle);
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
    int rc = broadcast_sc_start(s->tablename, s->iq->sc_seed, s->iq->sc_host,
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
    if (get_stopsc(__func__, __LINE__)) {
        if (!s->nothrevent)
            backend_thread_event(thedb, COMDB2_THR_EVENT_DONE_RDWR);
        if (s->sb) {
            sbuf2printf(s->sb, "!Master node downgrading - new master will "
                               "resume schemachange\n");
            sbuf2close(s->sb);
            s->sb = NULL;
        }
        logmsg(
            LOGMSG_WARN,
            "Master node downgrading - new master will resume schemachange\n");
        return SC_MASTER_DOWNGRADE;
    }
    return SC_OK;
}

static void free_sc(struct schema_change_type *s)
{
    free_schema_change_type(s);
    /* free any memory csc2 allocated when parsing schema */

    /* Bail out if we're in a time partition rollout otherwise
       we may deadlock with a regular schema change. The time partition
       rollout will invoke csc2_free_all() without holding views_lk. */
    if (s->views_locked)
        return;

    csc2_free_all();
}

static void stop_and_free_sc(struct ireq *iq, int rc,
                             struct schema_change_type *s, int do_free)
{
    if (s->kind != SC_PARTIALUPRECS) {
        if (rc != 0) {
            logmsg(LOGMSG_INFO, "Schema change returning FAILED\n");
            sbuf2printf(s->sb, "FAILED\n");
        } else {
            logmsg(LOGMSG_INFO, "Schema change returning SUCCESS\n");
            sbuf2printf(s->sb, "SUCCESS\n");
        }
    }
    sc_set_running(iq, s, s->tablename, 0, NULL, 0, 0, __func__, __LINE__);
    if (do_free) {
        free_sc(s);
    }
}

static int set_original_tablename(struct schema_change_type *s)
{
    struct dbtable *db = get_dbtable_by_name(s->tablename);
    if (db) {
        strncpy0(s->tablename, db->tablename, sizeof(s->tablename));
        return 0;
    }
    return 1;
}

int do_upgrade_table(struct schema_change_type *s, struct ireq *unused)
{
    int rc;

    set_original_tablename(s);

    if (!s->resume) set_sc_flgs(s);
    if ((rc = mark_sc_in_llmeta(s))) return rc;

    if (rc == SC_OK) rc = do_upgrade_table_int(s);

    if (rc) {
        mark_schemachange_over(s->tablename);
    } else if (s->finalize) {
        rc = finalize_upgrade_table(s);
    } else {
        rc = SC_COMMIT_PENDING;
    }

    return rc;
}

/*
** Start transaction if not passed in (comdb2sc.tsk)
** If started transaction, then
**   1. also commit it
**   2. log scdone here
*/
static inline int replication_only_error_code(int rc)
{
    switch (rc) {
    case -1:
    case BDBERR_NOT_DURABLE:
        return 1;
    }
    return 0;
}

int llog_scdone_rename_wrapper(bdb_state_type *bdb_state,
                               struct schema_change_type *s, tran_type *tran,
                               int *bdberr)
{
    int rc;
    char *mashup = NULL;
    int oldlen = 0;
    int newlen = 0;

    if (s->db) {
        mashup = s->tablename;
        oldlen = strlen(s->tablename) + 1;

        /* embed new name after old name if rename */
        if (s->done_type == rename_table || s->done_type == alias_table) {
            char *dst;
            if (s->done_type == alias_table && s->db->sqlaliasname) {
                dst = s->db->sqlaliasname;
            } else {
                dst = s->db->tablename;
            }
            newlen = strlen(dst) + 1;
            mashup = alloca(oldlen + newlen);
            memcpy(mashup, s->tablename, oldlen); /* old */
            memcpy(mashup + oldlen, dst, newlen); /* new */
        }
    }
    if (!tran)
        rc = bdb_llog_scdone(bdb_state, s->done_type, mashup, oldlen + newlen,
                             1, bdberr);
    else
        rc = bdb_llog_scdone_tran(bdb_state, s->done_type, tran, mashup,
                                  oldlen + newlen, bdberr);

    return rc;
}

static int do_finalize(ddl_t func, struct ireq *iq,
                       struct schema_change_type *s, tran_type *input_tran)
{
    int rc, bdberr = 0;
    tran_type *tran = input_tran;

    if (tran == NULL) {
        rc = trans_start_sc(iq, NULL, &tran);
        if (rc) {
            sc_errf(s, "Failed to start finalize transaction\n");
            return rc;
        }
    }
    uint64_t sc_nrecs = 0;
    if (s->db)
        sc_nrecs = s->db->sc_nrecs; // take a copy, func will clear to 0

    rc = func(iq, s, tran);

    if (rc) {
        if (input_tran == NULL) {
            trans_abort(iq, tran);
            mark_schemachange_over(s->tablename);
            sc_del_unused_files(s->db);
        }
        return rc;
    }

    if ((rc = mark_schemachange_over_tran(s->tablename, tran)))
        return rc;

    if (bdb_set_schema_change_status(tran, s->tablename, iq->sc_seed, sc_nrecs,
                                     NULL, 0, BDB_SC_COMMITTED, NULL,
                                     &bdberr) ||
        bdberr != BDBERR_NOERROR) {
        logmsg(LOGMSG_ERROR,
               "%s: failed to set bdb schema change status, bdberr %d\n",
               __func__, bdberr);
    }

    if (input_tran == NULL) {
        // void all_locks(void*);
        // all_locks(thedb->bdb_env);
        if (debug_switch_fake_sc_replication_timeout()) {
            logmsg(LOGMSG_USER,
                   "Forcing replication error table %s '%s' for tran %p\n",
                   bdb_get_scdone_str(s->done_type), s->tablename, tran);
        }
        if (s->keep_locked) {
            rc = trans_commit(iq, tran, gbl_myhostname);
        } else {
            rc = trans_commit_adaptive(iq, tran, gbl_myhostname);
        }
        if (debug_switch_fake_sc_replication_timeout())
            rc = -1;
        if (rc && !replication_only_error_code(rc)) {
            sc_errf(s, "Failed to commit finalize transaction\n");
            return rc;
        }

        rc = llog_scdone_rename_wrapper(thedb->bdb_env, s, NULL, &bdberr);
        if (rc || bdberr != BDBERR_NOERROR) {
            sc_errf(s, "Failed to send scdone rc=%d bdberr=%d\n", rc, bdberr);
            return -1;
        }
        sc_del_unused_files(s->db);
    } else if (bdb_attr_get(thedb->bdb_attr, BDB_ATTR_SC_DONE_SAME_TRAN)) {
        int bdberr = 0;
        rc = llog_scdone_rename_wrapper(thedb->bdb_env, s, input_tran, &bdberr);
        if (rc || bdberr != BDBERR_NOERROR) {
            sc_errf(s, "Failed to send scdone rc=%d bdberr=%d\n", rc, bdberr);
            return -1;
        }
    }
    return rc;
}

static int check_table_version(struct ireq *iq, struct schema_change_type *sc)
{
    if (sc->kind == SC_ADDTABLE || sc->resume || sc->fix_tp_badvers)
        return 0;
    int rc, bdberr;
    unsigned long long version;
    rc = bdb_table_version_select(sc->tablename, NULL, &version, &bdberr);
    if (rc != 0) {
        errstat_set_strf(&iq->errstat,
                         "failed to get version for table:%s rc:%d",
                         sc->tablename, rc);
        iq->errstat.errval = ERR_SC;
        return SC_INTERNAL_ERROR;
    }
    if (sc->usedbtablevers != version) {
        errstat_set_strf(&iq->errstat,
                         "stale version for table:%s master:%llu replicant:%d",
                         sc->tablename, version, sc->usedbtablevers);
        iq->errstat.errval = ERR_SC;
        return SC_INTERNAL_ERROR;
    }
    return 0;
}

static int do_ddl(ddl_t pre, ddl_t post, struct ireq *iq,
                  struct schema_change_type *s, tran_type *tran)
{
    int rc, bdberr = 0;

    if (s->finalize_only) {
        return s->sc_rc;
    }
    if (s->done_type != user_view) {
        set_original_tablename(s);
        if ((rc = check_table_version(iq, s)) != 0) { // non-tran ??
            goto end;
        }
    }
    if (!s->resume) {
        set_sc_flgs(s);

        rc = trim_sc_history_entries(NULL, s->tablename);
        if (rc)
            logmsg(LOGMSG_ERROR,
                   "Cant cleanup comdb2_sc_history and keep last entries\n");
    }
    if ((rc = mark_sc_in_llmeta_tran(s, NULL))) // non-tran ??
        goto end;

    if (!s->resume && s->done_type == alter &&
        bdb_attr_get(thedb->bdb_attr, BDB_ATTR_SC_DETACHED)) {
        sc_printf(s, "Starting Schema Change with seed 0x%llx\n",
                  flibc_htonll(iq->sc_seed));
        sbuf2printf(s->sb, "SUCCESS\n");
        s->sc_rc = SC_DETACHED;
    }

    broadcast_sc_start(s->tablename, iq->sc_seed, iq->sc_host,
                       time(NULL));                   // dont care rcode
    rc = pre(iq, s, NULL);                            // non-tran ??
    if (s->done_type == alter && master_downgrading(s)) {
        s->sc_rc = SC_MASTER_DOWNGRADE;
        errstat_set_strf(
            &iq->errstat,
            "Master node downgrading - new master will resume schemachange\n");
        return SC_MASTER_DOWNGRADE;
    }
    if (rc == SC_PAUSED) {
        errstat_set_strf(&iq->errstat, "Schema change was paused");
        if (bdb_set_schema_change_status(NULL, s->tablename, iq->sc_seed, 0,
                                         NULL, 0, BDB_SC_PAUSED, NULL,
                                         &bdberr) ||
            bdberr != BDBERR_NOERROR) {
            logmsg(LOGMSG_ERROR,
                   "%s: failed to set bdb schema change status, bdberr %d\n",
                   __func__, bdberr);
        }
        return rc;
    } else if (rc == SC_PREEMPTED) {
        errstat_set_strf(&iq->errstat, "Schema change was preempted");
        return rc;
    } else if (rc) {
        if (rc == SC_ABORTED)
            errstat_set_strf(&iq->errstat, "Schema change was aborted");
        mark_schemachange_over_tran(s->tablename, NULL); // non-tran ??
        broadcast_sc_end(s->tablename, iq->sc_seed);
        if (bdb_set_schema_change_status(
                NULL, s->tablename, iq->sc_seed, 0, NULL, 0, BDB_SC_ABORTED,
                errstat_get_str(&iq->errstat), &bdberr) ||
            bdberr != BDBERR_NOERROR) {
            logmsg(LOGMSG_ERROR,
                   "%s: failed to set bdb schema change status, bdberr %d\n",
                   __func__, bdberr);
        }
    } else if (s->preempted == SC_ACTION_RESUME ||
               s->kind == SC_ALTERTABLE_PENDING) {
        s->finalize = 0;
        rc = SC_COMMIT_PENDING;
    } else if (s->finalize) {
        int local_lock = 0;
        if (!iq->sc_locked) {
            wrlock_schema_lk();
            local_lock = 1;
        }
        rc = do_finalize(post, iq, s, tran);
        if (local_lock)
            unlock_schema_lk();
        if (s->done_type == fastinit && gbl_replicate_local)
            local_replicant_write_clear(iq, tran, s->db);
        broadcast_sc_end(s->tablename, iq->sc_seed);
    } else {
        rc = SC_COMMIT_PENDING;
    }
end:
    s->sc_rc = rc;
    return rc;
}

int do_alter_queues(struct schema_change_type *s, struct ireq *unused)
{
    int rc;

    set_original_tablename(s);

    if (!s->resume) set_sc_flgs(s);

    rc = propose_sc(s);

    if (rc == SC_OK) rc = do_alter_queues_int(s);

    if (master_downgrading(s)) return SC_MASTER_DOWNGRADE;

    broadcast_sc_end(s->tablename, s->iq->sc_seed);

    if (gbl_pushlogs_after_sc)
        push_next_log();

    return rc;
}

int do_alter_stripes(struct schema_change_type *s, struct ireq *unused)
{
    int rc;

    set_original_tablename(s);

    if (!s->resume) set_sc_flgs(s);

    rc = do_alter_stripes_int(s);

    if (master_downgrading(s)) return SC_MASTER_DOWNGRADE;

    broadcast_sc_end(s->tablename, s->iq->sc_seed);

    if (gbl_pushlogs_after_sc)
        push_next_log();

    s->finalize = 1;
    return rc;
}

char *get_ddl_type_str(struct schema_change_type *s)
{
    char *sc_labels[] = {
        "UNKNOWN",    "ALTER QUEUE",   "ALTER STRIPE", "QUEUE_DB",
        "QUEUE_DB",   "VIEW",          "VIEW",         "ADDSP",
        "DELSP",      "DEFAULTSP",     "SHOWSP",       "IS_TRIGGER",
        "IS_TRIGGER", "IS_SFUNC",      "IS_SFUNC",     "IS_AFUNC",
        "IS_AFUNC",   "UPGRADE",       "UPGRADE",      "DROP",
        "TRUNCATE",   "CREATE",        "RENAME",       "ALIAS",
        "ALTER",      "ALTER_PENDING", "REBUILD",      "ALTER_INDEX",
        "DROP_INDEX", "REBUILD_INDEX"};

    return sc_labels[s->kind];
}

char *get_ddl_csc2(struct schema_change_type *s)
{
    return s->newcsc2 ? s->newcsc2 : "";
}
int do_add_view(struct ireq *iq, struct schema_change_type *s, tran_type *tran);
int do_drop_view(struct ireq *iq, struct schema_change_type *s,
                 tran_type *tran);

/* in sync with enum schema_change_kind ! */
struct {
    int run_do_ddl;
    scdone_t type;
    ddl_t pre;
    ddl_t post;
    int (*non_std_pre)(struct schema_change_type *, struct ireq *);
    int (*non_std_post)(struct schema_change_type *);
} do_schema_change_if[] = {
    {0, 0, NULL, NULL, NULL, NULL},
    {0, 0, NULL, NULL, do_alter_queues, NULL},
    {0, 0, NULL, NULL, do_alter_stripes, NULL},
    {1, add_queue_file, do_add_qdb_file, finalize_add_qdb_file, NULL, NULL},
    {1, del_queue_file, do_del_qdb_file, finalize_del_qdb_file, NULL, NULL},
    {1, user_view, do_add_view, finalize_add_view, NULL, NULL},
    {1, user_view, do_drop_view, finalize_drop_view, NULL, NULL},
    {0, 0, NULL, NULL, do_add_sp, finalize_add_sp},
    {0, 0, NULL, NULL, do_del_sp, finalize_del_sp},
    {0, 0, NULL, NULL, do_default_sp, finalize_default_sp},
    {0, 0, NULL, NULL, do_show_sp, NULL},
    {0, 0, NULL, NULL, perform_trigger_update, finalize_trigger},
    {0, 0, NULL, NULL, perform_trigger_update, finalize_trigger},
    {0, 0, NULL, NULL, do_lua_sfunc, finalize_lua_sfunc},
    {0, 0, NULL, NULL, do_lua_sfunc, finalize_lua_sfunc},
    {0, 0, NULL, NULL, do_lua_afunc, finalize_lua_afunc},
    {0, 0, NULL, NULL, do_lua_afunc, finalize_lua_afunc},
    {0, 0, NULL, NULL, do_upgrade_table, finalize_upgrade_table},
    {0, 0, NULL, NULL, do_upgrade_table, finalize_upgrade_table},
    {1, drop, do_drop_table, finalize_drop_table, NULL, NULL},
    {1, fastinit, do_fastinit, finalize_fastinit_table, NULL, NULL},
    {1, add, do_add_table, finalize_add_table, NULL, NULL},
    {1, rename_table, do_rename_table, finalize_rename_table, NULL, NULL},
    {1, alias_table, do_rename_table, finalize_alias_table, NULL, NULL},
    {1, alter, do_alter_table, finalize_alter_table, NULL, NULL},
    {1, alter, do_alter_table, finalize_alter_table, NULL, NULL},
    {1, alter, do_alter_table, finalize_alter_table, NULL, NULL},
    {1, alter, do_alter_table, finalize_alter_table, NULL, NULL},
    {1, alter, do_alter_table, finalize_alter_table, NULL, NULL},
    {1, alter, do_alter_table, finalize_alter_table, NULL, NULL},
};

static int do_schema_change_tran_int(sc_arg_t *arg, int no_reset)
{
    struct ireq *iq = arg->iq;
    tran_type *trans = arg->trans;
    int rc = SC_OK;
    struct schema_change_type *s = arg->sc;
    free(arg);

    if (iq == NULL) {
        abort();
    }

    Pthread_mutex_lock(&s->mtx);
    Pthread_mutex_lock(&s->mtxStart);
    s->started = 1;
    Pthread_cond_signal(&s->condStart);
    Pthread_mutex_unlock(&s->mtxStart);

    enum thrtype oldtype = 0;
    int detached = 0;

    if (!no_reset)
        oldtype = prepare_sc_thread(s);

    if (!bdb_iam_master(thedb->bdb_env) || thedb->master != gbl_myhostname) {
        logmsg(LOGMSG_INFO, "%s downgraded master\n", __func__);
        rc = SC_MASTER_DOWNGRADE;
        goto downgraded;
    }

    s->iq = iq;

    if (s->kind == SC_ALTERTABLE_PENDING || s->preempted == SC_ACTION_RESUME)
        detached = 1;
    if (s->resume && IS_ALTERTABLE(s) && s->preempted == SC_ACTION_PAUSE)
        detached = 1;

    /* initialize the scdone callback type here;
     * we can updated it dynamically if needed
     */
    s->done_type = do_schema_change_if[s->kind].type;

    if (do_schema_change_if[s->kind].run_do_ddl) {
        rc = do_ddl(do_schema_change_if[s->kind].pre,
                    do_schema_change_if[s->kind].post, iq, s, trans);
    } else {
        rc = do_schema_change_if[s->kind].non_std_pre(s, iq);
    }

downgraded:
    if (rc == SC_MASTER_DOWNGRADE) {
        while (s->logical_livesc) {
            poll(NULL, 0, 100);
        }
        if (s && s->newdb && s->newdb->handle) {
            int bdberr;

            if (!trans)
                backend_thread_event(thedb, COMDB2_THR_EVENT_START_RDWR);

            /* return NOMASTER for live schemachange writes */
            sc_set_downgrading(s);
            bdb_close_only(s->newdb->handle, &bdberr);
            freedb(s->newdb);
            s->newdb = NULL;

            if (!trans)
                backend_thread_event(thedb, COMDB2_THR_EVENT_DONE_RDWR);
        }
    }
    if (rc && rc != SC_COMMIT_PENDING && s->preempted != SC_ACTION_RESUME) {
        logmsg(LOGMSG_ERROR, ">>> SCHEMA CHANGE ERROR: TABLE %s, RC %d\n",
               s->tablename, rc);
        iq->sc_should_abort = 1;
    }
    if (detached || rc == SC_PREEMPTED)
        s->sc_rc = SC_DETACHED;
    else
        s->sc_rc = rc;
    if (!s->nothrevent) {
        Pthread_mutex_lock(&sc_async_mtx);
        sc_async_threads--;
        Pthread_cond_broadcast(&sc_async_cond);
        Pthread_mutex_unlock(&sc_async_mtx);
    }
    if (rc == SC_COMMIT_PENDING && (s->preempted == SC_ACTION_RESUME ||
                                    s->kind == SC_ALTERTABLE_PENDING)) {
        int bdberr = 0;
        add_ongoing_alter(s);
        if (bdb_set_schema_change_status(NULL, s->tablename, iq->sc_seed, 0,
                                         NULL, 0, BDB_SC_COMMIT_PENDING, NULL,
                                         &bdberr) ||
            bdberr != BDBERR_NOERROR) {
            logmsg(LOGMSG_ERROR,
                   "%s: failed to set bdb schema change status, bdberr %d\n",
                   __func__, bdberr);
        }
        if (!no_reset)
            reset_sc_thread(oldtype, s);
        Pthread_mutex_unlock(&s->mtx);
        return 0;
    } else if (s->resume == SC_NEW_MASTER_RESUME || rc == SC_COMMIT_PENDING ||
               rc == SC_PREEMPTED || rc == SC_PAUSED ||
               (!s->nothrevent && !s->finalize)) {
        if (!no_reset)
            reset_sc_thread(oldtype, s);
        Pthread_mutex_unlock(&s->mtx);
        return rc;
    }
    if (!no_reset)
        reset_sc_thread(oldtype, s);
    Pthread_mutex_unlock(&s->mtx);
    if (!s->is_osql) {
        if (rc == SC_MASTER_DOWNGRADE) {
            sc_set_running(iq, s, s->tablename, 0, NULL, 0, 0, __func__,
                           __LINE__);
            free_sc(s);
        } else {
            stop_and_free_sc(iq, rc, s, 1);
        }
    }
    return rc;
}

int do_schema_change_tran(sc_arg_t *arg)
{
    return do_schema_change_tran_int(arg, 0);
}

int do_schema_change_tran_thd(sc_arg_t *arg)
{
    comdb2_name_thread(__func__);
    int rc;
    bdb_state_type *bdb_state = thedb->bdb_env;
    thread_started("schema_change");
    bdb_thread_event(bdb_state, 1);
    rc = do_schema_change_tran_int(arg, 1);
    bdb_thread_event(bdb_state, 0);
    return rc;
}

int do_schema_change_locked(struct schema_change_type *s, void *tran)
{
    comdb2_name_thread(__func__);
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
        s->db = get_dbtable_by_name(s->tablename);
    }
    iq->usedb = s->db;
    iq->sc_running = (s->set_running ? 1 : 0);
    s->usedbtablevers = s->db ? s->db->tableversion : 0;
    sc_arg_t *arg = malloc(sizeof(sc_arg_t));
    arg->iq = iq;
    arg->sc = s;
    arg->trans = tran;
    /* the only callers are lightweight timepartition events,
       which already have schema lock */
    arg->iq->sc_locked = 1;
    rc = do_schema_change_tran(arg);
    free(iq);
    return rc;
}

int finalize_schema_change_thd(struct ireq *iq, tran_type *trans)
{
    if (iq == NULL || iq->sc == NULL) abort();
    struct schema_change_type *s = iq->sc;
    Pthread_mutex_lock(&s->mtx);
    enum thrtype oldtype = prepare_sc_thread(s);
    int rc = SC_OK;

    if (gbl_test_scindex_deadlock) {
        logmsg(LOGMSG_INFO, "%s: sleeping for 30s\n", __func__);
        sleep(30);
        logmsg(LOGMSG_INFO, "%s: slept 30s\n", __func__);
    }

    if (do_schema_change_if[s->kind].run_do_ddl) {
        rc = do_finalize(do_schema_change_if[s->kind].post, iq, s, trans);
    } else {
        rc = do_schema_change_if[s->kind].non_std_post(s);
    }

    reset_sc_thread(oldtype, s);
    Pthread_mutex_unlock(&s->mtx);

    if (!s->is_osql)
        stop_and_free_sc(iq, rc, s, 0 /*free_sc*/);
    return rc;
}

void *sc_resuming_watchdog(void *p)
{
    comdb2_name_thread(__func__);
    struct ireq iq;
    struct schema_change_type *stored_sc = NULL;
    int time = bdb_attr_get(thedb->bdb_attr, BDB_ATTR_SC_RESUME_WATCHDOG_TIMER);
    logmsg(LOGMSG_INFO, "%s: started, sleeping %d seconds\n", __func__, time);
    sleep(time);
    logmsg(LOGMSG_INFO, "%s: waking up\n", __func__);
    bdb_thread_event(thedb->bdb_env, BDBTHR_EVENT_START_RDWR);
    init_fake_ireq(thedb, &iq);
    Pthread_mutex_lock(&sc_resuming_mtx);
    stored_sc = sc_resuming;
    while (stored_sc) {
        iq.sc = stored_sc;
        if (iq.sc->db)
            iq.sc->db->sc_abort = 1;
        Pthread_mutex_lock(&(iq.sc->mtx));
        stored_sc = stored_sc->sc_next;
        logmsg(LOGMSG_INFO, "%s: aborting schema change of table '%s'\n",
               __func__, iq.sc->tablename);
        mark_schemachange_over(iq.sc->tablename);
        if (iq.sc->kind == SC_ADDTABLE) {
            delete_temp_table(&iq, iq.sc->db);
            if (iq.sc->add_state == SC_DONE_ADD) {
                rem_dbtable_from_thedb_dbs(iq.sc->db);
            }
        }
        /* TODO: (NC) Also delete view? */
        sc_del_unused_files(iq.sc->db);
        Pthread_mutex_unlock(&(iq.sc->mtx));
        free_schema_change_type(iq.sc);
        iq.sc = NULL;
    }
    sc_resuming = NULL;
    logmsg(LOGMSG_INFO, "%s: existing\n", __func__);
    bdb_thread_event(thedb->bdb_env, BDBTHR_EVENT_DONE_RDWR);
    Pthread_mutex_unlock(&sc_resuming_mtx);
    return NULL;
}

struct timepart_sc_resuming {
    char *viewname;
    struct schema_change_type *s;
    int nshards;
};

void *bplog_commit_timepart_resuming_sc(void *p);
static int process_tpt_sc_hash(void *obj, void *arg)
{
    struct timepart_sc_resuming *tpt_sc = (struct timepart_sc_resuming *)obj;
    pthread_t tid;
    logmsg(LOGMSG_INFO, "%s: processing view '%s'\n", __func__,
           tpt_sc->viewname);
    pthread_create(&tid, &gbl_pthread_attr_detached,
                   bplog_commit_timepart_resuming_sc, tpt_sc->s);
    free(tpt_sc);
    return 0;
}

static int verify_sc_resumed_for_shard(const char *shardname,
                                       timepart_sc_arg_t *arg)
{
    struct schema_change_type *sc = NULL;
    int rc;
    assert(arg->s);
    sc = arg->s;
    while (sc) {
        /* already resumed */
        if (strcasecmp(shardname, sc->tablename) == 0)
            return 0;
        sc = sc->sc_next;
    }

    /* start a new sc for shard that was not already resumed */
    struct schema_change_type *new_sc = clone_schemachange_type(arg->s);
    strncpy0(new_sc->tablename, shardname, sizeof(new_sc->tablename));
    new_sc->iq = NULL;
    new_sc->tran = NULL;
    new_sc->resume = 0;
    new_sc->must_resume = 1; /* this is a shard, we cannot complete partition sc without it */
    new_sc->nothrevent = 0;
    new_sc->finalize = 0;

    /* put new_sc to linked list */
    new_sc->sc_next = arg->s;
    arg->s = new_sc;

    logmsg(LOGMSG_INFO, "Restarting schema change for view '%s' shard '%s'\n",
           arg->view_name, new_sc->tablename);
    rc = start_schema_change(new_sc);
    if (rc != SC_ASYNC && rc != SC_COMMIT_PENDING) {
        logmsg(LOGMSG_ERROR, "%s: failed to restart shard '%s', rc %d\n",
               __func__, shardname, rc);
        /* bplog_commit_timepart_resuming_sc will check rc */
        new_sc->sc_rc = rc;
    }
    return 0;
}

static int verify_sc_resumed_for_all_shards(void *obj, void *arg)
{
    struct timepart_sc_resuming *tpt_sc = (struct timepart_sc_resuming *)obj;

    /* all shards resumed, including the next shard if any */
    if (tpt_sc->nshards > timepart_get_num_shards(tpt_sc->viewname))
        return 0;

    timepart_sc_arg_t sc_arg = {0};
    sc_arg.view_name = tpt_sc->viewname;
    sc_arg.s = tpt_sc->s;
    /* start new sc for shards that were not resumed */
    timepart_foreach_shard(tpt_sc->viewname, verify_sc_resumed_for_shard,
                           &sc_arg, -1);
    assert(sc_arg.s != tpt_sc->s);
    tpt_sc->s = sc_arg.s;
    return 0;
}

int resume_schema_change(void)
{
    int i;
    int rc;
    int scabort = 0;
    char *abort_filename = NULL;

    /* if we're not the master node/phys replicant then we can't do schema
     * change! */
    if (gbl_is_physical_replicant) {
        return 0;
    }
    if (thedb->master != gbl_myhostname) {
        logmsg(LOGMSG_WARN,
               "resume_schema_change: not the master, cannot resume a"
               " schema change\n");
        return -1;
    }

    /* if a schema change is currently running don't try to resume one */
    clear_ongoing_alter();

    hash_t *tpt_sc_hash = hash_init_strcaseptr(offsetof(struct timepart_sc_resuming, viewname));
    if (!tpt_sc_hash) {
        logmsg(LOGMSG_FATAL, "%s: ran out of memory\n", __func__);
        abort();
    }

    /* Give operators a chance to prevent a schema change from resuming. */
    abort_filename = comdb2_location("marker", "%s.scabort", thedb->envname);
    if (access(abort_filename, F_OK) == 0) {
        scabort = 1;
        logmsg(LOGMSG_INFO, "%s: found '%s'\n", __func__, abort_filename);
    }

    Pthread_mutex_lock(&sc_resuming_mtx);
    sc_resuming = NULL;
    for (i = 0; i < thedb->num_dbs; ++i) {
        int bdberr;
        void *packed_sc_data = NULL;
        size_t packed_sc_data_len;

        // unset downgrading flag
        thedb->dbs[i]->sc_downgrading = 0;

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
            if (!thedb->dbs[i]->timepartition_name) {
                logmsg(LOGMSG_WARN,
                       "resume_schema_change: table: %s is in the middle of a "
                       "schema change, resuming...\n",
                       thedb->dbs[i]->tablename);
            } else {
                logmsg(
                    LOGMSG_WARN,
                    "resume_schema_change: shard: %s for partition: %s is in "
                    "the middle of a schema change, resuming...\n",
                    thedb->dbs[i]->tablename,
                    thedb->dbs[i]->timepartition_name);
            }

            s = new_schemachange_type();
            if (!s) {
                logmsg(LOGMSG_FATAL, "%s: ran out of memory\n", __func__);
                abort();
            }

            if (unpack_schema_change_type(s, packed_sc_data,
                                          packed_sc_data_len)) {
                sc_errf(s, "could not unpack the schema change data retrieved "
                           "from the low level meta table\n");
                free(packed_sc_data);
                free_schema_change_type(s);
                continue;
            }

            free(packed_sc_data);

            if (scabort) {
                rc = bdb_set_in_schema_change(NULL, thedb->dbs[i]->tablename,
                                              NULL, 0, &bdberr);
                if (rc)
                    logmsg(LOGMSG_ERROR,
                           "Failed to cancel resuming schema change %d %d\n",
                           rc, bdberr);
                free_schema_change_type(s);
                continue; /* unmark all ongoing schema changes */
            }

            if (IS_UPRECS(s)) {
                logmsg(LOGMSG_DEBUG,
                       "%s: This was a table upgrade. Skipping...\n", __func__);
                free_schema_change_type(s);
                continue;
            }
            if (!IS_SC_DBTYPE_TAGGED_TABLE(s)) { /* see do_schema_change_thd()*/
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

            s->timepartition_name = thedb->dbs[i]->timepartition_name;

            logmsg(LOGMSG_INFO,
                   "%s: resuming schema change: rqid [%llx %s] "
                   "table %s, add %d, drop %d, fastinit %d, alter %d%s%s\n",
                   __func__, s->rqid, us, s->tablename, s->kind == SC_ADDTABLE,
                   s->kind == SC_DROPTABLE, IS_FASTINIT(s), IS_ALTERTABLE(s),
                   s->timepartition_name ? " timepartition " : "",
                   s->timepartition_name ? s->timepartition_name : "");

            if (bdb_attr_get(thedb->bdb_attr, BDB_ATTR_SC_RESUME_AUTOCOMMIT) &&
                s->rqid == 0 && comdb2uuid_is_zero(s->uuid)) {
                s->resume = SC_RESUME;
                if (s->timepartition_name) {
                    logmsg(LOGMSG_INFO,
                           "Resuming schema change for view '%s' shard '%s'\n",
                           s->timepartition_name, s->tablename);
                    s->finalize = 0;
                } else {
                    s->finalize = 1; /* finalize at the end of resume */
                }
            } else {
                s->resume = SC_NEW_MASTER_RESUME;
                s->finalize = 0; /* wait for resubmit of bplog */
            }

            MEMORY_SYNC;

            /* start the schema change back up */
            rc = start_schema_change(s);
            if (rc != SC_OK && rc != SC_ASYNC) {
                logmsg(
                    LOGMSG_ERROR,
                    "%s: failed to resume schema change for table '%s' rc %d\n",
                    __func__, s->tablename, rc);
                /* start_schema_change will free if this fails */
                /*
                free_schema_change_type(s);
                */
                continue;
            } else if (s->timepartition_name) {
                struct timepart_sc_resuming *tpt_sc = NULL;
                tpt_sc = hash_find(tpt_sc_hash, &s->timepartition_name);
                if (tpt_sc == NULL) {
                    /* not found */
                    tpt_sc = calloc(1, sizeof(struct timepart_sc_resuming));
                    if (!tpt_sc) {
                        logmsg(LOGMSG_FATAL, "%s: ran out of memory\n",
                               __func__);
                        abort();
                    }
                    tpt_sc->viewname = (char *)s->timepartition_name;
                    tpt_sc->s = s;
                    tpt_sc->nshards = 1;
                    hash_add(tpt_sc_hash, tpt_sc);
                } else {
                    /* linked list of all shards for the same view */
                    s->sc_next = tpt_sc->s;
                    tpt_sc->s = s;
                    tpt_sc->nshards++;
                }
            } else if (s->finalize == 0 && s->kind != SC_ALTERTABLE_PENDING) {
                s->sc_next = sc_resuming;
                sc_resuming = s;
            }
        }
    }

    if (sc_resuming) {
        pthread_t tid;
        rc = pthread_create(&tid, &gbl_pthread_attr_detached,
                            sc_resuming_watchdog, NULL);
        if (rc)
            logmsg(LOGMSG_ERROR, "%s: failed to start sc_resuming_watchdog\n",
                   __FILE__);
    }
    Pthread_mutex_unlock(&sc_resuming_mtx);

    hash_for(tpt_sc_hash, verify_sc_resumed_for_all_shards, NULL);
    hash_for(tpt_sc_hash, process_tpt_sc_hash, NULL);
    hash_free(tpt_sc_hash);

    if (scabort) {
        logmsg(LOGMSG_WARN, "Cancelling schema change\n");
        rc = unlink(abort_filename);
        if (rc)
            logmsg(LOGMSG_ERROR,
                   "Can't delete abort marker file %s - future sc may abort\n",
                   abort_filename);
    }

    free(abort_filename);

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
            db->ix_datacopy, db->ix_datacopylen, db->ix_collattr, db->ix_nullsallowed,
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
            db->ix_datacopy, db->ix_datacopylen, db->ix_collattr, db->ix_nullsallowed,
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
    iq->usedb = NULL;
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
            sc_printf(s, "removing temp table for <%s>\n", newdb->tablename);
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

    rc = trans_commit(iq, tran, gbl_myhostname);
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
    if ((rc = trans_commit(iq, tran, gbl_myhostname)) == 0) {
        logmsg(LOGMSG_USER, "%s -- TABLE:%s  REC COMP:%s  BLOB COMP:%s\n",
               __func__, db->tablename, bdb_algo2compr(ra), bdb_algo2compr(ba));
    } else {
        sbuf2printf(iq->sb, ">%s -- trans_commit rc:%d\n", __func__, rc);
    }
    tran = NULL;

    int bdberr = 0;
    if ((rc = bdb_llog_scdone(thedb->bdb_env, setcompr, db->tablename,
                              strlen(db->tablename) + 1, 1, &bdberr)) != 0) {
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

    changed = ondisk_schema_changed(s->tablename, newdb, NULL, s);
    if (changed < 0) {
        if (changed == SC_BAD_NEW_FIELD) {
            sc_client_error(s, ">Cannot add new field without dbstore or null");
            return -1;
        } else if (changed == SC_BAD_INDEX_CHANGE) {
            sc_client_error(s, ">Cannot change index referenced by other tables\n");
            return -1;
        } else if (changed == SC_BAD_DBPAD) {
            sc_client_error(s, ">Cannot change size of byte array without dbpad\n");
            return -1;
        } else if (changed == SC_BAD_DBSTORE_FUNC_NOT_NULL) {
            sc_client_error(s, ">Column must be nullable to use a function as its default value");
            return -1;
        } else {
            sc_client_error(s, ">Failed to process schema!\n");
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
    } else if (db->schema_version >= MAXVER && newdb->instant_schema_change) {
        sbuf2printf(s->sb, ">Table is at version: %d MAXVER: %d\n",
                    db->schema_version, MAXVER);
        sbuf2printf(s->sb, ">Will need to rebuild table\n");
    }

out:
    print_schemachange_info(s, db, newdb);
    return 0;
}

int backout_schema_changes(struct ireq *iq, tran_type *tran)
{
    struct schema_change_type *s = NULL;
    comdb2_cheapstack_sym(stderr, "%s iq %p", __func__, iq);

    if (iq->sc_pending && !iq->sc_locked) {
        wrlock_schema_lk();
        iq->sc_locked = 1;
    }
    iq->sc_should_abort = 1;
    s = iq->sc = iq->sc_pending;
    while (s != NULL) {
        while (s->logical_livesc) {
            poll(NULL, 0, 100);
        }
        if (s->kind == SC_ADDTABLE) {
            if (s->add_state == SC_DONE_ADD) {
                rem_dbtable_from_thedb_dbs(s->db);
            }
            if (s->newdb) {
                backout_schemas(s->newdb->tablename);
            }
        } else if (s->db) {
            if (s->already_finalized)
                reload_db_tran(s->db, tran);
            else if (s->newdb) {
                backout_constraint_pointers(s->newdb, s->db);
            }
            change_schemas_recover(s->db->tablename);
        }
        /* TODO: (NC) Also delete view? */
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
    int bdberr = 0;
    struct schema_change_type *s = iq->sc;
    mark_schemachange_over(s->tablename);
    if (s->set_running)
        sc_set_running(iq, s, s->tablename, 0, gbl_myhostname, time(NULL), 0,
                       __func__, __LINE__);
    if (s->db && s->db->handle) {
        if (s->kind == SC_ADDTABLE) {
            delete_temp_table(iq, s->db);
            cleanup_newdb(s->db);
        } else {
            sc_del_unused_files(s->db);
        }
    }
    broadcast_sc_end(s->tablename, iq->sc_seed);
    if (bdb_set_schema_change_status(NULL, s->tablename, iq->sc_seed, 0, NULL,
                                     0, BDB_SC_ABORTED,
                                     errstat_get_str(&iq->errstat), &bdberr) ||
        bdberr != BDBERR_NOERROR) {
        logmsg(LOGMSG_ERROR,
               "%s: failed to set bdb schema change status, bdberr %d\n",
               __func__, bdberr);
    }
    return 0;
}
