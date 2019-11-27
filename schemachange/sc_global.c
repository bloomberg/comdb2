/*
   Copyright 2015, 2017, Bloomberg Finance L.P.

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
#include "schemachange.h"
#include "sc_global.h"
#include "logmsg.h"
#include "sc_callbacks.h"
#include "bbinc/cheapstack.h"
#include "crc32c.h"
#include "comdb2_atomic.h"

#include <plhash.h>

pthread_mutex_t schema_change_in_progress_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t fastinit_in_progress_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t schema_change_sbuf2_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t csc2_subsystem_mtx = PTHREAD_MUTEX_INITIALIZER;
volatile int gbl_schema_change_in_progress = 0;
volatile int gbl_lua_version = 0;
int gbl_default_livesc = 1;
int gbl_default_plannedsc = 1;
int gbl_default_sc_scanmode = SCAN_PARALLEL;
hash_t *sc_tables = NULL;

pthread_mutex_t ongoing_alter_mtx = PTHREAD_MUTEX_INITIALIZER;
hash_t *ongoing_alters = NULL;

pthread_mutex_t sc_resuming_mtx = PTHREAD_MUTEX_INITIALIZER;
struct schema_change_type *sc_resuming = NULL;

/* async ddl sc */
pthread_mutex_t sc_async_mtx = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t sc_async_cond = PTHREAD_COND_INITIALIZER;
volatile int sc_async_threads = 0;

/* Throttle settings, which you can change with message traps.  Note that if
 * you have gbl_sc_usleep=0, the important live writer threads never get to
 * run. */
int gbl_sc_usleep = 1000;
int gbl_sc_wrusleep = 0;

/* When the last write came in.  Live schema change thread needn't sleep
 * if the database is idle. */
int gbl_sc_last_writer_time = 0;

/* updates/deletes done behind the cursor since schema change started */
pthread_mutex_t gbl_sc_lock = PTHREAD_MUTEX_INITIALIZER;
int gbl_sc_report_freq = 15; /* seconds between reports */
int gbl_sc_abort = 0;
uint32_t gbl_sc_resume_start = 0;
/* see sc_del_unused_files() and sc_del_unused_files_check_progress() */
int sc_del_unused_files_start_ms = 0;
int gbl_sc_del_unused_files_threshold_ms = 30000;

int gbl_sc_commit_count = 0; /* number of schema change commits - these can
                                render a backup unusable */

int gbl_pushlogs_after_sc = 1;

int rep_sync_save;
int log_sync_save;
int log_sync_time_save;

int stopsc = 0; /* stop schemachange, so it can resume */

inline int is_dta_being_rebuilt(struct scplan *plan)
{
    if (!plan) return 1;
    if (!gbl_use_plan) return 1;
    if (plan->dta_plan) return 1;

    return 0;
}

int gbl_verbose_set_sc_in_progress = 0;

void set_schema_change_in_progress(const char *func, int line, int val)
{
    if (gbl_verbose_set_sc_in_progress) {
        logmsg(LOGMSG_USER, "%s line %d set schema_change_in_progress to %d\n",
               func, line, val);
    }
    gbl_schema_change_in_progress = val;
}

const char *get_sc_to_name(const char *name)
{
    static char pref[256] = {0};
    static int preflen = 0;

    if (!pref[0]) {
        int bdberr;

        bdb_get_new_prefix(pref, sizeof(pref), &bdberr);
        preflen = strlen(pref);
    }

    if (gbl_schema_change_in_progress) {
        if (strncasecmp(name, pref, preflen) == 0) return name + preflen;
    }
    return NULL;
}

void wait_for_sc_to_stop(const char *operation)
{
    stopsc = 1;
    logmsg(LOGMSG_INFO, "%s: set stopsc for %s\n", __func__, operation);
    if (gbl_schema_change_in_progress) {
        logmsg(LOGMSG_INFO, "giving schemachange time to stop\n");
        int waited = 0;
        while (gbl_schema_change_in_progress) {
            sleep(1);
            waited++;
            if (waited > 10)
                logmsg(LOGMSG_ERROR,
                       "%s: waiting schema changes to stop for: %ds\n",
                       operation, waited);
            if (waited > 60) {
                logmsg(LOGMSG_FATAL,
                       "schema changes take too long to stop, waited %ds\n",
                       waited);
                abort();
            }
        }
        logmsg(LOGMSG_INFO, "proceeding with %s (waited for: %ds)\n", operation,
               waited);
    }
    extern int gbl_test_sc_resume_race;
    if (gbl_test_sc_resume_race) {
        logmsg(LOGMSG_INFO, "%s: sleeping 5s to test\n", __func__);
        sleep(5);
    }
}

void allow_sc_to_run(void)
{
    stopsc = 0;
    logmsg(LOGMSG_INFO, "%s: allow sc to run\n", __func__);
}

typedef struct {
    char *tablename;
    uint64_t seed;
    uint32_t host; /* crc32 of machine name */
    time_t time;
    uint32_t logical_lwm;
    char mem[1];
} sc_table_t;

/* Atomically set the schema change running status, and mark it in glm for
 * the schema change in progress dbdwn alarm.
 *
 * The seed was a silly idea to stop imaginary race conditions on the
 * replicants.  I'm not sure what race conditions I was thinking of at the
 * time, but this just seemed to cause problems with replicants getting
 * stuck in schema change mode.  Therefore we will always allow this operation
 * to succeed unless this is the master node, in which case the purpose of the
 * flag is as before to prevent 2 schema changes from running at once.
 *
 * If we are using the low level meta table then this isn't called on the
 * replicants at all when doing a schema change, its still called for queue or
 * dtastripe changes. */
int sc_set_running(char *table, int running, uint64_t seed, const char *host,
                   time_t time)
{
    sc_table_t *sctbl = NULL;
#ifdef DEBUG_SC
    printf("%s: %d\n", __func__, running);
    comdb2_linux_cheap_stack_trace();
#endif
    Pthread_mutex_lock(&schema_change_in_progress_mutex);
    if (sc_tables == NULL) {
        sc_tables =
            hash_init_user((hashfunc_t *)strhashfunc, (cmpfunc_t *)strcmpfunc,
                           offsetof(sc_table_t, tablename), 0);
    }
    assert(sc_tables);

    if (thedb->master == gbl_mynode) {
        if (running && table &&
            (sctbl = hash_find_readonly(sc_tables, &table)) != NULL &&
            sctbl->seed != seed) {
            if (running > 1) /* preempted mode */
                sctbl->seed = seed;
            else {
                Pthread_mutex_unlock(&schema_change_in_progress_mutex);
                logmsg(LOGMSG_INFO,
                       "schema change for table %s already in progress\n",
                       table);
                return -1;
            }
        } else if (!running && table &&
                   (sctbl = hash_find_readonly(sc_tables, &table)) != NULL &&
                   seed && sctbl->seed != seed) {
            Pthread_mutex_unlock(&schema_change_in_progress_mutex);
            logmsg(LOGMSG_ERROR,
                   "cannot stop schema change for table %s: wrong seed given\n",
                   table);
            return -1;
        }
    }
    if (running) {
        /* this is an osql replay of a resuming schema change */
        if (sctbl) {
            Pthread_mutex_unlock(&schema_change_in_progress_mutex);
            return 0;
        }
        if (table) {
            sctbl = calloc(1, offsetof(sc_table_t, mem) + strlen(table) + 1);
            assert(sctbl);
            strcpy(sctbl->mem, table);
            sctbl->tablename = sctbl->mem;

            sctbl->seed = seed;
            sctbl->host = host ? crc32c((uint8_t *)host, strlen(host)) : 0;
            sctbl->time = time;
            hash_add(sc_tables, sctbl);
        }
        set_schema_change_in_progress(__func__, __LINE__,
                                      gbl_schema_change_in_progress + 1);
    } else { /* not running */
        if (table && (sctbl = hash_find_readonly(sc_tables, &table)) != NULL) {
            hash_del(sc_tables, sctbl);
            free(sctbl);
            if (gbl_schema_change_in_progress > 0)
                set_schema_change_in_progress(
                    __func__, __LINE__, gbl_schema_change_in_progress - 1);
        } else if (!table && gbl_schema_change_in_progress) {
            if (gbl_schema_change_in_progress > 0)
                set_schema_change_in_progress(
                    __func__, __LINE__, gbl_schema_change_in_progress - 1);
        }

        if (gbl_schema_change_in_progress <= 0 || (!table && !seed)) {
            gbl_sc_resume_start = 0;
            set_schema_change_in_progress(__func__, __LINE__, 0);
            sc_async_threads = 0;
            hash_clear(sc_tables);
            hash_free(sc_tables);
            sc_tables = NULL;
        }
    }
    ctrace("sc_set_running(table=%s running=%d seed=0x%llx): "
           "gbl_schema_change_in_progress %d\n",
           table ? table : "", running, (unsigned long long)seed,
           gbl_schema_change_in_progress);
    Pthread_mutex_unlock(&schema_change_in_progress_mutex);
    return 0;
}

void sc_status(struct dbenv *dbenv)
{
    unsigned int bkt;
    void *ent;
    sc_table_t *sctbl = NULL;
    Pthread_mutex_lock(&schema_change_in_progress_mutex);
    if (sc_tables)
        sctbl = hash_first(sc_tables, &ent, &bkt);
    while (gbl_schema_change_in_progress && sctbl) {
        const char *mach = get_hostname_with_crc32(thedb->bdb_env, sctbl->host);
        time_t timet = sctbl->time;
        struct tm tm;
        localtime_r(&timet, &tm);

        logmsg(LOGMSG_USER, "-------------------------\n");
        logmsg(LOGMSG_USER,
               "Schema change in progress for table %s "
               "with seed 0x%" PRIx64 "\n",
               sctbl->tablename, sctbl->seed);
        logmsg(LOGMSG_USER,
               "(Started on node %s at %04d-%02d-%02d %02d:%02d:%02d)\n",
               mach ? mach : "(unknown)", tm.tm_year + 1900, tm.tm_mon + 1,
               tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec);
        struct dbtable *db = get_dbtable_by_name(sctbl->tablename);

        if (db && db->doing_conversion)
            logmsg(LOGMSG_USER,
                   "Conversion phase running %" PRId64 " converted\n",
                   db->sc_nrecs);
        else if (db && db->doing_upgrade)
            logmsg(LOGMSG_USER, "Upgrade phase running %" PRId64 " upgraded\n",
                   db->sc_nrecs);

        logmsg(LOGMSG_USER, "-------------------------\n");
        sctbl = hash_next(sc_tables, &ent, &bkt);
    }
    if (!gbl_schema_change_in_progress) {
        logmsg(LOGMSG_USER, "schema change running   NO\n");
    }
    Pthread_mutex_unlock(&schema_change_in_progress_mutex);
}

void reset_sc_stat()
{
    gbl_sc_abort = 0;
}
/* Turn off live schema change.  This HAS to be done while holding the exclusive
 * lock, and it has to be done BEFORE we try to recover from a failed schema
 * change (removing temp tables etc). */
void live_sc_off(struct dbtable *db)
{
#ifdef DEBUG
    logmsg(LOGMSG_INFO, "live_sc_off()\n");
#endif
    Pthread_rwlock_wrlock(&db->sc_live_lk);
    db->sc_to = NULL;
    db->sc_from = NULL;
    db->sc_abort = 0;
    db->sc_downgrading = 0;
    db->sc_adds = 0;
    db->sc_updates = 0;
    db->sc_deletes = 0;
    db->sc_nrecs = 0;
    db->sc_prev_nrecs = 0;
    db->doing_conversion = 0;
    Pthread_rwlock_unlock(&db->sc_live_lk);
}

void sc_set_downgrading(struct schema_change_type *s)
{
    struct ireq iq = {0};
    tran_type *tran = NULL;
    init_fake_ireq(thedb, &iq);
    iq.usedb = s->db;
    trans_start(&iq, NULL, &tran);
    if (tran == NULL) {
        logmsg(LOGMSG_FATAL, "%s: failed to start tran\n", __func__);
        abort();
    }

    /* make sure no one writes to the table */
    bdb_lock_table_write(s->db->handle, tran);

    Pthread_rwlock_wrlock(&s->db->sc_live_lk);
    /* live_sc_post* code will look at this and return errors properly */
    s->db->sc_downgrading = 1;
    s->db->sc_to = NULL;
    s->db->sc_from = NULL;
    s->db->sc_abort = 0;
    s->db->doing_conversion = 0;
    Pthread_rwlock_unlock(&s->db->sc_live_lk);

    if (s->db->sc_live_logical) {
        int rc =
            bdb_clear_logical_live_sc(s->db->handle, 0 /* already locked */);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: failed to clear logical live sc\n",
                   __func__);
        }
    }

    trans_abort(&iq, tran);
}

int reload_lua()
{
    ++gbl_lua_version;
    logmsg(LOGMSG_DEBUG, "Replicant invalidating Lua machines\n");
    return 0;
}

int replicant_reload_analyze_stats()
{
    ATOMIC_ADD32(gbl_analyze_gen, 1);
    logmsg(LOGMSG_DEBUG, "Replicant invalidating SQLite stats\n");
    return 0;
}

int is_table_in_schema_change(const char *tbname, tran_type *tran)
{
    int bdberr;
    void *packed_sc_data = NULL;
    size_t packed_sc_data_len;
    int rc = 0;
    rc = bdb_get_in_schema_change(tran, tbname, &packed_sc_data,
                                  &packed_sc_data_len, &bdberr);
    if (rc || bdberr != BDBERR_NOERROR) {
        logmsg(LOGMSG_ERROR, "%s: failed to read llmeta\n", __func__);
        return -1;
    }
    if (packed_sc_data) {
        struct schema_change_type *s = new_schemachange_type();
        if (s == NULL) {
            logmsg(LOGMSG_ERROR, "%s: out of memory\n", __func__);
            return -1;
        }
        rc = unpack_schema_change_type(s, packed_sc_data, packed_sc_data_len);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: failed to unpack schema change\n",
                   __func__);
            return -1;
        }
        rc = (strcasecmp(tbname, s->tablename) == 0);
        free(packed_sc_data);
        free_schema_change_type(s);
        return rc;
    }
    return 0;
}

void sc_set_logical_redo_lwm(char *table, unsigned int file)
{
    sc_table_t *sctbl = NULL;
    Pthread_mutex_lock(&schema_change_in_progress_mutex);
    assert(sc_tables);
    sctbl = hash_find_readonly(sc_tables, &table);
    assert(sctbl);
    sctbl->logical_lwm = file;
    Pthread_mutex_unlock(&schema_change_in_progress_mutex);
}

unsigned int sc_get_logical_redo_lwm()
{
    unsigned int bkt;
    void *ent;
    sc_table_t *sctbl = NULL;
    unsigned int lwm = 0;
    if (!gbl_logical_live_sc)
        return 0;
    Pthread_mutex_lock(&schema_change_in_progress_mutex);
    if (sc_tables)
        sctbl = hash_first(sc_tables, &ent, &bkt);
    while (gbl_schema_change_in_progress && sctbl) {
        if (lwm == 0 || sctbl->logical_lwm < lwm)
            lwm = sctbl->logical_lwm;
        sctbl = hash_next(sc_tables, &ent, &bkt);
    }
    Pthread_mutex_unlock(&schema_change_in_progress_mutex);
    return lwm - 1;
}

unsigned int sc_get_logical_redo_lwm_table(char *table)
{
    sc_table_t *sctbl = NULL;
    unsigned int lwm = 0;
    if (!gbl_logical_live_sc)
        return 0;
    Pthread_mutex_lock(&schema_change_in_progress_mutex);
    assert(sc_tables);
    sctbl = hash_find_readonly(sc_tables, &table);
    if (sctbl)
        lwm = sctbl->logical_lwm;
    Pthread_mutex_unlock(&schema_change_in_progress_mutex);
    return lwm;
}

void add_ongoing_alter(struct schema_change_type *sc)
{
    assert(sc->alteronly);
    Pthread_mutex_lock(&ongoing_alter_mtx);
    if (ongoing_alters == NULL) {
        ongoing_alters =
            hash_init_strcase(offsetof(struct schema_change_type, tablename));
    }
    hash_add(ongoing_alters, sc);
    Pthread_mutex_unlock(&ongoing_alter_mtx);
}

void remove_ongoing_alter(struct schema_change_type *sc)
{
    assert(sc->alteronly);
    Pthread_mutex_lock(&ongoing_alter_mtx);
    if (ongoing_alters != NULL) {
        hash_del(ongoing_alters, sc);
    }
    Pthread_mutex_unlock(&ongoing_alter_mtx);
}

struct schema_change_type *find_ongoing_alter(char *table)
{
    struct schema_change_type *s = NULL;
    Pthread_mutex_lock(&ongoing_alter_mtx);
    if (ongoing_alters != NULL) {
        s = hash_find_readonly(ongoing_alters, table);
    }
    Pthread_mutex_unlock(&ongoing_alter_mtx);
    return s;
}

struct schema_change_type *preempt_ongoing_alter(char *table, int action)
{
    struct schema_change_type *s = NULL;
    Pthread_mutex_lock(&ongoing_alter_mtx);
    if (ongoing_alters != NULL) {
        s = hash_find_readonly(ongoing_alters, table);
        if (s) {
            int ok = 0;
            switch (action) {
            default:
                break;
            case SC_ACTION_PAUSE:
                if (s->preempted != SC_ACTION_PAUSE)
                    ok = 1;
                if (s->alteronly == SC_ALTER_PENDING)
                    s->alteronly = SC_ALTER_ONLY;
                break;
            case SC_ACTION_RESUME:
                if (s->preempted == SC_ACTION_PAUSE)
                    ok = 1;
                break;
            case SC_ACTION_COMMIT:
                if (s->preempted == SC_ACTION_PAUSE ||
                    s->preempted == SC_ACTION_RESUME)
                    ok = 1;
                else if (s->alteronly == SC_ALTER_PENDING) {
                    s->alteronly = SC_ALTER_ONLY;
                    ok = 1;
                }
                break;
            case SC_ACTION_ABORT:
                ok = 1;
                if (s->alteronly == SC_ALTER_PENDING)
                    s->alteronly = SC_ALTER_ONLY;
                break;
            }
            if (ok) {
                s->preempted = action;
                hash_del(ongoing_alters, s);
            } else {
                s = NULL;
            }
        }
    }
    Pthread_mutex_unlock(&ongoing_alter_mtx);
    return s;
}

void clear_ongoing_alter()
{
    Pthread_mutex_lock(&ongoing_alter_mtx);
    if (ongoing_alters != NULL) {
        hash_clear(ongoing_alters);
    }
    Pthread_mutex_unlock(&ongoing_alter_mtx);
}
