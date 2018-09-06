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

#include "limit_fortify.h"
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

pthread_rwlock_t schema_lk = PTHREAD_RWLOCK_INITIALIZER;
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
int gbl_sc_resume_start = 0;
/* see sc_del_unused_files() and sc_del_unused_files_check_progress() */
int sc_del_unused_files_start_ms = 0;
int gbl_sc_del_unused_files_threshold_ms = 30000;

int gbl_sc_commit_count = 0; /* number of schema change commits - these can
                                render a backup unusable */

int gbl_pushlogs_after_sc = 1;

int rep_sync_save;
int log_sync_save;
int log_sync_time_save;

int gbl_sc_thd_failed = 0;

/* All writer threads have to grab the lock in read/write mode.  If a live
 * schema change is in progress then they have to do extra stuff. */
pthread_rwlock_t sc_live_rwlock = PTHREAD_RWLOCK_INITIALIZER;

int schema_change = SC_NO_CHANGE; /*static int schema_change_doomed = 0;*/

int stopsc = 0; /* stop schemachange, so it can resume */

inline int is_dta_being_rebuilt(struct scplan *plan)
{
    if (!plan) return 1;
    if (!gbl_use_plan) return 1;
    if (plan->dta_plan) return 1;

    return 0;
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

void wait_for_sc_to_stop(void)
{
    stopsc = 1;
    logmsg(LOGMSG_INFO, "%s: set stopsc\n", __func__);
    if (gbl_schema_change_in_progress) {
        logmsg(LOGMSG_INFO, "giving schemachange time to stop\n");
        int waited = 0;
        while (gbl_schema_change_in_progress) {
            sleep(1);
            waited++;
            if (waited > 10)
                logmsg(LOGMSG_ERROR,
                       "downgrade waiting schema changes to stop for: %ds\n",
                       waited);
            if (waited > 60) {
                logmsg(LOGMSG_FATAL,
                       "schema changes take too long to stop, waited %ds\n",
                       waited);
                abort();
            }
        }
        logmsg(LOGMSG_INFO, "proceeding with downgrade (waited for: %ds)\n",
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
    pthread_mutex_lock(&schema_change_in_progress_mutex);
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
            pthread_mutex_unlock(&schema_change_in_progress_mutex);
            logmsg(LOGMSG_INFO,
                   "schema change for table %s already in progress\n", table);
            return -1;
        } else if (!running && table &&
                   (sctbl = hash_find_readonly(sc_tables, &table)) != NULL &&
                   seed && sctbl->seed != seed) {
            pthread_mutex_unlock(&schema_change_in_progress_mutex);
            logmsg(LOGMSG_ERROR,
                   "cannot stop schema change for table %s: wrong seed given\n",
                   table);
            return -1;
        }
    }
    if (running) {
        /* this is an osql replay of a resuming schema change */
        if (sctbl) {
            pthread_mutex_unlock(&schema_change_in_progress_mutex);
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
        gbl_schema_change_in_progress++;
    } else { /* not running */
        if (table && (sctbl = hash_find_readonly(sc_tables, &table)) != NULL) {
            hash_del(sc_tables, sctbl);
            free(sctbl);
            gbl_schema_change_in_progress--;
        } else if (!table && gbl_schema_change_in_progress)
            gbl_schema_change_in_progress--;

        if (gbl_schema_change_in_progress == 0 || (!table && !seed)) {
            gbl_sc_resume_start = 0;
            gbl_schema_change_in_progress = 0;
            sc_async_threads = 0;
            hash_clear(sc_tables);
            hash_free(sc_tables);
            sc_tables = NULL;
        }
    }
    ctrace("sc_set_running(running=%d seed=0x%llx): "
           "gbl_schema_change_in_progress %d\n",
           running, (unsigned long long)seed, gbl_schema_change_in_progress);
    logmsg(LOGMSG_INFO,
           "sc_set_running(table=%s running=%d seed=0x%llx): "
           "gbl_schema_change_in_progress %d\n",
           table, running, (unsigned long long)seed,
           gbl_schema_change_in_progress);
    pthread_mutex_unlock(&schema_change_in_progress_mutex);
    return 0;
}

void sc_status(struct dbenv *dbenv)
{
    unsigned int bkt;
    void *ent;
    sc_table_t *sctbl = NULL;
    pthread_mutex_lock(&schema_change_in_progress_mutex);
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
               "with seed 0x%lx\n",
               sctbl->tablename, sctbl->seed);
        logmsg(LOGMSG_USER,
               "(Started on node %s at %04d-%02d-%02d %02d:%02d:%02d)\n",
               mach ? mach : "(unknown)", tm.tm_year + 1900, tm.tm_mon + 1,
               tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec);
        struct dbtable *db = get_dbtable_by_name(sctbl->tablename);

        if (db && db->doing_conversion)
            logmsg(LOGMSG_USER, "Conversion phase running %lld converted\n",
                   db->sc_nrecs);
        else if (db && db->doing_upgrade)
            logmsg(LOGMSG_USER, "Upgrade phase running %lld upgraded\n",
                   db->sc_nrecs);

        logmsg(LOGMSG_USER, "-------------------------\n");
        sctbl = hash_next(sc_tables, &ent, &bkt);
    }
    if (!gbl_schema_change_in_progress) {
        logmsg(LOGMSG_USER, "schema change running   NO\n");
    }
    pthread_mutex_unlock(&schema_change_in_progress_mutex);
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
    pthread_rwlock_wrlock(&sc_live_rwlock);
    db->sc_to = NULL;
    db->sc_from = NULL;
    db->sc_abort = 0;
    db->sc_downgrading = 0;
    db->sc_adds = 0;
    db->sc_updates = 0;
    db->sc_deletes = 0;
    db->sc_nrecs = 0;
    db->sc_prev_nrecs = 0;
    pthread_rwlock_unlock(&sc_live_rwlock);
}

int reload_lua()
{
    ++gbl_lua_version;
    logmsg(LOGMSG_DEBUG, "Replicant invalidating Lua machines\n");
    return 0;
}

int replicant_reload_analyze_stats()
{
    ATOMIC_ADD(gbl_analyze_gen, 1);
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
        rc = (strcasecmp(tbname, s->table) == 0);
        free(packed_sc_data);
        free_schema_change_type(s);
        return rc;
    }
    return 0;
}
