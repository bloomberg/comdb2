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
#include <assert.h>

#include <plhash.h>

pthread_mutex_t schema_change_in_progress_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t fastinit_in_progress_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t schema_change_sbuf2_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t csc2_subsystem_mtx = PTHREAD_MUTEX_INITIALIZER;
static int gbl_schema_change_in_progress = 0;
volatile int gbl_lua_version = 0;
int gbl_default_livesc = 1;
int gbl_default_plannedsc = 1;
int gbl_default_sc_scanmode = SCAN_PARALLEL;
static hash_t *sc_tables = NULL;

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

static int stopsc = 0; /* stop schemachange, so it can resume */

inline int is_dta_being_rebuilt(struct scplan *plan)
{
    if (!plan) return 1;
    if (!gbl_use_plan) return 1;
    if (plan->dta_plan) return 1;

    return 0;
}

int gbl_verbose_set_sc_in_progress = 0;
static pthread_mutex_t gbl_sc_progress_lk = PTHREAD_MUTEX_INITIALIZER;

int get_stopsc(const char *func, int line)
{
    int ret;
    Pthread_mutex_lock(&gbl_sc_progress_lk);
    ret = stopsc;
    Pthread_mutex_unlock(&gbl_sc_progress_lk);
    if (gbl_verbose_set_sc_in_progress) {
        logmsg(LOGMSG_USER, "%s line %d %s returning %d\n", func, line,
               __func__, ret);
    }
    return ret;
}

void comdb2_cheapstack_sym(FILE *f, char *fmt, ...);
static int increment_schema_change_in_progress(const char *func, int line)
{
    int val = 0, didit = 0;
    Pthread_mutex_lock(&gbl_sc_progress_lk);
    if (stopsc) {
        logmsg(LOGMSG_ERROR, "%s line %d ignoring in-progress\n", func, line);
    } else {
        val = (++gbl_schema_change_in_progress);
        didit = 1;
    }
    Pthread_mutex_unlock(&gbl_sc_progress_lk);
    if (gbl_verbose_set_sc_in_progress) {
        if (didit) {
            comdb2_cheapstack_sym(stderr, "Incrementing sc");
            logmsg(LOGMSG_USER, "%s line %d %s incremented to %d\n", func, line,
                   __func__, val);
        } else {
            logmsg(LOGMSG_USER, "%s line %d %s stopsc ignored increment (%d)\n",
                   func, line, __func__, val);
        }
    }
    return (didit == 0);
}

static int decrement_schema_change_in_progress(const char *func, int line)
{
    int val;
    Pthread_mutex_lock(&gbl_sc_progress_lk);
    val = (--gbl_schema_change_in_progress);
    if (gbl_schema_change_in_progress < 0) {
        logmsg(LOGMSG_FATAL, "%s:%d gbl_sc_ip is %d\n", func, line,
               gbl_schema_change_in_progress);
        abort();
    }
    Pthread_mutex_unlock(&gbl_sc_progress_lk);
    if (gbl_verbose_set_sc_in_progress) {
        comdb2_cheapstack_sym(stderr, "Decremented sc");
        logmsg(LOGMSG_USER, "%s line %d %s decremented to %d\n", func, line,
               __func__, val);
    }
    return 0;
}

int get_schema_change_in_progress(const char *func, int line)
{
    int val, stopped = 0;
    Pthread_mutex_lock(&gbl_sc_progress_lk);
    stopped = stopsc;
    val = gbl_schema_change_in_progress;
    if (gbl_schema_change_in_progress < 0) {
        logmsg(LOGMSG_FATAL, "%s:%d negative sc_in_progress\n", func, line);
        abort();
    }
    Pthread_mutex_unlock(&gbl_sc_progress_lk);
    if (gbl_verbose_set_sc_in_progress) {
        logmsg(LOGMSG_USER, "%s line %d %s returning %d stopsc is %d\n", func,
               line, __func__, val, stopped);
    }
    return val;
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

    if (get_schema_change_in_progress(__func__, __LINE__)) {
        if (strncasecmp(name, pref, preflen) == 0) return name + preflen;
    }
    return NULL;
}

void wait_for_sc_to_stop(const char *operation, const char *func, int line)
{
    Pthread_mutex_lock(&gbl_sc_progress_lk);
    if (stopsc) {
        logmsg(LOGMSG_INFO, "%s:%d stopsc already set for %s\n", func, line,
               operation);
    } else {
        logmsg(LOGMSG_INFO, "%s:%d set stopsc for %s\n", func, line, operation);
        stopsc = 1;
    }
    Pthread_mutex_unlock(&gbl_sc_progress_lk);
    logmsg(LOGMSG_INFO, "%s: set stopsc for %s\n", __func__, operation);
    int waited = 0;
    while (get_schema_change_in_progress(func, line)) {
        if (waited == 0) {
            logmsg(LOGMSG_INFO, "giving schemachange time to stop\n");
        }
        sleep(1);
        waited++;
        if (waited > 10)
            logmsg(LOGMSG_ERROR,
                   "%s: waiting schema changes to stop for: %ds\n", operation,
                   waited);
        if (waited > 60) {
            logmsg(LOGMSG_FATAL,
                   "schema changes take too long to stop, waited %ds\n",
                   waited);
            abort();
        }
    }
    logmsg(LOGMSG_INFO, "proceeding with %s (waited for: %ds)\n", operation,
           waited);
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

static int freesc(void *obj, void *arg)
{
    sc_table_t *sctbl = (sc_table_t *)obj;
    free(sctbl);
    return 0;
}

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
int sc_set_running(struct ireq *iq, struct schema_change_type *s, char *table,
                   int running, const char *host, time_t time, int replicant,
                   const char *func, int line)
{
    sc_table_t *sctbl = NULL;
#ifdef DEBUG_SC
    printf("%s: table %s : %d from %s:%d\n", __func__, table, running, func,
           line);
    comdb2_linux_cheap_stack_trace();
#endif
    int rc = 0;

    assert(running >= 0);
    assert(table);

    Pthread_mutex_lock(&schema_change_in_progress_mutex);
    if (sc_tables == NULL) {
        sc_tables = hash_init_strcaseptr(offsetof(sc_table_t, tablename));
    }
    assert(sc_tables);

    if (running && (sctbl = hash_find_readonly(sc_tables, &table)) != NULL) {
        if (!s || s->kind != SC_DEFAULTSP) {
            logmsg(LOGMSG_ERROR, "%s sc already running against table %s\n", __func__, table);
            rc = -1;
            goto done;
        }
    }
    if (running) {
        if (s && s->kind != SC_DEFAULTSP) {
            /* We are master and already found it. */
            assert(!sctbl);
        }
        /* These two (increment and add to hash) should stay in lockstep */
        if (increment_schema_change_in_progress(func, line)) {
            logmsg(LOGMSG_INFO, "%s:%d aborting sc because stopsc is set\n", __func__, __LINE__);
            rc = -1;
            goto done;
        }
        sctbl = calloc(1, offsetof(sc_table_t, mem) + strlen(table) + 1);
        assert(sctbl);
        strcpy(sctbl->mem, table);
        sctbl->tablename = sctbl->mem;
        sctbl->seed = s ? s->seed : 0;

        sctbl->host = host ? crc32c((uint8_t *)host, strlen(host)) : 0;
        sctbl->time = time;
        hash_add(sc_tables, sctbl);
        if (iq)
            iq->sc_running++;
        if (s) {
            assert(s->set_running == 0);
            s->set_running = 1;
        }
    } else { /* not running */
        if ((sctbl = hash_find_readonly(sc_tables, &table)) != NULL) {
            hash_del(sc_tables, sctbl);
            free(sctbl);
            decrement_schema_change_in_progress(func, line);
            if (iq) {
                iq->sc_running--;
                assert(iq->sc_running >= 0);
            }
            if (s) {
                assert(s->set_running == 1);
                s->set_running = 0;
            }
        } else {
            logmsg(LOGMSG_FATAL, "%s:%d unfound table %s\n", func, line, table);
            abort();
        }
    }
    rc = 0;

done:
    ctrace("sc_set_running(table=%s running=%d): "
           "gbl_schema_change_in_progress %d from %s:%d rc=%d\n",
           table, running, get_schema_change_in_progress(__func__, __LINE__),
           func, line, rc);

    /* I think there's a place that decrements this without waiting for
     * the async sc thread to complete (which is wrong) */
    logmsg(LOGMSG_INFO,
           "sc_set_running(table=%s running=%d): from "
           "%s:%d rc=%d\n",
           table, running, func, line, rc);

    Pthread_mutex_unlock(&schema_change_in_progress_mutex);
    return rc;
}

void sc_assert_clear(const char *func, int line)
{
    Pthread_mutex_lock(&schema_change_in_progress_mutex);
    if (gbl_schema_change_in_progress != 0) {
        logmsg(LOGMSG_FATAL, "%s:%d downgrading with outstanding sc\n", func,
               line);
        abort();
    }
    Pthread_mutex_unlock(&schema_change_in_progress_mutex);
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
               "with seed %0#16" PRIx64 "\n",
               sctbl->tablename, flibc_htonll(sctbl->seed));
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

uint64_t sc_get_seed_table(char *table)
{
    sc_table_t *sctbl = NULL;
    uint64_t seed = 0;
    Pthread_mutex_lock(&schema_change_in_progress_mutex);
    if (sc_tables) {
        sctbl = hash_find_readonly(sc_tables, &table);
        if (sctbl)
            seed = sctbl->seed;
    }
    Pthread_mutex_unlock(&schema_change_in_progress_mutex);
    return seed;
}

void add_ongoing_alter(struct schema_change_type *sc)
{
    assert(IS_ALTERTABLE(sc));
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
    assert(IS_ALTERTABLE(sc));
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
                if (s->kind == SC_ALTERTABLE_PENDING)
                    s->kind = SC_ALTERTABLE;
                break;
            case SC_ACTION_RESUME:
                if (s->preempted == SC_ACTION_PAUSE)
                    ok = 1;
                break;
            case SC_ACTION_COMMIT:
                if (s->preempted == SC_ACTION_PAUSE ||
                    s->preempted == SC_ACTION_RESUME)
                    ok = 1;
                else if (s->kind == SC_ALTERTABLE_PENDING) {
                    s->kind = SC_ALTERTABLE;
                    ok = 1;
                }
                break;
            case SC_ACTION_ABORT:
                ok = 1;
                if (s->kind == SC_ALTERTABLE_PENDING)
                    s->kind = SC_ALTERTABLE;
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
