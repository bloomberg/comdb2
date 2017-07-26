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

pthread_rwlock_t schema_lk = PTHREAD_RWLOCK_INITIALIZER;
pthread_mutex_t schema_change_in_progress_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t fastinit_in_progress_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t schema_change_sbuf2_lock = PTHREAD_MUTEX_INITIALIZER;
volatile int gbl_schema_change_in_progress = 0;
volatile int gbl_lua_version = 0;
uint64_t sc_seed;
static char *sc_host;
static time_t sc_time;
int gbl_default_livesc = 1;
int gbl_default_plannedsc = 1;
int gbl_default_sc_scanmode = SCAN_PARALLEL;

pthread_mutex_t sc_resuming_mtx = PTHREAD_MUTEX_INITIALIZER;
struct schema_change_type *sc_resuming = NULL;

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
/* boolean value set to nonzero if table rebuild is in progress */
int doing_conversion = 0;
/* boolean value set to nonzero if table upgrade is in progress */
int doing_upgrade = 0;
unsigned gbl_sc_adds;
unsigned gbl_sc_updates;
unsigned gbl_sc_deletes;
long long gbl_sc_nrecs;
long long gbl_sc_prev_nrecs; /* nrecs since last report */
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
int sc_live = 0;
/*static pthread_rwlock_t sc_rwlock = PTHREAD_RWLOCK_INITIALIZER;*/

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
    if (gbl_schema_change_in_progress) {
        stopsc = 1;
        logmsg(LOGMSG_INFO, "giving schemachange time to stop\n");
        int retry = 10;
        while (gbl_schema_change_in_progress && retry--) {
            sleep(1);
        }
        logmsg(LOGMSG_INFO, "proceeding with downgrade (waited for: %ds)\n",
               10 - retry);
    }
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
int sc_set_running(int running, uint64_t seed, char *host, time_t time)
{
#ifdef DEBUG
    printf("%s: %d\n", __func__, running);
    comdb2_linux_cheap_stack_trace();
#endif

    pthread_mutex_lock(&schema_change_in_progress_mutex);
    if (thedb->master == gbl_mynode) {
        if (running && gbl_schema_change_in_progress) {
            pthread_mutex_unlock(&schema_change_in_progress_mutex);
            if (seed == sc_seed)
                return 0;
            else {
                logmsg(LOGMSG_ERROR, "schema change already in progress\n");
                return -1;
            }
        } else if (!running && seed != sc_seed && seed) {
            pthread_mutex_unlock(&schema_change_in_progress_mutex);
            logmsg(LOGMSG_ERROR,
                   "cannot stop schema change; wrong seed given\n");
            return -1;
        }
    }
    gbl_schema_change_in_progress = running;
    if (running) {
        sc_seed = seed;
        sc_host = host;
        sc_time = time;
    } else {
        sc_seed = 0;
        sc_host = NULL;
        sc_time = 0;
        gbl_sc_resume_start = 0;
    }
    ctrace("sc_set_running: running=%d seed=0x%llx\n", running,
           (unsigned long long)seed);
    logmsg(LOGMSG_INFO, "sc_set_running: running=%d seed=0x%llx\n", running,
           (unsigned long long)seed);
    pthread_mutex_unlock(&schema_change_in_progress_mutex);
    return 0;
}

void sc_status(struct dbenv *dbenv)
{
    if (gbl_schema_change_in_progress) {
        const char *mach;
        time_t timet;
        getMachineAndTimeFromFstSeed(&mach, &timet);
        struct tm tm;
        localtime_r(&timet, &tm);

        logmsg(LOGMSG_USER, "-------------------------\n");
        logmsg(LOGMSG_USER, "Schema change in progress with seed 0x%llx\n",
               sc_seed);
        logmsg(LOGMSG_USER,
               "(Started on node %s at %04d-%02d-%02d %02d:%02d:%02d)\n",
               mach ? mach : "(unknown)", tm.tm_year + 1900, tm.tm_mon + 1,
               tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec);
        if (doing_conversion) {
            logmsg(LOGMSG_USER, "Conversion phase running %lld converted\n",
                   gbl_sc_nrecs);
        } else if (doing_upgrade) {
            logmsg(LOGMSG_USER, "Upgrade phase running %lld upgraded\n",
                   gbl_sc_nrecs);
        }
        logmsg(LOGMSG_USER, "-------------------------\n");
    } else {
        logmsg(LOGMSG_USER, "schema change running   NO\n");
    }
}

void reset_sc_stat()
{
    gbl_sc_adds = 0;
    gbl_sc_updates = 0;
    gbl_sc_deletes = 0;
    gbl_sc_nrecs = 0;
    gbl_sc_prev_nrecs = 0;
    gbl_sc_abort = 0;
}
/* Turn off live schema change.  This HAS to be done while holding the exclusive
 * lock, and it has to be done BEFORE we try to recover from a failed schema
 * change (removing temp tables etc). */
void live_sc_off(struct dbtable *db)
{
    db->sc_to = NULL;
    db->sc_from = NULL;
    sc_live = 0;
}

int reload_lua()
{
    ++gbl_lua_version;
    logmsg(LOGMSG_DEBUG, "Replicant invalidating Lua machines\n");
    return 0;
}

int replicant_reload_analyze_stats()
{
    ++gbl_analyze_gen;
    logmsg(LOGMSG_DEBUG, "Replicant invalidating SQLite stats\n");
    return 0;
}
