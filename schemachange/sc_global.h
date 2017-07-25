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

#ifndef INCLUDE_SC_GLOBAL_H
#define INCLUDE_SC_GLOBAL_H

extern pthread_mutex_t schema_change_in_progress_mutex;
extern pthread_mutex_t fastinit_in_progress_mutex;
extern pthread_mutex_t schema_change_sbuf2_lock;
extern pthread_mutex_t sc_resuming_mtx;
extern struct schema_change_type *sc_resuming;
extern volatile int gbl_schema_change_in_progress;
extern volatile int gbl_lua_version;
extern uint64_t sc_seed;
extern int gbl_default_livesc;
extern int gbl_default_plannedsc;
extern int gbl_default_sc_scanmode;

/* Throttle settings, which you can change with message traps.  Note that if
 * you have gbl_sc_usleep=0, the important live writer threads never get to
 * run. */
extern int gbl_sc_usleep;
extern int gbl_sc_wrusleep;

/* When the last write came in.  Live schema change thread needn't sleep
 * if the database is idle. */
extern int gbl_sc_last_writer_time;

/* updates/deletes done behind the cursor since schema change started */
extern pthread_mutex_t gbl_sc_lock;
extern int doing_conversion;
extern int doing_upgrade;
extern unsigned gbl_sc_adds;
extern unsigned gbl_sc_updates;
extern unsigned gbl_sc_deletes;
extern long long gbl_sc_nrecs;
extern long long gbl_sc_prev_nrecs; /* nrecs since last report */
extern int gbl_sc_report_freq;      /* seconds between reports */
extern int gbl_sc_abort;
extern int gbl_sc_resume_start;
/* see sc_del_unused_files() and sc_del_unused_files_check_progress() */
extern int sc_del_unused_files_start_ms;
extern int gbl_sc_del_unused_files_threshold_ms;

extern int gbl_sc_commit_count; /* number of schema change commits - these can
                                render a backup unusable */

extern int gbl_pushlogs_after_sc;

extern int rep_sync_save;
extern int log_sync_save;
extern int log_sync_time_save;

extern int gbl_sc_thd_failed;

/* All writer threads have to grab the lock in read/write mode.  If a live
 * schema change is in progress then they have to do extra stuff. */
extern int sc_live;
/* pthread_rwlock_t sc_rwlock = PTHREAD_RWLOCK_INITIALIZER;*/

extern int schema_change; /*static int schema_change_doomed = 0;*/
extern int stopsc;        /* stop schemachange, so it can resume */

int is_dta_being_rebuilt(struct scplan *plan);
const char *get_sc_to_name();
void wait_for_sc_to_stop();
int sc_set_running(int running, uint64_t seed, char *host, time_t time);
void sc_status(struct dbenv *dbenv);
void live_sc_off(struct dbtable *db);
void reset_sc_stat();
int reload_lua();
int replicant_reload_analyze_stats();

#endif
