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
extern pthread_mutex_t csc2_subsystem_mtx;
extern struct schema_change_type *sc_resuming;
extern volatile int gbl_schema_change_in_progress;
extern volatile int gbl_lua_version;
extern int gbl_default_livesc;
extern int gbl_default_plannedsc;
extern int gbl_default_sc_scanmode;

extern pthread_mutex_t sc_async_mtx;
extern pthread_cond_t sc_async_cond;
extern volatile int sc_async_threads;

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
extern int gbl_sc_report_freq;      /* seconds between reports */
extern int gbl_sc_abort;
extern uint32_t gbl_sc_resume_start;
/* see sc_del_unused_files() and sc_del_unused_files_check_progress() */
extern int sc_del_unused_files_start_ms;
extern int gbl_sc_del_unused_files_threshold_ms;

extern int gbl_sc_commit_count; /* number of schema change commits - these can
                                render a backup unusable */

extern int gbl_pushlogs_after_sc;

extern int rep_sync_save;
extern int log_sync_save;
extern int log_sync_time_save;

extern int stopsc;        /* stop schemachange, so it can resume */

int is_dta_being_rebuilt(struct scplan *plan);
const char *get_sc_to_name();
void wait_for_sc_to_stop(const char *operation);
void allow_sc_to_run();
int sc_set_running(char *table, int running, uint64_t seed, const char *host,
                   time_t time);
void sc_status(struct dbenv *dbenv);
void live_sc_off(struct dbtable *db);
void sc_set_downgrading(struct schema_change_type *s);
void reset_sc_stat();
int reload_lua();
int replicant_reload_analyze_stats();

void sc_set_logical_redo_lwm(char *table, unsigned int file);
unsigned int sc_get_logical_redo_lwm();
unsigned int sc_get_logical_redo_lwm_table(char *table);

void add_ongoing_alter(struct schema_change_type *sc);
void remove_ongoing_alter(struct schema_change_type *sc);
struct schema_change_type *find_ongoing_alter(char *table);
struct schema_change_type *preempt_ongoing_alter(char *table, int action);
void clear_ongoing_alter();

#endif
