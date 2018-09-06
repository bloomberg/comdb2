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

#ifndef DEBUG_SWITCHES_H
#define DEBUG_SWITCHES_H

/* boolean switches */
int debug_switch_alternate_verify_fail(void);            /* 0 */
int debug_exit_on_pthread_create_error(void);            /* 1 - not debug */
int debug_preserve_logs_on_panic(void);                  /* 1 - not debug */
int debug_switch_verbose_deadlocks_log(void);            /* 0 - not debug */
int debug_throttle_incoherent_nodes(void);               /* 1 - not debug */
int debug_switch_rep_delay(void);                        /* 0 */
int debug_switch_force_incoherent(void);                 /* 0 */
int debug_switch_net_verbose(void);                      /* 0 */
int debug_switch_use_blackout_list(void);                /* 1 - not debug */
int debug_switch_osql_force_local(void);                 /* 0 */
int debug_switch_osql_simulate_send_error(void);         /* 0 */
int debug_switch_osql_verbose_clear(void);               /* 0 */
int debug_switch_sql_close_sbuf(void);                   /* 0 */
int debug_switch_skip_table_schema_check(void);          /* 0 */
int debug_switch_inline_mtraps(void);                    /* 0 */
int debug_switch_osql_verbose_history_replay(void);      /* 0 */
int debug_switch_abort_on_invalid_context(void);         /* 0 */
int debug_switch_reject_writes_on_rtcpu(void);           /* 1 - not debug */
int debug_switch_ignore_extra_blobs(void);               /* 0 - not debug */
int debug_switch_support_datetimes(void);                /* 1 */
int debug_switch_ignore_datetime_cast_failures(void);    /* 0 */
int debug_switch_unlimited_datetime_range(void);         /* 0 */
int debug_switch_pause_moveto(void);                     /* 0 */
int debug_switch_simulate_verify_error(void);            /* 0 */
int debug_switch_reset_deadlock_race(void);              /* 0 */
int debug_switch_cursor_deadlock(void);                  /* 0 */
int debug_switch_recover_deadlock_newmode(void);         /* 1 - not debug */
int debug_switch_poll_on_lock_desired(void);             /* 1 - not debug */
int debug_switch_simulate_find_deadlock(void);           /* 0 */
int debug_switch_simulate_find_deadlock_retry(void);     /* 0 */
int debug_switch_disable_force_readonly(void);           /* 1 */
int debug_switch_verbose_sbuf(void);                     /* 1 */
int debug_switch_disable_connection_refresh(void);       /* 0 */
int debug_switch_offload_check_hostname(void);           /* 0 */
int debug_switch_skip_duplicate_seqnums(void);           /* 1 */
int debug_switch_allow_key_typechange(void);             /* 0 - not debug*/
int debug_switch_check_for_hung_checkpoint_thread(void); /* 0 */
int debug_switch_skip_skipables_on_verify(void);         /* 0 - not debug */
int debug_switch_verbose_deadlocks(void);                /* 0 */
int debug_switch_stack_on_deadlock(void);                /* 0 */
int debug_switch_verbose_fix_pinref(void);               /* 1 */
int debug_switch_fix_pinref(void);                       /* 1 - not debug */
int debug_switch_verbose_cursor_deadlocks(void);         /* 0 */
int debug_switch_check_multiple_lockers(void);           /* 1 */
int debug_switch_dump_pool_on_full(void);                /* 1 */

/* value switches */
int debug_switch_net_delay(void); /* 0 */

#endif
