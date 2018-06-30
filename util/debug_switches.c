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

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stddef.h>

#include "switches.h"

/* TODO: macros to generate all these? */

static struct debug_switches {
    int alternate_verify_fail;
    int on_pthread_create_error;
    int logs_on_panic;
    int verbose_deadlocks_log;
    int incoherent_nodes;
    int rep_delay;
    int force_incoherent;
    int net_verbose;
    int use_blackout_list;
    int osql_force_local;
    int osql_simulate_send_error;
    int osql_verbose_clear;
    int sql_close_sbuf;
    int skip_table_schema_check;
    int inline_mtraps;
    int osql_verbose_history_replay;
    int abort_on_invalid_context;
    int reject_writes_on_rtcpu;
    int ignore_extra_blobs;
    int support_datetimes;
    int ignore_datetime_cast_failures;
    int unlimited_datetime_range;
    int pause_moveto;
    int simulate_verify_error;
    int reset_deadlock_race;
    int cursor_deadlock;
    int recover_deadlock_newmode;
    int poll_on_lock_desired;
    int simulate_find_deadlock;
    int simulate_find_deadlock_retry;
    int verbose_sbuf;
    int disable_connection_refresh;
    int offload_check_hostname;
    int skip_duplicate_seqnums;
    int allow_key_typechange;
    int check_for_hung_checkpoint_thread;
    int skip_skipables_on_verify;
    int verbose_deadlocks;
    int stack_on_deadlock;
    int verbose_fix_pinref;
    int fix_pinref;
    int verbose_cursor_deadlocks;
    int check_multiple_lockers;
    int dump_pool_on_full;
    int net_delay;
} debug_switches;

int init_debug_switches(void)
{
    debug_switches.alternate_verify_fail = 0;
    debug_switches.on_pthread_create_error = 1;
    debug_switches.logs_on_panic = 1;
    debug_switches.verbose_deadlocks_log = 0;
    debug_switches.incoherent_nodes = 1;
    debug_switches.rep_delay = 0;
    debug_switches.force_incoherent = 0;
    debug_switches.net_verbose = 0;
    debug_switches.use_blackout_list = 1;
    debug_switches.osql_force_local = 0;
    debug_switches.osql_simulate_send_error = 0;
    debug_switches.osql_verbose_clear = 0;
    debug_switches.sql_close_sbuf = 0;
    debug_switches.skip_table_schema_check = 0;
    debug_switches.inline_mtraps = 0;
    debug_switches.osql_verbose_history_replay = 0;
    debug_switches.abort_on_invalid_context = 0;
    debug_switches.reject_writes_on_rtcpu = 1;
    debug_switches.ignore_extra_blobs = 0;
    debug_switches.support_datetimes = 1;
    debug_switches.ignore_datetime_cast_failures = 0;
    debug_switches.unlimited_datetime_range = 0;
    debug_switches.pause_moveto = 0;
    debug_switches.simulate_verify_error = 0;
    debug_switches.reset_deadlock_race = 0;
    debug_switches.cursor_deadlock = 0;
    debug_switches.recover_deadlock_newmode = 1;
    debug_switches.poll_on_lock_desired = 1;
    debug_switches.simulate_find_deadlock = 0;
    debug_switches.simulate_find_deadlock_retry = 0;
    debug_switches.verbose_sbuf = 1;
    debug_switches.disable_connection_refresh = 0;
    debug_switches.offload_check_hostname = 0;
    debug_switches.skip_duplicate_seqnums = 1;
    debug_switches.allow_key_typechange = 0;
    debug_switches.check_for_hung_checkpoint_thread = 0;
    debug_switches.skip_skipables_on_verify = 1;
    debug_switches.verbose_deadlocks = 0;
    debug_switches.stack_on_deadlock = 0;
    debug_switches.verbose_fix_pinref = 1;
    debug_switches.fix_pinref = 1;
    debug_switches.verbose_cursor_deadlocks = 0;
    debug_switches.check_multiple_lockers = 1;
    debug_switches.dump_pool_on_full = 1;
    debug_switches.net_delay = 0;

    register_int_switch("alternate_verify_fail", "alternate_verify_fail",
                        &debug_switches.alternate_verify_fail);
    register_int_switch("on_pthread_create_error", "on_pthread_create_error",
                        &debug_switches.on_pthread_create_error);
    register_int_switch("logs_on_panic", "logs_on_panic",
                        &debug_switches.logs_on_panic);
    register_int_switch("verbose_deadlocks_log", "verbose_deadlocks_log",
                        &debug_switches.verbose_deadlocks_log);
    register_int_switch("incoherent_nodes", "incoherent_nodes",
                        &debug_switches.incoherent_nodes);
    register_int_switch("rep_delay", "rep_delay", &debug_switches.rep_delay);
    register_int_switch("force_incoherent", "force_incoherent",
                        &debug_switches.force_incoherent);
    register_int_switch("net_verbose", "net_verbose",
                        &debug_switches.net_verbose);
    register_int_switch("use_blackout_list", "use_blackout_list",
                        &debug_switches.use_blackout_list);
    register_int_switch("osql_force_local", "osql_force_local",
                        &debug_switches.osql_force_local);
    register_int_switch("osql_simulate_send_error", "osql_simulate_send_error",
                        &debug_switches.osql_simulate_send_error);
    register_int_switch("osql_verbose_clear", "osql_verbose_clear",
                        &debug_switches.osql_verbose_clear);
    register_int_switch("sql_close_sbuf", "sql_close_sbuf",
                        &debug_switches.sql_close_sbuf);
    register_int_switch("skip_table_schema_check", "skip_table_schema_check",
                        &debug_switches.skip_table_schema_check);
    register_int_switch("inline_mtraps", "inline_mtraps",
                        &debug_switches.inline_mtraps);
    register_int_switch("osql_verbose_history_replay",
                        "osql_verbose_history_replay",
                        &debug_switches.osql_verbose_history_replay);
    register_int_switch("abort_on_invalid_context", "abort_on_invalid_context",
                        &debug_switches.abort_on_invalid_context);
    register_int_switch("reject_writes_on_rtcpu", "reject_writes_on_rtcpu",
                        &debug_switches.reject_writes_on_rtcpu);
    register_int_switch("ignore_extra_blobs", "ignore_extra_blobs",
                        &debug_switches.ignore_extra_blobs);
    register_int_switch("support_datetimes", "support_datetimes",
                        &debug_switches.support_datetimes);
    register_int_switch("ignore_datetime_cast_failures",
                        "ignore_datetime_cast_failures",
                        &debug_switches.ignore_datetime_cast_failures);
    register_int_switch("unlimited_datetime_range", "unlimited_datetime_range",
                        &debug_switches.unlimited_datetime_range);
    register_int_switch("pause_moveto", "pause_moveto",
                        &debug_switches.pause_moveto);
    register_int_switch("simulate_verify_error", "simulate_verify_error",
                        &debug_switches.simulate_verify_error);
    register_int_switch("reset_deadlock_race", "reset_deadlock_race",
                        &debug_switches.reset_deadlock_race);
    register_int_switch("cursor_deadlock", "cursor_deadlock",
                        &debug_switches.cursor_deadlock);
    register_int_switch("recover_deadlock_newmode", "recover_deadlock_newmode",
                        &debug_switches.recover_deadlock_newmode);
    register_int_switch("poll_on_lock_desired", "poll_on_lock_desired",
                        &debug_switches.poll_on_lock_desired);
    register_int_switch("simulate_find_deadlock", "simulate_find_deadlock",
                        &debug_switches.simulate_find_deadlock);
    register_int_switch("simulate_find_deadlock_retry",
                        "simulate_find_deadlock_retry",
                        &debug_switches.simulate_find_deadlock_retry);
    register_int_switch("verbose_sbuf", "verbose_sbuf",
                        &debug_switches.verbose_sbuf);
    register_int_switch("disable_connection_refresh",
                        "disable_connection_refresh",
                        &debug_switches.disable_connection_refresh);
    register_int_switch("offload_check_hostname", "offload_check_hostname",
                        &debug_switches.offload_check_hostname);
    register_int_switch("skip_duplicate_seqnums", "skip_duplicate_seqnums",
                        &debug_switches.skip_duplicate_seqnums);
    register_int_switch("allow_key_typechange", "allow_key_typechange",
                        &debug_switches.allow_key_typechange);
    register_int_switch("check_for_hung_checkpoint_thread",
                        "check_for_hung_checkpoint_thread",
                        &debug_switches.check_for_hung_checkpoint_thread);
    register_int_switch("skip_skipables_on_verify", "skip_skipables_on_verify",
                        &debug_switches.skip_skipables_on_verify);
    register_int_switch("verbose_deadlocks", "verbose_deadlocks",
                        &debug_switches.verbose_deadlocks);
    register_int_switch("stack_on_deadlock", "stack_on_deadlock",
                        &debug_switches.stack_on_deadlock);
    register_int_switch("verbose_fix_pinref", "verbose_fix_pinref",
                        &debug_switches.verbose_fix_pinref);
    register_int_switch("fix_pinref", "fix_pinref", &debug_switches.fix_pinref);
    register_int_switch("verbose_cursor_deadlocks", "verbose_cursor_deadlocks",
                        &debug_switches.verbose_cursor_deadlocks);
    register_int_switch("check_multiple_lockers", "check_multiple_lockers",
                        &debug_switches.check_multiple_lockers);
    register_int_switch("dump_pool_on_full", "dump_pool_on_full",
                        &debug_switches.dump_pool_on_full);

    return 0;
}

int debug_switch_alternate_verify_fail(void)
{
    return debug_switches.alternate_verify_fail;
}
int debug_exit_on_pthread_create_error(void)
{
    return debug_switches.on_pthread_create_error;
}
int debug_preserve_logs_on_panic(void)
{
    return debug_switches.logs_on_panic;
}
int debug_switch_verbose_deadlocks_log(void)
{
    return debug_switches.verbose_deadlocks_log;
}
int debug_throttle_incoherent_nodes(void)
{
    return debug_switches.incoherent_nodes;
}
int debug_switch_rep_delay(void)
{
    return debug_switches.rep_delay;
}
int debug_switch_force_incoherent(void)
{
    return debug_switches.force_incoherent;
}
int debug_switch_net_verbose(void)
{
    return debug_switches.net_verbose;
}
int debug_switch_use_blackout_list(void)
{
    return debug_switches.use_blackout_list;
}
int debug_switch_osql_force_local(void)
{
    return debug_switches.osql_force_local;
}
int debug_switch_osql_simulate_send_error(void)
{
    return debug_switches.osql_simulate_send_error;
}
int debug_switch_osql_verbose_clear(void)
{
    return debug_switches.osql_verbose_clear;
}
int debug_switch_sql_close_sbuf(void)
{
    return debug_switches.sql_close_sbuf;
}
int debug_switch_skip_table_schema_check(void)
{
    return debug_switches.skip_table_schema_check;
}
int debug_switch_inline_mtraps(void)
{
    return debug_switches.inline_mtraps;
}
int debug_switch_osql_verbose_history_replay(void)
{
    return debug_switches.osql_verbose_history_replay;
}
int debug_switch_abort_on_invalid_context(void)
{
    return debug_switches.abort_on_invalid_context;
}
int debug_switch_reject_writes_on_rtcpu(void)
{
    return debug_switches.reject_writes_on_rtcpu;
}
int debug_switch_ignore_extra_blobs(void)
{
    return debug_switches.ignore_extra_blobs;
}
int debug_switch_support_datetimes(void)
{
    return debug_switches.support_datetimes;
}
int debug_switch_ignore_datetime_cast_failures(void)
{
    return debug_switches.ignore_datetime_cast_failures;
}
int debug_switch_unlimited_datetime_range(void)
{
    return debug_switches.unlimited_datetime_range;
}
int debug_switch_pause_moveto(void)
{
    return debug_switches.pause_moveto;
}
int debug_switch_simulate_verify_error(void)
{
    return debug_switches.simulate_verify_error;
}
int debug_switch_reset_deadlock_race(void)
{
    return debug_switches.reset_deadlock_race;
}
int debug_switch_cursor_deadlock(void)
{
    return debug_switches.cursor_deadlock;
}
int debug_switch_recover_deadlock_newmode(void)
{
    return debug_switches.recover_deadlock_newmode;
}
int debug_switch_poll_on_lock_desired(void)
{
    return debug_switches.poll_on_lock_desired;
}
int debug_switch_simulate_find_deadlock(void)
{
    return debug_switches.simulate_find_deadlock;
}
int debug_switch_simulate_find_deadlock_retry(void)
{
    return debug_switches.simulate_find_deadlock_retry;
}
int debug_switch_verbose_sbuf(void)
{
    return debug_switches.verbose_sbuf;
}
int debug_switch_disable_connection_refresh(void)
{
    return debug_switches.disable_connection_refresh;
}
int debug_switch_offload_check_hostname(void)
{
    return debug_switches.offload_check_hostname;
}
int debug_switch_skip_duplicate_seqnums(void)
{
    return debug_switches.skip_duplicate_seqnums;
}
int debug_switch_allow_key_typechange(void)
{
    return debug_switches.allow_key_typechange;
}
int debug_switch_check_for_hung_checkpoint_thread(void)
{
    return debug_switches.check_for_hung_checkpoint_thread;
}
int debug_switch_skip_skipables_on_verify(void)
{
    return debug_switches.skip_skipables_on_verify;
}
int debug_switch_verbose_deadlocks(void)
{
    return debug_switches.verbose_deadlocks;
}
int debug_switch_stack_on_deadlock(void)
{
    return debug_switches.stack_on_deadlock;
}
int debug_switch_verbose_fix_pinref(void)
{
    return debug_switches.verbose_fix_pinref;
}
int debug_switch_fix_pinref(void)
{
    return debug_switches.fix_pinref;
}
int debug_switch_verbose_cursor_deadlocks(void)
{
    return debug_switches.verbose_cursor_deadlocks;
}
int debug_switch_check_multiple_lockers(void)
{
    return debug_switches.check_multiple_lockers;
}
int debug_switch_dump_pool_on_full(void)
{
    return debug_switches.dump_pool_on_full;
}
int debug_switch_net_delay(void)
{
    return debug_switches.net_delay;
}
