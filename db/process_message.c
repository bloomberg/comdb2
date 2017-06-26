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

extern int __berkdb_write_alarm_ms;
extern int __berkdb_read_alarm_ms;
extern int __berkdb_seek_alarm_ms;
extern int __berkdb_fsync_alarm_ms;

#include <pthread.h>

#include "limit_fortify.h"
#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <stdint.h>
#include <errno.h>

#include <unistd.h>
#include <sys/types.h>
#include <sys/statvfs.h>

#include <segstr.h>
#include <plink.h>
#include <epochlib.h>
#include <net.h>
#include <memory_sync.h>
#include <plink.h>

#include <bdb_api.h>
#ifdef _LINUX_SOURCE
#endif
#include <sqliteInt.h>
#include <ctrace.h>

#include "comdb2.h"
#include "block_internal.h"
#include "timer.h"
#include "translistener.h"
#include "tag.h"
#include "sigutil.h"
#include "util.h"
#include "memdebug.h"
#include "verify.h"
#include "switches.h"

#include "osqlrepository.h"
#include "osqlcomm.h"
#include "osqlblockproc.h"
#include "comdb2_shm.h"
#include "thdpool.h"
#include "bdb_access.h"
#include "analyze.h"
#include "dbdest.h"
#include "intern_strings.h"
#include "comdb2util.h"
#include <stdbool.h>
#include "utilmisc.h"
#include "sqllog.h"
#include "views.h"
#include <autoanalyze.h>
#include "quantize.h"
#include <machine.h>
#include "timers.h"
#include "crc32c.h"
#include "ssl_bend.h"

#include <trigger.h>
#include <sc_stripes.h>
#include <sc_global.h>
#include <logmsg.h>

extern int gbl_exit_alarm_sec;
extern int gbl_sql_tranlevel_sosql_pref;
extern int gbl_disable_rowlocks_logging;
extern int gbl_disable_rowlocks;
extern int gbl_disable_rowlocks_sleepns;
extern int gbl_dispatch_rowlocks_bench;
extern int gbl_rowlocks_bench_logical_rectype;
extern int n_fstrap;
extern unsigned long long gbl_sql_deadlock_reconstructions;
extern unsigned long long gbl_sql_deadlock_failures;
extern int gbl_net_lmt_upd_incoherent_nodes;
extern int gbl_dump_sql_dispatched;
extern int gbl_dump_fsql_response;
extern int gbl_time_osql;
extern int gbl_time_fdb;
extern int gbl_enable_good_sql_return_codes;
extern int gbl_enable_cache_internal_nodes;
extern int gbl_test_badwrite_intvl;
extern int gbl_skip_ratio_trace;
extern int gbl_test_blob_race;
extern int gbl_test_badwrite_zerop_intvl;
extern int gbl_move_deadlk_max_attempt;
extern int gbl_lock_conflict_trace;
extern unsigned long long gbl_verify_retry;
extern int gbl_early;
extern int gbl_reallyearly;
extern int gbl_udp;
extern int gbl_prefault_udp;
extern int gbl_prefault_latency;
extern int gbl_update_shadows_interval;
extern int gbl_notimeouts;

void debug_bulktraverse_data(char *tbl);

int gbl_track_sqlengine_states = 0;
int gbl_break_lua;

extern void reinit_sql_hint_table();

static void dump_table_sizes(struct dbenv *dbenv);
static void request_stats(struct dbenv *dbenv);
void loadrawfile(char *fname, char *table);
void berk_memp_sync_alarm_ms(int x);
int berkdb_get_max_rep_retries();

void walkback_set_warnthresh(int thresh);
int walkback_get_warnthresh(void);
void walkback_disable(void);
void walkback_enable(void);

extern int gbl_osql_verify_retries_max;
extern int gbl_blocksql_grace;
extern bool gbl_rcache;

static pthread_mutex_t testguard = PTHREAD_MUTEX_INITIALIZER;
void bdb_locktest(void *);
void bdb_berktest(void *, uint32_t);
void bdb_berktest_multi(void *);
void bdb_berktest_commit_delay(uint32_t);
void rowlocks_clear_stats(void);
void rowlocks_print_stats(FILE *f);
void rowlocks_bench(void *, int, int);
void rowlocks_lock1_bench(void *, int, int);
void rowlocks_lock2_bench(void *, int, int);
void commit_bench(void *, int, int);
void set_cursor_rowlocks(int cr);
void bdb_detect(void *);
void enable_ack_trace(void);
void disable_ack_trace(void);
int bdb_dump_logical_tranlist(void *state, FILE *f);

static const char *HELP_MAIN[] = {
    "stat           - status report",
    "async ...      - execute mtrap asynchronously",
    "detach_mtrap   - detach currently running mtrap thread",
    "cton           - use constraint logic",
    "ctof           - stop using constraint logic",
    "debg #         - operation debugging", "who #          - who debugging",
    "meta ...       - meta db debugging",
    "netdon         - net direct writes (non-queued) on!",
    "netdof         - net direct writes (non-queued) off!",
    "netdbg #       - net library trace level (0-off)",
    "netpoll #      - net library accept-poll ms",
    "osqlnetpoll #  - osql net library accept-poll ms",
    "sqlpool        - on/off/stat/mark #/ # of threads.  fast sql pool thread "
    "control",
    "scon/scof      - request report",
    "erron/erroff   - db error report back to client",
    "ling #         - set of seconds for idle thread linger",
    "maxt #         - set max # of threads",
    "maxwt #        - set max # of writer threads",
    "maxq #         - set max # of items on queue",
    "sync <full|normal|source|none> - sync parameters (sync he for more)",
    "electtime #    - override timeout for elections in seconds,",
    "                 0 for no override",
    "incoh 1/0      - set incoherency status 1=incoherent, 0=coherent",
    "delay #        - set commit delay in ms; use this to throttle the write "
    "rate",
    "delaymax #     - set maximum commit delay in ms; this is the max delay "
    "that",
    "                 the master will set automatically at the request of "
    "replicants",
    "allow ...      - same format as in lrl file",
    "disallow ...   - same format as in lrl file",
    "bdb ...        - backend commands", "debug ...      - misc debugging",
    "blob ...       - blob subsystem commands",
    "sql ...        - sql subsystem commands",
    "help stat      - other general status query commands",
    "help bdb       - database backend commands",
    "help java      - java stored procedure engine commands",
    "help schema    - schema related commands",
    "help queue     - ondisk queue commands",
    "help fstblk    - fstblk commands", "help compr     - compression commands",
    "help analyze   - analyze commands", "exit           - exit task", NULL};

static const char *HELP_JAVA[] = {
    "Java stored procedure engine commands:-", "java stat",
    "java load <name> <jar> [args...]", "java reload <name> <jar> [args...]",
    "java unload <name>", "java sigjava           - Set JVM signal handlers",
    "java sigorig           - Set pre-JVM signal handlers", NULL};
static const char *HELP_QUEUES[] = {
    "Queue commands:-", "queue stat              - status report on all queues",
    "queue stat name         - status report on named queue",
    "queue slowstat name     - full status report on named queue",
    "queue flush <name> <#>  - flush a given queue by consumer number",
    "queue flush abort       - abort active flush operation",
    "queue create <name> <sz> - create queue with given avg item sz",
    "queue consumer <name> <#> <method> - set consumer #",
    "queue debg <#>          - debug trace for next # events in consumer "
    "threads ",
    "queue dump <name> <file>- dump queue to file",
    "queue goose <name>      - goose the head of the queue",
    "queue gooserates #1 #2  - set/query goose rates.",
    "                          #1 is add rate, #2 is consume rate in seconds",
    "queue poll #            - set poll interval in seconds.  0 is default and "
    "is",
    "                          best for cpu.",
    "queue wake              - immediately wake all consumers on all queues",
    NULL};
static const char *HELP_STAT[] = {
    "Database status query commands:-",
    "stat                       - basic status report",
    "stat stax                  - dumps all tables and basic schema info",
    "stat stal                  - thread status",
    "stat long                  - request statistics",
    "stat reql                  - dumps long request settings",
    "stat appsock               - socket request statistics",
    "stat fstblk                - fstblk statistics",
    "stat blob                  - blob subsystems statistics",
    "stat resources             - dump list of registered resources",
    "stat signals               - signal handling setup",
    "stat csc2vers <table>      - get current schema version for table",
    "stat dumpcsc2 <table> #    - dump version # of schema for given table",
    "stat rmtpol #              - remote policy for given machine number",
    "stat thr                   - dump all registered threads",
    "stat dumpsql               - running sql statements",
    "stat size                  - database ondisk size info",
    "stat ixstat                - index usage stats",
    "stat cursors               - cursor mode stats",
    "stat sc                    - view status of current schema change",
    "stat switch                - show switch statuses",
    "stat clnt [#] [rates|totals]- show per client request stats",
    "stat mtrap                 - show mtrap system stats",
    "dmpl                       - dump threads",
    "dmptrn                     - show long transaction stats",
    "dmpcts                     - show table constraints", NULL,
};
static const char *HELP_SQL[] = {
    "sql ...",
    "dump               - dump currently running statements and cursor info",
    "keep N             - keep stats on last N statements",
    "hist               - show recently run statements",
    "cancel N           - cancel running statement",
    "rdtimeout N        - set read timeout in ms",
    "wrtimeout N        - set write timeout in ms",
    "help               - this information", NULL,
};
static const char *HELP_SCHEMA[] = {
    "Commands for inspecting and altering schema information:-",
    "morestripe #                   - increase dtastripe factor to #",
    "morestripe blobstripe          - enable blob striping",
    "load <table> <path/to/csc2>    - change the schema of an existing table,",
    "                                 or add a new table",
    "reinit <table>                 - reinitialize a table in a db",
    "count <table>                  - count the records in the given table",
#ifdef BERKDB_46
    "compact <table> [timeout] [-f] - compact table btree, use timeout ms for "
    "each lock -default 2000ms-, free fs space if -f",
    "pacecompact <time>             - after each microtransaction the loop "
    "will sleep for \"time\" msec; -1 will stop it",
#endif
    "stat csc2vers <table>          - get current schema version for table",
    "stat dumpcsc2 <table> #        - dump version # of schema for given table",
    "dumprecord <table> <rrn/genid> - dump record by rrn/genid",
    "dmpcts                         - show table constraints",
    "dumptags                       - list known tags",
    "screportfreq                   - set progress report frequency in seconds",
    "Live schema change specific commands:-", "scdelay #                      "
                                              "- set conversion thread delay "
                                              "in milliseconds",
    "scwrdelay #                    - set transaction delay while in live "
    "schema",
    "                                 change in milliseconds",
    "scabort                        - abort schema change in progress",
    "stat sc                        - view status of current schema change",
    "checkctags <off|soft|full>     - check converting client tags to make "
    "sure all fields exist in server; off - skip check; soft - print warning "
    "on error; full - error out to caller",
    "Note that commands that alter schema must be run on the master node.",
    NULL};

static const char *HELP_COMPR[] = {
    "Commands for inspecting and altering compression flags:-",
    "stat compr          - query compression flags",
    "compr <table> <alg> - change compression algorithm for table",
    "bcompr <table> <alg> - change compression algorithm for blobs in table",
    "                      Compression algorithms are:",
    "                      1 - zlib, 0 - none", "",
    "To rebuild a table with different compression options:",
    "load <table> <file.csc2> <odh> <alg> <blobalg>",
    "  where odh is 1 or 0 (required for compression).", NULL};
static const char *HELP_ANALYZE[] = {
    "Commands for setting analyze options:-",
    "stat analyze       - print analyze stats",
    "backout [table]    - backout analyze stats [optionally for table]",
    "sample             - enable sampling btrees",
    "nosample           - disable sampling btrees",
    "thresh <size>      - sample tables larger than <size>",
    "compthd <numthds>  - set maximum concurrent sampling-threads",
    "tblthd <numthds>   - set maximum concurrent tbl-threads",
    "headroom <n%>      - fail if freespace falls below n%",
    "abort              - abort currently running analyze on this node",
    NULL};

static const char *HELP_MEMDEBUG[] = {
    "Commands for every Nth friday when we discover terrible memory leaks:",
    "callers            - dump all stacks with outstanding blocks",
    "blocks             - dump all outstanding blocks", NULL};

static const char *HELP_MEMSTAT[] = {
    "Commands for memory status display:-",
    "memstat [hr] [verbose] [pattern] "
    "[group_by_name|group_by_scope|group_by_name_scope] "
    "[name_asc|name_desc|scope_asc|scope_desc|total_asc|total_desc|used_asc|"
    "used_desc]",
    "memstat release      - release reserved memory back to OS. This is an "
    "expensive operation. "
    "The database may experience slowness when releasing memory.",
    "memstat autoreport # - auto report memory usage every # seconds. "
    "Caution should be used when setting the frequency. Performance issues may "
    "result "
    "if auto reporting too frequently.",
    "", "Examples",
    "memstat hr - display memstats on all subsystems in human readable format",
    "memstat total_asc - display memstats on all subsystems sorted by memory "
    "usage",
    "memstat total_asc berkdb* - display memstats on subsystems whose names "
    "match the pattern 'berkdb*', sorted by memory usage",
    "memstat total_asc group_by_name - display memstats on all subsystems, "
    "order by memory usage, group by name",
    "", "Columns", "name  - area name", "scope - scope of the area",
    "MT    - Multi-thread safe", "init  - initial size",
    "cap   - maximum capacity",
    "file  - in which file the allocator is created",
    "line  - at which line the allocator is created",
    "total - total memory consumption and the percentage compared to the "
    "amount consumed by all subsystems",
    "used  - actual used memory. A small chunk (~1KB on 64-bit and ~0.5KB "
    "bytes on 32-bit, typically) will be reserved by dlmalloc.",
    "free  - free memory", NULL};


static int comdb2_toknum(const char *tok, int ltok, int *output)
{
    int rc;
    char *endptr;
    char *numstr = tokdup(tok, ltok);
    int n = strtol(numstr, &endptr, 10);
    if (*endptr || endptr == numstr) {
        rc = 1;
    } else {
        *output = n;
        rc = 0;
    }
    free(numstr);
    return rc;
}

static void print_help_page(const char *lines[])
{
    int ii;
    for (ii = 0; lines[ii]; ii++)
        logmsg(LOGMSG_USER, "%s\n", lines[ii]);
}

/* SJ - only print databases that have numbers.  Fed up of useless spew when
 * I send stat to a database.  "stat stax" can give a full table list. */
void print_dbs(struct dbenv *dbenv)
{
    int i;
    for (i = 0; i < dbenv->num_dbs; i++) {
        if (dbenv->dbs[i]->dbnum > 0)
            logmsg(LOGMSG_USER, "managing db %-5d '%s'\n", dbenv->dbs[i]->dbnum,
                   dbenv->dbs[i]->dbname);
    }
}

extern struct dbenv *thedb;

static int linecmp(char *line, int lline, int st, const char *str)
{
    int len = strlen(str);
    if (lline - st >= len && strncmp(line + st, str, len) == 0)
        return 0;
    else
        return 1;
}

void replication_stats(struct dbenv *dbenv)
{
    logmsg(LOGMSG_USER, "Replication statistics:-\n");
    logmsg(LOGMSG_USER, "   Num commits      %d\n", dbenv->num_txns);
    if (dbenv->num_txns > 0) {
        logmsg(LOGMSG_USER, "   Avg txn sz           %llu\n",
               dbenv->total_txn_sz / dbenv->num_txns);
        logmsg(LOGMSG_USER, "   Avg txn rep timeout  %d\n",
               dbenv->total_timeouts_ms / dbenv->num_txns);
        logmsg(LOGMSG_USER, "   Avg txn rep time     %d\n",
               dbenv->total_reptime_ms / dbenv->num_txns);
        logmsg(LOGMSG_USER, "   Max txn sz           %llu\n", dbenv->biggest_txn);
        logmsg(LOGMSG_USER, "   Max rep timeout      %d\n", dbenv->max_timeout_ms);
        logmsg(LOGMSG_USER, "   Max rep time         %d\n", dbenv->max_reptime_ms);
    }
    logmsg(LOGMSG_USER, "   Minimum        %d ms\n",
           bdb_attr_get(dbenv->bdb_attr, BDB_ATTR_MINREPTIMEOUT));
}

int process_sync_command(struct dbenv *dbenv, char *line, int lline, int st)
{
    char *tok;
    int ltok;
    while (1) {
        /* EZ sync config: */
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0)
            break;
        if (tokcmp(tok, ltok, "full") == 0) {
            dbenv->rep_sync = REP_SYNC_FULL;
            dbenv->log_sync = 1;
            dbenv->log_sync_time = 0;
        } else if (tokcmp(tok, ltok, "normal") == 0) {
            dbenv->rep_sync = REP_SYNC_FULL;
            dbenv->log_sync = 0;
            dbenv->log_sync_time = 10; /*sync logs every n seconds */
        } else if (tokcmp(tok, ltok, "source") == 0) {
            dbenv->rep_sync = REP_SYNC_SOURCE;
            dbenv->log_sync = 0;
            dbenv->log_sync_time = 10;
        } else if (tokcmp(tok, ltok, "rep_always_wait") == 0) {
            logmsg(LOGMSG_USER, "got rep_always_wait\n");
            dbenv->rep_always_wait = 1;
        }

        else if (tokcmp(tok, ltok, "none") == 0) {
            dbenv->rep_sync = REP_SYNC_NONE;
            dbenv->log_sync = 0;
            dbenv->log_sync_time = 30;
        } else if (tokcmp(tok, ltok, "room") == 0) {
            dbenv->rep_sync = REP_SYNC_ROOM;
            dbenv->log_sync = 1;
            dbenv->log_sync_time = 180;
        } else if (tokcmp(tok, ltok, "log-sync-time") == 0) {
            int tm;
            tok = segtok(line, lline, &st, &ltok);
            tm = toknum(tok, ltok);
            if (ltok == 0 || tm < 0) {
                logmsg(LOGMSG_ERROR, "must specify log_sync time\n");
                break;
            }
            if (tm == 0) {
                dbenv->log_sync = 1;
                dbenv->log_sync_time = 0;
            } else {
                dbenv->log_sync = 0;
                dbenv->log_sync_time = tm;
            }
        } else if (tokcmp(tok, ltok, "log-delete-now") == 0) {
            dbenv->log_delete_age = 0;
        } else if (tokcmp(tok, ltok, "log-delete-before") == 0) {
            dbenv->log_delete_age = time_epoch();
        } else if (tokcmp(tok, ltok, "log-delete-age-set") == 0) {
            int epoch;
            tok = segtok(line, lline, &st, &ltok);
            epoch = toknum(tok, ltok);
            if (ltok == 0 || epoch < 0) {
                logmsg(LOGMSG_ERROR, "must specify a positive epoch time\n");
                break;
            }
            if (dbenv->log_delete_age > 0) {
                dbenv->log_delete_age = epoch ? epoch : time_epoch();
            }
        } else if (tokcmp(tok, ltok, "log-delete") == 0) {
            tok = segtok(line, lline, &st, &ltok);
            if (tokcmp(tok, ltok, "on") == 0) {
                log_delete_counter_change(dbenv, LOG_DEL_ABS_ON);
                cantim(TMEV_ENABLE_LOG_DELETE);
            } else if (tokcmp(tok, ltok, "off") == 0) {
                int reenab;
                log_delete_counter_change(dbenv, LOG_DEL_ABS_OFF);
                tok = segtok(line, lline, &st, &ltok);
                reenab = toknum(tok, ltok);
                if (reenab > 0) {
                    /* re-enable log in n minutes */
                    timer(reenab * 1000 * 60, TMEV_ENABLE_LOG_DELETE);
                } else {
                    /* no re-enable requested */
                    cantim(TMEV_ENABLE_LOG_DELETE);
                }
            } else {
                logmsg(LOGMSG_USER, "must specify log-delete on or off\n");
                break;
            }
        } else if (tokcmp(tok, ltok, "rep-sync") == 0) {
            tok = segtok(line, lline, &st, &ltok);

            if (tokcmp(tok, ltok, "full") == 0)
                dbenv->rep_sync = REP_SYNC_FULL;

            else if (tokcmp(tok, ltok, "source") == 0)
                dbenv->rep_sync = REP_SYNC_SOURCE;

            else if (tokcmp(tok, ltok, "none") == 0)
                dbenv->rep_sync = REP_SYNC_NONE;

            else if (tokcmp(tok, ltok, "room") == 0)
                dbenv->rep_sync = REP_SYNC_ROOM;

            else {
                logmsg(LOGMSG_USER, "rep_sync must be full,source,room or none\n");
                break;
            }
        } else if (tokcmp(tok, ltok, "adapt") == 0) {
            /* This is now all deprecated - the newest scheme is tunable at
             * the bdb level */
            tok = segtok(line, lline, &st, &ltok);
            if (ltok == 0) {
                logmsg(LOGMSG_ERROR, "adapt must be followed by a directive and a numerical "
                       "parameter\n");
                break;
            } else {
                char *tok2;
                int ltok2;
                tok2 = segtok(line, lline, &st, &ltok2);
                if (ltok2 == 0) {
                    logmsg(LOGMSG_ERROR, "adapt must be followed by a directive and a "
                           "numerical parameter\n");
                    break;
                }
                toknum(tok2, ltok2);
            }

            if (tokcmp(tok, ltok, "min") == 0)
                logmsg(LOGMSG_ERROR, "sync adapt commands are now deprecated\n");
            else if (tokcmp(tok, ltok, "max") == 0)
                logmsg(LOGMSG_ERROR, "sync adapt commands are now deprecated\n");
            else if (tokcmp(tok, ltok, "base") == 0)
                logmsg(LOGMSG_ERROR, "sync adapt commands are now deprecated\n");
            else if (tokcmp(tok, ltok, "bpms") == 0)
                logmsg(LOGMSG_ERROR, "sync adapt commands are now deprecated\n");
            else {
                logmsg(LOGMSG_ERROR, "unrecognised adapt command\n");
                break;
            }
        } else if (tokcmp(tok, ltok, "atleast") == 0) {
            int num;
            tok = segtok(line, lline, &st, &ltok);
            if (ltok == 0) {
                logmsg(LOGMSG_ERROR, "atleast must be followed by a directive and a "
                       "numerical parameter\n");
                break;
            }
            num = toknum(tok, ltok);
            dbenv->wait_for_N_nodes = num;
            dbenv->rep_sync = REP_SYNC_N;
        } else {
            logmsg(LOGMSG_USER, " ez options:\n");
            logmsg(LOGMSG_USER, "     sync <full|normal|source|none>\n");
            logmsg(LOGMSG_USER, "          full - full cache coherency, osync logs\n");
            logmsg(LOGMSG_USER, "          normal - full cache coherency, 10 second log "
                                "sync\n");
            logmsg(LOGMSG_USER, "          source - source cache coherency, 10 second log "
                                "sync\n");
            logmsg(LOGMSG_USER, "          none - no cache coherency, 30 second log sync\n");
            logmsg(LOGMSG_USER, " advanced options:\n");
            logmsg(LOGMSG_USER, "     sync rep-sync <full|source|none> -replication sync\n");
            logmsg(LOGMSG_USER, "     sync log-delete <on|off [re-enable_time_in_minutes]>\n");
            logmsg(LOGMSG_USER, "     sync log-delete-now - delete logs as soone as we can\n");
            logmsg(LOGMSG_USER, "     sync log-delete-before - delete log files that "
                                "predate this msgtrap\n");
            logmsg(LOGMSG_USER, "     sync log-sync-time <seconds> - how often to sync log\n");
            logmsg(LOGMSG_USER, "     sync adapt min  - min rep timeout in ms\n");
            logmsg(LOGMSG_USER, "     sync adapt max  - max rep timeout in ms\n");
            logmsg(LOGMSG_USER, "     sync adapt base - base rep timeout in ms\n");
            logmsg(LOGMSG_USER, "     sync adapt bpms - bytes per ms for adaptive rep "
                                "timeout\n");
            return -1;
        }
    }
    backend_update_sync(dbenv);
    return 0;
}

int sql_testrun(char *sql, int sqllen);

void fastcount(char *tablename);

/* Seem to need this all over the place. */
static void on_off_trap(char *line, int lline, int *st, int *ltok, char *msg,
                        char *trap, int *value)
{
    char *tok;
    tok = segtok(line, lline, st, ltok);
    if (*ltok == 0) {
        logmsg(LOGMSG_USER, "%s is currently %s\n", msg, *value ? "ON" : "OFF");
        return;
    } else if (tokcmp(tok, *ltok, "on") == 0) {
        if (*value) {
           logmsg(LOGMSG_USER, "%s is already on\n", msg);
            return;
        } else {
           logmsg(LOGMSG_USER, "%s is now on\n", msg);
            *value = 1;
        }
    } else if (tokcmp(tok, *ltok, "off") == 0) {
        if (!*value) {
           logmsg(LOGMSG_USER, "%s is already off\n", msg);
            return;
        } else {
           logmsg(LOGMSG_USER, "%s is now off\n", msg);
            *value = 0;
        }
    } else {
       logmsg(LOGMSG_USER, "Expected on/off for %s command\n", trap);
    }
}

char *deadlock_policy_str(int policy)
{
    switch (policy) {
    case 0:
        return "DB_LOCK_NORUN";
        break;

    case 1:
        return "DB_LOCK_DEFAULT";
        break;

    case 2:
        return "DB_LOCK_EXPIRE";
        break;

    case 3:
        return "DB_LOCK_MAXLOCKS";
        break;
    case 4:
        return "DB_LOCK_MINLOCKS";
        break;
    case 5:
        return "DB_LOCK_MINWRITE";
        break;
    case 6:
        return "DB_LOCK_OLDEST";
        break;
    case 7:
        return "DB_LOCK_RANDOM";
        break;
    case 8:
        return "DB_LOCK_YOUNGEST";
        break;
    case 9:
        return "DB_LOCK_MAXWRITE";
        break;
    case 10:
        return "DB_LOCK_MINWRITE_NOREAD";
        break;
    case 11:
        return "DB_LOCK_YOUNGEST_EVER";
        break;
    case 12:
        return "DB_LOCK_MINWRITE_EVER";
        break;
    default:
        return "INVALID_POLICY";
        break;
    }
}

extern int gbl_new_snapisol;
#ifdef NEWSI_STAT
void bdb_print_logfile_pglogs_stat();
#endif
void bdb_osql_trn_clients_status();


void *handle_exit_thd(void *arg) 
{
    static pthread_mutex_t exiting_lock = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_lock(&exiting_lock);
    if( gbl_exit ) {
       pthread_mutex_unlock(&exiting_lock);
       return NULL;
    }
    gbl_exit = 1;
    pthread_mutex_unlock(&exiting_lock);

    struct dbenv *dbenv = arg;
    int qid, dbnum, alarmtime = (gbl_exit_alarm_sec > 0 ? gbl_exit_alarm_sec : 300);

    /* this defaults to 5 minutes */
    alarm(alarmtime);


    if (bdb_is_an_unconnected_master(dbenv->dbs[0]->handle)) {
       logmsg(LOGMSG_INFO, "This was standalone\n");
        wait_for_sc_to_stop(); /* single node, need to stop SC */
    }
    else {
        /* hand over masterness to a new candidate if we're the master. */
        bdb_transfermaster(dbenv->dbs[0]->handle);
    }

    /* dont let any new requests come in.  we're going to go non-coherent
       here in a second, so letting new reads in would be bad. */
    no_new_requests(dbenv);

    /* let the lower level start advertising high lsns to go non-coherent
       - dont hang the master waiting for sync replication to an exiting
       node. */
    bdb_exiting(dbenv->dbs[0]->handle);

    /* XXX this should probably have a timeout */
    stop_threads(thedb);

    /* now that we are taking no more requests and have halted all request
     * threads, take a final snapshot of our queues (this helps things
     * like the audtqdb problem where we may want to know if the queue was
     * empty when the db exited - if so the db can be reinited when it
     * starts up again) */
    if (thedb->num_qdbs > 0 && gbl_dump_queues_on_exit) {
        int ii;
        logmsg(LOGMSG_USER, "Final status of queues:\n");
        for (ii = 0; ii < thedb->num_qdbs; ii++)
            dbqueue_stat(thedb->qdbs[ii], 0, 0, 1 /*(blocking call)*/);
    }

    dbnum = dbenv->dbnum;

    bdb_thread_event(thedb->bdb_env, BDBTHR_EVENT_START_RDWR);
    flush_db();
    clean_exit();
    bdb_thread_event(thedb->bdb_env, BDBTHR_EVENT_DONE_RDWR);

    return NULL;
}


int process_command(struct dbenv *dbenv, char *line, int lline, int st)
{
    char *tok;
    int ltok, tst, ntok, stsav = st, llinesav = lline;
    int i = 0;
    int rc = 0;

    /* prevent this if the threads are stopped; initial intent is
       to prevent a bdb access while the db is closed during schema
       change (which is crashing the db) */
    if (dbenv->stopped) {
        logmsg(LOGMSG_USER, "Threads are stopped, ignoring message trap.\n");
        return 0;
    }

    /*process*/
    tok = segtok(line, lline, &st, &ltok);
    if (ltok == 0)
        return -1;
    if (gbl_exit) {
        logmsg(LOGMSG_USER, "gbl_exit set, skipping command\n");
        return -1;
    }

    if (tokcmp(tok, ltok, "exit") == 0) {
        logmsg(LOGMSG_WARN, "requested exit...\n");

        pthread_t thread_id;
        pthread_attr_t thd_attr;

        pthread_attr_init(&thd_attr);
        pthread_attr_setstacksize(&thd_attr, 4 * 1024); /* 4K */
        pthread_attr_setdetachstate(&thd_attr, PTHREAD_CREATE_DETACHED);

        int rc = pthread_create(&thread_id, &thd_attr,
                handle_exit_thd, (void *)dbenv);
        if (rc != 0) {
            logmsgperror("create exit thread: pthread_create");
            exit(1);
        }
    } else if(tokcmp(tok,ltok, "partinfo")==0) {
        char opt[128];

        tok=segtok(line, lline, &st, &ltok);
        if(ltok != 0)
        {
            tokcpy0(tok, ltok, opt, sizeof(opt));
        }
        else
        {
            snprintf(opt, sizeof(opt), "all");
        }

        comdb2_partition_info_all(opt);
    } else if (tokcmp(tok, ltok, "fdbdebg") == 0) {
        extern int gbl_fdb_track;

        int dbgflag;
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected flag for fdbdebg\n");
            return -1;
        }
        dbgflag = toknum(tok, ltok);
        gbl_fdb_track = dbgflag;
    } else if (tokcmp(tok, ltok, "fdbtrackhints") == 0) {
        extern int gbl_fdb_track_hints;

        int dbgflag;
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected flag for fdbtrackhints\n");
            return -1;
        }
        dbgflag = toknum(tok, ltok);
        gbl_fdb_track_hints = dbgflag;
    } else if (tokcmp(tok, ltok, "fdb") == 0) {
        fdb_process_message(line + st, lline - st);
    }
    /*
     * freelist <tablename> datafile <number> stripe <number>
     * freelist <tablename> index <number>
     * freelist
     */
    else if (tokcmp(tok, ltok, "freelist") == 0) {
        char table[MAXTABLELEN];
        struct ireq iq;
        int datafile = -1;
        int stripe = -1;
        int index = -1;
        void bdb_dump_freelist(FILE * out, int datafile, int stripe, int ixnum,
                               bdb_state_type *bdb_state);
        init_fake_ireq(dbenv, &iq);

        tok = segtok(line, lline, &st, &ltok);
        if (ltok <= 0)
            goto freelisthelp;

        tokcpy0(tok, ltok, table, sizeof(table));
        if (!(iq.usedb = getdbbyname(table))) {
            logmsg(LOGMSG_ERROR, "Couldn't open table '%s'\n", table);
            goto freelisthelp;
        }

        tok = segtok(line, lline, &st, &ltok);
        if (tokcmp(tok, ltok, "datafile") == 0) {
            tok = segtok(line, lline, &st, &ltok);
            datafile = toknum(tok, ltok);

            tok = segtok(line, lline, &st, &ltok);
            if (tokcmp(tok, ltok, "stripe") != 0) {
                logmsg(LOGMSG_ERROR, "Input error (was expecting stripe directive)\n");
                goto freelisthelp;
            }

            tok = segtok(line, lline, &st, &ltok);
            stripe = toknum(tok, ltok);
            bdb_dump_freelist(stdout, datafile, stripe, -1, iq.usedb->handle);
        } else if (tokcmp(tok, ltok, "index") == 0) {
            tok = segtok(line, lline, &st, &ltok);
            index = toknum(tok, ltok);
            bdb_dump_freelist(stdout, -1, -1, index, iq.usedb->handle);
        } else if (tokcmp(tok, ltok, "all") == 0) {
            bdb_dump_freelist(stdout, -1, -1, -1, iq.usedb->handle);
        } else {
            logmsg(LOGMSG_ERROR, "Invalid freelist command\n");
            goto freelisthelp;
        }

        if (0) {
        freelisthelp:
           logmsg(LOGMSG_USER, "Dump freelist of a given btree.\n");
           logmsg(LOGMSG_USER, "freelist <tablename> datafile <number>  stripe <number>\n");
           logmsg(LOGMSG_USER, "freelist <tablename> index    <number>\n");
           logmsg(LOGMSG_USER, "freelist <tablename> all\n");
        }
    }
    /*
       else if (tokcmp(tok,ltok,"convertq")==0)
       {
       convert_freelist_to_queue();
       }
       else if (tokcmp(tok,ltok,"unconvertq")==0)
       {
       convert_freelist_to_btree();
       }
     */
    else if (tokcmp(tok, ltok, "downgrade") == 0) {
        bdb_transfermaster(dbenv->dbs[0]->handle);
    } else if (tokcmp(tok, ltok, "losemaster") == 0) {
        bdb_losemaster(dbenv->dbs[0]->handle);
    } else if (tokcmp(tok, ltok, "upgrade") == 0) {
        char *newmaster = 0;
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected hostname for new master\n");
            return -1;
        }
        tok = tokdup(tok, ltok);
        newmaster = intern(tok);
        free(tok);
        logmsg(LOGMSG_USER, "Trying to transfer master to node %s\n", newmaster);
        bdb_transfermaster_tonode(dbenv->dbs[0]->handle, newmaster);
    } else if (tokcmp(tok, ltok, "synccluster") == 0) {

        int outrc = -1;
        if (thedb->master != gbl_mynode) {
            logmsg(LOGMSG_USER, "Not the master node. \n");
        } else {
            tok = segtok(line, lline, &st, &ltok);
            if (tokcmp(tok, ltok, "full") == 0) {
                outrc = bdb_sync_cluster(thedb->bdb_env, 1);
            } else {
                outrc = bdb_sync_cluster(thedb->bdb_env, 0);
            }
        }
        if (outrc) {
           logmsg(LOGMSG_USER, "sync cluster failed. \n");
        } else {
           logmsg(LOGMSG_USER, "sync cluster done. \n");
        }
    } else if (tokcmp(tok, ltok, "net_lmt_upd_incoherent_nodes") == 0) {
        int num = -1;

        tok = segtok(line, lline, &st, &ltok);
        if (tok && ltok > 0)
            num = toknum(tok, ltok);
        if (num >= 0) {

           logmsg(LOGMSG_USER, "Setting replication update threshold to %d%%\n", num);

            gbl_net_lmt_upd_incoherent_nodes = num;
        } else {
            logmsg(LOGMSG_ERROR, "Incorrect argument; specify the percent of queue "
                            "to be used by replication update!\n");
        }
    } else if (tokcmp(tok, ltok, "enableprefersosql") == 0) {
        if (!gbl_sql_tranlevel_sosql_pref) {
            gbl_sql_tranlevel_sosql_pref = 1;
            /* update bits */
            comdb2_shm_set_flag(thedb->dbnum, CMDB2_SHMFLG_PREFER_SOSQL);
            for (i = 0; i < thedb->num_dbs; i++) {
                if (thedb->dbs[i]->dbnum)
                    comdb2_shm_set_flag(thedb->dbs[i]->dbnum,
                                        CMDB2_SHMFLG_PREFER_SOSQL);
            }
        }
    } else if (tokcmp(tok, ltok, "disableprefersosql") == 0) {
        if (gbl_sql_tranlevel_sosql_pref) {
            gbl_sql_tranlevel_sosql_pref = 0;
            /* update bits */
            comdb2_shm_clr_flag(thedb->dbnum, CMDB2_SHMFLG_PREFER_SOSQL);
            for (i = 0; i < thedb->num_dbs; i++) {
                if (thedb->dbs[i]->dbnum)
                    comdb2_shm_clr_flag(thedb->dbs[i]->dbnum,
                                        CMDB2_SHMFLG_PREFER_SOSQL);
            }
        }
    } else if (tokcmp(tok, ltok, "whohas") == 0) {
        int pgno = -1;

        tok = segtok(line, lline, &st, &ltok);
        if (tok && ltok > 0)
            pgno = toknum(tok, ltok);
        if (pgno >= 0) {
            bdb_check_pageno(thedb->bdb_env, pgno);
        } else
            logmsg(LOGMSG_ERROR, "incorrect page no %d\n", pgno);
    } else if (tokcmp(tok, ltok, "pushnext") == 0) {
        push_next_log();
    }

#if 0
   else if (tokcmp(tok,ltok,"netdon")==0)
   {
      net_direct_write_on(dbenv->handle_sibling);
   }
   else if (tokcmp(tok,ltok,"netdof")==0)
   {
      net_direct_write_off(dbenv->handle_sibling);
   }
   else if (tokcmp(tok,ltok,"netdbg")==0)
   {
      tok=segtok(line,lline,&st,&ltok);
      net_direct_write_trace_lvl(dbenv->handle_sibling, toknum(tok,ltok));
   }
#endif
    else if (tokcmp(tok, ltok, "netpoll") == 0) {
        int pval;
        tok = segtok(line, lline, &st, &ltok);
        pval = toknum(tok, ltok);
        logmsg(LOGMSG_USER, "Setting net accept-poll to %d ms.\n", pval);
        net_set_poll(dbenv->handle_sibling, pval);
    } else if (tokcmp(tok, ltok, "osqlnetpoll") == 0) {
        int pval;
        tok = segtok(line, lline, &st, &ltok);
        pval = toknum(tok, ltok);
        logmsg(LOGMSG_USER, "Setting osql-net accept-poll to %d ms.\n", pval);
        osql_set_net_poll(pval);
    } else if (tokcmp(tok, ltok, "exitalarmsec") == 0) {
        int alarmsec;
        tok = segtok(line, lline, &st, &ltok);
        alarmsec = toknum(tok, ltok);
        logmsg(LOGMSG_USER, "Setting exit alarm to %d seconds\n", alarmsec);
        gbl_exit_alarm_sec = alarmsec;
    }

#if defined ENABLE_COHERENCY_TEST_TRAPS
    else if (tokcmp(tok, ltok, "incoherent") == 0) {
        int node;
        int outrc;

        tok = segtok(line, lline, &st, &ltok);
        node = toknum(tok, ltok);
        send_notcoherent_current_lsn(node, 1, &outrc);
    }

    else if (tokcmp(tok, ltok, "coherent") == 0) {
        int node;
        int outrc;

        tok = segtok(line, lline, &st, &ltok);
        node = toknum(tok, ltok);
        send_notcoherent_current_lsn(node, 0, &outrc);
    }
#endif

#if defined SET_GBLCONTEXT_TEST_TRAPS
    else if (tokcmp(tok, ltok, "setcontext") == 0) {
        unsigned long long ctxt;
        extern void set_gblcontext(void *bdb_state,
                                   unsigned long long gblcontext);
        tok = segtok(line, lline, &st, &ltok);
        ctxt = strtoull(tok, NULL, 0);
        set_gblcontext(thedb->bdb_env, ctxt);
    }
#endif

    else if (tokcmp(tok, ltok, "ioalarm") == 0) {
        int num;

        tok = segtok(line, lline, &st, &ltok);
        num = toknum(tok, ltok);
        logmsg(LOGMSG_USER, "setting ioalarm to %d\n", num);

        __berkdb_write_alarm_ms = num;
        __berkdb_read_alarm_ms = num;

        berk_write_alarm_ms(__berkdb_write_alarm_ms);
        berk_read_alarm_ms(__berkdb_read_alarm_ms);

    } else if (tokcmp(tok, ltok, "memp_sync_alarm") == 0) {
        int num;

        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "expected #ms for memp_sync_alarm");
            return -1;
        }
        num = toknum(tok, ltok);
        logmsg(LOGMSG_USER, "setting memp_sync alarm to %d\n", num);
        berk_memp_sync_alarm_ms(num);
    } else if (tokcmp(tok, ltok, "disable_pageorder_recsz_check") == 0) {
        logmsg(LOGMSG_USER, "Disabled pageorder records per page check\n");
        bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_DISABLE_PAGEORDER_RECSZ_CHK, 1);
    } else if (tokcmp(tok, ltok, "enable_pageorder_recsz_check") == 0) {
        logmsg(LOGMSG_USER, "Enabled pageorder records per page check\n");
        bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_DISABLE_PAGEORDER_RECSZ_CHK, 0);
    }

    else if (tokcmp(tok, ltok, "simulate_rowlock_deadlock") == 0) {
        int num;
        tok = segtok(line, sizeof(line), &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Need to specify rowlock deadlock interval.\n");
            return -1;
        }
        num = toknum(tok, ltok);
        if (num == 0) {
            logmsg(LOGMSG_USER, "Disabling rowlock_deadlock simulator.\n");
            gbl_simulate_rowlock_deadlock_interval = 0;
        } else if (num < 2) {
            logmsg(LOGMSG_ERROR, "Invalid rowlock_deadlock interval.\n");
        } else {
            logmsg(LOGMSG_USER, "Will throw a rowlock deadlock every %d tries.\n",
                    num);
            gbl_simulate_rowlock_deadlock_interval = num;
        }
    } else if (tokcmp(tok, ltok, "debug_rowlocks") == 0) {
        if (gbl_debug_rowlocks) {
           logmsg(LOGMSG_USER, "Debug-rowlocks flag is already enabled.\n");
        } else {
            gbl_debug_rowlocks = 1;
           logmsg(LOGMSG_USER, "Enabled debug rowlocks flag.\n");
        }
    } else if (tokcmp(tok, ltok, "nodebug_rowlocks") == 0) {
        if (!gbl_debug_rowlocks) {
            logmsg(LOGMSG_USER, "Debug-rowlocks flag is already disabled.\n");
        } else {
            gbl_debug_rowlocks = 0;
            logmsg(LOGMSG_USER, "Disabled debug rowlocks flag.\n");
        }
    } else if (tokcmp(tok, ltok, "disable_tagged_api") == 0) {
        logmsg(LOGMSG_USER, "Disabled tagged api requests.\n");
        gbl_disable_tagged_api = 1;
    } else if (tokcmp(tok, ltok, "enable_tagged_api") == 0) {
        logmsg(LOGMSG_USER, "Enabled tagged api requests.\n");
        gbl_disable_tagged_api = 0;
    } else if (tokcmp(tok, ltok, "disable_selectv_range_check") == 0) {
        logmsg(LOGMSG_USER, "Disabled selectv range check.\n");
        gbl_selectv_rangechk = 0;
    } else if (tokcmp(tok, ltok, "enable_selectv_range_check") == 0) {
        logmsg(LOGMSG_USER, "Enabled selectv range check.\n");
        gbl_selectv_rangechk = 1;
    } else if (tokcmp(tok, ltok, "get_newsi_status") == 0) {
       logmsg(LOGMSG_USER, "new snapshot is %s; new snapshot logging is %s; new snapshot "
               "as-of is %s\n",
               gbl_new_snapisol ? "ENABLED" : "DISABLED",
               gbl_new_snapisol_logging ? "ENABLED" : "DISABLED",
               gbl_new_snapisol_asof ? "ENABLED" : "DISABLED");
        bdb_osql_trn_clients_status();
    } else if (tokcmp(tok, ltok, "stack_warn_threshold") == 0) {
        int thresh;
        tok = segtok(line, lline, &st, &ltok);
        if (ltok > 0 && (thresh = toknum(tok, ltok)) >= 0) {
            walkback_set_warnthresh(thresh);
            logmsg(LOGMSG_USER, 
                    "Set walkback warn-threshold to %d walkbacks per second\n",
                    thresh);
        } else {
            thresh = walkback_get_warnthresh();
            if (thresh > 0) {
                logmsg(LOGMSG_USER, 
                        "Warn for %d or more walkbacks in the past second\n",
                        thresh);
            } else {
                logmsg(LOGMSG_USER, "Walkback warning is disabled\n");
            }
        }
    } else if (tokcmp(tok, ltok, "stack_disable") == 0) {
        walkback_disable();
        logmsg(LOGMSG_USER, "Disabled walkbacks\n");
    } else if (tokcmp(tok, ltok, "stack_enable") == 0) {
        walkback_enable();
        logmsg(LOGMSG_USER, "Enabled walkbacks\n");
    } else if (tokcmp(tok, ltok, "disable_overflow_page_trace") == 0) {
        if (gbl_disable_overflow_page_trace) {
           logmsg(LOGMSG_USER, "Overflow page trace is not enabled\n");
        } else {
            gbl_disable_overflow_page_trace = 1;
           logmsg(LOGMSG_USER, "Disabled berkdb overflow page trace.\n");
        }
    } else if (tokcmp(tok, ltok, "enable_overflow_page_trace") == 0) {
        if (!gbl_disable_overflow_page_trace) {
            logmsg(LOGMSG_USER, "Overflow page trace is already enabled\n");
        } else {
            gbl_disable_overflow_page_trace = 0;
           logmsg(LOGMSG_USER, "Enabled berkdb overflow page trace.\n");
        }
    } else if (tokcmp(tok, ltok, "pageordertrace") == 0) {
        if (gbl_enable_pageorder_trace) {
           logmsg(LOGMSG_USER, "pageorder trace already on\n");
        } else {
            gbl_enable_pageorder_trace = 1;
           logmsg(LOGMSG_USER, "pageorder trace enabled\n");
        }
    } else if (tokcmp(tok, ltok, "nopageordertrace") == 0) {
        if (!gbl_enable_pageorder_trace) {
           logmsg(LOGMSG_USER, "pageorder trace already off\n");
        } else {
            gbl_enable_pageorder_trace = 0;
           logmsg(LOGMSG_USER, "pageorder trace disabled\n");
        }
    }

    else if (tokcmp(tok, ltok, "blocksqlthrottle") == 0) {
        int num;

        tok = segtok(line, lline, &st, &ltok);
        num = toknum(tok, ltok);
        logmsg(LOGMSG_USER, "setting blocksql maximmum concurrent in-transaction "
                        "sessions to %d\n",
                num);

        osql_bplog_setlimit(num);
    }

    else if (tokcmp(tok, ltok, "purge") == 0) {
        /* maintenance command: purge records/ index items/ blobs by genid
           These will typically be reported by e.g. the verify feature. */

        char table[MAXTABLELEN];
        struct db *db;

        /* expect table first */
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "expected table name first\n");
            return -1;
        }
        tokcpy0(tok, ltok, table, sizeof(table));
        db = getdbbyname(table);
        if (!db) {
            logmsg(LOGMSG_ERROR, "unknown table '%s'\n", table);
            return -1;
        }

        tok = segtok(line, lline, &st, &ltok);
        while (ltok > 0) {
            char genid_c[20];
            unsigned long long genid;
            tokcpy0(tok, ltok, genid_c, sizeof(genid_c));
            if (!strcmp(genid_c, "auto")) {
               logmsg(LOGMSG_FATAL, "AUTO PURGE\n");
                /* purge the first few genids we find.
                   Used for testing purposes. */
                purge_by_genid(db, NULL);
            } else {
                genid = strtoull(genid_c, NULL, 0);
               logmsg(LOGMSG_USER, "purge genid: 0x%llx\n", genid);
                purge_by_genid(db, &genid);
            }
            tok = segtok(line, lline, &st, &ltok);
        }
    } else if (tokcmp(tok, ltok, "pfrmtof") == 0) {
        if (!gbl_pfaultrmt) {
           logmsg(LOGMSG_USER, "remote pfault already switched off for dtastripe\n");
        } else {
            gbl_pfaultrmt = 0;
           logmsg(LOGMSG_USER, "remote pfault turned off for dtastripe\n");
        }

    } else if (tokcmp(tok, ltok, "disable_pageorder_recsz_check") == 0) {
       logmsg(LOGMSG_USER, "Disabled pageorder records per page check\n");
        bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_DISABLE_PAGEORDER_RECSZ_CHK, 1);
    } else if (tokcmp(tok, ltok, "enable_pageorder_recsz_check") == 0) {
       logmsg(LOGMSG_USER, "Enabled pageorder records per page check\n");
        bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_DISABLE_PAGEORDER_RECSZ_CHK, 0);
    } else if (tokcmp(tok, ltok, "disable_overflow_page_trace") == 0) {
        if (gbl_disable_overflow_page_trace) {
           logmsg(LOGMSG_USER, "Overflow page trace is not enabled\n");
        } else {
            gbl_disable_overflow_page_trace = 1;
           logmsg(LOGMSG_USER, "Disabled berkdb overflow page trace.\n");
        }
    } else if (tokcmp(tok, ltok, "enable_overflow_page_trace") == 0) {
        if (!gbl_disable_overflow_page_trace) {
           logmsg(LOGMSG_USER, "Overflow page trace is already enabled\n");
        } else {
            gbl_disable_overflow_page_trace = 0;
           logmsg(LOGMSG_USER, "Enabled berkdb overflow page trace.\n");
        }
    } else if (tokcmp(tok, ltok, "disable_osql_prefault") == 0) {
        if (!gbl_osqlpfault_threads) {
           logmsg(LOGMSG_USER, "Osql io prefault is already disabled\n");
        } else {
            gbl_osqlpfault_threads = 0;
           logmsg(LOGMSG_USER, "Disabled osql io prefault.\n");
        }
    } else if (tokcmp(tok, ltok, "enable_osql_prefault") == 0) {
        if (gbl_osqlpfault_threads) {
           logmsg(LOGMSG_USER, "Osql io prefault is already enabled\n");
        } else {
            if (!thdpool_get_maxthds(gbl_osqlpfault_thdpool)) {
                logmsg(LOGMSG_ERROR, "Please set max threads for osqlpfaultpool first\n");
                logmsg(LOGMSG_ERROR, "Osql io prefault is NOT enabled\n");
            } else {
                gbl_osqlpfault_threads = 1;
               logmsg(LOGMSG_USER, "Enabled osql io prefault.\n");
            }
        }
    } else if (tokcmp(tok, ltok, "get_osql_prefault_status") == 0) {
        if (gbl_osqlpfault_threads) {
           logmsg(LOGMSG_USER, "Osql io prefault is ENABLED\n");
        } else {
           logmsg(LOGMSG_USER, "Osql io prefault is DISABLED\n");
        }
        thdpool_print_stats(stdout, gbl_osqlpfault_thdpool);
    } else if (tokcmp(tok, ltok, "enable_prefault_udp") == 0) {
        if (gbl_prefault_udp) {
           logmsg(LOGMSG_USER, "prefault upd was already enabled on this node\n");
        } else {
            gbl_prefault_udp = 1;
           logmsg(LOGMSG_USER, "Enabled prefault upd on this node\n");
        }
    } else if (tokcmp(tok, ltok, "disable_prefault_udp") == 0) {
        if (!gbl_prefault_udp) {
           logmsg(LOGMSG_USER, "prefault upd was disabled on this node\n");
        } else {
            gbl_prefault_udp = 0;
           logmsg(LOGMSG_USER, "Disabled prefault upd on this node\n");
        }
    } else if (tokcmp(tok, ltok, "set_udp_prefault_latency") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (ltok) {
            gbl_prefault_latency = toknum(tok, ltok);
           logmsg(LOGMSG_USER, "set prefault latency to %d seconds\n",
                   gbl_prefault_latency);
        } else {
           logmsg(LOGMSG_USER, "set_udp_prefault_latency requires a number argument\n");
        }
    } else if (tokcmp(tok, ltok, "get_udp_prefault_status") == 0) {
        if (gbl_prefault_udp) {
           logmsg(LOGMSG_USER, "prefault upd was enabled on this node\n");
        } else {
           logmsg(LOGMSG_USER, "prefault upd was disabled on this node\n");
        }
        thdpool_print_stats(stdout, gbl_udppfault_thdpool);
    } else if (tokcmp(tok, ltok, "page_compact_target_ff") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        gbl_pg_compact_target_ff =
            (ltok <= 0) ? 0.693 : (toknumd(tok, ltok) / 100.0F);
        logmsg(LOGMSG_USER, "set page compact target fill ratio to %.2f%%\n",
               gbl_pg_compact_target_ff * 100);
    } else if (tokcmp(tok, ltok, "page_compact_thresh_ff") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        double tmpthresh =
            (ltok <= 0) ? 0.346 : (toknumd(tok, ltok) / 100.0F);
        if (gbl_pg_compact_thresh <= 0 && tmpthresh > 0)
            logmsg(LOGMSG_ERROR, "Change requires enabling page compaction in the LRL.\n");
        else {
            gbl_pg_compact_thresh = tmpthresh;
            logmsg(LOGMSG_USER, "set page compact fill ratio threshold to %.2f%%\n",
                    gbl_pg_compact_thresh * 100);
        }
    } else if (tokcmp(line, ltok, "max_num_compact_pages_per_txn") == 0) {
        tok = segtok(line, sizeof(line), &st, &ltok);
        if (ltok <= 0)
            logmsg(LOGMSG_ERROR, "Expected # for max_num_compact_pages_per_txn.\n");
        else
            gbl_max_num_compact_pages_per_txn = (unsigned int)toknum(tok, ltok);
    } else if (tokcmp(tok, ltok, "get_page_compact_status") == 0) {
        if (gbl_pg_compact_thresh > 0) {
           logmsg(LOGMSG_USER, "Page compact enabled. Thresh %.2f%%. Target %.2f%%.\n",
                   gbl_pg_compact_thresh * 100, gbl_pg_compact_target_ff * 100);
        } else {
           logmsg(LOGMSG_USER, "Page compact is disabled.\n");
        }
        thdpool_print_stats(stdout, gbl_pgcompact_thdpool);
    } else if (tokcmp(tok, ltok, "update_shadows_interval") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (ltok) {
            gbl_update_shadows_interval = toknum(tok, ltok);
           logmsg(LOGMSG_USER, "setting update_shadows_interval to %d\n",
                   gbl_update_shadows_interval);
        } else {
           logmsg(LOGMSG_USER, "update_shadows_interval requires an argument\n");
        }
    } else if (tokcmp(tok, ltok, "pageordertrace") == 0) {
        if (gbl_enable_pageorder_trace) {
           logmsg(LOGMSG_USER, "pageorder trace already on\n");
        } else {
            gbl_enable_pageorder_trace = 1;
           logmsg(LOGMSG_USER, "pageorder trace enabled\n");
        }
    } else if (tokcmp(tok, ltok, "nopageordertrace") == 0) {
        if (!gbl_enable_pageorder_trace) {
           logmsg(LOGMSG_USER, "pageorder trace already off\n");
        } else {
            gbl_enable_pageorder_trace = 0;
           logmsg(LOGMSG_USER, "pageorder trace disabled\n");
        }
    } else if (tokcmp(tok, ltok, "deadlkon") == 0) {
        if (gbl_disable_deadlock_trace) {
            logmsg(LOGMSG_USER, "deadlock report already on\n");
        } else {
            gbl_disable_deadlock_trace = 1;
           logmsg(LOGMSG_USER, "deadlock report turned on\n");
        }
    } else if (tokcmp(tok, ltok, "deadlkoff") == 0) {
        if (!gbl_disable_deadlock_trace) {
           logmsg(LOGMSG_USER, "deadlock report already off\n");
        } else {
            gbl_disable_deadlock_trace = 0;
           logmsg(LOGMSG_USER, "deadlock report turned off\n");
        }
    } else if (tokcmp(tok, ltok, "delfiles") == 0) {
        char table[MAXTABLELEN];
        int rc;
        int bdberr;
        struct db *db;

        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "delfiles: error must provide table name\n");
            return -1;
        }

        if (thedb->master != gbl_mynode) {
            logmsg(LOGMSG_ERROR, "Can't delete files: I am not master\n");
            return -1;
        }

        tokcpy(tok, ltok, table);

        db = getdbbyname(table);
        if (!db) {
            logmsg(LOGMSG_ERROR, "delfiles: could not find table: %s\n", table);
            return -1;
        }

       logmsg(LOGMSG_USER, "will attempt to delete unused files for %s\n", table);

        rc = bdb_del_unused_files(db->handle, &bdberr);
        if (rc != 0) {
            logmsg(LOGMSG_ERROR, "delfiles: errors deleting files\n");
            return -1;
        }

       logmsg(LOGMSG_USER, "successfully deleted files\n");
    }

    /* Temporary message-trap to delete the stale backup stats from llmeta. */
    else if (tokcmp(tok, ltok, "delstalestats") == 0) {
        SBUF2 *sb = sbuf2open(fileno(stdout), 0);
        cleanup_stats(sb);
    } else if (tokcmp(tok, ltok, "mallocstats") == 0) {
#ifdef _LINUX_SOURCE
        /* This is defined in malloc.h, as is struct mallinfo.  Including
         * malloc.h
         * causes a clash between mallinfo there and in dlmalloc.h. */
        dlmalloc_stats();
#endif
    } else if (tokcmp(tok, ltok, "deletehints") == 0) {
        reinit_sql_hint_table();
    } else if (tokcmp(tok, ltok, "scon") == 0) {
        if (gbl_report) {
           logmsg(LOGMSG_USER, "request report already on\n");
        } else {
            gbl_report = 1; /* update rate to log */
            gbl_report_last = time_epochms();
            gbl_report_last_n = n_qtrap;
            logmsg(LOGMSG_USER, "request report turned on\n");
        }
    } else if (tokcmp(tok, ltok, "scof") == 0) {
        if (gbl_report) {
           logmsg(LOGMSG_USER, "request report turned off\n");
        } else {
            logmsg(LOGMSG_USER, "request report already off\n");
        }
        gbl_report = 0; /* update rate to log */
    } else if (tokcmp(tok, ltok, "erron") == 0) {
        if (dbenv->errstaton) {
           logmsg(LOGMSG_USER, "db error report already on\n");
        } else {
            dbenv->errstaton = 1; /* ON */
           logmsg(LOGMSG_USER, "db error report turned on\n");
        }
    } else if (tokcmp(tok, ltok, "erroff") == 0) {
        if (!dbenv->errstaton) {
           logmsg(LOGMSG_USER, "db error report already off\n");
        } else {
            dbenv->errstaton = 0; /* OFF */
           logmsg(LOGMSG_USER, "db error report turned off\n");
        }

    } else if (tokcmp(tok, ltok, "resource") == 0) {
        char *name = NULL;
        char *file = NULL;
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected resource name\n");
            return -1;
        }
        name = tokdup(tok, ltok);
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected resource file path\n");
            free(name);
            return -1;
        }
        file = tokdup(tok, ltok);
        addresource(name, file);
        free(name);
        free(file);
    }

    else if (tokcmp(tok, ltok, "inplace") == 0) {
        logmsg(LOGMSG_USER, "gbl_upd_key: %d\n", gbl_upd_key);
    }

    else if (tokcmp(tok, ltok, "queue") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (tokcmp(tok, ltok, "help") == 0) {
            print_help_page(HELP_QUEUES);
        } else if (tokcmp(tok, ltok, "gooserates") == 0) {
            unsigned new_add_rate = gbl_goose_add_rate;
            unsigned new_consume_rate = gbl_goose_consume_rate;
            logmsg(LOGMSG_USER, "gbl_goose_add_rate = %u\n", gbl_goose_add_rate);
            logmsg(LOGMSG_USER, "gbl_goose_consume_rate = %u\n", gbl_goose_consume_rate);
            tok = segtok(line, lline, &st, &ltok);
            if (ltok) {
                new_add_rate = toknum(tok, ltok);
                tok = segtok(line, lline, &st, &ltok);
                if (ltok) {
                    new_consume_rate = toknum(tok, ltok);
                    gbl_goose_add_rate = new_add_rate;
                    gbl_goose_consume_rate = new_consume_rate;
                    logmsg(LOGMSG_USER, "new gbl_goose_add_rate = %u\n", gbl_goose_add_rate);
                    logmsg(LOGMSG_USER, "new gbl_goose_consume_rate = %u\n",
                           gbl_goose_consume_rate);
                }
            }
        } else if (tokcmp(tok, ltok, "poll") == 0) {
            tok = segtok(line, lline, &st, &ltok);
            gbl_queue_sleeptime = toknum(tok, ltok);
            logmsg(LOGMSG_USER, "setting gbl_queue_sleeptime to %d\n", gbl_queue_sleeptime);
        } else if (tokcmp(tok, ltok, "wake") == 0) {
            logmsg(LOGMSG_USER, "Waking all consumers on all queues\n");
            dbqueue_wake_all_consumers_all_queues(thedb, 1);
        } else if (tokcmp(tok, ltok, "debg") == 0) {
            tok = segtok(line, lline, &st, &ltok);
            gbl_queue_debug = toknum(tok, ltok);
            logmsg(LOGMSG_USER, "set full consumer debugging to %d\n", gbl_queue_debug);
        } else if (tokcmp(tok, ltok, "dump") == 0) {
            tok = segtok(line, lline, &st, &ltok);
            if (tok == 0) {
                logmsg(LOGMSG_USER, "expected queue name\n");
            } else {
                char *name = tokdup(tok, ltok);
                struct db *db = getqueuebyname(name);
                if (!db)
                    logmsg(LOGMSG_USER, "no queue named '%s'\n", name);
                else {
                    tok = segtok(line, lline, &st, &ltok);
                    if (tok == 0)
                        logmsg(LOGMSG_USER, "expected file name\n");
                    else {
                        char *file = tokdup(tok, ltok);
                        FILE *fh;
                        fh = fopen(file, "w");
                        if (!fh)
                            logmsg(LOGMSG_ERROR, "cannot open %s for writing: %s\n", file,
                                   strerror(errno));
                        else {
                            logmsg(LOGMSG_USER, "Dumping queue %s to file %s...\n", name,
                                    file);
                            dbq_dump(db, fh);
                            logmsg(LOGMSG_USER, "...finished\n");
                            fclose(fh);
                        }
                        free(file);
                    }
                }
                free(name);
            }
        } else if (tokcmp(tok, ltok, "goose") == 0) {
            tok = segtok(line, lline, &st, &ltok);
            if (ltok == 0) {
                logmsg(LOGMSG_ERROR, "goose which queue?\n");
            } else {
                /* stat on named queue */
                char *name = tokdup(tok, ltok);
                struct db *db = getqueuebyname(name);
                if (!db)
                    logmsg(LOGMSG_ERROR, "no queue named '%s'\n", name);
                else {
                    dbqueue_goose(db, 1);
                    logmsg(LOGMSG_USER, "goosed queue %s\n", db->dbname);
                }
                free(name);
            }
        } else if (tokcmp(tok, ltok, "stat") == 0) {
        queue_stat:
            tok = segtok(line, lline, &st, &ltok);
            if (ltok == 0) {
                /* stat on all queues */
                int ii;
                logmsg(LOGMSG_USER, "database has %d queues\n", thedb->num_qdbs);
                for (ii = 0; ii < thedb->num_qdbs; ii++)
                    dbqueue_stat(thedb->qdbs[ii], 0, 0, 0);
            } else {
                /* stat on named queue */
                char *name = tokdup(tok, ltok);
                struct db *db = getqueuebyname(name);
                if (!db)
                    logmsg(LOGMSG_ERROR, "no queue named '%s'\n", name);
                else
                    dbqueue_stat(db, 0, 0, 0);
                free(name);
            }
        } else if (tokcmp(tok, ltok, "slowstat") == 0) {
            tok = segtok(line, lline, &st, &ltok);
            if (ltok == 0) {
                /* stat on all queues */
                int ii;
                logmsg(LOGMSG_USER, "database has %d queues\n", thedb->num_qdbs);
                for (ii = 0; ii < thedb->num_qdbs; ii++)
                    dbqueue_stat(thedb->qdbs[ii], 1, 0, 0);
            } else {
                /* stat on named queue */
                char *name = tokdup(tok, ltok);
                struct db *db = getqueuebyname(name);
                if (!db)
                    logmsg(LOGMSG_ERROR, "no queue named '%s'\n", name);
                else
                    dbqueue_stat(db, 1, 0, 0);
                free(name);
            }
        } else if (tokcmp(tok, ltok, "countstat") == 0) {
            tok = segtok(line, lline, &st, &ltok);
            if (ltok == 0) {
                /* stat on all queues */
                int ii;
                logmsg(LOGMSG_USER, "database has %d queues\n", thedb->num_qdbs);
                for (ii = 0; ii < thedb->num_qdbs; ii++)
                    dbqueue_stat(thedb->qdbs[ii], 1, 1, 0);
            } else {
                /* stat on named queue */
                char *name = tokdup(tok, ltok);
                struct db *db = getqueuebyname(name);
                if (!db)
                    logmsg(LOGMSG_ERROR, "no queue named '%s'\n", name);
                else
                    dbqueue_stat(db, 1, 1, 0);
                free(name);
            }
        }

        else if (tokcmp(tok, ltok, "flush") == 0) {
            char *qname;
            int consumern;
            struct db *db;

            tok = segtok(line, lline, &st, &ltok);
            if (tokcmp(tok, ltok, "abort") == 0) {
                dbqueue_flush_abort();
            } else {
                if (ltok == 0) {
                    logmsg(LOGMSG_ERROR, "expected a queue name\n");
                    return -1;
                }
                qname = tokdup(tok, ltok);

                tok = segtok(line, lline, &st, &ltok);
                if (ltok == 0) {
                    logmsg(LOGMSG_ERROR, "expected a consumer number\n");
                    free(qname);
                    return -1;
                }
                consumern = toknum(tok, ltok);

                db = getqueuebyname(qname);
                if (!db)
                    logmsg(LOGMSG_ERROR, "no queue named '%s'\n", qname);
                else
                    dbqueue_flush_in_thread(db, consumern);
                free(qname);
            }
        } else if (tokcmp(tok, ltok, "create") == 0) {
            char qname[64];
            int avgitem;
            int pagesize = 0;

            tok = segtok(line, lline, &st, &ltok);
            if (ltok == 0) {
                logmsg(LOGMSG_ERROR, "expected a queue name\n");
                return -1;
            }
            tokcpy0(tok, ltok, qname, sizeof(qname));

            tok = segtok(line, lline, &st, &ltok);
            if (ltok == 0) {
                logmsg(LOGMSG_ERROR, "expected average item size in bytes\n");
                return -1;
            }
            avgitem = toknum(tok, ltok);
            if (avgitem <= 0) {
                logmsg(LOGMSG_ERROR, "expected average item size illegal\n");
                return -1;
            }

            /* This code is dupliated in the lrl parser.. sorry */
            tok = segtok(line, lline, &st, &ltok);
            while (ltok) {
                char ctok[64];
                tokcpy0(tok, ltok, ctok, sizeof(ctok));
                if (strncmp(ctok, "pagesize=", 9) == 0) {
                    pagesize = atoi(ctok + 9);
                } else {
                    logmsg(LOGMSG_ERROR, "Bad queue attribute '%s'\n", ctok);
                    return -1;
                }
                tok = segtok(line, lline, &st, &ltok);
            }

            /* create a new queue by the name. */
            create_queue(thedb, qname, avgitem, pagesize, 0);
        } else if (tokcmp(tok, ltok, "setopt") == 0) {
            char *qname;
            int consumern;
            char *opts;
            struct db *db;

            tok = segtok(line, lline, &st, &ltok);
            if (ltok == 0) {
                logmsg(LOGMSG_ERROR, "expected a queue name\n");
                return -1;
            }
            qname = tokdup(tok, ltok);

            tok = segtok(line, lline, &st, &ltok);
            if (ltok == 0) {
                logmsg(LOGMSG_ERROR, "expected a consumer number\n");
                free(qname);
                return -1;
            }
            consumern = toknum(tok, ltok);

            db = getqueuebyname(qname);
            if (!db) {
                logmsg(LOGMSG_ERROR, "no queue called '%s'\n", qname);
                free(qname);
                return -1;
            }

            tok = segtok(line, lline, &st, &ltok);
            if (ltok == 0) {
                logmsg(LOGMSG_ERROR, "expected queue options\n");
                free(qname);
                return -1;
            }
            opts = tokdup(tok, ltok);

            /* add/remove a consumer */
            dbqueue_set_consumern_options(db, consumern, opts);

            free(qname);
            free(opts);
        } else if (tokcmp(tok, ltok, "consumer") == 0) {
            char *qname;
            int consumern;
            char *method;

            tok = segtok(line, lline, &st, &ltok);
            if (ltok == 0) {
                logmsg(LOGMSG_ERROR, "expected a queue name\n");
                return -1;
            }
            qname = tokdup(tok, ltok);

            tok = segtok(line, lline, &st, &ltok);
            if (ltok == 0) {
                logmsg(LOGMSG_ERROR, "expected a consumer number\n");
                free(qname);
                return -1;
            }
            consumern = toknum(tok, ltok);

            tok = segtok(line, lline, &st, &ltok);
            if (ltok == 0) {
                logmsg(LOGMSG_ERROR, "expected a method\n");
                free(qname);
                return -1;
            }
            method = tokdup(tok, ltok);

            /* add/remove a consumer */
            consumer_change(qname, consumern, method);

            free(qname);
            free(method);
        } else {
            logmsg(LOGMSG_ERROR, "unknown queue command <%.*s>\n", ltok, tok);
        }
    }

    else if (tokcmp(tok, ltok, "netlock") == 0) {
        int secs;

        tok = segtok(line, lline, &st, &ltok);
        secs = toknum(tok, ltok);

        netinfo_lock(thedb->handle_sibling, secs);
    } else if (tokcmp(tok, ltok, "java") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (tokcmp(tok, ltok, "help") == 0) {
            print_help_page(HELP_JAVA);
        } else if (tokcmp(tok, ltok, "stat") == 0) {
            char *tail;
            tail = tokdup(line + st, lline - st);
            javasp_stat(tail);
            free(tail);
        } else if (tokcmp(tok, ltok, "unload") == 0) {
            char *name;
            int rc;

            tok = segtok(line, lline, &st, &ltok);
            if (ltok == 0) {
                logmsg(LOGMSG_ERROR, "expected name of stored procedure to unload\n");
                return -1;
            }
            name = tokdup(tok, ltok);

            rc = javasp_unload_procedure(name);

            if (rc == 0)
                logmsg(LOGMSG_ERROR, "unloaded stored procedure '%s': SUCCESS\n", name);
            else
                logmsg(LOGMSG_ERROR, "could not unload stored procedure '%s'\n", name);

            free(name);
        } else if (tokcmp(tok, ltok, "load") == 0 ||
                   tokcmp(tok, ltok, "reload") == 0) {
            char *opname = tokdup(tok, ltok);
            char *name;
            char *jarres;
            char *param;
            const char *jarfile;

            tok = segtok(line, lline, &st, &ltok);
            if (ltok == 0) {
                logmsg(LOGMSG_ERROR, "expected name of stored procedure to %s\n", opname);
                free(opname);
                return -1;
            }
            name = tokdup(tok, ltok);

            tok = segtok(line, lline, &st, &ltok);
            if (ltok == 0) {
                logmsg(LOGMSG_ERROR, "expected name of jar resource\n");
                free(name);
                free(opname);
                return -1;
            }
            jarres = tokdup(tok, ltok);

            tok = seglinel(line, lline, &st, &ltok);
            param = tokdup(tok, ltok);

            jarfile = getresourcepath(jarres);
            if (!jarfile) {
                logmsg(LOGMSG_ERROR, "resource '%s' not found\n", jarres);
            } else {
                int rc;
                if (strcasecmp(opname, "reload") == 0)
                    rc = javasp_reload_procedure(name, jarfile, param);
                else
                    rc = javasp_load_procedure(name, jarfile, param);

                if (rc == 0)
                    logmsg(LOGMSG_USER, "%sed stored procedure '%s': SUCCESS\n", opname,
                            name);
                else
                    logmsg(LOGMSG_ERROR, "%sing stored procedure '%s' FAILED\n", opname,
                            name);
            }

            free(name);
            free(opname);
            free(jarres);
            free(param);
        } else {
            logmsg(LOGMSG_ERROR, "unknown java command <%.*s>\n", ltok, tok);
        }
    } else if (tokcmp(tok, ltok, "diag") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (tokcmp(tok, ltok, "help") == 0) {
            logmsg(LOGMSG_USER, "diag rrn <tablename> #  - dump record with given rrn\n");
            logmsg(LOGMSG_USER, "diag dump <tablename> # - dump .dta# file of table\n");
        } else if (tokcmp(tok, ltok, "dump") == 0) {
            char table[MAXTABLELEN];
            int dtanum;
            struct db *db;
            tok = segtok(line, lline, &st, &ltok);
            if (ltok == 0) {
                logmsg(LOGMSG_ERROR, "Expected table\n");
                return -1;
            }
            if (ltok >= MAXTABLELEN) {
                logmsg(LOGMSG_ERROR, "Invalid table name: too long (max %d)\n", MAXTABLELEN);
                return -1;
            }
            tokcpy(tok, ltok, table);
            tok = segtok(line, lline, &st, &ltok);
            if (ltok == 0) {
                logmsg(LOGMSG_ERROR, "Expected dtanum\n");
                return -1;
            }
            dtanum = toknum(tok, ltok);

            db = getdbbyname(table);
            if (!db) {
                logmsg(LOGMSG_ERROR, "Invalid table %s\n", table);
            } else {
                diagnostics_dump_dta(db, dtanum);
            }
        } else {
            logmsg(LOGMSG_ERROR, "unknown diag command <%.*s>\n", ltok, tok);
        }
    } else if (tokcmp(tok, ltok, "reset_blkmax") == 0) {
        extern int reset_blkmax(void);
        reset_blkmax();
        logmsg(LOGMSG_USER, "Reset blkmax\n");
    }

    else if (tokcmp(tok, ltok, "get_blkmax") == 0) {
        extern int get_blkmax(void);
        int blkmax = get_blkmax();
        logmsg(LOGMSG_USER, 
                "Maximum concurrent block-processor threads is %d, maxwt is %d\n",
                blkmax, gbl_maxwthreads);
    }

    /*
       pagesize set <tablename> <data|blob|index> <pagesize>
       pagesize specified in bytes: 65536 for 64K pages

       not implemented:
       pagesize get <tablename> <data|blob|index>
       pagesize setall <data|blob|index> <pagesize>
       pagesize getall <data|blob|index> <pagesize>
     */
    else if (tokcmp(tok, ltok, "pagesize") == 0) {
        char table[MAXTABLELEN];
        int n;
        int rc;
        int bdberr;
        struct db *db;
        const char *which = NULL;

        tok = segtok(line, lline, &st, &ltok);

        if (tokcmp(tok, ltok, "set") == 0) {
            tok = segtok(line, lline, &st, &ltok);
            tokcpy(tok, ltok, table);

            db = getdbbyname(table);
            if (db == NULL) {
                goto pagesize_usage;
            }

            tok = segtok(line, lline, &st, &ltok);
            if (tokcmp(tok, ltok, "data") == 0) {
                which = "data";
                tok = segtok(line, lline, &st, &ltok);
                if (comdb2_toknum(tok, ltok, &n)) {
                    goto pagesize_usage;
                }
                rc = bdb_set_pagesize_data(db->handle, NULL, n, &bdberr);
            }

            else if (tokcmp(tok, ltok, "blob") == 0) {
                which = "blob";
                tok = segtok(line, lline, &st, &ltok);
                if (comdb2_toknum(tok, ltok, &n)) {
                    goto pagesize_usage;
                }
                rc = bdb_set_pagesize_blob(db->handle, NULL, n, &bdberr);
            }

            else if (tokcmp(tok, ltok, "index") == 0) {
                which = "index";
                tok = segtok(line, lline, &st, &ltok);
                if (comdb2_toknum(tok, ltok, &n)) {
                    goto pagesize_usage;
                }
                rc = bdb_set_pagesize_index(db->handle, NULL, n, &bdberr);
            } else {
                goto pagesize_usage;
            }
            goto pagesize_done;
        }

    pagesize_usage:
        logmsg(LOGMSG_USER, "pagesize set <tablename> <data|blob|index> <pagesize>\n"
               "<pagesize> specified in bytes: 65536 for 64K pages\n");
        return -1;

    pagesize_done:
       logmsg(LOGMSG_USER, "bdb_set_pagesize %s %d rc: %d\n", which, n, rc);
        return 0;
    }

    else if (tokcmp(tok, ltok, "blob") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (tokcmp(tok, ltok, "help") == 0) {
           logmsg(LOGMSG_USER, "blob stat         - status report\n");
           logmsg(LOGMSG_USER, "blob purge #      - set purge time in seconds\n");
           logmsg(LOGMSG_USER, "blob lose #       - lose cached blobs (debug aid)\n");
        } else if (tokcmp(tok, ltok, "stat") == 0) {
            blob_print_stats();
        } else if (tokcmp(tok, ltok, "purge") == 0) {
            int n;
            tok = segtok(line, lline, &st, &ltok);
            n = toknum(tok, ltok);
            if (n > 0) {
                gbl_blob_maxage = n;
                logmsg(LOGMSG_USER, "set blob purge time to %d seconds\n", gbl_blob_maxage);
            } else
                logmsg(LOGMSG_ERROR, "bad purge time to blob purge command\n");
        } else if (tokcmp(tok, ltok, "lose") == 0) {
            tok = segtok(line, lline, &st, &ltok);
            gbl_blob_lose_debug = toknum(tok, ltok);
            logmsg(LOGMSG_ERROR, "gbl_blob_lose_debug=%d\n", gbl_blob_lose_debug);
        } else {
            logmsg(LOGMSG_ERROR, "unknown blob command <%.*s>\n", ltok, tok);
        }
    } else if (tokcmp(tok, ltok, "stax") == 0) {
        showdbenv(dbenv);
    } else if (tokcmp(tok, ltok, "stal") == 0) {
        thd_stats();
    } else if (tokcmp(tok, ltok, "thr") == 0) {
        thrman_dump();
    } else if (tokcmp(tok, ltok, "thrtrc") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (ltok > 0)
            gbl_thrman_trace = toknum(tok, ltok);
        logmsg(LOGMSG_USER, "gbl_thrman_trace = %d\n", gbl_thrman_trace);
    } else if (tokcmp(tok, ltok, "long") == 0) {
        request_stats(dbenv);
    } else if (tokcmp(tok, ltok, "dmpl") == 0) {
        thd_dump();
    } else if (tokcmp(tok, ltok, "dmptrn") == 0) {
        tran_dump(&dbenv->long_trn_stats);
    } else if (tokcmp(tok, ltok, "dmpcts") == 0) {
        dump_all_constraints(dbenv);
    } else if (tokcmp(tok, ltok, "checkcsc2") == 0) {
        int rc;
        backend_thread_event(thedb, COMDB2_THR_EVENT_DONE_RDONLY);
        backend_thread_event(thedb, COMDB2_THR_EVENT_START_RDWR);
        logmsg(LOGMSG_USER, "checking schemas...\n");
        rc = check_current_schemas();
        logmsg(LOGMSG_USER, "checked schemas, this database is %s\n",
               rc == 0 ? "good" : "bad");
        backend_thread_event(thedb, COMDB2_THR_EVENT_DONE_RDWR);
        backend_thread_event(thedb, COMDB2_THR_EVENT_START_RDONLY);
    }

    else if (tokcmp(tok, ltok, "nowatch") == 0) {
        logmsg(LOGMSG_USER, "disabling watcher thread\n");
        watchdog_disable();
    }

    else if (tokcmp(tok, ltok, "notimeout") == 0) {
        logmsg(LOGMSG_USER, "disabling timeouts\n");
        gbl_notimeouts = 1;
    }

    else if (tokcmp(tok, ltok, "watch") == 0) {
        logmsg(LOGMSG_USER, "enabling watcher thread\n");
        watchdog_enable();
    }

    else if (tokcmp(tok, ltok, "reco") == 0) {
        bdb_set_recovery(dbenv->dbs[0]->handle);
    } else if (tokcmp(tok, ltok, "lua_break") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        int thread_id = toknum(tok, ltok);
        gbl_break_lua = thread_id;
    } else if (tokcmp(tok, ltok, "stat") == 0) {
        /* Sam J - allow us to get much more status from an op1 window by
         * forwarding commands to the bdb backend. */
        tok = segtok(line, lline, &st, &ltok);
        if (tokcmp(tok, ltok, "bdb") == 0) {
            /* Forward request to backend, which has its own safety checks
             * to prevent a dangerous command from running. */
            if (thedb->bdb_env == NULL)
                return -1;
            backend_cmd(dbenv, line, llinesav, stsav);
        } else if (tokcmp(tok, ltok, "osql") == 0) {
            osql_repository_printcrtsessions();
        } else if (tokcmp(tok, ltok, "net") == 0) {
            if (!thedb->handle_sibling)
                return -1;
            net_cmd(thedb->handle_sibling, line, lline, st, 1);
        } else if (tokcmp(tok, ltok, "osqlnet") == 0) {
            if (!thedb->handle_sibling)
                return -1;
            osql_net_cmd(line, lline, st, 1);
        } else if (tokcmp(tok, ltok, "signalnet") == 0) {
            if (!thedb->handle_sibling_signal)
                return -1;
            net_cmd(thedb->handle_sibling_signal, line, lline, st, 1);
        } else if (tokcmp(tok, ltok, "prefault") == 0) {
            prefault_stats(dbenv);
        } else if (tokcmp(tok, ltok, "stax") == 0) {
            showdbenv(dbenv);
        } else if (tokcmp(tok, ltok, "stal") == 0) {
            thd_stats();
        } else if (tokcmp(tok, ltok, "thr") == 0) {
            thrman_dump();
        } else if (tokcmp(tok, ltok, "sqlpool") == 0) {
            thdpool_print_stats(stdout, gbl_appsock_thdpool);
            thdpool_print_stats(stdout, gbl_sqlengine_thdpool);
            thdpool_print_stats(stdout, gbl_osqlpfault_thdpool);
            thdpool_print_stats(stdout, gbl_udppfault_thdpool);
            thdpool_print_stats(stdout, gbl_pgcompact_thdpool);
        } else if (tokcmp(tok, ltok, "dumpsql") == 0) {
            sql_dump_running_statements();
        } else if (tokcmp(tok, ltok, "rep") == 0) {
            replication_stats(dbenv);
        } else if (tokcmp(tok, ltok, "long") == 0) {
            request_stats(dbenv);
        } else if (tokcmp(tok, ltok, "appsock") == 0) {
            appsock_stat();
        } else if (tokcmp(tok, ltok, "blob") == 0) {
            blob_print_stats();
        } else if (tokcmp(tok, ltok, "compr") == 0) {
            compr_print_stats();
        } else if (tokcmp(tok, ltok, "resources") == 0) {
            dumpresources();
        } else if (tokcmp(tok, ltok, "queue") == 0) {
            goto queue_stat;
        } else if (tokcmp(tok, ltok, "signals") == 0) {
            dumpsignalsetupf(stdout);
        } else if (tokcmp(tok, ltok, "java") == 0) {
            char *tail;
            tail = tokdup(line + st, lline - st);
            javasp_stat(tail);
            free(tail);
        } else if (tokcmp(tok, ltok, "csc2vers") == 0) {
            int ii;
            for (ii = 0; ii < thedb->num_dbs; ii++) {
                if (thedb->dbs[ii]->dbtype == DBTYPE_TAGGED_TABLE) {
                    int version;
                    version = get_csc2_version(thedb->dbs[ii]->dbname);
                    logmsg(LOGMSG_USER, "table %s is at csc2 version %d\n",
                            thedb->dbs[ii]->dbname, version);
                }
            }
        } else if (tokcmp(tok, ltok, "dumpcsc2") == 0) {
            int version;
            char *dbname;
            struct db *db;

            tok = segtok(line, lline, &st, &ltok);
            if (ltok == 0) {
                logmsg(LOGMSG_ERROR, "expected table name\n");
                return -1;
            }
            dbname = tokdup(tok, ltok);

            tok = segtok(line, lline, &st, &ltok);
            if (ltok == 0) {
                logmsg(LOGMSG_ERROR, "expected csc2 version number\n");
                free(dbname);
                return -1;
            }
            version = toknum(tok, ltok);

            db = getdbbyname(dbname);
            if (!db) {
                logmsg(LOGMSG_ERROR, "no such table %s\n", dbname);
            } else if (db->dbtype != DBTYPE_TAGGED_TABLE) {
                logmsg(LOGMSG_ERROR, "not a tagged table\n");
            } else {
                char *csc2 = NULL;
                int rc, len;
                rc = get_csc2_file(db->dbname, version, &csc2, &len);
                logmsg(LOGMSG_ERROR, "Table '%s' get schema returned rcode %d\n", db->dbname,
                       rc);
                if (csc2) {
                    logmsg(LOGMSG_USER, "%s\n", csc2);
                    free(csc2);
                }
            }
            free(dbname);
        } else if (tokcmp(tok, ltok, "rmtpol") == 0) {
            char *host;
            logmsg(LOGMSG_USER, "I am running on a %s machine\n",
                   get_mach_class_str(machine()));
            tok = segtok(line, lline, &st, &ltok);
            if (ltok != 0) {
                char *m = tokdup(tok, ltok);
                host = intern(m);
                free(m);
                logmsg(LOGMSG_USER, "Machine %s is a %s machine\n", host,
                       get_mach_class_str(host));
                logmsg(LOGMSG_USER, "Allow writes from %s        ? %s\n", host,
                       allow_write_from_remote(host) ? "YES" : "NO");
                logmsg(LOGMSG_USER, "Allow cluster with %s       ? %s\n", host,
                       allow_cluster_from_remote(host) ? "YES" : "NO");
                logmsg(LOGMSG_USER, "Allow queue broadcast to %s ? %s\n", host,
                       allow_broadcast_to_remote(host) ? "YES" : "NO");
            }
        } else if (tokcmp(tok, ltok, "size") == 0) {
            dump_table_sizes(thedb);
        } else if (tokcmp(tok, ltok, "reql") == 0) {
            reqlog_stat();
        } else if (tokcmp(tok, ltok, "switch") == 0) {
            switch_status();
        } else if (tokcmp(tok, ltok, "clnt") == 0) {
            char *host = NULL;
            if (ltok > 0) {
                host = internn(tok, ltok);
            }
            if (ltok == 0 || tokcmp(tok, ltok, "totals") == 0) {
                if (host)
                    nodestats_node_report(stdout, NULL, 0, host);
                else
                    nodestats_report(stdout, NULL, 0);
            } else if (tokcmp(tok, ltok, "rates") == 0) {
                if (host)
                    nodestats_node_report(stdout, NULL, 1, host);
                else
                    nodestats_report(stdout, NULL, 1);
            } else {
                logmsg(LOGMSG_ERROR, "expected 'totals' or 'rates'\n");
            }
        } else if (tokcmp(tok, ltok, "repl_wait") == 0) {
            repl_wait_stats();
        } else if (ltok == 0) {
            unsigned long long rep_retry;
            unsigned long long msgs_processed;
            unsigned long long msgs_sent;
            unsigned long long txns_applied;
            int max_retries;

            bdb_get_rep_stats(dbenv->dbs[0]->handle, &msgs_processed,
                              &msgs_sent, &txns_applied, &rep_retry,
                              &max_retries);

            logmsg(LOGMSG_USER, "commit %lu abort %lu repcommit %llu retry %lu "
                   "verify retry %lld rep retry %llu max retry %d\n",
                   dbenv->txns_committed, dbenv->txns_aborted, txns_applied,
                   n_retries, gbl_verify_tran_replays, rep_retry, max_retries);

            logmsg(LOGMSG_USER, "readonly                %c\n", gbl_readonly ? 'Y' : 'N');
            logmsg(LOGMSG_USER, "num sql queries         %u\n", gbl_nsql);
            logmsg(LOGMSG_USER, "num new sql queries     %u\n", gbl_nnewsql);
            logmsg(LOGMSG_USER, "sql ticks               %llu\n", gbl_sqltick);
            logmsg(LOGMSG_USER, "sql deadlocks recover attempts %llu failures %llu\n",
                   gbl_sql_deadlock_reconstructions, gbl_sql_deadlock_failures);
            logmsg(LOGMSG_USER, "blocksql->socksql reqs  %lld\n",
                   gbl_converted_blocksql_requests);
            logmsg(LOGMSG_USER, "rowlocks is             %s\n",
                   gbl_rowlocks ? "enabled" : "disabled");
            appsock_quick_stat();
            osql_comm_quick_stat();
            logmsg(LOGMSG_USER, "elect timeout           %f (%s)\n",
                   (get_elect_time_microsecs() / 1000000.00),
                   gbl_elect_time_secs != 0 ? "local config" : "global config");
            sc_status(dbenv);
            print_dbs(dbenv);
            backend_stat(dbenv);
            logmsg(LOGMSG_USER, "Codename:      \"%s\"\n", gbl_db_release_name);
        } else if (tokcmp(tok, ltok, "ixstat") == 0) {
            ixstats(dbenv);
        } else if (tokcmp(tok, ltok, "cursors") == 0) {
            curstats(dbenv);
        } else if (tokcmp(tok, ltok, "sc") == 0) {
            sc_status(dbenv);
        } else if (tokcmp(tok, ltok, "dmpl") == 0) {
            thd_dump();
        } else if (tokcmp(tok, ltok, "analyze") == 0) {
            analyze_dump_stats();
        } else if (tokcmp(tok, ltok, "iopool") == 0) {
            berkdb_iopool_process_message("stat", 4, 0);
        } else if (tokcmp(tok, ltok, "reqrates") == 0) {
            logmsg(LOGMSG_ERROR, "Service time rates:\n");
            logmsg(LOGMSG_ERROR, "Non-sql requests this minute:\n");
            quantize_dump(q_min, stdout);
            logmsg(LOGMSG_ERROR, "Non-sql requests this hour:\n");
            quantize_dump(q_hour, stdout);
            logmsg(LOGMSG_ERROR, "Non-sql requests since startup:\n");
            quantize_dump(q_all, stdout);
            logmsg(LOGMSG_ERROR, "SQL requests this minute:\n");
            quantize_dump(q_sql_min, stdout);
            logmsg(LOGMSG_ERROR, "SQL requests this hour:\n");
            quantize_dump(q_sql_hour, stdout);
            logmsg(LOGMSG_ERROR, "SQL requests since startup:\n");
            quantize_dump(q_sql_all, stdout);
            logmsg(LOGMSG_ERROR, "SQL costs\n");
            logmsg(LOGMSG_ERROR, "SQL steps/query this minute:\n");
            quantize_dump(q_sql_steps_min, stdout);
            logmsg(LOGMSG_ERROR, "SQL steps/query this hour:\n");
            quantize_dump(q_sql_steps_hour, stdout);
            logmsg(LOGMSG_ERROR, "SQL steps/query since startup:\n");
            quantize_dump(q_sql_steps_all, stdout);
        } else if (tokcmp(tok, ltok, "trigger") == 0) {
            trigger_stat();
        } else if (tokcmp(tok, ltok, "keycompr") == 0) {
            extern uint64_t num_compressed;
            logmsg(LOGMSG_ERROR, "number of pages compressed: %" PRIu64 "\n", num_compressed);

            extern uint64_t total_pgsz;
            logmsg(LOGMSG_ERROR, "total page size processed: %" PRIu64 "\n", total_pgsz);

            extern uint64_t free_before_size;
            logmsg(LOGMSG_ERROR, "total before free size: %" PRIu64 "\n", free_before_size);

            extern uint64_t free_after_size;
            logmsg(LOGMSG_ERROR, "total after free size: %" PRIu64 "\n", free_after_size);

            int i;
            for (i = 0; i < thedb->num_dbs; ++i) {
                bdb_print_compression_flags(thedb->dbs[i]->handle);
            }
        }
#ifdef _LINUX_SOURCE
        else if (tokcmp(tok, ltok, "rcache") == 0) {
            extern uint32_t rcache_hits, rcache_miss, rcache_savd,
                rcache_invalid, rcache_collide;
            logmsg(LOGMSG_ERROR, "rcache enabled:%s\n", YESNO(gbl_rcache));
            logmsg(LOGMSG_ERROR, "cache hits: %u\n", rcache_hits);
            logmsg(LOGMSG_ERROR, "cache miss: %u\n", rcache_miss);
            logmsg(LOGMSG_ERROR, "cache save: %u\n", rcache_savd);
            logmsg(LOGMSG_ERROR, "cache invd: %u\n", rcache_invalid);
            logmsg(LOGMSG_ERROR, "cache coll: %u\n", rcache_collide);
        }
#endif
        else if (tokcmp(tok, ltok, "autoanalyze") == 0) {
            stat_auto_analyze();
        } else if (tokcmp(tok, ltok, "alias") == 0) {
            fdb_stat_alias();
        } else if (tokcmp(tok, ltok, "uprecs") == 0) {
            upgrade_records_stats();
        } else if (tokcmp(tok, ltok, "ssl") == 0) {
            ssl_stats();
        } else {
            logmsg(LOGMSG_ERROR, "bad stat command\n");
            print_help_page(HELP_STAT);
        }
    } else if (tokcmp(tok, ltok, "on") == 0) {
        change_switch(1, line, lline, st);
    } else if (tokcmp(tok, ltok, "off") == 0) {
        change_switch(0, line, lline, st);
    } else if (tokcmp(tok, ltok, "delay") == 0) {
        int z;
        tok = segtok(line, lline, &st, &ltok);
        z = toknum(tok, ltok);
        bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_COMMITDELAY, z);
        logmsg(LOGMSG_USER, "set commit delay to %d ms\n", z);
    } else if (tokcmp(tok, ltok, "repsleep") == 0) {
        int z;
        tok = segtok(line, lline, &st, &ltok);
        z = toknum(tok, ltok);
        bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_REPSLEEP, z);
        logmsg(LOGMSG_USER, "set repsleep to %d ms\n", z);
    }

    else if (tokcmp(tok, ltok, "delaymax") == 0) {
        int z;
        tok = segtok(line, lline, &st, &ltok);
        z = toknum(tok, ltok);
        bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_COMMITDELAYMAX, z);
        logmsg(LOGMSG_USER, "set commit delay max to %d ms\n", z);
    }

    else if (tokcmp(tok, ltok, "lock_conflict_trace") == 0) {
        logmsg(LOGMSG_USER, "Enabling lock-conflict trace.\n");
        gbl_lock_conflict_trace = 1;
    }

    else if (tokcmp(tok, ltok, "no_lock_conflict_trace") == 0) {
        logmsg(LOGMSG_USER, "Disabling lock-conflict trace.\n");
        gbl_lock_conflict_trace = 0;
    }

    else if (tokcmp(tok, ltok, "move_deadlock_max_attempt") == 0) {
        int z;
        tok = segtok(line, lline, &st, &ltok);
        z = toknum(tok, ltok);
        gbl_move_deadlk_max_attempt = z;
        logmsg(LOGMSG_USER, "setting deadlock-on-move max-attempts to %d\n", z);
    }

    else if (tokcmp(tok, ltok, "blocksql_grace") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        gbl_blocksql_grace = toknum(tok, ltok);
        logmsg(LOGMSG_USER, "setting blocksql grace timeout to %d seconds\n",
                gbl_blocksql_grace);
    }

    else if (tokcmp(tok, ltok, "maxt") == 0) {
        int z;
        tok = segtok(line, lline, &st, &ltok);
        z = toknum(tok, ltok);
        if (z < 1)
            logmsg(LOGMSG_ERROR, "must specify max number of threads\n");
        else {
            gbl_maxthreads = z; /* max # of threads */
            logmsg(LOGMSG_USER, "max number of threads set to %d\n", gbl_maxthreads);
            if (gbl_maxwthreads > gbl_maxthreads) {
                logmsg(LOGMSG_USER, "reducing max number of writer threads to %d\n",
                        gbl_maxthreads);
                gbl_maxwthreads = gbl_maxthreads;
            }
        }
    } else if (tokcmp(tok, ltok, "maxwt") == 0) {
        int z;
        tok = segtok(line, lline, &st, &ltok);
        z = toknum(tok, ltok);
        if (z < 1)
            logmsg(LOGMSG_ERROR, "must specify max number of writer threads\n");
        else if (z > gbl_maxthreads) {
            logmsg(LOGMSG_ERROR, "number of writer threads must be <= to total number of "
                   "threads (%d)\n",
                   gbl_maxthreads);
        } else {
            gbl_maxwthreads = z; /* max # of threads */
           logmsg(LOGMSG_USER, "max number of writer threads set to %d\n", gbl_maxwthreads);
        }
    }

    else if (tokcmp(tok, ltok, "penaltyincpercent") == 0) {
        int z;
        tok = segtok(line, lline, &st, &ltok);
        z = toknum(tok, ltok);
        gbl_penaltyincpercent = z;
    } else if (tokcmp(tok, ltok, "maxq") == 0) {
        int z;
        tok = segtok(line, lline, &st, &ltok);
        z = toknum(tok, ltok);
        if (z < 1 || z > 1000 /*can't be more than swapinit!*/)
            logmsg(LOGMSG_ERROR, "bad max number of items on queue\n");
        else {
            gbl_maxqueue = z; /* max pending requests.*/
            logmsg(LOGMSG_USER, "max number of items on queue set to %d\n", gbl_maxqueue);
        }
    } else if (tokcmp(tok, ltok, "ling") == 0) {
        int z;
        tok = segtok(line, lline, &st, &ltok);
        z = toknum(tok, ltok);
        if (z < 1)
            logmsg(LOGMSG_ERROR, "must specify number of seconds for idle thread linger\n");
        else {
            gbl_thd_linger = z; /* max pending requests.*/
            logmsg(LOGMSG_USER, "number of seconds for idle thread linger set to %d\n",
                   gbl_thd_linger);
        }
    } else if (tokcmp(tok, ltok, "reql") == 0) {
        reqlog_process_message(line, st, lline);
    } else if (tokcmp(tok, ltok, "debg") == 0 ||
               tokcmp(tok, ltok, "who") == 0) {
        int nsecs;
        tok = segtok(line, lline, &st, &ltok);
        /* generate debug trace for this many seconds */
        nsecs = toknum(tok, ltok);
        if (nsecs == 0) { /* 0 = turn off */
            gbl_debug_until = 0;
            logmsg(LOGMSG_USER, "Debugging off.\n");
        } else {
            gbl_debug_until = time_epoch() + nsecs;
            logmsg(LOGMSG_USER, "set full debugging for %d seconds\n", nsecs);
        }
    } else if (tokcmp(tok, ltok, "ndebg") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        gbl_who = toknum(tok, ltok);
        if (gbl_who > 0) {
            gbl_debug = 1;
            logmsg(LOGMSG_USER, "Debugging next %d requests\n", gbl_who);
        } else {
            gbl_who = 0;
            gbl_debug = 0;
            logmsg(LOGMSG_USER, "Debugging requests disabled\n", gbl_who);
        }
    } else if (tokcmp(tok, ltok, "inflatelog") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_USER, "Will inflate logs by %d%%.\n", gbl_inflate_log);
        } else {
            int il = toknum(tok, ltok);
            if (il >= 0) {
                gbl_inflate_log = il;
                logmsg(LOGMSG_USER, "Will inflate logs by %d%%\n", gbl_inflate_log);
            } else {
                logmsg(LOGMSG_ERROR, "Invalid argument for inflatelog\n");
            }
        }
    } else if (tokcmp(tok, ltok, "sdebg") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        gbl_who = toknum(tok, ltok);
        if (gbl_who > 0) {
            gbl_sdebug = 1;
            logmsg(LOGMSG_USER, "Debugging next %d sql requests\n", gbl_who);
        } else {
            gbl_who = 0;
            gbl_sdebug = 0;
            logmsg(LOGMSG_USER, "Debugging sql requests disabled\n", gbl_who);
        }
    } else if (tokcmp(tok, ltok, "who") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        gbl_who = toknum(tok, ltok);
        gbl_debug = gbl_sdebug = 0;
        logmsg(LOGMSG_USER, "Set who to %d\n", gbl_who);
    } else if (tokcmp(tok, ltok, "sync") == 0) {
        rc = process_sync_command(dbenv, line, lline, st);
        if (rc != 0)
            return -1;
    } else if (tokcmp(tok, ltok, "bdbrem") == 0) {
        int num;
        char *host;
        char *s;
        const char *realhost;

        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_USER, "Expected host name\n");
            return -1;
        }
        host = tokdup(tok, ltok);

        realhost = bdb_find_net_host(thedb->bdb_env, host);
        if (realhost == NULL) {
            logmsg(LOGMSG_ERROR, "WARNING: don't know about %s.\n", host);
            free(host);
            return -1;
        }
        free(host);

        host = intern(realhost);
        net_send_decom_all(thedb->handle_sibling, host);
        net_send_decom_all(thedb->handle_sibling_signal, host);
        osql_process_message_decom(host);
    } else if (tokcmp(tok, ltok, "electtime") == 0) {
        int num;

        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected timeout value\n");
            return -1;
        }
        num = toknum(tok, ltok);

        gbl_elect_time_secs = (num > 0 ? num : 0);

        if (gbl_elect_time_secs > 0)
            logmsg(LOGMSG_USER, "election timeout set to %d seconds\n", num);
        else
            logmsg(LOGMSG_USER, "election timeout override off\n");
    } else if (tokcmp(tok, ltok, "bdb") == 0) {
        if (thedb->bdb_env == NULL)
            return -1;
        backend_cmd(dbenv, line, lline, st);
    } else if (tokcmp(tok, ltok, "flush") == 0) {
        if (thedb->bdb_env == NULL)
            return -1;
        flush_db();
    } else if (tokcmp(tok, ltok, "ckp_sleep_before_sync") == 0) {
        /* Don't document the msgtrap -
           it is for debugging/testing only. */
        tok = segtok(line, lline, &st, &ltok);
        gbl_ckp_sleep_before_sync = (ltok != 0) ? toknum(tok, ltok) : 5;
        logmsg(LOGMSG_USER, "gbl_ckp_sleep_before_sync is now %d milliseconds\n",
               gbl_ckp_sleep_before_sync);
    } else if (tokcmp(tok, ltok, "load") == 0) {
        char fname[128];
        char table[MAXTABLELEN];
        int odh = -1, compress = -1, compress_blobs = -1;
        if (thedb->master != gbl_mynode) {
            logmsg(LOGMSG_ERROR, "I am not master\n");
            return -1;
        }

        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected db name\n");
            return -1;
        }
        if (ltok >= MAXTABLELEN) {
            logmsg(LOGMSG_ERROR, "Invalid table name: too long (max %d)\n", MAXTABLELEN);
            return -1;
        }
        tokcpy(tok, ltok, table);
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected schema file\n");
            return -1;
        }
        if (ltok >= sizeof(fname) - 1) {
            logmsg(LOGMSG_ERROR, "Invalid file name: too long (max %d)\n", sizeof(fname) - 1);
            return -1;
        }
        tokcpy(tok, ltok, fname);

        /* can optionally also specify odh, compress,
           compress_blobs  */

        tok = segtok(line, lline, &st, &ltok);
        if (tok) {
            odh = toknum(tok, ltok);

            tok = segtok(line, lline, &st, &ltok);
            if (tok) {
                compress = toknum(tok, ltok);
                tok = segtok(line, lline, &st, &ltok);
                if (tok) {
                    compress_blobs = toknum(tok, ltok);
                }
            }
        }
        if ((compress != 0 || compress_blobs != 0) && odh == 0) {
            logmsg(LOGMSG_ERROR, 
                    "Error - compression requires ODHs to be present\n");
            return -1;
        }

        /* and here we go... */
        rc = change_schema(table, fname, odh, compress, compress_blobs);
        if (rc != 0) {
            if (rc == -99)
                logmsg(LOGMSG_ERROR, 
                        "schema change already in progress, will not do\n");
            else
                logmsg(LOGMSG_ERROR, "error with schema change thread\n");
        }
    } else if (tokcmp(tok, ltok, "morestripe") == 0) {
        if (thedb->master != gbl_mynode) {
            logmsg(LOGMSG_ERROR, "I am not master\n");
            return -1;
        }
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected stripe factor\n");
            return -1;
        }
        if (tokcmp(tok, ltok, "blobstripe") == 0) {
            if (gbl_blobstripe) {
                logmsg(LOGMSG_USER, "I am already a blobstripe database\n");
                return -1;
            }
            return morestripe(thedb, gbl_dtastripe, 1);
        } else {
            int newdtastripe;
            newdtastripe = toknum(tok, ltok);
            if (newdtastripe <= gbl_dtastripe || newdtastripe > 16) {
                logmsg(LOGMSG_ERROR, "bad stripe factor %d, current factor is %d\n",
                       newdtastripe, gbl_dtastripe);
                return -1;
            }
            return morestripe(thedb, newdtastripe, gbl_blobstripe);
        }

    } else if (tokcmp(tok, ltok, "pushlogs") == 0) {
        char lsn[64];
        char *colon;
        uint32_t logfile = 0, logbyte = 0;
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "pushlogs should be followed by an lsn\n");
            return -1;
        }
        tokcpy(tok, ltok, lsn);
        colon = strchr(lsn, ':');
        if (colon) {
            *colon = '\0';
            logbyte = strtoul(colon + 1, NULL, 10);
        }
        logfile = strtoul(lsn, NULL, 10);
        set_target_lsn(logfile, logbyte);
    } else if (tokcmp(tok, ltok, "init_with_bthash") == 0) {
        int szkb;

        if (thedb->master != gbl_mynode) {
            logmsg(LOGMSG_ERROR, "I am not master\n");
            return -1;
        }

        tok = segtok(line, lline, &st, &ltok);
        szkb = toknum(tok, ltok);
        if (szkb <= 0) {
            logmsg(LOGMSG_ERROR, "Invalid hash size. init_with_bthash DISABLED\n");
        } else {
            gbl_init_with_bthash = szkb;
            logmsg(LOGMSG_USER, "Init with bthash %dkb per stripe\n", gbl_init_with_bthash);
        }
    } else if (tokcmp(tok, ltok, "bthash") == 0) {
        char table[MAXTABLELEN];
        int szkb;
        if (thedb->master != gbl_mynode) {
            logmsg(LOGMSG_ERROR, "I am not master\n");
            return -1;
        }

        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected db name\n");
            return -1;
        }
        if (ltok >= MAXTABLELEN) {
            logmsg(LOGMSG_ERROR, "Invalid table name: too long (max %d)\n", MAXTABLELEN);
            return -1;
        }

        tokcpy(tok, ltok, table);

        tok = segtok(line, lline, &st, &ltok);
        szkb = toknum(tok, ltok);
        if (szkb <= 0) {
            logmsg(LOGMSG_ERROR, "Invalid hash size. Please give positive hash size in kb\n");
            return -1;
        }

        if (bt_hash_table(table, szkb) != 0)
            return -1;
    } else if (tokcmp(tok, ltok, "bthashall") == 0) {
        int szkb;
        int idb;
        struct db *db;

        if (thedb->master != gbl_mynode) {
            logmsg(LOGMSG_ERROR, "I am not master\n");
            return -1;
        }

        tok = segtok(line, lline, &st, &ltok);
        szkb = toknum(tok, ltok);
        if (szkb <= 0) {
            logmsg(LOGMSG_ERROR, "Invalid hash size. Please give positive hash size in kb\n");
            return -1;
        }
        /* All tables */
        for (idb = 0; idb < dbenv->num_dbs; idb++) {
            db = dbenv->dbs[idb];
            if (bt_hash_table(db->dbname, szkb) != 0)
                return -1;
        }

    } else if (tokcmp(tok, ltok, "delbthash") == 0) {
        char table[MAXTABLELEN];
        int szkb;
        if (thedb->master != gbl_mynode) {
            logmsg(LOGMSG_ERROR, "I am not master\n");
            return -1;
        }

        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected db name\n");
            return -1;
        }
        if (ltok >= MAXTABLELEN) {
            logmsg(LOGMSG_ERROR, "Invalid table name: too long (max %d)\n", MAXTABLELEN);
            return -1;
        }

        tokcpy(tok, ltok, table);

        if (del_bt_hash_table(table) != 0)
            return -1;
    } else if (tokcmp(tok, ltok, "delbthashall") == 0) {
        struct db *db;
        int idb;

        if (thedb->master != gbl_mynode) {
            logmsg(LOGMSG_ERROR, "I am not master\n");
            return -1;
        }

        /* All tables */
        for (idb = 0; idb < dbenv->num_dbs; idb++) {
            db = dbenv->dbs[idb];
            if (del_bt_hash_table(db->dbname) != 0)
                return -1;
        }

    } else if (tokcmp(tok, ltok, "bthashstat") == 0) {
        char table[MAXTABLELEN];
        int szkb;

        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected db name\n");
            return -1;
        }
        if (ltok >= MAXTABLELEN) {
            logmsg(LOGMSG_ERROR, "Invalid table name: too long (max %d)\n", MAXTABLELEN);
            return -1;
        }

        tokcpy(tok, ltok, table);

        stat_bt_hash_table(table);
    } else if (tokcmp(tok, ltok, "clearbthashstat") == 0) {
        char table[MAXTABLELEN];
        int szkb;

        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected db name\n");
            return -1;
        }
        if (ltok >= MAXTABLELEN) {
            logmsg(LOGMSG_ERROR, "Invalid table name: too long (max %d)\n", MAXTABLELEN);
            return -1;
        }

        tokcpy(tok, ltok, table);

        stat_bt_hash_table_reset(table);
    } else if (tokcmp(tok, ltok, "fastinit") == 0) {
        char fname[128];
        char table[MAXTABLELEN];
        if (thedb->master != gbl_mynode) {
            logmsg(LOGMSG_ERROR, "I am not master\n");
            return -1;
        }

        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected db name\n");
            return -1;
        }
        if (ltok >= MAXTABLELEN) {
            logmsg(LOGMSG_ERROR, "Invalid table name: too long (max %d)\n", MAXTABLELEN);
            return -1;
        }

        tokcpy(tok, ltok, table);

        /*
           tok=segtok(line,lline,&st,&ltok);
           if (ltok == 0) {
           printf("Expected schema file\n");
           return -1;
           }
           if (ltok >= sizeof(fname) - 1) {
           printf("Invalid file name: too long (max %d)\n", sizeof(fname) - 1);
           return -1;
           }
           tokcpy(tok, ltok, fname);
         */

        /* and here we go... */
        rc = fastinit_table(dbenv, table);
        if (rc != 0) {
            if (rc == -99)
                fprintf(stderr,
                        "schema change already in progress, will not do\n");
            else {
                logmsg(LOGMSG_ERROR, "error with schema change thread\n");
            }
        }
    }

    else if (tokcmp(tok, ltok, "dumptags") == 0) {
        int i;
        for (i = 0; i < thedb->num_dbs; i++)
            debug_dump_tags(thedb->dbs[i]->dbname);
    } else if (tokcmp(tok, ltok, "screportfreq") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (ltok != 0) {
            gbl_sc_report_freq = toknum(tok, ltok);
        }
        logmsg(LOGMSG_USER, "schema change report frequency is now %d seconds\n",
               gbl_sc_report_freq);
    } else if (tokcmp(tok, ltok, "scdelay") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (ltok != 0) {
            gbl_sc_usleep = toknum(tok, ltok) * 1000;
        }
        logmsg(LOGMSG_USER, "live schema change sleep is now %dms (for sc thread)\n",
               gbl_sc_usleep / 1000);
    } else if (tokcmp(tok, ltok, "scwrdelay") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (ltok != 0) {
            gbl_sc_wrusleep = toknum(tok, ltok) * 1000;
        }
        logmsg(LOGMSG_USER, "live schema change sleep is now %dms (for writer threads)\n",
               gbl_sc_wrusleep / 1000);
    } else if (tokcmp(tok, ltok, "scabort") == 0) {
        logmsg(LOGMSG_USER, "Will abort schema change\n");
        gbl_sc_abort = 1;
        MEMORY_SYNC;
    } else if (tokcmp(tok, ltok, "scforceabort") == 0) {
        logmsg(LOGMSG_USER, "Forcibly resetting schema change flat\n");
        sc_set_running(0, 0, NULL, 0);
    } else if (tokcmp(tok, ltok, "debug") == 0) {
        debug_trap(line + st, lline - st);
    }

    /*
       access set password <user>  <password>
       access set read     <table> <user>
       access set write    <table> <user>
       access set authentication
       access set tableXnode

       access delete read  <table> <user>
       access delete write <table> <user>

       access get read     <table> <user>
       access get write    <table> <user>
       access get authentication
       access get tableXnode
     */
    else if (tokcmp(tok, ltok, "access") == 0) {
        char table[MAXTABLELEN];
        char user[17];
        char password[17];
        int rc;
        int bdberr;

        tok = segtok(line, lline, &st, &ltok);

        if (tokcmp(tok, ltok, "set") == 0) {
            tok = segtok(line, lline, &st, &ltok);

            if (tokcmp(tok, ltok, "password") == 0) {
                tok = segtok(line, lline, &st, &ltok);
                tokcpy(tok, ltok, user);

                tok = segtok(line, lline, &st, &ltok);
                tokcpy(tok, ltok, password);

                rc = bdb_user_password_set(NULL, user, password);
                if (!rc) {
                    logmsg(LOGMSG_USER, "set password for %s\n", user);
                } else {
                    logmsg(LOGMSG_ERROR, 
                            "FAILED set password for %s rc=%d\n",
                            user, rc);
                }
            } else if (tokcmp(tok, ltok, "read") == 0) {
                tok = segtok(line, lline, &st, &ltok);
                tokcpy(tok, ltok, table);

                tok = segtok(line, lline, &st, &ltok);
                tokcpy(tok, ltok, user);

                rc = bdb_tbl_access_read_set(dbenv->bdb_env, NULL, table, user,
                                             &bdberr);
                if (!rc) {
                    logmsg(LOGMSG_USER, "set read for %s and table %s\n", user,
                            table);
                } else {
                    logmsg(LOGMSG_ERROR, "FAILED set read for %s rc=%d bdberr=%d\n",
                            user, rc, bdberr);
                }
            } else if (tokcmp(tok, ltok, "write") == 0) {
                tok = segtok(line, lline, &st, &ltok);
                tokcpy(tok, ltok, table);

                tok = segtok(line, lline, &st, &ltok);
                tokcpy(tok, ltok, user);

                rc = bdb_tbl_access_write_set(dbenv->bdb_env, NULL, table, user,
                                              &bdberr);
                if (!rc) {
                    logmsg(LOGMSG_USER, "set write for %s and table %s\n", user,
                            table);
                } else {
                    logmsg(LOGMSG_ERROR, "FAILED set write for %s rc=%d bdberr=%d\n",
                            user, rc, bdberr);
                }
            } else if (tokcmp(tok, ltok, "authentication") == 0) {
                rc = bdb_authentication_set(dbenv->bdb_env, NULL, 1, &bdberr);
                if (rc == 0) {
                    logmsg(LOGMSG_USER, "authentication enabled\n");
                } else {
                    logmsg(LOGMSG_ERROR, "FAILED enable authentication rc=%d bdberr=%d\n",
                            rc, bdberr);
                }
            } else if (tokcmp(tok, ltok, "tableXnode") == 0) {
                rc = bdb_accesscontrol_tableXnode_set(dbenv->bdb_env, NULL,
                                                      &bdberr);
                if (rc == 0) {
                    logmsg(LOGMSG_USER, "enabled access control tableXnode\n");
                } else {
                    logmsg(LOGMSG_ERROR, 
                            "FAILED enable tableXnode rc=%d bdberr=%d\n", rc,
                            bdberr);
                }
            } else {
                logmsg(LOGMSG_ERROR, "unrecognized \"%.*s\"\n", ltok, tok);
            }
        } else if (tokcmp(tok, ltok, "get") == 0) {
            tok = segtok(line, lline, &st, &ltok);

            if (tokcmp(tok, ltok, "read") == 0) {
                tok = segtok(line, lline, &st, &ltok);
                tokcpy(tok, ltok, table);

                tok = segtok(line, lline, &st, &ltok);
                tokcpy(tok, ltok, user);

                rc = bdb_tbl_access_read_get(dbenv->bdb_env, NULL, table, user,
                                             &bdberr);
                logmsg(LOGMSG_ERROR, "rc = %d (\"%s\")\n", rc,
                        (rc == 0) ? "enabled" : "disabled");
            } else if (tokcmp(tok, ltok, "write") == 0) {
                tok = segtok(line, lline, &st, &ltok);
                tokcpy(tok, ltok, table);

                tok = segtok(line, lline, &st, &ltok);
                tokcpy(tok, ltok, user);

                rc = bdb_tbl_access_write_get(dbenv->bdb_env, NULL, table, user,
                                              &bdberr);
                logmsg(LOGMSG_ERROR, "rc = %d (\"%s\")\n", rc,
                        (rc == 0) ? "enabled" : "disabled");

            } else if (tokcmp(tok, ltok, "authentication") == 0) {
                rc = bdb_authentication_get(dbenv->bdb_env, NULL, &bdberr);
                logmsg(LOGMSG_ERROR, "rc = %d (\"%s\")\n", rc,
                        (rc == 0) ? "enabled" : "disabled");
            } else if (tokcmp(tok, ltok, "tableXnode") == 0) {
                rc = bdb_accesscontrol_tableXnode_get(dbenv->bdb_env, NULL,
                                                      &bdberr);
                logmsg(LOGMSG_ERROR, "rc = %d (\"%s\")\n", rc,
                        (rc == 0) ? "enabled" : "disabled");
            } else {
                logmsg(LOGMSG_ERROR, "unrecognized \"%.*s\"\n", ltok, tok);
            }
        } else if (tokcmp(tok, ltok, "del") == 0) {

            tok = segtok(line, lline, &st, &ltok);

            if (tokcmp(tok, ltok, "read") == 0) {
                tok = segtok(line, lline, &st, &ltok);
                tokcpy(tok, ltok, table);

                tok = segtok(line, lline, &st, &ltok);
                tokcpy(tok, ltok, user);

                rc = bdb_tbl_access_read_delete(dbenv->bdb_env, NULL, table,
                                                user, &bdberr);
                if (rc == 0) {
                    logmsg(LOGMSG_ERROR, "deleted read for %s and table %s\n", user,
                            table);
                } else {
                    logmsg(LOGMSG_ERROR, 
                            "FAILED delete read for %s rc=%d bdberr=%d\n", user,
                            rc, bdberr);
                }
            } else if (tokcmp(tok, ltok, "write") == 0) {
                tok = segtok(line, lline, &st, &ltok);
                tokcpy(tok, ltok, table);

                tok = segtok(line, lline, &st, &ltok);
                tokcpy(tok, ltok, user);

                rc = bdb_tbl_access_write_delete(dbenv->bdb_env, NULL, table,
                                                 user, &bdberr);
                if (rc == 0) {
                    logmsg(LOGMSG_ERROR, "deleted write for %s and table %s\n", user,
                            table);
                } else {
                    logmsg(LOGMSG_ERROR, 
                            "FAILED delete write for %s rc=%d bdberr=%d\n",
                            user, rc, bdberr);
                }
            } else {
                logmsg(LOGMSG_ERROR, "unknown option \"%.*s\"\n", ltok, tok);
            }
        } else {
            logmsg(LOGMSG_ERROR, "unknown option \"%.*s\"\n", ltok, tok);
        }
    }

    else if (tokcmp(tok, ltok, "llmeta") == 0) {
        char table[MAXTABLELEN];
        char user[17];
        char password[17];
        int rc;
        int bdberr;

        tok = segtok(line, lline, &st, &ltok);
        /*
      if (tokcmp(tok,ltok,"set")==0)
      {
         tok=segtok(line,lline,&st,&ltok);

         if (tokcmp(tok,ltok,"password")==0)
         {
            tok=segtok(line,lline,&st,&ltok);
            tokcpy(tok, ltok, user);

            tok=segtok(line,lline,&st,&ltok);
            tokcpy(tok, ltok, password);

            rc = bdb_user_password_set(dbenv->bdb_env, NULL, 
                  user, password, &bdberr);
            if (!rc)
            {
               fprintf( stderr,"set password for %s\n", user);
            }
            else
            {
               fprintf( stderr,"FAILED set password for %s rc=%d bdberr=%d\n",
                     user, rc, bdberr);
            }
         }
         else if (tokcmp(tok,ltok,"read")==0)
         {
            tok=segtok(line,lline,&st,&ltok);
            tokcpy(tok, ltok, table);

            tok=segtok(line,lline,&st,&ltok);
            tokcpy(tok, ltok, user);

            rc = bdb_tbl_access_read_set(dbenv->bdb_env, NULL,
                  table, user, &bdberr);
            if (!rc)
            {
               fprintf( stderr,"set read for %s and table %s\n", 
                     user, table);
            }
            else
            {
               fprintf( stderr,"FAILED set read for %s rc=%d bdberr=%d\n",
                     user, rc, bdberr);
            }
         }
         else if (tokcmp(tok,ltok,"write")==0)
         {
            tok=segtok(line,lline,&st,&ltok);
            tokcpy(tok, ltok, table);

            tok=segtok(line,lline,&st,&ltok);
            tokcpy(tok, ltok, user);

            rc = bdb_tbl_access_write_set(dbenv->bdb_env, NULL,
                  table, user, &bdberr);
            if (!rc)
            {
               fprintf( stderr,"set write for %s and table %s\n", 
                     user, table);
            }
            else
            {
               fprintf( stderr,"FAILED set write for %s rc=%d bdberr=%d\n",
                     user, rc, bdberr);
            }
         }
         else if (tokcmp(tok,ltok,"authentication")==0)
         {
            rc = bdb_authentication_set(dbenv->bdb_env, NULL,
                  &bdberr);
            if (rc == 0)
            {
               fprintf(stderr, "authentication enabled\n");
            }
            else
            {
               fprintf(stderr,"FAILED enable authentication rc=%d bdberr=%d\n",
                     rc, bdberr);
            }
         }
         else if (tokcmp(tok,ltok,"tableXnode")==0)
         {
            rc = bdb_accesscontrol_tableXnode_set(dbenv->bdb_env, NULL,
                  &bdberr);
            if (rc == 0)
            {
               fprintf(stderr, "enabled access control tableXnode\n");
            }
            else
            {
               fprintf(stderr,"FAILED enable tableXnode rc=%d bdberr=%d\n",
                     rc, bdberr);
            }
         }
         else
         {
            fprintf( stderr, "unrecognized \"%.*s\"\n", ltok, tok);
         }
      }
      else if (tokcmp(tok,ltok,"get")==0)
      {
         tok=segtok(line,lline,&st,&ltok);

         if (tokcmp(tok,ltok,"read")==0)
         {
            tok=segtok(line,lline,&st,&ltok);
            tokcpy(tok, ltok, table);

            tok=segtok(line,lline,&st,&ltok);
            tokcpy(tok, ltok, user);

            rc = bdb_tbl_access_read_get(dbenv->bdb_env, NULL,
                  table, user, &bdberr);
            fprintf(stderr, "rc = %d (\"%s\")\n", rc, 
                  (rc==0)?"enabled":"disabled");
         }
         else if (tokcmp(tok,ltok,"write")==0)
         {
            tok=segtok(line,lline,&st,&ltok);
            tokcpy(tok, ltok, table);

            tok=segtok(line,lline,&st,&ltok);
            tokcpy(tok, ltok, user);

            rc = bdb_tbl_access_write_get(dbenv->bdb_env, NULL,
                  table, user, &bdberr);
            fprintf(stderr, "rc = %d (\"%s\")\n", rc,
                  (rc==0)?"enabled":"disabled");

         }
         else if (tokcmp(tok,ltok,"authentication")==0)
         {
            rc = bdb_authentication_get(dbenv->bdb_env, NULL,
                  &bdberr);
            fprintf(stderr, "rc = %d (\"%s\")\n", rc,
                  (rc==0)?"enabled":"disabled");
         }
         else if (tokcmp(tok,ltok,"tableXnode")==0)
         {
            rc = bdb_accesscontrol_tableXnode_get(dbenv->bdb_env, NULL,
                  &bdberr);
            fprintf(stderr, "rc = %d (\"%s\")\n", rc,
                  (rc==0)?"enabled":"disabled");
         }
         else
         {
            fprintf( stderr, "unrecognized \"%.*s\"\n", ltok, tok);
         }
      }
      else if( tokcmp(tok, ltok, "del")==0)
      {

         tok=segtok(line,lline,&st,&ltok);

         if (tokcmp(tok,ltok,"read")==0)
         {
            tok=segtok(line,lline,&st,&ltok);
            tokcpy(tok, ltok, table);

            tok=segtok(line,lline,&st,&ltok);
            tokcpy(tok, ltok, user);

            rc = bdb_tbl_access_read_delete(dbenv->bdb_env, NULL,
                  table, user, &bdberr);
            if (rc == 0)
            {
               fprintf(stderr, "deleted read for %s and table %s\n",
                     user, table);
            }
            else
            {
               fprintf(stderr,"FAILED delete read for %s rc=%d bdberr=%d\n",
                     user, rc, bdberr);
            }
         }
         else if (tokcmp(tok,ltok,"write")==0)
         {
            tok=segtok(line,lline,&st,&ltok);
            tokcpy(tok, ltok, table);

            tok=segtok(line,lline,&st,&ltok);
            tokcpy(tok, ltok, user);

            rc = bdb_tbl_access_write_delete(dbenv->bdb_env, NULL,
                  table, user, &bdberr);
            if (rc == 0)
            {
               fprintf(stderr, "deleted write for %s and table %s\n",
                     user, table);
            }
            else
            {
               fprintf(stderr,"FAILED delete write for %s rc=%d bdberr=%d\n",
                     user, rc, bdberr);
            }
         }
         else
         {
            fprintf( stderr,"unknown option \"%.*s\"\n", ltok, tok);
         }
      }
      else */ if (tokcmp(tok, ltok, "list") == 0) {
            rc = bdb_llmeta_list_records(thedb->bdb_env, &bdberr);
            if (rc) {
                logmsg(LOGMSG_ERROR, 
                        "%s:%d: failed to list all options rc=%d bdberr=%d\n",
                        __FILE__, __LINE__, rc, bdberr);
            }
        } else {
            logmsg(LOGMSG_ERROR, "unknown option \"%.*s\"\n", ltok, tok);
        }
    }

    else if (tokcmp(tok, ltok, "readonly") == 0) {
        gbl_readonly = 1;
    } else if (tokcmp(tok, ltok, "readwrite") == 0) {
        gbl_readonly = 0;
    } else if (tokcmp(tok, ltok, "allow") == 0 ||
               tokcmp(tok, ltok, "disallow") == 0 ||
               tokcmp(tok, ltok, "clrpol") == 0 ||
               tokcmp(tok, ltok, "setclass") == 0) {
        process_allow_command(line + stsav, llinesav - stsav);
    } else if (tokcmp(tok, ltok, "reinit") == 0) {
        struct db *db;
        char dbname[100];

        if (gbl_mynode != thedb->master) {
            logmsg(LOGMSG_ERROR, "Please run reinit on master\n");
            return -1;
        }

        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "usage: reinit tablename\n");
            return -1;
        }
        tokcpy(tok, ltok, dbname);
        db = getdbbyname(dbname);
        if (db == NULL) {
            logmsg(LOGMSG_ERROR, "No such db %s\n", dbname);
        } else {
            rc = reinit_db(db);
            if (rc)
                logmsg(LOGMSG_ERROR, "reinit %s failed rc %d\n", dbname, rc);
            else
                logmsg(LOGMSG_USER, "reinit %s ok\n", dbname);
        }
    } else if (tokcmp(tok, ltok, "cleartable") == 0) {
        struct db *db;
        char dbname[100];

        if (gbl_mynode != thedb->master) {
            logmsg(LOGMSG_ERROR, "Please run cleartable on master\n");
            return -1;
        }

        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "usage: cleartable tablename\n");
            return -1;
        }
        tokcpy(tok, ltok, dbname);
        db = getdbbyname(dbname);
        if (db == NULL) {
            logmsg(LOGMSG_ERROR, "No such db %s\n", dbname);
        } else {
            rc = truncate_db(db);
            if (rc)
                logmsg(LOGMSG_ERROR, "reinit %s failed rc %d\n", dbname, rc);
            else
                logmsg(LOGMSG_USER, "reinit %s ok\n", dbname);
        }
    }
    else if (tokcmp(tok, ltok, "fastcount") == 0) {
        struct db *db;
        char dbname[100];

        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "usage: count tablename\n");
            return -1;
        }
        tokcpy(tok, ltok, dbname);
        db = getdbbyname(dbname);
        if (db == NULL) {
            logmsg(LOGMSG_ERROR, "No such db %s\n", dbname);
        } else {
            fastcount(dbname);
        }
    } else if (tokcmp(tok, ltok, "sqlflush") == 0) {
        int freq;
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
           logmsg(LOGMSG_USER, "Currently flushing every %d records\n", gbl_sqlflush_freq);
            return -1;
        }
        freq = toknum(tok, ltok);
        if (gbl_sqlflush_freq < 0) {
            logmsg(LOGMSG_ERROR, "Invalid flush frequency\n");
            return -1;
        }
        gbl_sqlflush_freq = freq;
        logmsg(LOGMSG_USER, "SQL flush frequency: %d\n", gbl_sqlflush_freq);
    } else if (tokcmp(tok, ltok, "sbuftimeout") == 0) {
        int tmout;
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_USER, "Timing out sbuf after %dms\n", gbl_sbuftimeout);
            return -1;
        }
        tmout = toknum(tok, ltok);
        if (gbl_sbuftimeout < 0) {
            gbl_sbuftimeout = 0;
            logmsg(LOGMSG_ERROR, "Invalid sbuf timeout\n");
            return -1;
        }
        gbl_sbuftimeout = tmout;
        set_sbuftimeout(gbl_sbuftimeout);
    } else if (tokcmp(tok, ltok, "sqldbgtrace") == 0) {
        int dbgflag;
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected flag for sqldbgtrace\n");
            return -1;
        }
        dbgflag = toknum(tok, ltok);
        sqldbgflag = dbgflag;
    } else if (tokcmp(tok, ltok, "sqllog") == 0) {
        int dbgflag;
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected flag for sqllog\n");
            return -1;
        }
        dbgflag = toknum(tok, ltok);
        gbl_dump_sql_dispatched = dbgflag;
        if (dbgflag == 2)
            gbl_dump_fsql_response = 1;
        else
            gbl_dump_fsql_response = 0;
        logmsg(LOGMSG_USER, "sqllog %s\n",
               (gbl_dump_sql_dispatched) ? "enabled" : "disabled");
    } else if (tokcmp(tok, ltok, "sqllogtime") == 0) {
        int dbgflag;
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected flag for sqllog\n");
            return -1;
        }
        dbgflag = toknum(tok, ltok);
        gbl_time_osql = dbgflag;
        logmsg(LOGMSG_USER, "sqllogtime %s\n", (gbl_time_osql) ? "enabled" : "disabled");
    } else if (tokcmp(tok, ltok, "fdblogtime") == 0) {
        int dbgflag;
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected flag for sqllog\n");
            return -1;
        }
        dbgflag = toknum(tok, ltok);
        gbl_time_fdb = dbgflag;
        logmsg(LOGMSG_USER, "fdblogtime %s\n", (gbl_time_fdb) ? "enabled" : "disabled");
    } else if (tokcmp(tok, ltok, "osqlenginetrack") == 0) {
        int dbgflag;
        tok = segtok(line, lline, &st, &ltok);
        if (tok) {
            dbgflag = toknum(tok, ltok);
            gbl_track_sqlengine_states = dbgflag;
            logmsg(LOGMSG_USER, "sql track sqlengine %s\n",
                   (gbl_track_sqlengine_states) ? "enabled" : "disabled");
        }

    } else if (tokcmp(tok, ltok, "osqlecho") == 0) {
        int tonode;
        int stream;
        unsigned long long *sent;
        unsigned long long *replied;
        unsigned long long *received;
        char *host;

        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected host for osqlecho\n");
            return -1;
        }
        host = internn(tok, ltok);
        tok = segtok(line, lline, &st, &ltok);
        if (ltok) {
            stream = toknum(tok, ltok);
        } else {
            stream = 1;
        }

        sent =
            (unsigned long long *)malloc(stream * sizeof(unsigned long long));
        replied =
            (unsigned long long *)malloc(stream * sizeof(unsigned long long));
        received =
            (unsigned long long *)malloc(stream * sizeof(unsigned long long));
        rc = osql_comm_echo(host, stream, sent, replied, received);
        if (rc == 0) {
            int i = 0;
            for (i = 0; i < stream; i++) {
               logmsg(LOGMSG_USER, "%s: total=%llu msec sent=%llu msec return=%llu msec "
                       "(%llu %llu %llu)\n",
                       host, received[i] - sent[i], replied[i] - sent[i],
                       received[i] - replied[i], sent[i], replied[i],
                       received[i]);
            }
        } else {
            logmsg(LOGMSG_ERROR, "echo failed\n");
        }

    } else if (tokcmp(tok, ltok, "dumprecord") == 0) {
        struct db *db;
        int rrn;
        unsigned long long genid;
        char *tbl;
        char *snum;

        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected table name.\n");
            return -1;
        }
        tbl = tokdup(tok, ltok);
        db = getdbbyname(tbl);
        if (db == NULL) {
            logmsg(LOGMSG_ERROR, "Unknown table %s\n", tbl);
            free(tbl);
            return -1;
        }
        free(tbl);

        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected genid\n");
            return -1;
        }
        snum = tokdup(tok, ltok);
        if (!snum)
            return -1;
        genid = strtoull(snum, NULL, 0);
        free(snum);
        rrn = 2;
        dump_record_by_rrn_genid(db, rrn, genid);
    } else if (tokcmp(tok, ltok, "upgraderecord") == 0) {
        unsigned long long genid;
        char *tbl;
        char *snum;

        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected table name.\n");
            return -1;
        }
        tbl = tokdup(tok, ltok);

        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected genid\n");
            return -1;
        }
        snum = tokdup(tok, ltok);
        if (!snum)
            return -1;
        genid = strtoull(snum, NULL, 0);
        free(snum);

        rc = offload_comm_send_upgrade_record(tbl, genid);
        if (rc != 0)
            logmsg(LOGMSG_ERROR, 
                    "Error in offload_comm_send_upgrade_record. rc = %d\n", rc);
        free(tbl);
    } else if (tokcmp(tok, ltok, "upgradetable") == 0) {
        // TODO this should be a schemachange cmd. Used for testing only.
        char *tbl;
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected table name.\n");
            return -1;
        }
        tbl = tokdup(tok, ltok);
        rc = start_table_upgrade(dbenv, tbl, 0, 1, 0, 1);
        free(tbl);
    } else if (tokcmp(tok, ltok, "enable_upgrade_ahead") == 0) {
        tok = segtok(line, sizeof(line), &st, &ltok);
        if (ltok <= 0)
            gbl_num_record_upgrades = 32;
        else
            gbl_num_record_upgrades = toknum(tok, ltok);
       logmsg(LOGMSG_USER, "Upgrade ahead enabled with size %d.\n",
               gbl_num_record_upgrades);
    } else if (tokcmp(tok, ltok, "disable_upgrade_ahead") == 0) {
        gbl_num_record_upgrades = toknum(tok, ltok);
       logmsg(LOGMSG_USER, "Upgrade ahead disabled.\n");
    } else if (tokcmp(tok, ltok, "checkctags") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            if (gbl_check_client_tags == 0) {
               logmsg(LOGMSG_USER, "currently check tag logic is off\n");
            } else if (gbl_check_client_tags == 1) {
                logmsg(LOGMSG_USER, "currently check tag logic will return error to client\n");
            } else if (gbl_check_client_tags == 2) {
               logmsg(LOGMSG_USER, "currently check tag logic will produce soft warning\n");
            } else {
                logmsg(LOGMSG_ERROR, "currently check tag logic in unknown state! %d\n",
                        gbl_check_client_tags);
            }
            return 0;
        }
        if (tokcmp(tok, ltok, "off") == 0) {
            logmsg(LOGMSG_USER, "check tag logic is now off\n");
            gbl_check_client_tags = 0;
        } else if (tokcmp(tok, ltok, "soft") == 0) {
           logmsg(LOGMSG_USER, "check tag logic will now produce warning\n");
            gbl_check_client_tags = 2;
        } else if (tokcmp(tok, ltok, "full") == 0) {
            logmsg(LOGMSG_USER, "check tag logic will now error out to client\n");
            gbl_check_client_tags = 1;
        } else {
            char tokv[32];
            bzero(tokv, sizeof(tokv));
            strncpy(tokv, tok,
                    (ltok >= sizeof(tokv)) ? (sizeof(tokv) - 1) : ltok);
            logmsg(LOGMSG_ERROR, "Invalid command for checktags '%s'. use one of "
                            "'off','soft','full'. seek help!\n",
                    tokv);
        }

        return 0;
    } else if (tokcmp(tok, ltok, "sql") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (tokcmp(tok, ltok, "dump") == 0) {
            sql_dump_running_statements();
        } else if (tokcmp(tok, ltok, "keep") == 0) {
            int n;
            tok = segtok(line, lline, &st, &ltok);
            if (ltok != 0) {
                n = toknum(tok, ltok);
                if (n < 0)
                    logmsg(LOGMSG_ERROR, "Invalid size\n");
                else {
                    gbl_sqlhistsz = n;
                }
            }
            logmsg(LOGMSG_ERROR, "Keeping stats on last %d sql statements\n", gbl_sqlhistsz);
        } else if (tokcmp(tok, ltok, "hist") == 0) {
            sql_dump_hist_statements();
        } else if (tokcmp(tok, ltok, "cancel") == 0) {
            int qid;
            tok = segtok(line, lline, &st, &ltok);
            if (ltok == 0 || (qid = toknum(tok, ltok)) == 0)
                logmsg(LOGMSG_ERROR, "Usage: sql cancel queryid.  You can get query id with "
                       "\"sql dump\".\n");
            else
                cancel_sql_statement(qid);
        } else if (tokcmp(tok, ltok, "rdtimeout") == 0) {
            tok = segtok(line, lline, &st, &ltok);
            gbl_sqlrdtimeoutms = toknum(tok, ltok);
            logmsg(LOGMSG_USER, "SQL read timeout now set to %d ms\n", gbl_sqlrdtimeoutms);
        } else if (tokcmp(tok, ltok, "wrtimeout") == 0) {
            tok = segtok(line, lline, &st, &ltok);
            gbl_sqlwrtimeoutms = toknum(tok, ltok);
           logmsg(LOGMSG_USER, "SQL write timeout now set to %d ms\n", gbl_sqlwrtimeoutms);
        } else if (tokcmp(tok, ltok, "help") == 0) {
            print_help_page(HELP_SQL);
        } else if (tokcmp(tok, ltok, "testrun") == 0) {
            sql_testrun(&line[st], lline - st);
        } else if (tokcmp(tok, ltok, "debug") == 0) {
            extern int gbl_debug_sql_opcodes;
            on_off_trap(line, lline, &st, &ltok, "SQL debug mode", "sql debug",
                        &gbl_debug_sql_opcodes);
        } else if (tokcmp(tok, ltok, "dumphints") == 0) {
            sql_dump_hints();
        }
    } else if (tokcmp(tok, ltok, "ixstat") == 0) {
        ixstats(dbenv);
    } else if (tokcmp(tok, ltok, "lrepl") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (tokcmp(tok, ltok, "on") == 0) {
            if (gbl_replicate_local) {
               logmsg(LOGMSG_USER, "Local replicants already enabled\n");
                return 1;
            }
            gbl_replicate_local = 1;
           logmsg(LOGMSG_USER, "Local replicants enabled\n");
        } else if (tokcmp(tok, ltok, "off") == 0) {
            if (gbl_replicate_local == 0) {
               logmsg(LOGMSG_USER, "Local replicants already isabled\n");
                return 1;
            }
            gbl_replicate_local = 0;
           logmsg(LOGMSG_USER, "Local replicants disabled\n");
        } else {
            logmsg(LOGMSG_ERROR, "Local replicant options\n");
            logmsg(LOGMSG_ERROR, "  on   - enable  (must have correct tables set up)\n");
            logmsg(LOGMSG_ERROR, "  off  - disable\n");
        }
        return 0;
    } else if (tokcmp(tok, ltok, "analyze") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            print_help_page(HELP_ANALYZE);
            return 0;
        } else if (tokcmp(tok, ltok, "sample") == 0) {
            analyze_enable_sampled_indicies();
           logmsg(LOGMSG_USER, "Enabled sampling tables for analyze\n");
        } else if (tokcmp(tok, ltok, "nosample") == 0) {
            analyze_disable_sampled_indicies();
           logmsg(LOGMSG_USER, "Disabled sampling tables for analyze\n");
        } else if (tokcmp(tok, ltok, "thresh") == 0) {
            int thresh = 0;
            tok = segtok(line, lline, &st, &ltok);
            if (ltok <= 0) {
               logmsg(LOGMSG_USER, "Analyze thresh command needs a size-argument\n");
                return 0;
            }
            thresh = toknum(tok, ltok);
            analyze_set_sampling_threshold(thresh);
           logmsg(LOGMSG_USER, "Analyze sampling threshold set to %d\n", thresh);
        } else if (tokcmp(tok, ltok, "tblthd") == 0) {
            int maxtd = 0;
            tok = segtok(line, lline, &st, &ltok);
            if (ltok <= 0) {
                logmsg(LOGMSG_ERROR, "Analyze tblthd command requires number-of-threads!\n");
                return 0;
            }
            maxtd = toknum(tok, ltok);
            analyze_set_max_table_threads(maxtd);
        } else if (tokcmp(tok, ltok, "compthd") == 0) {
            int maxtd = 0;
            tok = segtok(line, lline, &st, &ltok);
            if (ltok <= 0) {
                logmsg(LOGMSG_ERROR, "Analyze compthd command requires number-of-threads!\n");
                return 0;
            }
            maxtd = toknum(tok, ltok);
            analyze_set_max_sampling_threads(maxtd);
        } else if (tokcmp(tok, ltok, "headroom") == 0) {
            uint64_t headroom = 0;
            tok = segtok(line, lline, &st, &ltok);
            if (ltok <= 0) {
                logmsg(LOGMSG_ERROR, "Analyze headroom command needs a %%argument\n");
                return 0;
            }
            headroom = toknum(tok, ltok);
            analyze_set_headroom(headroom);
        } else if (tokcmp(tok, ltok, "backout") == 0) {
            int maxtd = 0;
            tok = segtok(line, lline, &st, &ltok);
            char * table = NULL;
            if (ltok > 0) 
                table = tokdup(tok, ltok);
            SBUF2 *sb = sbuf2open(fileno(stdout), 0);
            handle_backout(sb, table);
            if(table) free(table);
        } else if(tokcmp(tok,ltok,"abort") == 0) {
            if(!analyze_is_running()) {
                logmsg(LOGMSG_ERROR, "Analyze is not running [or not running on this node].\n");
                return 0;
            }

            logmsg(LOGMSG_USER, "Abort ongoing analyze\n");
            void set_analyze_abort_requested();
            set_analyze_abort_requested();
        } else {
            logmsg(LOGMSG_ERROR, "unknown command <%.*s>\n", ltok, tok);
        }
        return 0;
    } else if (tokcmp(tok, ltok, "memdebug") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            print_help_page(HELP_MEMDEBUG);
        } else if (tokcmp(tok, ltok, "callers") == 0) {
            tok = segtok(line, lline, &st, &ltok);
            if (ltok == 0)
                memdebug_dump_callers(stderr, 1);
            else {
                char *fname;
                FILE *f;
                fname = tokdup(tok, ltok);
                f = fopen(fname, "w");
                if (f == NULL) {
                    logmsg(LOGMSG_ERROR, "Can't open %s: %s\n", fname,
                            strerror(errno));
                    free(fname);
                    return 0;
                }
                memdebug_dump_callers(f, 1);
                fclose(f);
                logmsg(LOGMSG_ERROR, "Dumped callers to %s\n", fname);
                free(fname);
            }
        } else if (tokcmp(tok, ltok, "blocks") == 0) {
            tok = segtok(line, lline, &st, &ltok);
            if (ltok == 0)
                memdebug_dump_blocks(stderr);
            else {
                char *fname;
                FILE *f;
                fname = tokdup(tok, ltok);
                f = fopen(fname, "w");
                if (f == NULL) {
                    logmsg(LOGMSG_ERROR, "Can't open %s: %s\n", fname,
                            strerror(errno));
                    free(fname);
                    return 0;
                }
                memdebug_dump_blocks(f);
                fclose(f);
                logmsg(LOGMSG_USER, "Dumped allocated blocks to %s\n", fname);
                free(fname);
            }
        } else
            print_help_page(HELP_MEMDEBUG);
        return 0;
    } else if (tokcmp(tok, ltok, "bdbscan") == 0) {
        char *tbl;
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "expected table name\n");
            return 1;
        }
        tbl = tokdup(tok, ltok);
        debug_traverse_data(tbl);
        free(tbl);
    } else if (tokcmp(tok, ltok, "bdbbulkscan") == 0) {
        char *tbl;
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "expected table name\n");
            return 1;
        }
        tbl = tokdup(tok, ltok);
        debug_bulktraverse_data(tbl);
        free(tbl);
    } else if (tokcmp(tok, ltok, "isthreadalive") == 0) {
        int rc;
        pthread_t tid;

        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "expected thread id\n");
            return 1;
        }
        tid = toknum(tok, ltok);
        rc = pthread_kill(tid, 0);
        logmsg(LOGMSG_USER, "kill tid %d rc %d\n", tid, rc);
    } else if (tokcmp(tok, ltok, "chkpoint_alarm_time") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (ltok > 0) {
            gbl_chkpoint_alarm_time = toknum(tok, ltok);
        }
        logmsg(LOGMSG_USER, "Checkpoint thread hang alarm time is %d seconds\n",
               gbl_chkpoint_alarm_time);
    } else if (tokcmp(tok, ltok, "incoherent_msg_freq") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (ltok > 0) {
            gbl_incoherent_msg_freq = toknum(tok, ltok);
        }
        logmsg(LOGMSG_USER, "Incoherent message freq is %d seconds\n",
               gbl_incoherent_msg_freq);
    } else if (tokcmp(tok, ltok, "incoherent_alarm_time") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (ltok > 0) {
            gbl_incoherent_alarm_time = toknum(tok, ltok);
        }
        logmsg(LOGMSG_USER, "Incoherent alarm time is %d seconds\n",
               gbl_incoherent_alarm_time);
    } else if (tokcmp(tok, ltok, "max_incoherent_nodes") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (ltok > 0) {
            gbl_max_incoherent_nodes = toknum(tok, ltok);
        }
        logmsg(LOGMSG_USER, "Max incoherent nodes allowed before alarm is %d\n",
               gbl_max_incoherent_nodes);
    } else if (tokcmp(tok, ltok, "sleep") == 0) {
        /* Undocumented debugging trap, in case you were wondering */
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "expected seconds to sleep for\n");
        } else {
            int secs = toknum(tok, ltok);
            int ii;
            for (ii = 1; ii <= secs; ii++) {
                logmsg(LOGMSG_USER, "sleeping %d/%d\n", ii, secs);
                sleep(1);
            }
            logmsg(LOGMSG_USER, "Done sleeping\n");
        }
    } else if (tokcmp(tok, ltok, "help") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
           logmsg(LOGMSG_USER, "%s\n", plink_constant(PLINK_TIME));
            print_help_page(HELP_MAIN);
        } else if (tokcmp(tok, ltok, "java") == 0) {
            print_help_page(HELP_JAVA);
        } else if (tokcmp(tok, ltok, "queue") == 0) {
            print_help_page(HELP_QUEUES);
        } else if (tokcmp(tok, ltok, "stat") == 0) {
            print_help_page(HELP_STAT);
        } else if (tokcmp(tok, ltok, "compr") == 0) {
            print_help_page(HELP_COMPR);
        } else if (tokcmp(tok, ltok, "analyze") == 0) {
            print_help_page(HELP_ANALYZE);
        } else if (tokcmp(tok, ltok, "bdb") == 0) {
            backend_cmd(dbenv, "help", 4, 0);
        } else if (tokcmp(tok, ltok, "schema") == 0) {
            print_help_page(HELP_SCHEMA);
        } else if (tokcmp(tok, ltok, "sql") == 0) {
            print_help_page(HELP_SQL);
        } else if (tokcmp(tok, ltok, "memdebug") == 0) {
            print_help_page(HELP_MEMDEBUG);
        } else if (tokcmp(tok, ltok, "memstat") == 0) {
            print_help_page(HELP_MEMSTAT);
        } else if (tokcmp(tok, ltok, "reql") == 0) {
            reqlog_help();
        }
    } else if (tokcmp(tok, ltok, "use_bbipc_fastseed") == 0) {
        gbl_use_bbipc_global_fastseed = 1;
        logmsg(LOGMSG_USER, "Using bbipc_global_fastseed.\n");
    } else if (tokcmp(tok, ltok, "dont_use_bbipc_fastseed") == 0) {
        gbl_use_bbipc_global_fastseed = 0;
        logmsg(LOGMSG_USER, "Disabling bbipc_global_fastseed.\n");
    } else if (tokcmp(tok, ltok, "appsockpool") == 0) {
        thdpool_process_message(gbl_appsock_thdpool, line, lline, st);
    } else if (tokcmp(tok, ltok, "sqlenginepool") == 0) {
        thdpool_process_message(gbl_sqlengine_thdpool, line, lline, st);
    } else if (tokcmp(tok, ltok, "osqlpfaultpool") == 0) {
        thdpool_process_message(gbl_osqlpfault_thdpool, line, lline, st);
    } else if (tokcmp(tok, ltok, "udppfaultpool") == 0) {
        thdpool_process_message(gbl_udppfault_thdpool, line, lline, st);
    } else if (tokcmp(tok, ltok, "pgcompactpool") == 0) {
        thdpool_process_message(gbl_pgcompact_thdpool, line, lline, st);
    } else if (tokcmp(tok, ltok, "oldestgenids") == 0) {
        int i, stripe;
        void *buf = malloc(64 * 1024);
        int rc;
        struct ireq iq;
        int reclen;
        unsigned long long genid;
        int bdberr;
        init_fake_ireq(thedb, &iq);
        for (i = 0; i < thedb->num_dbs; i++) {
            iq.usedb = thedb->dbs[i];
            for (stripe = 0; stripe < gbl_dtastripe; stripe++) {
                uint8_t ver;
                rc = bdb_find_oldest_genid(iq.usedb->handle, NULL, stripe, buf,
                                           &reclen, 64 * 1024, &genid, &ver,
                                           &bdberr);
               logmsg(LOGMSG_USER, "%s stripe %d ", iq.usedb->dbname, stripe);
                if (rc == 0)
                   logmsg(LOGMSG_USER, "%016llx %d", genid, bdb_genid_timestamp(genid));
                else if (rc == 1)
                    logmsg(LOGMSG_USER, "no records");
                else
                    logmsg(LOGMSG_ERROR, "error rc %d bdberr %d", rc, bdberr);
                logmsg(LOGMSG_USER, "\n");
            }
        }
    }

    else if (tokcmp(tok, ltok, "exclusive_blockop_qconsume") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
           logmsg(LOGMSG_USER, "gbl_exclusive_blockop_qconsume is %s\n",
                   gbl_exclusive_blockop_qconsume ? "on" : "off");
            return 0;
        }
        if (tokcmp(tok, ltok, "on") == 0) {
           logmsg(LOGMSG_USER, "Enabled gbl_exclusive_blockop_qconsume\n");
            gbl_exclusive_blockop_qconsume = 1;
        } else if (tokcmp(tok, ltok, "off") == 0) {
           logmsg(LOGMSG_USER, "Disabled gbl_exclusive_blockop_qconsume\n");
            gbl_exclusive_blockop_qconsume = 0;
        } else {
           logmsg(LOGMSG_USER, "gbl_exclusive_blockop_qconsume is %s\n",
                   gbl_exclusive_blockop_qconsume ? "on" : "off");
        }
    } else if (tokcmp(tok, ltok, "getfilever") == 0) {
        char *table_name = NULL;
        struct db *db;
        int bdberr, rc, is_file_type_dta = 0, file_num;
        unsigned long long file_version;

        /*get file type*/
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "usage getfilever ix|dta num tablename\n");
            return 1;
        }
        if (tokcmp(tok, ltok, "dta") == 0)
            is_file_type_dta = 1;
        else if (tokcmp(tok, ltok, "ix") != 0) {
            logmsg(LOGMSG_ERROR, "usage getfilever ix|dta num tablename\n");
            return 1;
        }

        /*get file number*/
        file_num = toknum(tok, ltok);

        /*get table*/
        table_name = tokdup(tok, ltok);
        db = getdbbyname(table_name);
        free(table_name);
        table_name = NULL;
        if (db == NULL) {
            logmsg(LOGMSG_ERROR, "usage getfilever ix|dta num tablename\n");
            return 1;
        }

        if (is_file_type_dta)
            rc = bdb_get_file_version_data(db->handle, NULL, file_num,
                                           &file_version, &bdberr);
        else
            rc = bdb_get_file_version_index(db->handle, NULL, file_num,
                                            &file_version, &bdberr);

        if (rc || bdberr != BDBERR_NOERROR)
            logmsg(LOGMSG_ERROR, "bdb_get_file_version failed with rc: %d, ",
                    bdberr);
        else {
            logmsg(LOGMSG_USER, "bdb_get_file_version succeeded: %016llx\n", file_version);
        }
    } else if (tokcmp(tok, ltok, "newfilever") == 0) {
        /*TODO add ability to specify version_num*/
        char *table_name = NULL;
        struct db *db;
        int bdberr, rc, is_file_type_dta = 0, file_num;
        unsigned long long file_version;

        /*get file type*/
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "usage newfilever ix|dta num tablename\n");
            return 1;
        }
        if (tokcmp(tok, ltok, "dta") == 0)
            is_file_type_dta = 1;
        else if (tokcmp(tok, ltok, "ix") != 0) {
            logmsg(LOGMSG_ERROR, "usage newfilever ix|dta num tablename\n");
            return 1;
        }

        /*get file number*/
        file_num = toknum(tok, ltok);

        /*get table*/
        table_name = tokdup(tok, ltok);
        db = getdbbyname(table_name);
        free(table_name);
        table_name = NULL;
        if (db == NULL) {
            logmsg(LOGMSG_ERROR, "usage newfilever ix|dta num tablename\n");
            return 1;
        }

        file_version = bdb_get_cmp_context(dbenv->bdb_env);

        if (is_file_type_dta)
            rc = bdb_new_file_version_data(db->handle, NULL, file_num,
                                           file_version, &bdberr);
        else
            rc = bdb_new_file_version_index(db->handle, NULL, file_num,
                                            file_version, &bdberr);

        if (rc || bdberr != BDBERR_NOERROR)
            logmsg(LOGMSG_ERROR, "bdb_new_file_version failed with rc: %d, ",
                    bdberr);
        else
            logmsg(LOGMSG_USER, "bdb_new_file_version succeeded: %016llx\n", file_version);
    } else if (tokcmp(tok, ltok, "usellmeta") == 0) {
        logmsg(LOGMSG_USER, "All dbs using this version of comdb2 use llmeta\n");
    } else if (tokcmp(tok, ltok, "dumpllmeta") == 0) {
        llmeta_dump_mapping(thedb);
    } else if (tokcmp(tok, ltok, "sql_time_threshold") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
           logmsg(LOGMSG_USER, "SQL warn threshold is %dms\n", gbl_sql_time_threshold);
            return 0;
        }
        gbl_sql_time_threshold = toknum(tok, ltok);
        logmsg(LOGMSG_USER, "Set SQL warn threshold to %dms\n", gbl_sql_time_threshold);
    } else if (tokcmp(tok, ltok, "toblock_net_throttle") == 0) {
        gbl_toblock_net_throttle = 1;
        logmsg(LOGMSG_USER, "I will throttle my writes in apply_changes\n");
    } else if (tokcmp(tok, ltok, "no_toblock_net_throttle") == 0) {
        gbl_toblock_net_throttle = 0;
        logmsg(LOGMSG_USER, "I will not throttle my writes in apply_changes\n");
    } else if (tokcmp(tok, ltok, "enque_flush_interval") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected time value for enque_flush_interval\n");
            return 0;
        }
        gbl_enque_flush_interval = toknum(tok, ltok);

        logmsg(LOGMSG_USER, "net_set_enque_flush_interval %d\n",
                gbl_enque_flush_interval);
        net_set_enque_flush_interval(thedb->handle_sibling,
                                     gbl_enque_flush_interval);
    }

    else if (tokcmp(tok, ltok, "slow_rep_process_txn_maxms") == 0) {
        extern int gbl_slow_rep_process_txn_maxms;
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected value for slow_rep_process_txn_maxms\n");
            return 0;
        }
        gbl_slow_rep_process_txn_maxms = toknum(tok, ltok);
        logmsg(LOGMSG_USER, "slow_rep_process_txn_maxms set to %d ms\n",
                gbl_slow_rep_process_txn_maxms);
    }

    else if (tokcmp(tok, ltok, "slow_rep_process_txn_freq") == 0) {
        extern int gbl_slow_rep_process_txn_freq;
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected value for slow_rep_process_txn_freq\n");
            return 0;
        }
        gbl_slow_rep_process_txn_freq = toknum(tok, ltok);
        logmsg(LOGMSG_USER, "slow_rep_process_txn_freq set to %d ms\n",
                gbl_slow_rep_process_txn_freq);
    }

    else if (tokcmp(tok, ltok, "netuse") == 0) {
        unsigned long long read, written, waits, reorders;
        int rc;
        const char *hosts[REPMAX];
        int num_nodes;
        rc = net_get_network_usage(thedb->handle_sibling, &written, &read,
                                   &waits, &reorders);
        logmsg(LOGMSG_USER, 
            "Read: %llu    Written: %llu    Throttles: %llu   Reorders: %llu\n",
            read, written, waits, reorders);
        num_nodes = net_get_all_nodes(thedb->handle_sibling, hosts);
        if (num_nodes > 0) {
            int i;
            const char *host;
            logmsg(LOGMSG_USER, "%5s %15s %15s %15s %15s\n", "Node", "Read", "Written",
                   "Throttles", "Reorders");
            for (i = 0; i < num_nodes; i++) {
                host = hosts[i];
                rc = net_get_host_network_usage(thedb->handle_sibling, host,
                                                &written, &read, &waits,
                                                &reorders);
                if (rc == 0)
                    logmsg(LOGMSG_USER, "%20s %15llu %15llu %15llu %15llu\n", host, read,
                           written, waits, reorders);
            }
        }
    } else if (tokcmp(tok, ltok, "sc_del_unused_files_threshold") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_USER, "gbl_sc_del_unused_files_threshold_ms is %dms\n",
                   gbl_sc_del_unused_files_threshold_ms);
            return 0;
        }

        gbl_sc_del_unused_files_threshold_ms = toknum(tok, ltok);
        logmsg(LOGMSG_USER, "setting gbl_sc_del_unused_files_threshold_ms to %dms\n",
               gbl_sc_del_unused_files_threshold_ms);
    } else if (tokcmp(tok, ltok, "dumpsqlattr") == 0) {
        sqlite3_dump_tunables();
    } else if (tokcmp(tok, ltok, "setsqlattr") == 0) {
        char *attrname = NULL;
        char *attrval = NULL;
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected sql attribute name\n");
            return 0;
        }
        attrname = tokdup(tok, ltok);
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected sql attribute name\n");
            free(attrname);
            return 0;
        }
        attrval = tokdup(tok, ltok);
        sqlite3_set_tunable_by_name(attrname, attrval);
        free(attrname);
        free(attrval);
    } else if (tokcmp(tok, ltok, "debugthreads") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "debugthreads: expected on or off\n");
            return 0;
        }
        if (tokcmp(tok, ltok, "on") == 0) {
            thread_util_enable_debug();
        } else if (tokcmp(tok, ltok, "off") == 0) {
            thread_util_disable_debug();
        } else {
            logmsg(LOGMSG_ERROR, "debugthreads: expected on or off\n");
            return 0;
        }
    } else if (tokcmp(tok, ltok, "dumpthreadonexit") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "dumpthreadonexit: expected on or off\n");
            return -1;
        }
        if (tokcmp(tok, ltok, "on") == 0) {
            thread_util_dump_on_exit_enable();
        } else if (tokcmp(tok, ltok, "off") == 0) {
            thread_util_dump_on_exit_disable();
        } else {
            logmsg(LOGMSG_ERROR, "dumpthreadonexit: expected on or off\n");
            return -1;
        }
    } else if (tokcmp(tok, ltok, "rangextlim") == 0) {
        int num;
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            if (gbl_rangextunit == 0) {
                logmsg(LOGMSG_USER, "No per-buffer limit set on old range-extract opcodes\n");
                return -1;
            } else {
                logmsg(LOGMSG_USER, "Per-buffer limit on old range-extract opcodes set to %d\n",
                    gbl_rangextunit);
            }
        }
        num = toknum(tok, ltok);
        if (num < 0) {
            logmsg(LOGMSG_ERROR, "Invalid limit\n");
            return -1;
        } else if (num == 0) {
           logmsg(LOGMSG_USER, "Disabled old range extract per-buffer limits\n");
        } else {
           logmsg(LOGMSG_USER, "Old rangeextract per-buffer limit set to %d records\n",
                   num);
        }
        gbl_rangextunit = num;
    } else if (tokcmp(tok, ltok, "oldrangexlim") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            if (gbl_honor_rangextunit_for_old_apis)
               logmsg(LOGMSG_USER, "Not honoring limit for old rangeextract calls\n");
            else
               logmsg(LOGMSG_USER, "Honoring limit for old rangeextract calls\n");
        } else if (tokcmp(tok, ltok, "on") == 0) {
            logmsg(LOGMSG_USER, "Will honor limit for old rangextract calls\n");
            gbl_honor_rangextunit_for_old_apis = 1;
        } else if (tokcmp(tok, ltok, "off") == 0) {
            logmsg(LOGMSG_ERROR, "Won't honor limit for old rangextract calls\n");
            gbl_honor_rangextunit_for_old_apis = 0;
        } else {
            logmsg(LOGMSG_ERROR, "Expected on/off for oldrangexlim\n");
            return -1;
        }
    } else if (tokcmp(tok, ltok, "setthrottle") == 0) {
        extern int throttle_lim;
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0)
            return -1;
        throttle_lim = toknum(tok, ltok);
        logmsg(LOGMSG_USER, "set tthrottle number to %d\n", throttle_lim);
    } else if (tokcmp(tok, ltok, "setthrottlecpu") == 0) {
        extern int cpu_throttle_threshold;
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0)
            return -1;
        cpu_throttle_threshold = toknum(tok, ltok);
        logmsg(LOGMSG_USER, "will tthrottle at %%%d\n", cpu_throttle_threshold);
    } else if (tokcmp(tok, ltok, "setthrottlesleeptime") == 0) {
        extern int throttle_sleep_time;
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0)
            return -1;
        throttle_sleep_time = toknum(tok, ltok);
        logmsg(LOGMSG_USER, "will sleep for %d ms\n", throttle_sleep_time);
    } else if (tokcmp(tok, ltok, "resetsetthrottle") == 0) {
        set_throttle(0);
    } else if (tokcmp(tok, ltok, "trackcursors") == 0) {
        on_off_trap(line, lline, &st, &ltok, "cursor tracking", "trackcursors",
                    &gbl_berk_track_cursors);
    } else if (tokcmp(tok, ltok, "querylimit") == 0) {
        query_limit_cmd(line, lline, st);
    } else if (tokcmp(tok, ltok, "enable_good_sql_return_codes") == 0) {
        if (!gbl_enable_good_sql_return_codes) {
            gbl_enable_good_sql_return_codes = 1;
            comdb2_shm_set_flag(thedb->dbnum, CMDB2_SHMFLG_GOODSQLCODES);
            for (i = 0; i < thedb->num_dbs; i++) {
                comdb2_shm_set_flag(thedb->dbs[i]->dbnum,
                                    CMDB2_SHMFLG_GOODSQLCODES);
            }
            logmsg(LOGMSG_USER, "Enabled good sql return codes\n");

        } else
            logmsg(LOGMSG_USER, "Good sql return codes already enabled\n");
    } else if (tokcmp(tok, ltok, "disable_good_sql_return_codes") == 0) {
        if (gbl_enable_good_sql_return_codes) {
            gbl_enable_good_sql_return_codes = 0;
            comdb2_shm_clr_flag(thedb->dbnum, CMDB2_SHMFLG_GOODSQLCODES);
            for (i = 0; i < thedb->num_dbs; i++) {
                comdb2_shm_clr_flag(thedb->dbs[i]->dbnum,
                                    CMDB2_SHMFLG_GOODSQLCODES);
            }
            logmsg(LOGMSG_USER, "Disabled good sql return codes\n");
        } else
            logmsg(LOGMSG_USER, "Good sql return codes already disabled\n");
    } else if (tokcmp(tok, ltok, "maxretries") == 0) {
        int n;
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_USER, "maxretries: %d\n", gbl_maxretries);
            return 0;
        }
        n = toknum(tok, ltok);
        if (n < 2) {
            logmsg(LOGMSG_USER, "Invalid setting for maxretries\n");
            return 0;
        }
        gbl_maxretries = n;
        logmsg(LOGMSG_USER, "Set max retries to %d\n", gbl_maxretries);
    } else if (tokcmp(tok, ltok, "maxblobretries") == 0) {
        int n;
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_USER, "maxblobretries: %d\n", gbl_maxblobretries);
            return 0;
        }
        n = toknum(tok, ltok);
        if (n < 2) {
            logmsg(LOGMSG_ERROR, "Invalid setting for maxblobretries\n");
            return 0;
        }
        gbl_maxblobretries = n;
        logmsg(LOGMSG_USER, "Set max blob retries to %d\n", gbl_maxblobretries);
    }

    else if (tokcmp(tok, ltok, "deadlock") == 0) {

        unsigned long long rep_retry = 0;
        unsigned long long msgs_processed = 0;
        unsigned long long msgs_sent = 0;
        unsigned long long txns_applied = 0;
        int max_retries = 0;
        int onoff;
        int retry_limit;

        tok = segtok(line, lline, &st, &ltok);

        retry_limit = berkdb_get_max_rep_retries();

        onoff = bdb_attr_get(thedb->bdb_attr,
                             BDB_ATTR_DEADLOCK_WRITERS_WITH_LEAST_WRITES);
        if (onoff == 0) {
            logmsg(LOGMSG_USER, "DEADLOCK_WRITERS_WITH_LEAST_WRITES is disabled\n");
            return 0;
        }

        if (ltok == 0) {
            bdb_get_rep_stats(dbenv->dbs[0]->handle, &msgs_processed,
                              &msgs_sent, &txns_applied, &rep_retry,
                              &max_retries);
            logmsg(LOGMSG_USER, "Retries: %lld, max %d, limit %d\n", rep_retry, max_retries,
                   retry_limit);
        } else if (tokcmp(tok, ltok, "help") == 0) {
            logmsg(LOGMSG_USER, 
                "off      : disable DEADLOCK_WRITERS_WITH_LEAST_WRITES mode\n"
                "on       : enable DEADLOCK_WRITERS_WITH_LEAST_WRITES mode\n"
                "limit n  : set max retries before auto-disable (current %d)\n",
                retry_limit);
        } else if (tokcmp(tok, ltok, "on") == 0) {
            logmsg(LOGMSG_USER, "Enabled DEADLOCK_WRITERS_WITH_LEAST_WRITES\n");
            bdb_attr_set(thedb->bdb_attr,
                         BDB_ATTR_DEADLOCK_WRITERS_WITH_LEAST_WRITES, 1);
        } else if (tokcmp(tok, ltok, "off") == 0) {
            logmsg(LOGMSG_USER, "Disabled DEADLOCK_WRITERS_WITH_LEAST_WRITES\n");
            bdb_attr_set(thedb->bdb_attr,
                         BDB_ATTR_DEADLOCK_WRITERS_WITH_LEAST_WRITES, 0);
        } else if (tokcmp(tok, ltok, "limit") == 0) {
            int lim;
            tok = segtok(line, lline, &st, &ltok);
            if (ltok == 0) {
                logmsg(LOGMSG_ERROR, "Expected value for deadlock limit\n");
                return 0;
            }
            lim = toknum(tok, ltok);
            if (lim <= 0) {
                logmsg(LOGMSG_ERROR, "Invalid value for deadlock limit\n");
                return 0;
            }
            berkdb_set_max_rep_retries(lim);
            logmsg(LOGMSG_USER, "Set deadlock retry limit to %d\n", lim);
        }
    } else if (tokcmp(tok, ltok, "deadlock_rep_retry_max") == 0) {
        int lim;
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_USER, "Expected value for deadlock limit\n");
            return 0;
        }
        lim = toknum(tok, ltok);
        if (lim <= 0) {
            logmsg(LOGMSG_ERROR, "Invalid value for deadlock limit\n");
            return 0;
        }
        berkdb_set_max_rep_retries(lim);
        logmsg(LOGMSG_ERROR, "Set deadlock retry limit to %d\n", lim);
    } else if (tokcmp(tok, ltok, "enable_cache_internal_nodes") == 0) {
        gbl_enable_cache_internal_nodes = 1;
        logmsg(LOGMSG_USER, "Will increase cache-priority for btree internal nodes.\n");
    } else if (tokcmp(tok, ltok, "badwrite_intvl") == 0) {
        int tmp;
        tok = segtok(line, lline, &st, &ltok);
        if (ltok <= 0) {
            logmsg(LOGMSG_ERROR, "Expected value for badwrite_intvl\n");
            return 0;
        }
        tmp = toknum(tok, ltok);
        if (tmp >= 0) {
            logmsg(LOGMSG_ERROR, "Will force a bad-write and abort randomly every %d "
                   "pgwrites.\n",
                   tmp);
            gbl_test_badwrite_intvl = tmp;
        } else {
            logmsg(LOGMSG_ERROR, "Invalid badwrite_intvl.\n");
        }
    }

    else if (tokcmp(tok, ltok, "skip_ratio_trace") == 0) {
        logmsg(LOGMSG_USER, "Enabled skip-ratio trace.\n");
        gbl_skip_ratio_trace = 1;
    } else if (tokcmp(tok, ltok, "no_skip_ratio_trace") == 0) {
        logmsg(LOGMSG_USER, "Disabled skip-ratio trace.\n");
        gbl_skip_ratio_trace = 0;
    }

    else if (tokcmp(tok, ltok, "test_blob_race") == 0) {
        int tmp;
        tok = segtok(line, lline, &st, &ltok);
        if (ltok <= 0) {
            logmsg(LOGMSG_USER, "Expected value for test_blob_race\n");
            return 0;
        }
        tmp = toknum(tok, ltok);
        if (tmp >= 1) {
            logmsg(LOGMSG_USER, "Will force a blob-race condition once every %d lookups.\n",
                   tmp);
            gbl_test_blob_race = tmp;
        } else if (tmp == 0) {
            logmsg(LOGMSG_USER, "Disabled blob-race testing.\n");
            gbl_test_blob_race = 0;
        } else {
            logmsg(LOGMSG_ERROR, "Invalid test_blob_race value: should be above 2 (or 0 to "
                   "disable).\n");
        }
    }

    else if (tokcmp(tok, ltok, "disable_cache_internal_nodes") == 0) {
        gbl_enable_cache_internal_nodes = 0;
        logmsg(LOGMSG_USER, "Will treat btree internal nodes the same as leaf-nodes.\n");
    } else if (tokcmp(tok, ltok, "instant_schema_change") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (tokcmp(tok, ltok, "on") == 0)
            gbl_init_with_instant_sc = 1;
        else if (tokcmp(tok, ltok, "off") == 0)
            gbl_init_with_instant_sc = 0;
        logmsg(LOGMSG_ERROR, "Instant schema change: %s\n",
                gbl_init_with_instant_sc ? "On" : "Off");
    } else if (tokcmp(tok, ltok, "getver") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected table name\n");
            return 0;
        }
        char *dbname = tokdup(tok, ltok);
        struct db *db = getdbbyname(dbname);
        if (db) {
            logmsg(LOGMSG_USER, "table:%s  odh:%s  instant_schema_change:%s  "
                   "inplace_updates:%s  version:%d\n",
                   db->dbname, YESNO(db->instant_schema_change),
                   YESNO(db->inplace_updates), YESNO(db->odh), db->version);
        } else {
            logmsg(LOGMSG_ERROR, "no such table: %s\n", dbname);
        }
        free(dbname);
        return 0;
    } else if (tokcmp(tok, ltok, "setver") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected table name\n");
            return 0;
        }
        char *dbname = tokdup(tok, ltok);

        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected table version\n");
            free(dbname);
            return 0;
        }
        int ver = toknum(tok, ltok);
        if (ver < 1 || ver > 255) {
            logmsg(LOGMSG_ERROR, "Table version can be [1-255]\n");
            free(dbname);
            return 0;
        }
        struct db *db = getdbbyname(dbname);
        if (db) {
            db->version = ver;
            bdb_set_csc2_version(db->handle, db->version);
        } else {
            logmsg(LOGMSG_ERROR, "no such table: %s\n", dbname);
        }
        free(dbname);
        return 0;
    } else if (tokcmp(tok, ltok, "setipu") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected table name\n");
            return 0;
        }
        char *dbname = tokdup(tok, ltok);
        struct db *db = getdbbyname(dbname);
        if (db == NULL) {
            logmsg(LOGMSG_ERROR, "No such table: %s\n", dbname);
            goto out;
        }
        if (!db->odh) {
            logmsg(LOGMSG_ERROR, "No ODH support for table: %s\n", dbname);
            goto out;
        }

        int state;
        tok = segtok(line, lline, &st, &ltok);
        if (tokcmp(tok, ltok, "on") == 0) {
            state = 1;
        } else if (tokcmp(tok, ltok, "off") == 0) {
            state = 0;
        } else {
            logmsg(LOGMSG_ERROR, "Expected on/off\n");
            goto out;
        }
        db->inplace_updates = state;
        bdb_set_inplace_updates(db->handle, state);
        put_db_inplace_updates(db, NULL, state);
        logmsg(LOGMSG_USER, "inplace update turned %s for table: %s\n", state ? "on" : "off",
               dbname);

    out:
        free(dbname);
        return 0;
    } else if (tokcmp(tok, ltok, "testrep") == 0) {
        int nitems;
        int size;
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0)
            goto testrep_usage;
        nitems = toknum(tok, ltok);
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0)
            goto testrep_usage;
        size = toknum(tok, ltok);

        testrep(nitems, size);
        return 0;

    testrep_usage:
        logmsg(LOGMSG_ERROR, "Usage: testrep num_items item_size\n");
    } else if (tokcmp(tok, ltok, "random_lock_release_interval") == 0) {
        int tmp;
        tok = segtok(line, sizeof(line), &st, &ltok);

        if (ltok <= 0) {
            logmsg(LOGMSG_ERROR, "Expected value for random_release_locks_interval\n");
            return 0;
        }

        tmp = toknum(tok, ltok);
        if (tmp >= 0) {
            logmsg(LOGMSG_USER, "Will release locks randomly an average once every %d checks\n",
                tmp);
            gbl_sql_random_release_interval = tmp;
        } else {
            logmsg(LOGMSG_USER, "Disabled random release-locks\n");
            gbl_sql_random_release_interval = 0;
        }
    } else if (tokcmp(tok, ltok, "osql_verify_retry_max") == 0) {
        int tmp;
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected value for blocksql_verify_retry_max\n");
            return 0;
        }
        tmp = toknum(tok, ltok);
        if (tmp >= 0) {
            logmsg(LOGMSG_USER, "Osql transaction will repeat %d times if verify error (was "
                   "%d times)\n",
                   tmp, gbl_osql_verify_retries_max);
            gbl_osql_verify_retries_max = tmp;
        }
    } else if (tokcmp(tok, ltok, "osql_verify_ext_chk") == 0) {
        int tmp;
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected value for osql_verify_ext_chk\n");
            return 0;
        }
        tmp = toknum(tok, ltok);
        if (tmp >= 0) {
           logmsg(LOGMSG_USER, "Osql will do extended genid-checking after %d verify "
                   "errors (was %d)\n",
                   tmp, gbl_osql_verify_ext_chk);
            gbl_osql_verify_ext_chk = tmp;
        }
    } else if (tokcmp(tok, ltok, "iopool") == 0) {
        berkdb_iopool_process_message(line, lline, st);
    } else if (tokcmp(tok, ltok, "pageordertablescan") == 0) {
        int state;
        tok = segtok(line, lline, &st, &ltok);
        if (tokcmp(tok, ltok, "on") == 0) {
            state = 1;
        } else if (tokcmp(tok, ltok, "off") == 0) {
            state = 0;
        } else {
            logmsg(LOGMSG_ERROR, "Expected on/off\n");
            return 0;
        }
        bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_PAGE_ORDER_TABLESCAN, state);
        logmsg(LOGMSG_USER, "Page order table scan set to %s.\n", state ? "on" : "off");
    }

    /* page_order_scan per-table message trap */
    else if (tokcmp(tok, ltok, "page_order_scan") == 0) {
        char *cmd = "page_order_scan";
        struct db *db = NULL;
        char *table;

        tok = segtok(line, lline, &st, &ltok);

        /* Enable for a table */
        if (ltok && tokcmp(tok, ltok, "enable") == 0) {
            tok = segtok(line, lline, &st, &ltok);
            if (ltok == 0) {
                logmsg(LOGMSG_ERROR, "expected table-name to enable\n");
                return 0;
            }
            table = tokdup(tok, ltok);
            db = getdbbyname(table);
            if (!db) {
                logmsg(LOGMSG_ERROR, "Could not find table '%s'\n", table);
                free(table);
                return 0;
            }

            bdb_enable_page_scan_for_table(db->handle);
            logmsg(LOGMSG_USER, "Enabled page order table scan for '%s'\n", table);
            free(table);
            return 0;
        }

        /* Disable */
        if (ltok && tokcmp(tok, ltok, "disable") == 0) {
            tok = segtok(line, lline, &st, &ltok);

            if (ltok == 0) {
                logmsg(LOGMSG_USER, "expected table-name to disable\n");
                return 0;
            }

            table = tokdup(tok, ltok);
            db = getdbbyname(table);

            if (!db) {
                logmsg(LOGMSG_ERROR, "Could not find table '%s'\n", table);
                free(table);
                return 0;
            }

            bdb_disable_page_scan_for_table(db->handle);
            logmsg(LOGMSG_ERROR, "Disabled page order table scan for '%s'\n", table);
            free(table);

            return 0;
        }

        /* Stat */
        if (ltok && tokcmp(tok, ltok, "stat") == 0) {
            int ii, percent, nexts, global;

            /* Single table */
            tok = segtok(line, lline, &st, &ltok);
            if (ltok) {
                int pgscan;
                table = tokdup(tok, ltok);
                db = getdbbyname(table);
                if (!db) {
                    logmsg(LOGMSG_ERROR, "Could not find table '%s'\n", table);
                    free(table);
                    return 0;
                }

                pgscan = bdb_get_page_scan_for_table(db->handle);
                logmsg(LOGMSG_USER, "Page-order tablescan for table '%s' is %s\n", table,
                       pgscan ? "enabled" : "disabled");
                free(table);
                return 0;
            }

            logmsg(LOGMSG_USER, "-\n");

            /* All tables */
            for (ii = 0; ii < dbenv->num_dbs; ii++) {
                db = dbenv->dbs[ii];
                table = db->dbname;
                int pgscan = bdb_get_page_scan_for_table(db->handle);
                logmsg(LOGMSG_USER, "Page-order tablescan for table '%s' is %s\n", table,
                       pgscan ? "enabled" : "disabled");
            }

            /* General stats */
            logmsg(LOGMSG_USER, "-\n");

            global =
                bdb_attr_get(dbenv->bdb_attr, BDB_ATTR_PAGE_ORDER_TABLESCAN);
            percent = bdb_attr_get(dbenv->bdb_attr,
                                   BDB_ATTR_DISABLE_PGORDER_THRESHOLD);
            nexts = bdb_attr_get(dbenv->bdb_attr,
                                 BDB_ATTR_DISABLE_PGORDER_MIN_NEXTS);

            logmsg(LOGMSG_USER, "Global page order table scan flag is %s\n",
                    global ? "enabled" : "disabled");
            logmsg(LOGMSG_USER, "Disable page order scan for tables with more than %d%% "
                    "skip pages.\n",
                    percent);
            logmsg(LOGMSG_USER, "Only disable page order scan if there are at least %d pages\n",
                    nexts);
            logmsg(LOGMSG_USER, "Skip-ratio trace is %s\n",
                   gbl_skip_ratio_trace ? "on" : "off");

            return 0;
        }

        /* Set percent threshold */
        if (ltok && tokcmp(tok, ltok, "thresh") == 0) {
            int tmp;
            tok = segtok(line, lline, &st, &ltok);
            if (ltok == 0) {
                logmsg(LOGMSG_ERROR, "Expected page-order disable-threshold percentage\n");
                return 0;
            }
            tmp = toknum(tok, ltok);

            if (tmp < 0 || tmp > 100) {
                logmsg(LOGMSG_ERROR, "Invalid disable-threshold percentage\n");
                return 0;
            }

            bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_DISABLE_PGORDER_THRESHOLD,
                         tmp);
            logmsg(LOGMSG_USER, "Disable page order scan for %d%% or more skip-pages.\n",
                   tmp);
            return 0;
        }

        /* Set the minumum number of pages before disable */
        if (ltok && tokcmp(tok, ltok, "minnext") == 0) {
            int tmp;
            tok = segtok(line, lline, &st, &ltok);
            if (ltok == 0) {
                logmsg(LOGMSG_ERROR, "Expected minimum page count\n");
                return 0;
            }
            tmp = toknum(tok, ltok);

            if (tmp < 0) {
                logmsg(LOGMSG_ERROR, "Invalid minimum page count\n");
                return 0;
            }

            bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_DISABLE_PGORDER_MIN_NEXTS,
                         tmp);
           logmsg(LOGMSG_USER, "Disable page-order tablescan for tables with at least %d "
                   "pages.\n",
                   tmp);
            return 0;
        }

        if (ltok && tokcmp(tok, ltok, "skiptrc") == 0) {
            tok = segtok(line, lline, &st, &ltok);

            if (ltok && tokcmp(tok, ltok, "on") == 0) {
                gbl_skip_ratio_trace = 1;
                logmsg(LOGMSG_USER, "Enabled skip-ratio trace\n");
                return 0;
            }

            if (ltok && tokcmp(tok, ltok, "off") == 0) {
                gbl_skip_ratio_trace = 0;
                logmsg(LOGMSG_USER, "Disabled skip-ratio trace\n");
                return 0;
            }

            logmsg(LOGMSG_ERROR, "Expected 'on' or 'off' argument\n");
            return 0;
        }

        /* Help menu */
        if (ltok == 0 || tokcmp(tok, ltok, "help") == 0) {
            logmsg(LOGMSG_USER, "%s help             - this menu\n", cmd);
            logmsg(LOGMSG_USER, "%s enable  <table>  - enable page-order scans on <table>\n",
                   cmd);
            logmsg(LOGMSG_USER, "%s disable <table>  - disable page-order scans on <table>\n",
                   cmd);
            logmsg(LOGMSG_ERROR, "%s thresh  <prcnt>  - set auto-disable minimum skip/next "
                   "ratio\n",
                   cmd);
            logmsg(LOGMSG_ERROR, "%s minnext <count>  - set auto-disable minimum pages\n",
                   cmd);
            logmsg(LOGMSG_ERROR, "%s skiptrc <on|off> - enable/disable skip-trace\n", cmd);
            logmsg(LOGMSG_ERROR, "%s stat    <table>  - print enabled status for <table>\n",
                   cmd);
            logmsg(LOGMSG_ERROR, "%s stat             - print enabled status\n", cmd);
            return 0;
        }

        /* Unknown */
        logmsg(LOGMSG_ERROR, "%s : Unknown command.  Use 'page_order_scan help' for menu\n",
               cmd);
    }

    else if (tokcmp(tok, ltok, "tablescan_cache_utilization") == 0) {
        int ii;
        tok = segtok(line, lline, &st, &ltok);
        ii = toknum(tok, ltok);
        if (ii < 0 || ii > 100) {
            logmsg(LOGMSG_ERROR, "Max tablescan cache should be between 0 and 100.\n");
        } else {
            logmsg(LOGMSG_USER, "Set max tablescan cache utilization to %d.\n", ii);
            bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_TABLESCAN_CACHE_UTILIZATION,
                         ii);
        }
    } else if (tokcmp(tok, ltok, "dbreg") == 0) {
        bdb_verify_dbreg(thedb->bdb_env);
    } else if (tokcmp(tok, ltok, "delayed") == 0) {
        extern unsigned int gbl_delayed_skip;
        logmsg(LOGMSG_USER, "Number of skipped delayed_key_adds: %u\n", gbl_delayed_skip);
    } else if (tokcmp(tok, ltok, "lockspeed") == 0) {
        pthread_mutex_t lk;
        int i;
        int start, end;

        pthread_mutex_init(&lk, NULL);
        start = time_epochms();
        for (i = 0; i < 100000000; i++) {
            pthread_mutex_lock(&lk);
            pthread_mutex_unlock(&lk);
        }
        end = time_epochms();

        logmsg(LOGMSG_USER, "pthread took %dms (%d per second)\n", end - start,
               100000000 / (end - start) * 1000);
        bdb_lockspeed(dbenv->bdb_env);
    } else if (tokcmp(tok, ltok, "blocksql_over_sockets") == 0) {
        logmsg(LOGMSG_USER, "Enabled blocksql over sockets\n");
        gbl_upgrade_blocksql_to_socksql = 1;
        comdb2_shm_set_flag(thedb->dbnum, CMDB2_SHMFLG_SOSQL_DFLT);
    } else if (tokcmp(tok, ltok, "noblocksql_over_sockets") == 0) {
        logmsg(LOGMSG_USER, "Disabled blocksql over sockets\n");
        gbl_upgrade_blocksql_to_socksql = 0;
        comdb2_shm_clr_flag(thedb->dbnum, CMDB2_SHMFLG_SOSQL_DFLT);
    } else if (tokcmp(tok, ltok, "logevents") == 0) {
        extern void dump_log_event_counts(void);
        dump_log_event_counts();
    } else if (tokcmp(tok, ltok, "abort_on_in_use_rqid") == 0) {
        gbl_abort_on_clear_inuse_rqid = 1;
    } else if (tokcmp(tok, ltok, "dont_abort_on_in_use_rqid") == 0) {
        gbl_abort_on_clear_inuse_rqid = 0;
    }

    else if (tokcmp(tok, ltok, "ping") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "usage: ping <all|node #>\n");
        } else if (tokcmp(tok, ltok, "all") == 0) {
            ping_all(dbenv->dbs[0]->handle);
        } else {
            ping_node(dbenv->dbs[0]->handle, internn(tok, ltok));
        }
    } else if (tokcmp(tok, ltok, "tcp") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (tokcmp(tok, ltok, "ping") == 0) {
            tok = segtok(line, lline, &st, &ltok);
            if (ltok == 0) {
                logmsg(LOGMSG_ERROR, "usage: tcp ping <all|node #>\n");
            } else if (tokcmp(tok, ltok, "all") == 0) {
                tcp_ping_all(dbenv->dbs[0]->handle);
            } else {
                tcp_ping(dbenv->dbs[0]->handle, internn(tok, ltok));
            }
        }
    } else if (tokcmp(tok, ltok, "udp") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (tokcmp(tok, ltok, "stat") == 0) {
            logmsg(LOGMSG_USER, "UDP enabled: %s\n", YESNO(gbl_udp));
            logmsg(LOGMSG_USER, "Early ACK enabled: %s\n", YESNO(gbl_early));
            udp_summary();
            tok = segtok(line, lline, &st, &ltok);
            if (ltok == 0 || tokcmp(tok, ltok, "replication") == 0) {
                print_all_udp_stat(dbenv->handle_sibling);
            } else if (tokcmp(tok, ltok, "signal") == 0) {
                print_all_udp_stat(dbenv->handle_sibling_signal);
            } else if (tokcmp(tok, ltok, "offloadsql") == 0) {
                print_all_udp_stat(osql_get_netinfo());
            } else if (tokcmp(tok, ltok, "all") == 0) {
                netinfo_type *netinfo;
                logmsg(LOGMSG_USER, "Replication:\n");
                netinfo = dbenv->handle_sibling;
                print_all_udp_stat(netinfo);

                logmsg(LOGMSG_USER, "Signal:\n");
                netinfo = dbenv->handle_sibling_signal;
                print_all_udp_stat(netinfo);

                logmsg(LOGMSG_USER, "Offloadsql:\n");
                netinfo = osql_get_netinfo();
                print_all_udp_stat(netinfo);
            } else {
                char netinfo[64];
                tokcpy0(tok, ltok, netinfo, sizeof(netinfo));
                logmsg(LOGMSG_ERROR, "Unknown netinfo: %s\n", netinfo);
            }
        } else if (tokcmp(tok, ltok, "reset") == 0) {
            udp_reset(dbenv->handle_sibling);
            udp_reset(dbenv->handle_sibling_signal);
            udp_reset(osql_get_netinfo());
        } else if (tokcmp(tok, ltok, "ping") == 0) {
            tok = segtok(line, lline, &st, &ltok);
            if (ltok == 0) {
                logmsg(LOGMSG_ERROR, "usage: udp ping <all|ip:port|node #>\n");
            } else if (tokcmp(tok, ltok, "all") == 0) {
                udp_ping_all(dbenv->dbs[0]->handle);
            } else {
                char *ip = tokdup(tok, ltok);
                if (strstr(ip, ":") != NULL) {
                    udp_ping_ip(dbenv->dbs[0]->handle, ip);
                } else {
                    udp_ping(dbenv->dbs[0]->handle, internn(tok, ltok));
                }
                free(ip);
            }
        } else if (tokcmp(tok, ltok, "on") == 0) {
            if (gbl_udp) {
                logmsg(LOGMSG_USER, "UDP is already on\n");
            } else {
                udp_reset(dbenv->handle_sibling);
                gbl_udp = 1;
                logmsg(LOGMSG_USER, "UDP turned on\n");
            }
        } else if (tokcmp(tok, ltok, "off") == 0) {
            if (gbl_udp == 0) {
                logmsg(LOGMSG_USER, "UDP is already off\n");
            } else {
                gbl_udp = 0;
                logmsg(LOGMSG_USER, "UDP turned off\n");
            }
        } else {
            logmsg(LOGMSG_USER, "usage:\n"
                   "  udp on\n"
                   "  udp off -Disable use of UDP to send ack\n"
                   "  udp stat [replication|offloadsql|all] (replication is "
                   "the default)\n"
                   "  udp reset -Rebuild host hashmap from netinfo\n"
                   "  udp stale -Resolve hostnames (don't use cache)\n"
                   "  udp ping <all|ip:port|node #>\n");
        }
    } else if (tokcmp(tok, ltok, "dumpslows") == 0) {
        logmsg(LOGMSG_USER, "slowdown for memp_fget:   %dns\n", __slow_memp_fget_ns);
        logmsg(LOGMSG_USER, "slowdown for page reads:  %dns\n", __slow_read_ns);
        logmsg(LOGMSG_USER, "slowdown for page writes: %dns\n", __slow_write_ns);
    } else if (tokcmp(tok, ltok, "slowfget") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (ltok <= 0) {
            logmsg(LOGMSG_ERROR, "Expected # for slowfget.\n");
            return 0;
        }
        __slow_memp_fget_ns = toknum(tok, ltok);
    } else if (tokcmp(tok, ltok, "slowread") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (ltok <= 0) {
            logmsg(LOGMSG_ERROR, "Expected # for slowfget.\n");
            return 0;
        }
        __slow_read_ns = toknum(tok, ltok);
    } else if (tokcmp(tok, ltok, "slowwrite") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (ltok <= 0) {
            logmsg(LOGMSG_ERROR, "Expected # for slowfget.\n");
            return 0;
        }
        __slow_write_ns = toknum(tok, ltok);
    } else if (tokcmp(tok, ltok, "ctrace_rollat") == 0) {
        int n;
        tok = segtok(line, lline, &st, &ltok);
        if (ltok <= 0) {
            logmsg(LOGMSG_ERROR, "Expected # for ctrace_rollat.\n");
            return 0;
        }
        n = toknum(tok, ltok);
        ctrace_set_rollat(n);
    } else if (tokcmp(tok, ltok, "ctrace_nlogs") == 0) {
        int n;
        tok = segtok(line, lline, &st, &ltok);
        if (ltok <= 0) {
            logmsg(LOGMSG_ERROR, "Expected # for ctrace_nlogs.\n");
            return 0;
        }
        n = toknum(tok, ltok);
        ctrace_set_nlogs(n);
    } else if (tokcmp(tok, ltok, "optimize_repdb_truncate") == 0) {
        logmsg(LOGMSG_USER, "Will use optimized repdb truncate code.\n");
        gbl_optimize_truncate_repdb = 1;
    } else if (tokcmp(tok, ltok, "dont_optimize_repdb_truncate") == 0) {
        logmsg(LOGMSG_ERROR, "Will use unoptimized repdb truncate code.\n");
        gbl_optimize_truncate_repdb = 0;
    } else if (tokcmp(tok, ltok, "ctrace_roll") == 0) {
        ctrace_roll();
    } else if (tokcmp(tok, ltok, "early") == 0) {
        if (gbl_early)
            logmsg(LOGMSG_USER, "Early ack is already on\n");
        else {
            gbl_early = 1;
            logmsg(LOGMSG_USER, "Early ack enabled\n");
        }
    } else if (tokcmp(tok, ltok, "noearly") == 0) {
        if (!gbl_early)
            logmsg(LOGMSG_USER, "Early ack already disabled\n");
        else {
            gbl_early = 0;
            logmsg(LOGMSG_USER, "Early ack disabled\n");
        }
    } else if (tokcmp(tok, ltok, "reallyearly") == 0) {
        if (gbl_reallyearly)
            logmsg(LOGMSG_USER, "Really early ack is already on\n");
        else {
            gbl_reallyearly = 1;
            logmsg(LOGMSG_USER, "Really early ack enabled\n");
        }
    } else if (tokcmp(tok, ltok, "noreallyearly") == 0) {
        if (!gbl_reallyearly)
            logmsg(LOGMSG_USER, "Really early ack already disabled\n");
        else {
            gbl_reallyearly = 0;
            logmsg(LOGMSG_USER, "Really early ack disabled\n");
        }
    } else if (tokcmp(tok, ltok, "testcompr") == 0) {
        extern int gbl_testcompr_percent;
        extern int gbl_testcompr_max;
        tok = segtok(line, lline, &st, &ltok);
        if (tokcmp(tok, ltok, "percent") == 0) {
            tok = segtok(line, lline, &st, &ltok);
            int percent = toknum(tok, ltok);
            if (percent > 0 && percent <= 100) {
                gbl_testcompr_percent = percent;
            }
        } else if (tokcmp(tok, ltok, "max") == 0) {
            tok = segtok(line, lline, &st, &ltok);
            int max = toknum(tok, ltok);
            if (max >= 0) {
                gbl_testcompr_max = max;
            }
        } else if (tokcmp(tok, ltok, "table") == 0) {
            char table[128];
            tok = segtok(line, lline, &st, &ltok);
            tokcpy0(tok, ltok, table, sizeof(table));
            FILE *f = io_override_get_std();
            SBUF2 *sb = sbuf2open(fileno((f?f:stdout)), 0);
            handle_testcompr(sb, table);
        } else {
            logmsg(LOGMSG_USER, 
                   "testcompr table <tbl> - Test compression for table tbl\n"
                   "testcompr percent <number> - Default 10%%\n"
                   "testcompr max <number> - Set to 0 to process all records; "
                   "Default 300,000\n");
        }

       logmsg(LOGMSG_USER, "Current tunables:\nCompress %d%% of the records",
               gbl_testcompr_percent);
        if (gbl_testcompr_max) {
           logmsg(LOGMSG_USER, ", upto a max of %d", gbl_testcompr_max);
        }
       logmsg(LOGMSG_USER, "\n");
    } else if (tokcmp(tok, ltok, "decimal_rounding") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (ltok > 0 && tok[0]) {
            gbl_decimal_rounding = dec_parse_rounding(tok, ltok);
           logmsg(LOGMSG_USER, "Default decimal rounding is %s\n",
                   dec_print_mode(gbl_decimal_rounding));
        } else {
            logmsg(LOGMSG_USER, 
                    "Missing option for decimal rounding, current is %s\n",
                    dec_print_mode(gbl_decimal_rounding));
        }
    } else if (tokcmp(tok, ltok, "localrep") == 0) {
        struct db *db;
        int i;
        logmsg(LOGMSG_USER, "%-30s %10s\n", "table", "localrep?");
        for (i = 0; i < thedb->num_dbs; i++) {
            db = thedb->dbs[i];
            logmsg(LOGMSG_USER, "%-30s %10s\n", db->dbname,
                   db->do_local_replication ? "YES" : "NO");
        }
    } else if (tokcmp(tok, ltok, "transtat") == 0) {
        extern void bdb_dumptrans(bdb_state_type * bdb_state);
        bdb_dumptrans(thedb->bdb_env);
    } else if (tokcmp(tok, ltok, "ddlk") == 0) {
        extern unsigned gbl_ddlk;
        tok = segtok(line, lline, &st, &ltok);
        gbl_ddlk = toknum(tok, ltok);
        if (gbl_ddlk) {
            logmsg(LOGMSG_USER, "1 in every %d lock requests will deadlock\n", gbl_ddlk);
        } else {
            logmsg(LOGMSG_USER, "DDLK generator turned off\n");
        }
    } else if (tokcmp(tok, ltok, "locktest") == 0) {
        pthread_mutex_lock(&testguard);
        bdb_locktest(thedb->bdb_env);
        pthread_mutex_unlock(&testguard);
    } else if (tokcmp(tok, ltok, "berkdelay") == 0) {
        uint32_t commit_delay_ms = 0;
        tok = segtok(line, lline, &st, &ltok);
        if (ltok > 0) {
            commit_delay_ms = toknum(tok, ltok);
            pthread_mutex_lock(&testguard);
            pthread_mutex_unlock(&testguard);
        } else {
            logmsg(LOGMSG_USER, "berkdelay requires commit-delay-ms argument\n");
        }
    } else if (tokcmp(tok, ltok, "berktest") == 0) {
        uint32_t txnsize = 0;
        tok = segtok(line, lline, &st, &ltok);
        if (ltok > 0)
            txnsize = toknum(tok, ltok);
        pthread_mutex_lock(&testguard);
        if (txnsize <= 0)
            bdb_berktest_multi(thedb->bdb_env);
        else
            bdb_berktest(thedb->bdb_env, txnsize);
        pthread_mutex_unlock(&testguard);
    } else if (tokcmp(tok, ltok, "dump_ltran_list") == 0) {
        bdb_dump_logical_tranlist(thedb->bdb_env, stderr);
    } else if (tokcmp(tok, ltok, "clear_rowlocks_stats") == 0) {
        rowlocks_clear_stats();
    } else if (tokcmp(tok, ltok, "print_rowlocks_stats") == 0) {
        rowlocks_print_stats(stdout);
    } else if (tokcmp(tok, ltok, "rep_process_txn_trace") == 0) {
        gbl_rep_process_txn_time = 1;
        logmsg(LOGMSG_USER, "Enabled rep-collect transaction trace\n");
    } else if (tokcmp(tok, ltok, "no_rep_process_txn_trace") == 0) {
        gbl_rep_process_txn_time = 0;
        logmsg(LOGMSG_ERROR, "Disabled rep-collect transaction trace\n");
    } else if (tokcmp(tok, ltok, "ack_trace") == 0) {
        enable_ack_trace();
        logmsg(LOGMSG_ERROR, "Enabled ack trace\n");
    } else if (tokcmp(tok, ltok, "no_ack_trace") == 0) {
        disable_ack_trace();
        logmsg(LOGMSG_ERROR, "Disabled ack trace\n");
    } else if (tokcmp(tok, ltok, "net_explicit_flush_trace") == 0) {
        net_enable_explicit_flush_trace();
        logmsg(LOGMSG_ERROR, "Enabled cheapstack for explicit net flush\n");
    } else if (tokcmp(tok, ltok, "no_net_explicit_flush_trace") == 0) {
        net_disable_explicit_flush_trace();
        logmsg(LOGMSG_ERROR, "Disabled cheapstack for explicit net flush\n");
    } else if (tokcmp(tok, ltok, "rowlocks_bench_logical_rectype") == 0) {
        gbl_rowlocks_bench_logical_rectype = 1;
        logmsg(LOGMSG_ERROR, "I will consider rowlocks_bench record (10019) a logical "
               "rectype\n");
    }

    else if (tokcmp(tok, ltok, "rowlocks_bench_no_logical_rectype") == 0) {
        gbl_rowlocks_bench_logical_rectype = 0;
        logmsg(LOGMSG_ERROR, "I will not consider rowlocks_bench record (10019) a logical "
               "rectype\n");
    }

    else if (tokcmp(tok, ltok, "enable_rowlock_logging") == 0) {
        gbl_disable_rowlocks_logging = 0;
        logmsg(LOGMSG_ERROR, "I perform all rowlocks logging\n");
    }

    else if (tokcmp(tok, ltok, "disable_rowlock_logging") == 0) {
        gbl_disable_rowlocks_logging = 1;
        logmsg(LOGMSG_ERROR, "I disable all rowlocks logging\n");
    }

    else if (tokcmp(tok, ltok, "enable_rowlock_locking") == 0) {
        gbl_disable_rowlocks = 0;
        logmsg(LOGMSG_ERROR, "I acquire all rowlocks\n");
    } else if (tokcmp(tok, ltok, "disable_rowlock_locking") == 0) {
        gbl_disable_rowlocks = 1;
        logmsg(LOGMSG_ERROR, "I will not actually acquire any rowlocks (but will still "
               "follow the codepath)\n");
    }

    else if (tokcmp(tok, ltok, "dispatch_bench") == 0) {
        gbl_dispatch_rowlocks_bench = 1;
        logmsg(LOGMSG_ERROR, 
               "I will dispatch rowlocks_bench record (10019) to db_dispatch\n");
    } else if (tokcmp(tok, ltok, "dont_dispatch_bench") == 0) {
        gbl_dispatch_rowlocks_bench = 0;
        logmsg(LOGMSG_USER, "I will not dispatch rowlocks_bench record (10019) to "
               "db_dispatch\n");
    } else if (tokcmp(tok, ltok, "disable_rowlocks_sleepns") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (ltok > 0) {
            int sleepns = toknum(tok, ltok);
            if (sleepns >= 0)
                gbl_disable_rowlocks_sleepns = sleepns;
        }
       logmsg(LOGMSG_USER, "disable_rowlocks_sleepns is %d\n",
               gbl_disable_rowlocks_sleepns);
    } else if (tokcmp(tok, ltok, "commit_bench") == 0) {
        int tcnt = 0;
        int cnt = 0;
        tok = segtok(line, lline, &st, &ltok);
        if (ltok > 0) {
            tcnt = toknum(tok, ltok);
            tok = segtok(line, lline, &st, &ltok);
            if (ltok > 0)
                cnt = toknum(tok, ltok);
        }
        if (thedb->master != gbl_mynode) {
            logmsg(LOGMSG_ERROR, "I am not the master node\n");
        } else if (tcnt <= 0 || cnt <= 0) {
            logmsg(LOGMSG_ERROR, 
                   "commit_bench requires txn-count & iters-per-txn count\n");
        } else {
            pthread_mutex_lock(&testguard);
            commit_bench(thedb->bdb_env, tcnt, cnt);
            pthread_mutex_unlock(&testguard);
        }
    } else if (tokcmp(tok, ltok, "rowlocks_bench") == 0) {
        int lcnt = 0;
        int pcnt = 0;
        tok = segtok(line, lline, &st, &ltok);
        if (ltok > 0) {
            lcnt = toknum(tok, ltok);
            tok = segtok(line, lline, &st, &ltok);
            if (ltok > 0)
                pcnt = toknum(tok, ltok);
        }
        if (thedb->master != gbl_mynode) {
            logmsg(LOGMSG_ERROR, "I am not the master node\n");
        } else if (!gbl_rowlocks) {
            logmsg(LOGMSG_ERROR, "I am not in rowlocks mode\n");
        } else if (lcnt <= 0 || pcnt <= 0) {
            logmsg(LOGMSG_ERROR, "rowlocks_bench requires ltxn-count & ptxn-count\n");
        } else {
            pthread_mutex_lock(&testguard);
            rowlocks_bench(thedb->bdb_env, lcnt, pcnt);
            pthread_mutex_unlock(&testguard);
        }
    } else if (tokcmp(tok, ltok, "rowlocks_lock1_bench") == 0) {
        int lcnt = 0;
        int pcnt = 0;
        tok = segtok(line, lline, &st, &ltok);
        if (ltok > 0) {
            lcnt = toknum(tok, ltok);

            tok = segtok(line, lline, &st, &ltok);
            if (ltok > 0) {
                pcnt = toknum(tok, ltok);
            }
        }
        if (thedb->master != gbl_mynode) {
            logmsg(LOGMSG_ERROR, "I am not the master node\n");
        } else if (!gbl_rowlocks) {
            logmsg(LOGMSG_ERROR, "I am not in rowlocks mode\n");
        } else if (lcnt <= 0 || pcnt <= 0) {
            logmsg(LOGMSG_ERROR, "rowlocks_lock1_bench requires ltxn-count & ptxn-count\n");
        } else {
            pthread_mutex_lock(&testguard);
            rowlocks_lock1_bench(thedb->bdb_env, lcnt, pcnt);
            pthread_mutex_unlock(&testguard);
        }
    }

    else if (tokcmp(tok, ltok, "rowlocks_lock2_bench") == 0) {
        int lcnt = 0;
        int pcnt = 0;
        tok = segtok(line, lline, &st, &ltok);
        if (ltok > 0) {
            lcnt = toknum(tok, ltok);

            tok = segtok(line, lline, &st, &ltok);
            if (ltok > 0) {
                pcnt = toknum(tok, ltok);
            }
        }
        if (thedb->master != gbl_mynode) {
            logmsg(LOGMSG_ERROR, "I am not the master node\n");
        } else if (!gbl_rowlocks) {
            logmsg(LOGMSG_ERROR, "I am not in rowlocks mode\n");
        } else if (lcnt <= 0 || pcnt <= 0) {
            logmsg(LOGMSG_ERROR, 
                   "rowlocks_lock2_bench requires ltxn-count & ptxn-count\n");
        } else {
            pthread_mutex_lock(&testguard);
            rowlocks_lock2_bench(thedb->bdb_env, lcnt, pcnt);
            pthread_mutex_unlock(&testguard);
        }
    } else if (tokcmp(tok, ltok, "deadlock_policy_override") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (ltok > 0) {
            gbl_deadlock_policy_override = toknum(tok, ltok);
            logmsg(LOGMSG_USER, "Set deadlock policy to %s\n",
                   deadlock_policy_str(gbl_deadlock_policy_override));
        } else {
            logmsg(LOGMSG_ERROR, "Must specify policy:\n");
            logmsg(LOGMSG_ERROR, "1     - DB_LOCK_DEFAULT\n");
            logmsg(LOGMSG_ERROR, "2     - DB_LOCK_EXPIRE\n");
            logmsg(LOGMSG_ERROR, "3     - DB_LOCK_MAXLOCKS\n");
            logmsg(LOGMSG_ERROR, "4     - DB_LOCK_MINLOCKS\n");
            logmsg(LOGMSG_ERROR, "5     - DB_LOCK_MINWRITE\n");
            logmsg(LOGMSG_ERROR, "6     - DB_LOCK_OLDEST\n");
            logmsg(LOGMSG_ERROR, "7     - DB_LOCK_RANDOM\n");
            logmsg(LOGMSG_ERROR, "8     - DB_LOCK_YOUNGEST\n");
            logmsg(LOGMSG_ERROR, "9     - DB_LOCK_MAXWRITE\n");
            logmsg(LOGMSG_ERROR, "10    - DB_LOCK_MINWRITE_NOREAD\n");
            logmsg(LOGMSG_ERROR, "11    - DB_LOCK_YOUNGEST_EVER\n");
            logmsg(LOGMSG_ERROR, "12    - DB_LOCK_MINWRITE_EVER\n");
        }
    } else if (tokcmp(tok, ltok, "detect") == 0) {
        bdb_detect(thedb->bdb_env);
    } else if (tokcmp(tok, ltok, "lsum") == 0) {
        void bdb_locker_summary(void *_bdb_state);
        bdb_locker_summary(thedb->bdb_env);
    } else if (tokcmp(tok, ltok, "mempget_timeout") == 0) {
        extern int __gbl_max_mpalloc_sleeptime;
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_USER, "Current mempget_timeout value: %d seconds\n",
                   __gbl_max_mpalloc_sleeptime);
            return 1;
        }
        __gbl_max_mpalloc_sleeptime = toknum(tok, ltok);
        logmsg(LOGMSG_USER, "mempget timeout set to %d seconds\n",
               __gbl_max_mpalloc_sleeptime);
    } else if (tokcmp(tok, ltok, "listpools") == 0) {
        thdpool_list_pools();
    } else if (tokcmp(tok, ltok, "pools_do_all") == 0) {
        thdpool_command_to_all(line, lline, st);
    } else if (tokcmp(tok, ltok, "berkattr") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            bdb_berkdb_dump_attrs(dbenv->bdb_env, stdout);
            return 1;
        } else if (tokcmp(tok, ltok, "set") == 0) {
            char *attr = NULL;
            char *value = NULL;
            int ivalue;
            int optlen;
            tok = segtok(line, lline, &st, &ltok);
            if (ltok == 0)
                goto bad_berkattr_set;
            attr = tokdup(tok, ltok);
            tok = segtok(line, lline, &st, &ltok);
            if (ltok == 0)
                goto bad_berkattr_set;
            optlen = lline - st + 2;
            value = malloc(optlen);
            strncpy(value, tok, optlen - 1);
            value[optlen - 1] = 0;
            ivalue = toknum(tok, ltok);
            rc = bdb_berkdb_set_attr(thedb->bdb_env, attr, value, ivalue);
            if (rc)
                logmsg(LOGMSG_ERROR, "couldn't set berkdb %s option\n", attr);
            return 0;
        bad_berkattr_set:
            if (attr)
                free(attr);
            if (value)
                free(value);
            logmsg(LOGMSG_ERROR, "usage: set option value\n");
        }
    } else if (tokcmp(tok, ltok, "repscon") == 0) {
        if (gbl_repscore) {
            logmsg(LOGMSG_USER, "Replication score report already on\n");
        } else {
            gbl_repscore = 1;
           logmsg(LOGMSG_USER, "Replication score report on\n");
        }
    } else if (tokcmp(tok, ltok, "repscof") == 0) {
        if (!gbl_repscore) {
           logmsg(LOGMSG_USER, "Replication score report already off\n");
        } else {
            gbl_repscore = 0;
           logmsg(LOGMSG_USER, "Replication score report off\n");
        }
    } else if (tokcmp(tok, ltok, "surprise") == 0) {
        gbl_surprise = 1;
    } else if (tokcmp(tok, ltok, "nosurprise") == 0) {
        gbl_surprise = 0;
    } else if (tokcmp(tok, ltok, "printlog") == 0) {
        int startfile = 0, startoff = 0;
        int endfile = 0, endoff = 0;
        char *s;

        tok = segtok(line, lline, &st, &ltok);
        if (ltok) {
            if ((s = strnchr(tok, ltok, ':'))) {
                if (s == tok)
                    startfile = 0;
                else
                    startfile = toknum(tok, s - tok);
                if (ltok - (s - tok) == 0)
                    startoff = 0;
                else
                    startoff = toknum(s + 1, ltok - (s - tok));
            } else
                startfile = toknum(tok, ltok);
            tok = segtok(line, lline, &st, &ltok);
            if (ltok == 0) {
                endfile = endoff = 0;
            } else if ((s = strnchr(tok, ltok, ':'))) {
                if (s == tok)
                    endfile = 0;
                else
                    endfile = toknum(tok, s - tok);
                if (ltok - (s - tok) == 0)
                    endoff = 0;
                else
                    endoff = toknum(s + 1, ltok - (s - tok));
            } else
                endfile = toknum(tok, ltok);
        }
        extern int printlog(bdb_state_type * bdb_state, int startfile,
                            int startoff, int endfile, int endoff);
        printlog(thedb->bdb_env, startfile, startoff, endfile, endoff);
#ifdef _LINUX_SOURCE
    } else if (tokcmp(tok, ltok, "rcache") == 0) {
        gbl_rcache = true;
       logmsg(LOGMSG_USER, "enabled rcache\n");
    } else if (tokcmp(tok, ltok, "norcache") == 0) {
        gbl_rcache = false;
       logmsg(LOGMSG_USER, "disabled rcache\n");
#endif
    } else if (tokcmp(tok, ltok, "swing") == 0) {
        extern int gbl_master_changes;
        ++gbl_master_changes;
    } else if (tokcmp(tok, ltok, "stat4dump") == 0) {
        int more;
        segtok(line, lline, &st, &more);
        stat4dump(more, NULL, false);
    } else if (tokcmp(tok, ltok, "sqllogger") == 0) {
        sqllogger_process_message(line + st, lline - st);
    } else if (tokcmp(tok, ltok, "blkseqv3") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0)
            return 0;
        if (tokcmp(tok, ltok, "dump") == 0) {
            int nstripes =
                bdb_attr_get(thedb->bdb_attr, BDB_ATTR_PRIVATE_BLKSEQ_STRIPES);
            for (int stripe = 0; stripe < nstripes; stripe++) {
               logmsg(LOGMSG_USER, "stripe %d\n", stripe);
                bdb_blkseq_dumpall(thedb->bdb_env, stripe);
            }
        } else if (tokcmp(tok, ltok, "logdel") == 0) {
            bdb_blkseq_dumplogs(thedb->bdb_env);
        }
    } else if (tokcmp(tok, ltok, "master_swing_osql_verbose") == 0) {
        gbl_master_swing_osql_verbose = 1;
    } else if (tokcmp(tok, ltok, "master_swing_osql_verbose_off") == 0) {
        gbl_master_swing_osql_verbose = 0;
    } else if (tokcmp(tok, ltok, "panic") == 0) {
        bdb_panic(thedb->bdb_env);
    } else if (tokcmp(tok, ltok, "debug_logreq") == 0) {
        int file, offset;
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_USER, "expected file number\n");
            return 1;
        }
        file = toknum(tok, ltok);
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_USER, "expected offset number\n");
            return 1;
        }
        offset = toknum(tok, ltok);
        bdb_debug_logreq(thedb->bdb_env, file, offset);
    } else if (tokcmp(tok, ltok, "memstat") == 0) {
        char *prefix = NULL;
        int verbose = 0, hr = 0;
        comdb2ma_order_by ord = COMDB2MA_TOTAL_DESC;
        comdb2ma_group_by grp = COMDB2MA_GRP_NONE;
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            comdb2ma_stats(prefix, verbose, hr, ord, grp, 0);
#ifndef PER_THREAD_MALLOC
        } else if (tokcmp(tok, ltok, "net") == 0) {
            tok = segtok(line, lline, &st, &ltok);
            if (ltok == 0 || tokcmp(tok, ltok, "hr") != 0)
                print_net_memstat(0);
            else
                print_net_memstat(1);
#endif
        } else if (tokcmp(tok, ltok, "nice") == 0) {
            int nicerc;
            tok = segtok(line, lline, &st, &ltok);
            nicerc = comdb2ma_nice((ltok <= 0) ? 1 : toknum(tok, ltok));
            if (nicerc != 0) {
                fprintf(stderr, "Failed to change mem niceness: rc = %d.\n",
                        nicerc);
                return 1;
            }
        } else if (tokcmp(tok, ltok, "release") == 0) {
            comdb2ma_release();
        } else if (tokcmp(tok, ltok, "autoreport") == 0) {
            tok = segtok(line, lline, &st, &ltok);
            if (ltok != 0)
                gbl_memstat_freq = toknum(tok, ltok);
           logmsg(LOGMSG_USER, "auto report memstat every %d seconds\n", gbl_memstat_freq);
        } else {
            while (ltok != 0) {
                if (tokcmp(tok, ltok, "verbose") == 0)
                    verbose = 1;
                else if (tokcmp(tok, ltok, "hr") == 0)
                    hr = 1;
                else if (tokcmp(tok, ltok, "group_by_name") == 0)
                    grp = COMDB2MA_GRP_NAME;
                else if (tokcmp(tok, ltok, "group_by_scope") == 0)
                    grp = COMDB2MA_GRP_SCOPE;
                else if (tokcmp(tok, ltok, "group_by_name_scope") == 0 ||
                         tokcmp(tok, ltok, "group_by_scope_name") == 0)
                    grp = COMDB2MA_GRP_NAME_SCOPE;
#ifdef PER_THREAD_MALLOC
                else if (tokcmp(tok, ltok, "group_by_thread") == 0)
                    grp = COMDB2MA_GRP_THR;
#endif
                else if (tokcmp(tok, ltok, "name") == 0 ||
                         tokcmp(tok, ltok, "name_asc") == 0)
                    ord = COMDB2MA_NAME_ASC;
                else if (tokcmp(tok, ltok, "name_desc") == 0)
                    ord = COMDB2MA_NAME_DESC;
                else if (tokcmp(tok, ltok, "scope") == 0 ||
                         tokcmp(tok, ltok, "scope_asc") == 0)
                    ord = COMDB2MA_SCOPE_ASC;
                else if (tokcmp(tok, ltok, "scope_desc") == 0)
                    ord = COMDB2MA_SCOPE_DESC;
                else if (tokcmp(tok, ltok, "total") == 0 ||
                         tokcmp(tok, ltok, "total_asc") == 0)
                    ord = COMDB2MA_TOTAL_ASC;
                else if (tokcmp(tok, ltok, "total_desc") == 0)
                    ord = COMDB2MA_TOTAL_DESC;
                else if (tokcmp(tok, ltok, "used") == 0 ||
                         tokcmp(tok, ltok, "used_asc") == 0)
                    ord = COMDB2MA_USED_ASC;
                else if (tokcmp(tok, ltok, "used_desc") == 0)
                    ord = COMDB2MA_USED_DESC;
#ifdef PER_THREAD_MALLOC
                else if (tokcmp(tok, ltok, "thread") == 0 ||
                         tokcmp(tok, ltok, "thread_asc") == 0)
                    ord = COMDB2MA_THR_ASC;
                else if (tokcmp(tok, ltok, "thread_desc") == 0)
                    ord = COMDB2MA_THR_DESC;
#endif
                else
                    prefix = tok;

                tok = segtok(line, lline, &st, &ltok);
            }
            // mtrap memstat always go to printf()
            comdb2ma_stats(prefix, verbose, hr, ord, grp, 0);
        }
    } else if (tokcmp(tok, ltok, "partitions") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (tokcmp(tok, ltok, "roll") == 0) {
            char *tblname;

            tok = segtok(line, lline, &st, &ltok);
            if (!tok) {
                logmsg(LOGMSG_ERROR, "Usage: partitions roll <partitionname>");
                return -1;
            }
            tblname = tokdup(tok, ltok);
            views_do_rollout(thedb->timepart_views, tblname);
            free(tblname);
        } else if (tokcmp(tok, ltok, "purge") == 0) {
            char *tblname;

            tok = segtok(line, lline, &st, &ltok);
            if (!tok) {
                logmsg(LOGMSG_ERROR, "Usage: partitions purge <partitionname>");
                return -1;
            }
            tblname = tokdup(tok, ltok);
            views_do_purge(thedb->timepart_views, tblname);
            free(tblname);
        } else {
            char *str = NULL;
            int lrc;

            lrc = timepart_serialize(thedb->timepart_views, &str, 1);

            if (str) {
                logmsg(LOGMSG_USER, "%s\n", str);
                free(str);
            } else {
                logmsg(LOGMSG_ERROR, "ERROR\n");
            }
        }
    } else if (tokcmp(tok, ltok, "tableparams") == 0) {
        print_tableparams();
        tok = segtok(line, lline, &st, &ltok);
        if (tokcmp(tok, ltok, "clear") == 0) {
            struct db *db = NULL;
            char *table;
            tok = segtok(line, lline, &st, &ltok);
            if (ltok == 0)
                logmsg(LOGMSG_ERROR, "Need table name to clear\n");
            else {
               logmsg(LOGMSG_USER, "Clearing entry for '%s'\n", tok);
                table = tokdup(tok, ltok);
                db = getdbbyname(table);
                if (!db) {
                    logmsg(LOGMSG_ERROR, "Could not find table '%s'\n", table);
                } else {
                    int lrc = bdb_del_table_csonparameters(NULL, table);
                    if (lrc)
                        logmsg(LOGMSG_ERROR, "Error deleting tbl params for tbl %s\n", table);
                    else
                        print_tableparams();
                }
                free(table);
            }
        }
    } else if (tokcmp(tok, ltok, "goslow") == 0) {
        extern int gbl_goslow;
        gbl_goslow = 1;
    } else if (tokcmp(tok, ltok, "gofast") == 0) {
        extern int gbl_goslow;
        gbl_goslow = 0;
    } else if (tokcmp(tok, ltok, "logmsg") == 0) {
        logmsg_process_message(line, lline);
    } else {
        logmsg(LOGMSG_ERROR, "unknown command <%.*s>\n", ltok, tok);
    }
    return 0;
}

static void request_stats(struct dbenv *dbenv)
{
    int ii;
    for (ii = 0; ii < dbenv->num_dbs; ii++) {
        req_stats(dbenv->dbs[ii]);
    }
}

void fastcount(char *tablename)
{
    uint64_t dtasize;
    int recsize;
    int numrecs;
    struct db *p_db;

    if (!(p_db = getdbbyname(tablename))) {
        logmsg(LOGMSG_ERROR, "%s: couldn't find table: %s\n", __func__, tablename);
        return;
    }

    calc_table_size(p_db);
    dtasize = p_db->totalsize / 3;
    recsize = p_db->lrl;
    numrecs = dtasize / recsize;

    logmsg(LOGMSG_USER, "table %s has approximately %d records\n", tablename,
            numrecs);
}

static void dump_table_sizes(struct dbenv *dbenv)
{
    struct db *db;
    int ndb;
    uint64_t total = 0;
    int maxtblname = 9; /* for "log files" */
    char b[32], b1[32], b2[32];
    int rc;
    struct statvfs st;
    uint64_t percent;
    int ii, len;
    unsigned num_logs;
    uint64_t logsize;

    for (ndb = 0; ndb < dbenv->num_dbs; ndb++) {
        db = dbenv->dbs[ndb];
        total += calc_table_size(db);
        len = strlen(db->dbname);
        if (len > maxtblname)
            maxtblname = len;
    }
    for (ndb = 0; ndb < dbenv->num_qdbs; ndb++) {
        db = dbenv->qdbs[ndb];
        total += calc_table_size(db);
        len = strlen(db->dbname);
        if (len > maxtblname)
            maxtblname = len;
    }
    logsize = bdb_logs_size(dbenv->bdb_env, &num_logs);
    total += logsize;

    for (ndb = 0; ndb < dbenv->num_dbs; ndb++) {
        db = dbenv->dbs[ndb];

        if (total > 0)
            percent = (db->totalsize * 100ULL) / total;
        else
            percent = 0;
       logmsg(LOGMSG_USER, "table %*s sz %12s %3d%% ", maxtblname, db->dbname,
               fmt_size(b, sizeof(b), db->totalsize), (int)percent);
       logmsg(LOGMSG_USER, "(dta %s", fmt_size(b, sizeof(b), db->dtasize));
        for (ii = 0; ii < db->nix; ii++) {
           logmsg(LOGMSG_USER, ", ix%d %s", ii, fmt_size(b, sizeof(b), db->ixsizes[ii]));
        }
        for (ii = 0; ii < db->numblobs; ii++) {
           logmsg(LOGMSG_USER, ", blob%d %s", ii,
                   fmt_size(b, sizeof(b), db->blobsizes[ii]));
        }
       logmsg(LOGMSG_USER, ")\n");
    }
    for (ndb = 0; ndb < dbenv->num_qdbs; ndb++) {
        db = dbenv->qdbs[ndb];

        if (total > 0)
            percent = (db->totalsize * 100ULL) / total;
        else
            percent = 0;
        logmsg(LOGMSG_USER, "queue %*s sz %12s %3d%% (%u extents)\n", maxtblname, db->dbname,
               fmt_size(b, sizeof(b), db->totalsize), (int)percent,
               db->numextents);
    }
    if (total > 0)
        percent = (logsize * 100ULL) / total;
    else
        percent = 0;
   logmsg(LOGMSG_USER, "%-*s sz %12s %3d%% (%u logs)\n", maxtblname + 6, "log files",
           fmt_size(b, sizeof(b), logsize), (int)percent, num_logs);

   logmsg(LOGMSG_USER, "GRAND TOTAL %s\n", fmt_size(b, sizeof(b), total));

    rc = statvfs(thedb->basedir, &st);
    if (rc == -1) {
        logmsg(LOGMSG_ERROR, "cannot get file system data for %s: %d %s\n",
                thedb->basedir, errno, strerror(errno));
    } else {
        uint64_t fsavail, fstotal;
        fsavail = (uint64_t)st.f_bavail * (uint64_t)st.f_frsize;
        fstotal = (uint64_t)st.f_blocks * (uint64_t)st.f_frsize;
        logmsg(LOGMSG_USER, "FILESYSTEM SIZE %s, AVAILABLE SPACE %s\n",
               fmt_size(b1, sizeof(b1), fstotal),
               fmt_size(b2, sizeof(b2), fsavail));
    }
}

void ixstats(struct dbenv *dbenv)
{
    int dbn, ix;
    struct db *db;

    for (dbn = 0; dbn < dbenv->num_dbs; dbn++) {
        db = dbenv->dbs[dbn];
        logmsg(LOGMSG_USER, "table '%s'\n", db->dbname);
        for (ix = 0; ix < db->nix; ix++) {
            logmsg(LOGMSG_USER, "  ix %2d:   %lld steps  %lld sql steps\n", ix,
                   db->ixuse[ix], db->sqlixuse[ix]);
        }
    }
}

void curstats(struct dbenv *dbenv)
{
    int dbn;
    struct db *db;

    for (dbn = 0; dbn < dbenv->num_dbs; dbn++) {
        db = dbenv->dbs[dbn];
        logmsg(LOGMSG_USER, "table '%s' : ix = %u cur = %u\n", db->dbname, db->sqlcur_ix,
               db->sqlcur_cur);
    }
}

void query_limit_stats(void)
{
    logmsg(LOGMSG_USER, "Default query limits:\n");
    logmsg(LOGMSG_USER, "   Max cost:  ");
    if (gbl_querylimits_maxcost == 0)
        logmsg(LOGMSG_USER, "not set.\n");
    else
        logmsg(LOGMSG_USER, "%f.\n", gbl_querylimits_maxcost);
    logmsg(LOGMSG_USER, "   Table scans ok? %s.\n",
           gbl_querylimits_tablescans_ok ? "Yes" : "No");
    logmsg(LOGMSG_USER, "   Temp tables ok? %s.\n",
           gbl_querylimits_temptables_ok ? "Yes" : "No");
    logmsg(LOGMSG_USER, "   Warn at cost: ");
    if (gbl_querylimits_maxcost_warn == 0)
        logmsg(LOGMSG_USER, "not set.\n");
    else
        logmsg(LOGMSG_USER, "%f.\n", gbl_querylimits_maxcost_warn);
    logmsg(LOGMSG_USER, "   Warn on table scans? %s.\n",
           gbl_querylimits_tablescans_warn ? "Yes" : "No");
    logmsg(LOGMSG_USER, "   Warn on temp tables? %s.\n",
           gbl_querylimits_temptables_warn ? "Yes" : "No");
}

int query_limit_cmd(char *line, int llen, int toff)
{
    char *tok;
    int tlen;
    char *toks;
    double maxcost;
    int iswarn = 0;

    tok = segtok(line, llen, &toff, &tlen);
    if (tlen == 0) {
        query_limit_stats();
        return 0;
    }

    if (tokcmp(tok, tlen, "warn") == 0) {
        iswarn = 1;
        tok = segtok(line, llen, &toff, &tlen);
    }

    if (tokcmp(tok, tlen, "maxcost") == 0) {
        tok = segtok(line, llen, &toff, &tlen);
        if (tlen == 0) {
            logmsg(LOGMSG_ERROR, "Expected value for maxcost\n");
            return 0;
        }
        toks = tokdup(tok, tlen);
        maxcost = strtod(toks, NULL);
        free(toks);
        if (tokcmp(tok, tlen, "off") == 0)
            maxcost = 0;
        if (maxcost == 0) {
            logmsg(LOGMSG_USER, "Disabled max query cost.\n");
        } else
            logmsg(LOGMSG_USER, "Set max query cost to %f\n", maxcost);
        if (iswarn)
            gbl_querylimits_maxcost_warn = maxcost;
        else
            gbl_querylimits_maxcost = maxcost;
    } else if (tokcmp(tok, tlen, "tablescans") == 0) {
        if (iswarn)
            on_off_trap(line, llen, &toff, &tlen, "Tablescans",
                        "querylimit tablescans",
                        &gbl_querylimits_tablescans_warn);
        else
            on_off_trap(line, llen, &toff, &tlen, "Tablescans",
                        "querylimit tablescans",
                        &gbl_querylimits_tablescans_ok);
    } else if (tokcmp(tok, tlen, "temptables") == 0) {
        if (iswarn)
            on_off_trap(line, llen, &toff, &tlen, "Temptables",
                        "querylimit temptables",
                        &gbl_querylimits_temptables_warn);
        else
            on_off_trap(line, llen, &toff, &tlen, "Temptables",
                        "querylimit temptables",
                        &gbl_querylimits_temptables_ok);
    } else {
        query_limit_stats();
        return 0;
    }
    return 0;
}
