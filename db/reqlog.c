/*
   Copyright 2015, 2022 Bloomberg Finance L.P.

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

/*
 * Advanced/Smart/Too-clever-for-its-own-good logging module for comdb2.
 *
 * The aims here are:-
 * - minimal impact on speed when logging is all turned off.
 * - ability to log specific events (e.g. requests from certain sourcs,
 *   certain types of requests, requests that fail in certain ways etc)
 * - ability to log to act.log or a file.
 * - unified interface for sql and tagged requests.
 * - free beer.
 *
 * To make this as fast as possible and accessible in deeply nested routines
 * each thread has a request logging object associated with it whose memory
 * is recycled between requests.
 */

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <arpa/inet.h>
#include <ctype.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>
#include <bdb_api.h>
#include <alloca.h>

#include <str0.h>
#include <rtcpu.h>
#include <list.h>
#include <segstr.h>
#include <plhash.h>
#include <memory_sync.h>

#include <epochlib.h>

#include <crc32c.h>
#include "comdb2.h"
#include "sqloffload.h"
#include "osqlblockproc.h"
#include "cdb2_constants.h"
#include "comdb2_atomic.h"

#include "nodemap.h"
#include "intern_strings.h"
#include "util.h"
#include "tohex.h"
#include "logmsg.h"
#include "reqlog.h"
#include "comdb2uuid.h"
#include "strbuf.h"
#include "roll_file.h"

#include "eventlog.h"
#include "reqlog_int.h"
#include "sp.h"

#include <tohex.h>
#include "string_ref.h"

/* The normal case is for there to be no rules, just a long request threshold
 * which takes some default action on long requests.  If you want anything
 * different then you add rules and you have to lock around the list. */
static int long_request_ms = 2000;
static struct output *long_request_out = NULL;
static pthread_mutex_t rules_mutex;
static LISTC_T(struct logrule) rules;
static LISTC_T(struct output) outputs;

static int reqlog_init_off = 0;
static struct output *default_out;

int diffstat_thresh = 60; /* every sixty seconds */
static struct output *stat_request_out = NULL;

/* These global lockless variables define what we will log for all requests
 * just in case the request meets all the criteria to trigger a rule. */
static int master_event_mask = 0;
static int master_all_requests = 0;
static struct list master_opcode_list = {0};
static struct list master_opcode_inv_list = {0};
static int master_table_rules = 0;
static char master_stmts[NUMSTMTS][MAXSTMT + 1];
static int master_num_stmts = 0;
int reqltruncate = 0;

/* sometimes you have to debug the debugger */
static int verbose = 0;

/* stolen from sltdbt.c */
static int long_reqs = 0;
static int norm_reqs = 0;

/* for the sqldbgtrace message trap */
int sqldbgflag = 0;

/* Log long running requests this frequently (in seconds) */
int gbl_longreq_log_freq_sec = 60;

static void log_all_events(struct reqlogger *logger, struct output *out);

extern int is_stored_proc(struct sqlclntstate*);

inline void sltdbt_get_stats(int *n_reqs, int *l_reqs)
{
    *n_reqs = norm_reqs;
    *l_reqs = long_reqs;
    norm_reqs = long_reqs = 0;
}

/* maintain a logging trace prefix with a stack structure */
static void prefix_init(struct prefix_type *p)
{
    p->pos = 0;
    p->stack_pos = 0;
    p->prefix[0] = '\0';
}

static void prefix_push(struct prefix_type *p, const char *prefix, int len)
{
    if (p->stack_pos < MAX_PREFIXES) {
        p->stack[p->stack_pos] = p->pos;
        if (len + p->pos >= sizeof(p->prefix)) {
            len = (sizeof(p->prefix) - 1) - p->pos;
        }
        memcpy(p->prefix + p->pos, prefix, len);
        p->pos += len;
        p->prefix[p->pos] = '\0';
    }
    p->stack_pos++;
}

static void prefix_pop(struct prefix_type *p)
{
    p->stack_pos--;
    if (p->stack_pos < 0) {
        p->stack_pos = 0;
        p->pos = 0;
        logmsg(LOGMSG_ERROR, "%s: stack pos went -ve!\n", __func__);
    } else if (p->stack_pos < MAX_PREFIXES) {
        p->pos = p->stack[p->stack_pos];
    }
    p->prefix[p->pos] = '\0';
}

static void prefix_pop_all(struct prefix_type *p)
{
    p->stack_pos = 0;
    p->pos = 0;
    p->prefix[p->pos] = '\0';
}

/* This should be called under lock unless the output is the default
 * output (act.log). */
static void flushdump(struct reqlogger *logger, struct output *out)
{
    if (logger->dumplinepos > 0) {
        struct iovec iov[5];
        int niov = 0;
        int append_duration = 0;
        char durstr[16];
        if (!out) {
            out = default_out;
            append_duration = 1;
        }
        if (out->use_time_prefix && out != default_out) {
            int now = comdb2_time_epoch();
            if (now != out->lasttime) {
                time_t timet = (time_t)now;
                struct tm tm;
                out->lasttime = now;
                localtime_r(&timet, &tm);
                int rc = snprintf(out->timeprefix, sizeof(out->timeprefix),
                                  "%02d/%02d %02d:%02d:%02d: ", tm.tm_mon + 1,
                                  tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec);
                if (rc >= sizeof(out->timeprefix)) {
                    snprintf(out->timeprefix, sizeof(out->timeprefix),
                             "truncated-time");
                }
            }
            iov[niov].iov_base = out->timeprefix;
            iov[niov].iov_len = sizeof(out->timeprefix) - 1;
            niov++;
        }
        if (logger->prefix.pos > 0) {
            iov[niov].iov_base = logger->prefix.prefix;
            iov[niov].iov_len = logger->prefix.pos;
            niov++;
        }
        iov[niov].iov_base = logger->dumpline;
        iov[niov].iov_len = logger->dumplinepos;
        niov++;
        if (append_duration) {
            iov[niov].iov_base = durstr;
            iov[niov].iov_len =
                snprintf(durstr, sizeof(durstr), " TIME +%d",
                         U2M(comdb2_time_epochus() - logger->startus));
            niov++;
        }
        iov[niov].iov_base = "\n";
        iov[niov].iov_len = 1;
        niov++;
        if (out == default_out) {
            for (int i = 0; i < niov; i++)
                logmsg(LOGMSG_USER, "%.*s", (int)iov[i].iov_len,
                       (char *)iov[i].iov_base);
        } else {
            ssize_t rc = writev(out->fd, iov, niov);
            if (rc == -1) {
                logmsg(LOGMSG_USER, "%s:%d writev returns rc=%zd\n", __FILE__,
                       __LINE__, rc);
            }
        }
        logger->dumplinepos = 0;
    }
}

static void dump(struct reqlogger *logger, struct output *out, const char *s,
                 int len)
{
    int ii;
    for (ii = 0; ii < len;) {
        if (logger->dumplinepos >= sizeof(logger->dumpline)) {
            flushdump(logger, out);
            continue;
        }
        if (s[ii] == '\n') {
            flushdump(logger, out);
        } else {
            logger->dumpline[(logger->dumplinepos)++] = s[ii];
        }
        ii++;
    }
}

/* if we provide here an output bigger than 256, we used to print
   the stack in the log file;
   in this new attempt, we're providing a slow path in which we
   malloc/free a buffer large enough to fit the size and preserve
   the output string (and its termination)
 */
static void dumpf(struct reqlogger *logger, struct output *out, const char *fmt,
                  ...)
{
    va_list args;
    char buf[256], *buf_slow = NULL;
    int len;

    va_start(args, fmt);
    len = vsnprintf(buf, sizeof(buf), fmt, args);
    if (len >= sizeof(buf)) {
        buf_slow = malloc(len + 1); /* with the trailing 0 */
        if (!buf_slow) {
            /* no memory, or asking for too much;
               i am gonna assume we are providing a new line */
            buf[sizeof(buf) - 1] = '\n';
            len = sizeof(buf);
        } else {
            len = vsnprintf(buf_slow, len + 1, fmt, args);
        }
    }
    va_end(args);

    dump(logger, out, (buf_slow) ? buf_slow : buf,
         len); /* don't dump the terminating \0 */

    if (buf_slow) free(buf_slow);
}

static void init_range(struct range *range)
{
    range->from = -1;
    range->to = -1;
}

static void init_dblrange(struct dblrange *dblrange)
{
    dblrange->from = -1;
    dblrange->to = -1;
}

static int add_list(struct list *list, int value, unsigned inv)
{
    int ii;
    inv = inv ? ~0 : 0;
    if (inv != list->inv) {
        list->num = 0;
        list->inv = inv;
    }
    for (ii = 0; ii < list->num; ii++) {
        if (list->list[ii] == value) {
            return 0;
        }
    }
    if (list->num >= LIST_MAX) {
        return -1;
    }
    list->list[list->num] = value;
    list->num++;
    return 0;
}

/* see if value matches criteria of the list */
static int check_list(const struct list *list, int value)
{
    unsigned ii;
    if (list->num == 0) {
        /* empty list matches all values */
        return 1;
    }
    for (ii = 0; ii < list->num && ii < LIST_MAX; ii++) {
        if (list->list[ii] == value) {
            return !list->inv;
        }
    }
    return list->inv;
}

static void printlist(FILE *fh, const struct list *list,
                      const char *(*item2a)(int value))
{
    int ii;
    if (list->inv) {
        logmsgf(LOGMSG_USER, fh, "not in ");
    } else {
        logmsgf(LOGMSG_USER, fh, "in ");
    }
    for (ii = 0; ii < list->num; ii++) {
        if (ii > 0) {
            logmsgf(LOGMSG_USER, fh, ", ");
        }
        logmsgf(LOGMSG_USER, fh, "%d", list->list[ii]);
        if (item2a) {
            logmsgf(LOGMSG_USER, fh, " (%s)", item2a(list->list[ii]));
        }
    }
}

/* get and reference a file output */
static struct output *get_output_ll(const char *filename)
{
    struct output *out;
    int fd, len;
    LISTC_FOR_EACH(&outputs, out, linkv)
    {
        if (strcmp(out->filename, filename) == 0) {
            out->refcount++;
            return out;
        }
    }
    fd = open(filename, O_WRONLY | O_APPEND | O_CREAT, 0666);
    if (fd == -1) {
        logmsg(LOGMSG_ERROR, "error opening '%s' for logging: %d %s\n",
               filename, errno, strerror(errno));
        default_out->refcount++;
        return default_out;
    }
    len = strlen(filename);
    out = calloc(offsetof(struct output, filename) + len + 1, 1);
    if (!out) {
        logmsg(LOGMSG_ERROR, "%s: out of memory\n", __func__);
        close(fd);
        default_out->refcount++;
        return default_out;
    }
    logmsg(LOGMSG_INFO, "opened request log file %s\n", filename);
    memcpy(out->filename, filename, len + 1);
    out->use_time_prefix = 1;
    out->refcount = 1;
    out->fd = fd;
    listc_atl(&outputs, out);
    Pthread_mutex_init(&out->mutex, NULL);
    return out;
}

/* dereference a file output */
static void deref_output_ll(struct output *out)
{
    out->refcount--;
    if (out->refcount <= 0 && out->fd > 2) {
        close(out->fd);
        logmsg(LOGMSG_INFO, "closed request log file %s\n", out->filename);
        listc_rfl(&outputs, out);
        free(out);
    }
}

/* should be called while you hold the rules_mutex */
static struct logrule *new_rule_ll(const char *name)
{
    struct logrule *rule;

    rule = calloc(sizeof(struct logrule), 1);
    if (!rule) {
        logmsg(LOGMSG_ERROR, "%s: calloc failed\n", __func__);
        return NULL;
    }
    strncpy0(rule->name, name, sizeof(rule->name));

    init_range(&rule->duration);
    init_range(&rule->retries);
    init_dblrange(&rule->sql_cost);
    init_range(&rule->sql_rows);

    rule->out = default_out;
    rule->out->refcount++;

    listc_abl(&rules, rule);
    return rule;
}

static void del_rule_ll(struct logrule *rule)
{
    if (rule) {
        listc_rfl(&rules, rule);
        deref_output_ll(rule->out);
        free(rule);
    }
}

static const char *rangestr(const struct range *range, char *buf, size_t buflen)
{
    if (range->from >= 0 && range->to >= 0) {
        snprintf(buf, buflen, "%d..%d", range->from, range->to);
    } else if (range->from >= 0) {
        snprintf(buf, buflen, ">=%d", range->from);
    } else if (range->to >= 0) {
        snprintf(buf, buflen, "<=%d", range->to);
    } else {
        strncpy0(buf, "<no constraint>", buflen);
    }
    return buf;
}

static const char *dblrangestr(const struct dblrange *range, char *buf,
                               size_t buflen)
{
    if (range->from >= 0 && range->to >= 0) {
        snprintf(buf, buflen, "%f..%f", range->from, range->to);
    } else if (range->from >= 0) {
        snprintf(buf, buflen, ">=%f", range->from);
    } else if (range->to >= 0) {
        snprintf(buf, buflen, "<=%f", range->to);
    } else {
        strncpy0(buf, "<no constraint>", buflen);
    }
    return buf;
}

static void printrule(struct logrule *rule, FILE *fh, const char *p)
{
    char b[32];
    logmsgf(LOGMSG_USER, fh, "%sRULE '%s'", p, rule->name);
    if (!rule->active) logmsgf(LOGMSG_USER, fh, " (INACTIVE)");
    logmsgf(LOGMSG_USER, fh, "\n");
    if (rule->count)
        logmsgf(LOGMSG_USER, fh, "%s  Log next %d requests where:\n", p,
                rule->count);
    else
        logmsgf(LOGMSG_USER, fh, "%s  Log all requests where:\n", p);
    if (rule->duration.from >= 0 || rule->duration.to >= 0)
        logmsgf(LOGMSG_USER, fh, "%s    duration %s msec\n", p,
                rangestr(&rule->duration, b, sizeof(b)));
    if (rule->retries.from >= 0 || rule->retries.to >= 0)
        logmsgf(LOGMSG_USER, fh, "%s    retries %s\n", p,
                rangestr(&rule->retries, b, sizeof(b)));
    if (rule->vreplays.from >= 0 || rule->vreplays.to >= 0)
        logmsgf(LOGMSG_USER, fh, "%s    verify replays %s\n", p,
                rangestr(&rule->vreplays, b, sizeof(b)));
    if (rule->sql_cost.from >= 0 || rule->sql_cost.to >= 0)
        logmsgf(LOGMSG_USER, fh, "%s    SQL cost %s\n", p,
                dblrangestr(&rule->sql_cost, b, sizeof(b)));
    if (rule->sql_rows.from >= 0 || rule->sql_rows.to >= 0)
        logmsgf(LOGMSG_USER, fh, "%s    SQL rows %s\n", p,
                rangestr(&rule->sql_rows, b, sizeof(b)));
    if (rule->rc_list.num > 0) {
        logmsgf(LOGMSG_USER, fh, "%s    rcode is ", p);
        printlist(fh, &rule->rc_list, NULL);
        logmsgf(LOGMSG_USER, fh, "\n");
    }
    if (rule->opcode_list.num > 0) {
        logmsgf(LOGMSG_USER, fh, "%s    opcode is ", p);
        printlist(fh, &rule->opcode_list, req2a);
        logmsgf(LOGMSG_USER, fh, "\n");
    }
    if (rule->tablename[0]) {
        logmsgf(LOGMSG_USER, fh, "%s    touches table '%s'\n", p,
                rule->tablename);
    }
    if (rule->stmt[0]) {
        logmsgf(LOGMSG_USER, fh, "%s    sql statement like '%%%s%%'\n", p,
                rule->stmt);
    }
    if (rule->event_mask & REQL_TRACE)
        logmsgf(LOGMSG_USER, fh, "%s  Logging detailed trace\n", p);
    if (rule->event_mask & REQL_RESULTS)
        logmsgf(LOGMSG_USER, fh, "%s  Logging query results\n", p);
    logmsgf(LOGMSG_USER, fh, "%s  Log to %s\n", p, rule->out->filename);
}

/* scan all the rules and setup our master settings that define what we log
 * for each request.  We want to log as little as possible to be fast, but
 * we have to make sure that we log enough so that if a request matches some
 * of our criteria we can catch it. */
static void scanrules_ll(void)
{
    int ii, rc;
    int table_rules = 0;
    struct logrule *rule;
    unsigned event_mask = 0;
    int log_all_reqs = 0;
    master_all_requests = 1;
    master_num_stmts = 0;
    bzero(&master_opcode_list, sizeof(master_opcode_list));
    bzero(&master_opcode_inv_list, sizeof(master_opcode_inv_list));
    bzero(master_stmts, NUMSTMTS * (MAXSTMT + 1));
    LISTC_FOR_EACH(&rules, rule, linkv)
    {
        /* ignore inactive rules */
        if (!rule->active) {
            continue;
        }
        /* if the rule doesn't have any criteria that can be tested before the
         * request starts, then we definateky have to log for all requests */
        if (rule->opcode_list.num == 0 && rule->stmt[0] == 0) {
            log_all_reqs = 1;
        }
        /* if the rule has opcode criteria then build a list of opcodes to
         * allow through */
        for (ii = 0; ii < rule->opcode_list.num; ii++) {
            if (rule->opcode_list.inv) {
                rc = add_list(&master_opcode_inv_list,
                              rule->opcode_list.list[ii], 1);
            } else {
                rc = add_list(&master_opcode_list, rule->opcode_list.list[ii],
                              0);
            }
            if (rc != 0) {
                log_all_reqs = 1;
            }
        }
        /* if the rule has table name criteria then we must track tables used */
        if (rule->tablename[0]) {
            table_rules = 1;
        }
        /* if the rule has sql stmt criteria then add it to the list */
        if (rule->stmt[0]) {
            if (master_num_stmts == NUMSTMTS) {
                log_all_reqs = 1;
            } else {
                strncpy(master_stmts[master_num_stmts], rule->stmt, MAXSTMT);
                master_num_stmts++;
            }
        }
        event_mask |= rule->event_mask;
    }
    master_event_mask = event_mask;
    master_table_rules = table_rules;
    master_all_requests = log_all_reqs;
    if (verbose) {
        logmsg(LOGMSG_USER, "%s: master_event_mask=0x%x\n", __func__,
               master_event_mask);
        logmsg(LOGMSG_USER, "%s: master_table_rules=%d\n", __func__,
               master_table_rules);
        logmsg(LOGMSG_USER, "%s: master_all_requests=%d\n", __func__,
               master_all_requests);
        logmsg(LOGMSG_USER, "%s: master_opcode_inv_list: ", __func__);
        printlist(stdout, &master_opcode_inv_list, NULL);
        logmsg(LOGMSG_USER, "\n");
        logmsg(LOGMSG_USER, "%s: master_opcode_list: ", __func__);
        printlist(stdout, &master_opcode_list, NULL);
        logmsg(LOGMSG_USER, "\n");
        for (ii = 0; ii < master_num_stmts; ii++) {
            logmsg(LOGMSG_USER, "master_stmts[%d] = '%s'\n", ii,
                   master_stmts[ii]);
        }
    }
}

int reqlog_init(const char *dbname)
{
    struct output *out;
    char *filename;

    Pthread_mutex_init(&rules_mutex, NULL);
    listc_init(&rules, offsetof(struct logrule, linkv));
    listc_init(&outputs, offsetof(struct output, linkv));

    out = calloc(sizeof(struct output) + strlen("<stdout>") + 1, 1);
    if (!out) {
        logmsg(LOGMSG_ERROR, "%s:calloc failed\n", __func__);
        return -1;
    }
    strcpy(out->filename, "<stdout>");
    out->fd = 2;
    out->refcount = 1;
    default_out = out;
    listc_atl(&outputs, out);
    Pthread_mutex_init(&out->mutex, NULL);

    filename = comdb2_location("logs", "%s.longreqs", dbname);
    long_request_out = get_output_ll(filename);
    free(filename);

    filename = comdb2_location("logs", "%s.statreqs", dbname);
    stat_request_out = get_output_ll(filename);
    free(filename);

    eventlog_init();

    scanrules_ll();
    return 0;
}

static const char *help_text[] = {
    "Request logging framework commands",
    "reql off                     - request logging turn off for performance",
    "reql longrequest #           - set long request threshold in msec",
    "reql longsqlrequest #        - set long SQL request threshold in msec",
    "reql longreqfile <filename>  - set file to log long requests in",
    "reql diffstat #              - set diff stat threshold in sec",
    "reql truncate #              - set request truncation",
    "reql stat                    - status, print rules",
    "reql events                  - event logging",
    "   Subcommands for events: ",
    "       on               - turn on event logging",
    "       off              - turn off event logging",
    "       roll             - roll over log file",
    "       keep N           - keep N log files",
    "       detailed on/off  - turn on/off detailed mode (ex. sql bound param)",
    "       rollat N         - roll when log file size larger than N MB",
    "       every N          - log only every Nth event, 0 logs all",
    "       verbose on/off   - turn on/off verbose mode",
    "       dir <dir>        - set custom directory for event log files\n",
    "       file <file>      - set log file to custom location\n",
    "       flush            - flush log file to disk",
    "reql [rulename] ...     - add/modify rules.  The default rule is '0'.",
    "                          Valid rule names begin with a digit or '.'.",
    "   General commands:", "       delete           - delete named rule",
    "       go               - start logging with rule",
    "       stop             - stop logging with this rule",
    "   Specify criteria:",
    "       opcode [!]#      - log regular requests with opcode [other than] #",
    "       rc [!]#          - log requests with rcode [other than] #",
    "       ms <range>       - log requests within a range of msecs",
    "       retries <range>  - log requests with that many retries",
    "       cost <range>     - log SQL requests with the given cost",
    "       rows <range>     - log SQL requests with the given row count",
    "       table <name>     - log requests that touch given table",
    "       stmt 'sql stmt'  - log requests where sql contains that text",
    "       vreplays <range> - log requests with given number of verify "
    "replays",
    "   Specify what to log:", "       trace            - log detailed trace",
    "       results          - log query results",
    "       cnt #            - log up to # before removing rule",
    "   Specify where to log:",
    "       file <filename>  - log to filename rather than stdout",
    "       stdout           - log to stdout",
    "<range> is a range specification.  valid range specifications are:-",
    "   #+                   - match any number >=#",
    "   #-                   - match any number <=#",
    "   #..#                 - match anything between the two numbers "
    "inclusive",
    "<filename> must be a filename or the keyword '<stdout>'", NULL};

void reqlog_help(void)
{
    int ii;
    for (ii = 0; help_text[ii]; ii++) {
        logmsg(LOGMSG_USER, "%s\n", help_text[ii]);
    }
}

/* parse a range specification */
static int parse_range_tok(struct range *range, char *tok, int ltok)
{
    if (ltok > 0) {
        if (tok[ltok - 1] == '-') {
            range->from = -1;
            range->to = toknum(tok, ltok - 1);
            return 0;
        } else if (tok[ltok - 1] == '+') {
            range->from = toknum(tok, ltok - 1);
            range->to = -1;
            return 0;
        } else {
            int ii;
            for (ii = 0; ii < ltok - 1; ii++) {
                if (tok[ii] == '.' && tok[ii + 1] == '.') {
                    int end;
                    for (end = ii + 2; end < ltok && tok[end] == '.'; end++)
                        ;
                    range->from = toknum(tok, ii);
                    range->to = toknum(tok + end, ltok - end);
                    return 0;
                }
            }
        }
    }
    logmsg(LOGMSG_ERROR, "bad range specification '%*.*s'\n", ltok, ltok, tok);
    return -1;
}

static int parse_dblrange_tok(struct dblrange *dblrange, char *tok, int ltok)
{
    struct range range;
    int rc;

    rc = parse_range_tok(&range, tok, ltok);
    if (rc) return rc;
    dblrange->from = range.from;
    dblrange->to = range.to;
    return 0;
}

static void tokquoted(char *line, int lline, int *st, char *buf, size_t bufsz)
{
    int stage = 0;
    char quote = 0;
    if (bufsz == 0) {
        return;
    }
    while (bufsz > 0 && *st < lline) {
        char ch = line[*st];
        switch (stage) {
        case 0:
            /* scan for start */
            if (ch == '\'' || ch == '"') {
                quote = ch;
                stage = 2;
                break;
            } else if (isspace(ch)) {
                break;
            }
            stage = 1;
        /* fall through; found first character of line */
        case 1:
            /* unquoted text, scan for next whitespace */
            if (isspace(ch)) {
                goto end;
            }
            *buf = ch;
            buf++;
            bufsz--;
            break;
        case 2:
            /* quoted text */
            if (ch == quote) {
                if ((*st) + 1 < lline && buf[(*st) + 1] == ch) {
                    (*st)++;
                } else {
                    (*st)++;
                    goto end;
                }
            }
            *buf = ch;
            buf++;
            bufsz--;
            break;
        }
        (*st)++;
    }
end:
    if (bufsz == 0) {
        bufsz--;
        buf--;
    }
    *buf = 0;
}

void reqlog_process_message(char *line, int st, int lline)
{
    char *tok;
    int ltok;
    tok = segtok(line, lline, &st, &ltok);
    if (tokcmp(tok, ltok, "off") == 0) {
        logmsg(LOGMSG_USER, "Turn off Request logging\n");
        reqlog_init_off = 1;
    } else if (tokcmp(tok, ltok, "longrequest") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        long_request_ms = toknum(tok, ltok);
        logmsg(LOGMSG_USER, "Long request threshold now %d msec\n",
               long_request_ms);
    } else if (tokcmp(tok, ltok, "longsqlrequest") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        gbl_sql_time_threshold = toknum(tok, ltok);
        logmsg(LOGMSG_USER, "Long SQL request threshold now %d msec\n",
               gbl_sql_time_threshold);
    } else if (tokcmp(tok, ltok, "longreqfile") == 0) {
        char filename[128];
        struct output *out;
        tok = segtok(line, lline, &st, &ltok);
        tokcpy0(tok, ltok, filename, sizeof(filename));
        out = get_output_ll(filename);
        if (out) {
            deref_output_ll(long_request_out);
            long_request_out = out;
        }
    } else if (tokcmp(tok, ltok, "diffstat") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            reqlog_help();
        } else {
            reqlog_set_diffstat_thresh(toknum(tok, ltok));
        }
    } else if (tokcmp(tok, ltok, "truncate") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (ltok == 0) {
            reqlog_help();
        } else {
            reqlog_set_truncate(toknum(tok, ltok));
        }
    } else if (tokcmp(tok, ltok, "stat") == 0) {
        reqlog_stat();
    } else if (tokcmp(tok, ltok, "help") == 0) {
        reqlog_help();
    } else if (tokcmp(tok, ltok, "vbon") == 0) {
        verbose = 1;
    } else if (tokcmp(tok, ltok, "vbof") == 0) {
        verbose = 0;
    } else if (ltok == 0) {
        logmsg(LOGMSG_ERROR, "huh?\n");
    } else if (tokcmp(tok, ltok, "events") == 0) {
        eventlog_process_message(line, lline, &st);
    } else {
        char rulename[32];
        struct logrule *rule;

        if (isdigit(tok[0]) || tok[0] == '.') {
            tokcpy0(tok, ltok, rulename, sizeof(rulename));
            tok = segtok(line, lline, &st, &ltok);
        } else {
            strncpy0(rulename, "0", sizeof(rulename));
        }
        if (verbose) {
            logmsg(LOGMSG_USER, "rulename='%s'\n", rulename);
        }

        Pthread_mutex_lock(&rules_mutex);
        LISTC_FOR_EACH(&rules, rule, linkv)
        {
            if (strcmp(rulename, rule->name) == 0) {
                break;
            }
        }
        if (!rule) {
            rule = new_rule_ll(rulename);
            if (!rule) {
                logmsg(LOGMSG_ERROR, "error creating new rule %s\n", rulename);
                Pthread_mutex_unlock(&rules_mutex);
                return;
            }
        }
        while (ltok > 0) {
            if (tokcmp(tok, ltok, "go") == 0) {
                rule->active = 1;
            } else if (tokcmp(tok, ltok, "stop") == 0) {
                rule->active = 0;
            } else if (tokcmp(tok, ltok, "delete") == 0) {
                del_rule_ll(rule);
                rule = NULL;
                logmsg(LOGMSG_USER, "Rule deleted\n");
                break;
            } else if (tokcmp(tok, ltok, "cnt") == 0) {
                tok = segtok(line, lline, &st, &ltok);
                rule->count = toknum(tok, ltok);
            } else if (tokcmp(tok, ltok, "file") == 0) {
                char filename[128];
                struct output *out;
                tok = segtok(line, lline, &st, &ltok);
                tokcpy0(tok, ltok, filename, sizeof(filename));
                out = get_output_ll(filename);
                if (out) {
                    deref_output_ll(rule->out);
                    rule->out = out;
                }
            } else if (tokcmp(tok, ltok, "stdout") == 0) {
                struct output *out;
                out = default_out;
                out->refcount++;
                deref_output_ll(rule->out);
                rule->out = out;
            } else if (tokcmp(tok, ltok, "ms") == 0) {
                tok = segtok(line, lline, &st, &ltok);
                parse_range_tok(&rule->duration, tok, ltok);
            } else if (tokcmp(tok, ltok, "retries") == 0) {
                tok = segtok(line, lline, &st, &ltok);
                parse_range_tok(&rule->retries, tok, ltok);
            } else if (tokcmp(tok, ltok, "vreplays") == 0) {
                tok = segtok(line, lline, &st, &ltok);
                parse_range_tok(&rule->vreplays, tok, ltok);
            } else if (tokcmp(tok, ltok, "cost") == 0) {
                tok = segtok(line, lline, &st, &ltok);
                parse_dblrange_tok(&rule->sql_cost, tok, ltok);
            } else if (tokcmp(tok, ltok, "rows") == 0) {
                tok = segtok(line, lline, &st, &ltok);
                parse_range_tok(&rule->sql_rows, tok, ltok);
            } else if (tokcmp(tok, ltok, "sql") == 0) {
                add_list(&rule->opcode_list, OP_SQL, 0);
            } else if (tokcmp(tok, ltok, "stmt") == 0) {
                tokquoted(line, lline, &st, rule->stmt, sizeof(rule->stmt));
            } else if (tokcmp(tok, ltok, "opcode") == 0) {
                int opcode;
                char opname[32];
                int inv = 0;
                tok = segtok(line, lline, &st, &ltok);
                if (ltok > 0 && tok[0] == '!') {
                    tok++;
                    ltok--;
                    inv = 1;
                }
                tokcpy0(tok, ltok, opname, sizeof(opname));
                opcode = a2req(opname);
                if (opcode >= 0 && opcode < MAXTYPCNT) {
                    add_list(&rule->opcode_list, opcode, inv);
                }
            } else if (tokcmp(tok, ltok, "rc") == 0) {
                int rc;
                int inv = 0;
                tok = segtok(line, lline, &st, &ltok);
                if (ltok > 0 && tok[0] == '!') {
                    tok++;
                    ltok--;
                    inv = 1;
                }
                rc = toknum(tok, ltok);
                add_list(&rule->rc_list, rc, inv);
            } else if (tokcmp(tok, ltok, "table") == 0) {
                tok = segtok(line, lline, &st, &ltok);
                tokcpy0(tok, ltok, rule->tablename, sizeof(rule->tablename));
            } else if (tokcmp(tok, ltok, "trace") == 0) {
                rule->event_mask |= REQL_TRACE;
            } else if (tokcmp(tok, ltok, "results") == 0) {
                rule->event_mask |= REQL_RESULTS;
            } else {
                logmsg(LOGMSG_ERROR, "unknown rule command <%*.*s>\n", ltok,
                       ltok, tok);
            }
            tok = segtok(line, lline, &st, &ltok);
        }
        if (rule) {
            printrule(rule, stdout, "");
        }
        scanrules_ll();
        Pthread_mutex_unlock(&rules_mutex);
    }
}

void reqlog_stat(void)
{
    struct logrule *rule;
    struct output *out;
    logmsg(LOGMSG_USER, "Long request threshold : %d msec (%dmsec  for SQL)\n",
           long_request_ms, gbl_sql_time_threshold);
    logmsg(LOGMSG_USER, "Long request log file  : %s\n",
           long_request_out->filename);
    logmsg(LOGMSG_USER, "diffstat threshold     : %d s\n", diffstat_thresh);
    logmsg(LOGMSG_USER, "diffstat log file      : %s\n",
           stat_request_out->filename);
    logmsg(LOGMSG_USER, "request truncation     : %s\n",
           reqltruncate ? "enabled" : "disabled");
    logmsg(LOGMSG_USER, "SQL cost thresholds    :\n");
    logmsg(LOGMSG_USER, "   error               : ");
    if (gbl_sql_cost_error_threshold == -1)
        logmsg(LOGMSG_USER, "not set\n");
    else
        logmsg(LOGMSG_USER, "%f\n", gbl_sql_cost_error_threshold);
    Pthread_mutex_lock(&rules_mutex);
    logmsg(LOGMSG_USER, "%d rules currently active\n", rules.count);
    LISTC_FOR_EACH(&rules, rule, linkv)
    {
        printrule(rule, stdout, "");
    }
    LISTC_FOR_EACH(&outputs, out, linkv)
    {
        logmsg(LOGMSG_USER, "Output file open: %s\n", out->filename);
    }
    eventlog_status();
    Pthread_mutex_unlock(&rules_mutex);
}

void reqlog_reset(struct reqlogger *logger)
{
    if (logger)
        logger->ncontext = 0;
}

struct reqlogger *reqlog_alloc(void)
{
    struct reqlogger *logger;
    logger = calloc(sizeof(struct reqlogger), 1);
    if (!logger) {
        logmsg(LOGMSG_ERROR, "%s: calloc failed\n", __func__);
        return NULL;
    }

    return logger;
}

static void reqlog_free_error(struct reqlogger *logger) {
    if (logger->error) {
        free(logger->error);
        logger->error = NULL;
    }
}

static void reqlog_free_events(struct reqlogger *logger) {
    struct logevent *event;
    struct print_event *pevent;
    struct push_prefix_event *pushevent;

    while ((event = logger->events) != NULL) {
        logger->events = event->next;
        if (event->type == EVENT_PRINT) {
            pevent = (struct print_event *)event;
            free(pevent->text);
        } else if (event->type == EVENT_PUSH_PREFIX) {
            pushevent = (struct push_prefix_event *)event;
            free((void *)pushevent->text);
        }
        free(event);
    }
    logger->events = NULL;
}

static void reqlog_free_tables(struct reqlogger *logger) {
    struct tablelist *table;

    while ((table = logger->tables) != NULL) {
        logger->tables = table->next;
        free(table);
    }
    assert(logger->tables == NULL);

    for (int i = 0; i < logger->ntables; ++i) {
        free(logger->sqltables[i]);
    }
    free(logger->sqltables);
    logger->sqltables = NULL;
}

static void reqlog_free_all(struct reqlogger *logger)
{
    reqlog_free_error(logger);
    reqlog_free_events(logger);
    reqlog_free_tables(logger);

    put_ref(&logger->sql_ref);
}

void reqlog_free(struct reqlogger *logger)
{
    if (logger) {
        reqlog_free_all(logger);
        free(logger);
    }
}

void reqlog_reset_logger(struct reqlogger *logger)
{
    if (logger) {
        reqlog_free_all(logger);
        bzero(&logger->start_transient,
              sizeof(struct reqlogger) -
                  offsetof(struct reqlogger, start_transient));
    }
}

/* append an event onto the list of events for this logger */
static void reqlog_append_event(struct reqlogger *logger,
                                enum logevent_type type, void *voidevent)
{
    struct logevent *event = voidevent;
    event->type = type;
    event->next = NULL;
    if (logger->events) {
        logger->last_event->next = event;
        logger->last_event = event;
    } else {
        logger->events = logger->last_event = event;
    }
}

/* push an output trace prefix */
int reqlog_pushprefixv(struct reqlogger *logger, const char *fmt, va_list args)
{
    char *s;
    int len;
    int nchars;
    va_list args_c;

    if (logger == NULL) return 0;

    len = 256;
    s = malloc(len);
    if (!s) {
        logmsg(LOGMSG_ERROR, "%s:malloc(%d) failed\n", __func__, len);
        return -1;
    }

    va_copy(args_c, args);
    nchars = vsnprintf(s, len, fmt, args);
    if (nchars >= len && reqltruncate == 0) {
        len = nchars + 1;
        char *news = realloc(s, len);
        if (!news) {
            logmsg(LOGMSG_ERROR, "%s:realloc(%d) failed\n", __func__, len);
            free(s);
            return -1;
        }
        s = news;
        len = vsnprintf(s, len, fmt, args_c);
    } else {
        len = strlen(s);
    }
    va_end(args_c);

    if (logger->dump_mask) {
        flushdump(logger, NULL);
        prefix_push(&logger->prefix, s, len);
    }

    if (logger->event_mask) {
        struct push_prefix_event *event;

        event = malloc(sizeof(struct push_prefix_event));
        if (!event) {
            logmsg(LOGMSG_ERROR, "%s:malloc failed\n", __func__);
            free(s);
            return -1;
        }
        event->length = len;
        event->text = s;
        reqlog_append_event(logger, EVENT_PUSH_PREFIX, event);
    } else {
        free(s);
    }
    return 0;
}

int reqlog_pushprefixf(struct reqlogger *logger, const char *fmt, ...)
{
    va_list args;
    int rc;

    va_start(args, fmt);
    rc = reqlog_pushprefixv(logger, fmt, args);
    va_end(args);
    return rc;
}

/* pop an output trace prefix */
int reqlog_popprefix(struct reqlogger *logger)
{
    if (logger) {
        if (logger->dump_mask) {
            flushdump(logger, NULL);
            prefix_pop(&logger->prefix);
        }

        if (logger->event_mask) {
            struct pop_prefix_event *event;
            event = malloc(sizeof(struct pop_prefix_event));
            if (!event) {
                logmsg(LOGMSG_ERROR, "%s:malloc failed\n", __func__);
                return -1;
            }
            reqlog_append_event(logger, EVENT_POP_PREFIX, event);
        }
    }
    return 0;
}

int reqlog_popallprefixes(struct reqlogger *logger)
{
    if (logger) {
        if (logger->dump_mask) {
            flushdump(logger, NULL);
            prefix_pop_all(&logger->prefix);
        }

        if (logger->event_mask) {
            struct pop_prefix_event *event;
            event = malloc(sizeof(struct pop_prefix_event));
            if (!event) {
                logmsg(LOGMSG_ERROR, "%s:malloc failed\n", __func__);
                return -1;
            }
            reqlog_append_event(logger, EVENT_POP_PREFIX_ALL, event);
        }
    }
    return 0;
}

static int reqlog_logv_int(struct reqlogger *logger, unsigned event_flag,
                           const char *fmt, va_list args)
{
    char *s;
    int len;
    int nchars;
    va_list args_c;

    if (logger == NULL) return 0;

    len = 256;
    s = malloc(len);
    if (!s) {
        logmsg(LOGMSG_ERROR, "%s:malloc(%d) failed\n", __func__, len);
        return -1;
    }

    va_copy(args_c, args);
    nchars = vsnprintf(s, len, fmt, args);
    if (nchars >= len && reqltruncate == 0) {
        len = nchars + 1;
        char *news = realloc(s, len);
        if (!news) {
            logmsg(LOGMSG_ERROR, "%s:realloc(%d) failed\n", __func__, len);
            va_end(args_c);
            free(s);
            return -1;
        }
        s = news;
        len = vsnprintf(s, len, fmt, args_c);
    } else {
        len = strlen(s);
    }
    va_end(args_c);

    if (logger->dump_mask & event_flag) {
        dump(logger, NULL, s, len);
    }

    if (logger->event_mask & event_flag) {
        struct print_event *event;

        event = malloc(sizeof(struct print_event));
        if (!event) {
            logmsg(LOGMSG_ERROR, "%s:malloc failed\n", __func__);
            free(s);
            return -1;
        }
        event->event_flag = event_flag;
        event->length = len;
        event->text = s;
        reqlog_append_event(logger, EVENT_PRINT, event);
    } else {
        free(s);
    }
    return 0;
}

int reqlog_logv(struct reqlogger *logger, unsigned event_flag, const char *fmt,
                va_list args)
{
    if (logger && (logger->mask & event_flag)) {
        return reqlog_logv_int(logger, event_flag, fmt, args);
    } else {
        return 0;
    }
}

/* Log a formatted string.  This incurs some copy overhead if logging is
 * turned on. */
int reqlog_logf(struct reqlogger *logger, unsigned event_flag, const char *fmt,
                ...)
{
    if (logger && (logger->mask & event_flag)) {
        va_list args;
        int rc;

        va_start(args, fmt);
        rc = reqlog_logv_int(logger, event_flag, fmt, args);
        va_end(args);
        return rc;
    }
    return 0;
}

/* Log a string literal. */
int reqlog_logl(struct reqlogger *logger, unsigned event_flag, const char *s)
{
    if (logger && (logger->mask & event_flag)) {
        if (logger->event_mask & event_flag) {
            struct print_event *event;
            event = malloc(sizeof(struct print_event));
            if (!event) {
                logmsg(LOGMSG_ERROR, "%s:malloc failed\n", __func__);
                return -1;
            }
            event->event_flag = event_flag;
            event->length = -1; /* to indicate length is unknown */
            event->text = strdup(s);
            reqlog_append_event(logger, EVENT_PRINT, event);
        }
        if (logger->dump_mask & event_flag) {
            dump(logger, NULL, s, strlen(s));
        }
    }
    return 0;
}

int reqlog_loghex(struct reqlogger *logger, unsigned event_flag, const void *d,
                  size_t len)
{
    if (logger && (logger->mask & event_flag)) {
        struct print_event *event;
        char *hexstr;
        const char *dptr = d;

        hexstr = malloc(len * 2 + 1);
        if (!hexstr) {
            logmsg(LOGMSG_ERROR, "%s:malloc failed\n", __func__);
            return -1;
        }

        if (logger->event_mask & event_flag) {
            event = malloc(sizeof(struct print_event));
            if (!event) {
                logmsg(LOGMSG_ERROR, "%s:malloc failed\n", __func__);
                return -1;
            }

            event->event_flag = event_flag;
            event->length = len * 2;
            event->text = hexstr;
            reqlog_append_event(logger, EVENT_PRINT, event);
        }

        util_tohex(hexstr, dptr, len);
        if (logger->dump_mask & event_flag) {
            assert(strlen(hexstr) + 1 >= len * 2);
            dump(logger, NULL, hexstr, len * 2);
        }
    }
    return 0;
}

void reqlog_usetable(struct reqlogger *logger, const char *tablename)
{
    if (logger && logger->tracking_tables) {
        struct tablelist *table;
        int len;
        if (verbose) {
            logmsg(LOGMSG_USER, "%s: table %s\n", __func__, tablename);
        }
        for (table = logger->tables; table; table = table->next) {
            if (strcasecmp(table->name, tablename) == 0) {
                table->count++;
                return;
            }
        }
        len = strlen(tablename);
        table = malloc(offsetof(struct tablelist, name) + len + 1);
        if (!table) {
            logmsg(LOGMSG_ERROR, "%s: malloc failed\n", __func__);
        } else {
            table->next = logger->tables;
            table->count = 1;
            memcpy(table->name, tablename, len + 1);
            logger->tables = table;
        }
    }
}

void reqlog_setflag(struct reqlogger *logger, unsigned flag)
{
    if (logger) {
        logger->reqflags |= flag;
    }
}

/* figure out what to log for this request */
static void reqlog_start_request(struct reqlogger *logger)
{
    int gather;
    int ii;

    logger->tracking_tables = master_table_rules;

    if (logger->iq && logger->iq->debug) {
        logger->dump_mask = REQL_TRACE;
    }

    if (logger->opcode == OP_SQL && sqldbgflag) {
        logger->dump_mask = REQL_TRACE;
    }

    /* always gather info if global not set */
    if (!reqlog_init_off)
        logger->event_mask |= REQL_INFO;

    /* try to filter out this request based on opcode */
    gather = 0;
    if (master_all_requests) {
        gather = 1;
    } else {
        /* see if we match any of the criteria for a request that we have to
         * log. */
        if (master_opcode_list.num > 0 &&
            check_list(&master_opcode_list, logger->opcode)) {
            gather = 1;
        } else if (master_opcode_inv_list.num > 0 &&
                   check_list(&master_opcode_inv_list, logger->opcode)) {
            gather = 1;
        } else if (logger->sql_ref && master_num_stmts > 0) {
            const char *str = string_ref_cstr(logger->sql_ref);
            for (ii = 0; ii < master_num_stmts && ii < NUMSTMTS; ii++) {
                if (strstr(str, master_stmts[ii])) {
                    gather = 1;
                    break;
                }
            }
        }
    }
    if (gather) {
        logger->event_mask |= master_event_mask;
        if (logger->iq) {
            /* force legacy code to call reqprintf functions */
            logger->iq->debug = 1;
        }
    }

    logger->mask = logger->event_mask | logger->dump_mask;

    logger->in_request = 1;

    if (verbose) {
        logmsg(LOGMSG_USER, "gather=%d opcode=%d mask=0x%x\n", gather,
               logger->opcode, logger->mask);
    }
}

/* Set up the request logger for a new regular request with an ireq. */
void reqlog_new_request(struct ireq *iq)
{
    struct reqlogger *logger;

    logger = iq->reqlogger;
    if (!logger) {
        return;
    }

    logger->startus = iq->nowus;
    logger->startprcsus = logger->startus;
    logger->iq = iq;
    logger->opcode = iq->opcode;
    if (iq->is_fromsocket) {
        logger->request_type = "socket/fstsnd request";
    } else {
        logger->request_type = "regular request";
    }
    reqlog_start_request(logger);
}

inline void reqlog_set_sql(struct reqlogger *logger, struct string_ref *sr)
{
    put_ref(&logger->sql_ref);
    if (sr) {
        logger->sql_ref = get_ref(sr);
        reqlog_logf(logger, REQL_INFO, "sql=%s", string_ref_cstr(logger->sql_ref));
    }
}

inline void reqlog_set_startprcs(struct reqlogger *logger, uint64_t val)
{
    logger->startprcsus = val;
}

void reqlog_new_sql_request(struct reqlogger *logger, struct string_ref *sr)
{
    if (!logger) {
        return;
    }
    logger->request_type = "sql_request";
    logger->opcode = OP_SQL;
    logger->startus = comdb2_time_epochus();
    logger->startprcsus = logger->startus;
    reqlog_start_request(logger);

    logger->nsqlreqs = ATOMIC_LOAD32(gbl_nnewsql);
    if (sr) {
        reqlog_set_sql(logger, sr);
    }
}

void reqlog_diffstat_init(struct reqlogger *logger)
{
    if (!logger) {
        return;
    }

    reqlog_reset_logger(logger);
    logger->request_type = "stat dump";
    logger->opcode = OP_DEBUG;
    if (!reqlog_init_off) {
        logger->mask = REQL_INFO;
        logger->event_mask = REQL_INFO;
    }
}

/* Get the origin string for the request */
static const char *reqorigin(struct reqlogger *logger)
{
    if (logger->iq) {
        return getorigin(logger->iq);
    } else if (logger->origin[0]) {
        return logger->origin;
    } else {
        return "<unknown origin>";
    }
}

struct reqlog_print_callback_args {
    struct reqlogger *logger;
    struct output *out;
};

static int reqlog_print_callback(const char *s, void *context)
{
    struct reqlog_print_callback_args *args = context;
    dumpf(args->logger, args->out, "%s", s);
    return 0;
}

/* same as dump_client_query_stats, except print in readable text */
static void print_client_query_stats(struct reqlogger *logger,
                                     struct client_query_stats *st,
                                     struct output *out)
{
    for (int ii = 0; ii < st->n_components; ii++) {
        dumpf(logger, out, "    ");
        if (st->path_stats[ii].ix >= 0)
            dumpf(logger, out, "index %d on ", st->path_stats[ii].ix);
        dumpf(logger, out, "table %s ", st->path_stats[ii].table);
        dumpf(logger, out, "finds %d ", st->path_stats[ii].nfind);
        dumpf(logger, out, "next/prev %d ", st->path_stats[ii].nnext);
        if (st->path_stats[ii].nwrite)
            dumpf(logger, out, "nwrite %d ", st->path_stats[ii].nwrite);
        dumpf(logger, out, "\n");
    }
}

/* print the request header for the request. */
static void log_header_ll(struct reqlogger *logger, struct output *out,
                          int is_running)
{
    const struct berkdb_thread_stats *thread_stats = bdb_get_thread_stats();
    struct reqlog_print_callback_args args;

    if (out == long_request_out) {
        dumpf(logger, out, "LONG REQUEST %d msec ", U2M(logger->durationus));
    } else {
        dumpf(logger, out, "%s %d msec ", logger->request_type,
              U2M(logger->durationus));
    }

    /* If fingerprinting is enabled and the logger has a fingerprint,
       log the fingerprint as well. */
    if (gbl_fingerprint_queries && logger->have_fingerprint) {
        char expanded_fp[2 * FINGERPRINTSZ + 1];
        util_tohex(expanded_fp, logger->fingerprint, FINGERPRINTSZ);
        dumpf(logger, out, "for fingerprint %.*s", FINGERPRINTSZ * 2,
                    expanded_fp);
    }

    struct sqlclntstate *clnt = logger->clnt;
    if(clnt) {
        dumpf(logger, out, " pid %d task %s",
              clnt->conninfo.pid, clnt->argv0 ? clnt->argv0 : "???");
    }

    dumpf(logger, out, " rqid %s from %s", logger->id, reqorigin(logger));

    if (out == long_request_out && is_running) {
        /* No need to include 'rc' if the request is still running. */
        dumpf(logger, out, " **running**\n");
    } else {
        dumpf(logger, out, " rc %d\n", logger->rc);
    }

    if (logger->iq) {
        struct ireq *iq = logger->iq;
        if (iq->reptimems > 0) {
            uint64_t rate = iq->total_txnsize / iq->reptimems;

            dumpf(logger, out, "  Wrote %llu log bytes in %d ms rep time (%llu bytes/ms)\n", iq->txnsize, iq->reptimems,
                  rate);
        } else {
            dumpf(logger, out, "  Wrote %llu log bytes\n", iq->total_txnsize);
        }

        dumpf(logger, out, " nretries %d reply len %td\n", iq->retries,
              (ptrdiff_t)(iq->p_buf_out - iq->p_buf_out_start));
    }

    args.logger = logger;
    args.out = out;
    bdb_print_stats(thread_stats, "  ", reqlog_print_callback, &args);

    if (out == long_request_out &&
        bdb_attr_get(thedb->bdb_attr, BDB_ATTR_SHOW_COST_IN_LONGREQ)) {
        struct client_query_stats *qstats = get_query_stats_from_thd();
        if (qstats) print_client_query_stats(logger, qstats, out);
    }
    log_all_events(logger, out);
}

static void log_header(struct reqlogger *logger, struct output *out,
                       int is_running)
{
    Pthread_mutex_lock(&rules_mutex);
    Pthread_mutex_lock(&out->mutex);
    log_header_ll(logger, out, is_running);
    Pthread_mutex_unlock(&out->mutex);
    Pthread_mutex_unlock(&rules_mutex);
}

static void print_str(const char *data, int len, char *s, int n)
{
    int need = len + 3; // space for ''
    if (n < need) {
        int half = (n - 5) / 2; /* minus '..' and NUL */
        snprintf(s, n, "'%.*s..%.*s'", half, data, half, data + len - half);
    } else {
        snprintf(s, n, "'%.*s'", len, data);
    }
}

static void print_blob(const char *data, int len, char *s, int n)
{
    int need = (len * 2) + 4; // space for x''

    if (n < need) {
        int half = (n - 6) / 2; /* minus x'..' and NULL */
        char buf0[half + 1], buf1[half + 1];
        half /= 2;
        util_tohex(buf0, data, half);
        util_tohex(buf1, data + len - half, half);
        snprintf(s, n, "x'%s..%s'", buf0, buf1);
    } else {
        char buf[len * 2 + 1];
        util_tohex(buf, data, len);
        snprintf(s, n, "x'%s'", buf);
    }
}

static void print_ym(const intv_t *tv,  char *s, int n)
{
    const intv_ym_t *ym = &tv->u.ym;
    snprintf(s, n, "%s%u-%2.2u", tv->sign == -1 ? "-" : "", ym->years, ym->months);
}

static void print_ds(const intv_t *tv, char *s, int n)
{
    const intv_ds_t *ds = &tv->u.ds;
    snprintf(s, n, "%s%u %2.2u:%2.2u:%2.2u.%.*u", tv->sign == -1 ? "- " : "",
             ds->days, ds->hours, ds->mins, ds->sec, ds->prec, ds->frac);
}

char *param_string_value(struct sqlclntstate *clnt, int n, char *out, int outlen) {
    struct param_data p = {0};

    if (param_value(clnt, &p, n) != 0) {
        return NULL;
    }
    int len;
    char value[64];
    char *type = CLIENT_TYPE_TO_STR(p.type);
    if (p.null || p.type == COMDB2_NULL_TYPE) {
        snprintf(value, sizeof(value), "null");
    } else {
        switch (p.type) {
            case CLIENT_UINT:
            case CLIENT_INT:
                snprintf(value, sizeof(value), "%"PRId64, p.u.i);
                break;
            case CLIENT_REAL:
                snprintf(value, sizeof(value), "%f", p.u.r);
                break;
            case CLIENT_CSTR:
            case CLIENT_PSTR:
            case CLIENT_PSTR2:
            case CLIENT_VUTF8:
                print_str(p.u.p, p.len, value, sizeof(value));
                break;
            case CLIENT_BYTEARRAY:
            case CLIENT_BLOB:
            case CLIENT_BLOB2:
                print_blob(p.u.p, p.len, value, sizeof(value));
                break;
            case CLIENT_DATETIME:
            case CLIENT_DATETIMEUS: {
                dttz_to_str(&p.u.dt, value, sizeof(value), &len, clnt->tzname);
                break; }
            case CLIENT_INTVYM:
                print_ym(&p.u.tv, value, sizeof(value));
                break;
            case CLIENT_INTVDS:
            case CLIENT_INTVDSUS:
                print_ds(&p.u.tv, value, sizeof(value));
                break;
            default:
                value[0] = 0;
                break;
        }
    }
    if (p.pos)
        snprintf(out, outlen,
                "param%-3d type=%-12s len=%-3d indx=%-16d value=%s", n,
                type, p.len, p.pos, value);
    else
        snprintf(out, outlen,
                "param%-3d type=%-12s len=%-3d name=%-16s value=%s", n,
                type, p.len, p.name, value);
    out[outlen-1]=0;
    return out;
}

static void log_params(struct reqlogger *logger)
{
    struct sqlclntstate *clnt = logger->clnt;
    int n = param_count(clnt);
    if (n <= 0) return;
    char param_out[256];
    reqlog_logf(logger, REQL_INFO, "params=%d\n", n);
    for (int i = 0; i < n; ++i) {
        char *value = param_string_value(clnt, i, param_out, sizeof(param_out));
        if (value)
            reqlog_logf(logger, REQL_INFO, "%s", param_out);
    }
}

static void log_all_events(struct reqlogger *logger, struct output *out)
{
    struct logevent *event;

    /* now scan for all tidbits of information about the request to publish */
    for (event = logger->events; event; event = event->next) {
        if (event->type == EVENT_PRINT) {
            struct print_event *pevent = (struct print_event *)event;
            if (pevent->event_flag & REQL_INFO) {
                if (pevent->length < 0) {
                    pevent->length = strlen(pevent->text);
                }
                if (logger->dumplinepos != 0 &&
                    pevent->length + logger->dumplinepos > 70) {
                    flushdump(logger, out);
                }
                if (logger->dumplinepos == 0) {
                    dump(logger, out, "  ", 2);
                } else {
                    dump(logger, out, ", ", 2);
                }
                assert(strlen(pevent->text) + 1 >= pevent->length);
                dump(logger, out, pevent->text, pevent->length);
            }
        }
    }
    flushdump(logger, out);
}

static void log_rule(struct reqlogger *logger, struct output *out,
                     unsigned event_mask)
{
    struct logevent *event;

    Pthread_mutex_lock(&out->mutex);
    prefix_init(&logger->prefix);
    log_header_ll(logger, out, 0);
    if (event_mask == 0) {
        Pthread_mutex_unlock(&out->mutex);
        return;
    }
    /* print all events that this rule wanted to log */
    for (event = logger->events; event; event = event->next) {
        struct push_prefix_event *pushevent;
        struct print_event *pevent;
        switch (event->type) {
        case EVENT_PUSH_PREFIX:
            pushevent = (struct push_prefix_event *)event;
            if (pushevent->length < 0) {
                pushevent->length = strlen(pushevent->text);
            }
            prefix_push(&logger->prefix, pushevent->text, pushevent->length);
            break;

        case EVENT_POP_PREFIX: prefix_pop(&logger->prefix); break;

        case EVENT_POP_PREFIX_ALL: prefix_pop_all(&logger->prefix); break;

        case EVENT_PRINT:
            pevent = (struct print_event *)event;
            if (pevent->event_flag & event_mask) {
                if (pevent->length < 0) {
                    pevent->length = strlen(pevent->text);
                }
                assert(strlen(pevent->text) + 1 >= pevent->length);
                dump(logger, out, pevent->text, pevent->length);
            }
            break;

        default:
            Pthread_mutex_unlock(&out->mutex);
            logmsg(LOGMSG_ERROR, "%s: bad event type %d?!\n", __func__,
                   event->type);
            return;
        }
    }
    flushdump(logger, out);
    logger->prefix.pos = 0;
    dump(logger, out, "----------", 10);
    flushdump(logger, out);
    Pthread_mutex_unlock(&out->mutex);
}

static int inrange(const struct range *range, int value)
{
    if (range->from >= 0 && value < range->from) {
        return 0;
    } else if (range->to >= 0 && value > range->to) {
        return 0;
    } else {
        return 1;
    }
}

static int indblrange(const struct dblrange *range, double value)
{
    if (range->from >= 0 && value < range->from) {
        return 0;
    } else if (range->to >= 0 && value > range->to) {
        return 0;
    } else {
        return 1;
    }
}

inline void reqlog_set_cost(struct reqlogger *logger, double cost)
{
    if (logger) logger->sqlcost = cost;
}

inline void reqlog_set_rows(struct reqlogger *logger, int rows)
{
    if (logger) logger->sqlrows = rows;
}

uint64_t reqlog_current_us(struct reqlogger *logger)
{
    return (comdb2_time_epochus() - logger->startus);
}

inline void reqlog_set_rqid(struct reqlogger *logger, void *id, int idlen)
{
    assert (idlen == sizeof(unsigned long long) || idlen == sizeof(uuid_t));
    if (idlen == sizeof(uuid_t))
        comdb2uuidstr(id, logger->id);
    else
        sprintf(logger->id, "%llx", *(unsigned long long *)id);
    logger->have_id = 1;
}

static int current_long_request_count = 0;
static int current_long_request_duration_ms = 0;
static int current_longest_long_request_ms = 0;
static int current_shortest_long_request_ms = INT_MAX;

void reqlog_long_running_clnt(struct sqlclntstate *clnt)
{
    if (clnt->done || !clnt->thd || !clnt->sql || !clnt->thd->logger) return;

    if (can_consume(clnt) == 1) {
        return; /* Do not log consumers */
    }

    struct string_ref *sql = clnt->sql_ref;
    struct reqlogger logger;

    memset(&logger, 0, sizeof(struct reqlogger));

    logger.request_type = "sql_request";
    logger.opcode = OP_SQL;
    logger.startus = clnt->enque_timeus;
    logger.startprcsus = logger.startus;

    logger.durationus = comdb2_time_epochus() - logger.startprcsus;

    int duration_ms = U2M(logger.durationus);

    if (duration_ms < gbl_sql_time_threshold) {
        return;
    }

    current_long_request_duration_ms = duration_ms;

    if (duration_ms > current_longest_long_request_ms) {
        current_longest_long_request_ms = duration_ms;
    }

    if (duration_ms < current_shortest_long_request_ms) {
        current_shortest_long_request_ms = duration_ms;
    }

    logger.rc = 0;

    /* Should this long running statement be reported at this instant? */

    int duration_beyond_thresh_sec = (duration_ms - gbl_sql_time_threshold) / 1000;
    if ((duration_beyond_thresh_sec % gbl_longreq_log_freq_sec) != 0) {
        return;
    }

    if (!reqlog_init_off && duration_beyond_thresh_sec >= 0 &&
        duration_beyond_thresh_sec < gbl_longreq_log_freq_sec) {
        /* Only for the first time, also log more information (like sql)
           about this long running query. */
        logger.event_mask |= REQL_INFO;
    }
    logger.mask = logger.event_mask | logger.dump_mask;

    reqlog_set_clnt(&logger, clnt);
    reqlog_set_origin(&logger, "%s", clnt->origin);
    reqlog_set_vreplays(&logger, clnt->verify_retries);
    reqlog_set_sql(&logger, sql);

    if (gbl_fingerprint_queries) {
        unsigned char fp[FINGERPRINTSZ];
        size_t unused;
        const char *normSql = NULL;
        if (is_stored_proc(clnt)) {
            normSql = clnt->work.zOrigNormSql;
        } else {
            normSql = (clnt->work.zNormSql) ? clnt->work.zNormSql :
          clnt->work.zOrigNormSql;
        }
        if (normSql) {
            calc_fingerprint(normSql, &unused, fp);
            reqlog_set_fingerprint(&logger, (const char *)fp, FINGERPRINTSZ);
        }
    }

    if (logger.vreplays) {
        reqlog_logf(&logger, REQL_INFO, "verify replays=%d", logger.vreplays);
    }

    flushdump(&logger, NULL);

    log_header(&logger, long_request_out, 1);

    current_long_request_count++;

    reqlog_free_all(&logger);

    put_ref(&logger.sql_ref);
}

void reqlog_log_all_longreqs(void)
{
    /* Periodic long request logging disabled */
    if (gbl_longreq_log_freq_sec < 0)
        return;

    /*
      Also disable periodic logging if "long request threshold" is set to <= 0.
      It is a popular trick to temporarily set long sql request threshold to 0
      to force logging of all statements to the longreqs log. We don't want to
      double log all the statements.
    */
    if (gbl_sql_time_threshold <= 0)
        return;

    current_long_request_count = 0;
    current_long_request_duration_ms = 0;
    current_longest_long_request_ms = 0;
    current_shortest_long_request_ms = INT_MAX;

    reqlog_long_running_sql_statements();

    if ((current_long_request_count > 0) && (long_request_out != default_out)) {
        if (current_long_request_count == 1) {
            logmsg(LOGMSG_USER, "LONG SQL REQUEST (%d ms) still running (see %s for details)\n",
                   current_long_request_duration_ms,
                   long_request_out->filename);
        } else {
            logmsg(LOGMSG_USER, "%d LONG SQL REQUESTS (%d ms - %d ms) still running (see %s for details)\n",
                   current_long_request_count,
                   current_shortest_long_request_ms,
                   current_longest_long_request_ms,
                   long_request_out->filename);
        }
    }
}

pthread_mutex_t reqlog_longreq_log_mtx = PTHREAD_MUTEX_INITIALIZER;

/* End of a request. */
void reqlog_end_request(struct reqlogger *logger, int rc, const char *callfunc,
                        int line)
{
    static int long_request_count = 0;
    static int last_long_request_epoch = 0;
    static int longest_long_request_ms = 0;
    static int shortest_long_request_ms = INT_MAX;

    struct logruleuse {
        struct logruleuse *next;
        struct output *out;
        unsigned event_mask;
    };

    struct logrule *rule;
    struct logrule *tmprule;

    struct logruleuse *use_rules = NULL;
    struct logruleuse *use_rule;

    int long_request_thresh;

    if (!logger)
        return;
    if (!logger->in_request)
        goto out;

    if (logger->sqlrows > 0) {
        reqlog_logf(logger, REQL_INFO, "rowcount=%d", logger->sqlrows);
    }
    if (logger->sqlcost > 0) {
        reqlog_logf(logger, REQL_INFO, "cost=%f", logger->sqlcost);
    }
    if (logger->vreplays) {
        reqlog_logf(logger, REQL_INFO, "verify replays=%d", logger->vreplays);
    }

    logger->in_request = 0;

    flushdump(logger, NULL);

    logger->rc = rc;

    logger->durationus =
        (comdb2_time_epochus() - logger->startprcsus) + logger->queuetimeus;

    eventlog_add(logger);

    /* now see if this matches any of our rules */
    if (rules.count != 0) {
        Pthread_mutex_lock(&rules_mutex);
        LISTC_FOR_EACH_SAFE(&rules, rule, tmprule, linkv)
        {
            if (!rule->active) {
                continue;
            }

            if (logger->iq) {
                /* rules that apply only to regular style requests */
                if (!inrange(&rule->retries, logger->iq->retries)) {
                    continue;
                }
            }

            if (!inrange(&rule->duration, U2M(logger->durationus))) {
                continue;
            }

            if (!inrange(&rule->vreplays, logger->vreplays)) {
                continue;
            }

            if (!indblrange(&rule->sql_cost, logger->sqlcost)) {
                continue;
            }

            if (!inrange(&rule->sql_rows, logger->sqlrows)) {
                continue;
            }

            if (!check_list(&rule->opcode_list, logger->opcode)) {
                continue;
            }

            if (!check_list(&rule->rc_list, logger->rc)) {
                continue;
            }

            if (rule->stmt[0] &&
                (!logger->sql_ref || !strstr(string_ref_cstr(logger->sql_ref), rule->stmt))) {
                continue;
            }

            if (rule->tablename[0]) {
                struct tablelist *table;
                for (table = logger->tables; table; table = table->next) {
                    if (strcasecmp(table->name, rule->tablename) == 0) {
                        break;
                    }
                }
                if (!table) {
                    continue;
                }
            }

            if (verbose) {
                logmsg(LOGMSG_USER, "matched rule %s event_mask 0x%x\n",
                       rule->name, rule->event_mask);
            }

            /* all conditions met (or no conditions); log this request
             * using this rule. */
            for (use_rule = use_rules; use_rule; use_rule = use_rule->next) {
                if (use_rule->out == rule->out) {
                    use_rule->event_mask |= rule->event_mask;
                    break;
                }
            }
            if (!use_rule) {
                use_rule = malloc(sizeof(struct logruleuse));
                if (!use_rule) {
                    logmsg(LOGMSG_ERROR, "%s:malloc failed\n", __func__);
                } else {
                    use_rule->next = use_rules;
                    use_rule->event_mask = rule->event_mask;
                    use_rule->out = rule->out;
                    use_rules = use_rule;
                    use_rule->out->refcount++;
                }
            }

            if (rule->count > 0) {
                rule->count--;
                if (rule->count == 0) {
                    /* discard this rule */
                    logmsg(LOGMSG_USER, "Discarding logging rule '%s'\n",
                           rule->name);
                    del_rule_ll(rule);
                }
            }
        }
        for (use_rule = use_rules; use_rule; use_rule = use_rule->next) {
            if (verbose) {
                logmsg(LOGMSG_USER, "print to %s with event_mask 0x%x\n",
                       use_rule->out->filename, use_rule->event_mask);
            }
            log_rule(logger, use_rule->out, use_rule->event_mask);
            deref_output_ll(use_rule->out);
        }
        while ((use_rule = use_rules) != NULL) {
            use_rules = use_rule->next;
            free(use_rule);
        }

        Pthread_mutex_unlock(&rules_mutex);
    }

    /* check for bad cstrings */
    if (logger->reqflags & REQL_BAD_CSTR_FLAG) {
        logmsg(LOGMSG_WARN,
               "WARNING: THIS DATABASE IS RECEIVING NON NUL TERMINATED CSTRINGS\n");
        log_header(logger, default_out, 0);
    }

    /* check for long requests */
    if (logger->opcode == OP_SQL && !logger->iq) {
        long_request_thresh = gbl_sql_time_threshold;
    } else {
        long_request_thresh = long_request_ms;
    }

    if (logger->durationus >= M2U(long_request_thresh)) {
        if (logger->clnt) {
            log_params(logger);
        }

        log_header(logger, long_request_out, 0);
        long_reqs++;

        Pthread_mutex_lock(&reqlog_longreq_log_mtx);
        {
            if (logger->durationus > M2U(longest_long_request_ms)) {
                longest_long_request_ms = U2M(logger->durationus);
            }
            if (shortest_long_request_ms == -1 ||
                logger->durationus < M2U(shortest_long_request_ms)) {
                shortest_long_request_ms = U2M(logger->durationus);
            }
            long_request_count++;
            if (last_long_request_epoch != comdb2_time_epoch()) {
                last_long_request_epoch = comdb2_time_epoch();

                if (long_request_out != default_out && long_request_thresh && logger->clnt && !can_consume(logger->clnt)) {

                    if (logger->iq && logger->iq->sorese) {
                        char *sqlinfo = osql_sess_info(logger->iq->sorese);
                        if (long_request_count == 1) {
                            logmsg(LOGMSG_USER,
                                   "LONG REQUEST (%d ms) finished [%s time %d] (see %s for details)\n",
                                   U2M(logger->durationus),
                                   sqlinfo,
                                   U2M(logger->iq->sorese->sess_endus - logger->iq->sorese->sess_startus),
                                   long_request_out->filename);
                        } else {
                            logmsg(LOGMSG_USER,
                                   "%d LONG REQUESTS (%d ms - %d ms) finished [last %s] (see %s for details)\n",
                                   long_request_count,
                                   shortest_long_request_ms,
                                   longest_long_request_ms,
                                   sqlinfo,
                                   long_request_out->filename);
                        }
                        free(sqlinfo);
                    } else {
                        if (long_request_count == 1) {
                            logmsg(LOGMSG_USER, "LONG REQUEST (%d ms) finished (see %s for details)\n",
                                   U2M(logger->durationus),
                                   long_request_out->filename);
                        } else {
                            logmsg(LOGMSG_USER,
                                   "%d LONG REQUESTS (%d ms - %d ms) finished (see %s for details)\n",
                                   long_request_count, shortest_long_request_ms,
                                   longest_long_request_ms,
                                   long_request_out->filename);
                        }
                    }
                }
                long_request_count = 0;
                longest_long_request_ms = 0;
                shortest_long_request_ms = -1;
            }
        }
        Pthread_mutex_unlock(&reqlog_longreq_log_mtx);
    } else {
        norm_reqs++;
    }

    if (logger->iq && logger->iq->sorese) {
        if (gbl_time_osql)
            osql_bplog_time_done(&logger->iq->timings);

        /* here, closing the session doesn't destroy iq*/
        logger->iq->sorese->iq = NULL;
        osql_sess_close(&logger->iq->sorese, 1);
    }
out:
    reqlog_reset_logger(logger); //will reset which bzeros much of logger
    assert(logger->have_id == 0);
    assert(logger->have_fingerprint == 0);
    assert(logger->error_code == 0);
    assert(logger->path == 0);
}

/* this is meant to be called by only 1 thread, will need locking if
 * more than one threads were to be involved */
void reqlog_diffstat_dump(struct reqlogger *logger)
{
    if (!logger) {
        return;
    }
    log_all_events(logger, stat_request_out);
    reqlog_diffstat_init(logger);
}

int reqlog_diffstat_thresh()
{
    return diffstat_thresh;
}

void reqlog_set_diffstat_thresh(int val)
{
    diffstat_thresh = val;
    logmsg(LOGMSG_USER, "diffstat threshold now %d s\n", diffstat_thresh);
    if (diffstat_thresh == 0) {
        logmsg(LOGMSG_USER, "diffstat thresh feature is disabled\n");
    }
}

int reqlog_truncate()
{
    return reqltruncate;
}

inline void reqlog_set_truncate(int val)
{
    reqltruncate = val;
    logmsg(LOGMSG_USER, "truncate %s\n", reqltruncate ? "enabled" : "disabled");
}

/* Client Stats LRU Hash */
hash_t *clientstats = NULL;
static LISTC_T(struct nodestats) clntlru;

pthread_rwlock_t clientstats_lk = PTHREAD_RWLOCK_INITIALIZER;
pthread_mutex_t clntlru_mtx = PTHREAD_MUTEX_INITIALIZER;

int gbl_max_clientstats_cache = 10000;

void init_clientstats_table()
{
    clientstats = hash_init_o(offsetof(nodestats_t, checksum),
                              sizeof(unsigned) + sizeof(int));
    listc_init(&clntlru, offsetof(struct nodestats, linkv));
    assert(clientstats);
}

#define UNKNOWN_NAME "Unknown"
#define GET_NAME_AND_LEN(s, s_len)                                             \
    do {                                                                       \
        if (!s || (s_len = strlen(s) + 1) <= 1) {                              \
            s = UNKNOWN_NAME;                                                  \
            s_len = sizeof(UNKNOWN_NAME);                                      \
        }                                                                      \
    } while (0);

static int hash_free_element(void *elem, void *unused) {
    free(elem);
    return 0;
}

static nodestats_t *add_clientstats(unsigned checksum,
                                    const char *task_and_stack, int task_len,
                                    int stack_len, int node, int fd)
{
    int nclntstats;
    nodestats_t *old_entry = NULL;
    nodestats_t *entry = NULL;
    nodestats_t *entry_chk = NULL;

    entry = calloc(1, offsetof(nodestats_t, mem) + task_len + stack_len);
    if (entry == NULL) {
        logmsg(LOGMSG_ERROR, "%s: out of memory!\n", __func__);
        return NULL;
    }

    /* hashtable key is the checksum: crc32c(task + stack) + nodeix. */
    memcpy(entry->mem, task_and_stack, task_len + stack_len);
    entry->checksum = checksum;
    entry->node = node;
    Pthread_mutex_init(&entry->rawtotals.lk, NULL);

    Pthread_rwlock_wrlock(&clientstats_lk);
    {
        entry_chk = hash_find(clientstats, entry);
        if (entry_chk) {
            free(entry);
            entry = entry_chk;
            Pthread_mutex_lock(&entry->mtx);
            entry->ref++;
            if (entry->ref == 1) {
                Pthread_mutex_lock(&clntlru_mtx);
                listc_rfl(&clntlru, entry);
                Pthread_mutex_unlock(&clntlru_mtx);
            }
            Pthread_mutex_unlock(&entry->mtx);
        } else {
            entry->task = entry->mem;
            entry->stack = entry->mem + task_len;
            entry->host = intern(nodeat(node));
            Pthread_mutex_init(&entry->mtx, 0);
            entry->ref = 1;
            entry->rawtotals.svc_time = time_metric_new("svc_time");
            entry->prevtotals.svc_time = entry->rawtotals.svc_time;

            if (fd < 0) {
                bzero(&(entry->addr), sizeof(struct in_addr));
            } else {
                struct sockaddr_in peeraddr;
                socklen_t len = sizeof(peeraddr);
                bzero(&peeraddr, sizeof(peeraddr));
                if (getpeername(fd, (struct sockaddr *)&peeraddr, &len) < 0) {
                    if (errno != ENOTCONN)
                        logmsg(LOGMSG_ERROR, "%s: getpeername failed fd %d: %d %s\n", __func__, fd, errno,
                               strerror(errno));
                    bzero(&(entry->addr), sizeof(struct in_addr));
                } else {
                    memcpy(&(entry->addr), &peeraddr.sin_addr,
                           sizeof(struct in_addr));
                }
            }
            Pthread_mutex_lock(&clntlru_mtx);
            while ((nclntstats = hash_get_num_entries(clientstats) + 1) >
                   gbl_max_clientstats_cache) {
                old_entry = listc_rtl(&clntlru); // get+remove oldest from list
                if (old_entry) {
                    hash_del(clientstats, old_entry);
                    Pthread_mutex_destroy(&old_entry->mtx);
                    Pthread_mutex_destroy(&old_entry->rawtotals.lk);
                    if (old_entry->rawtotals.fingerprints) {
                        hash_for(old_entry->rawtotals.fingerprints, hash_free_element, NULL);
                        hash_free(old_entry->rawtotals.fingerprints);
                    }
                    time_metric_free(old_entry->rawtotals.svc_time);
                    free(old_entry);
                } else {
                    if (gbl_max_clientstats_cache &&
                        (nclntstats % gbl_max_clientstats_cache == 1))
                        logmsg(LOGMSG_ERROR,
                               "%s: too many clientstats %d, max %d\n",
                               __func__, nclntstats, gbl_max_clientstats_cache);
                    break;
                }
            }
            Pthread_mutex_unlock(&clntlru_mtx);
            hash_add(clientstats, entry);
        }
    }
    Pthread_rwlock_unlock(&clientstats_lk);

    return entry;
}

static nodestats_t *find_clientstats(unsigned checksum, int node, int fd)
{
    nodestats_t key;
    nodestats_t *entry = NULL;
    key.checksum = checksum;
    key.node = node;
    Pthread_rwlock_rdlock(&clientstats_lk);
    {
        entry = hash_find_readonly(clientstats, &key);
        if (entry) {
            Pthread_mutex_lock(&entry->mtx);
            entry->ref++;
            if (entry->ref == 1) {
                Pthread_mutex_lock(&clntlru_mtx);
                listc_rfl(&clntlru, entry);
                Pthread_mutex_unlock(&clntlru_mtx);
            }
            Pthread_rwlock_unlock(&clientstats_lk);
            if (*(unsigned *)&(entry->addr) == 0 && fd > 0) {
                struct sockaddr_in peeraddr;
                socklen_t len = sizeof(peeraddr);
                bzero(&peeraddr, sizeof(peeraddr));
                if (getpeername(fd, (struct sockaddr *)&peeraddr, &len) < 0) {
                    if (errno != ENOTCONN)
                        logmsg(LOGMSG_ERROR, "%s: getpeername failed fd %d: %d %s\n", __func__, fd, errno,
                               strerror(errno));
                    bzero(&(entry->addr), sizeof(struct in_addr));
                } else {
                    memcpy(&(entry->addr), &peeraddr.sin_addr,
                           sizeof(struct in_addr));
                }
            }
            Pthread_mutex_unlock(&entry->mtx);
            return entry;
        }
    }
    Pthread_rwlock_unlock(&clientstats_lk);
    return NULL;
}

static int release_clientstats(unsigned checksum, int node)
{
    int rc = 0;
    nodestats_t key;
    nodestats_t *entry = NULL;
    key.checksum = checksum;
    key.node = node;
    Pthread_rwlock_rdlock(&clientstats_lk);
    {
        if ((entry = hash_find_readonly(clientstats, &key)) != NULL) {
            Pthread_mutex_lock(&entry->mtx);
            entry->ref--;
            if (entry->ref < 0) {
                logmsg(LOGMSG_ERROR,
                       "key released more often than found, ref %d\n",
                       entry->ref);
                entry->ref = 0;
            }
            if (entry->ref == 0) {
                Pthread_mutex_lock(&clntlru_mtx);
                listc_abl(&clntlru, entry);
                Pthread_mutex_unlock(&clntlru_mtx);
            }
            Pthread_mutex_unlock(&entry->mtx);
        } else {
            rc = -1;
        }
    }
    Pthread_rwlock_unlock(&clientstats_lk);
    return rc;
}

struct rawnodestats *get_raw_node_stats(const char *task, const char *stack,
                                        char *host, int fd, int is_ssl)
{
    struct nodestats *nodestats = NULL;
    unsigned checksum;
    int namelen, node;
    int task_len, stack_len = 0;
    char *tmp;

    host = intern(host);
    node = nodeix(host);
    GET_NAME_AND_LEN(task, task_len);
    GET_NAME_AND_LEN(stack, stack_len);

    namelen = task_len + stack_len;
    if (namelen < 1024)
        tmp = alloca(namelen);
    else
        tmp = malloc(namelen);
    if (!tmp) {
        logmsg(LOGMSG_ERROR, "%s: out of memory\n", __func__);
        return NULL;
    }
    memcpy(tmp, task, task_len);
    memcpy(tmp + task_len, stack, stack_len);
    checksum = crc32c((const uint8_t *)tmp, namelen);
    if ((nodestats = find_clientstats(checksum, node, fd)) == NULL) {
        nodestats =
            add_clientstats(checksum, tmp, task_len, stack_len, node, fd);
        if (nodestats == NULL) {
            logmsg(
                LOGMSG_ERROR,
                "%s: failed to add client stats, task %s, stack %s, node %d\n",
                __func__, task, stack, node);
        }
    }

    /* The same task name from the same node may switch from plaintext to SSL
     * on a config change; it may also fall back from SSL to plaintext, on an SSL
     * error. Always get its latest SSL status from clnt. */
    nodestats->is_ssl = is_ssl;

    if (tmp && namelen >= 1024)
        free(tmp);
    return nodestats ? &(nodestats->rawtotals) : NULL;
}

int release_node_stats(const char *task, const char *stack, char *host)
{
    unsigned checksum;
    int namelen;
    int task_len, stack_len = 0;
    char *tmp;

    host = intern(host);
    GET_NAME_AND_LEN(task, task_len);
    GET_NAME_AND_LEN(stack, stack_len);

    namelen = task_len + stack_len;
    if (namelen < 1024)
        tmp = alloca(namelen);
    else
        tmp = malloc(namelen);
    if (!tmp)
        return -1;
    memcpy(tmp, task, task_len);
    memcpy(tmp + task_len, stack, stack_len);
    checksum = crc32c((const uint8_t *)tmp, namelen);
    if (release_clientstats(checksum, nodeix(host)) != 0) {
        logmsg(LOGMSG_ERROR,
               "%s: failed to release host=%s, node=%d, task=%s, stack=%s\n",
               __func__, host, nodeix(host), task, stack);
        cheap_stack_trace();
    }

    if (tmp && namelen >= 1024)
        free(tmp);
    return 0;
}

typedef struct {
    nodestats_t **list;
    int i;
    int n;
} clientstats_ptr_list;

int collect_clientstats(void *ent, void *arg)
{
    nodestats_t *entry = ent;
    clientstats_ptr_list *klist = arg;
    if (klist->i < klist->n) {
        klist->list[klist->i] = entry;
    } else
        logmsg(LOGMSG_ERROR, "%s: BUG! expected %d entries but saw more\n",
               __func__, klist->n);
    klist->i += 1;
    return 0;
}

/* called (roughly) once a second to update our per node stats */
void process_nodestats(void)
{
    static int last_time_ms = 0;
    int span_ms;
    clientstats_ptr_list cpl;
    nodestats_t *nodestats;
    nodestats_t **list = NULL;
    int i;
    int nclnts = 0;

    if (last_time_ms == 0)
        last_time_ms = comdb2_time_epochms();
    span_ms = comdb2_time_epochms() - last_time_ms;
    last_time_ms = comdb2_time_epochms();

    Pthread_rwlock_rdlock(&clientstats_lk);

    nclnts = hash_get_num_entries(clientstats);
    if (nclnts == 0)
        goto done;
    list = calloc(nclnts, sizeof(nodestats_t *));
    if (list == NULL) {
        logmsg(LOGMSG_ERROR, "%s: no memory\n", __func__);
        goto done;
    }
    cpl.list = list;
    cpl.i = 0;
    cpl.n = nclnts;
    hash_for(clientstats, collect_clientstats, &cpl);
    for (i = 0; i < nclnts; i++) {
        unsigned next_bucket;
        unsigned ii;
        unsigned *nowptr;
        unsigned *prevptr;
        unsigned *bucketptr;
        nodestats = list[i];

        time_metric_purge_old(nodestats->rawtotals.svc_time);

        nowptr = (unsigned *)&nodestats->rawtotals;
        prevptr = (unsigned *)&nodestats->prevtotals;

        nodestats->bucket_spanms[nodestats->cur_bucket] = span_ms;
        bucketptr = (unsigned *)&nodestats->raw_buckets[nodestats->cur_bucket];

        for (ii = 0; ii < NUM_RAW_NODESTATS; ii++) {
            unsigned prev_value, diff;
            prev_value = nowptr[ii];
            diff = prev_value - prevptr[ii];
            prevptr[ii] = prev_value;
            bucketptr[ii] = diff;
        }

        next_bucket = nodestats->cur_bucket + 1;
        if (next_bucket >= NUM_BUCKETS)
            next_bucket = 0;
        nodestats->cur_bucket = next_bucket;
    }
done:
    Pthread_rwlock_unlock(&clientstats_lk);
    if (list)
        free(list);
}

static void snap_nodestats_ll(struct nodestats *nodestats,
                              struct rawnodestats *snap, int disp_rates)
{
    bzero(snap, sizeof(*snap));

    if (!nodestats) {
        return;
    } else if (disp_rates) {
        /* calculate rates for raw figures in n per second */
        unsigned bucket, ii;
        int timespanms = 0;
        unsigned *snapptr = (unsigned *)snap;
        unsigned *bktptr;
        for (bucket = 0; bucket < NUM_BUCKETS; bucket++) {
            timespanms += nodestats->bucket_spanms[bucket];
            bktptr = (unsigned *)&nodestats->raw_buckets[bucket];
            for (ii = 0; ii < NUM_RAW_NODESTATS; ii++) {
                snapptr[ii] += bktptr[ii];
            }
        }

        if (timespanms <= 0)
            timespanms = 1;
        for (ii = 0; ii < NUM_RAW_NODESTATS; ii++) {
            snapptr[ii] = 0.5 + (NUM_BUCKETS * 1000.00 *
                                 ((double)snapptr[ii] / (double)timespanms));
        }
    } else {
        memcpy(snap, &nodestats->prevtotals, sizeof(*snap));
    }
}

struct summary_nodestats *get_nodestats_summary(unsigned *nodes_cnt,
                                                int disp_rates)
{
    unsigned max_clients = 0;
    unsigned ii;
    struct summary_nodestats *summaries = NULL;
    struct nodestats *nodestats;
    struct rawnodestats snap;

    clientstats_ptr_list cpl;
    nodestats_t **list = NULL;
    int i;
    int nclnts = 0;

    Pthread_rwlock_rdlock(&clientstats_lk);

    nclnts = hash_get_num_entries(clientstats);
    if (nclnts == 0)
        goto done;

    summaries = calloc(nclnts, sizeof(struct summary_nodestats));
    if (!summaries) {
        logmsg(LOGMSG_ERROR, "%s: out of memory %u nodes\n", __func__, nclnts);
        goto done;
    }

    list = calloc(nclnts, sizeof(nodestats_t *));
    if (list == NULL) {
        logmsg(LOGMSG_ERROR, "%s: no memory\n", __func__);
        goto done;
    }
    cpl.list = list;
    cpl.i = 0;
    cpl.n = nclnts;
    hash_for(clientstats, collect_clientstats, &cpl);

    for (i = 0, ii = 0; i < nclnts; i++) {
        unsigned opcode;
        nodestats = list[i];

        snap_nodestats_ll(nodestats, &snap, disp_rates);

        summaries[ii].node = machine_num(nodestats->host);
        summaries[ii].host = nodestats->host;
        memcpy(&summaries[ii].addr, &(nodestats->addr), sizeof(struct in_addr));
        summaries[ii].task = strcmp(nodestats->task, UNKNOWN_NAME)
                                 ? strdup(nodestats->task)
                                 : NULL;
        summaries[ii].stack = strcmp(nodestats->stack, UNKNOWN_NAME)
                                  ? strdup(nodestats->stack)
                                  : NULL;
        summaries[ii].ref = nodestats->ref;
        summaries[ii].is_ssl = nodestats->is_ssl;

        summaries[ii].sql_queries = snap.sql_queries;
        summaries[ii].sql_steps = snap.sql_steps;
        summaries[ii].sql_rows = snap.sql_rows;
        summaries[ii].svc_time = time_metric_average(snap.svc_time);

        for (opcode = 0; opcode < MAXTYPCNT; opcode++) {
            unsigned n = snap.opcode_counts[opcode];
            if (!n)
                continue;
            switch (opcode) {
            case OP_FIND:
            case OP_NEXT:
            case OP_JSTNX:
            case OP_JSTFND:
            case OP_FNDRRN:
            case OP_PREV:
            case OP_JSTPREV:
            case OP_FIND2:
            case OP_NEXT2:
            case OP_PREV2:
            case OP_JFND2:
            case OP_JNXT2:
            case OP_JPRV2:
            case OP_FNDKLESS:
            case OP_JFNDKLESS:
            case OP_FNDNXTKLESS:
            case OP_FNDPRVKLESS:
            case OP_JFNDNXTKLESS:
            case OP_JFNDPRVKLESS:
                summaries[ii].finds += n;
                break;

            case OP_STORED:
            case OP_RNGEXT2:
            case OP_RNGEXTP2:
            case OP_RNGEXTTAG:
            case OP_RNGEXTTAGP:
            case OP_RNGEXTTAGTZ:
            case OP_RNGEXTTAGPTZ:
            case OP_NEWRNGEX:
                summaries[ii].rngexts += n;
                break;

            case OP_BLOCK:
            case OP_FWD_BLOCK:
            case OP_LONGBLOCK:
            case OP_FWD_LBLOCK:
            case OP_CLEARTABLE:
            case OP_FASTINIT:
                summaries[ii].writes += n;
                break;

            default:
                summaries[ii].other_fstsnds += n;
                break;
            }
        }

        for (opcode = 0; opcode < BLOCK_MAXOPCODE; opcode++) {
            unsigned n = snap.blockop_counts[gbl_blockop_count_xrefs[opcode]];
            if (!n)
                continue;
            switch (opcode) {
            case BLOCK2_ADDDTA:
            case BLOCK2_ADDKL:
            case BLOCK2_ADDKL_POS:
            case BLOCK_ADDSL:
                summaries[ii].adds += n;
                break;

            case BLOCK_UPVRRN:
            case BLOCK2_UPDATE:
            case BLOCK2_UPDKL:
            case BLOCK2_UPDKL_POS:
                summaries[ii].upds += n;
                break;

            case BLOCK_DELSEC:
            case BLOCK_DELNOD:
            case BLOCK2_DELDTA:
            case BLOCK2_DELKL:
                summaries[ii].dels += n;
                break;

            case BLOCK2_RECOM:
                summaries[ii].recom += n;
                break;

            case BLOCK2_SNAPISOL:
                summaries[ii].snapisol += n;
                break;

            case BLOCK2_SERIAL:
                summaries[ii].serial += n;
                break;
            }
        }
        ii++;
    }
    max_clients = ii;

done:
    Pthread_rwlock_unlock(&clientstats_lk);
    if (list)
        free(list);
    *nodes_cnt = max_clients;
    return summaries;
}

void nodestats_node_report(FILE *fh, const char *prefix, int disp_rates,
                           char *host)
{
    struct nodestats *nodestats;
    struct rawnodestats snap;
    int opcode;

    if (!prefix)
        prefix = "";

    clientstats_ptr_list cpl;
    nodestats_t **list = NULL;
    int i;
    int nclnts = 0;

    Pthread_rwlock_rdlock(&clientstats_lk);

    nclnts = hash_get_num_entries(clientstats);
    if (nclnts == 0)
        goto done;

    list = calloc(nclnts, sizeof(nodestats_t *));
    if (list == NULL) {
        logmsg(LOGMSG_ERROR, "%s: no memory\n", __func__);
        goto done;
    }
    cpl.list = list;
    cpl.i = 0;
    cpl.n = nclnts;
    hash_for(clientstats, collect_clientstats, &cpl);

    host = intern(host);

    for (i = 0; i < nclnts; i++) {
        nodestats = list[i];
        if (nodestats->host != host)
            continue;

        snap_nodestats_ll(nodestats, &snap, disp_rates);

        logmsgf(LOGMSG_USER, fh, "%sRAW STATISTICS FOR NODE %s\n", prefix,
                host);
        logmsgf(LOGMSG_USER, fh,
                "%s--- opcode counts for regular fstsnd requests\n", prefix);
        for (opcode = 0; opcode < MAXTYPCNT; opcode++) {
            if (snap.opcode_counts[opcode]) {
                logmsgf(LOGMSG_USER, fh, "%s%-20s  %u\n", prefix, req2a(opcode),
                        snap.opcode_counts[opcode]);
            }
        }
        logmsgf(LOGMSG_USER, fh,
                "%s--- block operation opcode counts (for transactions)\n",
                prefix);
        for (opcode = 0; opcode < BLOCK_MAXOPCODE; opcode++) {
            if (snap.blockop_counts[gbl_blockop_count_xrefs[opcode]]) {
                logmsgf(LOGMSG_USER, fh, "%s%-20s  %u\n", prefix,
                        breq2a(opcode),
                        snap.blockop_counts[gbl_blockop_count_xrefs[opcode]]);
            }
        }
        logmsgf(LOGMSG_USER, fh, "%s--- SQL statistics\n", prefix);
        if (snap.sql_queries)
            logmsgf(LOGMSG_USER, fh, "%s%-20s  %u\n", prefix, "queries",
                    snap.sql_queries);
        if (snap.sql_steps)
            logmsgf(LOGMSG_USER, fh, "%s%-20s  %u\n", prefix, "steps",
                    snap.sql_steps);
        if (snap.sql_rows)
            logmsgf(LOGMSG_USER, fh, "%s%-20s  %u\n", prefix, "rows",
                    snap.sql_rows);
    }

done:
    Pthread_rwlock_unlock(&clientstats_lk);
    if (list)
        free(list);
}

void nodestats_report(FILE *fh, const char *prefix, int disp_rates)
{
    unsigned max_clients;
    unsigned ii;
    struct summary_nodestats *summaries;

    if (!prefix) prefix = "";

    if (disp_rates) {
        logmsgf(LOGMSG_USER, fh,
                "%sCURRENT REQUEST RATE OVER LAST %d SECONDS\n", prefix,
                NUM_BUCKETS);
    } else {
        logmsgf(LOGMSG_USER, fh, "%sTOTAL REQUESTS SUMMARY\n", prefix);
    }
    logmsgf(LOGMSG_USER, fh,
            "%s%5s | regular fstsnds                 |  blockops               "
            "                                | sql\n",
            prefix, "node");
    logmsgf(LOGMSG_USER, fh,
            "%s%5s |   finds rngexts  writes   other |    adds    upds    dels "
            "blk/sql   recom snapisl  serial | queries   steps    rows\n",
            prefix, "");

    summaries = get_nodestats_summary(&max_clients, disp_rates);
    if (summaries == NULL)
        return;

    for (ii = 0; ii < max_clients; ii++) {
        logmsgf(LOGMSG_USER, fh,
                "%s task[%s] stack[%s] host[%s] ip[%s] refcnt %d\n", prefix,
                summaries[ii].task, summaries[ii].stack, summaries[ii].host,
                inet_ntoa(summaries[ii].addr), summaries[ii].ref);
        logmsgf(LOGMSG_USER, fh,
                "%s%5d | %7u %7u %7u %7u | %7u %7u %7u %7u %7u %7u %7u | %7u "
                "%7u %7u\n",
                prefix, summaries[ii].node, summaries[ii].finds,
                summaries[ii].rngexts, summaries[ii].writes,
                summaries[ii].other_fstsnds, summaries[ii].adds,
                summaries[ii].upds, summaries[ii].dels, summaries[ii].bsql,
                summaries[ii].recom, summaries[ii].snapisol,
                summaries[ii].serial, summaries[ii].sql_queries,
                summaries[ii].sql_steps, summaries[ii].sql_rows);
    }

    for (ii = 0; ii < max_clients; ii++) {
        if (summaries[ii].task)
            free(summaries[ii].task);
        if (summaries[ii].stack)
            free(summaries[ii].stack);
    }
    free(summaries);
}

void reqlog_set_origin(struct reqlogger *logger, const char *fmt, ...)
{
    va_list args;

    va_start(args, fmt);
    vsnprintf(logger->origin, sizeof(logger->origin) - 1, fmt, args);
    va_end(args);
    logger->origin[sizeof(logger->origin) - 1] = 0;
}

inline const char *reqlog_get_origin(const struct reqlogger *logger)
{
    return logger->origin;
}

inline void reqlog_set_vreplays(struct reqlogger *logger, int replays)
{
    if (logger) logger->vreplays = replays;
}

inline void reqlog_set_queue_time(struct reqlogger *logger, uint64_t timeus)
{
    if (logger) logger->queuetimeus = timeus;
}

inline uint64_t reqlog_get_queue_time(const struct reqlogger *logger)
{
    return logger->queuetimeus;
}

inline void reqlog_reset_fingerprint(struct reqlogger *logger, size_t n)
{
    if (logger == NULL)
        return;
    size_t min = (FINGERPRINTSZ < n) ? FINGERPRINTSZ : n;
    memset(logger->fingerprint, 0, min);
    logger->have_fingerprint = 1;
}

void reqlog_set_fingerprint(struct reqlogger *logger, const char *fingerprint,
                            size_t n)
{
    if (logger == NULL)
        return;
    size_t min = (FINGERPRINTSZ < n) ? FINGERPRINTSZ : n;
    memcpy(logger->fingerprint, fingerprint, min);
    logger->have_fingerprint = 1;
}

inline void reqlog_set_event(struct reqlogger *logger, evtype_t ev)
{
    logger->event_type = ev;
}

inline evtype_t reqlog_get_event(struct reqlogger *logger)
{
    return logger->event_type;
}

void reqlog_add_table(struct reqlogger *logger, const char *table)
{
    if (logger->ntables == logger->alloctables) {
        logger->alloctables = logger->alloctables * 2 + 10;
        logger->sqltables =
            realloc(logger->sqltables, logger->alloctables * sizeof(char *));
    }
    logger->sqltables[logger->ntables++] = strdup(table);
}

inline void reqlog_set_error(struct reqlogger *logger, const char *error,
                             int error_code)
{
    free(logger->error);
    logger->error = strdup(error);
    logger->error_code = error_code;
}

inline int reqlog_get_error_code(const struct reqlogger *logger)
{
    return logger->error_code;
}

inline void reqlog_set_path(struct reqlogger *logger,
                            struct client_query_stats *path)
{
    logger->path = path;
}

inline void reqlog_set_context(struct reqlogger *logger, int ncontext,
                               char **context)
{
    logger->ncontext = ncontext;
    logger->context = context;
}

inline void reqlog_set_clnt(struct reqlogger *logger, struct sqlclntstate *clnt)
{
    logger->clnt = clnt;
}

inline int reqlog_get_retries(const struct reqlogger *logger)
{
    return logger->iq ? logger->iq->retries : 0;
}

inline void reqlog_set_nwrites(struct reqlogger *logger, int nwrites, int cascaded_nwrites)
{
    logger->nwrites = nwrites;
    logger->cascaded_nwrites = cascaded_nwrites;
}

struct dump_client_sql_options  {
    struct reqlogger *logger;
    int do_snap;
    nodestats_t *st;
    char fingerprint[FINGERPRINTSZ*2+1];
};

static int print_client_fingerprint(struct reqlogger *logger, char *host, char *task, char *fp, int64_t count, int64_t cost, int64_t time, int64_t rows) {
    reqlog_logf(logger, REQL_INFO, "host=%s task=%s fp=%s count=%"PRId64" cost=%"PRId64" time=%"PRId64 " rows=%"PRId64"\n", host, task, fp, count, cost, time, rows);
    return 0;
}

static int dump_client_fingerprint(void *ent, void *arg) {
    struct dump_client_sql_options *options = (struct dump_client_sql_options*) arg;
    struct query_count *cnt = (struct query_count*) ent;
    char fingerprint[FINGERPRINTSZ*2+1];

    util_tohex(fingerprint, cnt->fingerprint, FINGERPRINTSZ);
    if (options->do_snap) {
        if (cnt->count != cnt->last_count || cnt->cost != cnt->last_cost || cnt->timems != cnt->last_timems || cnt->rows != cnt->last_rows) {
            print_client_fingerprint(options->logger, options->st->host, options->st->task, fingerprint,
                        cnt->count - cnt->last_count,
                        cnt->cost - cnt->last_cost,
                        cnt->timems - cnt->last_timems,
                        cnt->rows - cnt->last_rows);
            cnt->last_count = cnt->count;
            cnt->last_cost = cnt->cost;
            cnt->last_rows = cnt->rows;
            cnt->last_timems = cnt->timems;
        }
    }
    else {
        print_client_fingerprint(options->logger, options->st->host, options->st->task, fingerprint, cnt->count, cnt->cost, cnt->timems, cnt->rows);
    }
    return 0;
}

static int dump_client_sql_data_single(void *ent, void *arg) {
    nodestats_t *st = (nodestats_t*) ent;
    struct dump_client_sql_options *options = (struct dump_client_sql_options*) arg;
    options->st = st;
    Pthread_mutex_lock(&st->rawtotals.lk);
    if (st->rawtotals.fingerprints)
        hash_for(st->rawtotals.fingerprints, dump_client_fingerprint, arg);
    Pthread_mutex_unlock(&st->rawtotals.lk);
    return 0;
}

void dump_client_sql_data(struct reqlogger *logger, int do_snapshot) {
    struct dump_client_sql_options options = { .do_snap = do_snapshot, .logger = logger };
    Pthread_rwlock_wrlock(&clientstats_lk);
    if (clientstats)
        hash_for(clientstats, dump_client_sql_data_single, &options);
    Pthread_rwlock_unlock(&clientstats_lk);
}

void add_fingerprint_to_rawstats(struct rawnodestats *stats, unsigned char *fingerprint, int cost, int rows, int timems) {
    Pthread_mutex_lock(&stats->lk);
    if (stats->fingerprints == NULL) {
        // TODO: where does this get destroyed?
        stats->fingerprints = hash_init_o(offsetof(struct query_count, fingerprint), FINGERPRINTSZ);
        if (stats->fingerprints == NULL)
            abort();
    }
    struct query_count *ct = hash_find(stats->fingerprints, fingerprint);
    if (ct == NULL) {
        ct = calloc(1, sizeof(struct query_count));
        memcpy(ct->fingerprint, fingerprint, FINGERPRINTSZ);
        hash_add(stats->fingerprints, ct);
    }
    ct->count++;
    ct->rows += rows;
    ct->cost += cost;
    ct->timems += timems;
    Pthread_mutex_unlock(&stats->lk);
}

struct client_sql_systable_options {
    struct client_sql_systable_data *stats;
    int nstats;
    int nalloced;

    nodestats_t *st; 
};


int gather_client_sql_data_fingerprint(void *ent, void *arg) {
    struct client_sql_systable_options *opt = (struct client_sql_systable_options*) arg;
    struct query_count *cnt = (struct query_count*) ent;

    if (opt->nstats == opt->nalloced) {
        opt->nalloced = opt->nalloced * 2 + 16;
        void *p = realloc(opt->stats, sizeof(struct client_sql_systable_data) * opt->nalloced);
        if (p == NULL)
            return 1;
        opt->stats = p;
    }
    opt->stats[opt->nstats].host = opt->st->host;
    opt->stats[opt->nstats].task = strdup(opt->st->task);
    util_tohex(opt->stats[opt->nstats].fp, cnt->fingerprint, FINGERPRINTSZ);
    opt->stats[opt->nstats].count = cnt->count;
    opt->stats[opt->nstats].cost = cnt->cost;
    opt->stats[opt->nstats].rows = cnt->rows;
    opt->stats[opt->nstats].timems = cnt->timems;
    opt->nstats++;
    return 0;
}

int gather_client_sql_data_single(void *ent, void *arg) {
    struct client_sql_systable_options *opt = (struct client_sql_systable_options*) arg;
    nodestats_t *st = (nodestats_t*) ent;
    opt->st = st;
    Pthread_mutex_lock(&st->rawtotals.lk);
    int rc = 0;
    if (st->rawtotals.fingerprints)
        rc = hash_for(st->rawtotals.fingerprints, gather_client_sql_data_fingerprint, arg);
    Pthread_mutex_unlock(&st->rawtotals.lk);
    return rc;
}

void free_client_sql_data(void *data, int npoints);

int gather_client_sql_data(void **data_out, int *npoints) {
    struct client_sql_systable_options opt = {0};

    *npoints = 0;

    Pthread_rwlock_wrlock(&clientstats_lk);
    int rc = 0;
    if (clientstats)
        rc = hash_for(clientstats, gather_client_sql_data_single, &opt);
    Pthread_rwlock_unlock(&clientstats_lk);
    if (rc)
        free_client_sql_data(opt.stats, opt.nstats);
    else {
        *npoints = opt.nstats;
        *data_out = opt.stats;
        for (int i = 0; i < opt.nstats; i++)
            opt.stats[i].fingerprint = opt.stats[i].fp;
    }

    return rc;
}

void free_client_sql_data(void *data, int npoints) {
    struct client_sql_systable_data *stats = (struct client_sql_systable_data*) data;
    for (int i = 0; i < npoints; i++) {
        free(stats[i].task);
    }
    free(stats);
}
