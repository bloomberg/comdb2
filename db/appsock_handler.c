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

/*
 * Application socket handler,
 * For dump/reload and alternate interfaces
 *
 * We now use a thread pool for appsock.  The threads have a small (64KB)
 * stack by default allowing us to maintain lots of connections.  Be aware
 * of this when doing appsock stuff!
 */
#include <pthread.h>

#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "limit_fortify.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <pthread.h>
#include <time.h>

#include <segstr.h>
#include <machpthread.h>
#include <plink.h>
#include <tcputil.h>
#include <lockmacro.h>

#include <memory_sync.h>

#include <sbuf2.h>
#include <bdb_api.h>

#include "util.h"
#include "socket_interfaces.h"
#include "comdb2.h"
#include "thdpool.h"
#include "analyze.h"

#include <alloca.h>

#include "fdb_comm.h"

#include <bdb_api.h>
#include <bdb_schemachange.h>
#include <logmsg.h>

void delete_null_entries(void);

extern struct dbenv *thedb; /*handles 1 db for now*/
extern int gbl_use_appsock_as_sqlthread;

/* A crude usage tracking thingy */
struct appsock_cmd {
    const char *cmd;
    unsigned long long num_uses;
};

struct appsock_thd_state {
    struct thr_handle *thr_self;
};

#ifdef DEBUG
// was crashing because of the small stack size when debug was on
#define GBL_APPSOCK_THDPOOL_STCKSZ 512 * 1024
#else
#define GBL_APPSOCK_THDPOOL_STCKSZ 192 * 1024
#endif

struct thdpool *gbl_appsock_thdpool = NULL;

char appsock_unknown_old[] = "-1 #unknown command\n";
char appsock_unknown[] = "Error: -1 #unknown command\n";
char appsock_supported[] = "supported\n";

static unsigned long long total_appsock_conns = 0;
static unsigned long long num_bad_toks = 0;
static unsigned long long total_toks = 0;
static unsigned long long total_appsock_rejections = 0;

int active_appsock_conns = 0;
static pthread_mutex_t appsock_conn_lk = PTHREAD_MUTEX_INITIALIZER;

static struct appsock_cmd commands[100];
static size_t num_commands = 0;

static const struct appsock_cmd *cmd_fastsql;
static const struct appsock_cmd *cmd_newsql;
static const struct appsock_cmd *cmd_version;
static const struct appsock_cmd *cmd_help;
static const struct appsock_cmd *cmd_explain;
static const struct appsock_cmd *cmd_schemachange;
static const struct appsock_cmd *cmd_repopnewlrl;
static const struct appsock_cmd *cmd_whomaster;
static const struct appsock_cmd *cmd_whomasterhost;
static const struct appsock_cmd *cmd_logdelete;
static const struct appsock_cmd *cmd_logdelete2;
static const struct appsock_cmd *cmd_logdelete3;
static const struct appsock_cmd *cmd_disableskipscan;
static const struct appsock_cmd *cmd_testcompr;
static const struct appsock_cmd *cmd_remcur;
static const struct appsock_cmd *cmd_remsql;
static const struct appsock_cmd *cmd_remtran;
static const struct appsock_cmd *cmd_alias;
static const struct appsock_cmd *cmd_partition;
static const struct appsock_cmd *cmd_genid48;

static void appsock_thd_start(struct thdpool *pool, void *thddata);
static void appsock_thd_end(struct thdpool *pool, void *thddata);

void close_appsock(SBUF2 *sb)
{
    net_end_appsock(sb);
    LOCK(&appsock_conn_lk) { active_appsock_conns--; }
    UNLOCK(&appsock_conn_lk);
}

static const struct appsock_cmd *add_command(const char *cmd)
{
    if (num_commands == sizeof(commands) / sizeof(commands[0])) {
        logmsg(LOGMSG_FATAL, "appsock.c: please make the commands array bigger!\n");
        exit(1);
    }

    bzero(&commands[num_commands], sizeof(struct appsock_cmd));
    commands[num_commands].cmd = cmd;

    return &commands[num_commands++];
}

int appsock_init(void)
{
    int rc;

    /* setup our commands table so we can track usage */
    cmd_schemachange = add_command("schemachange");
    cmd_repopnewlrl = add_command("repopnewlrl");
    cmd_explain = add_command("explain");
    cmd_fastsql = add_command("fastsql");
    cmd_newsql = add_command("newsql");
    cmd_version = add_command("version");
    cmd_help = add_command("help");
    cmd_whomaster = add_command("whomaster");
    cmd_whomasterhost = add_command("whomasterhost");
    cmd_logdelete = add_command("logdelete");
    cmd_logdelete2 = add_command("logdelete2");
    cmd_logdelete3 = add_command("logdelete3");
    cmd_disableskipscan = add_command("disableskipscan");
    cmd_testcompr = add_command("testcompr");
    cmd_remcur = add_command("remcur");
    cmd_remsql = add_command("remsql");
    cmd_remtran = add_command("remtran");
    cmd_alias = add_command("alias");
    cmd_partition = add_command("partition");
    cmd_genid48 = add_command("genid48");

    gbl_appsock_thdpool =
        thdpool_create("appsock pool", sizeof(struct appsock_thd_state));

    if (gbl_exit_on_pthread_create_fail)
        thdpool_set_exit(gbl_appsock_thdpool);

    /* Nice small stack so we can handle lots of connections */
    thdpool_set_stack_size(gbl_appsock_thdpool, GBL_APPSOCK_THDPOOL_STCKSZ);
    thdpool_set_init_fn(gbl_appsock_thdpool, appsock_thd_start);
    thdpool_set_delt_fn(gbl_appsock_thdpool, appsock_thd_end);
    thdpool_set_minthds(gbl_appsock_thdpool, 1);
    thdpool_set_linger(gbl_appsock_thdpool, 10);

    thdpool_set_mem_size(gbl_appsock_thdpool, 4 * 1024);

    return 0;
}

void appsock_quick_stat(void)
{
    logmsg(LOGMSG_USER, "num appsock connections %llu\n", total_appsock_conns);
    logmsg(LOGMSG_USER, "num active appsock connections %d\n", active_appsock_conns);
    logmsg(LOGMSG_USER, "num appsock commands    %llu\n", total_toks);
}

void appsock_stat(void)
{
    int ii;
    appsock_quick_stat();
    logmsg(LOGMSG_USER, "bad appsock commands    %llu\n", num_bad_toks);
    logmsg(LOGMSG_USER, "rejected appsock conns  %llu\n", total_appsock_rejections);
    for (ii = 0; ii < num_commands; ii++) {
        if (commands[ii].num_uses) {
            logmsg(LOGMSG_USER, "  num %-16s  %llu\n", commands[ii].cmd,
                   commands[ii].num_uses);
        }
    }
}

void appsock_get_dbinfo2_stats(uint32_t *n_appsock, uint32_t *n_sql)
{
    *n_appsock = total_appsock_conns;
    *n_sql = cmd_fastsql->num_uses;
}

static struct appsock_cmd *tok2command(char *tok, int ltok)
{
    int ii;
    for (ii = 0; ii < num_commands; ii++)
        if (tokcmp(tok, ltok, (char *)commands[ii].cmd) == 0)
            return &commands[ii];
    return NULL;
}

static void dumprrns(struct dbtable *tbl, SBUF2 *sb)
{
    char key[MAXKEYLEN];
    char fndkey[MAXKEYLEN];
    char fnddta[MAXLRL];
    int fndrrn, fndlen, ii, rc, rrn;
    static char *hexchars = "0123456789ABCDEF";
    unsigned char u;
    struct ireq liq = {0};
    unsigned long long genid;

    memset(key, 0, sizeof(key));
    liq.usedb = tbl;
    rc = ix_find(&liq, 0, key, 1, fndkey, &fndrrn, &genid, fnddta, &fndlen,
                 sizeof(fnddta));
    while (rc >= 0 && rc <= 2) /*got a key*/
    {
        sbuf2printf(sb, "%d rrn %d len %d ", rc, fndrrn, fndlen);
        for (ii = 0; ii < fndlen; ii++) {
            u = fnddta[ii];
            sbuf2putc(sb, hexchars[(u >> 4) & 0xf]);
            sbuf2putc(sb, hexchars[u & 0xf]);
        }
        sbuf2putc(sb, '\n');
        memcpy(key, fndkey, MAXKEYLEN);
        rrn = fndrrn;
        rc = ix_next(&liq, 0, key, 1, key, rrn, genid, fndkey, &fndrrn, &genid,
                     fnddta, &fndlen, sizeof(fnddta), 0);
    }
    sbuf2printf(sb, "%d #done\n", rc);
}

struct loadrrn_cmd {
    int op;
    int parm;
};

enum { LOAD_ADD_RECORD, LOAD_GET_STATUS };
static int loadrrns(struct dbtable *tbl, SBUF2 *sb, char *tag)
{
    int len;
    char *buf;
    int rc, rrc;
    int recno = 0;
    int op;
    int status;
    char *dta;
    unsigned char nullbits[MAXNULLBITS] = {0};

    len = get_size_of_schema_by_name(tbl->dbname, tag);
    if (len == 0)
        return -1;
    buf = malloc(len);
    dta = malloc(getdatsize(tbl));

    do {
        rrc = sbuf2fread((char *)&op, sizeof(int), 1, sb);
        if (rrc != 1) {
            rc = -1;
            break;
        }

        switch (op) {
        case LOAD_ADD_RECORD:
            rrc = sbuf2fread(buf, len, 1, sb);
            if (rrc != 1) {
                rc = -1;
                break;
            }

            rc = ctag_to_stag_buf(tbl->dbname, tag, buf, len, nullbits,
                                  ".ONDISK", dta, 0, NULL);
            if (rc != -1) {
                rc = load_record(tbl, buf);
                recno++;
            }
            break;

        case LOAD_GET_STATUS:
            rrc = sbuf2fwrite((char *)&rc, sizeof(int), 1, sb);
            if (rrc != 1) {
                rc = -1;
                break;
            }
            break;
        }
    } while (rrc == 1);

    free(buf);
    return rc;
}

/* callback for converting records to given tag */
static int fstdump_callback(void *rec, size_t reclen, void *clientrec,
                            size_t clientreclen, struct dbtable *tbl, const char *tag,
                            const char *tzname, uint8_t ver, int conv_flags)
{
    unsigned char nulls[MAXNULLBITS];
    int rc = 0;
    if (tbl->dbtype == DBTYPE_TAGGED_TABLE) {
        int len = reclen;
        if (ver < tbl->version) {
            void *newrec = alloca(tbl->lrl);
            memcpy(newrec, rec, reclen);
            rec = newrec;
        }
        vtag_to_ondisk(tbl, rec, &len, ver, 0);
        rc =
            stag_to_ctag_buf_tz(tbl->dbname, ".ONDISK", rec, len, tag, clientrec,
                                nulls, conv_flags, NULL, NULL, tzname);
    } else {
        memcpy(clientrec, rec, clientreclen);
    }
    if (rc < 0)
        return -1;
    else {
        /* reverse psychology?  calling code used to treat -1 as good,
           everything
           else as a failure */
        return 0;
    }
}

extern void verify_table(char *table, SBUF2 *sb, int progress_report_seconds,
                    int attempt_fix);

struct fstdmp_t {
    int rc;
    bdb_state_type *bdb_state;
    SBUF2 *sb;
    size_t sendrecsz;
    bdb_fstdumpdta_callback_t convert_callback;
    int callback_flags;
    void *userptr;
    void *userptr2;
    int timeoutms;
    int safe_mode;
    int *bdberr;
    const char *tzname;
    int get_genids;
};

static void *fstdump_hndlr(void *arg_)
{
    struct fstdmp_t *arg = arg_;
    backend_thread_event(thedb, COMDB2_THR_EVENT_START_RDONLY);
    arg->rc = bdb_fstdumpdta_sendsz(
        arg->bdb_state, arg->sb, arg->sendrecsz, arg->convert_callback,
        arg->callback_flags, arg->userptr, arg->userptr2, arg->timeoutms,
        arg->safe_mode, arg->bdberr, arg->tzname, arg->get_genids);
    backend_thread_event(thedb, COMDB2_THR_EVENT_DONE_RDONLY);
    return NULL;
}

static void *thd_appsock_int(SBUF2 *sb, int *keepsocket,
                             struct thr_handle *thr_self)
{
    int rc, ltok, st;
    char line[128] = {0};
    char *tok;
    struct dbtable *usedb, *tbl;
    int bdberr, conv_flags = 0;
    struct dbenv *dbenv;
    *keepsocket = 0;
    sbuf2settimeout(sb, IOTIMEOUTMS, IOTIMEOUTMS);
    usedb = thedb->dbs[0];
    dbenv = usedb->dbenv;
    int lline;
    int wrong_db = 0;

    while (1) {
        struct appsock_cmd *cmd;

        thrman_where(thr_self, NULL);

        rc = sbuf2gets(line, sizeof(line), sb);
        lline = rc;
        if (rc <= 0)
            break;
        st = 0;

#ifdef DEBUG
        printf("line '%s'\n", line);
#endif

        if (!strncmp(line, cmd_fastsql->cmd, 7)) {
            cmd = (struct appsock_cmd *)cmd_fastsql;
            if (line[7] != '\n') {
                int envlen = strlen(thedb->envname);
                if ((line[8 + envlen] != '\n') ||
                    strncmp(&line[8], thedb->envname, envlen)) {
                    /* We can't just send an error down to the client because
                     * they aren't
                     * expecting to read a response.  Remember that the client
                     * has a wrong
                     * connection and return the error later, when they expect
                     * one. */
                    if (gbl_check_wrong_db)
                        wrong_db = 1;
                }
            }
        } else {
            tok = segtok(line, rc, &st, &ltok);
            if (ltok == 0)
                continue;
            if (tok[0] == '#')
                continue;

            cmd = tok2command(tok, ltok);
            /* track usage */
        }

        if (cmd) {
            cmd->num_uses++;
            thrman_where(thr_self, cmd->cmd);
        } else
            num_bad_toks++;
        total_toks++;

#if 0
      if (tokcmp(tok,ltok,"dumprrns")==0)
      {
         dumprrns(usedb, sb);
      }
#endif
        if (cmd == cmd_fastsql) {
            if (usedb->dbtype != DBTYPE_TAGGED_TABLE) {
                /* Don't change this message.  The sql api recognises the first
                 * four characters (Erro) and can respond gracefully. */
                sbuf2printf(
                    sb, "Error: fastsql is only supported for tagged DBs\n");
                logmsg(LOGMSG_ERROR, 
                    "Error: fastsql is only supported for tagged DBs\n");
                sbuf2flush(sb);
                break;
            }

            if (!gbl_use_appsock_as_sqlthread &&
                !bdb_am_i_coherent(thedb->bdb_env))
                break;

            /* there are points when we can't accept any more connections. */
            if (dbenv->no_more_sql_connections) {
                break;
            }

            /* if we are NOT the master, and the db is set up for async
               replication,
               we should return an error at this point rather than proceed with
               potentially incoherent data. */
            if (thedb->rep_sync == REP_SYNC_NONE && thedb->master != gbl_mynode)
                break;

            /* New way.  Do the basic socket I/O in line in this thread
             * (which has a very small stack); the handle_fastsql_requests
             * function will dispatch to a pooled sql engine for performing
             * queries. */
            thrman_change_type(thr_self, THRTYPE_APPSOCK_SQL);
            handle_fastsql_requests(thr_self, sb, keepsocket, wrong_db);

            break;
        } else if (cmd == cmd_newsql) {
            if (usedb->dbtype != DBTYPE_TAGGED_TABLE) {
                /* Don't change this message.  The sql api recognises the first
                 * four characters (Erro) and can respond gracefully. */
                sbuf2printf(sb,
                            "Error: newsql is only supported for tagged DBs\n");
                logmsg(LOGMSG_ERROR, 
                       "Error: newsql is only supported for tagged DBs\n");
                sbuf2flush(sb);
                break;
            }

            if (!bdb_am_i_coherent(thedb->bdb_env))
                break;

            /* there are points when we can't accept any more connections. */
            if (dbenv->no_more_sql_connections) {
                break;
            }

            /* if we are NOT the master, and the db is set up for async
               replication,
               we should return an error at this point rather than proceed with
               potentially incoherent data. */
            if (thedb->rep_sync == REP_SYNC_NONE && thedb->master != gbl_mynode)
                break;

            /* New way.  Do the basic socket I/O in line in this thread
             * (which has a very small stack); the handle_fastsql_requests
             * function will dispatch to a pooled sql engine for performing
             * queries. */
            thrman_change_type(thr_self, THRTYPE_APPSOCK_SQL);
            handle_newsql_requests(thr_self, sb, keepsocket);

            break;
        } else if (cmd == cmd_remcur) {
            rc = handle_remcur(sb, dbenv);
            *keepsocket = 0;
            break;
        } else if (cmd == cmd_remsql) {
            rc = handle_remsql(sb, dbenv);
            *keepsocket = 0;
            break;
        } else if (cmd == cmd_remtran) {
            rc = handle_remtran(sb, dbenv);
            *keepsocket = 0;
            break;
        } else if (cmd == cmd_alias) {
            char *errstr = NULL;

            *keepsocket = 0;

            if (thedb->master != gbl_mynode) {
                sbuf2printf(sb, "!master swinged, now on %s, please rerun\n",
                            thedb->master);
                sbuf2printf(sb, "FAILED\n");
            } else {
                rc = fdb_alias_command(line + strlen("alias "), &errstr, sb);
                if (rc) {
                    sbuf2printf(sb, "!%s\n", (errstr) ? errstr : "no string");
                    sbuf2printf(sb, "FAILED\n");

                    if (errstr) {
                        free(errstr);
                    }
                } else {
                    sbuf2printf(sb, "SUCCESS\n");
                }
            }
            sbuf2flush(sb);
        } 
        else if (cmd == cmd_repopnewlrl) {
            /* TODO check rc? */
            appsock_repopnewlrl(sb, keepsocket);
            return 0;
        } else if (cmd == cmd_version) {
            sbuf2printf(sb, "0 %s\n", plink_constant(PLINK_TIME));
            sbuf2flush(sb);
        } else if (cmd == cmd_explain) {
            int trace, all;
            trace = all = 0;
            while (tok = segtok(line, rc, &st, &ltok)) {
                if (ltok == 0) {
                    break;
                } else if (tokcmp(tok, ltok, "-v") == 0) {
                    trace = 1;
                } else if (tokcmp(tok, ltok, "-a") == 0) {
                    all = 1;
                }
            }
            handle_explain(sb, trace, all);
            return 0;
        } else if (cmd == cmd_whomaster) {
            sbuf2printf(sb, "-1 whomaster request not supported\n",
                        thedb->master);
            sbuf2flush(sb);
        } else if (cmd == cmd_whomasterhost) {
            char host[50];
            char *master;
            master = thedb->master;
            if (master == NULL) {
                sbuf2printf(sb, "-1\n");
                sbuf2flush(sb);
            } else {
                sbuf2printf(sb, "%s\n", master);
                sbuf2flush(sb);
            }
        } else if (cmd == cmd_help) {
            sbuf2flush(sb);
        } else if (cmd == cmd_logdelete || cmd == cmd_logdelete2 ||
                   cmd == cmd_logdelete3) {
            int before_count, after_count;
            int before_master, after_master;
            int before_sc, after_sc;
            int report_back = 0;
            struct log_delete_state log_delete_state;
            char recovery_command[200] = {0}, recovery_lsn[100] = {0};

            /* There is no difference between log delete one and two, just that
             * if the db doesn't have log delete two then the comdb2logdel.tsk
             * knows that this is an old binary that won't give feedback. */

            /* Make us a special log deletion holding thread so that we don't
             * hold up bounces/schema changes. */
            thrman_change_type(thr_self, THRTYPE_LOGDELHOLD);

            /* disable log file deletion until this socket gets read from
             * again. */
            log_delete_state.filenum = 0;
            log_delete_add_state(thedb, &log_delete_state);
            log_delete_counter_change(thedb, LOG_DEL_REFRESH);
            backend_update_sync(thedb);
            before_count = bdb_get_low_headroom_count(thedb->bdb_env);
            before_master = gbl_master_changes;
            before_sc = gbl_sc_commit_count;
            logmsg(LOGMSG_INFO, "Disabling log file deletion\n");

            /* respond so that comdb2logdel.tsk knows it got through. */
            sbuf2printf(sb, "log file deletion disabled\n");
            sbuf2flush(sb);

            if (cmd == cmd_logdelete3) {
                rc = bdb_recovery_start_lsn(thedb->bdb_env, recovery_lsn,
                                            sizeof(recovery_lsn));
                if (rc) {
                    logmsg(LOGMSG_ERROR, "bdb_recovery_start_lsn rc %d\n", rc);
                    snprintf(recovery_command, sizeof(recovery_command),
                             "-fullrecovery");
                } else {
                    snprintf(recovery_command, sizeof(recovery_command),
                             "-recovery_lsn %s", recovery_lsn);
                }
            }

            /* read from socket until it closes */
            sbuf2settimeout(sb, 0, 0);
            while (sbuf2gets(line, sizeof(line), sb) > 0) {
                static const char *delims = " \r\t\n";
                char *lasts;
                char *tok;
                tok = strtok_r(line, delims, &lasts);
                if (!tok) {
                    continue;
                } else if (strcmp(tok, "report_back") == 0) {
                    report_back = 1;
                    break;
                } else if (strcmp(tok, "filenum") == 0) {
                    int filenum;
                    tok = strtok_r(NULL, delims, &lasts);
                    errno = 0;
                    if (tok && (filenum = strtol(tok, &lasts, 0)) > 0 &&
                        errno == 0 && lasts && *lasts == '\0') {
                        log_delete_state.filenum = filenum;
                        log_delete_counter_change(thedb, LOG_DEL_REFRESH);
                        backend_update_sync(thedb);
                    } else {
                        logmsg(LOGMSG_ERROR, 
                                "logdelete2 thread got bad filenum <%s>\n",
                                tok);
                        sbuf2printf(sb, "expected +ve filenum\n");
                        sbuf2flush(sb);
                    }
                } else if (strcmp(tok, "recovery_options") == 0) {
                    logmsg(LOGMSG_DEBUG, "sent recovery options: %s\n", recovery_command);
                    sbuf2printf(sb, "%s\n", recovery_command);
                    sbuf2flush(sb);
                } else {
                    logmsg(LOGMSG_ERROR, "logdelete2 thread got unknown token <%s>\n", tok);
                    /* la la la la fingers in my ears */
                }
            }

            logmsg(LOGMSG_INFO, "Reenabling log file deletion\n");
            log_delete_rem_state(thedb, &log_delete_state);
            log_delete_counter_change(thedb, LOG_DEL_REFRESH);
            backend_update_sync(thedb);
            after_count = bdb_get_low_headroom_count(thedb->bdb_env);
            after_master = gbl_master_changes;
            after_sc = gbl_sc_commit_count;

            /* The text we report back here is a binary protocol so don't
             * go changing the wording without checking the logic in
             * comdb2logdel.tsk. */
            if (report_back) {
                /* If we deleted log files during that due to log file deletion
                 * then report so */
                /*
                   if(after_count != before_count) {
                   sbuf2printf(sb, "Alert: log files deleted due to low disk
                   headroom\n");
                   }
                 */
                /* (this test is not reliable) */

                /* If the master node changed during that then report that too
                 */
                if (before_master != after_master) {
                    sbuf2printf(sb, "Alert: master changed during operation\n");
                }

                /* If we committed a schema change then that's ruined it too...
                 */
                if (before_sc != after_sc) {
                    sbuf2printf(
                        sb,
                        "Alert: schema changes committed during operation\n");
                }

                sbuf2printf(sb, ".\n");
                sbuf2flush(sb);
            }
            break;
        } 
        else if (cmd == cmd_genid48) {
            tok = segtok(line, rc, &st, &ltok);
            if (ltok <= 0) {
                sbuf2printf(sb, "?No command specified.\nFAILED\n");
                sbuf2flush(sb);
                continue;
            }
            if (thedb->master != gbl_mynode) {
                sbuf2printf(sb, "?Must be run on the master\nFAILED\n");
                sbuf2flush(sb);
                continue;
            }
            if (ltok && !tokcmp(tok, ltok, "enable")) {
                handle_genid48_enable(sb);
                continue;
            }
            if (ltok && !tokcmp(tok, ltok, "disable")) {
                handle_genid48_disable(sb);
                continue;
            }
            sbuf2printf(sb, "?Invalid genid48 command.\nFAILED\n");
            sbuf2flush(sb);
            continue;
        } else if (cmd == cmd_testcompr) {
            char table[128];
            tok = segtok(line, rc, &st, &ltok);
            tokcpy0(tok, ltok, table, sizeof(table));
            handle_testcompr(sb, table);
            return 0;
        } else if (cmd == cmd_partition) {
            handle_partition(sb);
            return 0;
        } else {
            sbuf2printf(sb, appsock_unknown);
            sbuf2flush(sb);
            continue;
        }
    }
    thrman_where(thr_self, NULL);
    /* printf("done application socket handler sb %d tid
     * %ld\n",sb,pthread_self()); */

    return 0;
}

static void appsock_thd_start(struct thdpool *pool, void *thddata)
{
    struct appsock_thd_state *state = thddata;
    state->thr_self = thrman_register(THRTYPE_APPSOCK_POOL);
    if (!gbl_use_appsock_as_sqlthread)
        backend_thread_event(thedb, COMDB2_THR_EVENT_START_RDWR);
}

static void appsock_thd_end(struct thdpool *pool, void *thddata)
{
    struct appsock_thd_state *state = thddata;
    if (!gbl_use_appsock_as_sqlthread)
        backend_thread_event(thedb, COMDB2_THR_EVENT_DONE_RDWR);
}

static void appsock_work(struct thdpool *pool, void *work, void *thddata)
{
    struct appsock_thd_state *state = thddata;
    SBUF2 *sb = work;
    int keepsocket = 0;

    thrman_setfd(state->thr_self, sbuf2fileno(sb));
    thd_appsock_int(sb, &keepsocket, state->thr_self);
    thrman_setfd(state->thr_self, -1);
    thrman_where(state->thr_self, NULL);
    if (keepsocket == 0)
        close_appsock(sb);

    if (thrman_get_type(state->thr_self) != THRTYPE_APPSOCK_POOL)
        thrman_change_type(state->thr_self, THRTYPE_APPSOCK_POOL);
}

static void appsock_work_pp(struct thdpool *pool, void *work, void *thddata,
                            int op)
{
    SBUF2 *sb = (SBUF2 *)work;

    switch (op) {
    case THD_RUN:
        appsock_work(pool, work, thddata);
        break;

    case THD_FREE:
        close_appsock(sb);
        break;

    default:
        abort();
    }
}

int gbl_appsock_connection_warn_threshold = 80;

void dump_appsock_threads(void)
{
    thdpool_print_stats(stderr, gbl_appsock_thdpool);
    thrman_dump();
}

void appsock_handler_start(struct dbenv *dbenv, SBUF2 *sb)
{
    /*START HANDLER THREAD*/
    int rc;
    pthread_t tid;
    static int last_thread_dump_time = 0;
    static int last_thread_dump_warn_time = 0;
    int nconns = 0;
    int maxconns;
    time_t now;

    now = time(NULL);

    maxconns = thdpool_get_maxthds(gbl_appsock_thdpool);
    nconns = thdpool_get_nthds(gbl_appsock_thdpool);
    if (nconns < maxconns &&
        nconns > (double)maxconns *
                     ((double)gbl_appsock_connection_warn_threshold / 100)) {
        if ((now - last_thread_dump_warn_time) > 10) {
            logmsg(LOGMSG_WARN, "Warning: reached %d%% of max concurrent connections "
                   "(%d/%d):\n",
                   gbl_appsock_connection_warn_threshold, nconns, maxconns);
            last_thread_dump_warn_time = now;
            dump_appsock_threads();
        }
    }

    /* reject requests if we're not up, going down, or not interested */
    if (dbenv->stopped || gbl_exit || !gbl_ready) {
        total_appsock_rejections++;
        net_end_appsock(sb);
        return;
    }

    if (active_appsock_conns >=
        bdb_attr_get(thedb->bdb_attr, BDB_ATTR_APPSOCKSLIMIT)) {
        static int lastprint = 0;
        int now = time_epoch();

        if ((now - lastprint) > 0) {
            int max = bdb_attr_get(thedb->bdb_attr, BDB_ATTR_MAXAPPSOCKSLIMIT);

            logmsg(LOGMSG_WARN, "%s: Maximum appsock connection limit approaching (%d/%d).\n",
                __func__, active_appsock_conns, max);
            lastprint = now;

            if ((now - last_thread_dump_time) > 10) {
                last_thread_dump_time = now;
                dump_appsock_threads();
            }
        }
    }

    if (thdpool_enqueue(gbl_appsock_thdpool, appsock_work_pp, sb, 0, NULL) != 0)
   {
        total_appsock_rejections++;
        if ((now - last_thread_dump_time) > 10) {
            logmsg(LOGMSG_WARN, "Too many concurrent SQL connections:\n");
            last_thread_dump_time = now;
            dump_appsock_threads();
        }

        logmsg(LOGMSG_ERROR, "%s:thdpool_enqueue error\n", __func__);
        net_end_appsock(sb);
        return;
    } 

    total_appsock_conns++;
    LOCK(&appsock_conn_lk) { active_appsock_conns++; }
    UNLOCK(&appsock_conn_lk);
    if (active_appsock_conns >
        bdb_attr_get(thedb->bdb_attr, BDB_ATTR_MAXSOCKCACHED)) {
        logmsg(LOGMSG_WARN, 
                "TOO many active socket connections. current limit %d\n",
                bdb_attr_get(thedb->bdb_attr, BDB_ATTR_MAXSOCKCACHED));
    }
}

static int set_genid48(int enable)
{
    scdone_t llog;
    int rc, bdberr, format;
    if (enable) {
        llog = genid48_enable;
        format = LLMETA_GENID_48BIT;
    } else {
        llog = genid48_disable;
        format = LLMETA_GENID_ORIGINAL;
    }

    if ((rc = bdb_set_genid_format(format, &bdberr)) != 0) {
        logmsg(LOGMSG_FATAL, "Error setting genid format, rc=%d, bdberr=%d\n", rc,
                bdberr);
        abort();
    }

    if ((rc = bdb_llog_genid_format(thedb->bdb_env, llog, &bdberr)) != 0) {
        logmsg(LOGMSG_FATAL, "Error writing genid format log, rc=%d, bdberr=%d\n",
                rc, bdberr);
        abort();
    }

    return 0;
}

int set_rowlocks(void *trans, int enable)
{
    int rc, bdberr, rlstate;
    if (enable) {
        rlstate = LLMETA_ROWLOCKS_ENABLED;
    } else {
        rlstate = LLMETA_ROWLOCKS_DISABLED;
    }

    if ((rc = bdb_set_rowlocks_state(NULL, rlstate, &bdberr)) != 0) {
        logmsg(LOGMSG_ERROR, "Error setting rowlocks state, rc=%d, bdberr=%d\n", rc,
                bdberr);
        return -1;
    }

    if (gbl_rowlocks) {
        gbl_sql_tranlevel_preserved = gbl_sql_tranlevel_default;
        gbl_sql_tranlevel_default = SQL_TDEF_SNAPISOL;
    } else {
        gbl_sql_tranlevel_default = gbl_sql_tranlevel_preserved;
    }

    return 0;
}

void handle_genid48_enable(SBUF2 *sb)
{
    int format = bdb_genid_format(thedb->bdb_env), rc;

    if (format == LLMETA_GENID_48BIT) {
        sbuf2printf(sb, "?Genid48 is already enabled.\nFAILED\n");
        sbuf2flush(sb);
        return;
    }

    rc = set_genid48(LLMETA_GENID_48BIT);
    if (rc == 0)
        sbuf2printf(sb, "SUCCESS\n");
    else
        sbuf2printf(sb, "FAILED\n");

    sbuf2flush(sb);
}

void handle_genid48_disable(SBUF2 *sb)
{
    int format = bdb_genid_format(thedb->bdb_env), rc;

    if (format == LLMETA_GENID_ORIGINAL) {
        sbuf2printf(sb, "?Genid48 is already disabled.\nFAILED\n");
        sbuf2flush(sb);
        return;
    }

    /* Allow if now is greater than maximum genid */
    if (!bdb_genid_allow_original_format(thedb->bdb_env)) {
        sbuf2printf(
            sb, "?Genid48 genid-time is larger than current-time.\nFAILED\n");
        sbuf2flush(sb);
        return;
    }

    rc = set_genid48(LLMETA_GENID_ORIGINAL);
    if (rc == 0)
        sbuf2printf(sb, "SUCCESS\n");
    else
        sbuf2printf(sb, "FAILED\n");

    sbuf2flush(sb);
}


