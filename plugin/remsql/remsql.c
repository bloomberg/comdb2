/*
   Copyright 2018 Bloomberg Finance L.P.

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

#include "comdb2_plugin.h"
#include "comdb2.h"
#include "comdb2_appsock.h"
typedef struct VdbeSorter VdbeSorter;
#include "fdb_comm.h"
#include "gettimeofday_ms.h"

extern int gbl_fdb_track_times;
extern int gbl_fdb_track;
extern int gbl_notimeouts;
extern int gbl_expressions_indexes;
extern fdb_svc_callback_t callbacks[];

/* Forward declarations */
int fdb_msg_read_message(SBUF2 *sb, fdb_msg_t *msg, enum recv_flags flags);
int fdb_svc_sql_row(SBUF2 *sb, char *cid, char *row, int rowlen, int rc,
                    int isuuid);
int fdb_svc_cursor_close(char *cid, int isuuid, struct sqlclntstate **clnt);
int fdb_svc_trans_rollback(char *tid, enum transaction_level lvl,
                           struct sqlclntstate *clnt, int seq);

static int _check_code_release(SBUF2 *sb, char *cid, int code_release,
                               int isuuid)
{
    char errstr[256];
    int errval;
    int rc;

    code_release = fdb_ver_decoded(code_release);

    /* lets make sure we ask for sender to downgrade if its code is too new */
    if (unlikely(code_release > FDB_VER)) {

        snprintf(errstr, sizeof(errstr), "%d protocol %d too high", FDB_VER,
                 code_release);
        errval = FDB_ERR_FDB_VERSION;

        /* we need to send back a rc code */
        rc = fdb_svc_sql_row(sb, cid, errstr, strlen(errstr) + 1, errval,
                             isuuid);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: fdb_send_rc failed rc=%d\n", __func__,
                   rc);
        }

        return -1;
    }

    return 0;
}

static int handle_remsql_session(SBUF2 *sb, struct dbenv *dbenv)
{
    fdb_msg_cursor_open_t open_msg;
    fdb_msg_t msg;
    int rc = 0;
    svc_callback_arg_t arg;
    int flags;

    bzero(&msg, sizeof(msg));

    rc = fdb_msg_read_message(sb, &msg, 0);
    if (rc) {
        logmsg(LOGMSG_ERROR,
               "%s: failed to handle remote cursor request rc=%d\n", __func__,
               rc);
        return rc;
    }

    flags = msg.hd.type & ~FD_MSG_TYPE;
    msg.hd.type &= FD_MSG_TYPE;

    if (msg.hd.type != FDB_MSG_CURSOR_OPEN) {
        logmsg(LOGMSG_ERROR,
               "%s: received wrong packet type=%d, expecting cursor open\n",
               __func__, msg.hd.type);
        return -1;
    }

    memcpy(&open_msg, &msg, sizeof open_msg);

    /* check and protect against newer versions */
    if (_check_code_release(sb, open_msg.cid, open_msg.rootpage,
                            flags & FD_MSG_FLAGS_ISUUID)) {
        logmsg(LOGMSG_ERROR, "PROTOCOL TOO NEW %d, asking to downgrade to %d\n",
               fdb_ver_decoded(open_msg.rootpage), FDB_VER);
        return 0;
    }

    while (1) {

        arg.isuuid = flags & FD_MSG_FLAGS_ISUUID;

        if (gbl_fdb_track) {
            if (arg.isuuid) {
                fdb_msg_print_message_uuid(sb, &msg, "received msg");
            } else
                fdb_msg_print_message(sb, &msg, "received msg");
        }

        rc = callbacks[msg.hd.type](sb, &msg, &arg);

        if (msg.hd.type == FDB_MSG_CURSOR_CLOSE) {
            break;
        }
        if (rc != 0) {
            int rc2;

            rc2 = fdb_svc_cursor_close(open_msg.cid,
                                       flags & FD_MSG_FLAGS_ISUUID, &arg.clnt);
            if (rc2) {
                logmsg(LOGMSG_ERROR, "%s: fdb_svc_cursor_close failed rc=%d\n",
                       __func__, rc2);
            }
            break;
        }

        /*fprintf(stderr, "XYXY %llu calling recv message\n",
         * osql_log_time());*/
        rc = fdb_msg_read_message(sb, &msg, 0);
        if (rc) {
            logmsg(LOGMSG_ERROR,
                   "%s: failed to handle remote cursor request rc=%d\n",
                   __func__, rc);
            break;
        }

        flags = msg.hd.type & ~FD_MSG_TYPE;
        msg.hd.type &= FD_MSG_TYPE;
    }

    fdb_msg_clean_message(&msg);

    return rc;
}

static int handle_remcur_request(comdb2_appsock_arg_t *arg)
{
    struct sbuf2 *sb;
    fdb_msg_cursor_open_t open_msg;
    fdb_msg_t msg;
    int rc = 0;
    svc_callback_arg_t svc_cb_arg = {0};
    int flags;

    sb = arg->sb;

    bzero(&msg, sizeof(msg));

    logmsg(LOGMSG_DEBUG, "Received remcu request\n");

    rc = fdb_msg_read_message(sb, &msg, 0);
    if (rc) {
        logmsg(LOGMSG_ERROR,
               "%s: failed to handle remote cursor request rc=%d\n", __func__,
               rc);
        return rc;
    }

    flags = msg.hd.type & ~FD_MSG_TYPE;
    msg.hd.type &= FD_MSG_TYPE;

    if (msg.hd.type != FDB_MSG_CURSOR_OPEN) {
        logmsg(LOGMSG_ERROR,
               "%s: received wrong packet type=%d, expecting cursor open\n",
               __func__, msg.hd.type);
        return -1;
    }

    memcpy(&open_msg, &msg, sizeof open_msg);

    while (1) {
        if (gbl_fdb_track) {
            fdb_msg_print_message(sb, &msg, "received msg");
        }

        rc = callbacks[msg.hd.type](sb, &msg, &svc_cb_arg);
        if (msg.hd.type == FDB_MSG_CURSOR_CLOSE) {
            break;
        }
        if (rc != 0) {
            int rc2;
            rc2 = fdb_svc_cursor_close(open_msg.cid,
                                       flags & FD_MSG_FLAGS_ISUUID, NULL);
            if (rc2 && !rc)
                rc = rc2;
            break;
        }

        rc = fdb_msg_read_message(sb, &msg, 0);
        if (rc) {
            logmsg(LOGMSG_ERROR,
                   "%s: failed to handle remote cursor request rc=%d\n",
                   __func__, rc);
            break;
        }

        flags = msg.hd.type & ~FD_MSG_TYPE;
        msg.hd.type &= FD_MSG_TYPE;
    }

    return rc;
}

static int handle_remsql_request(comdb2_appsock_arg_t *arg)
{
    struct dbenv *dbenv;
    struct sbuf2 *sb;
    char line[128];
    int rc = FDB_NOERR;
    static uint64_t old = 0ULL;
    uint64_t now;
    uint64_t then;

    dbenv = arg->dbenv;
    sb = arg->sb;

    /* We will rely on socket drop to determine the end of connection. */
    sbuf2settimeout(sb, 0, 0);

    if (gbl_fdb_track_times) {
        now = gettimeofday_ms();
        logmsg(LOGMSG_USER, "RRRRRR start now=%lu\n", now);
    }

    while (1) {
        if (gbl_fdb_track_times) {
            now = gettimeofday_ms();
        }

        rc = handle_remsql_session(sb, dbenv);
        if (gbl_fdb_track)
            logmsg(LOGMSG_USER, "%lu: %s: executed session rc=%d\n",
                   pthread_self(), __func__, rc);

        if (gbl_fdb_track_times) {
            then = gettimeofday_ms();

            if (old == 0ULL) {
                logmsg(LOGMSG_USER, "RRRRRR now=%lu 0 %lu\n", now, then - now);
            } else {
                logmsg(LOGMSG_USER, "RRRRRR now=%lu delta=%lu %lu\n", now,
                       now - old, then - now);
            }
            old = now;
        }

        if (rc == FDB_NOERR) {
            /* we need to read the header again, waiting here */
            rc = sbuf2gets(line, sizeof(line), sb);
            if (rc != strlen("remsql\n")) {
                if (rc != -1)
                    logmsg(LOGMSG_ERROR,
                           "%s: received wrong request! rc=%d: %s\n", __func__,
                           rc, line);
                rc = FDB_NOERR;
                break;
            }
            /* execute next session */
            continue;
        } else {
            break;
        }
    }
    if (gbl_fdb_track)
        logmsg(LOGMSG_USER, "%lu: %s: done processing\n", pthread_self(),
               __func__);

    return rc;
}

static int handle_remtran_request(comdb2_appsock_arg_t *arg)
{
    struct sbuf2 *sb;
    fdb_msg_tran_t open_msg;
    fdb_msg_t msg;
    int rc = 0;
    svc_callback_arg_t svc_cb_arg = {0};

    sb = arg->sb;

    bzero(&msg, sizeof(msg));

    /* This does insert on behalf of an sql transaction */
    svc_cb_arg.thd = start_sql_thread();

    rc = fdb_msg_read_message(sb, &msg, 0);
    if (rc) {
        logmsg(LOGMSG_ERROR,
               "%s: failed to handle remote cursor request rc=%d\n", __func__,
               rc);
        return rc;
    }

    if ((msg.hd.type & FD_MSG_TYPE) != FDB_MSG_TRAN_BEGIN) {
        logmsg(LOGMSG_ERROR,
               "%s: received wrong packet type=%d, expecting tran begin\n",
               __func__, msg.hd.type);
        return -1;
    }

    memcpy(&open_msg, &msg, sizeof open_msg);
    open_msg.tid = open_msg.tiduuid;
    uuidstr_t us;
    comdb2uuidstr(open_msg.tid, us);

    /* TODO: review the no-timeout transaction later on */
    if (gbl_notimeouts) {
        sbuf2settimeout(sb, 0, 0);
        /*   net_add_watch(sb, 0, 0); */
    }

    while (1) {
        if (gbl_fdb_track) {
            fdb_msg_print_message(sb, &msg, "received msg");
        }

        svc_cb_arg.isuuid = (msg.hd.type & FD_MSG_FLAGS_ISUUID);

        rc = callbacks[msg.hd.type & FD_MSG_TYPE](sb, &msg, &svc_cb_arg);

        if ((msg.hd.type & FD_MSG_TYPE) == FDB_MSG_TRAN_COMMIT ||
            (msg.hd.type & FD_MSG_TYPE) == FDB_MSG_TRAN_ROLLBACK ||
            (msg.hd.type & FD_MSG_TYPE) ==
                FDB_MSG_TRAN_RC /* this should be actuall the only case,
                  since we reuse the buffer to send back results */
        ) {
            if ((msg.hd.type & FD_MSG_TYPE) == FDB_MSG_TRAN_COMMIT ||
                (msg.hd.type & FD_MSG_TYPE) == FDB_MSG_TRAN_ROLLBACK)
                abort();

            break;
        }
        if (rc != 0) {
            int rc2;
        clear:
            rc2 = fdb_svc_trans_rollback(
                open_msg.tid, open_msg.lvl, svc_cb_arg.clnt,
                svc_cb_arg.clnt->dbtran.dtran->fdb_trans.top->seq);
            if (rc2) {
                logmsg(LOGMSG_ERROR,
                       "%s: fdb_svc_trans_rollback failed rc=%d\n", __func__,
                       rc2);
            }
            break;
        }

        /*fprintf(stderr, "XYXY %llu calling recv message\n",
         * osql_log_time());*/
        rc = fdb_msg_read_message(sb, &msg, svc_cb_arg.flags);
        if (rc) {
            logmsg(LOGMSG_ERROR,
                   "%s: failed to handle remote cursor request rc=%d\n",
                   __func__, rc);
            goto clear;
        }
    }

    if (gbl_expressions_indexes) {
        free(svc_cb_arg.clnt->idxInsert);
        free(svc_cb_arg.clnt->idxDelete);
        svc_cb_arg.clnt->idxInsert = svc_cb_arg.clnt->idxDelete = NULL;
    }

    reset_clnt(svc_cb_arg.clnt, NULL, 0);

    done_sql_thread();

    pthread_mutex_destroy(&svc_cb_arg.clnt->wait_mutex);
    pthread_cond_destroy(&svc_cb_arg.clnt->wait_cond);
    pthread_mutex_destroy(&svc_cb_arg.clnt->write_lock);
    pthread_mutex_destroy(&svc_cb_arg.clnt->dtran_mtx);

    free(svc_cb_arg.clnt);
    svc_cb_arg.clnt = NULL;

    return rc;
}

static int _is_tablename_unique(const char *name)
{
    int i;
    int llen = strlen(name);

    for (i = 0; i < thedb->num_dbs; i++) {
        if (llen != strlen(thedb->dbs[i]->tablename))
            continue;
        if (strncasecmp(thedb->dbs[i]->tablename, name, llen) == 0)
            return -1;
    }

    return 0;
}

static int handle_alias_request(comdb2_appsock_arg_t *arg)
{
    struct dbenv *dbenv;
    struct sbuf2 *sb;
    char *op = NULL;
    char *aliasname = NULL;
    char *url = NULL;
    char *errstr = NULL;
    char *tok;
    char *line;
    int llen;
    int ltok = 0;
    int st = 0;
    int rc = -1;

    dbenv = arg->dbenv;
    sb = arg->sb;
    line = arg->cmdline;
    llen = strlen(line);

    if (dbenv->master != gbl_mynode) {
        sbuf2printf(sb, "!master swinged, now on %s, please rerun\n",
                    thedb->master);
        sbuf2printf(sb, "FAILED\n");
        return APPSOCK_RETURN_CONT;
    }

    tok = segtok(line, llen, &st, &ltok);
    assert((strncmp(tok, "alias", ltok) == 0));

    tok = segtok(line, llen, &st, &ltok);
    if (ltok == 0) {
    usage:
        sbuf2printf(sb, "!Usage: [alias set ALIASNAME URL|alias get ALIASNAME| "
                        "alias rem ALIASNAME]\n");
        sbuf2printf(sb, "FAILED\n");

        if (op)
            free(op);
        if (aliasname)
            free(aliasname);
        if (url)
            free(url);

        arg->error = FDB_ERR_GENERIC;
        return APPSOCK_RETURN_ERR;
    }

    op = tokdup(tok, ltok);
    if (!op) {

        arg->error = FDB_ERR_MALLOC;
        return APPSOCK_RETURN_ERR;
    }

    tok = segtok(line, llen, &st, &ltok);
    if (ltok == 0)
        goto usage;
    aliasname = tokdup(tok, ltok);

    rc = _is_tablename_unique(aliasname);
    if (rc) {
        char msg[256];
        snprintf(msg, sizeof(msg),
                 "alias \"%s\" exists as table name, must be unique",
                 aliasname);
        sbuf2printf(sb, "!%s\n", msg);
        sbuf2printf(sb, "FAILED\n");
        arg->error = FDB_ERR_GENERIC;
        return APPSOCK_RETURN_ERR;
    }

    if (strncasecmp(op, "set", 3) == 0) {
        tok = segtok(line, llen, &st, &ltok);
        if (ltok == 0)
            goto usage;
        url = tokdup(tok, ltok);

        rc = llmeta_set_tablename_alias(NULL, aliasname, url, &errstr);
    } else if (strncasecmp(op, "get", 3) == 0) {
        url = llmeta_get_tablename_alias(aliasname, &errstr);
        rc = (url == 0);

        if (rc == 0) {
            sbuf2printf(sb, ">%s\n", url);
        }
    } else if (strncasecmp(op, "rem", 3) == 0) {
        rc = llmeta_rem_tablename_alias(aliasname, &errstr);
    } else {
        goto usage;
    }

    if (op)
        free(op);
    if (aliasname)
        free(aliasname);
    if (url)
        free(url);

    if (rc) {
        sbuf2printf(sb, "!%s\n", (errstr) ? errstr : "no string");
        sbuf2printf(sb, "FAILED\n");

        if (errstr) {
            free(errstr);
        }
    } else {
        sbuf2printf(sb, "SUCCESS\n");
    }

    arg->error = rc;
    return (rc) ? APPSOCK_RETURN_ERR : APPSOCK_RETURN_OK;
}

comdb2_appsock_t remcur_plugin = {
    "remcur",             /* Name */
    "",                   /* Usage info */
    0,                    /* Execution count */
    0,                    /* Flags */
    handle_remcur_request /* Handler function */
};

comdb2_appsock_t remsql_plugin = {
    "remsql",             /* Name */
    "",                   /* Usage info */
    0,                    /* Execution count */
    0,                    /* Flags */
    handle_remsql_request /* Handler function */
};

comdb2_appsock_t remtran_plugin = {
    "remtran",             /* Name */
    "",                    /* Usage info */
    0,                     /* Execution count */
    0,                     /* Flags */
    handle_remtran_request /* Handler function */
};

comdb2_appsock_t alias_plugin = {
    "alias",             /* Name */
    "",                  /* Usage info */
    0,                   /* Execution count */
    0,                   /* Flags */
    handle_alias_request /* Handler function */
};

#include "plugin.h"
