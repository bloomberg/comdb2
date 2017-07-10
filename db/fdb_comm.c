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

#include <gettimeofday_ms.h>

#include "sql.h"
#include "fdb_comm.h"
#include "fdb_bend.h"
#include "fdb_bend_sql.h"
#include "poll.h"
#include "flibc.h"
#include "logmsg.h"
#include "ssl_bend.h" /* for gbl_client_ssl_mode & gbl_ssl_allow_remsql */

extern int gbl_fdb_track;
extern int gbl_fdb_track_times;
extern int gbl_time_fdb;
extern int gbl_notimeouts;

extern int gbl_expressions_indexes;
void free_cached_idx(uint8_t **cached_idx);

/* matches fdb_svc_callback_t callbacks */
enum {
    FDB_MSG_TRAN_BEGIN = 0,
    FDB_MSG_TRAN_PREPARE = 1,
    FDB_MSG_TRAN_COMMIT = 2,
    FDB_MSG_TRAN_ROLLBACK = 3,
    FDB_MSG_TRAN_RC = 4,

    FDB_MSG_CURSOR_OPEN = 5,
    FDB_MSG_CURSOR_CLOSE = 6,
    FDB_MSG_CURSOR_FIND = 7,
    FDB_MSG_CURSOR_FIND_LAST = 8,
    FDB_MSG_CURSOR_FIRST = 9,
    FDB_MSG_CURSOR_LAST = 10,
    FDB_MSG_CURSOR_NEXT = 11,
    FDB_MSG_CURSOR_PREV = 12,

    FDB_MSG_DATA_ROW = 13,
    FDB_MSG_DATA_RC = 14,

    FDB_MSG_RUN_SQL = 15,

    FDB_MSG_INSERT = 16,
    FDB_MSG_DELETE = 17,
    FDB_MSG_UPDATE = 18,

    FDB_MSG_HBEAT = 19,

    /* OPs for partial indexes */
    FDB_MSG_INSERT_PI = 20,
    FDB_MSG_DELETE_PI = 21,
    FDB_MSG_UPDATE_PI = 22,

    FDB_MSG_INDEX = 23,

    FDB_MSG_MAX_OP
};

enum { FD_MSG_TYPE = 0x0fff, FD_MSG_FLAGS_ISUUID = 0x1000 };

struct fdb_msg_header {
    int type;
};
typedef struct fdb_msg_header fdb_msg_header_t;

struct prefilt_sql {
    int l_incl; /* length of bitmap */
    char *incl; /* bitmap of ondisk included columns */
    int l_expr; /* length of expression */
    char *expr; /* serialized expression */
};

struct fdb_msg_cursor_open {
    fdb_msg_header_t type; /* FDB_MSG_CURSOR_OPEN */
    char *cid;             /* cursor id */
    char *tid;             /* transaction id, create one if not existing */
    int flags;             /* feature-based */
    int rootpage;          /* which btree */
    int version;           /* which version of schema */
    int seq;               /* used for ordering of certain operations in
                              transactional cursors (see fdb_msg_tran_t) */
    uuid_t ciduuid;
    uuid_t tiduuid;
    int srcpid;     /* pid of the source */
    int srcnamelen; /* hostname of the source */
    char *srcname;
};
typedef struct fdb_msg_cursor_open fdb_msg_cursor_open_t;

struct fdb_msg_cursor_close {
    fdb_msg_header_t type; /* FDB_MSG_CURSOR_CLOSE */
    char *cid;             /* cursor id */
    char *tid;             /* transaction id */
    uuid_t ciduuid;
    uuid_t tiduuid;
    int seq; /* used for ordering of certain operations in
                transactional cursors (see fdb_msg_tran_t); */
};
typedef struct fdb_msg_cursor_close fdb_msg_cursor_close_t;

struct fdb_msg_cursor_move {
    fdb_msg_header_t type; /* FDB_MSG_CURSOR_FIND, .... */
    char *cid;             /* cursor id */
    uuid_t ciduuid;
};

typedef struct fdb_msg_cursor_move fdb_msg_cursor_move_t;

struct fdb_msg_tran {
    fdb_msg_header_t type;      /* FDB_MSG_TRAN_BEGIN, ... */
    char *tid;                  /* transaction id */
    enum transaction_level lvl; /* TRANLEVEL_SOSQL & co. */
    int flags;                  /* extensions */
    uuid_t tiduuid;
    int seq; /* sequencing tran begin/commit/rollback, writes, cursor open/close
                */
};
typedef struct fdb_msg_tran fdb_msg_tran_t;

struct fdb_msg_tran_rc {
    fdb_msg_header_t type; /* FDB_MS_TRAN_RC */
    char *tid;             /* transaction id */
    int rc;                /* result code */
    int errstrlen;         /* error string length */
    uuid_t tiduuid;
    char *errstr; /* error string, if any */
};
typedef struct fdb_msg_tran_rc fdb_msg_tran_rc_t;

struct fdb_msg_data_row {
    fdb_msg_header_t type;    /* FDB_MSG_DATA_ROW */
    char *cid;                /* id of the cursor */
    int rc;                   /* results of the search */
    unsigned long long genid; /* genid */
    int datalen; /* NET FORMAT = data length: high 16 bits; datacopy length: low
                    16 bits*/
    int datacopylen; /* datacopylen after extraction */
    char *data;      /* row(including datacopy) or error string */
    char *datacopy;  /* after extraction */
    uuid_t ciduuid;
};
typedef struct fdb_msg_data_row fdb_msg_data_row_t;

struct fdb_msg_cursor_find {
    fdb_msg_header_t type; /* FDB_MSG_CURSOR_FIND */
    char *cid;             /* id of the cursor */
    int keylen;            /* keylen, serialized sqlite */
    char *key;             /* key, serialized sqlite */
    uuid_t ciduuid;
};
typedef struct fdb_msg_cursor_find fdb_msg_cursor_find_t;

#define FDB_RUN_SQL

struct fdb_msg_run_sql {
    fdb_msg_header_t type;    /* FDB_MSG_RUN_SQL */
    char *cid;                /* cursor id */
    int version;              /* schema version */
    enum run_sql_flags flags; /* flags changing the remote behaviour */
    int sqllen;               /* len of sql query */
    char *sql;                /* sql query */
    int keylen; /* keylen used for end trimming  NOT USED RIGHT NOW!*/
    char *key;  /* key ised for end trimming  NOT USED RIGHT NOW! */
    uuid_t ciduuid;
};
typedef struct fdb_msg_run_sql fdb_msg_run_sql_t;

struct fdb_msg_insert {
    fdb_msg_header_t type;       /* FDB_MSG_INSERT */
    char *cid;                   /* cursor id */
    int version;                 /* schema version */
    int rootpage;                /* which btree I am inserting into */
    unsigned long long genid;    /* genid */
    unsigned long long ins_keys; /* indexes to insert */
    int datalen;                 /* length of sqlite row, see below */
    int seq;                     /* transaction sequencing */
    uuid_t ciduuid;
    char *data; /* sqlite generated row from MakeRecord, serialized */
};
typedef struct fdb_msg_insert fdb_msg_insert_t;

struct fdb_msg_index {
    fdb_msg_header_t type;    /* FDB_MSG_INDEX */
    char *cid;                /* cursor id */
    int version;              /* schema version */
    int rootpage;             /* which btree I am inserting into */
    unsigned long long genid; /* genid */
    int is_delete;            /* 1 for delete, 0 for add */
    int ixnum;                /* index number */
    int ixlen;                /* length of sqlite index row, see below */
    int seq;                  /* transaction sequencing */
    uuid_t ciduuid;
    char *ix; /* sqlite generated index, serialized */
};
typedef struct fdb_msg_index fdb_msg_index_t;

struct fdb_msg_delete {
    fdb_msg_header_t type;       /* FDB_MSG_DELETE */
    char *cid;                   /* cursor id */
    int version;                 /* schema version */
    int rootpage;                /* which btree I am deleting from */
    unsigned long long genid;    /* genid */
    unsigned long long del_keys; /* indexes to delete */
    uuid_t ciduuid;
    int seq; /* transaction sequencing */
};
typedef struct fdb_msg_delete fdb_msg_delete_t;

struct fdb_msg_update {
    fdb_msg_header_t type;       /* FDB_MSG_UPDATE */
    char *cid;                   /* cursor id */
    int version;                 /* schema version */
    int rootpage;                /* which btree I am inserting into */
    unsigned long long oldgenid; /* oldgenid */
    unsigned long long genid;    /* genid */
    unsigned long long ins_keys; /* indexes to insert */
    unsigned long long del_keys; /* indexes to delete */
    int datalen;                 /* length of sqlite row, see below */
    int seq;                     /* transaction sequencing */
    uuid_t ciduuid;
    char *data; /* sqlite generated row from MakeRecord, serialized */
};
typedef struct fdb_msg_update fdb_msg_update_t;

struct fdb_msg_hbeat {
    fdb_msg_header_t type;    /* FDB_MSG_UPDATE */
    char *tid;                /* tran id */
    struct timespec timespec; /* when was this hbeat sent */
    uuid_t tiduuid;
};
typedef struct fdb_msg_hbeat fdb_msg_hbeat_t;

union fdb_msg {
    fdb_msg_header_t hd;
    fdb_msg_cursor_open_t co;
    fdb_msg_cursor_close_t cc;
    fdb_msg_cursor_move_t cm;
    fdb_msg_tran_t tr;
    fdb_msg_tran_rc_t rc;
    fdb_msg_data_row_t dr;
    fdb_msg_cursor_find_t cf;
    fdb_msg_run_sql_t sq;
    fdb_msg_insert_t in;
    fdb_msg_delete_t de;
    fdb_msg_update_t up;
    fdb_msg_index_t ix;
    fdb_msg_hbeat_t hb;
};

static int fdb_msg_read_message(SBUF2 *sb, fdb_msg_t *msg);
static int fdb_msg_write_message(SBUF2 *sb, fdb_msg_t *msg, int flush);
static void fdb_msg_print_message(SBUF2 *sb, fdb_msg_t *msg, char *prefix);
static void fdb_msg_print_message_uuid(SBUF2 *sb, fdb_msg_t *msg, char *prefix);

typedef struct {
    struct sqlclntstate *clnt;
    struct sql_thread *thd;
    int isuuid;
} svc_callback_arg_t;

typedef int (*fdb_svc_callback_t)(SBUF2 *sb, fdb_msg_t *msg,
                                  svc_callback_arg_t *arg);

int fdb_remcur_trans_begin(SBUF2 *sb, fdb_msg_t *msg, svc_callback_arg_t *arg);
int fdb_remcur_trans_prepare(SBUF2 *sb, fdb_msg_t *msg,
                             svc_callback_arg_t *arg);
int fdb_remcur_trans_commit(SBUF2 *sb, fdb_msg_t *msg, svc_callback_arg_t *arg);
int fdb_remcur_trans_rollback(SBUF2 *sb, fdb_msg_t *msg,
                              svc_callback_arg_t *arg);
int fdb_remcur_trans_rc(SBUF2 *sb, fdb_msg_t *msg, svc_callback_arg_t *arg);
int fdb_remcur_cursor_open(SBUF2 *sb, fdb_msg_t *msg, svc_callback_arg_t *arg);
int fdb_remcur_cursor_close(SBUF2 *sb, fdb_msg_t *msg, svc_callback_arg_t *arg);
int fdb_remcur_cursor_find(SBUF2 *sb, fdb_msg_t *msg, svc_callback_arg_t *arg);
int fdb_remcur_cursor_move(SBUF2 *sb, fdb_msg_t *msg, svc_callback_arg_t *arg);
int fdb_remcur_nop(SBUF2 *sb, fdb_msg_t *msg, svc_callback_arg_t *arg);
int fdb_remcur_run_sql(SBUF2 *sb, fdb_msg_t *msg, svc_callback_arg_t *arg);
int fdb_remcur_insert(SBUF2 *sb, fdb_msg_t *msg, svc_callback_arg_t *arg);
int fdb_remcur_delete(SBUF2 *sb, fdb_msg_t *msg, svc_callback_arg_t *arg);
int fdb_remcur_update(SBUF2 *sb, fdb_msg_t *msg, svc_callback_arg_t *arg);
int fdb_remcur_index(SBUF2 *sb, fdb_msg_t *msg, svc_callback_arg_t *arg);
int fdb_remcur_trans_hbeat(SBUF2 *sb, fdb_msg_t *msg, svc_callback_arg_t *arg);

/* matches FDB_MSG... enums */
fdb_svc_callback_t callbacks[] = {
    fdb_remcur_trans_begin, fdb_remcur_trans_prepare, fdb_remcur_trans_commit,
    fdb_remcur_trans_rollback, fdb_remcur_trans_rc,

    fdb_remcur_cursor_open, fdb_remcur_cursor_close, fdb_remcur_cursor_find,
    fdb_remcur_cursor_find, fdb_remcur_cursor_move, fdb_remcur_cursor_move,
    fdb_remcur_cursor_move, fdb_remcur_cursor_move,

    fdb_remcur_nop, fdb_remcur_nop,

    fdb_remcur_run_sql,

    fdb_remcur_insert, fdb_remcur_delete, fdb_remcur_update,

    fdb_remcur_trans_hbeat,

    fdb_remcur_insert, fdb_remcur_delete, fdb_remcur_update,

    fdb_remcur_index};

char *fdb_msg_type(int type)
{
    switch (type) {
    case FDB_MSG_TRAN_BEGIN:
        return "FDB_MSG_TRAN_BEGIN";
    case FDB_MSG_TRAN_PREPARE:
        return "FDB_MSG_TRAN_PREPARE";
    case FDB_MSG_TRAN_COMMIT:
        return "FDB_MSG_TRAN_COMMIT";
    case FDB_MSG_TRAN_ROLLBACK:
        return "FDB_MSG_TRAN_ROLLBACK";
    case FDB_MSG_TRAN_RC:
        return "FDB_MSG_TRAN_RC";
    case FDB_MSG_CURSOR_OPEN:
        return "FDB_MSG_CURSOR_OPEN";
    case FDB_MSG_CURSOR_CLOSE:
        return "FDB_MSG_CURSOR_CLOSE";
    case FDB_MSG_CURSOR_FIND:
        return "FDB_MSG_CURSOR_FIND";
    case FDB_MSG_CURSOR_FIND_LAST:
        return "FDB_MSG_CURSOR_FIND_LAST";
    case FDB_MSG_CURSOR_FIRST:
        return "FDB_MSG_CURSOR_FIRST";
    case FDB_MSG_CURSOR_LAST:
        return "FDB_MSG_CURSOR_LAST";
    case FDB_MSG_CURSOR_NEXT:
        return "FDB_MSG_CURSOR_NEXT";
    case FDB_MSG_CURSOR_PREV:
        return "FDB_MSG_CURSOR_PREV";
    case FDB_MSG_DATA_ROW:
        return "FDB_MSG_DATA_ROW";
    case FDB_MSG_DATA_RC:
        return "FDB_MSG_DATA_RC";
    case FDB_MSG_RUN_SQL:
        return "FDB_MSG_RUN_SQL";
    case FDB_MSG_INSERT:
        return "FDB_MSG_INSERT";
    case FDB_MSG_DELETE:
        return "FDB_MSG_DELETE";
    case FDB_MSG_UPDATE:
        return "FDB_MSG_UPDATE";
    case FDB_MSG_HBEAT:
        return "FDB_MSG_HBEAT";
    case FDB_MSG_INSERT_PI:
        return "FDB_MSG_INSERT_PI";
    case FDB_MSG_DELETE_PI:
        return "FDB_MSG_DELETE_PI";
    case FDB_MSG_UPDATE_PI:
        return "FDB_MSG_UPDATE_PI";
    case FDB_MSG_INDEX:
        return "FDB_MSG_INDEX";

    default:
        return "???";
    }
}

int fdb_send_open(fdb_msg_t *msg, char *cid, fdb_tran_t *trans, int rootp,
                  int flags, int version, int isuuid, SBUF2 *sb)
{
    int rc;

    /* useless but uniform */
    fdb_msg_clean_message(msg);

    msg->hd.type = FDB_MSG_CURSOR_OPEN;

    msg->co.cid = (char *)msg->co.ciduuid;
    msg->co.tid = (char *)msg->co.tiduuid;

    comdb2uuid_clear(msg->co.cid);
    comdb2uuid_clear(msg->co.tid);

    if (isuuid) {
        memcpy(msg->co.cid, cid, sizeof(uuid_t));
        if (trans)
            memcpy(msg->co.tid, trans->tid, sizeof(uuid_t));
        msg->hd.type |= FD_MSG_FLAGS_ISUUID;
    } else {
        memcpy(msg->co.cid, cid, sizeof(unsigned long long));
        if (trans)
            memcpy(msg->co.tid, trans->tid, sizeof(unsigned long long));
    }

    msg->co.flags = flags;
    msg->co.rootpage = rootp;
    msg->co.version = version;
    /* FDB_VER_LEGACY packet ends here */
    msg->co.seq = (trans) ? trans->seq : 0;
    /* FDB_VER_COVE_VERSION ends here; seq only for transactional cursors */
    msg->co.srcpid = gbl_mypid;
    msg->co.srcnamelen = strlen(gbl_myhostname) + 1;
    msg->co.srcname = gbl_myhostname;
    /* FDB_VER_SOURCE_ID ends here */

    sbuf2printf(sb, "remsql\n");

    rc = fdb_msg_write_message(sb, msg, 1);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: failed sending fdbc cursor_open message rc=%d\n",
                __func__, rc);
        return rc;
    }

    if (gbl_fdb_track) {
        fdb_msg_print_message(sb, msg, "sending open cursor");
    }

    return FDB_NOERR;
}

int fdb_send_close(fdb_msg_t *msg, char *cid, char *tid, int isuuid, int seq,
                   SBUF2 *sb)
{
    int rc;

    fdb_msg_clean_message(msg);

    comdb2uuid_clear(msg->cc.ciduuid);
    comdb2uuid_clear(msg->cc.tiduuid);

    msg->hd.type = FDB_MSG_CURSOR_CLOSE;

    msg->cc.cid = (char *)msg->cc.ciduuid;
    msg->cc.tid = (char *)msg->cc.tiduuid;

    if (isuuid) {
        memcpy(msg->cc.cid, cid, sizeof(uuid_t));
        if (tid)
            comdb2uuidcpy(msg->cc.tid, tid);
        msg->hd.type |= FD_MSG_FLAGS_ISUUID;
    } else {
        memcpy(msg->cc.cid, cid, sizeof(unsigned long long));
        if (tid)
            memcpy(msg->cc.tid, tid, sizeof(unsigned long long));
    }

    msg->cc.seq = seq;

    rc = fdb_msg_write_message(sb, msg, 1);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: failed sending fdbc cursor_close message rc=%d\n",
                __func__, rc);
        return rc;
    }

    if (gbl_fdb_track) {
        fdb_msg_print_message(sb, msg, "sending close cursor");
    }

    return rc;
}

static int fdb_move_type(int sqlglue_type)
{
    switch (sqlglue_type) {
    case CFIRST:
        return FDB_MSG_CURSOR_FIRST;
    case CNEXT:
        return FDB_MSG_CURSOR_NEXT;
    case CPREV:
        return FDB_MSG_CURSOR_PREV;
    case CLAST:
        return FDB_MSG_CURSOR_LAST;
    }

    logmsg(LOGMSG_ERROR, "%s: unknown move rc=%d?\n", __func__, sqlglue_type);
    return -1;
}

int fdb_send_move(fdb_msg_t *msg, char *cid, int how, int isuuid, SBUF2 *sb)
{
    int rc = 0;
    fdb_msg_clean_message(msg);

    /* request row remotely */
    msg->hd.type = fdb_move_type(how);
    assert(msg->hd.type >= 0);

    comdb2uuid_clear(msg->cm.ciduuid);

    msg->cm.cid = (char *)msg->cm.ciduuid;

    if (isuuid) {
        memcpy(msg->cm.cid, cid, sizeof(uuid_t));
        msg->hd.type |= FD_MSG_FLAGS_ISUUID;
    } else {
        memcpy(msg->cm.cid, cid, sizeof(unsigned long long));
    }

    rc = fdb_msg_write_message(sb, msg, 1);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: failed fdbc move rc=%d\n", __func__, rc);
        return rc;
    }

    if (gbl_fdb_track) {
        fdb_msg_print_message(sb, msg, "sending msg");
    }

    return rc;
}

int fdb_send_run_sql(fdb_msg_t *msg, char *cid, int sqllen, char *sql,
                     int version, int keylen, char *key,
                     enum run_sql_flags flags, int isuuid, SBUF2 *sb)
{
    int rc = 0;
    fdb_msg_clean_message(msg);

    /* request streaming remotely */
    msg->hd.type = FDB_MSG_RUN_SQL;

    msg->sq.cid = (char *)msg->sq.ciduuid;

    comdb2uuid_clear(msg->sq.ciduuid);

    if (isuuid) {
        memmove(msg->sq.cid, cid, sizeof(uuid_t));
        msg->hd.type |= FD_MSG_FLAGS_ISUUID;
    } else
        memcpy(msg->sq.cid, cid, sizeof(unsigned long long));

    msg->sq.sqllen = sqllen;
    msg->sq.sql = sql;
    msg->sq.version = version;
    msg->sq.flags = flags;
    msg->sq.keylen = keylen;
    msg->sq.key = key;

    rc = fdb_msg_write_message(sb, msg, 1);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: failed fdbc run sql rc=%d\n", __func__, rc);
        goto done;
    }

    if (gbl_fdb_track) {
        fdb_msg_print_message(sb, msg, "sending run sql");
    }

done:
    /* requestor manages the passed attributes */
    msg->sq.sqllen = 0;
    msg->sq.sql = NULL;
    msg->sq.keylen = 0;
    msg->sq.key = NULL;

    return rc;
}

int fdb_msg_size(void) { return sizeof(fdb_msg_t); }

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
            logmsg(LOGMSG_ERROR, "%s: fdb_send_rc failed rc=%d\n", __func__, rc);
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

    rc = fdb_msg_read_message(sb, &msg);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: failed to handle remote cursor request rc=%d\n",
                __func__, rc);
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
        rc = fdb_msg_read_message(sb, &msg);
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

int handle_remsql(SBUF2 *sb, struct dbenv *dbenv)
{
    char line[128];
    int rc = FDB_NOERR;
    static uint64_t old = 0ULL;
    uint64_t now, then;

    /* we will rely of socket drop to determine the end of
       connection */
    sbuf2settimeout(sb, 0, 0);

    if (gbl_fdb_track_times) {
        now = gettimeofday_ms();
        logmsg(LOGMSG_USER, "RRRRRR start now=%lld\n", now);
    }

    while (1) {
        if (gbl_fdb_track_times) {
            now = gettimeofday_ms();
        }

        rc = handle_remsql_session(sb, dbenv);
        if (gbl_fdb_track)
            logmsg(LOGMSG_USER, "%p: %s: executed session rc=%d\n", pthread_self(),
                    __func__, rc);

        if (gbl_fdb_track_times) {
            then = gettimeofday_ms();

            if (old == 0ULL) {
                logmsg(LOGMSG_USER, "RRRRRR now=%lld 0 %lld\n", now, then - now);
            } else {
                logmsg(LOGMSG_USER, "RRRRRR now=%lld delta=%lld %lld\n", now,
                        now - old, then - now);
            }
            old = now;
        }

        if (rc == FDB_NOERR) {
            /* we need to read the header again, waiting here */
            rc = sbuf2gets(line, sizeof(line), sb);
            if (rc != strlen("remsql\n")) {
                if (rc != -1)
                    logmsg(LOGMSG_ERROR, "%s: received wrong request! rc=%d: %s\n",
                            __func__, rc, line);
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
        logmsg(LOGMSG_USER, "%p: %s: done processing\n", pthread_self(), __func__);

    return rc;
}

int handle_remtran(SBUF2 *sb, struct dbenv *dbenv)
{
    fdb_msg_tran_t open_msg;
    fdb_msg_t msg;
    int rc = 0;
    svc_callback_arg_t arg = {0};

    bzero(&msg, sizeof(msg));

    arg.thd = start_sql_thread(); /* this does inserts on behalf of an sql
                                     transaction */

    rc = fdb_msg_read_message(sb, &msg);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: failed to handle remote cursor request rc=%d\n",
                __func__, rc);
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

        arg.isuuid = (msg.hd.type & FD_MSG_FLAGS_ISUUID);

        rc = callbacks[msg.hd.type & FD_MSG_TYPE](sb, &msg, &arg);

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
                open_msg.tid, open_msg.lvl, arg.clnt,
                arg.clnt->dbtran.dtran->fdb_trans.top->seq);
            if (rc2) {
                logmsg(LOGMSG_ERROR, "%s: fdb_svc_trans_rollback failed rc=%d\n",
                        __func__, rc2);
            }
            break;
        }

        /*fprintf(stderr, "XYXY %llu calling recv message\n",
         * osql_log_time());*/
        rc = fdb_msg_read_message(sb, &msg);
        if (rc) {
            logmsg(LOGMSG_ERROR, 
                    "%s: failed to handle remote cursor request rc=%d\n",
                    __func__, rc);
            goto clear;
        }
    }

    if (gbl_expressions_indexes) {
        free(arg.clnt->idxInsert);
        free(arg.clnt->idxDelete);
        arg.clnt->idxInsert = arg.clnt->idxDelete = NULL;
    }

    reset_clnt(arg.clnt, NULL, 0);

    done_sql_thread();

    pthread_mutex_destroy(&arg.clnt->wait_mutex);
    pthread_cond_destroy(&arg.clnt->wait_cond);
    pthread_mutex_destroy(&arg.clnt->write_lock);
    pthread_mutex_destroy(&arg.clnt->dtran_mtx);

    free(arg.clnt);
    arg.clnt = NULL;

    return rc;
}

int handle_remcur(SBUF2 *sb, struct dbenv *dbenv)
{
    fdb_msg_cursor_open_t open_msg;
    fdb_msg_t msg;
    int rc = 0;
    svc_callback_arg_t arg = {0};
    int flags;

    bzero(&msg, sizeof(msg));

    logmsg(LOGMSG_DEBUG, "Received remcu request\n");

    rc = fdb_msg_read_message(sb, &msg);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: failed to handle remote cursor request rc=%d\n",
                __func__, rc);
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

        rc = callbacks[msg.hd.type](sb, &msg, &arg);
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

        rc = fdb_msg_read_message(sb, &msg);
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

int fdb_recv_row(fdb_msg_t *msg, char *cid, int isuuid, SBUF2 *sb)
{
    int rc = 0;
    int flags;

    rc = fdb_msg_read_message(sb, msg);
    if (rc) {
        /* maybe this should return FDB_ERR_READ_IO instead */
        return -1;
    }
    flags = msg->hd.type & ~FD_MSG_TYPE;

    if (gbl_fdb_track) {
        fdb_msg_print_message(sb, msg, "received message");
    }

    msg->hd.type &= FD_MSG_TYPE;

    switch (msg->hd.type) {
    case FDB_MSG_DATA_ROW:
#if 0
         /* check for my rows */
         if(memcmp(msg->dr.cid, cid, sizeof(msg->dr.cid)) == 0)
         {
            /* TODO: grab more than one row from socket? */
#endif
        rc = msg->dr.rc;
        goto ret_row;
#if 0
         }
         else
         {
            /* TODO : clone and cache for other receivers */
            abort ();
         }
#endif
        break;

    case FDB_MSG_TRAN_RC:

        rc = msg->rc.rc;
        goto ret_row;

    default:
        abort();
    }

ret_row:
    return rc;
}

int fdb_recv_rc(fdb_msg_t *msg, fdb_tran_t *trans)
{
    trans->rc = fdb_recv_row(msg, trans->tid, trans->isuuid, trans->sb);

    if (trans->isuuid) {
        if ((trans->rc == 0) && (comdb2uuidcmp(msg->rc.tid, trans->tid) != 0)) {
            abort();
        }
    } else {
        if ((trans->rc == 0) && (memcmp(msg->rc.tid, trans->tid,
                                        sizeof(unsigned long long)) != 0)) {
            abort();
        }
    }

    if (trans->rc) {
        trans->errstr = msg->rc.errstr;
        trans->errstrlen = msg->rc.errstrlen;
    } else {
        if (msg->rc.errstr) {
            logmsg(LOGMSG_ERROR, "%s: rc=%d but errror string present?\n", __func__,
                    msg->rc.rc);
            free(msg->rc.errstr);
        }
        trans->errstr = NULL;
        trans->errstrlen = 0;
    }

    /* errstr, if any, is owned by fdb_tran_t now */
    msg->rc.errstrlen = 0;
    msg->rc.errstr = NULL;

    return trans->rc;
}

unsigned long long fdb_msg_genid(fdb_msg_t *msg) { return msg->dr.genid; }

int fdb_msg_datalen(fdb_msg_t *msg) { return msg->dr.datalen; }

char *fdb_msg_data(fdb_msg_t *msg)
{

    /*
    printf("Returning %p\n", msg->dr.data);
    */
    return msg->dr.data;
}

void fdb_msg_clean_message(fdb_msg_t *msg)
{
    switch (msg->hd.type & FD_MSG_TYPE) {
    case FDB_MSG_TRAN_BEGIN:
    case FDB_MSG_TRAN_PREPARE:
    case FDB_MSG_TRAN_COMMIT:
    case FDB_MSG_TRAN_ROLLBACK:
        break;

    case FDB_MSG_TRAN_RC:
        if (msg->rc.errstrlen && msg->rc.errstr) {
            free(msg->rc.errstr);
            msg->rc.errstr = NULL;
            msg->rc.errstrlen = 0;
        }
        break;

    case FDB_MSG_CURSOR_OPEN:
        break;

    case FDB_MSG_CURSOR_CLOSE:
        break;

    case FDB_MSG_CURSOR_FIRST:
    case FDB_MSG_CURSOR_LAST:
    case FDB_MSG_CURSOR_NEXT:
    case FDB_MSG_CURSOR_PREV:
        break;

    case FDB_MSG_DATA_ROW:
        if ((msg->dr.datalen > 0 || msg->dr.datacopylen > 0) && msg->dr.data) {
            /*
            printf("Freeing %p\n", msg->dr.data);
            */
            free(msg->dr.data);
            msg->dr.data = NULL;
            msg->dr.datalen = msg->dr.datacopylen = 0;
        }
        break;

    case FDB_MSG_CURSOR_FIND:
    case FDB_MSG_CURSOR_FIND_LAST:
        if (msg->cf.keylen > 0 && msg->cf.key) {
            free(msg->cf.key);
            msg->cf.key = NULL;
            msg->cf.keylen = 0;
        }
        break;

    case FDB_MSG_RUN_SQL:
        if (msg->sq.sqllen > 0 && msg->sq.sql) {
            free(msg->sq.sql);
            msg->sq.sql = NULL;
            msg->sq.sqllen = 0;
        }

        if (msg->sq.flags == FDB_RUN_SQL_TRIM) {
            if (msg->sq.keylen > 0 && msg->sq.key) {
                free(msg->sq.key);
                msg->sq.key = NULL;
                msg->sq.keylen = 0;
            }
        }
        break;

    case FDB_MSG_INSERT:
    case FDB_MSG_INSERT_PI:
        if (msg->in.datalen > 0 && msg->in.data) {
            free(msg->in.data);
            msg->in.data = NULL;
            msg->in.datalen = 0;
        }
        break;

    case FDB_MSG_DELETE:
    case FDB_MSG_DELETE_PI:
        break;

    case FDB_MSG_UPDATE:
    case FDB_MSG_UPDATE_PI:
        if (msg->up.datalen > 0 && msg->up.data) {
            free(msg->up.data);
            msg->up.data = NULL;
            msg->up.datalen = 0;
        }
        break;

    case FDB_MSG_INDEX:
        if (msg->ix.ixlen > 0 && msg->ix.ix) {
            free(msg->ix.ix);
            msg->ix.ix = NULL;
            msg->ix.ixlen = 0;
        }
        break;

    case FDB_MSG_HBEAT:
        break;

    default:
        logmsg(LOGMSG_ERROR, "%s: unknown msg %d\n", __func__, msg->hd.type);
    }
}

static void fdb_msg_prepare_message(fdb_msg_t *msg)
{
    switch (msg->hd.type & FD_MSG_TYPE) {
    case FDB_MSG_TRAN_BEGIN:
    case FDB_MSG_TRAN_PREPARE:
    case FDB_MSG_TRAN_COMMIT:
    case FDB_MSG_TRAN_ROLLBACK:
        msg->tr.tid = msg->tr.tiduuid;
        break;

    case FDB_MSG_TRAN_RC:
        msg->rc.tid = msg->rc.tiduuid;
        break;

    case FDB_MSG_CURSOR_OPEN:
        msg->co.cid = msg->co.ciduuid;
        msg->co.tid = msg->co.tiduuid;
        break;

    case FDB_MSG_CURSOR_CLOSE:
        msg->cc.cid = msg->cc.ciduuid;
        msg->cc.tid = msg->cc.tiduuid;
        break;

    case FDB_MSG_CURSOR_FIRST:
    case FDB_MSG_CURSOR_LAST:
    case FDB_MSG_CURSOR_NEXT:
    case FDB_MSG_CURSOR_PREV:
        msg->cm.cid = msg->cm.ciduuid;
        break;

    case FDB_MSG_DATA_ROW:
        msg->dr.cid = msg->dr.ciduuid;
        break;

    case FDB_MSG_CURSOR_FIND:
    case FDB_MSG_CURSOR_FIND_LAST:
        msg->cf.cid = msg->cf.ciduuid;
        break;

    case FDB_MSG_RUN_SQL:
        msg->sq.cid = msg->sq.ciduuid;
        break;

    case FDB_MSG_INSERT:
    case FDB_MSG_INSERT_PI:
        msg->in.cid = msg->in.ciduuid;
        break;

    case FDB_MSG_DELETE:
    case FDB_MSG_DELETE_PI:
        msg->de.cid = msg->de.ciduuid;
        break;

    case FDB_MSG_UPDATE:
    case FDB_MSG_UPDATE_PI:
        msg->up.cid = msg->up.ciduuid;
        break;

    case FDB_MSG_INDEX:
        msg->ix.cid = msg->ix.ciduuid;
        break;

    case FDB_MSG_HBEAT:
        msg->hb.tid = msg->hb.tiduuid;
        break;

    default:
        logmsg(LOGMSG_ERROR, "%s: unknown msg %d\n", __func__, msg->hd.type);
    }
}

/* stuff comes in network endian fomat */
static int fdb_msg_read_message(SBUF2 *sb, fdb_msg_t *msg)
{
    int rc;
    unsigned long long lltmp;
    int tmp;
    int idsz;
    int isuuid = 0;
    int recv_dk = 0;

    if (gbl_client_ssl_mode >= SSL_REQUIRE && !gbl_ssl_allow_remsql) {
        logmsg(LOGMSG_ERROR,
               "Remote SQL is forbidden because client SSL is required.");
        return -1;
    }

    /* clean previous message */
    fdb_msg_clean_message(msg);

    rc = sbuf2fread((char *)&msg->hd.type, 1, sizeof(msg->hd.type), sb);

    /*fprintf(stderr, "XYXY returned from sbuf2fread %llu\n",
     * osql_log_time());*/

    if (rc != sizeof(msg->hd.type)) {
        logmsg(LOGMSG_ERROR, "%s: failed to read header rc=%d\n", __func__, rc);
        return -1;
    }

    msg->hd.type = ntohl(msg->hd.type);

    // printf(">>> type %d %s isuuid %d\n", msg->hd.type & FD_MSG_TYPE,
    // fdb_msg_type(msg->hd.type & FD_MSG_TYPE), FD_MSG_FLAGS_ISUUID);

    fdb_msg_prepare_message(msg);

    /*fprintf(stderr, "XYXY returned from sbuf2fread %llu\n",
     * osql_log_time());*/

    if (msg->hd.type & FD_MSG_FLAGS_ISUUID) {
        idsz = sizeof(uuid_t);
        isuuid = 1;
    } else
        idsz = sizeof(unsigned long long);

    recv_dk = 0;
    switch (msg->hd.type & FD_MSG_TYPE) {
    case FDB_MSG_TRAN_BEGIN:
    case FDB_MSG_TRAN_PREPARE:
    case FDB_MSG_TRAN_COMMIT:
    case FDB_MSG_TRAN_ROLLBACK:

        rc = sbuf2fread(msg->tr.tid, 1, idsz, sb);
        if (rc != idsz)
            return -1;

        rc = sbuf2fread((char *)&msg->tr.lvl, 1, sizeof(msg->tr.lvl), sb);
        if (rc != sizeof(msg->tr.lvl))
            return -1;
        msg->tr.lvl = ntohl(msg->tr.lvl);

        rc = sbuf2fread((char *)&msg->tr.flags, 1, sizeof(msg->tr.flags), sb);
        if (rc != sizeof(msg->tr.flags))
            return -1;
        msg->tr.flags = ntohl(msg->tr.flags);

        rc = sbuf2fread((char *)&msg->tr.seq, 1, sizeof(msg->tr.seq), sb);
        if (rc != sizeof(msg->tr.seq))
            return -1;
        msg->tr.seq = ntohl(msg->tr.seq);

        break;

    case FDB_MSG_TRAN_RC:

        rc = sbuf2fread((char *)msg->rc.tid, 1, idsz, sb);
        if (rc != idsz)
            return -1;

        rc = sbuf2fread((char *)&msg->rc.rc, 1, sizeof(msg->rc.rc), sb);
        if (rc != sizeof(msg->rc.rc))
            return -1;
        msg->rc.rc = ntohl(msg->rc.rc);

        rc = sbuf2fread((char *)&msg->rc.errstrlen, 1,
                        sizeof(msg->rc.errstrlen), sb);
        if (rc != sizeof(msg->rc.errstrlen))
            return -1;
        msg->rc.errstrlen = ntohl(msg->rc.errstrlen);

        if (msg->rc.errstrlen) {
            msg->rc.errstr = (char *)malloc(msg->rc.errstrlen);
            if (!msg->rc.errstr)
                return -1;

            rc = sbuf2fread(msg->rc.errstr, 1, msg->rc.errstrlen, sb);
            if (rc != msg->rc.errstrlen)
                return -1;
        } else {
            msg->rc.errstr = NULL;
        }

        break;

    case FDB_MSG_CURSOR_OPEN:

        rc = sbuf2fread(msg->co.cid, 1, idsz, sb);
        if (rc != idsz)
            return -1;

        rc = sbuf2fread(msg->co.tid, 1, idsz, sb);
        if (rc != idsz)
            return -1;

        rc = sbuf2fread((char *)&msg->co.flags, 1, sizeof(msg->co.flags), sb);
        if (rc != sizeof(msg->co.flags))
            return -1;
        msg->co.flags = ntohl(msg->co.flags);

        rc = sbuf2fread((char *)&msg->co.rootpage, 1, sizeof(msg->co.rootpage),
                        sb);
        if (rc != sizeof(msg->co.rootpage))
            return -1;
        msg->co.rootpage = ntohl(msg->co.rootpage);

        rc = sbuf2fread((char *)&msg->co.version, 1, sizeof(msg->co.version),
                        sb);
        if (rc != sizeof(msg->co.version))
            return -1;
        msg->co.version = ntohl(msg->co.version);

        if (msg->co.flags == FDB_MSG_CURSOR_OPEN_SQL) {
            msg->co.seq = 0;
            msg->co.srcpid = 0;
            msg->co.srcnamelen = 0;
            msg->co.srcname = NULL;
            break;
        }

        rc = sbuf2fread((char *)&msg->co.seq, 1, sizeof(msg->co.seq), sb);
        if (rc != sizeof(msg->co.seq))
            return -1;
        msg->co.seq = ntohl(msg->co.seq);

        if (msg->co.flags == FDB_MSG_CURSOR_OPEN_SQL_TRAN) {
            msg->co.srcpid = 0;
            msg->co.srcnamelen = 0;
            msg->co.srcname = NULL;
            break;
        }

        rc = sbuf2fread((char *)&msg->co.srcpid, 1, sizeof(msg->co.srcpid), sb);
        if (rc != sizeof(msg->co.srcpid))
            return -1;
        msg->co.srcpid = ntohl(msg->co.srcpid);

        rc = sbuf2fread((char *)&msg->co.srcnamelen, 1,
                        sizeof(msg->co.srcnamelen), sb);
        if (rc != sizeof(msg->co.srcnamelen))
            return -1;
        msg->co.srcnamelen = ntohl(msg->co.srcnamelen);

        if (msg->co.srcnamelen > 0) {
            msg->co.srcname = (char *)malloc(msg->co.srcnamelen);
            if (!msg->co.srcname)
                return -1;

            rc = sbuf2fread(msg->co.srcname, 1, msg->co.srcnamelen, sb);
            if (rc != msg->co.srcnamelen)
                return -1;
        } else {
            msg->co.srcname = NULL;
        }

        break;

    case FDB_MSG_CURSOR_CLOSE: {
        int haveid = 0;

        rc = sbuf2fread(msg->cc.cid, 1, idsz, sb);
        if (rc != idsz)
            return -1;

        /* locally populated, I can't break current read protocol */
        rc = fdb_svc_trans_get_tid(msg->cc.cid, msg->cc.tid, isuuid);
        if (rc)
            return -1;

        if (msg->hd.type & FD_MSG_FLAGS_ISUUID)
            haveid = !comdb2uuid_is_zero(msg->cc.tid);
        else
            haveid = *(unsigned long long *)msg->cc.tid != 0;

        if (haveid) {
            rc = sbuf2fread((char *)&tmp, 1, sizeof(tmp), sb);
            if (rc != sizeof(tmp))
                return FDB_ERR_WRITE_IO;
        }

        break;
    }

    case FDB_MSG_CURSOR_FIRST:
    case FDB_MSG_CURSOR_LAST:
    case FDB_MSG_CURSOR_NEXT:
    case FDB_MSG_CURSOR_PREV:

        rc = sbuf2fread(msg->cm.cid, 1, idsz, sb);
        if (rc != idsz)
            return -1;

        break;

    case FDB_MSG_DATA_ROW:

        rc = sbuf2fread(msg->dr.cid, 1, idsz, sb);
        if (rc != idsz)
            return -1;

        rc = sbuf2fread((char *)&msg->dr.rc, 1, sizeof(msg->dr.rc), sb);
        if (rc != sizeof(msg->dr.rc))
            return -1;
        msg->dr.rc = ntohl(msg->dr.rc);

        rc = sbuf2fread((char *)&msg->dr.genid, 1, sizeof(msg->dr.genid), sb);
        if (rc != sizeof(msg->dr.genid))
            return -1;
        msg->dr.genid = flibc_htonll(msg->dr.genid);

        rc = sbuf2fread((char *)&msg->dr.datalen, 1, sizeof(msg->dr.datalen),
                        sb);
        if (rc != sizeof(msg->dr.datalen))
            return -1;
        msg->dr.datalen = ntohl(msg->dr.datalen);
        msg->dr.datalen = (((unsigned)msg->dr.datalen) << 16) +
                          (((unsigned)msg->dr.datalen) >> 16);
        msg->dr.datacopylen = 0;

        if (msg->dr.datalen > 0 || msg->dr.datacopylen > 0) {
            msg->dr.data =
                (char *)malloc(msg->dr.datalen + msg->dr.datacopylen);
            if (!msg->dr.data)
                return -1;
            msg->dr.datacopy = msg->dr.data + msg->dr.datalen;
            /*
            printf("Received in %p\n", msg->dr.data);
            */
        } else {
            msg->dr.data = msg->dr.datacopy = NULL;
            msg->dr.datalen = msg->dr.datacopylen = 0;
        }

        if (msg->dr.datalen > 0) {
            rc = sbuf2fread(msg->dr.data, 1, msg->dr.datalen, sb);
            if (rc != msg->dr.datalen)
                return -1;
        } else {
            msg->dr.data = NULL;
        }

        if (msg->dr.datacopylen > 0) {
            rc = sbuf2fread(msg->dr.datacopy, 1, msg->dr.datacopylen, sb);
            if (rc != msg->dr.datacopylen)
                return -1;
        } else {
            msg->dr.datacopy = NULL;
        }

        break;

    case FDB_MSG_CURSOR_FIND:
    case FDB_MSG_CURSOR_FIND_LAST:

        rc = sbuf2fread((char *)msg->cf.cid, 1, idsz, sb);
        if (rc != idsz)
            return -1;

        rc = sbuf2fread((char *)&msg->cf.keylen, 1, sizeof(msg->cf.keylen), sb);
        if (rc != sizeof(msg->cf.keylen))
            return -1;
        msg->cf.keylen = ntohl(msg->cf.keylen);

        assert(msg->cf.keylen > 0);

        msg->cf.key = (char *)malloc(msg->cf.keylen);
        if (!msg->cf.key)
            return -1;

        rc = sbuf2fread(msg->cf.key, 1, msg->cf.keylen, sb);
        if (rc != msg->cf.keylen)
            return -1;

        break;

    case FDB_MSG_RUN_SQL:

        /*fprintf(stderr, "%d XYXY calling sbuf2fread %llu\n", __LINE__,
         * osql_log_time());*/

        rc = sbuf2fread((char *)msg->sq.cid, 1, idsz, sb);
        if (rc != idsz)
            return -1;
        /*fprintf(stderr, "%d XYXY DONE calling sbuf2fread %llu\n", __LINE__,
         * osql_log_time());*/

        /*fprintf(stderr, "%d XYXY calling sbuf2fread %llu\n", __LINE__,
         * osql_log_time());*/
        rc = sbuf2fread((char *)&msg->sq.version, 1, sizeof(msg->sq.version),
                        sb);
        if (rc != sizeof(msg->sq.version))
            return -1;
        msg->sq.version = ntohl(msg->sq.version);
        /*fprintf(stderr, "%d XYXY DONE calling sbuf2fread %llu\n", __LINE__,
         * osql_log_time());*/

        /*fprintf(stderr, "%d XYXY calling sbuf2fread %llu\n", __LINE__,
         * osql_log_time());*/
        rc = sbuf2fread((char *)&msg->sq.flags, 1, sizeof(msg->sq.flags), sb);
        if (rc != sizeof(msg->sq.flags))
            return -1;
        msg->sq.flags = ntohl(msg->sq.flags);
        /*fprintf(stderr, "%d XYXY DONE calling sbuf2fread %llu\n", __LINE__,
         * osql_log_time());*/

        /*fprintf(stderr, "%d XYXY calling sbuf2fread %llu\n", __LINE__,
         * osql_log_time());*/
        rc = sbuf2fread((char *)&msg->sq.sqllen, 1, sizeof(msg->sq.sqllen), sb);
        if (rc != sizeof(msg->sq.sqllen))
            return -1;
        msg->sq.sqllen = ntohl(msg->sq.sqllen);
        /*fprintf(stderr, "%d XYXY DONE calling sbuf2fread %llu\n", __LINE__,
         * osql_log_time());*/

        msg->sq.sql = (char *)malloc(msg->sq.sqllen);
        if (!msg->sq.sql)
            return -1;

        /*fprintf(stderr, "%d XYXY calling sbuf2fread %llu\n", __LINE__,
         * osql_log_time());*/
        rc = sbuf2fread(msg->sq.sql, 1, msg->sq.sqllen, sb);
        if (rc != msg->sq.sqllen)
            return -1;
        /*fprintf(stderr, "%d XYXY DONE calling sbuf2fread %llu\n", __LINE__,
         * osql_log_time());*/

        /* if we have end trimming, pass that */
        if (msg->sq.flags == FDB_RUN_SQL_TRIM) {
            /*fprintf(stderr, "%d XYXY calling sbuf2fread %llu\n", __LINE__,
             * osql_log_time());*/
            rc = sbuf2fread((char *)&msg->sq.keylen, 1, sizeof(msg->sq.keylen),
                            sb);
            if (rc != sizeof(msg->sq.keylen))
                return -1;
            msg->sq.keylen = ntohl(msg->sq.keylen);
            /*fprintf(stderr, "%d XYXY DONE calling sbuf2fread %llu\n",
             * __LINE__, osql_log_time());*/

            if (msg->sq.keylen > 0) {
                msg->sq.key = (char *)malloc(msg->sq.keylen);
                if (!msg->sq.key)
                    return -1;

                /*fprintf(stderr, "%d XYXY calling sbuf2fread %llu\n", __LINE__,
                 * osql_log_time());*/
                rc = sbuf2fread(msg->sq.key, 1, msg->sq.keylen, sb);
                if (rc != msg->sq.keylen)
                    return -1;
                /*fprintf(stderr, "%d XYXY DONE calling sbuf2fread %llu\n",
                 * __LINE__, osql_log_time());*/
            }

        } else {
            msg->sq.keylen = 0;
            msg->sq.key = NULL;
        }

        break;

    case FDB_MSG_INSERT_PI:
        recv_dk = 1;

    case FDB_MSG_INSERT:
        rc = sbuf2fread((char *)msg->in.cid, 1, idsz, sb);
        if (rc != idsz)
            return -1;

        rc = sbuf2fread((char *)&msg->in.version, 1, sizeof(msg->in.version),
                        sb);
        if (rc != sizeof(msg->in.version))
            return -1;
        msg->in.version = ntohl(msg->in.version);

        rc = sbuf2fread((char *)&msg->in.rootpage, 1, sizeof(msg->in.rootpage),
                        sb);
        if (rc != sizeof(msg->in.rootpage))
            return -1;
        msg->in.rootpage = ntohl(msg->in.rootpage);

        rc = sbuf2fread((char *)&msg->in.genid, 1, sizeof(msg->in.genid), sb);
        if (rc != sizeof(msg->in.genid))
            return -1;
        msg->in.genid = flibc_htonll(msg->in.genid);

        if (recv_dk) {
            rc = sbuf2fread((char *)&msg->in.ins_keys, 1,
                            sizeof(msg->in.ins_keys), sb);
            if (rc != sizeof(msg->in.ins_keys))
                return -1;
            msg->in.ins_keys = flibc_htonll(msg->in.ins_keys);
        } else {
            msg->in.ins_keys = -1ULL;
        }

        rc = sbuf2fread((char *)&msg->in.datalen, 1, sizeof(msg->in.datalen),
                        sb);
        if (rc != sizeof(msg->in.datalen))
            return -1;
        msg->in.datalen = ntohl(msg->in.datalen);

        rc = sbuf2fread((char *)&msg->in.seq, 1, sizeof(msg->in.seq), sb);
        if (rc != sizeof(msg->in.seq))
            return -1;
        msg->in.seq = ntohl(msg->in.seq);

        if (msg->in.datalen > 0) {
            msg->in.data = (char *)malloc(msg->in.datalen);
            if (!msg->in.data)
                return -1;

            rc = sbuf2fread(msg->in.data, 1, msg->in.datalen, sb);
            if (rc != msg->in.datalen)
                return -1;
        } else {
            msg->in.data = NULL;
        }

        break;

    case FDB_MSG_DELETE_PI:
        recv_dk = 1;

    case FDB_MSG_DELETE:

        rc = sbuf2fread(msg->de.cid, 1, idsz, sb);
        if (rc != idsz)
            return -1;

        rc = sbuf2fread((char *)&msg->de.version, 1, sizeof(msg->de.version),
                        sb);
        if (rc != sizeof(msg->de.version))
            return -1;
        msg->de.version = ntohl(msg->de.version);

        rc = sbuf2fread((char *)&msg->de.rootpage, 1, sizeof(msg->de.rootpage),
                        sb);
        if (rc != sizeof(msg->de.rootpage))
            return -1;
        msg->de.rootpage = ntohl(msg->de.rootpage);

        rc = sbuf2fread((char *)&msg->de.genid, 1, sizeof(msg->de.genid), sb);
        if (rc != sizeof(msg->de.genid))
            return -1;
        msg->de.genid = flibc_htonll(msg->de.genid);

        if (recv_dk) {
            rc = sbuf2fread((char *)&msg->de.del_keys, 1,
                            sizeof(msg->de.del_keys), sb);
            if (rc != sizeof(msg->de.del_keys))
                return -1;
            msg->de.del_keys = flibc_htonll(msg->de.del_keys);
        } else {
            msg->de.del_keys = -1ULL;
        }

        rc = sbuf2fread((char *)&msg->de.seq, 1, sizeof(msg->de.seq), sb);
        if (rc != sizeof(msg->de.seq))
            return -1;
        msg->de.seq = ntohl(msg->de.seq);

        break;

    case FDB_MSG_UPDATE_PI:
        recv_dk = 1;

    case FDB_MSG_UPDATE:

        rc = sbuf2fread(msg->up.cid, 1, idsz, sb);
        if (rc != idsz)
            return -1;

        rc = sbuf2fread((char *)&msg->up.version, 1, sizeof(msg->up.version),
                        sb);
        if (rc != sizeof(msg->up.version))
            return -1;
        msg->up.version = ntohl(msg->up.version);

        rc = sbuf2fread((char *)&msg->up.rootpage, 1, sizeof(msg->up.rootpage),
                        sb);
        if (rc != sizeof(msg->up.rootpage))
            return -1;
        msg->up.rootpage = ntohl(msg->up.rootpage);

        rc = sbuf2fread((char *)&msg->up.oldgenid, 1, sizeof(msg->up.oldgenid),
                        sb);
        if (rc != sizeof(msg->up.oldgenid))
            return -1;
        msg->up.oldgenid = flibc_htonll(msg->up.oldgenid);

        rc = sbuf2fread((char *)&msg->up.genid, 1, sizeof(msg->up.genid), sb);
        if (rc != sizeof(msg->up.genid))
            return -1;
        msg->up.genid = flibc_htonll(msg->up.genid);

        if (recv_dk) {
            rc = sbuf2fread((char *)&msg->up.ins_keys, 1,
                            sizeof(msg->up.ins_keys), sb);
            if (rc != sizeof(msg->up.ins_keys))
                return -1;
            msg->up.ins_keys = flibc_htonll(msg->up.ins_keys);

            rc = sbuf2fread((char *)&msg->up.del_keys, 1,
                            sizeof(msg->up.del_keys), sb);
            if (rc != sizeof(msg->up.del_keys))
                return -1;
            msg->up.del_keys = flibc_htonll(msg->up.del_keys);
        } else {
            msg->up.ins_keys = -1ULL;
            msg->up.del_keys = -1ULL;
        }

        rc = sbuf2fread((char *)&msg->up.datalen, 1, sizeof(msg->up.datalen),
                        sb);
        if (rc != sizeof(msg->up.datalen))
            return -1;
        msg->up.datalen = ntohl(msg->up.datalen);

        rc = sbuf2fread((char *)&msg->up.seq, 1, sizeof(msg->up.seq), sb);
        if (rc != sizeof(msg->up.seq))
            return -1;
        msg->up.seq = ntohl(msg->up.seq);

        if (msg->up.datalen > 0) {
            msg->up.data = (char *)malloc(msg->up.datalen);
            if (!msg->up.data)
                return -1;

            rc = sbuf2fread(msg->up.data, 1, msg->up.datalen, sb);
            if (rc != msg->up.datalen)
                return -1;
        } else {
            msg->up.data = NULL;
        }

        break;

    case FDB_MSG_INDEX:
        rc = sbuf2fread((char *)msg->ix.cid, 1, idsz, sb);
        if (rc != idsz)
            return -1;

        rc = sbuf2fread((char *)&msg->ix.version, 1, sizeof(msg->ix.version),
                        sb);
        if (rc != sizeof(msg->ix.version))
            return -1;
        msg->ix.version = ntohl(msg->ix.version);

        rc = sbuf2fread((char *)&msg->ix.rootpage, 1, sizeof(msg->ix.rootpage),
                        sb);
        if (rc != sizeof(msg->ix.rootpage))
            return -1;
        msg->ix.rootpage = ntohl(msg->ix.rootpage);

        rc = sbuf2fread((char *)&msg->ix.genid, 1, sizeof(msg->ix.genid), sb);
        if (rc != sizeof(msg->ix.genid))
            return -1;
        msg->ix.genid = flibc_htonll(msg->ix.genid);

        rc = sbuf2fread((char *)&msg->ix.is_delete, 1,
                        sizeof(msg->ix.is_delete), sb);
        if (rc != sizeof(msg->ix.is_delete))
            return -1;
        msg->ix.is_delete = ntohl(msg->ix.is_delete);

        rc = sbuf2fread((char *)&msg->ix.ixnum, 1, sizeof(msg->ix.ixnum), sb);
        if (rc != sizeof(msg->ix.ixnum))
            return -1;
        msg->ix.ixnum = ntohl(msg->ix.ixnum);

        rc = sbuf2fread((char *)&msg->ix.ixlen, 1, sizeof(msg->ix.ixlen), sb);
        if (rc != sizeof(msg->ix.ixlen))
            return -1;
        msg->ix.ixlen = ntohl(msg->ix.ixlen);

        rc = sbuf2fread((char *)&msg->ix.seq, 1, sizeof(msg->ix.seq), sb);
        if (rc != sizeof(msg->ix.seq))
            return -1;
        msg->ix.seq = ntohl(msg->ix.seq);

        if (msg->ix.ixlen > 0) {
            msg->ix.ix = (char *)malloc(msg->ix.ixlen);
            if (!msg->ix.ix)
                return -1;

            rc = sbuf2fread(msg->ix.ix, 1, msg->ix.ixlen, sb);
            if (rc != msg->ix.ixlen)
                return -1;
        } else {
            msg->ix.ix = NULL;
        }

        break;

    case FDB_MSG_HBEAT:
        rc = sbuf2fread((char *)&msg->hb.tid, 1, idsz, sb);
        if (rc != idsz)
            return -1;

        rc = sbuf2fread((char *)&lltmp, 1, sizeof(lltmp), sb);
        if (rc != sizeof(lltmp))
            return -1;
        msg->hb.timespec.tv_sec = ntohl(lltmp);

        rc = sbuf2fread((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return -1;
        msg->hb.timespec.tv_nsec = ntohl(tmp);

        break;

    default:
        logmsg(LOGMSG_ERROR, "%s: unknown msg %d\n", __func__, msg->hd.type);
        return -1;
    }

    /*fprintf(stderr, "%d XYXY ALL DONE calling sbuf2fread %llu\n", __LINE__,
     * osql_log_time());*/
    return 0;
}

static const char *__req_2_str(int req_type)
{
    req_type &= FD_MSG_TYPE;
    if (req_type == FDB_MSG_TRAN_BEGIN)
        return "TRAN_BEGIN";
    if (req_type == FDB_MSG_TRAN_PREPARE)
        return "TRAN_PREPARE";
    if (req_type == FDB_MSG_TRAN_COMMIT)
        return "TRAN_COMMIT";
    if (req_type == FDB_MSG_TRAN_ROLLBACK)
        return "TRAN_ROLLBACK";
    if (req_type == FDB_MSG_TRAN_RC)
        return "TRAN_RC";
    return "???";
}

static const char *__tran_2_str(enum transaction_level lvl)
{
    if (lvl == TRANLEVEL_SOSQL)
        return "SOSQL";
    if (lvl == TRANLEVEL_RECOM)
        return "RECOM";
    if (lvl == TRANLEVEL_SNAPISOL)
        return "SNAPISOL";
    return "???";
}

static void fdb_msg_print_message_uuid(SBUF2 *sb, fdb_msg_t *msg, char *prefix)
{
    int rc;
    int i;
    unsigned long long t = osql_log_time();
    uuidstr_t cus;
    uuidstr_t tus;

    if (!prefix)
        prefix = "";

    switch (msg->hd.type & FD_MSG_TYPE) {
    case FDB_MSG_TRAN_BEGIN:
    case FDB_MSG_TRAN_PREPARE:
    case FDB_MSG_TRAN_COMMIT:
    case FDB_MSG_TRAN_ROLLBACK:

        logmsg(LOGMSG_USER, "XXXX: %s %s tid=%s fl=%x lvl=%s\n", prefix,
                __req_2_str(msg->hd.type), comdb2uuidstr(msg->tr.tid, tus),
                msg->tr.flags, __tran_2_str(msg->tr.lvl));
        break;

    case FDB_MSG_TRAN_RC:

        logmsg(LOGMSG_USER, "XXXX: %s TRAN_RC tid=%s rc=%d %s\n", prefix,
                comdb2uuidstr(msg->rc.tid, tus), msg->rc.rc,
                (msg->rc.errstrlen > 0 && msg->rc.errstr) ? msg->rc.errstr
                                                          : "");
        break;

    case FDB_MSG_CURSOR_OPEN:
        if (msg->co.flags == FDB_MSG_CURSOR_OPEN_SQL) {
            logmsg(LOGMSG_USER, 
                    "XXXX: %llu %s sb=%p CURSOR_OPEN cid=%s tid=%s fl=%x "
                    "rootpage=%d version=%d\n",
                    t, prefix, sb, comdb2uuidstr(msg->co.cid, cus),
                    comdb2uuidstr(msg->co.tid, tus), msg->co.flags,
                    msg->co.rootpage, msg->co.version);
        } else if (msg->co.flags == FDB_MSG_CURSOR_OPEN_SQL_TRAN) {
            logmsg(LOGMSG_USER, 
                    "XXXX: %llu %s sb=%p CURSOR_OPEN cid=%s tid=%s fl=%x "
                    "rootpage=%d version=%d seq=%d\n",
                    t, prefix, sb, comdb2uuidstr(msg->co.cid, cus),
                    comdb2uuidstr(msg->co.tid, tus), msg->co.flags,
                    msg->co.rootpage, msg->co.version, msg->co.seq);
        } else {
            logmsg(LOGMSG_USER, 
                    "XXXX: %llu %s sb=%p CURSOR_OPEN cid=%s tid=%s fl=%x "
                    "rootpage=%d version=%d seq=%d SRC[%d, %s]\n",
                t, prefix, sb, comdb2uuidstr(msg->co.cid, cus),
                comdb2uuidstr(msg->co.tid, tus), msg->co.flags,
                msg->co.rootpage, msg->co.version, msg->co.seq, msg->co.srcpid,
                (msg->co.srcname) ? msg->co.srcname : "(unknown)");
        }

        break;

    case FDB_MSG_CURSOR_CLOSE:

        logmsg(LOGMSG_USER, "XXXX: %llu %s CURSOR_CLOSE cid=%s\n", t, prefix,
                comdb2uuidstr(msg->cc.cid, cus));
        break;

    case FDB_MSG_CURSOR_FIRST:

        logmsg(LOGMSG_USER, "XXXX: %llu %s CURSOR_FIRST cid=%s\n", t, prefix,
                comdb2uuidstr(msg->cc.cid, cus));
        break;

    case FDB_MSG_CURSOR_LAST:

        logmsg(LOGMSG_USER, "XXXX: %llu %s CURSOR_LAST cid=%s\n", t, prefix,
                comdb2uuidstr(msg->cc.cid, cus));
        break;

    case FDB_MSG_CURSOR_NEXT:

        logmsg(LOGMSG_USER, "XXXX: %llu %s CURSOR_NEXT cid=%s\n", t, prefix,
                comdb2uuidstr(msg->cc.cid, cus));
        break;

    case FDB_MSG_CURSOR_PREV:

        logmsg(LOGMSG_USER, "XXXX: %llu %s CURSOR_PREV cid=%s\n", t, prefix,
                comdb2uuidstr(msg->cc.cid, cus));
        break;

    case FDB_MSG_DATA_ROW:

        logmsg(LOGMSG_USER, "XXXX: %llu %s DATA_ROW cid=%s rc=%d genid=%llx "
                        "datalen=%d datacpylen=%d\n",
                t, prefix, comdb2uuidstr(msg->dr.cid, cus), msg->dr.rc,
                msg->dr.genid, msg->dr.datalen, msg->dr.datacopylen);
        break;

    case FDB_MSG_CURSOR_FIND:
    case FDB_MSG_CURSOR_FIND_LAST:

        logmsg(LOGMSG_USER, "XXXX: %llu %s %s cid=%s key[%d]=\"", t, prefix,
                (msg->hd.type == FDB_MSG_CURSOR_FIND) ? "CURSOR_FIND"
                                                      : "CURSOR_FIND_LAST",
                comdb2uuidstr(msg->cf.cid, cus), msg->cf.keylen);
        fsnapf(stderr, msg->cf.key, msg->cf.keylen);
        logmsg(LOGMSG_USER, "\"\n");

        break;

    case FDB_MSG_RUN_SQL:

        logmsg(LOGMSG_USER, "XXXX: %llu %s RUN_SQL cid=%s version=%d flags=%x "
                        "sqllen=%d sql=\"%s\" trim=\"%s\"\n",
                t, prefix, comdb2uuidstr(msg->sq.cid, cus), msg->sq.version,
                msg->sq.flags, msg->sq.sqllen, msg->sq.sql,
                (msg->sq.flags == FDB_RUN_SQL_TRIM) ? "yes" : "no");
        break;

    case FDB_MSG_INSERT_PI:
    case FDB_MSG_INSERT:

        logmsg(LOGMSG_USER, "XXXX: %llu %s INSERT cid=%s rootp=%d version=%d "
                        "genid=%llx datalen=%d\n",
                t, prefix, comdb2uuidstr(msg->in.cid, cus), msg->in.rootpage,
                msg->in.version, msg->in.genid, msg->in.datalen);
        break;

    case FDB_MSG_DELETE_PI:
    case FDB_MSG_DELETE:

        logmsg(LOGMSG_USER, 
                "XXXX: %llu %s DELETE cid=%s rootp=%d version=%d genid=%llx\n",
                t, prefix, comdb2uuidstr(msg->de.cid, cus), msg->de.rootpage,
                msg->de.version, msg->de.genid);

        break;

    case FDB_MSG_UPDATE_PI:
    case FDB_MSG_UPDATE:

        logmsg(LOGMSG_USER, "XXXX: %llu %s UPDATE cid=%s rootp=%d version=%d "
                        "oldgenid=%llx genid=%llx datalen=%d seq %d\n",
                t, prefix, comdb2uuidstr(msg->up.cid, cus), msg->up.rootpage,
                msg->up.version, msg->up.oldgenid, msg->up.genid,
                msg->up.datalen, msg->up.seq);
        break;

    case FDB_MSG_INDEX:

        logmsg(LOGMSG_USER, "XXXX: %llu %s INDEX cid=%s rootp=%d version=%d "
                        "genid=%llx is_delete=%d, ixnum=%d, ixlen=%d\n",
                t, prefix, comdb2uuidstr(msg->ix.cid, cus), msg->ix.rootpage,
                msg->ix.version, msg->ix.genid, msg->ix.is_delete,
                msg->ix.ixnum, msg->ix.ixlen);
        break;

    case FDB_MSG_HBEAT:
        break;

    default:
        logmsg(LOGMSG_USER, "%s: %s unknown msg %d\n", __func__, __func__, prefix,
                msg->hd.type);
    }
}

static void fdb_msg_print_message(SBUF2 *sb, fdb_msg_t *msg, char *prefix)
{
    int rc;
    int i;
    unsigned long long t = osql_log_time();
    int isuuid;
    char prf[512];

    snprintf(prf, sizeof(prf), "%llu%s%s", (unsigned long long) gettimeofday_ms(),
             (prefix) ? " " : "", (prefix) ? prefix : "");
    prefix = prf;

    isuuid = msg->hd.type & FD_MSG_FLAGS_ISUUID;

    if (isuuid) {
        fdb_msg_print_message_uuid(sb, msg, prefix);
        return;
    }

    switch (msg->hd.type & FD_MSG_TYPE) {
    case FDB_MSG_TRAN_BEGIN:
    case FDB_MSG_TRAN_PREPARE:
    case FDB_MSG_TRAN_COMMIT:
    case FDB_MSG_TRAN_ROLLBACK:

        logmsg(LOGMSG_USER, "XXXX: %s %s tid=%llx fl=%x lvl=%s\n", prefix,
                __req_2_str(msg->hd.type), *(unsigned long long *)msg->tr.tid,
                msg->tr.flags, __tran_2_str(msg->tr.lvl));
        break;

    case FDB_MSG_TRAN_RC:

        logmsg(LOGMSG_USER, "XXXX: %s TRAN_RC tid=%llx rc=%d %s\n", prefix,
                *(unsigned long long *)msg->rc.tid, msg->rc.rc,
                (msg->rc.errstrlen > 0 && msg->rc.errstr) ? msg->rc.errstr
                                                          : "");
        break;

    case FDB_MSG_CURSOR_OPEN:

        if (msg->co.flags == FDB_MSG_CURSOR_OPEN_SQL) {
            logmsg(LOGMSG_USER, 
                    "XXXX: %llu %s sb=%p CURSOR_OPEN cid=%llx tid=%llx fl=%x "
                    "rootpage=%d version=%d\n",
                    t, prefix, sb, *(unsigned long long *)msg->co.cid,
                    *(unsigned long long *)msg->co.tid, msg->co.flags,
                    msg->co.rootpage, msg->co.version);
        } else if (msg->co.flags == FDB_MSG_CURSOR_OPEN_SQL_TRAN) {
            logmsg(LOGMSG_USER, 
                    "XXXX: %llu %s sb=%p CURSOR_OPEN cid=%llx tid=%llx fl=%x "
                    "rootpage=%d version=%d seq=%d\n",
                    t, prefix, sb, *(unsigned long long *)msg->co.cid,
                    *(unsigned long long *)msg->co.tid, msg->co.flags,
                    msg->co.rootpage, msg->co.version, msg->co.seq);
        } else {
            logmsg(LOGMSG_USER, 
                    "XXXX: %llu %s sb=%p CURSOR_OPEN cid=%llx tid=%llx fl=%x "
                    "rootpage=%d version=%d seq=%d SRC[%d, %s]\n",
                    t, prefix, sb, *(unsigned long long *)msg->co.cid,
                    *(unsigned long long *)msg->co.tid, msg->co.flags,
                    msg->co.rootpage, msg->co.version, msg->co.seq,
                    msg->co.srcpid,
                    (msg->co.srcname) ? msg->co.srcname : "(unknown)");
        }

        break;

    case FDB_MSG_CURSOR_CLOSE:

        logmsg(LOGMSG_USER, "XXXX: %llu %s CURSOR_CLOSE cid=%llx\n", t, prefix,
                *(unsigned long long *)msg->cc.cid);
        break;

    case FDB_MSG_CURSOR_FIRST:

        logmsg(LOGMSG_USER, "XXXX: %llu %s CURSOR_FIRST cid=%llx\n", t, prefix,
                *(unsigned long long *)msg->cc.cid);
        break;

    case FDB_MSG_CURSOR_LAST:

        logmsg(LOGMSG_USER, "XXXX: %llu %s CURSOR_LAST cid=%llx\n", t, prefix,
                *(unsigned long long *)msg->cc.cid);
        break;

    case FDB_MSG_CURSOR_NEXT:

        logmsg(LOGMSG_USER, "XXXX: %llu %s CURSOR_NEXT cid=%llx\n", t, prefix,
                *(unsigned long long *)msg->cc.cid);
        break;

    case FDB_MSG_CURSOR_PREV:

        logmsg(LOGMSG_USER, "XXXX: %llu %s CURSOR_PREV cid=%llx\n", t, prefix,
                *(unsigned long long *)msg->cc.cid);
        break;

    case FDB_MSG_DATA_ROW:

        logmsg(LOGMSG_USER, "XXXX: %llu %s DATA_ROW cid=%llx rc=%d genid=%llx "
                        "datalen=%d datacpylen=%d\n",
                t, prefix, *(unsigned long long *)msg->dr.cid, msg->dr.rc,
                msg->dr.genid, msg->dr.datalen, msg->dr.datacopylen);
        break;

    case FDB_MSG_CURSOR_FIND:
    case FDB_MSG_CURSOR_FIND_LAST:

        logmsg(LOGMSG_USER, "XXXX: %llu %s %s cid=%llx key[%d]=\"", t, prefix,
                (msg->hd.type == FDB_MSG_CURSOR_FIND) ? "CURSOR_FIND"
                                                      : "CURSOR_FIND_LAST",
                *(unsigned long long *)msg->cf.cid, msg->cf.keylen);
        fsnapf(stderr, msg->cf.key, msg->cf.keylen);
        logmsg(LOGMSG_USER, "\"\n");

        break;

    case FDB_MSG_RUN_SQL:

        logmsg(LOGMSG_USER, "XXXX: %llu %s RUN_SQL cid=%llx version=%d flags=%x "
                        "sqllen=%d sql=\"%s\" trim=\"%s\"\n",
                t, prefix, *(unsigned long long *)msg->sq.cid, msg->sq.version,
                msg->sq.flags, msg->sq.sqllen, msg->sq.sql,
                (msg->sq.flags == FDB_RUN_SQL_TRIM) ? "yes" : "no");
        break;

    case FDB_MSG_INSERT_PI:
    case FDB_MSG_INSERT:

        logmsg(LOGMSG_USER, "XXXX: %llu %s INSERT cid=%llx rootp=%d version=%d "
                        "genid=%llx datalen=%d\n",
                t, prefix, *(unsigned long long *)msg->in.cid, msg->in.rootpage,
                msg->in.version, msg->in.genid, msg->in.datalen);
        break;

    case FDB_MSG_DELETE_PI:
    case FDB_MSG_DELETE:

        logmsg(LOGMSG_USER, 
                "XXXX: %llu %s DELETE cid=%llx rootp=%d version=%d genid=%llx\n", t,
                prefix, *(unsigned long long *)msg->de.cid, msg->de.rootpage,
                msg->de.version, msg->de.genid);

        break;

    case FDB_MSG_UPDATE_PI:
    case FDB_MSG_UPDATE:

        logmsg(LOGMSG_USER, "XXXX: %llu %s sb=%p UPDATE cid=%llx rootp=%d "
                        "version=%d oldgenid=%llx genid=%llx datalen=%d "
                        "seq=%d\n",
                t, prefix, sb, *(unsigned long long *)msg->up.cid,
                msg->up.rootpage, msg->up.version, msg->up.oldgenid,
                msg->up.genid, msg->up.datalen, msg->up.seq);
        break;

    case FDB_MSG_INDEX:

        logmsg(LOGMSG_USER, "XXXX: %llu %s INDEX cid=%llx rootp=%d version=%d "
                        "genid=%llx is_delete=%d, ixnum=%d, ixlen=%d\n",
                t, prefix, *(unsigned long long *)msg->ix.cid, msg->ix.rootpage,
                msg->ix.version, msg->ix.genid, msg->ix.is_delete,
                msg->ix.ixnum, msg->ix.ixlen);
        fsnapf(stderr, msg->ix.ix, msg->ix.ixlen);
        break;

    case FDB_MSG_HBEAT:
        logmsg(LOGMSG_USER, 
                "XXXX: %llu %s sb=%p HBEAT tid=%llx tv_sec=%lu tv_nsec=%u\n", t,
                prefix, sb, *(unsigned long long *)msg->hb.tid,
                msg->hb.timespec.tv_sec, msg->hb.timespec.tv_nsec);
        break;

    default:
        logmsg(LOGMSG_ERROR, "%s: %s unknown msg %d\n", __func__, __func__, prefix,
                msg->hd.type);
    }
}

/* stuff goes as network endian */
static int fdb_msg_write_message(SBUF2 *sb, fdb_msg_t *msg, int flush)
{
    int type;
    int tmp;
    unsigned long long lltmp;
    int rc;
    int idsz;
    int send_dk;

    // printf("<<< type %d %s isuuid %d\n", msg->hd.type & FD_MSG_TYPE,
    // fdb_msg_type(msg->hd.type & FD_MSG_TYPE), msg->hd.type &
    // FD_MSG_FLAGS_ISUUID);

    type = htonl(msg->hd.type);

    rc = sbuf2fwrite((char *)&type, 1, sizeof(type), sb);

    if (rc != sizeof(type)) {
        logmsg(LOGMSG_ERROR, "%s: failed to write header rc=%d\n", __func__, rc);
        return FDB_ERR_WRITE_IO;
    }

    if (msg->hd.type & FD_MSG_FLAGS_ISUUID)
        idsz = sizeof(uuid_t);
    else
        idsz = sizeof(unsigned long long);

    send_dk = 0;
    switch (msg->hd.type & FD_MSG_TYPE) {
    case FDB_MSG_TRAN_BEGIN:
    case FDB_MSG_TRAN_PREPARE:
    case FDB_MSG_TRAN_COMMIT:
    case FDB_MSG_TRAN_ROLLBACK:

        rc = sbuf2fwrite((char *)msg->tr.tid, 1, idsz, sb);
        if (rc != idsz)
            return FDB_ERR_WRITE_IO;

        tmp = htonl(msg->tr.lvl);
        rc = sbuf2fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        tmp = htonl(msg->tr.flags);
        rc = sbuf2fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        tmp = htonl(msg->tr.seq);
        rc = sbuf2fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        break;

    case FDB_MSG_TRAN_RC:

        rc = sbuf2fwrite((char *)msg->rc.tid, 1, idsz, sb);
        if (rc != idsz)
            return FDB_ERR_WRITE_IO;

        tmp = htonl(msg->rc.rc);
        rc = sbuf2fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        tmp = htonl(msg->rc.errstrlen);
        rc = sbuf2fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        if (msg->rc.errstrlen && msg->rc.errstr) {
            rc = sbuf2fwrite(msg->rc.errstr, 1, msg->rc.errstrlen, sb);
            if (rc != msg->rc.errstrlen)
                return FDB_ERR_WRITE_IO;
        }

        break;

    case FDB_MSG_CURSOR_OPEN:

        rc = sbuf2fwrite((char *)msg->co.cid, 1, idsz, sb);
        if (rc != idsz)
            return FDB_ERR_WRITE_IO;

        rc = sbuf2fwrite((char *)msg->co.tid, 1, idsz, sb);
        if (rc != idsz)
            return FDB_ERR_WRITE_IO;

        /* we don't wanna impact existing infrastructure, so for now hide
           transaction cursors as untransactional and rely on tid to make a
           difference */
        tmp = htonl(msg->co.flags);
        /*
        (msg->co.flags == FDB_MSG_CURSOR_OPEN_SQL_TRAN)?
           htonl(FDB_MSG_CURSOR_OPEN_SQL):
           htonl(msg->co.flags);
           */
        rc = sbuf2fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        tmp = htonl(msg->co.rootpage);
        rc = sbuf2fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        tmp = htonl(msg->co.version);
        rc = sbuf2fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        if (msg->co.flags == FDB_MSG_CURSOR_OPEN_SQL)
            break;

        /* always send the seq */
        tmp = htonl(msg->co.seq);
        rc = sbuf2fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        if (msg->co.flags == FDB_MSG_CURSOR_OPEN_SQL_TRAN)
            break;

        tmp = htonl(msg->co.srcpid);
        rc = sbuf2fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        tmp = htonl(msg->co.srcnamelen);
        rc = sbuf2fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        if (msg->co.srcname && msg->co.srcnamelen > 0) {
            rc = sbuf2fwrite(msg->co.srcname, 1, msg->co.srcnamelen, sb);
            if (rc != msg->co.srcnamelen)
                return FDB_ERR_WRITE_IO;
        }

        break;

    case FDB_MSG_CURSOR_CLOSE: {
        rc = sbuf2fwrite(msg->cc.cid, 1, idsz, sb);
        if (rc != idsz)
            return FDB_ERR_WRITE_IO;

        int haveid;

        if (msg->hd.type & FD_MSG_FLAGS_ISUUID)
            haveid = !comdb2uuid_is_zero(msg->co.tid);
        else
            haveid = *(unsigned long long *)msg->co.tid != 0;

        if (haveid) {
            tmp = htonl(msg->cc.seq);
            rc = sbuf2fwrite((char *)&tmp, 1, sizeof(tmp), sb);
            if (rc != sizeof(tmp))
                return FDB_ERR_WRITE_IO;
        }

        break;
    }

    case FDB_MSG_CURSOR_FIRST:
    case FDB_MSG_CURSOR_LAST:
    case FDB_MSG_CURSOR_NEXT:
    case FDB_MSG_CURSOR_PREV:

        rc = sbuf2fwrite(msg->cm.cid, 1, idsz, sb);
        if (rc != idsz)
            return FDB_ERR_WRITE_IO;

        break;

    case FDB_MSG_DATA_ROW:

        rc = sbuf2fwrite(msg->dr.cid, 1, idsz, sb);
        if (rc != idsz)
            return FDB_ERR_WRITE_IO;

        tmp = htonl(msg->dr.rc);
        rc = sbuf2fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        lltmp = flibc_htonll(msg->dr.genid);
        rc = sbuf2fwrite((char *)&lltmp, 1, sizeof(lltmp), sb);
        if (rc != sizeof(lltmp))
            return FDB_ERR_WRITE_IO;

        if (unlikely(msg->dr.datacopylen != 0))
            abort();
        tmp = (((unsigned)msg->dr.datalen) << 16) +
              (((unsigned)msg->dr.datalen) >> 16);
        tmp = htonl(tmp);
        rc = sbuf2fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        if (msg->dr.data && msg->dr.datalen > 0) {
            rc = sbuf2fwrite(msg->dr.data, 1, msg->dr.datalen, sb);
            if (rc != msg->dr.datalen)
                return FDB_ERR_WRITE_IO;
        }
        break;

    case FDB_MSG_CURSOR_FIND:
    case FDB_MSG_CURSOR_FIND_LAST:

        rc = sbuf2fwrite(msg->cf.cid, 1, idsz, sb);
        if (rc != idsz)
            return FDB_ERR_WRITE_IO;

        tmp = htonl(msg->cf.keylen);
        rc = sbuf2fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        assert(msg->cf.keylen > 0); /* TODO: 0 for match any ? */
        rc = sbuf2fwrite(msg->cf.key, 1, msg->cf.keylen, sb);
        if (rc != msg->cf.keylen)
            return FDB_ERR_WRITE_IO;

        break;

    case FDB_MSG_RUN_SQL:
        rc = sbuf2fwrite(msg->sq.cid, 1, idsz, sb);
        if (rc != idsz)
            return FDB_ERR_WRITE_IO;
        /*
        rc = sbuf2flush(sb);
        if (rc<=0)
        {
           fprintf(stderr, "Ugh %d?\n", __LINE__);
           return FDB_ERR_WRITE_IO;
        }
        */

        tmp = htonl(msg->sq.version);
        rc = sbuf2fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;
        /*
     rc = sbuf2flush(sb);
     if (rc<=0)
     {
        fprintf(stderr, "Ugh %d?\n", __LINE__);
        return FDB_ERR_WRITE_IO;
     }
     */

        tmp = htonl(msg->sq.flags);
        rc = sbuf2fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;
        /*
     rc = sbuf2flush(sb);
     if (rc<=0)
     {
        fprintf(stderr, "Ugh %d?\n", __LINE__);
        return FDB_ERR_WRITE_IO;
     }
     */

        tmp = htonl(msg->sq.sqllen);
        rc = sbuf2fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;
        /*
     rc = sbuf2flush(sb);
     if (rc<=0)
     {
        fprintf(stderr, "Ugh %d?\n", __LINE__);
        return FDB_ERR_WRITE_IO;
     }
     */

        rc = sbuf2fwrite(msg->sq.sql, 1, msg->sq.sqllen, sb);
        if (rc != msg->sq.sqllen)
            return FDB_ERR_WRITE_IO;
        /*
     rc = sbuf2flush(sb);
     if (rc<=0)
     {
        fprintf(stderr, "Ugh %d?\n", __LINE__);
        return FDB_ERR_WRITE_IO;
     }
     */

        if (msg->sq.flags == FDB_RUN_SQL_TRIM) {
            tmp = htonl(msg->sq.keylen);
            rc = sbuf2fwrite((char *)&tmp, 1, sizeof(tmp), sb);
            if (rc != sizeof(tmp))
                return FDB_ERR_WRITE_IO;
            /*
         rc = sbuf2flush(sb);
         if (rc<=0)
         {
            fprintf(stderr, "Ugh %d?\n", __LINE__);
            return FDB_ERR_WRITE_IO;
         }
         */

            rc = sbuf2fwrite(msg->sq.key, 1, msg->sq.keylen, sb);
            if (rc != msg->sq.keylen)
                return FDB_ERR_WRITE_IO;
        }

        break;

    case FDB_MSG_INSERT_PI:
        send_dk = 1;

    case FDB_MSG_INSERT:

        rc = sbuf2fwrite(msg->in.cid, 1, idsz, sb);
        if (rc != idsz)
            return FDB_ERR_WRITE_IO;

        tmp = htonl(msg->in.version);
        rc = sbuf2fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        tmp = htonl(msg->in.rootpage);
        rc = sbuf2fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        lltmp = flibc_htonll(msg->in.genid);
        rc = sbuf2fwrite((char *)&lltmp, 1, sizeof(lltmp), sb);
        if (rc != sizeof(lltmp))
            return FDB_ERR_WRITE_IO;

        if (send_dk) {
            lltmp = flibc_htonll(msg->in.ins_keys);
            rc = sbuf2fwrite((char *)&lltmp, 1, sizeof(lltmp), sb);
            if (rc != sizeof(lltmp))
                return FDB_ERR_WRITE_IO;
        }

        if (unlikely(msg->in.datalen > 0 && msg->in.data == NULL)) {
            abort();
        }

        tmp = msg->in.datalen;
        tmp = htonl(tmp);
        rc = sbuf2fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        tmp = msg->in.seq;
        tmp = htonl(tmp);
        rc = sbuf2fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        if (msg->in.data && msg->in.datalen > 0) {
            rc = sbuf2fwrite(msg->in.data, 1, msg->in.datalen, sb);
            if (rc != msg->in.datalen)
                return FDB_ERR_WRITE_IO;
        }

        break;

    case FDB_MSG_DELETE_PI:
        send_dk = 1;

    case FDB_MSG_DELETE:

        rc = sbuf2fwrite(msg->de.cid, 1, idsz, sb);
        if (rc != idsz)
            return FDB_ERR_WRITE_IO;

        tmp = htonl(msg->de.version);
        rc = sbuf2fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        tmp = htonl(msg->de.rootpage);
        rc = sbuf2fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        lltmp = flibc_htonll(msg->de.genid);
        rc = sbuf2fwrite((char *)&lltmp, 1, sizeof(lltmp), sb);
        if (rc != sizeof(lltmp))
            return FDB_ERR_WRITE_IO;

        if (send_dk) {
            lltmp = flibc_htonll(msg->de.del_keys);
            rc = sbuf2fwrite((char *)&lltmp, 1, sizeof(lltmp), sb);
            if (rc != sizeof(lltmp))
                return FDB_ERR_WRITE_IO;
        }

        tmp = htonl(msg->de.seq);
        rc = sbuf2fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        break;

    case FDB_MSG_UPDATE_PI:
        send_dk = 1;

    case FDB_MSG_UPDATE:

        rc = sbuf2fwrite(msg->up.cid, 1, idsz, sb);
        if (rc != idsz)
            return FDB_ERR_WRITE_IO;

        tmp = htonl(msg->up.version);
        rc = sbuf2fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        tmp = htonl(msg->up.rootpage);
        rc = sbuf2fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        lltmp = flibc_htonll(msg->up.oldgenid);
        rc = sbuf2fwrite((char *)&lltmp, 1, sizeof(lltmp), sb);
        if (rc != sizeof(lltmp))
            return FDB_ERR_WRITE_IO;

        lltmp = flibc_htonll(msg->up.genid);
        rc = sbuf2fwrite((char *)&lltmp, 1, sizeof(lltmp), sb);
        if (rc != sizeof(lltmp))
            return FDB_ERR_WRITE_IO;

        if (send_dk) {
            lltmp = flibc_htonll(msg->up.ins_keys);
            rc = sbuf2fwrite((char *)&lltmp, 1, sizeof(lltmp), sb);
            if (rc != sizeof(lltmp))
                return FDB_ERR_WRITE_IO;

            lltmp = flibc_htonll(msg->up.del_keys);
            rc = sbuf2fwrite((char *)&lltmp, 1, sizeof(lltmp), sb);
            if (rc != sizeof(lltmp))
                return FDB_ERR_WRITE_IO;
        }

        if (unlikely(msg->up.datalen > 0 && msg->up.data == NULL)) {
            abort();
        }

        tmp = msg->up.datalen;
        tmp = htonl(tmp);
        rc = sbuf2fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        tmp = htonl(msg->up.seq);
        rc = sbuf2fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        if (msg->up.data && msg->up.datalen > 0) {
            rc = sbuf2fwrite(msg->up.data, 1, msg->up.datalen, sb);
            if (rc != msg->up.datalen)
                return FDB_ERR_WRITE_IO;
        }

        break;

    case FDB_MSG_INDEX:

        rc = sbuf2fwrite(msg->ix.cid, 1, idsz, sb);
        if (rc != idsz)
            return FDB_ERR_WRITE_IO;

        tmp = htonl(msg->ix.version);
        rc = sbuf2fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        tmp = htonl(msg->ix.rootpage);
        rc = sbuf2fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        lltmp = flibc_htonll(msg->ix.genid);
        rc = sbuf2fwrite((char *)&lltmp, 1, sizeof(lltmp), sb);
        if (rc != sizeof(lltmp))
            return FDB_ERR_WRITE_IO;

        tmp = htonl(msg->ix.is_delete);
        rc = sbuf2fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        tmp = htonl(msg->ix.ixnum);
        rc = sbuf2fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        if (unlikely(msg->ix.ixlen > 0 && msg->ix.ix == NULL)) {
            abort();
        }

        tmp = msg->ix.ixlen;
        tmp = htonl(tmp);
        rc = sbuf2fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        tmp = msg->ix.seq;
        tmp = htonl(tmp);
        rc = sbuf2fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        if (msg->ix.ix && msg->ix.ixlen > 0) {
            rc = sbuf2fwrite(msg->ix.ix, 1, msg->ix.ixlen, sb);
            if (rc != msg->ix.ixlen)
                return FDB_ERR_WRITE_IO;
        }

        break;

    case FDB_MSG_HBEAT: {
        rc = sbuf2fwrite(msg->hb.tid, 1, idsz, sb);
        if (rc != idsz)
            return FDB_ERR_WRITE_IO;

        lltmp = flibc_htonll(msg->hb.timespec.tv_sec);
        rc = sbuf2fwrite((char *)&lltmp, 1, sizeof(lltmp), sb);
        if (rc != sizeof(lltmp))
            return FDB_ERR_WRITE_IO;

        tmp = htonl(msg->hb.timespec.tv_nsec);
        rc = sbuf2fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;
    } break;

    default:
        logmsg(LOGMSG_ERROR, "%s: unknown msg %d\n", __func__, type);
        return FDB_ERR_UNSUPPORTED;
    }

    unsigned long long t = osql_log_time();
    if (flush /*&& (msg->hd.type != FDB_MSG_RUN_SQL)*/) {
        /*
        fprintf(stderr, "Flushing %llu\n", t);
        */
        rc = sbuf2flush(sb);
        if (rc <= 0) {
            /*
               fprintf(stderr, "Ugh?\n");
            */
            return FDB_ERR_WRITE_IO;
        }
    }

    t = osql_log_time();
    /*fprintf(stderr, "Done %s %llu\n", __func__, t);*/
    return 0;
}

static void _fdb_extract_source_id(struct sqlclntstate *clnt, SBUF2 *sb,
                                   fdb_msg_t *msg)
{
    clnt->conninfo.node = -1; /*get_origin_mach_by_fd(sbuf2fileno(sb));*/

    if (msg->co.flags == FDB_MSG_CURSOR_OPEN_SQL_SID) {
        /* extract source */
        if (msg->co.srcname)
            strncpy(clnt->conninfo.pename, msg->co.srcname,
                    sizeof(clnt->conninfo.pename));
        else
            strncpy(clnt->conninfo.pename, "UNKNOWN",
                    sizeof(clnt->conninfo.pename));
        clnt->conninfo.pename[sizeof(clnt->conninfo.pename) - 1] = '\0';
        clnt->conninfo.pid = msg->co.srcpid;
    } else {
        strncpy(clnt->conninfo.pename, "UNKNOWN",
                sizeof(clnt->conninfo.pename));
        clnt->conninfo.pename[sizeof(clnt->conninfo.pename) - 1] = '\0';
    }
}

int fdb_remcur_cursor_open(SBUF2 *sb, fdb_msg_t *msg, svc_callback_arg_t *arg)
{
    char *cid = msg->co.cid;
    char *tid = msg->co.tid;
    int seq = msg->co.seq;
    struct sqlclntstate *clnt;

    assert(msg->hd.type == FDB_MSG_CURSOR_OPEN);

    /* create a cursor */
    if (!fdb_svc_cursor_open(tid, cid, msg->co.rootpage, msg->co.version,
                             msg->co.flags, seq, arg->isuuid, &clnt)) {
        logmsg(LOGMSG_ERROR, "%s: failed to open cursor\n", __func__);
        arg->clnt = NULL;
        return -1;
    }

    arg->clnt = clnt;

    if (gbl_expressions_indexes) {
        if (clnt->idxInsert || clnt->idxDelete) {
            free_cached_idx(clnt->idxInsert);
            free_cached_idx(clnt->idxDelete);
            free(clnt->idxInsert);
            free(clnt->idxDelete);
            clnt->idxInsert = clnt->idxDelete = NULL;
        }
        clnt->idxInsert = calloc(MAXINDEX, sizeof(uint8_t *));
        clnt->idxDelete = calloc(MAXINDEX, sizeof(uint8_t *));
        if (!clnt->idxInsert || !clnt->idxDelete) {
            logmsg(LOGMSG_ERROR, "%s:%d malloc failed\n", __func__, __LINE__);
            return -1;
        }
    }

    if (!clnt->conninfo.pename[0]) {
        _fdb_extract_source_id(clnt, sb, msg);
    }
    return 0;
}

int fdb_remcur_cursor_close(SBUF2 *sb, fdb_msg_t *msg, svc_callback_arg_t *arg)
{
    struct sqlclntstate *clnt = (arg) ? arg->clnt : NULL;
    char *cid = msg->cc.cid;
    char *tid = msg->cc.tid;
    int seq = msg->cc.seq;
    int rc;

    /* NOTE
       current implementation ignores seq number, since we don't sync on it
     */
    rc = fdb_svc_cursor_close(cid, arg->isuuid, (clnt) ? &clnt : NULL);
    if (rc < 0) {
        logmsg(LOGMSG_ERROR, "%s: failed to close cursor rc=%d\n", __func__, rc);
    }

    return rc;
}

int fdb_remcur_nop(SBUF2 *sb, fdb_msg_t *msg, svc_callback_arg_t *arg)
{
    abort();
}

static enum svc_move_types move_type(int type)
{
    switch (type) {
    case FDB_MSG_CURSOR_FIRST:
        return SVC_MOVE_FIRST;
    case FDB_MSG_CURSOR_NEXT:
        return SVC_MOVE_NEXT;
    case FDB_MSG_CURSOR_PREV:
        return SVC_MOVE_PREV;
    case FDB_MSG_CURSOR_LAST:
        return SVC_MOVE_LAST;
    }
    logmsg(LOGMSG_FATAL, "%s: unknown move\n", __func__, type);
    abort();
    return -1;
}

int fdb_remcur_send_row(SBUF2 *sb, fdb_msg_t *msg, char *cid,
                        unsigned long long genid, char *data, int datalen,
                        char *datacopy, int datacopylen, int ret, int isuuid)
{
    int rc;
    fdb_msg_t lcl_msg;

    if (msg != NULL)
        fdb_msg_clean_message(msg);
    else {
        msg = &lcl_msg;
        bzero(msg, sizeof(*msg));
        msg->hd.type = FDB_MSG_DATA_ROW | (isuuid ? FD_MSG_FLAGS_ISUUID : 0);
        fdb_msg_prepare_message(msg);
        memcpy(msg->dr.cid, cid,
               isuuid ? sizeof(uuid_t) : sizeof(unsigned long long));
    }

    msg->hd.type = FDB_MSG_DATA_ROW | (isuuid ? FD_MSG_FLAGS_ISUUID : 0);
    /* same cid */
    msg->dr.rc = ret;
    msg->dr.genid = genid;
    msg->dr.datalen = datalen;
    msg->dr.data = data;
    msg->dr.datacopylen = datacopylen;
    msg->dr.datacopy = datacopy;

    rc = fdb_msg_write_message(sb, msg, 1);

    if (gbl_fdb_track) {
        fdb_msg_print_message(sb, msg, "sending msg");
    }

    if (rc) {
        /* this happens natural for fractured streams */
        if (gbl_fdb_track)
            logmsg(LOGMSG_USER, "%s: failed send row back, rc=%d\n", __func__, rc);
    }

    return rc;
}

int fdb_remcur_cursor_move(SBUF2 *sb, fdb_msg_t *msg, svc_callback_arg_t *arg)
{
    struct db *db;
    char *cid = msg->cm.cid;
    unsigned long long genid;
    int rc = 0;
    char *data;
    char *datacopy;
    int datalen;
    int datacopylen;

    rc = fdb_svc_cursor_move(move_type(msg->hd.type), msg->cm.cid, &data,
                             &datalen, &genid, &datacopy, &datacopylen,
                             arg->isuuid);
    if (rc != IX_FND && rc != IX_NOTFND && rc != IX_PASTEOF && rc != IX_EMPTY) {
        logmsg(LOGMSG_ERROR, "%s: failed move rc %d\n", __func__, rc);
        /*TODO: notify the other side! */
        return rc;
    }

    rc = fdb_remcur_send_row(sb, msg, NULL, genid, data, datalen, datacopy,
                             datacopylen, rc, arg->isuuid);

    return rc;
}

int fdb_send_find(fdb_msg_t *msg, char *cid, int last, char *key, int keylen,
                  int isuuid, SBUF2 *sb)
{
    int rc;
    fdb_msg_clean_message(msg);

    msg->hd.type = (last) ? FDB_MSG_CURSOR_FIND_LAST : FDB_MSG_CURSOR_FIND;

    if (isuuid)
        msg->hd.type |= FD_MSG_FLAGS_ISUUID;

    msg->cf.cid = cid;
    msg->cf.keylen = keylen;
    msg->cf.key = key;

    rc = fdb_msg_write_message(sb, msg, 1);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: failed fdbc find%s rc=%d\n", __func__,
                (last) ? "_last" : "", rc);
        return rc;
    }

    if (gbl_fdb_track) {
        fdb_msg_print_message(sb, msg, "sending msg");
    }

    return rc;
}

int fdb_remcur_cursor_find(SBUF2 *sb, fdb_msg_t *msg, svc_callback_arg_t *arg)
{
    char *cid = msg->cf.cid;
    int keylen = msg->cf.keylen;
    char *key = msg->cf.key;
    unsigned long long genid;
    char *data;
    int datalen;
    char *datacopy;
    int datacopylen;
    int rc;

    assert(msg->hd.type == FDB_MSG_CURSOR_FIND ||
           msg->hd.type == FDB_MSG_CURSOR_FIND_LAST);

    rc = fdb_svc_cursor_find(
        cid, keylen, key, msg->hd.type == FDB_MSG_CURSOR_FIND_LAST, &genid,
        &datalen, &data, &datacopy, &datacopylen, arg->isuuid);
    if (rc != IX_FND && rc != IX_NOTFND && rc != IX_PASTEOF && rc != IX_EMPTY) {
        logmsg(LOGMSG_ERROR, "%s: failed to execute a cursor find rc=%d\n", __func__,
                rc);
        return rc;
    }

    rc = fdb_remcur_send_row(sb, msg, NULL, genid, data, datalen, datacopy,
                             datacopylen, rc, arg->isuuid);

    return rc;
}

int fdb_remcur_run_sql(SBUF2 *sb, fdb_msg_t *msg, svc_callback_arg_t *arg)
{
    char *cid = msg->sq.cid;
    int version = msg->sq.version;
    enum run_sql_flags flags = msg->sq.flags;
    char *sql = msg->sq.sql;
    int sqllen = msg->sq.sqllen;
    int rc = 0;
    char *trim_key = msg->sq.key;
    int trim_keylen = msg->sq.keylen;
    unsigned long long start_localrpc;
    unsigned long long end_localrpc;
    struct sqlclntstate *clnt = arg->clnt;

    /* TODO: get tid */

    start_localrpc = osql_log_time();
    /*fprintf(stderr, "=== Calling appsock %llu\n", start_localrpc);*/

    rc = fdb_appsock_work(cid, clnt, version, flags, sql, sqllen, trim_key,
                          trim_keylen, sb);
    if (rc) {
        /* this happens natural for fractured streams */
        if (gbl_fdb_track)
            logmsg(LOGMSG_USER, "%s: failed to dispatch request rc=%d \"%s\"\n",
                    __func__, rc, sql);
        return -1;
    }

    end_localrpc = osql_log_time();

    if (gbl_time_fdb) {
        logmsg(LOGMSG_USER, "=== DONE running remsql time %llu [%llu -> %llu] rc=%d\n",
            end_localrpc - start_localrpc, start_localrpc, end_localrpc, rc);
    }

    /* was there any error processing? */
    if (clnt->fdb_state.err.errval != 0) {
        /* we need to send back a rc code */
        rc = fdb_svc_sql_row(
            clnt->fdb_state.remote_sql_sb, cid,
            clnt->fdb_state.err.errstr, /* the actual row is the errstr */
            strlen(clnt->fdb_state.err.errstr) + 1, clnt->fdb_state.err.errval,
            arg->isuuid);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: fdb_send_rc failed rc=%d\n", __func__, rc);
        }
    }

    return rc;
}

/*====================== WRITE API ===================================*/

extern int gbl_partial_indexes;

int fdb_send_insert(fdb_msg_t *msg, char *cid, int version, int rootpage,
                    unsigned long long genid, unsigned long long ins_keys,
                    int datalen, char *data, int seq, int isuuid, SBUF2 *sb)
{
    int rc = 0;
    int send_dk = 0;

    fdb_msg_clean_message(msg);

    if (gbl_partial_indexes && ins_keys != -1ULL)
        send_dk = 1;

    msg->hd.type = send_dk ? FDB_MSG_INSERT_PI : FDB_MSG_INSERT;

    if (isuuid)
        msg->hd.type |= FD_MSG_FLAGS_ISUUID;

    msg->in.cid = cid;
    msg->in.version = version;
    msg->in.rootpage = rootpage;
    msg->in.genid = genid;
    msg->in.ins_keys = ins_keys;
    msg->in.datalen = datalen;
    msg->in.data = data;
    msg->in.seq = seq;

    rc = fdb_msg_write_message(sb, msg, 1);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: failed sending fdbc cursor_insert message rc=%d\n",
                __func__, rc);
        goto done;
    }

done:
    msg->in.data = NULL;
    msg->in.datalen = 0;

    return rc;
}

int fdb_send_delete(fdb_msg_t *msg, char *cid, int version, int rootpage,
                    unsigned long long genid, unsigned long long del_keys,
                    int seq, int isuuid, SBUF2 *sb)
{
    int rc = 0;
    int send_dk = 0;

    fdb_msg_clean_message(msg);

    if (gbl_partial_indexes && del_keys != -1ULL)
        send_dk = 1;

    msg->hd.type = send_dk ? FDB_MSG_DELETE_PI : FDB_MSG_DELETE;

    if (isuuid)
        msg->hd.type |= FD_MSG_FLAGS_ISUUID;

    msg->de.cid = cid;
    msg->de.version = version;
    msg->de.rootpage = rootpage;
    msg->de.genid = genid;
    msg->de.del_keys = del_keys;
    msg->de.seq = seq;

    rc = fdb_msg_write_message(sb, msg, 1);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: failed sending fdbc cursor_delete message rc=%d\n",
                __func__, rc);
        goto done;
    }

done:
    return rc;
}

int fdb_send_update(fdb_msg_t *msg, char *cid, int version, int rootpage,
                    unsigned long long oldgenid, unsigned long long genid,
                    unsigned long long ins_keys, unsigned long long del_keys,
                    int datalen, char *data, int seq, int isuuid, SBUF2 *sb)
{
    int rc = 0;
    int send_dk = 0;

    fdb_msg_clean_message(msg);

    if (gbl_partial_indexes && ins_keys != -1ULL && del_keys != -1ULL)
        send_dk = 1;

    msg->hd.type = send_dk ? FDB_MSG_UPDATE_PI : FDB_MSG_UPDATE;

    if (isuuid)
        msg->hd.type |= FD_MSG_FLAGS_ISUUID;

    msg->up.cid = cid;
    msg->up.version = version;
    msg->up.rootpage = rootpage;
    msg->up.oldgenid = oldgenid;
    msg->up.genid = genid;
    msg->up.ins_keys = ins_keys;
    msg->up.del_keys = del_keys;
    msg->up.datalen = datalen;
    msg->up.data = data;
    msg->up.seq = seq;

    rc = fdb_msg_write_message(sb, msg, 1);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: failed sending fdbc cursor_update message rc=%d\n",
                __func__, rc);
        goto done;
    }

done:
    msg->up.data = NULL;
    msg->up.datalen = 0;

    return rc;
}

int fdb_send_index(fdb_msg_t *msg, char *cid, int version, int rootpage,
                   unsigned long long genid, int is_delete, int ixnum,
                   int ixlen, char *ix, int seq, int isuuid, SBUF2 *sb)
{
    int rc = 0;

    fdb_msg_clean_message(msg);

    if (!gbl_expressions_indexes)
        return 0;

    msg->hd.type = FDB_MSG_INDEX;

    if (isuuid)
        msg->hd.type |= FD_MSG_FLAGS_ISUUID;

    msg->ix.cid = cid;
    msg->ix.version = version;
    msg->ix.rootpage = rootpage;
    msg->ix.genid = genid;
    msg->ix.is_delete = is_delete;
    msg->ix.ixnum = ixnum;
    msg->ix.ixlen = ixlen;
    msg->ix.ix = ix;
    msg->ix.seq = seq;

    rc = fdb_msg_write_message(sb, msg, 1);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: failed sending fdbc cursor_index  message rc=%d\n",
                __func__, rc);
        goto done;
    }

done:
    msg->ix.ix = NULL;
    msg->ix.ixlen = 0;

    return rc;
}

int fdb_remcur_insert(SBUF2 *sb, fdb_msg_t *msg, svc_callback_arg_t *arg)
{
    struct sqlclntstate *clnt = arg->clnt;
    unsigned long long genid = msg->in.genid;
    unsigned long long ins_keys = msg->in.ins_keys;
    char *data = msg->in.data;
    int datalen = msg->in.datalen;
    int rootpage = msg->in.rootpage;
    int version = msg->in.version;
    int seq = msg->in.seq;
    int rc;

    clnt->ins_keys = ins_keys;
    clnt->del_keys = 0ULL;

    rc = fdb_svc_cursor_insert(clnt, rootpage, version, genid, data, datalen,
                               seq);

    if (gbl_expressions_indexes) {
        free_cached_idx(clnt->idxInsert);
        free_cached_idx(clnt->idxDelete);
    }

    return rc;
}

int fdb_remcur_delete(SBUF2 *sb, fdb_msg_t *msg, svc_callback_arg_t *arg)
{
    struct sqlclntstate *clnt = arg->clnt;
    unsigned long long genid = msg->de.genid;
    unsigned long long del_keys = msg->de.del_keys;
    int rootpage = msg->de.rootpage;
    int version = msg->de.version;
    int seq = msg->de.seq;
    int rc;

    clnt->ins_keys = 0ULL;
    clnt->del_keys = del_keys;

    rc = fdb_svc_cursor_delete(clnt, rootpage, version, genid, seq);

    if (gbl_expressions_indexes) {
        free_cached_idx(clnt->idxInsert);
        free_cached_idx(clnt->idxDelete);
    }

    return rc;
}

int fdb_remcur_update(SBUF2 *sb, fdb_msg_t *msg, svc_callback_arg_t *arg)
{
    struct sqlclntstate *clnt = arg->clnt;
    unsigned long long oldgenid = msg->up.genid;
    unsigned long long genid = msg->up.genid;
    unsigned long long ins_keys = msg->up.ins_keys;
    unsigned long long del_keys = msg->up.del_keys;
    char *data = msg->up.data;
    int datalen = msg->up.datalen;
    int rootpage = msg->up.rootpage;
    int version = msg->up.version;
    int seq = msg->up.seq;
    int rc;

    clnt->ins_keys = ins_keys;
    clnt->del_keys = del_keys;

    rc = fdb_svc_cursor_update(clnt, rootpage, version, oldgenid, genid, data,
                               datalen, seq);

    if (gbl_expressions_indexes) {
        free_cached_idx(clnt->idxInsert);
        free_cached_idx(clnt->idxDelete);
    }

    return rc;
}

int fdb_remcur_index(SBUF2 *sb, fdb_msg_t *msg, svc_callback_arg_t *arg)
{
    struct sqlclntstate *clnt = arg->clnt;
    unsigned long long genid = msg->ix.genid;
    int is_delete = msg->ix.is_delete;
    int ixnum = msg->ix.ixnum;
    char *ix = msg->ix.ix;
    int ixlen = msg->ix.ixlen;
    int rootpage = msg->ix.rootpage;
    int version = msg->ix.version;
    int seq = msg->ix.seq;
    int rc;

    unsigned char *pIdx = NULL;

    if (is_delete) {
        assert(clnt->idxDelete[ixnum] == NULL);
        clnt->idxDelete[ixnum] = pIdx = malloc(sizeof(int) + ixlen);
    } else {
        assert(clnt->idxInsert[ixnum] == NULL);
        clnt->idxInsert[ixnum] = pIdx = malloc(sizeof(int) + ixlen);
    }
    if (pIdx == NULL) {
        logmsg(LOGMSG_ERROR, "%s:%d malloc %d failed\n", __func__, __LINE__,
                sizeof(int) + ixlen);
        return -1;
    }
    *((int *)pIdx) = ixlen;
    memcpy((unsigned char *)pIdx + sizeof(int), ix, ixlen);

    return 0;
}

int fdb_send_begin(fdb_msg_t *msg, fdb_tran_t *trans,
                   enum transaction_level lvl, int flags, int isuuid, SBUF2 *sb)
{
    int rc;

    /* clean previous whatever */
    fdb_msg_clean_message(msg);

    msg->hd.type = FDB_MSG_TRAN_BEGIN;
    msg->tr.tid = trans->tid;
    msg->tr.lvl = lvl;
    msg->tr.flags = flags;
    msg->tr.seq = 0; /* the beginnings: there was a zero */

    assert(trans->seq == 0);

    if (isuuid)
        msg->hd.type |= FD_MSG_FLAGS_ISUUID;

    sbuf2printf(sb, "%s\n", "remtran");

    rc = fdb_msg_write_message(sb, msg, 1);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: failed sending begin transaction message rc=%d\n",
                __func__, rc);
        return rc;
    }

    if (gbl_fdb_track) {
        fdb_msg_print_message(sb, msg, "sending msg");
    }

    return rc;
}

int fdb_send_prepare(fdb_msg_t *msg, char *tid, enum transaction_level lvl,
                     int isuuid, SBUF2 *sb)
{
    return -1;
}

int fdb_send_commit(fdb_msg_t *msg, fdb_tran_t *trans,
                    enum transaction_level lvl, int isuuid, SBUF2 *sb)
{
    int rc;

    msg->hd.type = FDB_MSG_TRAN_COMMIT;
    msg->tr.tid = trans->tid;
    msg->tr.lvl = lvl;
    msg->tr.flags = 0;
    msg->tr.seq = trans->seq;

    if (isuuid)
        msg->hd.type |= FD_MSG_FLAGS_ISUUID;

    rc = fdb_msg_write_message(sb, msg, 1);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: failed sending commit transaction message rc=%d\n",
                __func__, rc);
        return rc;
    }

    if (gbl_fdb_track) {
        fdb_msg_print_message(sb, msg, "sending msg");
    }

    return rc;
}

int fdb_send_rollback(fdb_msg_t *msg, fdb_tran_t *trans,
                      enum transaction_level lvl, int isuuid, SBUF2 *sb)
{
    int rc;

    msg->hd.type = FDB_MSG_TRAN_ROLLBACK;
    msg->tr.tid = trans->tid;
    msg->tr.lvl = lvl;
    msg->tr.flags = 0;
    msg->tr.seq = trans->seq;

    if (isuuid)
        msg->hd.type |= FD_MSG_FLAGS_ISUUID;

    rc = fdb_msg_write_message(sb, msg, 1);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: failed sending commit transaction message rc=%d\n",
                __func__, rc);
        return rc;
    }

    if (gbl_fdb_track) {
        fdb_msg_print_message(sb, msg, "sending msg");
    }

    return rc;
}

int fdb_send_rc(fdb_msg_t *msg, char *tid, int retrc, int errstrlen,
                char *errstr, int isuuid, SBUF2 *sb)
{
    int rc;

    fdb_msg_clean_message(msg);

    msg->hd.type = FDB_MSG_TRAN_RC;
    msg->rc.tid = msg->rc.tiduuid;

    if (isuuid) {
        msg->hd.type |= FD_MSG_FLAGS_ISUUID;
        comdb2uuidcpy(msg->rc.tid, tid);
    } else {
        memcpy(msg->co.tid, tid, sizeof(unsigned long long));
    }

    msg->rc.rc = retrc;
    msg->rc.errstrlen = errstrlen;
    msg->rc.errstr = errstr;

    rc = fdb_msg_write_message(sb, msg, 1);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: failed sending commit transaction message rc=%d\n",
                __func__, rc);
        return rc;
    }

    if (gbl_fdb_track) {
        fdb_msg_print_message(sb, msg, "sending msg");
    }

    return rc;
}

int fdb_send_heartbeat(fdb_msg_t *msg, char *tid, int isuuid, SBUF2 *sb)
{
    int rc;

    fdb_msg_clean_message(msg);

    msg->hd.type = FDB_MSG_HBEAT;

    msg->hb.tid = (char *)msg->hb.tiduuid;

    if (isuuid) {
        memmove(msg->hb.tiduuid, tid, sizeof(uuid_t));
        msg->hd.type |= FD_MSG_FLAGS_ISUUID;
    } else
        memcpy(msg->hb.tid, tid, sizeof(unsigned long long));

    clock_gettime(CLOCK_REALTIME, &msg->hb.timespec);

    rc = fdb_msg_write_message(sb, msg, 1);
    if (rc) {
        logmsg(LOGMSG_ERROR, 
                "%s: failed sending heartbeat transaction message rc=%d\n",
                __func__, rc);
        return rc;
    }

    if (gbl_fdb_track) {
        fdb_msg_print_message(sb, msg, "sending hbeat");
    }

    return rc;
}

int fdb_remcur_trans_begin(SBUF2 *sb, fdb_msg_t *msg, svc_callback_arg_t *arg)
{
    char *tid = msg->tr.tid;
    enum transaction_level lvl = msg->tr.lvl;
    int flags = msg->tr.flags;
    int seq = msg->tr.seq;
    int rc = 0;
    struct sqlclntstate *clnt;

    rc =
        fdb_svc_trans_begin(tid, lvl, flags, seq, arg->thd, arg->isuuid, &clnt);

    if (!rc) {
        arg->clnt = clnt;
        if (gbl_expressions_indexes) {
            if (clnt->idxInsert || clnt->idxDelete) {
                free_cached_idx(clnt->idxInsert);
                free_cached_idx(clnt->idxDelete);
                free(clnt->idxInsert);
                free(clnt->idxDelete);
                clnt->idxInsert = clnt->idxDelete = NULL;
            }
            clnt->idxInsert = calloc(MAXINDEX, sizeof(uint8_t *));
            clnt->idxDelete = calloc(MAXINDEX, sizeof(uint8_t *));
            if (!clnt->idxInsert || !clnt->idxDelete) {
                logmsg(LOGMSG_ERROR, "%s:%d malloc failed\n", __func__, __LINE__);
                return -1;
            }
        }
    }

    return rc;
}

int fdb_remcur_trans_prepare(SBUF2 *sb, fdb_msg_t *msg, svc_callback_arg_t *arg)
{
    abort();
}

int fdb_remcur_trans_commit(SBUF2 *sb, fdb_msg_t *msg, svc_callback_arg_t *arg)
{
    char *tid = msg->tr.tid;
    enum transaction_level lvl = msg->tr.lvl;
    int flags = msg->tr.flags;
    int seq = msg->tr.seq;
    int rc = 0, rc2;
    struct sqlclntstate *clnt = arg->clnt;
    char *errstr_if_any;
    int errstrlen;

    rc = fdb_svc_trans_commit(tid, lvl, clnt, seq);

    if (gbl_expressions_indexes) {
        free_cached_idx(clnt->idxInsert);
        free_cached_idx(clnt->idxDelete);
    }

    /* xerr.errstr has only prefix for success case */
    if (clnt->osql.xerr.errval && clnt->osql.xerr.errstr[0]) {
        errstrlen = strlen(clnt->osql.xerr.errstr) + 1;
        errstr_if_any = clnt->osql.xerr.errstr; /* cleanup will free */
    } else {
        errstrlen = 0;
        errstr_if_any = NULL;
    }

    /* send back the result */
    rc2 = fdb_send_rc(msg, tid, rc, errstrlen, errstr_if_any, arg->isuuid, sb);
    if (rc2) {
        logmsg(LOGMSG_ERROR, "%s: sending rc error rc=%d rc2=%d\n", __func__, rc,
                rc2);
        if (!rc)
            rc = rc2;
    }

    return rc;
}

int fdb_remcur_trans_rollback(SBUF2 *sb, fdb_msg_t *msg,
                              svc_callback_arg_t *arg)
{
    char *tid = msg->tr.tid;
    enum transaction_level lvl = msg->tr.lvl;
    int flags = msg->tr.flags;
    int seq = msg->tr.seq;
    int rc = 0;
    struct sqlclntstate *clnt = arg->clnt;

    rc = fdb_svc_trans_rollback(tid, lvl, clnt, seq);

    if (gbl_expressions_indexes) {
        free_cached_idx(clnt->idxInsert);
        free_cached_idx(clnt->idxDelete);
    }

    return rc;
}

int fdb_remcur_trans_rc(SBUF2 *sb, fdb_msg_t *msg, svc_callback_arg_t *arg)
{
    abort();
}

int fdb_remcur_trans_hbeat(SBUF2 *sb, fdb_msg_t *msg, svc_callback_arg_t *arg)
{
    /*fprintf(stderr, "Bingo!\n");*/
    /* We don't need to do anything, this just prevents the socket from timing
     * out */

    return 0;
}

