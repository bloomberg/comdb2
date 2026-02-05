/*
   Copyright 2015, 2018 Bloomberg Finance L.P.

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
#include "fdb_fend.h"
#include "fdb_comm.h"
#include "fdb_bend.h"
#include "fdb_bend_sql.h"
#include "fdb_whitelist.h"
#include "poll.h"
#include "flibc.h"
#include "logmsg.h"
#include "comdb2_appsock.h"
#include <disttxn.h>

#include "ssl_bend.h" /* for gbl_client_ssl_mode & gbl_ssl_allow_remsql */
#include "ssl_support.h"

extern int gbl_fdb_track;
extern int gbl_time_fdb;
extern int gbl_notimeouts;
extern int gbl_expressions_indexes;
extern int gbl_fdb_track_times;
extern int gbl_test_io_errors;
extern char *gbl_myuri;
extern int gbl_fdb_auth_enabled;

static int fdb_auth_enabled()
{
    return gbl_fdb_auth_enabled;
}

void comdb2_cheapstack_sym(FILE *f, char *fmt, ...);

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

    FDB_MSG_TRAN_2PC_BEGIN = 24,
    FDB_MSG_TRAN_2PC_RC = 25,
    FDB_MSG_MAX_OP
};

typedef struct {
    int type;
} fdb_msg_header_t;

typedef struct {
    fdb_msg_header_t type; /* FDB_MSG_CURSOR_CLOSE */
    char *cid;             /* cursor id */
    char *tid;             /* transaction id */
    uuid_t ciduuid;
    uuid_t tiduuid;
    int seq; /* used for ordering of certain operations in
                transactional cursors (see fdb_msg_tran_t); */
} fdb_msg_cursor_close_t;

typedef struct {
    fdb_msg_header_t type; /* FDB_MSG_CURSOR_FIND, .... */
    char *cid;             /* cursor id */
    uuid_t ciduuid;
} fdb_msg_cursor_move_t;

typedef struct {
    fdb_msg_header_t type;      /* FDB_MSG_TRAN_BEGIN, ... */
    char *tid;                  /* transaction id */
    enum transaction_level lvl; /* TRANLEVEL_SOSQL & co. */
    int flags;                  /* extensions */
    uuid_t tiduuid;
    int seq; /* sequencing tran begin/commit/rollback, writes, cursor open/close
              */
    int authdtalen;
    void *authdta;
} fdb_msg_tran_t;

typedef struct {
    fdb_msg_header_t type;      /* FDB_MSG_TRAN_BEGIN, ... */
    int version;                /* protocol version */
    char *tid;                  /* transaction id */
    enum transaction_level lvl; /* TRANLEVEL_SOSQL & co. */
    int flags;                  /* extensions */
    uuid_t tiduuid;
    int seq;
    char *dist_txnid;
    char *coordinator_dbname;
    char *coordinator_tier;
    int64_t timestamp;

    int authdtalen;
    void *authdta;
} fdb_msg_2pc_tran_t;

typedef struct {
    fdb_msg_header_t type; /* FDB_MS_TRAN_RC */
    char *tid;             /* transaction id */
    int rc;                /* result code */
    int errstrlen;         /* error string length */
    uuid_t tiduuid;
    char *errstr; /* error string, if any */
} fdb_msg_tran_rc_t;

typedef struct {
    fdb_msg_header_t type; /* FDB_MS_TRAN_RC */
    int version;           /* protocol version */
    char *tid;             /* transaction id */
    int rc;                /* result code */
    int errstrlen;         /* error string length */
    uuid_t tiduuid;
    char *errstr; /* error string, if any */
} fdb_msg_tran_2pc_rc_t;

typedef struct {
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
} fdb_msg_data_row_t;

typedef struct {
    fdb_msg_header_t type; /* FDB_MSG_CURSOR_FIND */
    char *cid;             /* id of the cursor */
    int keylen;            /* keylen, serialized sqlite */
    char *key;             /* key, serialized sqlite */
    uuid_t ciduuid;
} fdb_msg_cursor_find_t;

#define FDB_RUN_SQL

typedef struct {
    fdb_msg_header_t type;    /* FDB_MSG_RUN_SQL */
    char *cid;                /* cursor id */
    int version;              /* schema version */
    enum run_sql_flags flags; /* flags changing the remote behaviour */
    int sqllen;               /* len of sql query */
    char *sql;                /* sql query */
    int keylen; /* keylen used for end trimming  NOT USED RIGHT NOW!*/
    char *key;  /* key ised for end trimming  NOT USED RIGHT NOW! */
    uuid_t ciduuid;
} fdb_msg_run_sql_t;

typedef struct {
    fdb_msg_header_t type;       /* FDB_MSG_INSERT */
    char *cid;                   /* cursor id */
    int version;                 /* schema version */
    int rootpage;                /* which btree I am inserting into */
    unsigned long long genid;    /* genid */
    unsigned long long ins_keys; /* indexes to insert */
    int datalen;                 /* length of sqlite row, see below */
    int seq;                     /* transaction sequencing */
    uuid_t ciduuid;
    char *data;    /* sqlite generated row from MakeRecord, serialized */
    char *tblname; /* tblname matching rootpage */
} fdb_msg_insert_t;

typedef struct {
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
} fdb_msg_index_t;

typedef struct {
    fdb_msg_header_t type;       /* FDB_MSG_DELETE */
    char *cid;                   /* cursor id */
    int version;                 /* schema version */
    int rootpage;                /* which btree I am deleting from */
    unsigned long long genid;    /* genid */
    unsigned long long del_keys; /* indexes to delete */
    uuid_t ciduuid;
    int seq;       /* transaction sequencing */
    char *tblname; /* tblname matching rootpage */
} fdb_msg_delete_t;

typedef struct {
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
    char *data;    /* sqlite generated row from MakeRecord, serialized */
    char *tblname; /* tblname matching rootpage */
} fdb_msg_update_t;

typedef struct {
    fdb_msg_header_t type;    /* FDB_MSG_UPDATE */
    char *tid;                /* tran id */
    struct timespec timespec; /* when was this hbeat sent */
    uuid_t tiduuid;
} fdb_msg_hbeat_t;

typedef struct {
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
    int ssl;
    int authdtalen;
    void *authdta;
} fdb_msg_cursor_open_t;

union fdb_msg {
    fdb_msg_header_t hd;
    fdb_msg_cursor_open_t co;
    fdb_msg_cursor_close_t cc;
    fdb_msg_cursor_move_t cm;
    fdb_msg_tran_t tr;
    fdb_msg_tran_rc_t rc;
    fdb_msg_tran_2pc_rc_t rv;
    fdb_msg_data_row_t dr;
    fdb_msg_cursor_find_t cf;
    fdb_msg_run_sql_t sq;
    fdb_msg_insert_t in;
    fdb_msg_delete_t de;
    fdb_msg_update_t up;
    fdb_msg_index_t ix;
    fdb_msg_hbeat_t hb;
    fdb_msg_2pc_tran_t tv;
};

enum { FD_MSG_TYPE = 0x0fff, FD_MSG_FLAGS_ISUUID = 0x1000 };
typedef struct {
    struct sqlclntstate *clnt;
    struct sql_thread *thd;
    int flags;
} svc_callback_arg_t;

typedef int (*fdb_svc_callback_t)(COMDB2BUF *sb, fdb_msg_t *msg,
                                  svc_callback_arg_t *arg);

void free_cached_idx(uint8_t **cached_idx);

static int fdb_msg_write_message(COMDB2BUF *sb, fdb_msg_t *msg, int flush);

int fdb_bend_trans_begin(COMDB2BUF *sb, fdb_msg_t *msg, svc_callback_arg_t *arg);
int fdb_bend_trans_2pc_begin(COMDB2BUF *sb, fdb_msg_t *msg, svc_callback_arg_t *arg);
int fdb_bend_trans_prepare(COMDB2BUF *sb, fdb_msg_t *msg, svc_callback_arg_t *arg);
int fdb_bend_trans_commit(COMDB2BUF *sb, fdb_msg_t *msg, svc_callback_arg_t *arg);
int fdb_bend_trans_rollback(COMDB2BUF *sb, fdb_msg_t *msg, svc_callback_arg_t *arg);
int fdb_bend_trans_rc(COMDB2BUF *sb, fdb_msg_t *msg, svc_callback_arg_t *arg);
int fdb_bend_trans_2pc_rc(COMDB2BUF *sb, fdb_msg_t *msg, svc_callback_arg_t *arg);
int fdb_bend_cursor_open(COMDB2BUF *sb, fdb_msg_t *msg, svc_callback_arg_t *arg);
int fdb_bend_cursor_close(COMDB2BUF *sb, fdb_msg_t *msg, svc_callback_arg_t *arg);
int fdb_bend_cursor_find(COMDB2BUF *sb, fdb_msg_t *msg, svc_callback_arg_t *arg);
int fdb_bend_cursor_move(COMDB2BUF *sb, fdb_msg_t *msg, svc_callback_arg_t *arg);
int fdb_bend_nop(COMDB2BUF *sb, fdb_msg_t *msg, svc_callback_arg_t *arg);
int fdb_bend_run_sql(COMDB2BUF *sb, fdb_msg_t *msg, svc_callback_arg_t *arg);
int fdb_bend_insert(COMDB2BUF *sb, fdb_msg_t *msg, svc_callback_arg_t *arg);
int fdb_bend_delete(COMDB2BUF *sb, fdb_msg_t *msg, svc_callback_arg_t *arg);
int fdb_bend_update(COMDB2BUF *sb, fdb_msg_t *msg, svc_callback_arg_t *arg);
int fdb_bend_index(COMDB2BUF *sb, fdb_msg_t *msg, svc_callback_arg_t *arg);
int fdb_bend_trans_hbeat(COMDB2BUF *sb, fdb_msg_t *msg, svc_callback_arg_t *arg);

/* matches FDB_MSG... enums */
// clang-format off
fdb_svc_callback_t callbacks[] = {
    fdb_bend_trans_begin,  fdb_bend_trans_prepare,
    fdb_bend_trans_commit, fdb_bend_trans_rollback,
    fdb_bend_trans_rc,

    fdb_bend_cursor_open,  fdb_bend_cursor_close,
    fdb_bend_cursor_find,  fdb_bend_cursor_find,
    fdb_bend_cursor_move,  fdb_bend_cursor_move,
    fdb_bend_cursor_move,  fdb_bend_cursor_move,

    fdb_bend_nop,          fdb_bend_nop,

    fdb_bend_run_sql,

    fdb_bend_insert,       fdb_bend_delete,
    fdb_bend_update,

    fdb_bend_trans_hbeat,

    fdb_bend_insert,       fdb_bend_delete,
    fdb_bend_update,

    fdb_bend_index,

    fdb_bend_trans_2pc_begin
};
// clang-format on

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
    case FDB_MSG_TRAN_2PC_BEGIN:
        return "FDB_MSG_TRAN_2PC_BEGIN";
    case FDB_MSG_TRAN_2PC_RC:
        return "FDB_MSG_TRAN_2PC_RC";
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

int (*externalComdb2SerializeIdentity)(void *ID, int *length, void **dta) = NULL;

int fdb_send_open(struct sqlclntstate *clnt, fdb_msg_t *msg, char *cid, fdb_tran_t *trans, int rootp, int flags,
                  int version, COMDB2BUF *sb)
{
    int rc;

    /* useless but uniform */
    fdb_msg_clean_message(msg);

    msg->hd.type = FDB_MSG_CURSOR_OPEN;

    msg->co.cid = (char *)msg->co.ciduuid;
    msg->co.tid = (char *)msg->co.tiduuid;

    memcpy(msg->co.cid, cid, sizeof(uuid_t));
    if (trans)
        memcpy(msg->co.tid, trans->tid, sizeof(uuid_t));
    else
        comdb2uuid_clear((unsigned char *)msg->co.tid);
    msg->hd.type |= FD_MSG_FLAGS_ISUUID;

    msg->co.flags = flags;
    msg->co.rootpage = rootp;
    msg->co.version = version;
    msg->co.seq = (trans) ? trans->seq : 0;
    msg->co.srcpid = gbl_mypid;
    msg->co.srcnamelen = strlen(gbl_myuri) + 1;
    msg->co.srcname = gbl_myuri;
    msg->co.ssl = 0; /*TODO: do I need this? */

    if (clnt->authdata && fdb_auth_enabled() && externalComdb2SerializeIdentity) {
        rc = externalComdb2SerializeIdentity(clnt->authdata, &msg->co.authdtalen, &msg->co.authdta);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: failed to serialize identity\n", __func__);
            return rc;
        }
    }

    cdb2buf_printf(sb, "remsql\n");

    rc = fdb_msg_write_message(sb, msg, 1);
    if (rc) {
        logmsg(LOGMSG_ERROR,
               "%s: failed sending fdbc cursor_open message rc=%d\n", __func__,
               rc);
        return rc;
    }

    if (gbl_fdb_track) {
        fdb_msg_print_message(sb, msg, "sending open cursor");
    }

    return FDB_NOERR;
}

int fdb_send_close(fdb_msg_t *msg, char *cid, char *tid, int seq, COMDB2BUF *sb)
{
    int rc;

    fdb_msg_clean_message(msg);

    comdb2uuid_clear(msg->cc.ciduuid);
    comdb2uuid_clear(msg->cc.tiduuid);

    msg->hd.type = FDB_MSG_CURSOR_CLOSE;

    msg->cc.cid = (char *)msg->cc.ciduuid;
    msg->cc.tid = (char *)msg->cc.tiduuid;
    memcpy(msg->cc.cid, cid, sizeof(uuid_t));
    if (tid)
        comdb2uuidcpy((unsigned char *)msg->cc.tid, (unsigned char *)tid);
    msg->hd.type |= FD_MSG_FLAGS_ISUUID;
    msg->cc.seq = seq;

    rc = fdb_msg_write_message(sb, msg, 1);
    if (rc) {
        logmsg(LOGMSG_ERROR,
               "%s: failed sending fdbc cursor_close message rc=%d\n", __func__,
               rc);
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

int fdb_send_run_sql(fdb_msg_t *msg, char *cid, int sqllen, char *sql,
                     int version, int keylen, char *key,
                     enum run_sql_flags flags, COMDB2BUF *sb)
{
    int rc = 0;
    fdb_msg_clean_message(msg);

    /* request streaming remotely */
    msg->hd.type = FDB_MSG_RUN_SQL;

    msg->sq.cid = (char *)msg->sq.ciduuid;
    memmove(msg->sq.cid, cid, sizeof(uuid_t));
    msg->hd.type |= FD_MSG_FLAGS_ISUUID;

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

int fdb_msg_size(void)
{
    return sizeof(fdb_msg_t);
}


int fdb_msg_read_message_int(COMDB2BUF *sb, fdb_msg_t *msg, enum recv_flags flags,
                             const char *func, int line);

#define fdb_msg_read_message(sb, msg, flags) \
    fdb_msg_read_message_int(sb, msg, flags, __func__, __LINE__);


int fdb_recv_row_int(fdb_msg_t *msg, char *cid, COMDB2BUF *sb, const char *func, int line)
{
    int rc;

    rc = fdb_msg_read_message(sb, msg, 0);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: failed to receive remote row rc=%d (%s:%d)\n",
               __func__, rc, func, line);
        /* synthetic row containing the error */
        msg->hd.type = FDB_MSG_DATA_ROW;
        msg->dr.rc = FDB_ERR_READ_IO;
        msg->dr.data = strdup("failed to read row from socket");
        msg->dr.datalen = strlen(msg->rc.errstr) + 1;
        msg->dr.datacopylen = 0;
        msg->dr.datacopy = NULL;
        msg->dr.genid = -1LL;

        return FDB_ERR_READ_IO;
    }

    if (gbl_fdb_track) {
        fdb_msg_print_message(sb, msg, "received message");
    }

    msg->hd.type &= FD_MSG_TYPE;

    if (msg->hd.type != FDB_MSG_DATA_ROW)
        abort();

    return msg->dr.rc;
}

int fdb_recv_2pc_rc(fdb_msg_t *msg, fdb_tran_t *trans)
{
    int rc;

    rc = fdb_msg_read_message(trans->fcon.sb, msg, 0);
    if (rc != FDB_NOERR) {
        logmsg(LOGMSG_ERROR, "%s: failed to receive remote row rc=%d\n", __func__, rc);
        trans->rc = FDB_ERR_READ_IO;
        trans->errstr = strdup("failed to read rc from socket");
        trans->errstrlen = strlen(trans->errstr) + 1;
        return trans->rc;
    }

    if (gbl_fdb_track) {
        fdb_msg_print_message(trans->fcon.sb, msg, "received 2pc-rc message");
    }

    msg->hd.type &= FD_MSG_TYPE;

    if (msg->hd.type != FDB_MSG_TRAN_2PC_RC)
        abort();

    trans->rc = msg->rv.rc;
    trans->rmt2pcvers = msg->rv.version;

    if ((trans->rc == 0) && (comdb2uuidcmp((unsigned char *)msg->rv.tid, (unsigned char *)trans->tid) != 0)) {
        abort();
    }

    if (trans->rc) {
        trans->errstr = msg->rv.errstr;
        trans->errstrlen = msg->rv.errstrlen;
    } else {
        if (msg->rv.errstr) {
            logmsg(LOGMSG_ERROR, "%s: rc=%d but errror string present?\n", __func__, msg->rv.rc);
            free(msg->rv.errstr);
        }
        trans->errstr = NULL;
        trans->errstrlen = 0;
    }

    /* errstr, if any, is owned by fdb_tran_t now */
    msg->rv.errstrlen = 0;
    msg->rv.errstr = NULL;

    return trans->rc;
}

int fdb_recv_rc(fdb_msg_t *msg, fdb_tran_t *trans)
{
    int rc;

    rc = fdb_msg_read_message(trans->fcon.sb, msg, 0);
    if (rc != FDB_NOERR) {
        logmsg(LOGMSG_ERROR, "%s: failed to receive remote row rc=%d\n",
               __func__, rc);
        trans->rc = FDB_ERR_READ_IO;
        trans->errstr = strdup("failed to read rc from socket");
        trans->errstrlen = strlen(trans->errstr) + 1;
        return trans->rc;
    }

    if (gbl_fdb_track) {
        fdb_msg_print_message(trans->fcon.sb, msg, "received message");
    }

    msg->hd.type &= FD_MSG_TYPE;

    if (msg->hd.type != FDB_MSG_TRAN_RC)
        abort();

    trans->rc = msg->rc.rc;

    if ((trans->rc == 0) &&
        (comdb2uuidcmp((unsigned char *)msg->rc.tid,
                       (unsigned char *)trans->tid) != 0)) {
        abort();
    }

    if (trans->rc) {
        trans->errstr = msg->rc.errstr;
        trans->errstrlen = msg->rc.errstrlen;
    } else {
        if (msg->rc.errstr) {
            logmsg(LOGMSG_ERROR, "%s: rc=%d but errror string present?\n",
                   __func__, msg->rc.rc);
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

unsigned long long fdb_msg_genid(fdb_msg_t *msg)
{
    return msg->dr.genid;
}

int fdb_msg_datalen(fdb_msg_t *msg)
{
    return msg->dr.datalen;
}

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

    case FDB_MSG_TRAN_2PC_BEGIN:
        if (msg->tv.dist_txnid) {
            free(msg->tv.dist_txnid);
            msg->tv.dist_txnid = NULL;
        }
        if (msg->tv.coordinator_dbname) {
            free(msg->tv.coordinator_dbname);
            msg->tv.coordinator_dbname = NULL;
        }
        if (msg->tv.coordinator_tier) {
            free(msg->tv.coordinator_tier);
            msg->tv.coordinator_tier = NULL;
        }
        if (msg->tv.authdtalen && msg->tv.authdta) {
            free(msg->tv.authdta);
            msg->tv.authdtalen = 0;
            msg->tv.authdta = NULL;
        }
        break;

    case FDB_MSG_TRAN_BEGIN:
        if (msg->tr.authdtalen && msg->tr.authdta) {
            free(msg->tr.authdta);
            msg->tr.authdtalen = 0;
            msg->tr.authdta = NULL;
        }
    case FDB_MSG_TRAN_PREPARE:
    case FDB_MSG_TRAN_COMMIT:
    case FDB_MSG_TRAN_ROLLBACK:
        break;

    case FDB_MSG_TRAN_2PC_RC:
        if (msg->rv.errstrlen && msg->rv.errstr) {
            free(msg->rv.errstr);
            msg->rv.errstr = NULL;
            msg->rv.errstrlen = 0;
        }
        break;

    case FDB_MSG_TRAN_RC:
        if (msg->rc.errstrlen && msg->rc.errstr) {
            free(msg->rc.errstr);
            msg->rc.errstr = NULL;
            msg->rc.errstrlen = 0;
        }
        break;

    case FDB_MSG_CURSOR_OPEN:
        if (msg->co.srcname != gbl_myuri) {
            free(msg->co.srcname);
            msg->co.srcname = NULL;
            msg->co.srcnamelen = 0;
        }
        if (msg->co.authdta && msg->co.authdtalen) {
            free(msg->co.authdta);
            msg->co.authdtalen = 0;
            msg->co.authdta = NULL;
        }
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
        if (msg->in.tblname) {
            free(msg->in.tblname);
            msg->in.tblname = NULL;
        }
        break;

    case FDB_MSG_DELETE:
    case FDB_MSG_DELETE_PI:
        if (msg->de.tblname) {
            free(msg->de.tblname);
            msg->de.tblname = NULL;
        }
        break;

    case FDB_MSG_UPDATE:
    case FDB_MSG_UPDATE_PI:
        if (msg->up.datalen > 0 && msg->up.data) {
            free(msg->up.data);
            msg->up.data = NULL;
            msg->up.datalen = 0;
        }
        if (msg->up.tblname) {
            free(msg->up.tblname);
            msg->up.tblname = NULL;
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

static int fdb_msg_prepare_message(fdb_msg_t *msg)
{
    switch (msg->hd.type & FD_MSG_TYPE) {
    case FDB_MSG_TRAN_2PC_BEGIN:
        msg->tv.tid = (char *)msg->tv.tiduuid;
        break;

    case FDB_MSG_TRAN_2PC_RC:
        msg->rv.tid = (char *)msg->rv.tiduuid;
        break;

    case FDB_MSG_TRAN_BEGIN:
    case FDB_MSG_TRAN_COMMIT:
    case FDB_MSG_TRAN_ROLLBACK:
        msg->tr.tid = (char *)msg->tr.tiduuid;
        break;

    case FDB_MSG_TRAN_RC:
        msg->rc.tid = (char *)msg->rc.tiduuid;
        break;

    case FDB_MSG_CURSOR_OPEN:
        msg->co.cid = (char *)msg->co.ciduuid;
        msg->co.tid = (char *)msg->co.tiduuid;
        break;

    case FDB_MSG_CURSOR_CLOSE:
        msg->cc.cid = (char *)msg->cc.ciduuid;
        msg->cc.tid = (char *)msg->cc.tiduuid;
        break;

    case FDB_MSG_CURSOR_FIRST:
    case FDB_MSG_CURSOR_LAST:
    case FDB_MSG_CURSOR_NEXT:
    case FDB_MSG_CURSOR_PREV:
        msg->cm.cid = (char *)msg->cm.ciduuid;
        break;

    case FDB_MSG_DATA_ROW:
        msg->dr.cid = (char *)msg->dr.ciduuid;
        break;

    case FDB_MSG_CURSOR_FIND:
    case FDB_MSG_CURSOR_FIND_LAST:
        msg->cf.cid = (char *)msg->cf.ciduuid;
        break;

    case FDB_MSG_RUN_SQL:
        msg->sq.cid = (char *)msg->sq.ciduuid;
        break;

    case FDB_MSG_INSERT:
    case FDB_MSG_INSERT_PI:
        msg->in.cid = (char *)msg->in.ciduuid;
        break;

    case FDB_MSG_DELETE:
    case FDB_MSG_DELETE_PI:
        msg->de.cid = (char *)msg->de.ciduuid;
        break;

    case FDB_MSG_UPDATE:
    case FDB_MSG_UPDATE_PI:
        msg->up.cid = (char *)msg->up.ciduuid;
        break;

    case FDB_MSG_INDEX:
        msg->ix.cid = (char *)msg->ix.ciduuid;
        break;

    case FDB_MSG_HBEAT:
        msg->hb.tid = (char *)msg->hb.tiduuid;
        break;

    default:
        logmsg(LOGMSG_ERROR, "%s: unknown msg %d\n", __func__, (msg->hd.type & FD_MSG_TYPE));
        return -1;
    }
    return 0;
}

/* stuff comes in network endian fomat */
int fdb_msg_read_message_int(COMDB2BUF *sb, fdb_msg_t *msg, enum recv_flags flags,
                             const char *func, int line)
{
    int rc;
    unsigned long long lltmp;
    int tmp;
    int idsz;
    int recv_dk = 0;

    /* clean previous message */
    fdb_msg_clean_message(msg);

    if (gbl_test_io_errors) {
        static int counter = 0;
        if (random() % 5 == 0) {
            logmsg(LOGMSG_ERROR, "%s: triggered i/o error %d\n", __func__,
                   counter++);
            return -1;
        }
    }

    rc = cdb2buf_fread((char *)&msg->hd.type, 1, sizeof(msg->hd.type), sb);

    /*fprintf(stderr, "XYXY returned from cdb2buf_fread %llu\n",
     * osql_log_time());*/

    if (rc != sizeof(msg->hd.type)) {
        logmsg(LOGMSG_ERROR, "%s: failed to read header rc=%d (%s:%d)\n",
               __func__, rc, func, line);
        return -1;
    }

    msg->hd.type = ntohl(msg->hd.type);

    rc = fdb_msg_prepare_message(msg);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: failed to prepare message rc=%d (%s:%d)\n",
               __func__, rc, func, line);
        return -1;
    }

    /*fprintf(stderr, "XYXY returned from cdb2buf_fread %llu\n",
     * osql_log_time());*/

    assert (msg->hd.type & FD_MSG_FLAGS_ISUUID);
    idsz = sizeof(uuid_t);

    recv_dk = 0;
    switch (msg->hd.type & FD_MSG_TYPE) {
    case FDB_MSG_TRAN_2PC_BEGIN:

        rc = cdb2buf_fread((char *)&msg->tv.version, 1, sizeof(msg->tv.version), sb);
        if (rc != sizeof(msg->tv.version))
            return -1;
        msg->tv.version = ntohl(msg->tv.version);

        rc = cdb2buf_fread(msg->tv.tid, 1, idsz, sb);
        if (rc != idsz)
            return -1;

        rc = cdb2buf_fread((char *)&msg->tv.lvl, 1, sizeof(msg->tv.lvl), sb);
        if (rc != sizeof(msg->tv.lvl))
            return -1;
        msg->tv.lvl = ntohl(msg->tv.lvl);

        rc = cdb2buf_fread((char *)&msg->tv.flags, 1, sizeof(msg->tv.flags), sb);
        if (rc != sizeof(msg->tv.flags))
            return -1;
        msg->tv.flags = ntohl(msg->tv.flags);

        rc = cdb2buf_fread((char *)&msg->tv.seq, 1, sizeof(msg->tv.seq), sb);
        if (rc != sizeof(msg->tv.seq))
            return -1;
        msg->tv.seq = ntohl(msg->tv.seq);

        /* dist-txnid */
        rc = cdb2buf_fread((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return -1;
        tmp = ntohl(tmp);

        msg->tv.dist_txnid = malloc(tmp);
        rc = cdb2buf_fread((char *)msg->tv.dist_txnid, 1, tmp, sb);
        if (rc != tmp)
            return -1;

        /* coordinator-dbname */
        rc = cdb2buf_fread((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return -1;
        tmp = ntohl(tmp);

        msg->tv.coordinator_dbname = malloc(tmp);
        rc = cdb2buf_fread((char *)msg->tv.coordinator_dbname, 1, tmp, sb);
        if (rc != tmp)
            return -1;

        /* coordinator-tier */
        rc = cdb2buf_fread((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return -1;
        tmp = ntohl(tmp);

        msg->tv.coordinator_tier = malloc(tmp);
        rc = cdb2buf_fread((char *)msg->tv.coordinator_tier, 1, tmp, sb);
        if (rc != tmp)
            return -1;

        /* timestamp */
        rc = cdb2buf_fread((char *)&lltmp, 1, sizeof(lltmp), sb);
        if (rc != sizeof(lltmp))
            return -1;
        msg->tv.timestamp = flibc_ntohll(lltmp);

        /* auth */
        if (msg->tv.flags & FDB_MSG_TRANS_AUTH) {
            rc = cdb2buf_fread((char *)&msg->tv.authdtalen, 1, sizeof(msg->tv.authdtalen), sb);
            if (rc != sizeof(msg->tv.authdtalen))
                return -1;
            msg->tv.authdtalen = ntohl(msg->tv.authdtalen);
            msg->tv.authdta = malloc(msg->tv.authdtalen);
            rc = cdb2buf_fread(msg->tv.authdta, 1, msg->tv.authdtalen, sb);
            if (rc != msg->tv.authdtalen)
                return -1;
        }

        break;

    case FDB_MSG_TRAN_BEGIN:
    case FDB_MSG_TRAN_COMMIT:
    case FDB_MSG_TRAN_ROLLBACK:

        rc = cdb2buf_fread(msg->tr.tid, 1, idsz, sb);
        if (rc != idsz)
            return -1;

        rc = cdb2buf_fread((char *)&msg->tr.lvl, 1, sizeof(msg->tr.lvl), sb);
        if (rc != sizeof(msg->tr.lvl))
            return -1;
        msg->tr.lvl = ntohl(msg->tr.lvl);

        rc = cdb2buf_fread((char *)&msg->tr.flags, 1, sizeof(msg->tr.flags), sb);
        if (rc != sizeof(msg->tr.flags))
            return -1;
        msg->tr.flags = ntohl(msg->tr.flags);

        rc = cdb2buf_fread((char *)&msg->tr.seq, 1, sizeof(msg->tr.seq), sb);
        if (rc != sizeof(msg->tr.seq))
            return -1;
        msg->tr.seq = ntohl(msg->tr.seq);

        if (msg->tr.flags & FDB_MSG_TRANS_AUTH) {
            rc = cdb2buf_fread((char *)&msg->tr.authdtalen, 1, sizeof(msg->tr.authdtalen), sb);
            if (rc != sizeof(msg->tr.authdtalen))
                return -1;
            msg->tr.authdtalen = ntohl(msg->tr.authdtalen);
            msg->tr.authdta = malloc(msg->tr.authdtalen);
            rc = cdb2buf_fread(msg->tr.authdta, 1, msg->tr.authdtalen, sb);
            if (rc != msg->tr.authdtalen)
                return -1;
        }

        break;

    case FDB_MSG_TRAN_2PC_RC:

        rc = cdb2buf_fread((char *)&msg->rv.version, 1, sizeof(msg->rv.version), sb);
        if (rc != sizeof(msg->rv.version))
            return -1;
        msg->rv.version = ntohl(msg->rv.version);

        rc = cdb2buf_fread((char *)msg->rv.tid, 1, idsz, sb);
        if (rc != idsz)
            return -1;

        rc = cdb2buf_fread((char *)&msg->rv.rc, 1, sizeof(msg->rv.rc), sb);
        if (rc != sizeof(msg->rv.rc))
            return -1;
        msg->rv.rc = ntohl(msg->rv.rc);

        rc = cdb2buf_fread((char *)&msg->rv.errstrlen, 1, sizeof(msg->rv.errstrlen), sb);
        if (rc != sizeof(msg->rv.errstrlen))
            return -1;
        msg->rv.errstrlen = ntohl(msg->rv.errstrlen);

        if (msg->rv.errstrlen) {
            msg->rv.errstr = (char *)malloc(msg->rv.errstrlen);
            if (!msg->rv.errstr)
                return -1;

            rc = cdb2buf_fread(msg->rv.errstr, 1, msg->rv.errstrlen, sb);
            if (rc != msg->rv.errstrlen)
                return -1;
        } else {
            msg->rv.errstr = NULL;
        }

        break;

    case FDB_MSG_TRAN_RC:

        rc = cdb2buf_fread((char *)msg->rc.tid, 1, idsz, sb);
        if (rc != idsz)
            return -1;

        rc = cdb2buf_fread((char *)&msg->rc.rc, 1, sizeof(msg->rc.rc), sb);
        if (rc != sizeof(msg->rc.rc))
            return -1;
        msg->rc.rc = ntohl(msg->rc.rc);

        rc = cdb2buf_fread((char *)&msg->rc.errstrlen, 1,
                        sizeof(msg->rc.errstrlen), sb);
        if (rc != sizeof(msg->rc.errstrlen))
            return -1;
        msg->rc.errstrlen = ntohl(msg->rc.errstrlen);

        if (msg->rc.errstrlen) {
            msg->rc.errstr = (char *)malloc(msg->rc.errstrlen);
            if (!msg->rc.errstr)
                return -1;

            rc = cdb2buf_fread(msg->rc.errstr, 1, msg->rc.errstrlen, sb);
            if (rc != msg->rc.errstrlen)
                return -1;
        } else {
            msg->rc.errstr = NULL;
        }

        break;

    case FDB_MSG_CURSOR_OPEN:

        rc = cdb2buf_fread(msg->co.cid, 1, idsz, sb);
        if (rc != idsz)
            return -1;

        rc = cdb2buf_fread(msg->co.tid, 1, idsz, sb);
        if (rc != idsz)
            return -1;

        rc = cdb2buf_fread((char *)&msg->co.flags, 1, sizeof(msg->co.flags), sb);
        if (rc != sizeof(msg->co.flags))
            return -1;
        msg->co.flags = ntohl(msg->co.flags);

        rc = cdb2buf_fread((char *)&msg->co.rootpage, 1, sizeof(msg->co.rootpage),
                        sb);
        if (rc != sizeof(msg->co.rootpage))
            return -1;
        msg->co.rootpage = ntohl(msg->co.rootpage);

        rc = cdb2buf_fread((char *)&msg->co.version, 1, sizeof(msg->co.version),
                        sb);
        if (rc != sizeof(msg->co.version))
            return -1;
        msg->co.version = ntohl(msg->co.version);

        rc = cdb2buf_fread((char *)&msg->co.seq, 1, sizeof(msg->co.seq), sb);
        if (rc != sizeof(msg->co.seq))
            return -1;
        msg->co.seq = ntohl(msg->co.seq);

        rc = cdb2buf_fread((char *)&msg->co.srcpid, 1, sizeof(msg->co.srcpid), sb);
        if (rc != sizeof(msg->co.srcpid))
            return -1;
        msg->co.srcpid = ntohl(msg->co.srcpid);

        rc = cdb2buf_fread((char *)&msg->co.srcnamelen, 1,
                        sizeof(msg->co.srcnamelen), sb);
        if (rc != sizeof(msg->co.srcnamelen))
            return -1;
        msg->co.srcnamelen = ntohl(msg->co.srcnamelen);

        if (msg->co.srcnamelen > 0) {
            msg->co.srcname = (char *)malloc(msg->co.srcnamelen);
            if (!msg->co.srcname)
                return -1;

            rc = cdb2buf_fread(msg->co.srcname, 1, msg->co.srcnamelen, sb);
            if (rc != msg->co.srcnamelen)
                return -1;
        } else {
            msg->co.srcname = NULL;
        }

        if (msg->co.flags & FDB_MSG_CURSOR_OPEN_FLG_SSL) {
            rc = cdb2buf_fread((char *)&msg->co.ssl, 1, sizeof(msg->co.ssl), sb);
            if (rc != sizeof(msg->co.ssl))
                return -1;
            msg->co.ssl = ntohl(msg->co.ssl);
            /*fprintf(stderr, "Read ssl %d size %d\n", msg->co.ssl,
             * sizeof(tmp));*/

            if (gbl_client_ssl_mode < SSL_ALLOW)
                return -1;

            rc = cdb2buf_putc(sb, 'Y');
            if (rc < 0)
                return -1;
            rc = cdb2buf_flush(sb);
            if (rc < 0)
                return -1;
            rc = sslio_accept(sb, gbl_ssl_ctx, SSL_REQUIRE, NULL,
                              gbl_nid_dbname, 1);
            if (rc != 1)
                return -1;
        }

        if (msg->co.flags & FDB_MSG_CURSOR_OPEN_FLG_AUTH) {
            rc = cdb2buf_fread((char *)&msg->co.authdtalen, 1, sizeof(msg->co.authdtalen), sb);
            if (rc != sizeof(msg->co.authdtalen))
                return -1;
            msg->co.authdtalen = ntohl(msg->co.authdtalen);
            msg->co.authdta = malloc(msg->co.authdtalen);
            rc = cdb2buf_fread(msg->co.authdta, 1, msg->co.authdtalen, sb);
            if (rc != msg->co.authdtalen)
                return -1;
        }

        if (!fdb_is_dbname_in_whitelist(msg->co.srcname)) {
            char *data = strdup("Access Error: db not allowed to connect");
            int datalen = strlen(data) + 1;
            fdb_bend_send_row(sb, msg, NULL, 0, data, datalen, NULL, 0,
                              FDB_ERR_ACCESS);
            return -1;
        }

        break;

    case FDB_MSG_CURSOR_CLOSE: {
        int haveid = 0;

        rc = cdb2buf_fread(msg->cc.cid, 1, idsz, sb);
        if (rc != idsz)
            return -1;

        /* locally populated, I can't break current read protocol */
        rc = fdb_svc_trans_get_tid(msg->cc.cid, msg->cc.tid);
        if (rc)
            return -1;

        assert (msg->hd.type & FD_MSG_FLAGS_ISUUID);
        haveid = !comdb2uuid_is_zero((unsigned char *)msg->cc.tid);

        if (haveid) {
            rc = cdb2buf_fread((char *)&tmp, 1, sizeof(tmp), sb);
            if (rc != sizeof(tmp))
                return FDB_ERR_WRITE_IO;
            msg->cc.seq = htonl(tmp);
        } else {
            msg->cc.seq = 0;
        }

        break;
    }

    case FDB_MSG_CURSOR_FIRST:
    case FDB_MSG_CURSOR_LAST:
    case FDB_MSG_CURSOR_NEXT:
    case FDB_MSG_CURSOR_PREV:

        rc = cdb2buf_fread(msg->cm.cid, 1, idsz, sb);
        if (rc != idsz)
            return -1;

        break;

    case FDB_MSG_DATA_ROW:

        rc = cdb2buf_fread(msg->dr.cid, 1, idsz, sb);
        if (rc != idsz)
            return -1;

        rc = cdb2buf_fread((char *)&msg->dr.rc, 1, sizeof(msg->dr.rc), sb);
        if (rc != sizeof(msg->dr.rc))
            return -1;
        msg->dr.rc = ntohl(msg->dr.rc);

        rc = cdb2buf_fread((char *)&msg->dr.genid, 1, sizeof(msg->dr.genid), sb);
        if (rc != sizeof(msg->dr.genid))
            return -1;
        msg->dr.genid = flibc_htonll(msg->dr.genid);

        rc = cdb2buf_fread((char *)&msg->dr.datalen, 1, sizeof(msg->dr.datalen),
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
            rc = cdb2buf_fread(msg->dr.data, 1, msg->dr.datalen, sb);
            if (rc != msg->dr.datalen)
                return -1;
        } else {
            msg->dr.data = NULL;
        }

        if (msg->dr.datacopylen > 0) {
            rc = cdb2buf_fread(msg->dr.datacopy, 1, msg->dr.datacopylen, sb);
            if (rc != msg->dr.datacopylen)
                return -1;
        } else {
            msg->dr.datacopy = NULL;
        }

        break;

    case FDB_MSG_CURSOR_FIND:
    case FDB_MSG_CURSOR_FIND_LAST:

        rc = cdb2buf_fread((char *)msg->cf.cid, 1, idsz, sb);
        if (rc != idsz)
            return -1;

        rc = cdb2buf_fread((char *)&msg->cf.keylen, 1, sizeof(msg->cf.keylen), sb);
        if (rc != sizeof(msg->cf.keylen))
            return -1;
        msg->cf.keylen = ntohl(msg->cf.keylen);

        assert(msg->cf.keylen > 0);

        msg->cf.key = (char *)malloc(msg->cf.keylen);
        if (!msg->cf.key)
            return -1;

        rc = cdb2buf_fread(msg->cf.key, 1, msg->cf.keylen, sb);
        if (rc != msg->cf.keylen)
            return -1;

        break;

    case FDB_MSG_RUN_SQL:

        /*fprintf(stderr, "%d XYXY calling cdb2buf_fread %llu\n", __LINE__,
         * osql_log_time());*/

        rc = cdb2buf_fread((char *)msg->sq.cid, 1, idsz, sb);
        if (rc != idsz)
            return -1;
        /*fprintf(stderr, "%d XYXY DONE calling cdb2buf_fread %llu\n", __LINE__,
         * osql_log_time());*/

        /*fprintf(stderr, "%d XYXY calling cdb2buf_fread %llu\n", __LINE__,
         * osql_log_time());*/
        rc = cdb2buf_fread((char *)&msg->sq.version, 1, sizeof(msg->sq.version),
                        sb);
        if (rc != sizeof(msg->sq.version))
            return -1;
        msg->sq.version = ntohl(msg->sq.version);
        /*fprintf(stderr, "%d XYXY DONE calling cdb2buf_fread %llu\n", __LINE__,
         * osql_log_time());*/

        /*fprintf(stderr, "%d XYXY calling cdb2buf_fread %llu\n", __LINE__,
         * osql_log_time());*/
        rc = cdb2buf_fread((char *)&msg->sq.flags, 1, sizeof(msg->sq.flags), sb);
        if (rc != sizeof(msg->sq.flags))
            return -1;
        msg->sq.flags = ntohl(msg->sq.flags);
        /*fprintf(stderr, "%d XYXY DONE calling cdb2buf_fread %llu\n", __LINE__,
         * osql_log_time());*/

        /*fprintf(stderr, "%d XYXY calling cdb2buf_fread %llu\n", __LINE__,
         * osql_log_time());*/
        rc = cdb2buf_fread((char *)&msg->sq.sqllen, 1, sizeof(msg->sq.sqllen), sb);
        if (rc != sizeof(msg->sq.sqllen))
            return -1;
        msg->sq.sqllen = ntohl(msg->sq.sqllen);
        /*fprintf(stderr, "%d XYXY DONE calling cdb2buf_fread %llu\n", __LINE__,
         * osql_log_time());*/

        msg->sq.sql = (char *)malloc(msg->sq.sqllen);
        if (!msg->sq.sql)
            return -1;

        /*fprintf(stderr, "%d XYXY calling cdb2buf_fread %llu\n", __LINE__,
         * osql_log_time());*/
        rc = cdb2buf_fread(msg->sq.sql, 1, msg->sq.sqllen, sb);
        if (rc != msg->sq.sqllen)
            return -1;
        /*fprintf(stderr, "%d XYXY DONE calling cdb2buf_fread %llu\n", __LINE__,
         * osql_log_time());*/

        /* if we have end trimming, pass that */
        if (msg->sq.flags == FDB_RUN_SQL_TRIM) {
            /*fprintf(stderr, "%d XYXY calling cdb2buf_fread %llu\n", __LINE__,
             * osql_log_time());*/
            rc = cdb2buf_fread((char *)&msg->sq.keylen, 1, sizeof(msg->sq.keylen),
                            sb);
            if (rc != sizeof(msg->sq.keylen))
                return -1;
            msg->sq.keylen = ntohl(msg->sq.keylen);
            /*fprintf(stderr, "%d XYXY DONE calling cdb2buf_fread %llu\n",
             * __LINE__, osql_log_time());*/

            if (msg->sq.keylen > 0) {
                msg->sq.key = (char *)malloc(msg->sq.keylen);
                if (!msg->sq.key)
                    return -1;

                /*fprintf(stderr, "%d XYXY calling cdb2buf_fread %llu\n", __LINE__,
                 * osql_log_time());*/
                rc = cdb2buf_fread(msg->sq.key, 1, msg->sq.keylen, sb);
                if (rc != msg->sq.keylen)
                    return -1;
                /*fprintf(stderr, "%d XYXY DONE calling cdb2buf_fread %llu\n",
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
        rc = cdb2buf_fread((char *)msg->in.cid, 1, idsz, sb);
        if (rc != idsz)
            return -1;

        rc = cdb2buf_fread((char *)&msg->in.version, 1, sizeof(msg->in.version),
                        sb);
        if (rc != sizeof(msg->in.version))
            return -1;
        msg->in.version = ntohl(msg->in.version);

        rc = cdb2buf_fread((char *)&msg->in.rootpage, 1, sizeof(msg->in.rootpage),
                        sb);
        if (rc != sizeof(msg->in.rootpage))
            return -1;
        msg->in.rootpage = ntohl(msg->in.rootpage);

        rc = cdb2buf_fread((char *)&msg->in.genid, 1, sizeof(msg->in.genid), sb);
        if (rc != sizeof(msg->in.genid))
            return -1;
        msg->in.genid = flibc_htonll(msg->in.genid);

        if (recv_dk) {
            rc = cdb2buf_fread((char *)&msg->in.ins_keys, 1,
                            sizeof(msg->in.ins_keys), sb);
            if (rc != sizeof(msg->in.ins_keys))
                return -1;
            msg->in.ins_keys = flibc_htonll(msg->in.ins_keys);
        } else {
            msg->in.ins_keys = -1ULL;
        }

        rc = cdb2buf_fread((char *)&msg->in.datalen, 1, sizeof(msg->in.datalen),
                        sb);
        if (rc != sizeof(msg->in.datalen))
            return -1;
        msg->in.datalen = ntohl(msg->in.datalen);

        rc = cdb2buf_fread((char *)&msg->in.seq, 1, sizeof(msg->in.seq), sb);
        if (rc != sizeof(msg->in.seq))
            return -1;
        msg->in.seq = ntohl(msg->in.seq);

        if (msg->in.datalen > 0) {
            msg->in.data = (char *)malloc(msg->in.datalen);
            if (!msg->in.data)
                return -1;

            rc = cdb2buf_fread(msg->in.data, 1, msg->in.datalen, sb);
            if (rc != msg->in.datalen)
                return -1;
        } else {
            msg->in.data = NULL;
        }

        if (flags & FDB_MSG_TRAN_TBLNAME) {
            rc = cdb2buf_fread((char *)&tmp, 1, sizeof(tmp), sb);
            if (rc != sizeof(tmp))
                return -1;
            tmp = ntohl(tmp);
            msg->in.tblname = malloc(tmp);
            if (!msg->in.tblname)
                return -1;
            rc = cdb2buf_fread(msg->in.tblname, 1, tmp, sb);
            if (rc != tmp)
                return -1;
        }

        break;

    case FDB_MSG_DELETE_PI:
        recv_dk = 1;

    case FDB_MSG_DELETE:

        rc = cdb2buf_fread(msg->de.cid, 1, idsz, sb);
        if (rc != idsz)
            return -1;

        rc = cdb2buf_fread((char *)&msg->de.version, 1, sizeof(msg->de.version),
                        sb);
        if (rc != sizeof(msg->de.version))
            return -1;
        msg->de.version = ntohl(msg->de.version);

        rc = cdb2buf_fread((char *)&msg->de.rootpage, 1, sizeof(msg->de.rootpage),
                        sb);
        if (rc != sizeof(msg->de.rootpage))
            return -1;
        msg->de.rootpage = ntohl(msg->de.rootpage);

        rc = cdb2buf_fread((char *)&msg->de.genid, 1, sizeof(msg->de.genid), sb);
        if (rc != sizeof(msg->de.genid))
            return -1;
        msg->de.genid = flibc_htonll(msg->de.genid);

        if (recv_dk) {
            rc = cdb2buf_fread((char *)&msg->de.del_keys, 1,
                            sizeof(msg->de.del_keys), sb);
            if (rc != sizeof(msg->de.del_keys))
                return -1;
            msg->de.del_keys = flibc_htonll(msg->de.del_keys);
        } else {
            msg->de.del_keys = -1ULL;
        }

        rc = cdb2buf_fread((char *)&msg->de.seq, 1, sizeof(msg->de.seq), sb);
        if (rc != sizeof(msg->de.seq))
            return -1;
        msg->de.seq = ntohl(msg->de.seq);

        if (flags & FDB_MSG_TRAN_TBLNAME) {
            rc = cdb2buf_fread((char *)&tmp, 1, sizeof(tmp), sb);
            if (rc != sizeof(tmp))
                return -1;
            tmp = ntohl(tmp);
            msg->de.tblname = malloc(tmp);
            if (!msg->de.tblname)
                return -1;
            rc = cdb2buf_fread(msg->de.tblname, 1, tmp, sb);
            if (rc != tmp)
                return -1;
        }

        break;

    case FDB_MSG_UPDATE_PI:
        recv_dk = 1;

    case FDB_MSG_UPDATE:

        rc = cdb2buf_fread(msg->up.cid, 1, idsz, sb);
        if (rc != idsz)
            return -1;

        rc = cdb2buf_fread((char *)&msg->up.version, 1, sizeof(msg->up.version),
                        sb);
        if (rc != sizeof(msg->up.version))
            return -1;
        msg->up.version = ntohl(msg->up.version);

        rc = cdb2buf_fread((char *)&msg->up.rootpage, 1, sizeof(msg->up.rootpage),
                        sb);
        if (rc != sizeof(msg->up.rootpage))
            return -1;
        msg->up.rootpage = ntohl(msg->up.rootpage);

        rc = cdb2buf_fread((char *)&msg->up.oldgenid, 1, sizeof(msg->up.oldgenid),
                        sb);
        if (rc != sizeof(msg->up.oldgenid))
            return -1;
        msg->up.oldgenid = flibc_htonll(msg->up.oldgenid);

        rc = cdb2buf_fread((char *)&msg->up.genid, 1, sizeof(msg->up.genid), sb);
        if (rc != sizeof(msg->up.genid))
            return -1;
        msg->up.genid = flibc_htonll(msg->up.genid);

        if (recv_dk) {
            rc = cdb2buf_fread((char *)&msg->up.ins_keys, 1,
                            sizeof(msg->up.ins_keys), sb);
            if (rc != sizeof(msg->up.ins_keys))
                return -1;
            msg->up.ins_keys = flibc_htonll(msg->up.ins_keys);

            rc = cdb2buf_fread((char *)&msg->up.del_keys, 1,
                            sizeof(msg->up.del_keys), sb);
            if (rc != sizeof(msg->up.del_keys))
                return -1;
            msg->up.del_keys = flibc_htonll(msg->up.del_keys);
        } else {
            msg->up.ins_keys = -1ULL;
            msg->up.del_keys = -1ULL;
        }

        rc = cdb2buf_fread((char *)&msg->up.datalen, 1, sizeof(msg->up.datalen),
                        sb);
        if (rc != sizeof(msg->up.datalen))
            return -1;
        msg->up.datalen = ntohl(msg->up.datalen);

        rc = cdb2buf_fread((char *)&msg->up.seq, 1, sizeof(msg->up.seq), sb);
        if (rc != sizeof(msg->up.seq))
            return -1;
        msg->up.seq = ntohl(msg->up.seq);

        if (msg->up.datalen > 0) {
            msg->up.data = (char *)malloc(msg->up.datalen);
            if (!msg->up.data)
                return -1;

            rc = cdb2buf_fread(msg->up.data, 1, msg->up.datalen, sb);
            if (rc != msg->up.datalen)
                return -1;
        } else {
            msg->up.data = NULL;
        }

        if (flags & FDB_MSG_TRAN_TBLNAME) {
            rc = cdb2buf_fread((char *)&tmp, 1, sizeof(tmp), sb);
            if (rc != sizeof(tmp))
                return -1;
            tmp = ntohl(tmp);
            msg->up.tblname = malloc(tmp);
            if (!msg->up.tblname)
                return -1;
            rc = cdb2buf_fread(msg->up.tblname, 1, tmp, sb);
            if (rc != tmp)
                return -1;
        }

        break;

    case FDB_MSG_INDEX:
        rc = cdb2buf_fread((char *)msg->ix.cid, 1, idsz, sb);
        if (rc != idsz)
            return -1;

        rc = cdb2buf_fread((char *)&msg->ix.version, 1, sizeof(msg->ix.version),
                        sb);
        if (rc != sizeof(msg->ix.version))
            return -1;
        msg->ix.version = ntohl(msg->ix.version);

        rc = cdb2buf_fread((char *)&msg->ix.rootpage, 1, sizeof(msg->ix.rootpage),
                        sb);
        if (rc != sizeof(msg->ix.rootpage))
            return -1;
        msg->ix.rootpage = ntohl(msg->ix.rootpage);

        rc = cdb2buf_fread((char *)&msg->ix.genid, 1, sizeof(msg->ix.genid), sb);
        if (rc != sizeof(msg->ix.genid))
            return -1;
        msg->ix.genid = flibc_htonll(msg->ix.genid);

        rc = cdb2buf_fread((char *)&msg->ix.is_delete, 1,
                        sizeof(msg->ix.is_delete), sb);
        if (rc != sizeof(msg->ix.is_delete))
            return -1;
        msg->ix.is_delete = ntohl(msg->ix.is_delete);

        rc = cdb2buf_fread((char *)&msg->ix.ixnum, 1, sizeof(msg->ix.ixnum), sb);
        if (rc != sizeof(msg->ix.ixnum))
            return -1;
        msg->ix.ixnum = ntohl(msg->ix.ixnum);

        rc = cdb2buf_fread((char *)&msg->ix.ixlen, 1, sizeof(msg->ix.ixlen), sb);
        if (rc != sizeof(msg->ix.ixlen))
            return -1;
        msg->ix.ixlen = ntohl(msg->ix.ixlen);

        rc = cdb2buf_fread((char *)&msg->ix.seq, 1, sizeof(msg->ix.seq), sb);
        if (rc != sizeof(msg->ix.seq))
            return -1;
        msg->ix.seq = ntohl(msg->ix.seq);

        if (msg->ix.ixlen > 0) {
            msg->ix.ix = (char *)malloc(msg->ix.ixlen);
            if (!msg->ix.ix)
                return -1;

            rc = cdb2buf_fread(msg->ix.ix, 1, msg->ix.ixlen, sb);
            if (rc != msg->ix.ixlen)
                return -1;
        } else {
            msg->ix.ix = NULL;
        }

        break;

    case FDB_MSG_HBEAT:
        rc = cdb2buf_fread((char *)msg->hb.tid, 1, idsz, sb);
        if (rc != idsz)
            return -1;

        rc = cdb2buf_fread((char *)&lltmp, 1, sizeof(lltmp), sb);
        if (rc != sizeof(lltmp))
            return -1;
        msg->hb.timespec.tv_sec = ntohl(lltmp);

        rc = cdb2buf_fread((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return -1;
        msg->hb.timespec.tv_nsec = ntohl(tmp);

        break;

    default:
        logmsg(LOGMSG_ERROR, "%s: unknown msg %d\n", __func__, msg->hd.type);
        return -1;
    }

    /*fprintf(stderr, "%d XYXY ALL DONE calling cdb2buf_fread %llu\n", __LINE__,
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

void fdb_msg_print_message(COMDB2BUF *sb, fdb_msg_t *msg, char *prefix)
{
    unsigned long long t = osql_log_time();
    uuidstr_t cus;
    uuidstr_t tus;
    char prf[512];

    snprintf(prf, sizeof(prf), "%p: %llu%s%s", (void *)pthread_self(), (unsigned long long)gettimeofday_ms(),
             (prefix) ? " " : "", (prefix) ? prefix : "");
    prefix = prf;

    switch (msg->hd.type & FD_MSG_TYPE) {
    case FDB_MSG_TRAN_BEGIN:
    case FDB_MSG_TRAN_PREPARE:
    case FDB_MSG_TRAN_COMMIT:
    case FDB_MSG_TRAN_ROLLBACK:

        logmsg(LOGMSG_USER, "XXXX: %s %s tid=%s fl=%x lvl=%s\n", prefix, __req_2_str(msg->hd.type),
               comdb2uuidstr((unsigned char *)msg->tr.tid, tus), msg->tr.flags, tranlevel_tostr(msg->tr.lvl));
        break;

    case FDB_MSG_TRAN_2PC_BEGIN:
        logmsg(LOGMSG_USER, "XXXX: %s %s tid=%s version=%d fl=%x lvl=%s\n", prefix, __req_2_str(msg->hd.type),
               comdb2uuidstr((unsigned char *)msg->tv.tid, tus), msg->tv.version, msg->tv.flags,
               tranlevel_tostr(msg->tv.lvl));
        break;
    case FDB_MSG_TRAN_RC:

        logmsg(LOGMSG_USER, "XXXX: %s TRAN_RC tid=%s rc=%d %s\n", prefix,
               comdb2uuidstr((unsigned char *)msg->rc.tid, tus), msg->rc.rc,
               (msg->rc.errstrlen > 0 && msg->rc.errstr) ? msg->rc.errstr : "");
        break;

    case FDB_MSG_TRAN_2PC_RC:
        logmsg(LOGMSG_USER, "XXXX: %s TRAN_RC tid=%s rc=%d version=%d %s\n", prefix,
               comdb2uuidstr((unsigned char *)msg->rv.tid, tus), msg->rv.rc, msg->rv.version,
               (msg->rv.errstrlen > 0 && msg->rv.errstr) ? msg->rv.errstr : "");
        break;

    case FDB_MSG_CURSOR_OPEN:

        logmsg(LOGMSG_USER,
               "XXXX: %llu %s sb=%p CURSOR_OPEN cid=%s tid=%s fl=%x "
               "rootpage=%d version=%d seq=%d SRC[%d, %s]\n",
               t, prefix, sb, comdb2uuidstr((unsigned char *)msg->co.cid, cus),
               comdb2uuidstr((unsigned char *)msg->co.tid, tus), msg->co.flags, msg->co.rootpage, msg->co.version,
               msg->co.seq, msg->co.srcpid, (msg->co.srcname) ? msg->co.srcname : "(unknown)");

        break;

    case FDB_MSG_CURSOR_CLOSE:

        logmsg(LOGMSG_USER, "XXXX: %llu %s CURSOR_CLOSE cid=%s\n", t, prefix,
               comdb2uuidstr((unsigned char *)msg->cc.cid, cus));
        break;

    case FDB_MSG_CURSOR_FIRST:

        logmsg(LOGMSG_USER, "XXXX: %llu %s CURSOR_FIRST cid=%s\n", t, prefix,
               comdb2uuidstr((unsigned char *)msg->cc.cid, cus));
        break;

    case FDB_MSG_CURSOR_LAST:

        logmsg(LOGMSG_USER, "XXXX: %llu %s CURSOR_LAST cid=%s\n", t, prefix,
               comdb2uuidstr((unsigned char *)msg->cc.cid, cus));
        break;

    case FDB_MSG_CURSOR_NEXT:

        logmsg(LOGMSG_USER, "XXXX: %llu %s CURSOR_NEXT cid=%s\n", t, prefix,
               comdb2uuidstr((unsigned char *)msg->cc.cid, cus));
        break;

    case FDB_MSG_CURSOR_PREV:

        logmsg(LOGMSG_USER, "XXXX: %llu %s CURSOR_PREV cid=%s\n", t, prefix,
               comdb2uuidstr((unsigned char *)msg->cc.cid, cus));
        break;

    case FDB_MSG_DATA_ROW:

        logmsg(LOGMSG_USER, "XXXX: %llu %s DATA_ROW cid=%s rc=%d genid=%llx "
                            "datalen=%d datacpylen=%d\n",
               t, prefix, comdb2uuidstr((unsigned char *)msg->dr.cid, cus),
               msg->dr.rc, msg->dr.genid, msg->dr.datalen, msg->dr.datacopylen);
        break;

    case FDB_MSG_CURSOR_FIND:
    case FDB_MSG_CURSOR_FIND_LAST:

        logmsg(LOGMSG_USER, "XXXX: %llu %s %s cid=%s key[%d]=\"", t, prefix,
               (msg->hd.type == FDB_MSG_CURSOR_FIND) ? "CURSOR_FIND"
                                                     : "CURSOR_FIND_LAST",
               comdb2uuidstr((unsigned char *)msg->cf.cid, cus),
               msg->cf.keylen);
        fsnapf(stderr, msg->cf.key, msg->cf.keylen);
        logmsg(LOGMSG_USER, "\"\n");

        break;

    case FDB_MSG_RUN_SQL:

        logmsg(LOGMSG_USER, "XXXX: %llu %s RUN_SQL cid=%s version=%d flags=%x "
                            "sqllen=%d sql=\"%s\" trim=\"%s\"\n",
               t, prefix, comdb2uuidstr((unsigned char *)msg->sq.cid, cus),
               msg->sq.version, msg->sq.flags, msg->sq.sqllen, msg->sq.sql,
               (msg->sq.flags == FDB_RUN_SQL_TRIM) ? "yes" : "no");
        break;

    case FDB_MSG_INSERT_PI:
    case FDB_MSG_INSERT:
        logmsg(LOGMSG_USER, "XXXX: %llu %s sb=%p INSERT cid=%s rootp=%d "
                            "version=%d genid=%llx datalen=%d seq=%d%s%s\n",
               t, prefix, sb, comdb2uuidstr((unsigned char *)msg->in.cid, cus),
               msg->in.rootpage, msg->in.version, msg->in.genid,
               msg->in.datalen, msg->in.seq,
               (msg->in.tblname) ? " tblname=" : "",
               (msg->in.tblname) ? msg->in.tblname : "");
        break;

    case FDB_MSG_DELETE_PI:
    case FDB_MSG_DELETE:

        logmsg(LOGMSG_USER, "XXXX: %llu %s sb=%p DELETE cid=%s rootp=%d "
                            "version=%d genid=%llx seq=%d%s%s\n",
               t, prefix, sb, comdb2uuidstr((unsigned char *)msg->de.cid, cus),
               msg->de.rootpage, msg->de.version, msg->de.genid, msg->de.seq,
               (msg->de.tblname) ? " tblname=" : "",
               (msg->de.tblname) ? msg->de.tblname : "");
        break;

    case FDB_MSG_UPDATE_PI:
    case FDB_MSG_UPDATE:

        logmsg(LOGMSG_USER,
               "XXXX: %llu %s sb=%p UPDATE cid=%s rootp=%d "
               "version=%d oldgenid=%llx genid=%llx datalen=%d seq %d %s%s\n",
               t, prefix, sb, comdb2uuidstr((unsigned char *)msg->up.cid, cus),
               msg->up.rootpage, msg->up.version, msg->up.oldgenid,
               msg->up.genid, msg->up.datalen, msg->up.seq,
               (msg->up.tblname) ? " tblname=" : "",
               (msg->up.tblname) ? msg->up.tblname : "");
        break;

    case FDB_MSG_INDEX:

        logmsg(LOGMSG_USER, "XXXX: %llu %s INDEX cid=%s rootp=%d version=%d "
                            "genid=%llx is_delete=%d, ixnum=%d, ixlen=%d\n",
               t, prefix, comdb2uuidstr((unsigned char *)msg->ix.cid, cus),
               msg->ix.rootpage, msg->ix.version, msg->ix.genid,
               msg->ix.is_delete, msg->ix.ixnum, msg->ix.ixlen);
        break;

    case FDB_MSG_HBEAT:
        break;

    default:
        logmsg(LOGMSG_USER, "%s: %s unknown msg %d\n", __func__, prefix,
               msg->hd.type);
    }
}

/* stuff goes as network endian */
static int fdb_msg_write_message_lk(COMDB2BUF *sb, fdb_msg_t *msg, int flush)
{
    int type;
    int tmp;
    unsigned long long lltmp;
    int rc;
    int idsz;
    int send_dk;

    type = htonl(msg->hd.type);

    rc = cdb2buf_fwrite((char *)&type, 1, sizeof(type), sb);

    if (rc != sizeof(type)) {
        logmsg(LOGMSG_ERROR, "%s: failed to write header rc=%d\n", __func__,
               rc);
        return FDB_ERR_WRITE_IO;
    }

    assert(msg->hd.type & FD_MSG_FLAGS_ISUUID);
    idsz = sizeof(uuid_t);

    send_dk = 0;
    switch (msg->hd.type & FD_MSG_TYPE) {
    case FDB_MSG_TRAN_2PC_BEGIN:

        tmp = htonl(msg->tv.version);
        rc = cdb2buf_fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        rc = cdb2buf_fwrite((char *)msg->tv.tid, 1, idsz, sb);
        if (rc != idsz)
            return FDB_ERR_WRITE_IO;

        tmp = htonl(msg->tv.lvl);
        rc = cdb2buf_fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        tmp = htonl(msg->tv.flags);
        rc = cdb2buf_fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        tmp = htonl(msg->tv.seq);
        rc = cdb2buf_fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        /* dist-txnid */
        tmp = htonl(strlen(msg->tv.dist_txnid) + 1);
        rc = cdb2buf_fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        tmp = ntohl(tmp);
        rc = cdb2buf_fwrite((char *)msg->tv.dist_txnid, 1, tmp, sb);
        if (rc != tmp)
            return FDB_ERR_WRITE_IO;

        /* coordinator-dbname */
        tmp = htonl(strlen(msg->tv.coordinator_dbname) + 1);
        rc = cdb2buf_fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        tmp = ntohl(tmp);
        rc = cdb2buf_fwrite((char *)msg->tv.coordinator_dbname, 1, tmp, sb);
        if (rc != tmp)
            return FDB_ERR_WRITE_IO;

        /* coordinator-tier */
        tmp = htonl(strlen(msg->tv.coordinator_tier) + 1);
        rc = cdb2buf_fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        tmp = ntohl(tmp);
        rc = cdb2buf_fwrite((char *)msg->tv.coordinator_tier, 1, tmp, sb);
        if (rc != tmp)
            return FDB_ERR_WRITE_IO;

        /* timestamp */
        lltmp = flibc_htonll(msg->tv.timestamp);
        rc = cdb2buf_fwrite((char *)&lltmp, 1, sizeof(lltmp), sb);
        if (rc != sizeof(lltmp))
            return FDB_ERR_WRITE_IO;

        if (msg->tv.flags & FDB_MSG_TRANS_AUTH) {
            tmp = htonl(msg->tv.authdtalen);
            rc = cdb2buf_fwrite((char *)&tmp, 1, sizeof(tmp), sb);
            if (rc != sizeof(tmp))
                return FDB_ERR_WRITE_IO;
            rc = cdb2buf_fwrite(msg->tv.authdta, 1, msg->tv.authdtalen, sb);
            if (rc != msg->tv.authdtalen)
                return FDB_ERR_WRITE_IO;
        }

        break;

    case FDB_MSG_TRAN_BEGIN:
    case FDB_MSG_TRAN_PREPARE:
    case FDB_MSG_TRAN_COMMIT:
    case FDB_MSG_TRAN_ROLLBACK:

        rc = cdb2buf_fwrite((char *)msg->tr.tid, 1, idsz, sb);
        if (rc != idsz)
            return FDB_ERR_WRITE_IO;

        tmp = htonl(msg->tr.lvl);
        rc = cdb2buf_fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        tmp = htonl(msg->tr.flags);
        rc = cdb2buf_fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        tmp = htonl(msg->tr.seq);
        rc = cdb2buf_fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        if (msg->tr.flags & FDB_MSG_TRANS_AUTH) {
            tmp = htonl(msg->tr.authdtalen);
            rc = cdb2buf_fwrite((char *)&tmp, 1, sizeof(tmp), sb);
            if (rc != sizeof(tmp))
                return FDB_ERR_WRITE_IO;
            rc = cdb2buf_fwrite(msg->tr.authdta, 1, msg->tr.authdtalen, sb);
            if (rc != msg->tr.authdtalen)
                return FDB_ERR_WRITE_IO;
        }

        break;

    case FDB_MSG_TRAN_2PC_RC:

        tmp = htonl(msg->rv.version);
        rc = cdb2buf_fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        rc = cdb2buf_fwrite((char *)msg->rv.tid, 1, idsz, sb);
        if (rc != idsz)
            return FDB_ERR_WRITE_IO;

        tmp = htonl(msg->rv.rc);
        rc = cdb2buf_fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        tmp = htonl(msg->rv.errstrlen);
        rc = cdb2buf_fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        if (msg->rv.errstrlen && msg->rv.errstr) {
            rc = cdb2buf_fwrite(msg->rv.errstr, 1, msg->rv.errstrlen, sb);
            if (rc != msg->rv.errstrlen)
                return FDB_ERR_WRITE_IO;
        }

        break;

    case FDB_MSG_TRAN_RC:

        rc = cdb2buf_fwrite((char *)msg->rc.tid, 1, idsz, sb);
        if (rc != idsz)
            return FDB_ERR_WRITE_IO;

        tmp = htonl(msg->rc.rc);
        rc = cdb2buf_fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        tmp = htonl(msg->rc.errstrlen);
        rc = cdb2buf_fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        if (msg->rc.errstrlen && msg->rc.errstr) {
            rc = cdb2buf_fwrite(msg->rc.errstr, 1, msg->rc.errstrlen, sb);
            if (rc != msg->rc.errstrlen)
                return FDB_ERR_WRITE_IO;
        }

        break;

    case FDB_MSG_CURSOR_OPEN:

        rc = cdb2buf_fwrite((char *)msg->co.cid, 1, idsz, sb);
        if (rc != idsz)
            return FDB_ERR_WRITE_IO;

        rc = cdb2buf_fwrite((char *)msg->co.tid, 1, idsz, sb);
        if (rc != idsz)
            return FDB_ERR_WRITE_IO;

        tmp = htonl(msg->co.flags);
        rc = cdb2buf_fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        tmp = htonl(msg->co.rootpage);
        rc = cdb2buf_fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        tmp = htonl(msg->co.version);
        rc = cdb2buf_fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        tmp = htonl(msg->co.seq);
        rc = cdb2buf_fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        tmp = htonl(msg->co.srcpid);
        rc = cdb2buf_fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        tmp = htonl(msg->co.srcnamelen);
        rc = cdb2buf_fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        if (msg->co.srcname && msg->co.srcnamelen > 0) {
            rc = cdb2buf_fwrite(msg->co.srcname, 1, msg->co.srcnamelen, sb);
            if (rc != msg->co.srcnamelen)
                return FDB_ERR_WRITE_IO;
        }
        if (msg->co.flags & FDB_MSG_CURSOR_OPEN_FLG_SSL) {
            /*fprintf(stderr, "Writing ssl %d size %d\n", msg->co.ssl,
             * sizeof(tmp));*/
            tmp = htonl(msg->co.ssl);
            rc = cdb2buf_fwrite((char *)&tmp, 1, sizeof(tmp), sb);
            if (rc != sizeof(tmp))
                return FDB_ERR_WRITE_IO;
        }
        if (msg->co.flags & FDB_MSG_CURSOR_OPEN_FLG_AUTH) {
            tmp = htonl(msg->co.authdtalen);
            rc = cdb2buf_fwrite((char *)&tmp, 1, sizeof(tmp), sb);
            if (rc != sizeof(tmp))
                return FDB_ERR_WRITE_IO;
            rc = cdb2buf_fwrite(msg->co.authdta, 1, msg->co.authdtalen, sb);
            if (rc != msg->co.authdtalen)
                return FDB_ERR_WRITE_IO;
        }

        break;

    case FDB_MSG_CURSOR_CLOSE: {
        rc = cdb2buf_fwrite(msg->cc.cid, 1, idsz, sb);
        if (rc != idsz)
            return FDB_ERR_WRITE_IO;

        int haveid;

        assert(msg->hd.type & FD_MSG_FLAGS_ISUUID);
        haveid = !comdb2uuid_is_zero((unsigned char *)msg->cc.tid);

        if (haveid) {
            tmp = htonl(msg->cc.seq);
            rc = cdb2buf_fwrite((char *)&tmp, 1, sizeof(tmp), sb);
            if (rc != sizeof(tmp))
                return FDB_ERR_WRITE_IO;
        }

        break;
    }

    case FDB_MSG_CURSOR_FIRST:
    case FDB_MSG_CURSOR_LAST:
    case FDB_MSG_CURSOR_NEXT:
    case FDB_MSG_CURSOR_PREV:

        rc = cdb2buf_fwrite(msg->cm.cid, 1, idsz, sb);
        if (rc != idsz)
            return FDB_ERR_WRITE_IO;

        break;

    case FDB_MSG_DATA_ROW:

        rc = cdb2buf_fwrite(msg->dr.cid, 1, idsz, sb);
        if (rc != idsz)
            return FDB_ERR_WRITE_IO;

        tmp = htonl(msg->dr.rc);
        rc = cdb2buf_fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        lltmp = flibc_htonll(msg->dr.genid);
        rc = cdb2buf_fwrite((char *)&lltmp, 1, sizeof(lltmp), sb);
        if (rc != sizeof(lltmp))
            return FDB_ERR_WRITE_IO;

        if (unlikely(msg->dr.datacopylen != 0))
            abort();
        tmp = (((unsigned)msg->dr.datalen) << 16) +
              (((unsigned)msg->dr.datalen) >> 16);
        tmp = htonl(tmp);
        rc = cdb2buf_fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        if (msg->dr.data && msg->dr.datalen > 0) {
            rc = cdb2buf_fwrite(msg->dr.data, 1, msg->dr.datalen, sb);
            if (rc != msg->dr.datalen)
                return FDB_ERR_WRITE_IO;
        }
        break;

    case FDB_MSG_CURSOR_FIND:
    case FDB_MSG_CURSOR_FIND_LAST:

        rc = cdb2buf_fwrite(msg->cf.cid, 1, idsz, sb);
        if (rc != idsz)
            return FDB_ERR_WRITE_IO;

        tmp = htonl(msg->cf.keylen);
        rc = cdb2buf_fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        assert(msg->cf.keylen > 0); /* TODO: 0 for match any ? */
        rc = cdb2buf_fwrite(msg->cf.key, 1, msg->cf.keylen, sb);
        if (rc != msg->cf.keylen)
            return FDB_ERR_WRITE_IO;

        break;

    case FDB_MSG_RUN_SQL:
        rc = cdb2buf_fwrite(msg->sq.cid, 1, idsz, sb);
        if (rc != idsz)
            return FDB_ERR_WRITE_IO;
        /*
        rc = cdb2buf_flush(sb);
        if (rc<=0)
        {
           fprintf(stderr, "Ugh %d?\n", __LINE__);
           return FDB_ERR_WRITE_IO;
        }
        */

        tmp = htonl(msg->sq.version);
        rc = cdb2buf_fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;
        /*
     rc = cdb2buf_flush(sb);
     if (rc<=0)
     {
        fprintf(stderr, "Ugh %d?\n", __LINE__);
        return FDB_ERR_WRITE_IO;
     }
     */

        tmp = htonl(msg->sq.flags);
        rc = cdb2buf_fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;
        /*
     rc = cdb2buf_flush(sb);
     if (rc<=0)
     {
        fprintf(stderr, "Ugh %d?\n", __LINE__);
        return FDB_ERR_WRITE_IO;
     }
     */

        tmp = htonl(msg->sq.sqllen);
        rc = cdb2buf_fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;
        /*
     rc = cdb2buf_flush(sb);
     if (rc<=0)
     {
        fprintf(stderr, "Ugh %d?\n", __LINE__);
        return FDB_ERR_WRITE_IO;
     }
     */

        rc = cdb2buf_fwrite(msg->sq.sql, 1, msg->sq.sqllen, sb);
        if (rc != msg->sq.sqllen)
            return FDB_ERR_WRITE_IO;
        /*
     rc = cdb2buf_flush(sb);
     if (rc<=0)
     {
        fprintf(stderr, "Ugh %d?\n", __LINE__);
        return FDB_ERR_WRITE_IO;
     }
     */

        if (msg->sq.flags == FDB_RUN_SQL_TRIM) {
            tmp = htonl(msg->sq.keylen);
            rc = cdb2buf_fwrite((char *)&tmp, 1, sizeof(tmp), sb);
            if (rc != sizeof(tmp))
                return FDB_ERR_WRITE_IO;
            /*
         rc = cdb2buf_flush(sb);
         if (rc<=0)
         {
            fprintf(stderr, "Ugh %d?\n", __LINE__);
            return FDB_ERR_WRITE_IO;
         }
         */

            rc = cdb2buf_fwrite(msg->sq.key, 1, msg->sq.keylen, sb);
            if (rc != msg->sq.keylen)
                return FDB_ERR_WRITE_IO;
        }

        break;

    case FDB_MSG_INSERT_PI:
        send_dk = 1;

    case FDB_MSG_INSERT:

        rc = cdb2buf_fwrite(msg->in.cid, 1, idsz, sb);
        if (rc != idsz)
            return FDB_ERR_WRITE_IO;

        tmp = htonl(msg->in.version);
        rc = cdb2buf_fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        tmp = htonl(msg->in.rootpage);
        rc = cdb2buf_fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        lltmp = flibc_htonll(msg->in.genid);
        rc = cdb2buf_fwrite((char *)&lltmp, 1, sizeof(lltmp), sb);
        if (rc != sizeof(lltmp))
            return FDB_ERR_WRITE_IO;

        if (send_dk) {
            lltmp = flibc_htonll(msg->in.ins_keys);
            rc = cdb2buf_fwrite((char *)&lltmp, 1, sizeof(lltmp), sb);
            if (rc != sizeof(lltmp))
                return FDB_ERR_WRITE_IO;
        }

        if (unlikely(msg->in.datalen > 0 && msg->in.data == NULL)) {
            abort();
        }

        tmp = msg->in.datalen;
        tmp = htonl(tmp);
        rc = cdb2buf_fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        tmp = msg->in.seq;
        tmp = htonl(tmp);
        rc = cdb2buf_fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        if (msg->in.data && msg->in.datalen > 0) {
            rc = cdb2buf_fwrite(msg->in.data, 1, msg->in.datalen, sb);
            if (rc != msg->in.datalen)
                return FDB_ERR_WRITE_IO;
        }

        if (msg->in.tblname) {
            tmp = strlen(msg->in.tblname) + 1;
            tmp = htonl(tmp);
            rc = cdb2buf_fwrite((char *)&tmp, 1, sizeof(tmp), sb);
            if (rc != sizeof(tmp))
                return FDB_ERR_WRITE_IO;
            tmp = ntohl(tmp);
            rc = cdb2buf_fwrite(msg->in.tblname, 1, tmp, sb);
            if (rc != tmp)
                return FDB_ERR_WRITE_IO;
        }

        break;

    case FDB_MSG_DELETE_PI:
        send_dk = 1;

    case FDB_MSG_DELETE:

        rc = cdb2buf_fwrite(msg->de.cid, 1, idsz, sb);
        if (rc != idsz)
            return FDB_ERR_WRITE_IO;

        tmp = htonl(msg->de.version);
        rc = cdb2buf_fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        tmp = htonl(msg->de.rootpage);
        rc = cdb2buf_fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        lltmp = flibc_htonll(msg->de.genid);
        rc = cdb2buf_fwrite((char *)&lltmp, 1, sizeof(lltmp), sb);
        if (rc != sizeof(lltmp))
            return FDB_ERR_WRITE_IO;

        if (send_dk) {
            lltmp = flibc_htonll(msg->de.del_keys);
            rc = cdb2buf_fwrite((char *)&lltmp, 1, sizeof(lltmp), sb);
            if (rc != sizeof(lltmp))
                return FDB_ERR_WRITE_IO;
        }

        tmp = htonl(msg->de.seq);
        rc = cdb2buf_fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        if (msg->de.tblname) {
            tmp = strlen(msg->de.tblname) + 1;
            tmp = htonl(tmp);
            rc = cdb2buf_fwrite((char *)&tmp, 1, sizeof(tmp), sb);
            if (rc != sizeof(tmp))
                return FDB_ERR_WRITE_IO;
            tmp = ntohl(tmp);
            rc = cdb2buf_fwrite(msg->de.tblname, 1, tmp, sb);
            if (rc != tmp)
                return FDB_ERR_WRITE_IO;
        }

        break;

    case FDB_MSG_UPDATE_PI:
        send_dk = 1;

    case FDB_MSG_UPDATE:

        rc = cdb2buf_fwrite(msg->up.cid, 1, idsz, sb);
        if (rc != idsz)
            return FDB_ERR_WRITE_IO;

        tmp = htonl(msg->up.version);
        rc = cdb2buf_fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        tmp = htonl(msg->up.rootpage);
        rc = cdb2buf_fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        lltmp = flibc_htonll(msg->up.oldgenid);
        rc = cdb2buf_fwrite((char *)&lltmp, 1, sizeof(lltmp), sb);
        if (rc != sizeof(lltmp))
            return FDB_ERR_WRITE_IO;

        lltmp = flibc_htonll(msg->up.genid);
        rc = cdb2buf_fwrite((char *)&lltmp, 1, sizeof(lltmp), sb);
        if (rc != sizeof(lltmp))
            return FDB_ERR_WRITE_IO;

        if (send_dk) {
            lltmp = flibc_htonll(msg->up.ins_keys);
            rc = cdb2buf_fwrite((char *)&lltmp, 1, sizeof(lltmp), sb);
            if (rc != sizeof(lltmp))
                return FDB_ERR_WRITE_IO;

            lltmp = flibc_htonll(msg->up.del_keys);
            rc = cdb2buf_fwrite((char *)&lltmp, 1, sizeof(lltmp), sb);
            if (rc != sizeof(lltmp))
                return FDB_ERR_WRITE_IO;
        }

        if (unlikely(msg->up.datalen > 0 && msg->up.data == NULL)) {
            abort();
        }

        tmp = msg->up.datalen;
        tmp = htonl(tmp);
        rc = cdb2buf_fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        tmp = htonl(msg->up.seq);
        rc = cdb2buf_fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        if (msg->up.data && msg->up.datalen > 0) {
            rc = cdb2buf_fwrite(msg->up.data, 1, msg->up.datalen, sb);
            if (rc != msg->up.datalen)
                return FDB_ERR_WRITE_IO;
        }

        if (msg->up.tblname) {
            tmp = strlen(msg->up.tblname) + 1;
            tmp = htonl(tmp);
            rc = cdb2buf_fwrite((char *)&tmp, 1, sizeof(tmp), sb);
            if (rc != sizeof(tmp))
                return FDB_ERR_WRITE_IO;
            tmp = ntohl(tmp);
            rc = cdb2buf_fwrite(msg->up.tblname, 1, tmp, sb);
            if (rc != tmp)
                return FDB_ERR_WRITE_IO;
        }

        break;

    case FDB_MSG_INDEX:

        rc = cdb2buf_fwrite(msg->ix.cid, 1, idsz, sb);
        if (rc != idsz)
            return FDB_ERR_WRITE_IO;

        tmp = htonl(msg->ix.version);
        rc = cdb2buf_fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        tmp = htonl(msg->ix.rootpage);
        rc = cdb2buf_fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        lltmp = flibc_htonll(msg->ix.genid);
        rc = cdb2buf_fwrite((char *)&lltmp, 1, sizeof(lltmp), sb);
        if (rc != sizeof(lltmp))
            return FDB_ERR_WRITE_IO;

        tmp = htonl(msg->ix.is_delete);
        rc = cdb2buf_fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        tmp = htonl(msg->ix.ixnum);
        rc = cdb2buf_fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        if (unlikely(msg->ix.ixlen > 0 && msg->ix.ix == NULL)) {
            abort();
        }

        tmp = msg->ix.ixlen;
        tmp = htonl(tmp);
        rc = cdb2buf_fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        tmp = msg->ix.seq;
        tmp = htonl(tmp);
        rc = cdb2buf_fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;

        if (msg->ix.ix && msg->ix.ixlen > 0) {
            rc = cdb2buf_fwrite(msg->ix.ix, 1, msg->ix.ixlen, sb);
            if (rc != msg->ix.ixlen)
                return FDB_ERR_WRITE_IO;
        }

        break;

    case FDB_MSG_HBEAT: {
        rc = cdb2buf_fwrite(msg->hb.tid, 1, idsz, sb);
        if (rc != idsz)
            return FDB_ERR_WRITE_IO;

        lltmp = flibc_htonll(msg->hb.timespec.tv_sec);
        rc = cdb2buf_fwrite((char *)&lltmp, 1, sizeof(lltmp), sb);
        if (rc != sizeof(lltmp))
            return FDB_ERR_WRITE_IO;

        tmp = htonl(msg->hb.timespec.tv_nsec);
        rc = cdb2buf_fwrite((char *)&tmp, 1, sizeof(tmp), sb);
        if (rc != sizeof(tmp))
            return FDB_ERR_WRITE_IO;
    } break;

    default:
        logmsg(LOGMSG_ERROR, "%s: unknown msg %d\n", __func__, type);
        return FDB_ERR_UNSUPPORTED;
    }

    /*unsigned long long t = osql_log_time();*/
    if (flush /*&& (msg->hd.type != FDB_MSG_RUN_SQL)*/) {
        /*
        fprintf(stderr, "Flushing %llu\n", t);
        */
        rc = cdb2buf_flush(sb);
        if (rc <= 0) {
            /*
               fprintf(stderr, "Ugh?\n");
            */
            return FDB_ERR_WRITE_IO;
        }
    }

    /*
     t = osql_log_time();
     fprintf(stderr, "Done %s %llu\n", __func__, t);*/
    return 0;
}

static int fdb_msg_write_message(COMDB2BUF *sb, fdb_msg_t *msg, int flush)
{
    fdb_tran_t *tran = (fdb_tran_t *)cdb2buf_getuserptr(sb);
    pthread_mutex_t *sb_mtx =
        (tran && !memcmp(tran->magic, "FDBT", 4) && tran->hbeats.tran) ? &tran->hbeats.sb_mtx : NULL;
    if (sb_mtx)
        Pthread_mutex_lock(sb_mtx);
    int rc = fdb_msg_write_message_lk(sb, msg, flush);
    if (sb_mtx)
        Pthread_mutex_unlock(sb_mtx);
    return rc;
}

int (*externalComdb2DeSerializeIdentity)(void **ID, int length, unsigned char *dta) = NULL;

static void _fdb_extract_source_id(struct sqlclntstate *clnt, COMDB2BUF *sb, fdb_msg_t *msg)
{
    clnt->conninfo.node = -1; /*get_origin_mach_by_fd(cdb2buf_fileno(sb));*/

    /* extract source */
    if (msg->co.srcname)
        strncpy(clnt->conninfo.pename, msg->co.srcname, sizeof(clnt->conninfo.pename));
    else
        strncpy(clnt->conninfo.pename, "UNKNOWN", sizeof(clnt->conninfo.pename));
    clnt->conninfo.pename[sizeof(clnt->conninfo.pename) - 1] = '\0';
    clnt->conninfo.pid = msg->co.srcpid;

    /* set up reqlog */
    clnt->argv0 = strdup(clnt->conninfo.pename); /* reset_clnt frees argv0 and stack */
    clnt->stack = strdup("fdb");
    clnt->origin = get_origin_mach_by_buf(sb);
    if (clnt->origin == NULL)
        clnt->origin = intern("???");

    if (msg->co.authdta && fdb_auth_enabled() && externalComdb2DeSerializeIdentity) {
        int rc = externalComdb2DeSerializeIdentity(&clnt->authdata, msg->co.authdtalen, msg->co.authdta);
        if (rc)
            logmsg(LOGMSG_ERROR, "%s: failed to deserialize identity\n", __func__);
    }

    if (clnt->rawnodestats == NULL)
        clnt->rawnodestats = get_raw_node_stats(clnt->argv0, clnt->stack, clnt->plugin.get_identity(clnt), clnt->origin,
                                                cdb2buf_fileno(sb), msg->co.ssl);
}

int fdb_bend_cursor_open(COMDB2BUF *sb, fdb_msg_t *msg, svc_callback_arg_t *arg)
{
    char *cid = msg->co.cid;
    char *tid = msg->co.tid;
    int seq = msg->co.seq;
    struct sqlclntstate *clnt;

    assert((msg->hd.type & FD_MSG_TYPE) == FDB_MSG_CURSOR_OPEN);

    /* create a cursor */
    if (!fdb_svc_cursor_open(tid, cid, msg->co.rootpage, msg->co.version, msg->co.flags, seq, &clnt)) {
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

int fdb_bend_cursor_close(COMDB2BUF *sb, fdb_msg_t *msg, svc_callback_arg_t *arg)
{
    struct sqlclntstate *clnt = (arg) ? arg->clnt : NULL;
    char *cid = msg->cc.cid;
    int rc;

    /* NOTE
       current implementation ignores seq number, since we don't sync on it
     */
    if (clnt && fdb_auth_enabled() && externalComdb2DeSerializeIdentity) {
        rc = externalComdb2DeSerializeIdentity(&clnt->authdata, 0, NULL);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: failed to deserialize identity\n", __func__);
            return rc;
        }
    }

    rc = fdb_svc_cursor_close(cid, (clnt) ? &clnt : NULL);
    if (rc < 0) {
        logmsg(LOGMSG_ERROR, "%s: failed to close cursor rc=%d\n", __func__, rc);
    }

    return rc;
}

int fdb_bend_nop(COMDB2BUF *sb, fdb_msg_t *msg, svc_callback_arg_t *arg)
{
    return -1;
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
    logmsg(LOGMSG_FATAL, "%s: unknown move %d\n", __func__, type);
    abort();
    return -1;
}

int fdb_send_tran_2pc_rc(int version, char *tid, int rcode, char *errstr, COMDB2BUF *sb)
{
    int rc;
    fdb_msg_t lcl_msg = {0}, *msg = &lcl_msg;
    ;

    msg->hd.type = FDB_MSG_TRAN_2PC_RC | FD_MSG_FLAGS_ISUUID;
    fdb_msg_prepare_message(msg);

    memcpy(msg->rv.tid, tid, sizeof(uuid_t));

    msg->rv.version = version;
    msg->rv.rc = rcode;
    msg->rv.errstrlen = (errstr ? strlen(errstr) + 1 : 0);
    msg->rv.errstr = errstr;
    msg->hd.type |= FD_MSG_FLAGS_ISUUID;

    rc = fdb_msg_write_message(sb, msg, 1);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: failed sending fdb 2pc-rc message rc=%d\n", __func__, rc);
        return rc;
    }

    if (gbl_fdb_track) {
        fdb_msg_print_message(sb, msg, "sending 2pc rc");
    }

    return 0;
}

int fdb_bend_send_row(COMDB2BUF *sb, fdb_msg_t *msg, char *cid, unsigned long long genid, char *data, int datalen,
                      char *datacopy, int datacopylen, int ret)
{
    int rc;
    fdb_msg_t lcl_msg;

    if (msg != NULL)
        fdb_msg_clean_message(msg);
    else {
        msg = &lcl_msg;
        bzero(msg, sizeof(*msg));
        msg->hd.type = FDB_MSG_DATA_ROW | FD_MSG_FLAGS_ISUUID;
        fdb_msg_prepare_message(msg);
        memcpy(msg->dr.cid, cid, sizeof(uuid_t));
    }

    msg->hd.type = FDB_MSG_DATA_ROW | FD_MSG_FLAGS_ISUUID;
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

int fdb_bend_cursor_move(COMDB2BUF *sb, fdb_msg_t *msg, svc_callback_arg_t *arg)
{
    unsigned long long genid;
    int rc = 0;
    char *data;
    char *datacopy;
    int datalen;
    int datacopylen;

    rc = fdb_svc_cursor_move(move_type(msg->hd.type), msg->cm.cid, &data, &datalen, &genid, &datacopy, &datacopylen);
    if (rc != IX_FND && rc != IX_NOTFND && rc != IX_PASTEOF && rc != IX_EMPTY) {
        logmsg(LOGMSG_ERROR, "%s: failed move rc %d\n", __func__, rc);
        /*TODO: notify the other side! */
        return rc;
    }

    rc = fdb_bend_send_row(sb, msg, NULL, genid, data, datalen, datacopy, datacopylen, rc);

    return rc;
}

int fdb_bend_cursor_find(COMDB2BUF *sb, fdb_msg_t *msg, svc_callback_arg_t *arg)
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

    assert(msg->hd.type == FDB_MSG_CURSOR_FIND || msg->hd.type == FDB_MSG_CURSOR_FIND_LAST);

    rc = fdb_svc_cursor_find(cid, keylen, key, msg->hd.type == FDB_MSG_CURSOR_FIND_LAST, &genid, &datalen, &data,
                             &datacopy, &datacopylen);
    if (rc != IX_FND && rc != IX_NOTFND && rc != IX_PASTEOF && rc != IX_EMPTY) {
        logmsg(LOGMSG_ERROR, "%s: failed to execute a cursor find rc=%d\n", __func__, rc);
        return rc;
    }

    rc = fdb_bend_send_row(sb, msg, NULL, genid, data, datalen, datacopy, datacopylen, rc);

    return rc;
}

int fdb_bend_run_sql(COMDB2BUF *sb, fdb_msg_t *msg, svc_callback_arg_t *arg)
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

    rc = fdb_appsock_work(cid, clnt, version, flags, sql, sqllen, trim_key, trim_keylen, sb);
    if (rc) {
        /* this happens natural for fractured streams */
        if (gbl_fdb_track)
            logmsg(LOGMSG_USER, "%s: failed to dispatch request rc=%d \"%s\"\n", __func__, rc, sql);
        return -1;
    }

    end_localrpc = osql_log_time();

    if (gbl_time_fdb) {
        logmsg(LOGMSG_USER, "=== DONE running remsql time %llu [%llu -> %llu] rc=%d\n", end_localrpc - start_localrpc,
               start_localrpc, end_localrpc, rc);
    }

    /* was there any error processing? */
    int irc;
    if ((irc = errstat_get_rc(&clnt->fdb_state.xerr)) != 0) {
        /* we need to send back a rc code */
        const char *tmp = errstat_get_str(&clnt->fdb_state.xerr);
        rc = fdb_svc_sql_row(clnt->fdb_state.remote_sql_sb, cid, (char *)tmp, /* the actual row is the errstr */
                             strlen(tmp) + 1, irc);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: fdb_svc_sql_row failed rc=%d\n", __func__, rc);
        }
    }

    return rc;
}

/*====================== WRITE API ===================================*/

extern int gbl_partial_indexes;

int fdb_send_insert(fdb_msg_t *msg, char *cid, int version, int rootpage,
                    char *tblname, unsigned long long genid,
                    unsigned long long ins_keys, int datalen, char *data,
                    int seq, COMDB2BUF *sb)
{
    int rc = 0;
    int send_dk = 0;

    fdb_msg_clean_message(msg);

    if (gbl_partial_indexes && ins_keys != -1ULL)
        send_dk = 1;

    msg->hd.type = send_dk ? FDB_MSG_INSERT_PI : FDB_MSG_INSERT;
    msg->hd.type |= FD_MSG_FLAGS_ISUUID;

    msg->in.cid = cid;
    msg->in.version = version;
    msg->in.rootpage = rootpage;
    msg->in.genid = genid;
    msg->in.ins_keys = ins_keys;
    msg->in.datalen = datalen;
    msg->in.data = data;
    msg->in.seq = seq;
    msg->in.tblname = tblname;

    rc = fdb_msg_write_message(sb, msg, 1);
    if (rc) {
        logmsg(LOGMSG_ERROR,
               "%s: failed sending fdbc cursor_insert message rc=%d\n",
               __func__, rc);
        goto done;
    }

    if (gbl_fdb_track) {
        fdb_msg_print_message(sb, msg, "sending msg");
    }

done:
    msg->in.data = NULL;
    msg->in.datalen = 0;

    return rc;
}

int fdb_send_delete(fdb_msg_t *msg, char *cid, int version, int rootpage,
                    char *tblname, unsigned long long genid,
                    unsigned long long del_keys, int seq,COMDB2BUF *sb)
{
    int rc = 0;
    int send_dk = 0;

    fdb_msg_clean_message(msg);

    if (gbl_partial_indexes && del_keys != -1ULL)
        send_dk = 1;

    msg->hd.type = send_dk ? FDB_MSG_DELETE_PI : FDB_MSG_DELETE;
    msg->hd.type |= FD_MSG_FLAGS_ISUUID;

    msg->de.cid = cid;
    msg->de.version = version;
    msg->de.rootpage = rootpage;
    msg->de.genid = genid;
    msg->de.del_keys = del_keys;
    msg->de.seq = seq;
    msg->de.tblname = tblname;

    rc = fdb_msg_write_message(sb, msg, 1);
    if (rc) {
        logmsg(LOGMSG_ERROR,
               "%s: failed sending fdbc cursor_delete message rc=%d\n",
               __func__, rc);
        goto done;
    }

    if (gbl_fdb_track) {
        fdb_msg_print_message(sb, msg, "sending msg");
    }

done:
    return rc;
}

int fdb_send_update(fdb_msg_t *msg, char *cid, int version, int rootpage,
                    char *tblname, unsigned long long oldgenid,
                    unsigned long long genid, unsigned long long ins_keys,
                    unsigned long long del_keys, int datalen, char *data,
                    int seq, COMDB2BUF *sb)
{
    int rc = 0;
    int send_dk = 0;

    fdb_msg_clean_message(msg);

    if (gbl_partial_indexes && ins_keys != -1ULL && del_keys != -1ULL)
        send_dk = 1;

    msg->hd.type = send_dk ? FDB_MSG_UPDATE_PI : FDB_MSG_UPDATE;
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
    msg->up.tblname = tblname;

    rc = fdb_msg_write_message(sb, msg, 1);
    if (rc) {
        logmsg(LOGMSG_ERROR,
               "%s: failed sending fdbc cursor_update message rc=%d\n",
               __func__, rc);
        goto done;
    }

    if (gbl_fdb_track) {
        fdb_msg_print_message(sb, msg, "sending msg");
    }

done:
    msg->up.data = NULL;
    msg->up.datalen = 0;

    return rc;
}

int fdb_send_index(fdb_msg_t *msg, char *cid, int version, int rootpage,
                   unsigned long long genid, int is_delete, int ixnum,
                   int ixlen, char *ix, int seq, COMDB2BUF *sb)
{
    int rc = 0;

    fdb_msg_clean_message(msg);

    if (!gbl_expressions_indexes)
        return 0;

    msg->hd.type = FDB_MSG_INDEX;
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
        logmsg(LOGMSG_ERROR,
               "%s: failed sending fdbc cursor_index  message rc=%d\n",
               __func__, rc);
        goto done;
    }

done:
    msg->ix.ix = NULL;
    msg->ix.ixlen = 0;

    return rc;
}

int fdb_bend_insert(COMDB2BUF *sb, fdb_msg_t *msg, svc_callback_arg_t *arg)
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

    rc = fdb_svc_cursor_insert(clnt, msg->in.tblname, rootpage, version, genid,
                               data, datalen, seq);

    if (gbl_expressions_indexes) {
        free_cached_idx(clnt->idxInsert);
        free_cached_idx(clnt->idxDelete);
    }

    return rc;
}

int fdb_bend_delete(COMDB2BUF *sb, fdb_msg_t *msg, svc_callback_arg_t *arg)
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

    rc = fdb_svc_cursor_delete(clnt, msg->de.tblname, rootpage, version, genid,
                               seq);

    if (gbl_expressions_indexes) {
        free_cached_idx(clnt->idxInsert);
        free_cached_idx(clnt->idxDelete);
    }

    return rc;
}

int fdb_bend_update(COMDB2BUF *sb, fdb_msg_t *msg, svc_callback_arg_t *arg)
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

    rc = fdb_svc_cursor_update(clnt, msg->up.tblname, rootpage, version,
                               oldgenid, genid, data, datalen, seq);

    if (gbl_expressions_indexes) {
        free_cached_idx(clnt->idxInsert);
        free_cached_idx(clnt->idxDelete);
    }

    return rc;
}

int fdb_bend_index(COMDB2BUF *sb, fdb_msg_t *msg, svc_callback_arg_t *arg)
{
    struct sqlclntstate *clnt = arg->clnt;
    int is_delete = msg->ix.is_delete;
    int ixnum = msg->ix.ixnum;
    char *ix = msg->ix.ix;
    int ixlen = msg->ix.ixlen;

    unsigned char *pIdx = NULL;

    if (is_delete) {
        assert(clnt->idxDelete[ixnum] == NULL);
        clnt->idxDelete[ixnum] = pIdx = malloc(sizeof(int) + ixlen);
    } else {
        assert(clnt->idxInsert[ixnum] == NULL);
        clnt->idxInsert[ixnum] = pIdx = malloc(sizeof(int) + ixlen);
    }
    if (pIdx == NULL) {
        logmsg(LOGMSG_ERROR, "%s:%d malloc %zu failed\n", __func__, __LINE__,
               sizeof(int) + ixlen);
        return -1;
    }
    *((int *)pIdx) = ixlen;
    memcpy((unsigned char *)pIdx + sizeof(int), ix, ixlen);

    return 0;
}

int fdb_send_2pc_begin(struct sqlclntstate *clnt, fdb_msg_t *msg, fdb_tran_t *trans, enum transaction_level lvl,
                       int flags, char *dist_txnid, char *coordinator_dbname, char *coordinator_tier, int64_t timestamp,
                       COMDB2BUF *sb)
{
    int rc;

    /* clean previous whatever */
    fdb_msg_clean_message(msg);

    msg->hd.type = FDB_MSG_TRAN_2PC_BEGIN;
    msg->hd.type |= FD_MSG_FLAGS_ISUUID;

    msg->tv.tid = trans->tid;
    msg->tv.lvl = lvl;
    msg->tv.flags = flags;
    msg->tv.version = FDB_2PC_VER;
    msg->tv.seq = 0; /* the beginnings: there was a zero */
    msg->tv.dist_txnid = dist_txnid;
    msg->tv.coordinator_dbname = coordinator_dbname;
    msg->tv.coordinator_tier = coordinator_tier;
    msg->tv.timestamp = timestamp;

    clnt->authdata = get_authdata(clnt);

    if (clnt->authdata && fdb_auth_enabled() && externalComdb2SerializeIdentity) {
        rc = externalComdb2SerializeIdentity(clnt->authdata, &msg->tv.authdtalen, &msg->tv.authdta);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: failed to serialize identity\n", __func__);
            return rc;
        }
    }

    assert(trans->seq == 0);

    cdb2buf_printf(sb, "%s\n", "rem2pc");

    rc = fdb_msg_write_message(sb, msg, 1);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: failed sending begin transaction message rc=%d\n", __func__, rc);
        return rc;
    }

    rc = fdb_recv_2pc_rc(msg, trans);

    if (gbl_fdb_track) {
        fdb_msg_print_message(sb, msg, "2pc-handshake received");
    }

    return rc;
}

int fdb_send_begin(struct sqlclntstate *clnt, fdb_msg_t *msg, fdb_tran_t *trans, enum transaction_level lvl, int flags,
                   COMDB2BUF *sb)
{
    int rc;

    /* clean previous whatever */
    fdb_msg_clean_message(msg);

    msg->hd.type = FDB_MSG_TRAN_BEGIN;
    msg->hd.type |= FD_MSG_FLAGS_ISUUID;

    msg->tr.tid = trans->tid;
    msg->tr.lvl = lvl;
    msg->tr.flags = flags;
    msg->tr.seq = 0; /* the beginnings: there was a zero */

    clnt->authdata = get_authdata(clnt);

    if (clnt->authdata && fdb_auth_enabled() && externalComdb2SerializeIdentity) {
        rc = externalComdb2SerializeIdentity(clnt->authdata, &msg->tr.authdtalen, &msg->tr.authdta);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: failed to serialize identity\n", __func__);
            return rc;
        }
    }
    assert(trans->seq == 0);

    cdb2buf_printf(sb, "%s\n", "remtran");

    rc = fdb_msg_write_message(sb, msg, 1);
    if (rc) {
        logmsg(LOGMSG_ERROR,
               "%s: failed sending begin transaction message rc=%d\n", __func__,
               rc);
        return rc;
    }

    if (gbl_fdb_track) {
        fdb_msg_print_message(sb, msg, "sending msg");
    }

    return rc;
}

int fdb_send_commit(fdb_msg_t *msg, fdb_tran_t *trans,
                    enum transaction_level lvl, COMDB2BUF *sb)
{
    int rc;

    msg->hd.type = FDB_MSG_TRAN_COMMIT;
    msg->hd.type |= FD_MSG_FLAGS_ISUUID;

    msg->tr.tid = trans->tid;
    msg->tr.lvl = lvl;
    msg->tr.flags = 0;
    msg->tr.seq = trans->seq;

    rc = fdb_msg_write_message(sb, msg, 1);
    if (rc) {
        logmsg(LOGMSG_ERROR,
               "%s: failed sending commit transaction message rc=%d\n",
               __func__, rc);
        return rc;
    }

    if (gbl_fdb_track) {
        fdb_msg_print_message(sb, msg, "sending msg");
    }

    return rc;
}

int fdb_send_rollback(fdb_msg_t *msg, fdb_tran_t *trans,
                      enum transaction_level lvl, COMDB2BUF *sb)
{
    int rc;

    msg->hd.type = FDB_MSG_TRAN_ROLLBACK;
    msg->hd.type |= FD_MSG_FLAGS_ISUUID;

    msg->tr.tid = trans->tid;
    msg->tr.lvl = lvl;
    msg->tr.flags = 0;
    msg->tr.seq = trans->seq;

    rc = fdb_msg_write_message(sb, msg, 1);
    if (rc) {
        logmsg(LOGMSG_ERROR,
               "%s: failed sending commit transaction message rc=%d\n",
               __func__, rc);
        return rc;
    }

    if (gbl_fdb_track) {
        fdb_msg_print_message(sb, msg, "sending msg");
    }

    return rc;
}

int fdb_send_rc(fdb_msg_t *msg, char *tid, int retrc, int errstrlen,
                char *errstr, COMDB2BUF *sb)
{
    int rc;

    fdb_msg_clean_message(msg);

    msg->hd.type = FDB_MSG_TRAN_RC;
    msg->hd.type |= FD_MSG_FLAGS_ISUUID;

    msg->rc.tid = (char *)msg->rc.tiduuid;
    comdb2uuidcpy((unsigned char *)msg->rc.tid, (unsigned char *)tid);

    msg->rc.rc = retrc;
    msg->rc.errstrlen = errstrlen;
    msg->rc.errstr = errstr;

    rc = fdb_msg_write_message(sb, msg, 1);
    if (rc) {
        logmsg(LOGMSG_ERROR,
               "%s: failed sending commit transaction message rc=%d\n",
               __func__, rc);
        return rc;
    }

    if (gbl_fdb_track) {
        fdb_msg_print_message(sb, msg, "sending msg");
    }

    return rc;
}

int fdb_send_heartbeat(fdb_msg_t *msg, char *tid, COMDB2BUF *sb)
{
    int rc;

    fdb_msg_clean_message(msg);

    msg->hd.type = FDB_MSG_HBEAT;
    msg->hd.type |= FD_MSG_FLAGS_ISUUID;

    msg->hb.tid = (char *)msg->hb.tiduuid;
    memmove(msg->hb.tiduuid, tid, sizeof(uuid_t));

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

int fdb_bend_trans_begin(COMDB2BUF *sb, fdb_msg_t *msg, svc_callback_arg_t *arg)
{
    char *tid = msg->tr.tid;
    enum transaction_level lvl = msg->tr.lvl;
    int flags = msg->tr.flags;
    int seq = msg->tr.seq;
    int rc = 0;
    struct sqlclntstate *clnt;

    rc = fdb_svc_trans_begin(tid, lvl, flags, seq, arg->thd, NULL, NULL, NULL, 0, &clnt);

    /* clnt gets set to NULL on error. */
    arg->clnt = clnt;

    if (!rc) {
        arg->flags = flags;
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
                logmsg(LOGMSG_ERROR, "%s:%d malloc failed\n", __func__,
                       __LINE__);
                return -1;
            }
        }
    }

    if (!rc && msg->tr.authdta && fdb_auth_enabled() && externalComdb2DeSerializeIdentity) {
        rc = externalComdb2DeSerializeIdentity(&clnt->authdata, msg->tr.authdtalen, msg->tr.authdta);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: failed to deserialize identity\n", __func__);
            return rc;
        }
    }

    return rc;
}

int fdb_bend_trans_prepare(COMDB2BUF *sb, fdb_msg_t *msg, svc_callback_arg_t *arg)
{
    return -1;
}

int fdb_bend_trans_2pc_begin(COMDB2BUF *sb, fdb_msg_t *msg, svc_callback_arg_t *arg)
{
    char *tid = msg->tv.tid;
    char *dist_txnid = msg->tv.dist_txnid;
    char *coordinator_dbname = msg->tv.coordinator_dbname;
    char *coordinator_tier = msg->tv.coordinator_tier;
    int64_t timestamp = msg->tv.timestamp;
    enum transaction_level lvl = msg->tv.lvl;
    int flags = msg->tv.flags;
    int seq = msg->tv.seq;
    int rc = 0;
    struct sqlclntstate *clnt;

    if (gbl_fdb_incoherence_percentage) {
        if (gbl_fdb_incoherence_percentage <= (rand() % 100)) {
            logmsg(LOGMSG_ERROR, "Test incoherent rejection\n");
            return -1;
        }
    }

    if (!bdb_am_i_coherent(thedb->bdb_env)) {
        logmsg(LOGMSG_ERROR, "Rejecting 2pc transaction, node incoherent\n");
        return -1;
    }

    if (!coordinator_is_allowed(coordinator_dbname, coordinator_tier)) {
        logmsg(LOGMSG_ERROR, "Rejecting 2pc transaction, %s/%s is not an allowed coordinator\n", coordinator_dbname,
               coordinator_tier);
        return -1;
    }

    rc = fdb_svc_trans_begin(tid, lvl, flags, seq, arg->thd, dist_txnid, coordinator_dbname, coordinator_tier,
                             timestamp, &clnt);

    /* clnt gets set to NULL on error. */
    arg->clnt = clnt;

    if (!rc) {
        arg->flags = flags;
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
    /* Send response & version */
    fdb_send_tran_2pc_rc(FDB_2PC_VER, tid, rc, NULL, sb);

    return rc;
}

int fdb_bend_trans_commit(COMDB2BUF *sb, fdb_msg_t *msg, svc_callback_arg_t *arg)
{
    char *tid = msg->tr.tid;
    enum transaction_level lvl = msg->tr.lvl;
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

    /* send back the result now if this is not prepared
     * prepared txns won't wait */
    if (!clnt->is_participant) {
        rc2 = fdb_send_rc(msg, tid, rc, errstrlen, errstr_if_any, sb);
        if (rc2) {
            logmsg(LOGMSG_ERROR, "%s: sending rc error rc=%d rc2=%d\n", __func__, rc, rc2);
            if (!rc)
                rc = rc2;
        }
    }

    return rc;
}

int fdb_bend_trans_rollback(COMDB2BUF *sb, fdb_msg_t *msg, svc_callback_arg_t *arg)
{
    char *tid = msg->tr.tid;
    enum transaction_level lvl = msg->tr.lvl;
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

int fdb_bend_trans_rc(COMDB2BUF *sb, fdb_msg_t *msg, svc_callback_arg_t *arg)
{
    return -1;
}

int fdb_bend_trans_hbeat(COMDB2BUF *sb, fdb_msg_t *msg, svc_callback_arg_t *arg)
{
    /*fprintf(stderr, "Bingo!\n");*/
    /* We don't need to do anything, this just prevents the socket from timing
     * out */

    return 0;
}

static int _check_code_release(COMDB2BUF *sb, char *cid, int code_release)
{
    char errstr[256];
    int errval;
    int rc;

    code_release = fdb_ver_decoded(code_release);

    /* lets make sure we ask for sender to downgrade if its code is too new */
    if (unlikely(code_release > gbl_fdb_default_ver)) {

        snprintf(errstr, sizeof(errstr), "%d protocol %d too high",
                 gbl_fdb_default_ver, code_release);
        errval = FDB_ERR_FDB_VERSION;

        /* we need to send back a rc code */
        rc = fdb_svc_sql_row(sb, cid, errstr, strlen(errstr) + 1, errval);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: fdb_svc_sql_row failed rc=%d\n", __func__,
                   rc);
        }

        return -1;
    }

    return 0;
}

static int handle_remsql_session(COMDB2BUF *sb, struct dbenv *dbenv)
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

    /* let transactional cursors go through */ 
    if (gbl_fdb_incoherence_percentage) {
        if (gbl_fdb_incoherence_percentage <= (rand() % 100)) {
            logmsg(LOGMSG_ERROR, "Test incoherent rejection\n");
            return -1;
        }
    }
    if (!bdb_am_i_coherent(thedb->bdb_env)) {
        if (((flags & FD_MSG_FLAGS_ISUUID) && comdb2uuid_is_zero((unsigned char*)open_msg.tid)) ||
                (*(unsigned long long *)open_msg.tid == 0ULL)) {
            logmsg(LOGMSG_ERROR, "Rejecting standalone remsql, node incoherent\n");
            return -1;
        }
    }


    /* check and protect against newer versions */
    if (_check_code_release(sb, open_msg.cid, open_msg.rootpage)) {
        logmsg(LOGMSG_ERROR, "PROTOCOL TOO NEW %d, asking to downgrade to %d\n",
               fdb_ver_decoded(open_msg.rootpage), gbl_fdb_default_ver);
        return 0;
    }

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

            rc2 = fdb_svc_cursor_close(open_msg.cid, &arg.clnt);
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

int handle_remsql_request(comdb2_appsock_arg_t *arg)
{
    struct dbenv *dbenv;
    struct comdb2buf *sb;
    char line[128];
    int rc = FDB_NOERR;
    static uint64_t old = 0ULL;
    uint64_t now = 0;
    uint64_t then;

    dbenv = arg->dbenv;
    sb = arg->sb;

    /* We will rely on socket drop to determine the end of connection. */
    cdb2buf_settimeout(sb, 0, 0);

    if (gbl_fdb_track_times) {
        now = gettimeofday_ms();
        logmsg(LOGMSG_USER, "RRRRRR start now=%" PRIu64 "\n", now);
    }

    while (1) {
        if (gbl_fdb_track_times) {
            now = gettimeofday_ms();
        }

        rc = handle_remsql_session(sb, dbenv);
        if (gbl_fdb_track)
            logmsg(LOGMSG_USER, "%p: %s: executed session rc=%d\n", (void *)pthread_self(), __func__, rc);

        if (gbl_fdb_track_times) {
            then = gettimeofday_ms();

            if (old == 0ULL) {
                logmsg(LOGMSG_USER, "RRRRRR now=%" PRIu64 " 0 %" PRIu64 "\n",
                       now, then - now);
            } else {
                logmsg(LOGMSG_USER,
                       "RRRRRR now=%" PRIu64 " delta=%" PRIu64 " %" PRIu64 "\n",
                       now, now - old, then - now);
            }
            old = now;
        }

        if (rc == FDB_NOERR) {
            /* we need to read the header again, waiting here */
            rc = cdb2buf_gets(line, sizeof(line), sb);
            if (rc < 0) {
                /* I/O error */
                rc = FDB_NOERR;
                break;
            }
            if (rc != strlen("remsql\n")) {
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
        logmsg(LOGMSG_USER, "%p: %s: done processing\n", (void *)pthread_self(), __func__);

    return rc;
}

int handle_rem2pc_request(comdb2_appsock_arg_t *arg)
{
    struct comdb2buf *sb;
    fdb_msg_tran_t open_msg;
    fdb_msg_t msg;
    int rc = 0;
    svc_callback_arg_t svc_cb_arg = {0};

    sb = arg->sb;

    bzero(&msg, sizeof(msg));

    /* This does insert on behalf of an sql transaction */
    svc_cb_arg.thd = start_sql_thread();

    extern int gbl_fdb_socket_timeout_ms;
    cdb2buf_settimeout(sb, gbl_fdb_socket_timeout_ms, gbl_fdb_socket_timeout_ms);

    rc = fdb_msg_read_message(sb, &msg, 0);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: failed to handle remote cursor request rc=%d\n", __func__, rc);
        return rc;
    }

    if ((msg.hd.type & FD_MSG_TYPE) != FDB_MSG_TRAN_2PC_BEGIN) {
        logmsg(LOGMSG_ERROR, "%s: received wrong packet type=%d, expecting tran begin\n", __func__,
               (msg.hd.type & FD_MSG_TYPE));
        abort();
        return -1;
    }

    memcpy(&open_msg, &msg, sizeof open_msg);
    open_msg.tid = (char *)open_msg.tiduuid;
    uuidstr_t us;
    comdb2uuidstr((unsigned char *)open_msg.tid, us);

    /* TODO: review the no-timeout transaction later on */
    if (gbl_notimeouts) {
        cdb2buf_settimeout(sb, 0, 0);
    }

    while (1) {
        int msg_type;

        if (gbl_fdb_track) {
            fdb_msg_print_message(sb, &msg, "received msg");
        }

        msg_type = (msg.hd.type & FD_MSG_TYPE);

        rc = callbacks[msg_type](sb, &msg, &svc_cb_arg);

        if (msg_type == FDB_MSG_TRAN_COMMIT || msg_type == FDB_MSG_TRAN_PREPARE || msg_type == FDB_MSG_TRAN_ROLLBACK) {
            /* Sanity check:
             * The msg buffer is reused for response, thus in some cases,
             * the type it initially stored, could change.
             * This check ensures that the change adheres with the design.
             * NOTE: participant SKIPS sending an RC back!
             */
            if (!svc_cb_arg.clnt->is_participant && msg_type == FDB_MSG_TRAN_COMMIT && (msg.hd.type & FD_MSG_TYPE) != FDB_MSG_TRAN_RC) {
                abort();
            }
            break;
        }

        if (rc != 0) {
            int rc2;
        clear:
            /* Bail-out if we failed early. */
            if (svc_cb_arg.clnt == 0) {
                goto done;
            }

            rc2 = fdb_svc_trans_rollback(open_msg.tid, open_msg.lvl, svc_cb_arg.clnt,
                                         svc_cb_arg.clnt->dbtran.dtran->fdb_trans.top->seq);
            if (rc2) {
                logmsg(LOGMSG_ERROR, "%s: fdb_svc_trans_rollback failed rc=%d\n", __func__, rc2);
            }
            break;
        }

        /*fprintf(stderr, "XYXY %llu calling recv message\n",
         * osql_log_time());*/
        rc = fdb_msg_read_message(sb, &msg, svc_cb_arg.flags);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: failed to handle remote cursor request rc=%d\n", __func__, rc);
            goto clear;
        }
    }

    if (gbl_expressions_indexes) {
        free(svc_cb_arg.clnt->idxInsert);
        free(svc_cb_arg.clnt->idxDelete);
        svc_cb_arg.clnt->idxInsert = svc_cb_arg.clnt->idxDelete = NULL;
    }

    cleanup_clnt(svc_cb_arg.clnt);
    free(svc_cb_arg.clnt);
    svc_cb_arg.clnt = NULL;

done:
    done_sql_thread();

    return rc;
}

int handle_remtran_request(comdb2_appsock_arg_t *arg)
{
    struct comdb2buf *sb;
    fdb_msg_tran_t open_msg;
    fdb_msg_t msg;
    int rc = 0;
    svc_callback_arg_t svc_cb_arg = {0};

    sb = arg->sb;

    bzero(&msg, sizeof(msg));

    /* This does insert on behalf of an sql transaction */
    svc_cb_arg.thd = start_sql_thread();

    extern int gbl_fdb_socket_timeout_ms;
    cdb2buf_settimeout(sb, gbl_fdb_socket_timeout_ms, gbl_fdb_socket_timeout_ms);

    rc = fdb_msg_read_message(sb, &msg, 0);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: failed to handle remote cursor request rc=%d\n", __func__, rc);
        return rc;
    }

    if ((msg.hd.type & FD_MSG_TYPE) != FDB_MSG_TRAN_BEGIN) {
        logmsg(LOGMSG_ERROR, "%s: received wrong packet type=%d, expecting tran begin\n", __func__,
               (msg.hd.type & FD_MSG_TYPE));
        return -1;
    }

    memcpy(&open_msg, &msg, sizeof open_msg);
    open_msg.tid = (char *)open_msg.tiduuid;
    uuidstr_t us;
    comdb2uuidstr((unsigned char *)open_msg.tid, us);

    /* TODO: review the no-timeout transaction later on */
    if (gbl_notimeouts) {
        cdb2buf_settimeout(sb, 0, 0);
    }

    while (1) {
        int msg_type;

        if (gbl_fdb_track) {
            fdb_msg_print_message(sb, &msg, "received msg");
        }

        msg_type = (msg.hd.type & FD_MSG_TYPE);

        rc = callbacks[msg_type](sb, &msg, &svc_cb_arg);

        if (msg_type == FDB_MSG_TRAN_COMMIT ||
            msg_type == FDB_MSG_TRAN_ROLLBACK) {
            /* Sanity check:
             * The msg buffer is reused for response, thus in some cases,
             * the type it initially stored, could change.
             * This check ensures that the change adheres with the design.
             */
            if (msg_type == FDB_MSG_TRAN_COMMIT &&
                (msg.hd.type & FD_MSG_TYPE) != FDB_MSG_TRAN_RC) {
                abort();
            }
            break;
        }

        if (rc != 0) {
            int rc2;
        clear:
            /* Bail-out if we failed early. */
            if (svc_cb_arg.clnt == 0 || (!svc_cb_arg.clnt->intrans && !svc_cb_arg.clnt->in_client_trans)) {
                goto done;
            }

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
    if (fdb_auth_enabled() && externalComdb2DeSerializeIdentity) {
        rc = externalComdb2DeSerializeIdentity(&(svc_cb_arg.clnt->authdata), 0, NULL);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: failed to deserialize identity\n", __func__);
            return rc;
        }
    }
    cleanup_clnt(svc_cb_arg.clnt);
    free(svc_cb_arg.clnt);
    svc_cb_arg.clnt = NULL;

done:
    done_sql_thread();

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

int handle_alias_request(comdb2_appsock_arg_t *arg)
{
    struct dbenv *dbenv;
    struct comdb2buf *sb;
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

    if (dbenv->master != gbl_myhostname) {
        cdb2buf_printf(sb, "!master swinged, now on %s, please rerun\n",
                    thedb->master);
        cdb2buf_printf(sb, "FAILED\n");
        return APPSOCK_RETURN_CONT;
    }

    tok = segtok(line, llen, &st, &ltok);
    assert((strncmp(tok, "alias", ltok) == 0));

    tok = segtok(line, llen, &st, &ltok);
    if (ltok == 0) {
    usage:
        cdb2buf_printf(sb, "!Usage: [alias set ALIASNAME URL|alias get ALIASNAME| "
                        "alias rem ALIASNAME]\n");
        cdb2buf_printf(sb, "FAILED\n");

        if (op)
            free(op);
        if (aliasname)
            free(aliasname);
        if (url)
            free(url);

        arg->error = -1;
        return APPSOCK_RETURN_ERR;
    }

    op = tokdup(tok, ltok);
    if (!op) {

        arg->error = -1;
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
        cdb2buf_printf(sb, "!%s\n", msg);
        cdb2buf_printf(sb, "FAILED\n");
        arg->error = -1;
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
            cdb2buf_printf(sb, ">%s\n", url);
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
        cdb2buf_printf(sb, "!%s\n", (errstr) ? errstr : "no string");
        cdb2buf_printf(sb, "FAILED\n");

        if (errstr) {
            free(errstr);
        }
    } else {
        cdb2buf_printf(sb, "SUCCESS\n");
    }

    arg->error = rc;
    return (rc) ? APPSOCK_RETURN_ERR : APPSOCK_RETURN_OK;
}
