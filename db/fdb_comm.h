/*
   Copyright 2015, 2018, Bloomberg Finance L.P.

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

#ifndef _FDB_COMM_H_
#define _FDB_COMM_H_

#include <sbuf2.h>
#include "comdb2.h"
#include "sql.h"

enum {
    /* NOTE: version 0,1,2 were legacy ping&pong protocol options
       that were replaced by sql */
    FDB_MSG_CURSOR_OPEN_SQL = 3,      /* sql query */
    FDB_MSG_CURSOR_OPEN_SQL_TRAN = 4, /* sql query, possible transactional */
    FDB_MSG_CURSOR_OPEN_SQL_SID = 5   /* sql query with source id */
};

/* keep these flags a bitmask so we can OR them */
enum recv_flags {
    FDB_MSG_TRAN_TBLNAME = 1 /* tblname part of write msg */
};

enum run_sql_flags {
    FDB_RUN_SQL_NORMAL = 0, /* regular request */
    FDB_RUN_SQL_SCHEMA =
        1, /* special schema request, instructs sender to mark all indexes as
              covered indexes */
    FDB_RUN_SQL_TRIM =
        2 /* remote trimms data using the provided key as boundary condition */
};

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
} fdb_msg_tran_t;

typedef struct {
    fdb_msg_header_t type; /* FDB_MS_TRAN_RC */
    char *tid;             /* transaction id */
    int rc;                /* result code */
    int errstrlen;         /* error string length */
    uuid_t tiduuid;
    char *errstr; /* error string, if any */
} fdb_msg_tran_rc_t;

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
} fdb_msg_cursor_open_t;

typedef union {
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
} fdb_msg_t;

typedef struct {
    struct sqlclntstate *clnt;
    struct sql_thread *thd;
    int isuuid;
    int flags;
} svc_callback_arg_t;

typedef int (*fdb_svc_callback_t)(SBUF2 *sb, fdb_msg_t *msg,
                                  svc_callback_arg_t *arg);

int fdb_send_open(fdb_msg_t *msg, char *cid, fdb_tran_t *trans, int rootp,
                  int flags, int version, int isuuid, SBUF2 *sb);
int fdb_send_close(fdb_msg_t *msg, char *cid, char *tid, int isuuid, int seq,
                   SBUF2 *sb);
int fdb_send_move(fdb_msg_t *msg, char *cid, int how, int isuuid, SBUF2 *sb);
int fdb_send_find(fdb_msg_t *msg, char *cid, int last, char *key, int keylen,
                  int isuuid, SBUF2 *sb);

int fdb_send_run_sql(fdb_msg_t *msg, char *cid, int sqllen, char *sql,
                     int version, int keylen, char *key,
                     enum run_sql_flags flags, int isuuid, SBUF2 *sb);

int fdb_recv_row(fdb_msg_t *msg, char *cid, SBUF2 *sb);

int fdb_recv_rc(fdb_msg_t *msg, fdb_tran_t *trans);

int fdb_msg_size(void);

unsigned long long fdb_msg_genid(fdb_msg_t *msg);
int fdb_msg_datalen(fdb_msg_t *msg);
char *fdb_msg_data(fdb_msg_t *msg);

int fdb_remcur_send_row(SBUF2 *sb, fdb_msg_t *msg, char *cid,
                        unsigned long long genid, char *data, int datalen,
                        char *datacopy, int datacopylen, int ret, int isuuid);

int fdb_send_begin(fdb_msg_t *msg, fdb_tran_t *trans,
                   enum transaction_level lvl, int flags, int isuuid,
                   SBUF2 *sb);
int fdb_send_prepare(fdb_msg_t *msg, char *tid, enum transaction_level lvl,
                     int isuuid, SBUF2 *sb);
int fdb_send_commit(fdb_msg_t *msg, fdb_tran_t *trans,
                    enum transaction_level lvl, int isuuid, SBUF2 *sb);
int fdb_send_rollback(fdb_msg_t *msg, fdb_tran_t *trans,
                      enum transaction_level lvl, int isuuid, SBUF2 *sb);
int fdb_send_rc(fdb_msg_t *msg, char *tid, int rc, int errstrlen, char *errstr,
                int isuuid, SBUF2 *sb);

int fdb_send_insert(fdb_msg_t *msg, char *cid, int version, int rootpage,
                    char *tblname, unsigned long long genid,
                    unsigned long long ins_keys, int datalen, char *data,
                    int seq, int isuuid, SBUF2 *sb);
int fdb_send_delete(fdb_msg_t *msg, char *cid, int version, int rootpage,
                    char *tblname, unsigned long long genid,
                    unsigned long long del_keys, int seq, int isuuid,
                    SBUF2 *sb);
int fdb_send_update(fdb_msg_t *msg, char *cid, int version, int rootpage,
                    char *tblname, unsigned long long oldgenid,
                    unsigned long long genid, unsigned long long ins_keys,
                    unsigned long long del_keys, int datalen, char *data,
                    int seq, int isuuid, SBUF2 *sb);
int fdb_send_index(fdb_msg_t *msg, char *cid, int version, int rootpage,
                   unsigned long long genid, int is_delete, int ixnum,
                   int ixlen, char *ix, int seq, int isuuid, SBUF2 *sb);
int fdb_send_heartbeat(fdb_msg_t *msg, char *tid, int isuuid, SBUF2 *sb);

void fdb_msg_print_message(SBUF2 *sb, fdb_msg_t *msg, char *prefix);
void fdb_msg_print_message_uuid(SBUF2 *sb, fdb_msg_t *msg, char *prefix);
void fdb_msg_clean_message(fdb_msg_t *msg);

#endif
