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
    /* all previous versions 0-4 are legacy and reserved */
    FDB_MSG_CURSOR_OPEN_SQL_SID = 5 /* latest feature added, SSL */
    ,
    FDB_MSG_CURSOR_OPEN_SQL_SSL = 6 /* SSL supported */
    /* optional fields, powers of 2 */
    ,
    FDB_MSG_CURSOR_OPEN_FLG_SSL = 1 << 16 /* SSL required */
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

union fdb_msg;
typedef union fdb_msg fdb_msg_t;

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

int fdb_bend_send_row(SBUF2 *sb, fdb_msg_t *msg, char *cid,
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
