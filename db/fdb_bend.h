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

#ifndef _FDB_BEND_H_
#define _FDB_BEND_H_

typedef struct svc_cursor svc_cursor_t;

/**
 * Initialize the remote cursor service center
 *
 */
int fdb_svc_init(void);

/**
 * Destroy the remove cursor service center
 *
 */
void fdb_svc_destroy(void);

/**
 * Open a local cursor that will serve remote requests
 *
 */
svc_cursor_t *fdb_svc_cursor_open(char *tid, char *cid, int rootpage,
                                  int version, int flags, int seq,
                                  struct sqlclntstate **clnt);

/**
 * Close a local cursor serving remote requests
 *
 */
int fdb_svc_cursor_close(char *cid, struct sqlclntstate **clnt);

/**
 * Move cursor
 *
 */
enum svc_move_types {
    SVC_MOVE_FIRST = 1,
    SVC_MOVE_NEXT = 2,
    SVC_MOVE_PREV = 3,
    SVC_MOVE_LAST = 4
};

/**
 * Move a cursor first/next/prev/last
 *
 */
int fdb_svc_cursor_move(enum svc_move_types type, char *cid, char **data,
                        int *datalen, unsigned long long *genid,
                        char **datacopy, int *datacopylen);

/**
 * Find row matching a key, or the last match
 *
 */
int fdb_svc_cursor_find(char *cid, int keylen, char *key, int last,
                        unsigned long long *genid, int *datalen, char **data,
                        char **datacopy, int *datacopylen);

/**
 * Transaction cursors support
 *
 * Init routine
 */
int fdb_svc_trans_init(struct sqlclntstate *clnt, const char *tid,
                       enum transaction_level lvl, int seq);

/**
 * Transaction cursors support
 *
 * Destroy routine
 */
void fdb_svc_trans_destroy(struct sqlclntstate *clnt);

/**
 * Retrieve a transaction, if any, for a cid
 *
 */
int fdb_svc_trans_get_tid(char *cid, char *tid);

/**
 * Requests for the same transaction can come of differetn sockets
 * and will run in parallel.
 * We need to maintain the internal ordering for certain requests
 * to maintain to transaction consistency and semantics
 *
 * NOTE: this is called with dtran_mtx locked, and returns it LOCKED!
 */
void fdb_sequence_request(struct sqlclntstate *tran_clnt, fdb_tran_t *trans,
                          int seq);

#endif

