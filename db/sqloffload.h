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

#ifndef _SQLOFFLOAD_H_
#define _SQLOFFLOAD_H_

#include <sbuf2.h>

#include "comdb2.h"
#include "errstat.h"
#include "sql.h"

enum OSQL_REQ_TYPE {
    OSQL_REQINV = 0,
    OSQL_BLOCK_REQ = 1,
    OSQL_SOCK_REQ = 2,
    OSQL_RECOM_REQ = 3,
    OSQL_SERIAL_REQ = 4,

    OSQL_BLOCK_REQ_COST = 5,
    OSQL_SOCK_REQ_COST = 6,

    OSQL_SNAPISOL_REQ = 7,
    OSQL_SNAP_UID_REQ = 8,
    OSQL_MAX_REQ = 9,
};

/* codes for blockproc <-> osql comm */
enum {
    OSQL_RC_OK = 0,
    OSQL_RC_DONE = 0x7117,
    OSQL_TERMINATE = 0x7227,
    OSQL_TOOEARLY = 0x7337,
    OSQL_FAILDISPATCH = 0x7447,
    OSQL_UNSUPPORTED = 0x7557,
    OSQL_NOOSQLTHR = 0x7667,
    OSQL_SKIPSEQ = 0x7777
};

/* flags for handle_offloadsql_pool */
enum {
    OSQL_FLAGS_RECORD_COST = 0,
    OSQL_FLAGS_AUTH = 1,
    OSQL_FLAGS_ANALYZE = 2,
    /* sent after a verify to do the <slower> selfdeadlock test */
    OSQL_FLAGS_CHECK_SELFLOCK = 3,
    OSQL_FLAGS_ROWLOCKS = 4,
    OSQL_FLAGS_GENID48 = 5,
    OSQL_FLAGS_SCDONE = 6
};

int osql_open(struct dbenv *dbenv);
void osql_cleanup(void);

void set_osql_maxtransfer(int limit);
int get_osql_maxtransfer(void);

char *osql_breq2a(int op);

int block2_sorese(struct ireq *iq, const char *sql, int sqlen, int block2_type);

int req2netreq(int reqtype);
int req2netrpl(int reqtype);
int tran2req(int dbtran);
int tran2netreq(int dbtran);
int tran2netrpl(int dbtran);

int recom_commit(struct sqlclntstate *clnt, struct sql_thread *thd,
                 char *tzname, int is_distributed_tran);
int recom_abort(struct sqlclntstate *clnt);

int serial_commit(struct sqlclntstate *clnt, struct sql_thread *thd,
                  char *tzname);
int serial_abort(struct sqlclntstate *clnt);

int osql_clean_sqlclntstate(struct sqlclntstate *clnt);
int snapisol_commit(struct sqlclntstate *clnt, struct sql_thread *thd,
                    char *tzname);
int snapisol_abort(struct sqlclntstate *clnt);

void osql_checkboard_for_each(char *host, int (*func)(void *, void *));
int osql_checkboard_master_changed(void *obj, void *arg);
int osql_repository_cancelall(void);

int selectv_range_commit(struct sqlclntstate *clnt);

void osql_postcommit_handle(struct ireq *iq);
void osql_postabort_handle(struct ireq *iq);
#endif
