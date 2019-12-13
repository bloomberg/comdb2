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

/* flags for osql requests */
enum {
    OSQL_FLAGS_RECORD_COST = 0x00000001,
    OSQL_FLAGS_AUTH = 0x00000002,
    OSQL_FLAGS_ANALYZE = 0x00000004,
    /* sent after a verify to do the <slower> selfdeadlock test */
    OSQL_FLAGS_CHECK_SELFLOCK = 0x00000008,
    OSQL_FLAGS_ROWLOCKS = 0x00000010,
    OSQL_FLAGS_GENID48 = 0x00000020,
    OSQL_FLAGS_SCDONE = 0x00000040,
    /* indicates if blkseq reordering is turned on */
    OSQL_FLAGS_REORDER_ON = 0x00000080,
    /* indicates if index reordering is turned on */
    OSQL_FLAGS_REORDER_IDX_ON = 0x00000100,
};

int osql_open(struct dbenv *dbenv);
void osql_cleanup(void);

void set_osql_maxtransfer(int limit);
int get_osql_maxtransfer(void);

char *osql_breq2a(int op);

void block2_sorese(struct ireq *iq, const char *sql, int sqlen,
                   int block2_type);

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

void osql_checkboard_for_each(void *arg, int (*func)(void *, void *));
int osql_checkboard_master_changed(void *obj, void *arg);
int osql_repository_cancelall(void);

int selectv_range_commit(struct sqlclntstate *clnt);

void osql_postcommit_handle(struct ireq *iq);
void osql_postabort_handle(struct ireq *iq);

bool osql_is_index_reorder_on(int osql_flags);
void osql_unset_index_reorder_bit(int *osql_flags);
#endif
