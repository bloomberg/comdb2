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

#ifndef __SQLINTERFACES_H__
#define __SQLINTERFACES_H__

#define SQLHERR_APPSOCK_LIMIT -110
#define SQLHERR_WRONG_DB -111

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stddef.h>
#include <pthread.h>

#include <plhash.h>
#include <segstr.h>

#include <list.h>

#include <sbuf2.h>
#include <bdb_api.h>

#include "comdb2.h"
#include "types.h"
#include "tag.h"

#include <dynschematypes.h>
#include <dynschemaload.h>

#include <sqlite3.h>
#include "comdb2uuid.h"
#include <sqlresponse.pb-c.h>

struct fsqlreq {
    int request;   /* enum fsql_request */
    int flags;     /* request flags */
    int parm;      /* extra word of into differs per request */
    int followlen; /* how much data follows header*/
};
enum { FSQLREQ_LEN = 4 + 4 + 4 + 4 };
BB_COMPILE_TIME_ASSERT(fsqlreq_size, sizeof(struct fsqlreq) == FSQLREQ_LEN);

char *tranlevel_tostr(int lvl);

int sql_check_errors(struct sqlclntstate *clnt, sqlite3 *sqldb,
                     sqlite3_stmt *stmt, const char **errstr);

void sql_dump_hist_statements(void);

enum {
    SQL_PRAGMA_CASE_SENSITIVE_LIKE = 1,
    SQL_PRAGMA_MAXCOST = 2,
    SQL_PRAGMA_TABLESCAN_OK = 3,
    SQL_PRAGMA_TEMPTABLES_OK = 4,
    SQL_PRAGMA_MAXCOST_WARNING = 5,
    SQL_PRAGMA_TABLESCAN_WARNING = 6,
    SQL_PRAGMA_TEMPTABLES_WARNING = 7,
    SQL_PRAGMA_EXTENDED_TM = 8,
    TAGGED_PRAGMA_CLIENT_ENDIAN = 9,
    TAGGED_PRAGMA_EXTENDED_TM = 10,
    SQL_PRAGMA_SP_VERSION = 11,
    SQL_PRAGMA_ERROR = 12
};

struct sql_thread;
double query_cost(struct sql_thread *thd);
void run_internal_sql(char *sql);
void start_internal_sql_clnt(struct sqlclntstate *clnt);
int run_internal_sql_clnt(struct sqlclntstate *clnt, char *sql);
void end_internal_sql_clnt(struct sqlclntstate *clnt);
void reset_clnt_flags(struct sqlclntstate *);

#endif
