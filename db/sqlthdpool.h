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

#ifndef __SQLTHDPOOL_H__
#define __SQLTHDPOOL_H__

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stddef.h>
#include <pthread.h>

#include <plhash.h>
#include <segstr.h>

#include <list.h>
#include <queue.h>

#include <sbuf2.h>
#include <bdb_api.h>

#include "comdb2.h"
#include "types.h"
#include "tag.h"

#include <dynschematypes.h>
#include <dynschemaload.h>

#include <sqlite3.h>
#include "sqlinterfaces.h"

typedef struct sqlpool {
    int curnthd; /* current number of threads in
                    the pool */
    int nbusy;
    pthread_mutex_t sqlqmtx;
    pthread_cond_t sqlqcnd;
    queue_type *sqlq;

    /* some stats...its 5pm and im weary of implementing pools,
       so just add it to this structure
       */
    long long sqlopendb_msspent;
    int sqlopendb_ntimes;

    long long sqlopendb_fsql_msspent;
    int sqlopendb_fsql_ntimes;

    long long sqlopendb_sqlengine_msspent;
    int sqlopendb_sqlengine_ntimes;

    unsigned long long num_errors;
    unsigned long long num_becomes;
    unsigned long long num_queued;
    unsigned long long num_thr_starts;
    unsigned long long num_thr_ends;

} sqlpool_t;

int init_sqlthread_pool(sqlpool_t *stpool);
int add_sqlthread_req(struct dbtable *db, SBUF2 *sb, sqlpool_t *stpool);
int signal_sqlthread_pool(sqlpool_t *stpool);
void add_dbstat_fsql_open(sqlpool_t *stpool, int ms);
void add_dbstat_enginepool_open(sqlpool_t *stpool, int ms);
void sql_pool_stats(sqlpool_t *stpool);
#endif
