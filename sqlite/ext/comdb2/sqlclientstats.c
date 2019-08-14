/*
   Copyright 2019 Bloomberg Finance L.P.

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
#if (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2)) \
    && !defined(SQLITE_OMIT_VIRTUALTABLE)

#if defined(SQLITE_BUILDING_FOR_COMDB2) && !defined(SQLITE_CORE)
# define SQLITE_CORE 1
#endif

#include <pthread.h>
#include <comdb2systblInt.h>
#include <ezsystables.h>
#include "sql.h"

struct client_sql_systable_data {
    char *host;
    char *task;
    char *fingerprint;
    int64_t count;
    int64_t timems;
    int64_t cost;
    int64_t rows;

    char fp[FINGERPRINTSZ*2+1];
};

struct gather_options {
    struct client_sql_systable_data *stats;
    int nalloced;
    int nstats;
};

static int gather_client_sql_data_fingerprint(void *ent, void *arg) {
    struct gather_options *opt = (struct gather_options*) arg;
    struct query_count *cnt = (struct query_count*) ent;

    if (opt->nstats == opt->nalloced) {
        opt->nalloced = opt->nalloced * 2 + 16;
        void *p = realloc(opt->stats, sizeof(struct client_sql_systable_data) * opt->nalloced);
        if (p == NULL)
            return 1;
        opt->stats = p;
    }
    opt->stats[opt->nstats].host = opt->st->host;
    opt->stats[opt->nstats].task = strdup(opt->st->task);
    util_tohex(opt->stats[opt->nstats].fp, cnt->fingerprint, FINGERPRINTSZ);
    opt->stats[opt->nstats].count = cnt->count;
    opt->stats[opt->nstats].cost = cnt->cost;
    opt->stats[opt->nstats].rows = cnt->rows;
    opt->stats[opt->nstats].timems = cnt->timems;
    opt->nstats++;
    return 0;
}

static int gather_client_sql_data_single(void *ent, void *arg) {
    struct gather_options *opt = (struct gather_options*) arg;
    nodestats_t *st = (nodestats_t*) ent;
    opt->st = st;
    Pthread_mutex_lock(&st->rawtotals.lk);
    int rc = 0;
    if (st->rawtotals.fingerprints)
        rc = hash_for(st->rawtotals.fingerprints, gather_client_sql_data_fingerprint, arg);
    Pthread_mutex_unlock(&st->rawtotals.lk);
    return rc;
}

static int gather_client_sql_data(void **data_out, int *npoints) {
    struct gather_options opt = {0};

    *npoints = 0;

    Pthread_rwlock_wrlock(&clientstats_lk);
    int rc = 0;
    if (clientstats)
        rc = hash_for(clientstats, gather_client_sql_data_single, &opt);
    Pthread_rwlock_unlock(&clientstats_lk);
    if (rc)
        free_client_sql_data(opt.stats, opt.nstats);
    else {
        *npoints = opt.nstats;
        *data_out = opt.stats;
        for (int i = 0; i < opt.nstats; i++)
            opt.stats[i].fingerprint = opt.stats[i].fp;
    }

    return rc;
} 

static void free_client_sql_data(void *data, int npoints) {
    struct client_sql_systable_data *stats = (struct client_sql_systable_data*) data;
    for (int i = 0; i < npoints; i++) {
        free(stats[i].task);
    }
    free(stats);
}


static sqlite3_module systblSQLClientStatsModule = {
    .access_flag = CDB2_ALLOW_USER,
};

int systblSQLClientStats(sqlite3 *db) {
    return create_system_table(db, "comdb2_sql_client_stats", &systblSQLClientStatsModule,
            gather_client_sql_data, free_client_sql_data, sizeof(struct client_sql_systable_data),
            CDB2_CSTRING, "host", -1, offsetof(struct client_sql_systable_data, host),
            CDB2_CSTRING, "task", -1, offsetof(struct client_sql_systable_data, task),
            CDB2_CSTRING, "fingerprint", -1, offsetof(struct client_sql_systable_data, fingerprint),
            CDB2_INTEGER, "qcount", -1, offsetof(struct client_sql_systable_data, count),
            CDB2_INTEGER, "timems", -1, offsetof(struct client_sql_systable_data, timems),
            CDB2_INTEGER, "cost", -1, offsetof(struct client_sql_systable_data, cost),
            CDB2_INTEGER, "rows", -1, offsetof(struct client_sql_systable_data, rows),
            SYSTABLE_END_OF_FIELDS);
}

#endif /* (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2)) \
          && !defined(SQLITE_OMIT_VIRTUALTABLE) */
