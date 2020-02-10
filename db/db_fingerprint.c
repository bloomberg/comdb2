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

#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <stddef.h>

#include "logmsg.h"
#include "md5.h"
#include "sql.h"
#include "util.h"
#include "tohex.h"

extern int gbl_old_column_names;

hash_t *gbl_fingerprint_hash = NULL;
pthread_mutex_t gbl_fingerprint_hash_mu = PTHREAD_MUTEX_INITIALIZER;

extern int gbl_fingerprint_queries;
extern int gbl_verbose_normalized_queries;
int gbl_fingerprint_max_queries = 1000;

static int free_fingerprint(void *obj, void *arg){
    struct fingerprint_track *t = (struct fingerprint_track *)obj;
    if (t != NULL) {
        free(t->zNormSql);
        free(t);
    }
    return 0;
}

int clear_fingerprints(void) {
    int count = 0;
    Pthread_mutex_lock(&gbl_fingerprint_hash_mu);
    hash_info(gbl_fingerprint_hash, NULL, NULL, NULL, NULL, &count, NULL, NULL);
    hash_for(gbl_fingerprint_hash, free_fingerprint, NULL);
    hash_clear(gbl_fingerprint_hash);
    hash_free(gbl_fingerprint_hash);
    gbl_fingerprint_hash = NULL;
    Pthread_mutex_unlock(&gbl_fingerprint_hash_mu);
    return count;
}

void calc_fingerprint(const char *zNormSql, size_t *pnNormSql,
                      unsigned char fingerprint[FINGERPRINTSZ]) {
    MD5Context ctx = {0};

    assert(zNormSql);
    assert(pnNormSql);

    *pnNormSql = strlen(zNormSql);

    MD5Init(&ctx);
    MD5Update(&ctx, (unsigned char *)zNormSql, *pnNormSql);
    memset(fingerprint, 0, FINGERPRINTSZ);
    MD5Final(fingerprint, &ctx);
}

static int compare_column_names(struct sqlclntstate *clnt, sqlite3_stmt *stmt)
{
    if (gbl_old_column_names == 0 || clnt->old_columns_count == 0 ||
        (sqlite3_column_count(stmt) == 0)) {
        // do nothing
        return 0;
    }

    assert(clnt->old_columns_count == sqlite3_column_count(stmt));

    for (int i = 0; i < clnt->old_columns_count; i++) {
        if (strcmp(sqlite3_column_name(stmt, i), clnt->old_columns[i]))
            return 1; // mismatch!
    }
    return 0;
}

void add_fingerprint(struct sqlclntstate *clnt, sqlite3_stmt *stmt,
                     const char *zSql, const char *zNormSql, int64_t cost,
                     int64_t time, int64_t nrows, struct reqlogger *logger,
                     unsigned char *fingerprint_out)
{
    assert(zSql);
    assert(zNormSql);
    size_t nNormSql = strlen(zNormSql);
    unsigned char fingerprint[FINGERPRINTSZ];
    MD5Context ctx;
    MD5Init(&ctx);
    MD5Update(&ctx, (unsigned char *)zNormSql, nNormSql);
    memset(fingerprint, 0, sizeof(fingerprint));
    MD5Final(fingerprint, &ctx);
    Pthread_mutex_lock(&gbl_fingerprint_hash_mu);
    if (gbl_fingerprint_hash == NULL) gbl_fingerprint_hash = hash_init(FINGERPRINTSZ);
    struct fingerprint_track *t = hash_find(gbl_fingerprint_hash, fingerprint);
    if (t == NULL) {
        /* make sure we haven't generated an unreasonable number of these */
        int nents;
        hash_info(gbl_fingerprint_hash, NULL, NULL, NULL, NULL, &nents, NULL, NULL);
        if (nents >= gbl_fingerprint_max_queries) {
            static int complain_once = 1;
            if (complain_once) {
                logmsg(LOGMSG_WARN,
                       "Stopped tracking fingerprints, hit max #queries %d.\n",
                       gbl_fingerprint_max_queries);
                complain_once = 0;
            }
            Pthread_mutex_unlock(&gbl_fingerprint_hash_mu);
            goto done;
        }
        t = calloc(1, sizeof(struct fingerprint_track));
        memcpy(t->fingerprint, fingerprint, FINGERPRINTSZ);
        t->count = 1;
        t->cost = cost;
        t->time = time;
        t->rows = nrows;
        t->zNormSql = strdup(zNormSql);
        t->nNormSql = nNormSql;
        hash_add(gbl_fingerprint_hash, t);

        char fp[FINGERPRINTSZ * 2 + 1]; /* 16 ==> 33 */
        util_tohex(fp, t->fingerprint, FINGERPRINTSZ);
        struct reqlogger *statlogger = NULL;

        // dump to statreqs immediately
        statlogger = reqlog_alloc();
        reqlog_diffstat_init(statlogger);
        reqlog_logf(statlogger, REQL_INFO, "fp=%s sql=\"%s\"\n", fp,
                    t->zNormSql);
        reqlog_diffstat_dump(statlogger);
        reqlog_free(statlogger);

        if (gbl_verbose_normalized_queries) {
            logmsg(LOGMSG_USER, "NORMALIZED [%s] {%s} ==> {%s}\n",
                   fp, zSql, t->zNormSql);
        }

        if (gbl_old_column_names && compare_column_names(clnt, stmt)) {
            logmsg(LOGMSG_USER,
                   "COLUMN NAME MISMATCH DETECTED! Use 'AS' clause to keep "
                   "column names stable, fp:%s "
                   "(https://www.sqlite.org/c3ref/column_name.html)",
                   fp);
        }
    } else {
        t->count++;
        t->cost += cost;
        t->time += time;
        t->rows += nrows;
        assert( memcmp(t->fingerprint,fingerprint,FINGERPRINTSZ)==0 );
        assert( t->zNormSql!=zNormSql );
        assert( t->nNormSql==nNormSql );
        assert( strncmp(t->zNormSql,zNormSql,t->nNormSql)==0 );
    }
    reqlog_set_fingerprint(logger, (const char*)fingerprint, FINGERPRINTSZ);
    Pthread_mutex_unlock(&gbl_fingerprint_hash_mu);
done:
    if (fingerprint_out)
        memcpy(fingerprint_out, fingerprint, FINGERPRINTSZ);
}
