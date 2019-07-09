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

hash_t *gbl_fingerprint_hash = NULL;
pthread_mutex_t gbl_fingerprint_hash_mu = PTHREAD_MUTEX_INITIALIZER;

extern int gbl_fingerprint_queries;
extern int gbl_verbose_normalized_queries;
int gbl_fingerprint_max_queries = 1000; /* TODO: Tunable? */

// NOTE: must be called with gbl_fingerprint_hash_mu locked
struct fingerprint_track *find_fingerprint(char *fingerprint) {
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
            return NULL;
        }
        t = calloc(1, sizeof(struct fingerprint_track));
        memcpy(t->fingerprint, fingerprint, FINGERPRINTSZ);
        t->count = 0;
        t->cost = 0;
        t->time = 0;
        t->rows = 0;
        t->zNormSql = NULL;
        t->nNormSql = 0;
        hash_add(gbl_fingerprint_hash, t);
    }
    return t;
}

void add_fingerprint(const char *zSql, const char *zNormSql, int64_t cost,
                     int64_t time, int64_t nrows, struct reqlogger *logger) {
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
    struct fingerprint_track *t = find_fingerprint((char*) fingerprint);
    if (t == NULL) {
        Pthread_mutex_unlock(&gbl_fingerprint_hash_mu);
        return;
    }
    if (t->zNormSql == NULL) {
        t->zNormSql = strdup(zNormSql);
        t->nNormSql = nNormSql;
        if (gbl_verbose_normalized_queries) {
            char fp[FINGERPRINTSZ*2+1]; /* 16 ==> 33 */
            util_tohex(fp, t->fingerprint, FINGERPRINTSZ);
            logmsg(LOGMSG_USER, "NORMALIZED [%s] {%s} ==> {%s}\n",
                    fp, zSql, t->zNormSql);
        }
    } 
    t->count++;
    t->cost += cost;
    t->time += time;
    t->rows += nrows;
    assert( memcmp(t->fingerprint,fingerprint,FINGERPRINTSZ)==0 );
    assert( t->zNormSql!=zNormSql );
    assert( t->nNormSql==nNormSql );
    assert( strncmp(t->zNormSql,zNormSql,t->nNormSql)==0 );
    reqlog_set_fingerprint(logger, (const char*)fingerprint, FINGERPRINTSZ);
    Pthread_mutex_unlock(&gbl_fingerprint_hash_mu);
}

static int update_fingerprint_tunables_write_response_callback(struct sqlclntstate *a, int type, void *data, int n) {
    if (type == RESPONSE_ROW) {
        struct response_data *rsp = (struct response_data*) data;
        sqlite3_stmt *stmt = rsp->stmt;
        int ncols = sqlite3_column_count(stmt);
        for (int i = 0; i < ncols; i++) {
            const unsigned char *fingerprint;
            int threshold;

            fingerprint = sqlite3_column_text(stmt, 0);
            threshold = sqlite3_column_int(stmt, 1);
            printf("fingerprint %s threshold %d\n", fingerprint, threshold);
        }
    }
    return 0;
}

void update_fingerprint_tunables(void) {
    struct plugin_callbacks plugin;
    msys_init_default_callbacks(&plugin);
    plugin.write_response = update_fingerprint_tunables_write_response_callback;
    run_internal_sql_with_callbacks("select fingerprint, longreq_threshold from comdb2_fingerprint_tunables", &plugin);
}
