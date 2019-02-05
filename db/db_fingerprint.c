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
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stddef.h>

#include "logmsg.h"
#include "md5.h"
#include "sql.h"
#include "sqliteInt.h"
#include "util.h"
#include "tohex.h"

hash_t *gbl_fingerprint_hash = NULL;
pthread_mutex_t gbl_fingerprint_hash_mu = PTHREAD_MUTEX_INITIALIZER;

extern int gbl_fingerprint_queries;
extern int gbl_verbose_normalized_queries;
int gbl_fingerprint_max_queries = 1000; /* TODO: Tunable? */

/* Normalize a query - replace literals with ?.  Assumes dest is allocated to be
 * at least as large as source.  This code has been heavily revised to use the
 * upstream SQL normalization API provided by upstream SQLite.  Any subsequent
 * issues with it will be addressed upstream.
 */
static void normalize_query(sqlite3 *db, char *zSql, char **pzNormSql) {
    int rc;
    sqlite3_stmt *p = NULL;

    rc = sqlite3_prepare_v2(db, zSql, -1, &p, 0);

    if (rc == SQLITE_OK) {
        *pzNormSql = sqlite3_mprintf("%s", sqlite3_normalized_sql(p));
    } else if (gbl_verbose_normalized_queries) {
        logmsg(LOGMSG_ERROR,
               "FAILED sqlite3_prepare_v2(%p, {%s}) for normalization, rc=%d, msg=%s\n",
               db, zSql, rc, sqlite3_errmsg(db));
    }

    sqlite3_finalize(p);
}

void add_fingerprint(sqlite3 *sqldb, int64_t cost, int64_t time, int64_t nrows, char *sql) {
    char *zNormSql = NULL;

    normalize_query(sqldb, sql, &zNormSql);
    if (zNormSql != NULL) {
        unsigned char fingerprint[FINGERPRINTSZ];
        MD5Context ctx;
        MD5Init(&ctx);
        MD5Update(&ctx, (unsigned char *)zNormSql, strlen(zNormSql));
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
                sqlite3_free(zNormSql);
                goto done;
            }
            t = calloc(1, sizeof(struct fingerprint_track));
            memcpy(t->fingerprint, fingerprint, FINGERPRINTSZ);
            t->count = 1;
            t->cost = cost;
            t->time = time;
            t->rows = nrows;
            t->normalized_query = zNormSql;
            hash_add(gbl_fingerprint_hash, t);
            if (gbl_verbose_normalized_queries) {
                char fp[FINGERPRINTSZ*2+1]; /* 16 ==> 33 */
                util_tohex(fp, t->fingerprint, FINGERPRINTSZ);
                logmsg(LOGMSG_USER, "[%s] %s -> %s\n", fp, sql, t->normalized_query);
            }
        } else {
            t->count++;
            t->cost += cost;
            t->time += time;
            t->rows += nrows;
            assert( memcmp(t->fingerprint,fingerprint,FINGERPRINTSZ)==0 );
            assert( strcmp(t->normalized_query,zNormSql)==0 );
            sqlite3_free(zNormSql);
        }
        Pthread_mutex_unlock(&gbl_fingerprint_hash_mu);
    }
done:
    ; /* NOTE: Do nothing, silence compiler warning. */
}
