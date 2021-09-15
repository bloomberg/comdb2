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
#include "strbuf.h"

extern int gbl_old_column_names;

hash_t *gbl_fingerprint_hash;
pthread_mutex_t gbl_fingerprint_hash_mu = PTHREAD_MUTEX_INITIALIZER;

extern int gbl_fingerprint_queries;
extern int gbl_verbose_normalized_queries;
int gbl_fingerprint_max_queries = 1000;

static int free_fingerprint(void *obj, void *arg)
{
    struct fingerprint_track *t = (struct fingerprint_track *)obj;
    if (t != NULL) {
        free(t->zNormSql);
        /* Free cached column names */
        if (t->cachedColCount > 0) {
            for (int i = 0; i < t->cachedColCount; i++) {
                free(t->cachedColNames[i]);
                free(t->cachedColDeclTypes[i]);
            }
            free(t->cachedColNames);
            free(t->cachedColDeclTypes);
        }
        free(t);
    }
    return 0;
}

int clear_fingerprints(void) {
    int count = 0;
    Pthread_mutex_lock(&gbl_fingerprint_hash_mu);
    if (!gbl_fingerprint_hash) {
        Pthread_mutex_unlock(&gbl_fingerprint_hash_mu);
        return count;
    }
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

static int have_type_overrides(struct sqlclntstate *clnt) {
    return clnt->plugin.override_count(clnt) > 0;
}

static void do_name_checks(struct sqlclntstate *clnt, sqlite3_stmt *stmt, struct fingerprint_track *t) {
    t->didNameChecks = 1;
    char *namesep = "";

    t->cachedColCount = stmt_cached_column_count(stmt);
    t->cachedColNames = calloc(sizeof(char *), t->cachedColCount);
    if (t->cachedColNames == NULL) {
        logmsg(LOGMSG_ERROR, "%s:%d out of memory\n", __func__,
                __LINE__);
        return;
    }

    strbuf *oldnames = strbuf_new();
    strbuf *newnames = strbuf_new();

    int name_mismatches = 0;
    for (int i = 0; i < t->cachedColCount; i++) {
        t->cachedColNames[i] =
            strdup(stmt_cached_column_name(stmt, i));

        char *newname = stmt_column_name(stmt, i);
        char *oldname = stmt_cached_column_name(stmt, i);
        if (strcmp(newname, oldname) != 0) {
            /* mismatched column name from new sqlite engine */
            strbuf_appendf(newnames, "%s%s", namesep, stmt_column_name(stmt, i));

            /* mismatched column name from old sqlite engine */
            strbuf_appendf(oldnames, "%s%s", namesep, stmt_cached_column_name(stmt, i));
            namesep = ", ";
            name_mismatches++;
        }
    }

    if (name_mismatches) {
        char fp[FINGERPRINTSZ*2+1]; /* 16 ==> 33 */
        util_tohex(fp, t->fingerprint, FINGERPRINTSZ);

        logmsg(LOGMSG_USER,
                "COLUMN NAME MISMATCH DETECTED! Use 'AS' clause to keep "
                "column names in the result set stable across Comdb2 versions. "
                "fp:%s mismatched -- old: %s new: %s "
                "(https://www.sqlite.org/c3ref/column_name.html)\n",
                fp,
                strbuf_buf(oldnames), strbuf_buf(newnames));
    }


    strbuf_free(oldnames);
    strbuf_free(newnames);
}

static void collect_types(struct sqlclntstate *clnt, sqlite3_stmt *stmt, struct fingerprint_track *t) {
    t->haveTypes = 1;

    t->cachedColCount = stmt_cached_column_count(stmt);
    t->cachedColDeclTypes = calloc(sizeof(char *), t->cachedColCount);
    if (t->cachedColDeclTypes == NULL) {
        logmsg(LOGMSG_ERROR, "%s:%d out of memory\n", __func__,
                __LINE__);
        t->cachedColCount = 0;
        return;
    }

    for (int i = 0; i < t->cachedColCount; i++) {
        t->cachedColDeclTypes[i] =
            strdup(stmt_cached_column_decltype(stmt, i));
    }
}

static void do_type_checks(struct sqlclntstate *clnt, sqlite3_stmt *stmt, struct fingerprint_track *t) {
    t->didTypeChecks = 1;

    /* Temporary buffers to hold list of column names logging */
    strbuf *newtypes = strbuf_new();
    strbuf *oldtypes = strbuf_new();

    int decltype_mismatches = 0;
    char *typesep = "";

    for (int i = 0; i < t->cachedColCount; i++) {
        if (clnt->plugin.needs_decltypes) {
            char *newtype = stmt_column_decltype(stmt, i);
            char *oldtype = stmt_cached_column_decltype(stmt, i);
            if (strcmp(newtype, oldtype) != 0) {
                strbuf_appendf(newtypes, "%s%s %s", typesep, stmt_column_name(stmt, i), newtype);
                strbuf_appendf(oldtypes, "%s%s %s", typesep, stmt_cached_column_name(stmt, i), oldtype);
                typesep = ", ";
                decltype_mismatches++;
            }
        }
    }

    if (decltype_mismatches) {
        char fp[FINGERPRINTSZ*2+1]; /* 16 ==> 33 */
        util_tohex(fp, t->fingerprint, FINGERPRINTSZ);

        logmsg(LOGMSG_USER,
                "TYPE MISMATCH DETECTED! Use the *typed API variant to "
                "specify query output types and keep types stable across Comdb2 versions. "
                "fp:%s mismatched -- old: %s new: %s\n",
                fp,
                strbuf_buf(oldtypes), strbuf_buf(newtypes));
    }
    strbuf_free(oldtypes);
    strbuf_free(newtypes);
}

void add_fingerprint(struct sqlclntstate *clnt, sqlite3_stmt *stmt, const char *zSql, const char *zNormSql,
                     int64_t cost, int64_t time, int64_t prepTime, int64_t nrows,
                     struct reqlogger *logger, unsigned char *fingerprint_out)
{
    assert(zSql);
    size_t nNormSql = 0;
    unsigned char fingerprint[FINGERPRINTSZ];
    calc_fingerprint(zNormSql, &nNormSql, fingerprint);
    Pthread_mutex_lock(&gbl_fingerprint_hash_mu);
    if (gbl_fingerprint_hash == NULL) gbl_fingerprint_hash = hash_init(FINGERPRINTSZ);
    struct fingerprint_track *t = hash_find(gbl_fingerprint_hash, fingerprint);
    if (t == NULL) {
        /* make sure we haven't generated an unreasonable number of these */
        int nents = hash_get_num_entries(gbl_fingerprint_hash);
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
        t->prepTime = prepTime;
        t->rows = nrows;
        t->zNormSql = strdup(zNormSql);
        t->nNormSql = nNormSql;
        hash_add(gbl_fingerprint_hash, t);

        char fp[FINGERPRINTSZ*2+1]; /* 16 ==> 33 */
        util_tohex(fp, t->fingerprint, FINGERPRINTSZ);
        struct reqlogger *statlogger = NULL;

        // dump to statreqs immediately
        statlogger = reqlog_alloc();
        reqlog_diffstat_init(statlogger);
        reqlog_logf(statlogger, REQL_INFO, "fp=%s sql=\"%s\"\n", fp, t->zNormSql);
        reqlog_diffstat_dump(statlogger);
        reqlog_free(statlogger);

        if (gbl_verbose_normalized_queries) {
            logmsg(LOGMSG_USER, "NORMALIZED [%s] {%s} ==> {%s}\n",
                   fp, zSql, t->zNormSql);
        }
    } else {
        t->count++;
        t->cost += cost;
        t->time += time;
        t->prepTime += prepTime;
        t->rows += nrows;
        assert( memcmp(t->fingerprint,fingerprint,FINGERPRINTSZ)==0 );
        assert( t->zNormSql!=zNormSql );
        assert( t->nNormSql==nNormSql );
        assert( strncmp(t->zNormSql,zNormSql,t->nNormSql)==0 );
    }

    if (gbl_old_column_names && t && stmt) {
        if (!t->didNameChecks)
            do_name_checks(clnt, stmt, t);
        if (!t->haveTypes)
            collect_types(clnt, stmt, t);
        if (!t->didTypeChecks && !have_type_overrides(clnt))
            do_type_checks(clnt, stmt, t);
    }

    Pthread_mutex_unlock(&gbl_fingerprint_hash_mu);

    if (logger != NULL) {
        reqlog_set_fingerprint(
            logger, (const char*)fingerprint, FINGERPRINTSZ
        );
    }
done:
    if (fingerprint_out)
        memcpy(fingerprint_out, fingerprint, FINGERPRINTSZ);
}
