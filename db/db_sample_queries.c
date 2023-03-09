/*
   Copyright 2023 Bloomberg Finance L.P.

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

#include "sql.h"
#include "tohex.h"
#include "string_ref.h"
#include "reqlog.h"
#include "reqlog_int.h"

#include <cdb2api.h>
#include <str0.h>
#include <unistd.h>
#include <time.h>
#include <cson.h>

hash_t *gbl_sample_queries_hash = NULL;
int gbl_sample_queries = 1;
int gbl_sample_queries_max_queries = 1000;

extern hash_t *gbl_fingerprint_hash;
extern pthread_mutex_t gbl_fingerprint_hash_mu;


static void free_query_field(struct query_field *f) {
    put_ref(&f->zSql_ref);
    if (f->query_plan_ref)
        put_ref(&f->query_plan_ref);
    free(f->params);
    free(f);
}

static char *get_params_string(struct reqlogger *logger)
{
    if (!logger) {
        logmsg(LOGMSG_ERROR, "%s: Logger is NULL\n", __func__);
        return NULL;
    }
    if (!logger->bound_param_cson)
        return NULL;

    cson_buffer buf;
    int rc = cson_output_buffer(logger->bound_param_cson, &buf);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "%s: cson_output_buffer returned rc %d\n", __func__, rc);
        return NULL;
    }

    // JSON data is the first (buf.used) bytes of (buf.mem).
    return strndup((const char *)buf.mem, buf.used);
}

static void free_params_array(struct reqlogger *logger)
{
    if (!logger)
        return;
    if (!logger->bound_param_cson)
        return;

    cson_value_free(logger->bound_param_cson);
    logger->bound_param_cson = NULL;
}

// Assume have fingerprint lock
// Only add the most recent fingerprint + plan fingerprint combo
void add_query_to_samples_queries(const unsigned char *fingerprint, const unsigned char *plan_fingerprint,
                                  struct string_ref *zSql_ref, struct string_ref *query_plan_ref, struct sqlclntstate *clnt) {
    if (!gbl_sample_queries_hash) {
        gbl_sample_queries_hash = hash_init(2 * FINGERPRINTSZ); // fingerprint + plan fingerprint
    }

    char *params = NULL;
    if (param_count(clnt) > 0) {
        struct reqlogger *params_logger = reqlog_alloc();
        char *err = NULL;
        int rc = bind_parameters(params_logger, NULL, clnt, &err, 1);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: bind_parameters error %s\n", __func__, err);
            sqlite3_free(err);
            free_params_array(params_logger);
            reqlog_free(params_logger);
            return;
        }

        params = get_params_string(params_logger);
        free_params_array(params_logger);
        reqlog_free(params_logger);
    }

    // first just fill in the key
    struct query_field *f = calloc(1, sizeof(struct query_field));
    memcpy(f->fingerprint, fingerprint, FINGERPRINTSZ);
    memcpy(f->plan_fingerprint, plan_fingerprint, FINGERPRINTSZ);

    struct query_field *found = hash_find(gbl_sample_queries_hash, f);
    if (found == NULL) {
        /* make sure we haven't generated an unreasonable number of these */
        int nents = hash_get_num_entries(gbl_sample_queries_hash);
        if (nents >= gbl_sample_queries_max_queries) {
            static int complain_once = 1;
            if (complain_once) {
                logmsg(LOGMSG_WARN,
                       "Stopped tracking sample queries, hit max #queries %d.\n",
                       gbl_sample_queries_max_queries);
                complain_once = 0;
            }
            free(f);
            return;
        }

        f->zSql_ref = get_ref(zSql_ref);
        f->query_plan_ref = query_plan_ref ? get_ref(query_plan_ref) : NULL;
        f->params = params;
        f->timestamp = time(NULL);
        hash_add(gbl_sample_queries_hash, f);
    } else {
        free(f);
        // *** update unnormalized query, params, and timestamp (query plan should be the same)
        // this is because fingerprint is based on normalized query, so the same fingerprint can be mapped to multiple unnormalized queries that have different parameters
        put_ref(&found->zSql_ref);
        found->zSql_ref = get_ref(zSql_ref);
        free(found->params);
        found->params = params;
        found->timestamp = time(NULL);
    }
}

int clear_sample_queries() {
    int count = 0;
    Pthread_mutex_lock(&gbl_fingerprint_hash_mu);
    if (!gbl_sample_queries_hash) {
        Pthread_mutex_unlock(&gbl_fingerprint_hash_mu);
        return count;
    }

    count = hash_get_num_entries(gbl_sample_queries_hash);

    void *ent;
    unsigned int bkt;
    for (struct query_field *f = (struct query_field *)hash_first(gbl_sample_queries_hash, &ent, &bkt); f;
         f = (struct query_field *)hash_next(gbl_sample_queries_hash, &ent, &bkt)) {
        free_query_field(f);
    }

    hash_clear(gbl_sample_queries_hash);
    hash_free(gbl_sample_queries_hash);
    gbl_sample_queries_hash = NULL;

    Pthread_mutex_unlock(&gbl_fingerprint_hash_mu);
    return count;
}
