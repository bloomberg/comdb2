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
int gbl_sample_queries_thd_init = 0;
char gbl_sample_queries_dbname[MAX_DBNAME_LENGTH] = {0};
char gbl_sample_queries_tier[SAMPLE_QUERIES_TIER_LENGTH] = {0};
int gbl_sample_queries = 1;
int gbl_sample_queries_wait_time = 3600;

extern hash_t *gbl_fingerprint_hash;
extern pthread_mutex_t gbl_fingerprint_hash_mu;
extern char gbl_dbname[MAX_DBNAME_LENGTH];

// Assume db opened
// Return -1 if error, else 0 if does not exist, 1 if does
static int remote_table_exists(cdb2_hndl_tp *db) {
    int rc;
    char *sql = "select * from comdb2_tables where tablename='comdb2_queries';";

    rc = cdb2_run_statement(db, sql);
    if (rc != CDB2_OK) {
        logmsg(LOGMSG_ERROR, "%s: rc = %d, %s\n", __func__, rc, cdb2_errstr(db));
        return -1;
    }

    rc = cdb2_next_record(db);
    if (rc == CDB2_OK_DONE) {
        logmsg(LOGMSG_ERROR, "%s: comdb2_queries remote table does not exist\n", __func__);
        return 0;
    } else if (rc != CDB2_OK) {
        logmsg(LOGMSG_ERROR, "%s: rc = %d, %s\n", __func__, rc, cdb2_errstr(db));
        return -1;
    }

    rc = cdb2_next_record(db);
    if (rc == CDB2_OK) { // shouldn't happen, there should only be max 1 record
        logmsg(LOGMSG_ERROR, "%s: Unexpected number of records found\n", __func__);
        while (rc == CDB2_OK) { // cycle through rest of records
            rc = cdb2_next_record(db);
        }
        return -1;
    } else if (rc != CDB2_OK_DONE) {
        logmsg(LOGMSG_ERROR, "%s: rc = %d, %s\n", __func__, rc, cdb2_errstr(db));
        return -1;
    }

    return 1;
}

static int local_table_exists() {
    return get_dbtable_by_name("comdb2_queries") != NULL;
}

// Assume db opened
static int add_queries_to_table_int(const struct query_field *f, cdb2_hndl_tp *db) {
    int rc, rc2;

    char *sql = "insert into comdb2_queries values(@db, @fingerprint, @plan_fingerprint, @query, @query_plan, @params, @timestamp)"
                " on conflict (db, fingerprint, plan_fingerprint) do update set query = @query, params = @params, timestamp = @timestamp where @timestamp > timestamp;";
    // only need to update query, params, and timestamp on conflict, query plan should be same. See comment in add_query_to_samples_queries ***
    rc = cdb2_bind_param(db, "db", CDB2_CSTRING, gbl_dbname, strlen(gbl_dbname));
    if (rc != CDB2_OK) {
        logmsg(LOGMSG_ERROR, "%s: rc = %d, %s\n", __func__, rc, cdb2_errstr(db));
        goto done;
    }

    char fp[FINGERPRINTSZ*2+1]; /* 16 ==> 33 */
    util_tohex(fp, (char *)f->fingerprint, FINGERPRINTSZ);
    rc = cdb2_bind_param(db, "fingerprint", CDB2_CSTRING, fp, strlen(fp));
    if (rc != CDB2_OK) {
        logmsg(LOGMSG_ERROR, "%s: rc = %d, %s\n", __func__, rc, cdb2_errstr(db));
        goto done;
    }

    char fp2[FINGERPRINTSZ*2+1]; /* 16 ==> 33 */
    util_tohex(fp2, (char *)f->plan_fingerprint, FINGERPRINTSZ);
    rc = cdb2_bind_param(db, "plan_fingerprint", CDB2_CSTRING, fp2, strlen(fp2));
    if (rc != CDB2_OK) {
        logmsg(LOGMSG_ERROR, "%s: rc = %d, %s\n", __func__, rc, cdb2_errstr(db));
        goto done;
    }

    rc = cdb2_bind_param(db, "query", CDB2_CSTRING, string_ref_cstr(f->zSql_ref), string_ref_len(f->zSql_ref));
    if (rc != CDB2_OK) {
        logmsg(LOGMSG_ERROR, "%s: rc = %d, %s\n", __func__, rc, cdb2_errstr(db));
        goto done;
    }

    rc = cdb2_bind_param(db, "query_plan", CDB2_CSTRING, f->query_plan_ref ? string_ref_cstr(f->query_plan_ref) : NULL,
                         f->query_plan_ref ? string_ref_len(f->query_plan_ref) : 0);
    if (rc != CDB2_OK) {
        logmsg(LOGMSG_ERROR, "%s: rc = %d, %s\n", __func__, rc, cdb2_errstr(db));
        goto done;
    }

    rc = cdb2_bind_param(db, "params", CDB2_CSTRING, f->params, f->params ? strlen(f->params) : 0);
    if (rc != CDB2_OK) {
        logmsg(LOGMSG_ERROR, "%s: rc = %d, %s\n", __func__, rc, cdb2_errstr(db));
        goto done;
    }

    cdb2_client_datetime_t datetime = {0};
    gmtime_r(&f->timestamp, (struct tm*)&datetime.tm);
    strcpy(datetime.tzname, "UTC");
    rc = cdb2_bind_param(db, "timestamp", CDB2_DATETIME, &datetime, sizeof(datetime));
    if (rc != CDB2_OK) {
        logmsg(LOGMSG_ERROR, "%s: rc = %d, %s\n", __func__, rc, cdb2_errstr(db));
        goto done;
    }

    rc = cdb2_run_statement(db, sql);
    if (rc != CDB2_OK) {
        logmsg(LOGMSG_ERROR, "%s: rc = %d, %s\n", __func__, rc, cdb2_errstr(db));
        goto done;
    }

done:
    rc2 = cdb2_clearbindings(db);
    if (rc2 != CDB2_OK) {
        logmsg(LOGMSG_ERROR, "%s: rc = %d, %s\n", __func__, rc2, cdb2_errstr(db));
        return rc2;
    }

    return rc;
}

static void free_query_field(struct query_field *f) {
    put_ref(&f->zSql_ref);
    if (f->query_plan_ref)
        put_ref(&f->query_plan_ref);
    free(f->params);
    free(f);
}

// Assume db opened, return error if can't create table
static int create_local_table(cdb2_hndl_tp *db) {
    int rc;

    if (local_table_exists()) {
        return 0;
    }

    char sql[300];
    sprintf(sql, "create table comdb2_queries { "
                 "schema { "
                 "cstring db[%d] "
                 "cstring fingerprint[%d] "
                 "cstring plan_fingerprint[%d] "
                 "vutf8 query[1000] "
                 "vutf8 query_plan[1000] null=yes "
                 "vutf8 params[1000] null=yes "
                 "datetime timestamp "
                 "} "
                 "keys { "
                 "\"db_fp_qp\" = db + fingerprint + plan_fingerprint "
                 "dup \"timestamp\" = timestamp "
                 "} "
                 "}",
    MAX_DBNAME_LENGTH, FINGERPRINTSZ*2+1, FINGERPRINTSZ*2+1);

    rc = cdb2_run_statement(db, sql);
    if (rc != CDB2_OK) {
        logmsg(LOGMSG_ERROR, "%s: rc = %d, %s\n", __func__, rc, cdb2_errstr(db));
        return rc;
    }
    return 0;
}

static void add_queries_to_table() {
    cdb2_hndl_tp *db;
    int rc;

    char dbname[sizeof(gbl_sample_queries_dbname)];
    char tier[sizeof(gbl_sample_queries_tier)];

    Pthread_mutex_lock(&gbl_fingerprint_hash_mu);
    strncpy0(dbname, gbl_sample_queries_dbname, sizeof(dbname));
    strncpy0(tier, gbl_sample_queries_tier, sizeof(tier));
    Pthread_mutex_unlock(&gbl_fingerprint_hash_mu);

    if ((*dbname && !*tier) || (!*dbname && *tier)) {
        logmsg(LOGMSG_ERROR, "%s: Only specified one of dbname (%s) and tier (%s). Either specify both or none\n", __func__, dbname, tier);
        return;
    }

    rc = cdb2_open(&db, *dbname ? dbname : gbl_dbname, *tier ? tier : "local", 0);
    if (rc != CDB2_OK) {
        logmsg(LOGMSG_ERROR, "%s: rc = %d, %s\n", __func__, rc, cdb2_errstr(db));
        goto done;
    }

    if (*dbname || *tier) {
        if (remote_table_exists(db) != 1)
            goto done;
    } else if (create_local_table(db)) {
        goto done;
    }

    Pthread_mutex_lock(&gbl_fingerprint_hash_mu);
    if (!gbl_sample_queries_hash) {
        Pthread_mutex_unlock(&gbl_fingerprint_hash_mu);
        goto done;
    }

    // transfer from gbl_sample_queries_hash (clear this) to to_add so can unlock mutex
    hash_t *to_add = gbl_sample_queries_hash;
    gbl_sample_queries_hash = NULL;
    Pthread_mutex_unlock(&gbl_fingerprint_hash_mu);

    // add to sql table
    int begin_commit_failed = 0;
    int count_failed = 0;
    rc = cdb2_run_statement(db, "BEGIN");
    if (rc != CDB2_OK) {
        logmsg(LOGMSG_ERROR, "%s: rc = %d, %s\n", __func__, rc, cdb2_errstr(db));
        begin_commit_failed = 1; // just free to_add
    }

    int count = hash_get_num_entries(to_add);
    void *ent;
    unsigned int bkt;
    for (struct query_field *f = (struct query_field *)hash_first(to_add, &ent, &bkt); f;
         f = (struct query_field *)hash_next(to_add, &ent, &bkt)) {
        // if it fails then just discard the query. Don't try adding anymore either
        if (!begin_commit_failed && !count_failed && add_queries_to_table_int(f, db)) {
            count_failed = 1;
        }
        free_query_field(f);
    }

    hash_clear(to_add);
    hash_free(to_add);

    if (!begin_commit_failed) {
        char *commit_str = count_failed ? "ROLLBACK" : "COMMIT";
        rc = cdb2_run_statement(db, commit_str);
        if (rc != CDB2_OK) {
            logmsg(LOGMSG_ERROR, "%s: rc = %d, %s\n", __func__, rc, cdb2_errstr(db));
            begin_commit_failed = 1;
        }
    }

    if (begin_commit_failed || count_failed) // they all fail since we begin/commit
        logmsg(LOGMSG_ERROR, "%s: Failed to insert %d queries into comdb2_queries\n", __func__, count);

done:
    rc = cdb2_close(db);
    if (rc != CDB2_OK) {
        logmsg(LOGMSG_ERROR, "%s: rc = %d, %s\n", __func__, rc, cdb2_errstr(db));
    }
}

static void *add_queries_to_table_thread(void *arg) {
    comdb2_name_thread(__func__);
    while (1) {
        sleep(gbl_sample_queries_wait_time);
        if (gbl_sample_queries)
            add_queries_to_table();
    }
    return NULL;
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
// We don't need any limit on gbl_sample_queries_hash since this is only called from add_fingerprint/query_plan
// Thus this will have the same limit as those data structures
// Only add the most recent fingerprint + plan fingerprint combo
void add_query_to_samples_queries(const unsigned char *fingerprint, const unsigned char *plan_fingerprint,
                                  struct string_ref *zSql_ref, struct string_ref *query_plan_ref, struct sqlclntstate *clnt) {
    pthread_t thread_id;
    if (strstr(string_ref_cstr(zSql_ref), "comdb2_queries")) {
        return;
    }
    if (!gbl_sample_queries_hash) {
        gbl_sample_queries_hash = hash_init(2 * FINGERPRINTSZ); // fingerprint + plan fingerprint
    }
    if (!gbl_sample_queries_thd_init) {
        Pthread_create(&thread_id, NULL, add_queries_to_table_thread, NULL);
        gbl_sample_queries_thd_init = 1;
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
        found->params = params;
        found->timestamp = time(NULL);
    }
}

int clear_sample_queries_queue() {
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
