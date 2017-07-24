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

#include <comdb2.h>
#include <bdb_sqlstat1.h>
#include <strings.h>
#include <assert.h>
#include <limits.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <inttypes.h>
#include <pthread.h>
#include <epochlib.h>
#include "analyze.h"
#include "sql.h"
#include <sqliteInt.h>
#include "block_internal.h"
#include <thread_malloc.h>
#include <autoanalyze.h>
#include <bdb_schemachange.h>
#include <sqlstat1.h>
#include <sqloffload.h>
#include <comdb2_atomic.h>
#include <ctrace.h>
#include <logmsg.h>

/* amount of thread-memory initialized for this thread */
static int analyze_thread_memory = 1048576;

/* global is-running flag */
volatile int analyze_running_flag = 0;
static int analyze_abort_requested = 0;

/* global enable / disable switch */
static int sampled_tables_enabled = 1;

/* sampling threshold defaults to 100 Mb */
long long sampling_threshold = 104857600;

/* hard-maximum number of analyze-table threads */
static int analyze_hard_max_table_threads = 15;

/* maximum number of analyze-table threads */
int analyze_max_table_threads = 5;

/* current number of analyze-sampling threads */
static int analyze_cur_table_threads = 0;

/* maximum number of analyze-sampling threads */
static int analyze_hard_max_comp_threads = 40;

/* maximum number of analyze-sampling threads */
int analyze_max_comp_threads = 10;

/* current number of analyze-sampling threads */
static int analyze_cur_comp_threads = 0;

/* table-thread mutex */
static pthread_mutex_t table_thd_mutex = PTHREAD_MUTEX_INITIALIZER;

/* table-thread cond */
static pthread_cond_t table_thd_cond = PTHREAD_COND_INITIALIZER;

/* comp-thread mutex */
static pthread_mutex_t comp_thd_mutex = PTHREAD_MUTEX_INITIALIZER;

/* comp-thread cond */
static pthread_cond_t comp_thd_cond = PTHREAD_COND_INITIALIZER;

/* comp-state enum */
enum {
    SAMPLING_NOTINITED = 0,
    SAMPLING_STARTUP = 1,
    SAMPLING_RUNNING = 2,
    SAMPLING_COMPLETE = 3,
    SAMPLING_ERROR = 4
};

/* table-state enum */
enum {
    TABLE_NOTINITED = 0,
    TABLE_STARTUP = 1,
    TABLE_RUNNING = 2,
    TABLE_COMPLETE = 3,
    TABLE_FAILED = 4,
    TABLE_SKIPPED = 5
};

/* index-descriptor */
typedef struct index_descriptor {
    pthread_t thread_id;
    int comp_state;
    sampled_idx_t *s_ix;
    struct dbtable *tbl;
    int ix;
    int sampling_pct;
} index_descriptor_t;

/* table-descriptor */
typedef struct table_descriptor {
    pthread_t thread_id;
    int table_state;
    char table[MAXTABLELEN];
    SBUF2 *sb;
    int scale;
    int override_llmeta;
    index_descriptor_t index[MAXINDEX];
} table_descriptor_t;

/* loadStat4 (analyze.c) will ignore all stat entries
 * which have "tbl like 'cdb2.%' */
int backout_stats_frm_tbl(struct sqlclntstate *clnt, const char *table,
                          int stattbl)
{
    char sql[256];
    snprintf(sql, sizeof(sql), "delete from sqlite_stat%d where tbl='%s'",
             stattbl, table);
    int rc = run_internal_sql_clnt(clnt, sql);
    if (rc)
        return rc;

    snprintf(sql, sizeof(sql),
             "update sqlite_stat%d set tbl='%s' where tbl='cdb2.%s.sav'",
             stattbl, table, table);
    rc = run_internal_sql_clnt(clnt, sql);
    return rc;
}

/* Create an sql machine & run an analyze command. */
static int run_sql_part_trans(sqlite3 *sqldb, struct sqlclntstate *client,
                              char *sql, int *changes)
{
    int rc;
    char *msg;

    /* set sql and analyze flavor */
    client->sql = sql;

    /* set thread info */
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    sql_get_query_id(thd);

    /* run sql */
    rc = sqlite3_exec(sqldb, sql, NULL, NULL, &msg);
    if (rc) {
        logmsg(LOGMSG_ERROR, "Analyze failure rc %d: %s\n", rc,
               msg ? msg : "<unknown error>");
        return rc;
    }
    if (changes != NULL)
        *changes = sqlite3_changes(sqldb);

    return rc;
}

static int check_stat1_and_flag(SBUF2 *sb)
{
    /* verify sqlite_stat1 */
    if (NULL == get_dbtable_by_name("sqlite_stat1")) {
        sbuf2printf(sb, "?%s: analyze requires sqlite_stat1 to run\n",
                    __func__);
        sbuf2printf(sb, "FAILED\n");
        logmsg(LOGMSG_ERROR, "%s: analyze requires sqlite_stat1 to run\n", __func__);
        return -1;
    }

    /* check if its already running */
    if (analyze_running_flag) {
        sbuf2printf(sb, "?%s: analyze is already running\n", __func__);
        sbuf2printf(sb, "FAILED\n");
        logmsg(LOGMSG_ERROR, "%s: analyze is already running\n", __func__);
        return -1;
    }

    return 0;
}

void cleanup_stats(SBUF2 *sb)
{
    struct sqlclntstate clnt;
    start_internal_sql_clnt(&clnt);
    clnt.sb = sb;

    if (get_dbtable_by_name("sqlite_stat1")) {
        run_internal_sql_clnt(&clnt,
                              "delete from sqlite_stat1 where idx is null");
        run_internal_sql_clnt(&clnt, "delete from sqlite_stat1 where idx not "
                                     "in (select name from sqlite_master where "
                                     "type='index')");
    }

    if (get_dbtable_by_name("sqlite_stat2"))
        run_internal_sql_clnt(&clnt, "delete from sqlite_stat2 where idx not "
                                     "in (select name from sqlite_master where "
                                     "type='index')");

    if (get_dbtable_by_name("sqlite_stat4"))
        run_internal_sql_clnt(&clnt, "delete from sqlite_stat4 where idx not "
                                     "in (select name from sqlite_master where "
                                     "type='index' UNION select "
                                     "'cdb2.'||name||'.sav' from sqlite_master "
                                     "where type='index')");

    end_internal_sql_clnt(&clnt);
}

/* returns the index or NULL */
static sampled_idx_t *find_sampled_index(struct sqlclntstate *client,
                                         char *table, int ix)
{
    int i;

    /* punt if this wasn't sampled */
    if (NULL == client->sampled_idx_tbl || client->n_cmp_idx <= 0) {
        return NULL;
    }

    /* search for table / index */
    for (i = 0; i < client->n_cmp_idx; i++) {
        sampled_idx_t *s_ix = &(client->sampled_idx_tbl[i]);
        if (!strcmp(table, s_ix->name) && ix == s_ix->ixnum) {
            return &client->sampled_idx_tbl[i];
        }
    }
    return NULL;
}

/* sample (previously misnamed compress) this index */
static int sample_index_int(index_descriptor_t *ix_des)
{
    sampled_idx_t *s_ix = ix_des->s_ix;
    struct dbtable *tbl = ix_des->tbl;
    int sampling_pct = ix_des->sampling_pct;
    int ix = ix_des->ix;
    int rc;
    int bdberr;
    unsigned long long n_recs;
    unsigned long long n_sampled_recs;
    struct temp_table *tmptbl = NULL;

    /* cache the tablename for sqlglue */
    strncpy(s_ix->name, tbl->dbname, sizeof(s_ix->name));

    /* ask bdb to put a summary of this into a temp-table */
    rc = bdb_summarize_table(tbl->handle, ix, sampling_pct, &tmptbl,
                             &n_sampled_recs, &n_recs, &bdberr);

    /* failed */
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: failed to sample table '%s' idx %d\n", __func__,
                tbl->dbname, ix);
        return -1;
    }

    /* fill in structure */
    s_ix->ixnum = ix;
    s_ix->sampled_table = tmptbl;
    s_ix->sampling_pct = sampling_pct;
    s_ix->n_recs = n_recs;
    s_ix->n_sampled_recs = n_sampled_recs;

    return 0;
}

/* spawn a thread to sample an index */
static void *sampling_thread(void *arg)
{
    int rc;
    index_descriptor_t *ix_des = (index_descriptor_t *)arg;

    /* register thread */
    thrman_register(THRTYPE_ANALYZE);
    backend_thread_event(thedb, COMDB2_THR_EVENT_START_RDWR);

    /* update state */
    ix_des->comp_state = SAMPLING_RUNNING;

    /* sample the index */
    rc = sample_index_int(ix_des);

    /* mark the return */
    if (0 == rc) {
        ix_des->comp_state = SAMPLING_COMPLETE;
    } else {
        ix_des->comp_state = SAMPLING_ERROR;
    }

    /* release the thread */
    pthread_mutex_lock(&comp_thd_mutex);
    analyze_cur_comp_threads--;
    pthread_cond_broadcast(&comp_thd_cond);
    pthread_mutex_unlock(&comp_thd_mutex);

    /* cleanup */
    backend_thread_event(thedb, COMDB2_THR_EVENT_DONE_RDWR);

    return NULL;
}

/* dispatch a thread to sample this index */
static int dispatch_sample_index_thread(index_descriptor_t *ix_des)
{
    /* grab lock */
    pthread_mutex_lock(&comp_thd_mutex);

    /* wait for sampling thread availability */
    while (analyze_cur_comp_threads >= analyze_max_comp_threads) {
        pthread_cond_wait(&comp_thd_cond, &comp_thd_mutex);
    }

    /* grab sampling thread */
    analyze_cur_comp_threads++;

    /* release */
    pthread_mutex_unlock(&comp_thd_mutex);

    /* dispatch */
    int rc = pthread_create(&ix_des->thread_id, &gbl_pthread_attr_detached,
                        sampling_thread, ix_des);

    /* return */
    return rc;
}

/* wait for an index to complete */
static int wait_for_index(index_descriptor_t *ix_des)
{
    /* lock index mutex */
    pthread_mutex_lock(&comp_thd_mutex);

    /* wait for the state to change */
    while (ix_des->comp_state == SAMPLING_STARTUP ||
           ix_des->comp_state == SAMPLING_RUNNING) {
        pthread_cond_wait(&comp_thd_cond, &comp_thd_mutex);
    }

    /* release */
    pthread_mutex_unlock(&comp_thd_mutex);

    return 0;
}

/* sample all indicies in this table */
static int sample_indicies(table_descriptor_t *td, struct sqlclntstate *client,
                           struct dbtable *tbl, int sampling_pct, SBUF2 *sb)
{
    int i;
    int err = 0;
    char *table;
    index_descriptor_t *ix_des;

    /* find table to backout */
    table = tbl->dbname;

    /* allocate cmp_idx */
    client->sampled_idx_tbl = calloc(tbl->nix, sizeof(sampled_idx_t));
    if (!client->sampled_idx_tbl) {
        logmsg(LOGMSG_ERROR, "%s: out of memory\n", __func__);
        return -1;
    }

    /* set # sampled ixs */
    client->n_cmp_idx = tbl->nix;

    /* sample indicies */
    for (i = 0; i < client->n_cmp_idx; i++) {
        /* prepare index descriptor */
        ix_des = &td->index[i];
        ix_des->comp_state = SAMPLING_STARTUP;
        ix_des->s_ix = &client->sampled_idx_tbl[i];
        ix_des->tbl = tbl;
        ix_des->ix = i;
        ix_des->sampling_pct = sampling_pct;

        /* start an index sampling thread */
        int rc = dispatch_sample_index_thread(ix_des);
        if (0 != rc) {
            logmsg(LOGMSG_ERROR, "Couldn't start sampling-thread for table '%s' ix %d "
                   "rc=%d\n",
                   table, i, rc);
            err = 1;
            break;
        }
    }

    /* wait for them to complete */
    for (i = 0; i < client->n_cmp_idx; i++) {
        wait_for_index(&td->index[i]);
        if (SAMPLING_COMPLETE != td->index[i].comp_state)
            err = 1;
    }

    return err;
}

/* clean up */
static int cleanup_sampled_indicies(struct sqlclntstate *client, struct dbtable *tbl)
{
    int i;
    int rc;
    int bdberr;

    /* delete sampled temptables */
    for (i = 0; i < client->n_cmp_idx; i++) {
        sampled_idx_t *s_ix = &client->sampled_idx_tbl[i];
        if (!s_ix)
            continue;
        if (!s_ix->sampled_table)
            continue;

        rc = bdb_temp_table_close(tbl->handle, s_ix->sampled_table, &bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: error closing tmptable: rc=%d "
                            "bdberr=%d\n",
                    __func__, rc, bdberr);
        }
    }

    /* free & zero struct */
    free(client->sampled_idx_tbl);
    client->sampled_idx_tbl = NULL;
    return 0;
}

/* Return the requested sampled temptable */
struct temp_table *analyze_get_sampled_temptable(struct sqlclntstate *client,
                                                 char *table, int idx)
{
    sampled_idx_t *s_ix;

    s_ix = find_sampled_index(client, table, idx);
    if (!s_ix)
        return NULL;
    return s_ix->sampled_table;
}

/* Called from sqlite.  Return the number of records for a sampled table */
int analyze_get_nrecs(int iTable)
{
    struct sql_thread *thd;
    struct sqlclntstate *client;
    struct dbtable *db;
    sampled_idx_t *s_ix;
    int ixnum;
    int tblnum;

    /* get client structures */
    thd = pthread_getspecific(query_info_key);
    client = thd->sqlclntstate;

    /* comdb2-ize table-num and ixnum */
    get_sqlite_tblnum_and_ixnum(thd, iTable, &tblnum, &ixnum);
    assert(tblnum < thedb->num_dbs);

    /* retrieve table pointer  */
    if (tblnum < thedb->num_dbs) {
        db = thedb->dbs[tblnum];
    } else {
        return -1;
    }

    /* grab sampled table descriptor */
    s_ix = find_sampled_index(client, db->dbname, ixnum);

    /* return -1 if not sampled.  Sqlite will use the value it calculated. */
    if (!s_ix) {
        return -1;
    }

    /* boundry check return code */
    if (s_ix->n_recs > INT_MAX) {
        return INT_MAX;
    }
    /* return actual number of records */
    else {
        return (int)s_ix->n_recs;
    }
}

/* Return the number of records sampled for an index */
int64_t analyze_get_sampled_nrecs(const char *dbname, int ixnum)
{
    struct sql_thread *thd;
    struct sqlclntstate *client;

    /* get client structures */
    thd = pthread_getspecific(query_info_key);
    client = thd->sqlclntstate;

    /* Punt if this wasn't sampled. */
    if (NULL == client->sampled_idx_tbl || client->n_cmp_idx <= 0) {
        return -1;
    }

    assert(0 <= ixnum && ixnum < client->n_cmp_idx);
    return client->sampled_idx_tbl[ixnum].n_sampled_recs;
}

/* Return 1 if we have this sampled index, 0 otherwise */
int analyze_is_sampled(struct sqlclntstate *client, char *table, int idx)
{
    sampled_idx_t *s_ix;

    s_ix = find_sampled_index(client, table, idx);
    return (NULL != s_ix);
}

static int local_replicate_write_analyze(char *table)
{
    int rc;
    tran_type *trans = NULL;
    long long seqno;
    int nretries = 0;
    struct ireq iq;
    int arc;
    struct block_state blkstate = {0};

    /* skip if not needed */
    if (gbl_replicate_local == 0 || get_dbtable_by_name("comdb2_oplog") == NULL)
        return 0;

    init_fake_ireq(thedb, &iq);
    iq.use_handle = thedb->bdb_env;

    iq.blkstate = &blkstate;
again:
    nretries++;
    if (trans) {
        arc = trans_abort(&iq, trans);
        if (arc) {
            logmsg(LOGMSG_ERROR, "Analyze: trans_abort rc %d\n", arc);
            trans = NULL;
            goto done;
        }
        trans = NULL;
    }
    if (nretries > gbl_maxretries)
        return RC_INTERNAL_RETRY;
    rc = trans_start(&iq, NULL, &trans);
    if (rc) {
        logmsg(LOGMSG_ERROR, "analyze: trans_start rc %d\n", rc);
        goto done;
    }

    if (gbl_replicate_local_concurrent) {
        unsigned long long useqno;
        useqno = bdb_get_timestamp(thedb->bdb_env);
        memcpy(&seqno, &useqno, sizeof(seqno));
    } else
        rc = get_next_seqno(trans, &seqno);
    if (rc) {
        if (rc != RC_INTERNAL_RETRY) {
            logmsg(LOGMSG_ERROR, "get_next_seqno unexpected rc %d\n", rc);
            goto done;
        } else
            goto again;
    }
    iq.blkstate->seqno = seqno;
    iq.blkstate->pos = 0;
    rc = add_oplog_entry(&iq, trans, LCL_OP_ANALYZE, table, strlen(table));
    if (rc == RC_INTERNAL_RETRY)
        goto again;
    if (rc) {
        logmsg(LOGMSG_ERROR, "analyze: add_oplog_entry(analyze) rc %d\n", rc);
        goto done;
    }
    iq.blkstate->seqno = seqno;
    iq.blkstate->pos = 1;
    rc = add_oplog_entry(&iq, trans, LCL_OP_COMMIT, NULL, 0);
    if (rc == RC_INTERNAL_RETRY)
        goto again;
    if (rc) {
        logmsg(LOGMSG_ERROR, "analyze: add_oplog_entry(commit) rc %d\n", rc);
        goto done;
    }
    rc = trans_commit(&iq, trans, gbl_mynode);
    if (rc) {
        logmsg(LOGMSG_ERROR, "analyze: commit rc %d\n", rc);
        goto done;
    }
    trans = NULL;

done:
    if (trans) {
        arc = trans_abort(&iq, trans);
        if (arc)
            logmsg(LOGMSG_ERROR, "analyze: trans_abort rc %d\n", arc);
    }
    return rc;
}

/* get tbl sampling threshold, if NOT -1 set it
 */
static void get_sampling_threshold(char *table, long long *sampling_threshold)
{
    int bdberr = 0;
    long long threshold = 0;
    bdb_get_analyzethreshold_table(NULL, table, &threshold, &bdberr);

#ifdef DEBUG
    printf("retrieving from llmeta saved threshold for table '%s': %lld\n",
           table, threshold);
#endif

    if (threshold > 0) {
        *sampling_threshold = threshold;
#ifdef DEBUG
        printf("Using llmetasaved threshold %d\n", *sampling_threshold);
    }
    else {
        printf("Using default threshold %d\n", *sampling_threshold);
#endif
    }
}

/* get coverage value saved in llmeta for table
 * if value is not saved in llmeta, will not modify scale
 */
static void get_saved_scale(char *table, int *scale)
{
    int bdberr = 0;
    int coveragevalue = 0;
    bdb_get_analyzecoverage_table(NULL, table, &coveragevalue, &bdberr);

#ifdef DEBUG
    printf("retrieving from llmeta saved coverage for table '%s': %d\n", table,
           coveragevalue);
#endif

    if (coveragevalue >= 0 && coveragevalue <= 100) {
        *scale = coveragevalue;
    }
}

int delete_sav(sqlite3 *sqldb, struct sqlclntstate *client, SBUF2 *sb,
               int stat_tbl, const char *table)
{
    char sql[256];
    int ii = 0;
    int more = 1;
    int rc = 0;
    snprintf( sql, sizeof(sql),
            "delete from sqlite_stat%d where tbl='cdb2.%s.sav'", 
            stat_tbl, table);
#ifdef DEBUG
    printf("query '%s'\n", sql);
#endif
    if ( (rc = run_sql_part_trans( sqldb, client, sql, &more)) != 0) {
        logmsg(LOGMSG_ERROR, "delete sav failed");
        return rc;
    }
#ifdef DEBUG
    printf("deleted %d from tbl='cdb2.%s.sav'\n", more, table);
#endif
    ii++;
    return 0;
}


int update_sav(sqlite3 *sqldb, struct sqlclntstate *client, SBUF2 *sb,
               int stat_tbl, const char *table)
{
    char sql[256];
    int ii = 0;
    int more = 1;
    int rc = 0;
    snprintf( sql, sizeof(sql),
            "update sqlite_stat%d set tbl='cdb2.%s.sav' where tbl='%s'", 
            stat_tbl, table, table);
#ifdef DEBUG
    printf("query '%s'\n", sql);
#endif
    if ( (rc = run_sql_part_trans( sqldb, client, sql, &more)) != 0) {
        logmsg(LOGMSG_ERROR, "update sav failed");
        return rc;
    }
#ifdef DEBUG
    printf("updated %d from tbl='cdb2.%s.sav'\n", more, table);
#endif
    ii++;
    return 0;
}


static int analyze_table_int(table_descriptor_t *td,
                             struct thr_handle *thr_self)
{
#ifdef DEBUG
    printf("analyze_table_int() table '%s': scale %d\n", td->table, td->scale);
#endif

    /* make sure we can find this table */
    struct dbtable *tbl = get_dbtable_by_name(td->table);
    if (!tbl) {
        sbuf2printf(td->sb, "?Cannot find table '%s'\n", td->table);
        return -1;
    }

    if (td->override_llmeta == 0) // user did not specify override parameter
        get_saved_scale(td->table, &td->scale);

    if (td->scale == 0) {
        sbuf2printf(td->sb, "?Coverage for table '%s' is 0, skipping analyze\n",
                    td->table);
        logmsg(LOGMSG_INFO, "coverage for table '%s' is 0, skipping analyze\n", td->table);
        return TABLE_SKIPPED;
    }

    /* pass flush_resp fsql_write_response in sqlinterfaces.c
     * to catch where write to stdout is occurring put in gdb:
     * b write if 1==$rdi
     */
    SBUF2 *sb2 = sbuf2open(fileno(stdout), 0);

    struct sqlclntstate clnt;
    start_internal_sql_clnt(&clnt);
    clnt.osql_max_trans = 0; // allow large transactions
    clnt.sb = sb2;
    sbuf2settimeout(clnt.sb, 0, 0);

    logmsg(LOGMSG_INFO, "Analyze thread starting, table %s (%d%%)\n", td->table, td->scale);

    int rc = run_internal_sql_clnt(&clnt, "BEGIN");
    if (rc)
        goto cleanup;

    char sql[256];
    snprintf(sql, sizeof(sql),
             "delete from sqlite_stat1 where tbl='cdb2.%s.sav'", td->table);

    rc = run_internal_sql_clnt(&clnt, sql);
    if (rc)
        goto error;

    snprintf(sql, sizeof(sql),
             "update sqlite_stat1 set tbl='cdb2.%s.sav' where tbl='%s'",
             td->table, td->table);

    rc = run_internal_sql_clnt(&clnt, sql);
    if (rc)
        goto error;

    if (get_dbtable_by_name("sqlite_stat2")) {
        snprintf(sql, sizeof(sql),
                 "delete from sqlite_stat2 where tbl='cdb2.%s.sav'", td->table);

        rc = run_internal_sql_clnt(&clnt, sql);
        if (rc)
            goto error;

        snprintf(sql, sizeof(sql),
                 "update sqlite_stat2 set tbl='cdb2.%s.sav' where tbl='%s'",
                 td->table, td->table);

        rc = run_internal_sql_clnt(&clnt, sql);
        if (rc)
            goto error;
    }

    if (get_dbtable_by_name("sqlite_stat4")) {
        snprintf(sql, sizeof(sql),
                 "delete from sqlite_stat4 where tbl='cdb2.%s.sav'", td->table);

        rc = run_internal_sql_clnt(&clnt, sql);
        if (rc)
            goto error;

        snprintf(sql, sizeof(sql),
                 "update sqlite_stat4 set tbl='cdb2.%s.sav' where tbl='%s'",
                 td->table, td->table);

        rc = run_internal_sql_clnt(&clnt, sql);
        if (rc)
            goto error;
    }

    /* grab the size of the table */
    int64_t totsiz = calc_table_size_analyze(tbl);
    int sampled_table = 0;

    if (sampled_tables_enabled)
        get_sampling_threshold(td->table, &sampling_threshold);

    /* sample if enabled & large */
    if (sampled_tables_enabled && totsiz > sampling_threshold) {
        logmsg(LOGMSG_INFO, "Sampling table '%s' at %d%% coverage\n", td->table, td->scale);
        sampled_table = 1;
        rc = sample_indicies(td, &clnt, tbl, td->scale, td->sb);
        if (rc) {
            snprintf(sql, sizeof(sql), "Sampling table '%s'", td->table);
            goto error;
        }
    }

    clnt.is_analyze = 1;

    /* run analyze as sql query */
    snprintf(sql, sizeof(sql), "analyzesqlite main.\"%s\"", td->table);
    rc = run_internal_sql_clnt(&clnt, sql);
    clnt.is_analyze = 0;
    if (rc)
        goto error;

    snprintf(sql, sizeof(sql), "COMMIT");
    rc = run_internal_sql_clnt(&clnt, sql);

cleanup:
    sbuf2flush(sb2);
    sbuf2free(sb2);

    if (rc) { // send error to client
        sbuf2printf(td->sb, "?Analyze table %s. Error occurred with: %s\n",
                    td->table, sql);
    } else {
        sbuf2printf(td->sb, "?Analyze completed table %s\n", td->table);
       logmsg(LOGMSG_INFO, "Analyze completed, table %s\n", td->table);
    }

    end_internal_sql_clnt(&clnt);
    if (sampled_table) {
        cleanup_sampled_indicies(&clnt, tbl);
    }

    return rc;

error:
    run_internal_sql_clnt(&clnt, "ROLLBACK");
    goto cleanup;
}

/* spawn thread to analyze a table */
static void *table_thread(void *arg)
{
    int rc;
    table_descriptor_t *td = (table_descriptor_t *)arg;
    struct thr_handle *thd_self;

    /* register thread */
    thd_self = thrman_register(THRTYPE_ANALYZE);
    backend_thread_event(thedb, COMDB2_THR_EVENT_START_RDWR);

    stat4dump(1, td->table, 1); /* dump stats in trc file */

    sql_mem_init(NULL);
    thread_memcreate(analyze_thread_memory);

    /* update state */
    td->table_state = TABLE_RUNNING;

    /* analyze the table */
    rc = analyze_table_int(td, thd_self);

    ctrace("analyze_table_int: Table %s, rc = %d\n", td->table, rc);
    /* mark the return */
    if (0 == rc) {
        td->table_state = TABLE_COMPLETE;
        if (thedb->master == gbl_mynode) { // reset directly
            void reset_aa_counter(char *tblname);
            ctrace("analyze: Analyzed Table %s, reseting counter to 0\n", td->table);
            reset_aa_counter(td->table);
        } else {
            ctrace("analyze: Analyzed Table %s, msg to master to reset counter to 0\n", td->table);
            bdb_send_analysed_table_to_master(thedb->bdb_env, td->table);
        }
    } else if (TABLE_SKIPPED == rc) {
        td->table_state = TABLE_SKIPPED;
    } else {
        td->table_state = TABLE_FAILED;
    }

    /* release thread */
    pthread_mutex_lock(&table_thd_mutex);
    analyze_cur_table_threads--;
    pthread_cond_broadcast(&table_thd_cond);
    pthread_mutex_unlock(&table_thd_mutex);
    backend_thread_event(thedb, COMDB2_THR_EVENT_DONE_RDWR);

    thread_memdestroy();
    sql_mem_shutdown(NULL);

    return NULL;
}

/* dispatch a table thread when we're allowed to */
static int dispatch_table_thread(table_descriptor_t *td)
{
    int rc;
    /* grab lock */
    pthread_mutex_lock(&table_thd_mutex);

    /* wait for thread availability */
    while (analyze_cur_table_threads >= analyze_max_table_threads) {
        pthread_cond_wait(&table_thd_cond, &table_thd_mutex);
    }

    /* grab table thread */
    analyze_cur_table_threads++;

    /* release */
    pthread_mutex_unlock(&table_thd_mutex);

    /* dispatch */
    rc = pthread_create(&td->thread_id, &gbl_pthread_attr_detached,
                        table_thread, td);

    /* return */
    return rc;
}

/* wait for table to complete */
static int wait_for_table(table_descriptor_t *td)
{
    /* lock table mutex */
    pthread_mutex_lock(&table_thd_mutex);

    /* wait for the state to change */
    while (td->table_state == TABLE_STARTUP ||
           td->table_state == TABLE_RUNNING) {
        pthread_cond_wait(&table_thd_cond, &table_thd_mutex);
    }

    /* release */
    pthread_mutex_unlock(&table_thd_mutex);

    int rc = 0;
    if (TABLE_COMPLETE == td->table_state) {
        sbuf2printf(td->sb, ">Analyze table '%s' is complete\n", td->table);
    } else if (TABLE_SKIPPED == td->table_state) {
        sbuf2printf(td->sb, ">Analyze table '%s' skipped\n", td->table);
    } else {
        sbuf2printf(td->sb, ">Analyze table '%s' failed\n", td->table);
        rc = -1;
    }

    return rc;
}

/* check for existence of stat1 table -- make sure it exists
 * because without stat1 it is pointless to run analyze.
 */
static inline int check_stat1(SBUF2 *sb)
{
    /* verify sqlite_stat1 */
    if (NULL == get_dbtable_by_name("sqlite_stat1")) {
        sbuf2printf(sb, ">%s: analyze requires sqlite_stat1 to run\n",
                    __func__);
        sbuf2printf(sb, "FAILED\n");
        logmsg(LOGMSG_ERROR, "%s: analyze requires sqlite_stat1 to run\n", __func__);
        return -1;
    }

    return 0;
}

/* set analyze running with atomic ops
 * this makes sure two analyze requests don't race past
 * each-other both setting the flag.
 */
static inline int set_analyze_running(SBUF2 *sb)
{
    analyze_abort_requested = 0; 
    int old = XCHANGE(analyze_running_flag, 1); // set analyze_running_flag
    if (1 == old) // analyze_running_flag was already 1, so bail out
    {
        sbuf2printf(sb, ">%s: analyze is already running\n", __func__);
        sbuf2printf(sb, "FAILED\n");
        logmsg(LOGMSG_ERROR, "%s: analyze is already running\n", __func__);
        return -1;
    }
    return 0;
}

void set_analyze_abort_requested()
{
    if (!analyze_running_flag)
        return;

    analyze_abort_requested = 1; 

    return;
}

int get_analyze_abort_requested()
{
    return analyze_abort_requested;
}


/* analyze 'table' */
int analyze_table(char *table, SBUF2 *sb, int scale, int override_llmeta)
{
    if (check_stat1(sb))
        return -1;

    if (gbl_schema_change_in_progress) {
        logmsg(LOGMSG_ERROR, 
                "%s: Aborting Analyze because schema_change_in_progress\n",
                __func__);
        return -1;
    }

    if (set_analyze_running(sb))
        return -1;

    table_descriptor_t td = {0};
    /* initialize table sync structure */
    td.table_state = TABLE_STARTUP;
    td.sb = sb;
    td.scale = scale;
    td.override_llmeta = override_llmeta;
    strncpy(td.table, table, sizeof(td.table));

    /* dispatch */
    int rc = dispatch_table_thread(&td);

    if (0 != rc) {
       logmsg(LOGMSG_ERROR, "Analyze: Couldn't start table-thread for table '%s' rc=%d\n",
               table, rc);
        logmsg(LOGMSG_ERROR, "Analyze FAILED\n");
        analyze_running_flag = 0;
        return -1;
    }

    /* block waiting for analyze to complete */
    rc = wait_for_table(&td);

    if (rc == 0)
        sbuf2printf(sb, "SUCCESS\n");
    else
        sbuf2printf(sb, "FAILED\n");

    sbuf2flush(sb);
    /* no-longer running */
    analyze_running_flag = 0;

    return rc;
}

/* Analyze all tables in this database */
int analyze_database(SBUF2 *sb, int scale, int override_llmeta)
{
    int rc = 0;
    int i;
    int idx = 0;
    int failed = 0;
    table_descriptor_t *td;

    if (check_stat1(sb))
        return -1;

    if (set_analyze_running(sb))
        return -1;

    /* allocate descriptor */
    td = calloc(thedb->num_dbs, sizeof(table_descriptor_t));

    /* start analyzing each table */
    for (i = 0; i < thedb->num_dbs; i++) {
        /* skip sqlite_stat */
        if (is_sqlite_stat(thedb->dbs[i]->dbname)) {
            continue;
        }

        /* initialize table-descriptor */
        td[idx].table_state = TABLE_STARTUP;
        td[idx].sb = sb;
        td[idx].scale = scale;
        td[idx].override_llmeta = override_llmeta;
        strncpy(td[idx].table, thedb->dbs[i]->dbname, sizeof(td[idx].table));

        /* dispatch analyze table thread */
        rc = dispatch_table_thread(&td[idx]);
        if (0 != rc) {
            failed = 1;
            logmsg(LOGMSG_ERROR, "Couldn't start a table-thread for table '%s' rc=%d\n",
                   td[idx].table, rc);
            break;
        }
        idx++;
    }

    /* wait for this to complete */
    for (i = 0; i < idx; i++) {
        int lrc = wait_for_table(&td[i]);
        if (lrc)
            failed = 1;
    }

    /* tell comdb2sc the results */
    if (failed) {
        sbuf2printf(sb, "FAILED\n");
    } else {
        sbuf2printf(sb, "SUCCESS\n");
        cleanup_stats(sb);
    }
    sbuf2flush(sb);

    /* free descriptor */
    free(td);

    /* reset running flag */
    analyze_running_flag = 0;

    return rc;
}

/* dump some analyze stats */
int analyze_dump_stats(void)
{
    logmsg(LOGMSG_USER, "Sampled tables:                     %s\n",
           sampled_tables_enabled ? "Enabled" : "Disabled");
    logmsg(LOGMSG_USER, "Sampling threshold:                 %lld bytes\n",
           sampling_threshold);
    logmsg(LOGMSG_USER, "Max Analyze table-threads:             %d threads\n",
           analyze_max_table_threads);
    logmsg(LOGMSG_USER, "Current Analyze table-threads:         %d threads\n",
           analyze_cur_table_threads);
    logmsg(LOGMSG_USER, "Max Analyze sampling-threads:       %d threads\n",
           analyze_max_comp_threads);
    logmsg(LOGMSG_USER, "Current Analyze sampling-threads:   %d threads\n",
           analyze_cur_comp_threads);
    logmsg(LOGMSG_USER, "Current Analyze counter:               %d \n", gbl_analyze_gen);
    return 0;
}

/* enabled sampled indicies */
void analyze_enable_sampled_indicies(void) { sampled_tables_enabled = 1; }

/* disable sampled indicies */
void analyze_disable_sampled_indicies(void) { sampled_tables_enabled = 0; }

/* set sampling threshold */
int analyze_set_sampling_threshold(void *context, void *thresh)
{
    long long _thresh = *(int *)thresh;

    if (_thresh < 0) {
        logmsg(LOGMSG_ERROR, "%s: Invalid value for sampling threshold\n",
               __func__);
        return 1;
    }
    sampling_threshold = _thresh;
    return 0;
}

/* get sampling threshold */
long long analyze_get_sampling_threshold(void) { return sampling_threshold; }

/* set maximum analyze threads */
int analyze_set_max_table_threads(void *context, void *maxthd)
{
    int _maxthd = *(int *)maxthd;

    /* must have at least 1 */
    if (_maxthd < 1) {
        logmsg(LOGMSG_ERROR, "%s: invalid value for maxthd\n", __func__);
        return 1;
    }
    /* can have no more than hard-max */
    if (_maxthd > analyze_hard_max_table_threads) {
        logmsg(LOGMSG_ERROR, "%s: hard-maximum is %d\n", __func__,
               analyze_hard_max_table_threads);
        return 1;
    }
    analyze_max_table_threads = _maxthd;
    return 0;
}

/* Set maximum analyze sampling threads */
int analyze_set_max_sampling_threads(void *context, void *maxthd)
{
    int _maxthd = *(int *)maxthd;

    /* must have at least 1 */
    if (_maxthd < 1) {
        logmsg(LOGMSG_ERROR, "%s: invalid value for maxthd\n", __func__);
        return 1;
    }
    /* can have no more than hard-max */
    if (_maxthd > analyze_hard_max_comp_threads) {
        logmsg(LOGMSG_ERROR, "%s: hard-maximum is %d\n", __func__,
               analyze_hard_max_comp_threads);
        return 1;
    }
    analyze_max_comp_threads = _maxthd;
    return 0;
}

/* return a 1 if analyze is running, 0 otherwise */
int analyze_is_running(void) { return analyze_running_flag; }

/* analyze message-trap thread */
void *message_trap_td(void *args)
{
    SBUF2 *sb = NULL;

    /* open an sbuf pointed at stdout */
    sb = sbuf2open(1, 0);

    /* analyze the database */
    analyze_database(sb, 
         bdb_attr_get(thedb->bdb_attr, BDB_ATTR_DEFAULT_ANALYZE_PERCENT),
         0); //override llmeta scale 0 (false)

    /* flush sbuf */
    sbuf2flush(sb);

    /* free sbuf */
    sbuf2free(sb);

    return NULL;
}

/* Backout to previous analyze stats */
static inline int analyze_backout_table(struct sqlclntstate *clnt, char *table)
{
    if (is_sqlite_stat(table))
        return 0;
    int rc = run_internal_sql_clnt(clnt, "BEGIN");
    if (rc)
        return rc;

    rc = backout_stats_frm_tbl(clnt, table, 1);
    if (rc)
        goto error;

    if (get_dbtable_by_name("sqlite_stat2")) {
        rc = backout_stats_frm_tbl(clnt, table, 2);
        if (rc)
            goto error;
    }

    if (get_dbtable_by_name("sqlite_stat4")) {
        rc = backout_stats_frm_tbl(clnt, table, 4);
        if (rc)
            goto error;
    }
    rc = run_internal_sql_clnt(clnt, "COMMIT");
    return rc;

error:
    logmsg(LOGMSG_ERROR, "backout error, rolling back transaction\n");
    run_internal_sql_clnt(clnt, "ROLLBACK");
    return rc;
}

void handle_backout(SBUF2 *sb, char *table)
{
    if (check_stat1_and_flag(sb))
        return;

    struct sqlclntstate clnt;
    start_internal_sql_clnt(&clnt);
    SBUF2 *sb2 = sbuf2open(fileno(stdout), 0);

    int rc = 0;
    clnt.sb = sb2;

    rdlock_schema_lk();

    if (table) {
        rc = analyze_backout_table(&clnt, table);
    } else {
        int i = 0;
        while (i < thedb->num_dbs && rc == 0) {
            rc = analyze_backout_table(&clnt, thedb->dbs[i]->dbname);
            i++;
        }
    }
    unlock_schema_lk();

    sbuf2flush(sb2);
    sbuf2free(sb2);

    if (rc == 0)
        sbuf2printf(sb, "SUCCESS\n");
    else {
        sbuf2printf(sb, "?Error occured with query: '%s'\n", clnt.sql);
        sbuf2printf(sb, "FAILED\n");
    }

    end_internal_sql_clnt(&clnt);
    sbuf2flush(sb);
}

/* Add stats for indices of table newname by copying from
 * entries for table oldname. Used when renaming a table.
 */
void add_idx_stats(const char *tbl, const char *oldname, const char *newname)
{
    if (NULL == get_dbtable_by_name("sqlite_stat1"))
        return; // stat1 does not exist, nothing to do

    char sql[256];
    snprintf(sql, sizeof(sql), "INSERT INTO sqlite_stat1 select tbl, '%s' as "
                               "idx, stat FROM sqlite_stat1 WHERE tbl='%s' and "
                               "idx='%s' \n",
             newname, tbl, oldname);
    run_internal_sql(sql);

    if (get_dbtable_by_name("sqlite_stat2")) {
        snprintf(sql, sizeof(sql), "INSERT INTO sqlite_stat2 select tbl, '%s' "
                                   "as idx, sampleno, sample FROM sqlite_stat2 "
                                   "WHERE tbl='%s' and idx='%s' \n",
                 newname, tbl, oldname);
        run_internal_sql(sql);
    }

    if (get_dbtable_by_name("sqlite_stat4")) {
        snprintf(sql, sizeof(sql), "INSERT INTO sqlite_stat4 select tbl, '%s' "
                                   "as idx, neq, nlt, ndlt, sample FROM "
                                   "sqlite_stat4 WHERE tbl='%s' and idx='%s' "
                                   "\n",
                 newname, tbl, oldname);
        run_internal_sql(sql);
    }
}

int do_analyze(char *tbl, int percent)
{
    SBUF2 *sb2 = sbuf2open(fileno(stdout), 0);

    int overwrite_llmeta = 1;
    if (percent == 0) {
        overwrite_llmeta = 0;
        percent = bdb_attr_get(thedb->bdb_attr, 
                               BDB_ATTR_DEFAULT_ANALYZE_PERCENT);
    }

    int rc;
    if (tbl == NULL)
        rc = analyze_database(sb2, percent, overwrite_llmeta);
    else
        rc = analyze_table(tbl, sb2, percent, overwrite_llmeta);
    sbuf2flush(sb2);
    sbuf2free(sb2);
    return rc;
}

