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

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <fcntl.h>
#include <pthread.h>
#include <unistd.h>
#include <sql.h>
#include <inttypes.h>

#include <comdb2.h>
#include <util.h>
#include <analyze.h>
#include <bdb_api.h>
#include <locks.h>
#include <ctrace.h>
#include <autoanalyze.h>
#include <sqlstat1.h>
#include "sc_util.h"
#include "comdb2_atomic.h"

const char *aa_counter_str = "autoanalyze_counter";
const char *aa_lastepoch_str = "autoanalyze_lastepoch";
const char *aa_needs_analyze_time_str = "autoanalyze_needs_analyze_time";
static volatile int auto_analyze_running = 0;
int gbl_debug_aa;
extern int gbl_is_physical_replicant;

/* ctime_r no-new-line */
static char *ctime_r_nnl(time_t *t, char *out)
{
    ctime_r(t, out);
    size_t n = strlen(out);
    if (out[n - 1] == '\n') out[n - 1] = 0;
    return out;
}

/* reset autoanalyze counters to zero
 */
void reset_aa_counter(char *tblname)
{
    int save_freq = bdb_attr_get(thedb->bdb_attr, BDB_ATTR_AA_LLMETA_SAVE_FREQ);
    bdb_state_type *bdb_state = thedb->bdb_env;

    BDB_READLOCK(__func__);

    struct dbtable *tbl = get_dbtable_by_name(tblname);
    if (!tbl) {
        BDB_RELLOCK();
        return;
    }

    XCHANGE64(tbl->aa_saved_counter, 0);
    XCHANGE64(tbl->aa_lastepoch, (int64_t)time(NULL));
    XCHANGE64(tbl->aa_needs_analyze_time, 0);

    if (save_freq > 0 && thedb->master == gbl_myhostname) {
        // save updated counter
        const char *str = "0";
        bdb_set_table_parameter(NULL, tblname, aa_counter_str, str);

        char epoch[30] = {0};
        sprintf(epoch, "%lld", (long long)tbl->aa_lastepoch);
        bdb_set_table_parameter(NULL, tblname, aa_lastepoch_str, epoch);
        bdb_set_table_parameter(NULL, tblname, aa_needs_analyze_time_str, str);
    }

    BDB_RELLOCK();

    char my_buf[30];
    char my_buf2[30];
    ctrace("AUTOANALYZE: Analyzed Table %s, reseting counter to %"PRId64", last run time %s, needs analyze time %s\n",
           tbl->tablename, tbl->aa_saved_counter,
           ctime_r_nnl((time_t *)&tbl->aa_lastepoch, my_buf),
           ctime_r_nnl((time_t *)&tbl->aa_needs_analyze_time, my_buf2));
}

#define loc_print_date(t, outresult)                                                               \
    ({                                                                                             \
        struct tm tmresult;                                                                        \
        localtime_r((time_t *)t, &tmresult);                                                                 \
        strftime(outresult, sizeof(outresult), "%F %T (%s)", &tmresult);                           \
        outresult;                                                                                 \
    })

/* auto_analyze_table() will be passed a copy of the table name,
 * and it will free it.
 */
void *auto_analyze_table(void *arg)
{
    char *tblname = (char *)arg;
    if (is_sqlite_stat(tblname)) {
        free(tblname);
        return NULL;
    }
    int rc;

    for (int retries = 0;
         get_schema_change_in_progress(__func__, __LINE__) && retries < 10;
         retries++) {
        sleep(5); // wait around for sequential fastinits to finish
    }

    logmsg(LOGMSG_WARN, "%s: STARTING %s\n", __func__, tblname);
    COMDB2BUF *sb = cdb2buf_open(fileno(stdout), 0);
    bdb_thread_event(thedb->bdb_env, BDBTHR_EVENT_START_RDWR);
    int percent = bdb_attr_get(thedb->bdb_attr, 
                               BDB_ATTR_DEFAULT_ANALYZE_PERCENT);

    if ((rc = analyze_table(tblname, sb, percent, 0, 1)) == 0) {
        reset_aa_counter(tblname);
    } else {
        logmsg(LOGMSG_ERROR, "%s: analyze_table %s failed rc:%d\n", __func__,
               tblname, rc);
    }

    bdb_thread_event(thedb->bdb_env, BDBTHR_EVENT_DONE_RDWR);
    cdb2buf_free(sb);
    free(tblname);
    if (gbl_debug_aa) {
        ctrace("AUTOANALYZE: sleep for testing for %d seconds\n",
               bdb_attr_get(thedb->bdb_attr, BDB_ATTR_CHK_AA_TIME) + 1);
        sleep(bdb_attr_get(thedb->bdb_attr, BDB_ATTR_CHK_AA_TIME) + 1);
    }

    auto_analyze_running = 0;
    return NULL;
}

static void get_saved_counter_epochs(tran_type *trans, char *tblname, int64_t *aa_counter, int64_t *aa_lastepoch,
                                     int64_t *aa_needs_analyze_time)
{
    int rc;
    if (aa_counter) {
        char *counterstr = NULL;
        *aa_counter = 0;
        rc = bdb_get_table_parameter_tran(tblname, aa_counter_str, &counterstr, trans);
        if (rc == 0) {
            *aa_counter = atoll(counterstr);
            free(counterstr);
        }
    }

    if (aa_lastepoch) {
        char *epochstr = NULL;
        *aa_lastepoch = 0;
        rc = bdb_get_table_parameter_tran(tblname, aa_lastepoch_str, &epochstr, trans);
        if (rc == 0) {
            *aa_lastepoch = atoll(epochstr);
            free(epochstr);
        }
    }

    if (aa_needs_analyze_time) {
        char *needs_analyze_time_str = NULL;
        *aa_needs_analyze_time = 0;
        rc = bdb_get_table_parameter_tran(tblname, aa_needs_analyze_time_str, &needs_analyze_time_str, trans);
        if (rc == 0) {
            *aa_needs_analyze_time = atoll(needs_analyze_time_str);
            free(needs_analyze_time_str);
        }
    }
}

int load_auto_analyze_counters_tran(tran_type *trans)
{
    int save_freq = bdb_attr_get(thedb->bdb_attr, BDB_ATTR_AA_LLMETA_SAVE_FREQ);

    for (int i = 0; i < thedb->num_dbs; i++) {
        struct dbtable *tbl = thedb->dbs[i];
        if (is_sqlite_stat(tbl->tablename))
            continue;

        int64_t saved_counter = 0;
        int64_t lastepoch = 0;
        int64_t needs_analyze_time = 0;
        
        if (save_freq > 0) {
            get_saved_counter_epochs(trans, tbl->tablename, &saved_counter, &lastepoch, &needs_analyze_time);
            char my_buf[30];
            char my_buf2[30];
            ctrace("AUTOANALYZE: Loading table %s, count %"PRId64", last run time %s, needs analyze time %s\n",
                   tbl->tablename, saved_counter, ctime_r_nnl((time_t *)&lastepoch, my_buf),
                   ctime_r_nnl((time_t *)&needs_analyze_time, my_buf2));
        }
        
        XCHANGE64(tbl->aa_saved_counter, saved_counter);
        XCHANGE64(tbl->aa_lastepoch, lastepoch);
        XCHANGE64(tbl->aa_needs_analyze_time, needs_analyze_time);
    }

    return 0;
}

int load_auto_analyze_counters()
{
    return load_auto_analyze_counters_tran(NULL);
}

static long long get_num_rows_from_stat1(struct dbtable *tbldb)
{
    char ix_txt[128] = {0};
    char *rec = NULL;
    long long val = 0;
    struct ireq iq;
    tran_type *trans = NULL;
    char *stat1 = NULL;

    init_fake_ireq(thedb, &iq);
    iq.usedb = get_dbtable_by_name("sqlite_stat1");

    int rc = trans_start(&iq, NULL, &trans);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: Couldn't start a transaction rc=%d\n",
               __func__, rc);
        goto out;
    }

    struct schema *s;

    /* Grab the tag schema, or punt. */
    if (!(s = find_tag_schema(tbldb, ".ONDISK_ix_0"))) {
        /* This is not an error. This just means the table has no indexes. */
        goto abort;
    }

    /* Get the name for this index. */
    strcpy(ix_txt, s->sqlitetag);

    /* create a stat1 record */
    rc = stat1_ondisk_record(&iq, tbldb->tablename, ix_txt, NULL, (void **)&rec);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR,
               "%s: couldn't create ondisk record for sqlite_stat1\n",
               __func__);
        goto abort;
    }

    unsigned long long genid;
    rc = sqlstat_find_get_record(&iq, trans, rec, &genid);
    if (rc != IX_FND) {
        goto abort;
    }

    stat1 = get_field_from_sqlite_stat_rec(&iq, rec, "stat");
    if (!stat1) {
        ctrace("%s: cannot find field in sqlite_stat1!\n", __func__);
        goto abort;
    }

    char *endptr;
    errno = 0; /* To distinguish success/failure after call */
    val = strtoll(stat1, &endptr, 10);
    if (errno != 0 || endptr == stat1)
        logmsg(LOGMSG_ERROR, "%s: Error converting '%s' '%lld'\n", __func__,
               stat1, val);
    else
        logmsg(LOGMSG_DEBUG, "table %s has %lld rows\n", tbldb->tablename, val);

abort:
    trans_abort(&iq, trans);
out:
    free(stat1);
    if (rec)
        free(rec);
    if (val == 0)
        val = 1;
    return val;
}

void get_auto_analyze_tbl_stats(struct dbtable *tbl, int include_updates, int *delta, int64_t *saved_counter,
                                int64_t *newautoanalyze_counter, double *new_aa_percnt)
{
    int64_t prev = tbl->saved_write_count[RECORD_WRITE_DEL] +
                    tbl->saved_write_count[RECORD_WRITE_INS];
    int64_t curr = tbl->write_count[RECORD_WRITE_DEL] +
                    tbl->write_count[RECORD_WRITE_INS];

    if (include_updates) {
        prev += tbl->saved_write_count[RECORD_WRITE_UPD];
        curr += tbl->write_count[RECORD_WRITE_UPD];
    }

    *delta = curr - prev;
    *saved_counter = ATOMIC_LOAD64(tbl->aa_saved_counter);
    *newautoanalyze_counter = *saved_counter + *delta;

    *new_aa_percnt = 0;
    if (*newautoanalyze_counter > 0)
        *new_aa_percnt = (100.0 * *newautoanalyze_counter) / get_num_rows_from_stat1(tbl);
    if (*new_aa_percnt > 100)
        *new_aa_percnt = 100;
}

// print autoanalyze stats
void stat_auto_analyze(void)
{
    // refresh from saved if we are not master
    if (thedb->master != gbl_myhostname) {
        tran_type *trans = curtran_gettran();
        load_auto_analyze_counters_tran(trans);
        curtran_puttran(trans);
    }

    logmsg(LOGMSG_USER, "AUTOANALYZE: %s\n",
           YESNO(bdb_attr_get(thedb->bdb_attr, BDB_ATTR_AUTOANALYZE)));
    logmsg(LOGMSG_USER, "CONSIDER UPDATE OPS: %s\n",
           YESNO(bdb_attr_get(thedb->bdb_attr, BDB_ATTR_AA_COUNT_UPD)));
    logmsg(LOGMSG_USER, "MIN TIME BETWEEN RUNS: %dsecs (%dmins)\n",
           bdb_attr_get(thedb->bdb_attr, BDB_ATTR_MIN_AA_TIME),
           bdb_attr_get(thedb->bdb_attr, BDB_ATTR_MIN_AA_TIME) / 60);
    logmsg(LOGMSG_USER, "MIN OPERATIONS TO TRIGGER: %d\n",
           bdb_attr_get(thedb->bdb_attr, BDB_ATTR_MIN_AA_OPS));
    logmsg(LOGMSG_USER, "MIN PERCENT OF CHANGES TO TRIGGER: %d%% (+%d)\n",
           bdb_attr_get(thedb->bdb_attr, BDB_ATTR_AA_MIN_PERCENT),
           bdb_attr_get(thedb->bdb_attr, BDB_ATTR_AA_MIN_PERCENT_JITTER));
    logmsg(LOGMSG_USER, "UPDATE COUNTERS EVERY: %dsecs\n",
           bdb_attr_get(thedb->bdb_attr, BDB_ATTR_CHK_AA_TIME));
    logmsg(LOGMSG_USER, "SAVE COUNTERS FREQ: %d \n",
           bdb_attr_get(thedb->bdb_attr, BDB_ATTR_AA_LLMETA_SAVE_FREQ));
    logmsg(LOGMSG_USER, "REQUEST MODE: %s\n",
           YESNO(bdb_attr_get(thedb->bdb_attr, BDB_ATTR_AA_REQUEST_MODE)));
    int include_updates = bdb_attr_get(thedb->bdb_attr, BDB_ATTR_AA_COUNT_UPD);

    if (NULL == get_dbtable_by_name("sqlite_stat1")) {
        logmsg(LOGMSG_USER, "ANALYZE REQUIRES sqlite_stat1 to run but table is MISSING\n");
        return;
    }

    int delta;
    int64_t saved_counter;
    int64_t newautoanalyze_counter;
    double new_aa_percnt;
    for (int i = 0; i < thedb->num_dbs; i++) {
        struct dbtable *tbl = thedb->dbs[i];
        if (is_sqlite_stat(tbl->tablename)) continue;
        get_auto_analyze_tbl_stats(tbl, include_updates, &delta, &saved_counter, &newautoanalyze_counter, &new_aa_percnt);
        int64_t lastepoch = ATOMIC_LOAD64(tbl->aa_lastepoch);
        int64_t needs_analyze_time = ATOMIC_LOAD64(tbl->aa_needs_analyze_time);
        char lastepoch_str[128], needs_analyze_time_str[128];
        logmsg(LOGMSG_USER,
               "Table %s, aa counter=%"PRId64" (saved %"PRId64", new %d, percent of tbl %.2f), last run time=%s, needs analyze time=%s\n",
               tbl->tablename, newautoanalyze_counter, saved_counter, delta, new_aa_percnt,
               loc_print_date((time_t *) &lastepoch, lastepoch_str), loc_print_date((time_t *) &needs_analyze_time, needs_analyze_time_str));
    }
}

/* Update counters for every table
 * if a db surpases the limit then create a new thread to run analyze
 * Counters for other tables will still be updated,
 * but there can only be one analyze going on at any given time
 */
void *auto_analyze_main(void *unused)
{
    if (NULL == get_dbtable_by_name("sqlite_stat1")) {
        logmsg(LOGMSG_DEBUG,
               "ANALYZE REQUIRES sqlite_stat1 to run but table is MISSING\n");
        return NULL;
    }

    static int call_counter = 0;
    int now = comdb2_time_epoch();

    logmsg(LOGMSG_DEBUG, "%s: call_counter %d\n", __func__, call_counter);

    bdb_state_type *bdb_state = thedb->bdb_env;

    thrman_register(THRTYPE_ANALYZE);
    backend_thread_event(thedb, COMDB2_THR_EVENT_START_RDONLY);

    int save_freq = bdb_attr_get(thedb->bdb_attr, BDB_ATTR_AA_LLMETA_SAVE_FREQ);
    unsigned min_ops = bdb_attr_get(thedb->bdb_attr, BDB_ATTR_MIN_AA_OPS);
    unsigned min_time = bdb_attr_get(thedb->bdb_attr, BDB_ATTR_MIN_AA_TIME);
    int min_percent = bdb_attr_get(thedb->bdb_attr, BDB_ATTR_AA_MIN_PERCENT);
    int min_percent_jitter = bdb_attr_get(thedb->bdb_attr, BDB_ATTR_AA_MIN_PERCENT_JITTER);
    call_counter++;
    char my_buf[30];
    char my_buf2[30];

    int strt = comdb2_time_epochms();

    if (save_freq > 0)
        BDB_READLOCK(__func__);

    rdlock_schema_lk();
    // for each table update the counters
    for (int i = 0; i < thedb->num_dbs; i++) {
        if (thedb->master != gbl_myhostname ||
            get_schema_change_in_progress(__func__, __LINE__))
            break;

        struct dbtable *tbl = thedb->dbs[i];
        if (is_sqlite_stat(tbl->tablename))
            continue;

        // should we track this table? check analyzethreshold if zero, dont
        // track
        long long thresholdvalue = 0;
        int bdberr = 0;
        int rc = bdb_get_analyzethreshold_table(NULL, tbl->tablename,
                                                &thresholdvalue, &bdberr);
        if (rc != 0)
            logmsg(LOGMSG_WARN, "bdb_get_analyzethreshold_table rc = %d, bdberr=%d\n", rc, bdberr);
        else if (thresholdvalue == 0)
            continue;

        int64_t newautoanalyze_counter = ATOMIC_LOAD64(tbl->aa_saved_counter);
        int64_t lastepoch = ATOMIC_LOAD64(tbl->aa_lastepoch);
        int64_t needs_analyze_time = ATOMIC_LOAD64(tbl->aa_needs_analyze_time);
        double new_aa_percnt = 0;

        if (newautoanalyze_counter > 0) {
            long long int num = get_num_rows_from_stat1(tbl);
            new_aa_percnt = 100.0 * (newautoanalyze_counter - min_percent_jitter) / num;
        }

        /* if there is enough change, run analyze
         * only one analyze at a time is allowed to run (auto_analyze_running)
         * we should not auto analyze if analyze_is_running (manually) */
        if (!auto_analyze_running && !analyze_is_running() &&
            !get_schema_change_in_progress(__func__, __LINE__) &&
            ((newautoanalyze_counter > min_ops && now - lastepoch > min_time) ||
             (min_percent > 0 && new_aa_percnt > min_percent))) {

            if (!((newautoanalyze_counter > min_ops && now - lastepoch > min_time)))
                ctrace("AUTOANALYZE: Forcing analyze because new_aa_percnt %f > min_percent %d\n",
                       new_aa_percnt, min_percent);

            // In AA_REQUEST_MODE, date needs_analyze_time is set in comdb2_auto_analyze_tables
            // that another task can watch for and schedule analyze at a time of its choosing
            if (bdb_attr_get(thedb->bdb_attr, BDB_ATTR_AA_REQUEST_MODE)) {
                if (!needs_analyze_time) {
                    needs_analyze_time = time(NULL);
                    XCHANGE64(tbl->aa_needs_analyze_time, needs_analyze_time);
                    time_t t1 = tbl->aa_lastepoch;
                    time_t t2 = needs_analyze_time;
                    ctrace("AUTOANALYZE: Requesting analyze be run for Table %s, counter (%"PRId64") (setting needs analyze time); last run time %s, needs analyze time %s\n",
                           tbl->tablename, newautoanalyze_counter, ctime_r_nnl(&t1, my_buf), ctime_r_nnl(&t2, my_buf2));

                    if (save_freq > 0) {
                        char needs_analyze_time_str[30] = {0};
                        sprintf(needs_analyze_time_str, "%"PRId64"", needs_analyze_time);
                        bdb_set_table_parameter(NULL, tbl->tablename, aa_needs_analyze_time_str, needs_analyze_time_str);
                    }
                } else {
                    time_t t1 = tbl->aa_lastepoch;
                    time_t t2 = needs_analyze_time;
                    ctrace("AUTOANALYZE: Table %s, counter (%"PRId64") needs analyze time already set, doing nothing; last run time %s, needs analyze time %s\n",
                           tbl->tablename, newautoanalyze_counter, ctime_r_nnl(&t1, my_buf), ctime_r_nnl(&t2, my_buf2));
                }
            } else {
                ctrace(
                    "AUTOANALYZE: Analyzing Table %s, counter (%"PRId64"); last run time %s, needs analyze time %s\n",
                    tbl->tablename, newautoanalyze_counter, ctime_r_nnl((time_t *)&lastepoch, my_buf), ctime_r_nnl((time_t *)&needs_analyze_time, my_buf2));
                auto_analyze_running = 1; // will be reset by
                                             // auto_analyze_table()
                pthread_t analyze;
                // will be freed in auto_analyze_table()
                char *tblname = strdup(tbl->tablename);
                Pthread_create(&analyze, &gbl_pthread_attr_detached, auto_analyze_table, tblname);
            }
        } else if (save_freq > 0 && (call_counter % save_freq) == 0) {
            // save updated autoanalyze counter if there is a delta
            int64_t llmeta_aa_saved_counter;
            // get saved counter from llmeta
            get_saved_counter_epochs(NULL, tbl->tablename, &llmeta_aa_saved_counter, NULL, NULL);
            int delta = newautoanalyze_counter - llmeta_aa_saved_counter;
            if (delta > 0) {
                ctrace("AUTOANALYZE: Table %s, saving counter (%"PRId64"); last run time %s, needs analyze time %s\n",
                        tbl->tablename, newautoanalyze_counter, ctime_r_nnl((time_t *)&lastepoch, my_buf), ctime_r_nnl((time_t *)&needs_analyze_time, my_buf2));
                char str[12] = {0};
                sprintf(str, "%"PRId64"", newautoanalyze_counter);
                bdb_set_table_parameter(NULL, tbl->tablename, aa_counter_str, str);
            }
        }
    }
    unlock_schema_lk();

    if (save_freq > 0)
        BDB_RELLOCK();

    ctrace("AUTOANALYZE check took %d ms\n", comdb2_time_epochms() - strt);

    backend_thread_event(thedb, COMDB2_THR_EVENT_DONE_RDONLY);
    return NULL;
}

void autoanalyze_after_fastinit(char *table)
{
    if (gbl_is_physical_replicant || bdb_attr_get(thedb->bdb_attr, BDB_ATTR_AUTOANALYZE) == 0)
        return;
    pthread_t analyze;
    char *tblname = strdup(table); // will be freed in auto_analyze_table()
    Pthread_create(&analyze, &gbl_pthread_attr_detached, auto_analyze_table, tblname);
}
