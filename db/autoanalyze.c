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
#include <stdbool.h>
#include <sql.h>

#include <comdb2.h>
#include <util.h>
#include <analyze.h>
#include <bdb_api.h>
#include <locks.h>
#include <ctrace.h>
#include <autoanalyze.h>
#include <sqlstat1.h>

const char *aa_counter_str = "autoanalyze_counter";
const char *aa_lastepoch_str = "autoanalyze_lastepoch";
static volatile bool auto_analyze_running = false;

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

    tbl->aa_saved_counter = 0;
    tbl->aa_lastepoch = time(NULL);

    if (save_freq > 0 && thedb->master == gbl_mynode) {
        // save updated counter
        const char *str = "0";
        bdb_set_table_parameter(NULL, tblname, aa_counter_str, str);

        char epoch[30] = {0};
        sprintf(epoch, "%d", tbl->aa_lastepoch);
        bdb_set_table_parameter(NULL, tblname, aa_lastepoch_str, epoch);
    }

    BDB_RELLOCK();

    ctrace("AUTOANALYZE: Analyzed Table %s, reseting counter to %d and last "
           "run time %s",
           tbl->dbname, tbl->aa_saved_counter, ctime(&tbl->aa_lastepoch));
}

static inline void loc_print_date(const time_t *timep)
{
    struct tm tmresult;
    localtime_r(timep, &tmresult);
    char outresult[128];
    strftime(outresult, sizeof(outresult), "%F %T (%s)", &tmresult);
    logmsg(LOGMSG_USER, "%s", outresult);
}

/* auto_analyze_table() will be passed a copy of the table name,
 * and it will free it.
 */
void *auto_analyze_table(void *arg)
{
    char *tblname = (char *)arg;
    int rc;

    for (int retries = 0; gbl_schema_change_in_progress && retries < 10;
         retries++) {
        sleep(5); // wait around for sequential fastinits to finish
    }

    printf("%s: STARTING %s\n", __func__, tblname);
    SBUF2 *sb = sbuf2open(fileno(stdout), 0);
    bdb_thread_event(thedb->bdb_env, BDBTHR_EVENT_START_RDWR);
    int percent = bdb_attr_get(thedb->bdb_attr, 
                               BDB_ATTR_DEFAULT_ANALYZE_PERCENT);

    if ((rc = analyze_table(tblname, sb, percent, 0)) == 0) {
        reset_aa_counter(tblname);

    } else {
        fprintf(stderr, "%s: analyze_table %s failed rc:%d\n", __func__,
                tblname, rc);
    }

    bdb_thread_event(thedb->bdb_env, BDBTHR_EVENT_DONE_RDWR);
    sbuf2free(sb);
    auto_analyze_running = false;
    free(tblname);
    return NULL;
}

static void get_saved_counter_epoch(char *tblname, unsigned *aa_counter,
                                    time_t *aa_lastepoch)
{
    *aa_counter = 0;
    char *counterstr = NULL;
    int rc = bdb_get_table_parameter(tblname, aa_counter_str, &counterstr);
    if (rc == 0) {
        *aa_counter = strtoul(counterstr, NULL, 10);
        free(counterstr);
    }

    char *epochstr = NULL;
    *aa_lastepoch = 0;
    rc = bdb_get_table_parameter(tblname, aa_lastepoch_str, &epochstr);
    if (rc == 0) {
        *aa_lastepoch = atoi(epochstr);
        free(epochstr);
    }
}

int load_auto_analyze_counters(void)
{
    int save_freq = bdb_attr_get(thedb->bdb_attr, BDB_ATTR_AA_LLMETA_SAVE_FREQ);

    for (int i = 0; i < thedb->num_dbs; i++) {
        struct dbtable *tbl = thedb->dbs[i];
        if (is_sqlite_stat(tbl->dbname))
            continue;

        tbl->aa_saved_counter = 0;
        tbl->aa_lastepoch = 0;
        if (save_freq > 0) {
            get_saved_counter_epoch(tbl->dbname, &tbl->aa_saved_counter,
                                    &tbl->aa_lastepoch);

            ctrace("AUTOANALYZE: Loading table %s, count %d, last run time %s",
                   tbl->dbname, tbl->aa_saved_counter, ctime(&tbl->aa_lastepoch));
        }
    }

    return 0;
}

static long long get_num_rows_from_stat1(struct dbtable *tbldb)
{
    char fnd_txt[64] = {0};
    char ix_txt[128] = {0};
    char *rec = NULL;
    long long val = 0;
    struct ireq iq;
    tran_type *trans = NULL;

    init_fake_ireq(thedb, &iq);
    iq.usedb = get_dbtable_by_name("sqlite_stat1");

    int rc = trans_start(&iq, NULL, &trans);
    if (rc) {
        fprintf(stderr, "%s: Couldn't start a transaction rc=%d\n", __func__,
                rc);
        goto out;
    }

    /* form key for sqlite_stat1 */
    int idx = 0;
    snprintf(fnd_txt, sizeof(fnd_txt), ".ONDISK_ix_%d", idx);
    struct schema *s;

    /* Grab the tag schema, or punt. */
    if (!(s = find_tag_schema(tbldb->dbname, fnd_txt))) {
        fprintf(stderr, "Couldn't find tag schema for '%s'.\n", fnd_txt);
        goto abort;
    }

    /* Get the name for this index. */
    strcpy(ix_txt, s->sqlitetag);

    /* get the sqlite_stat1 schema */
    s = iq.usedb->schema;

    /* create a stat1 record */
    rc = stat1_ondisk_record(&iq, tbldb->dbname, ix_txt, NULL, (void **)&rec);
    if (rc != 0) {
        fprintf(stderr, "%s: couldn't create ondisk record for sqlite_stat1\n",
                __func__);
        goto abort;
    }

    unsigned long long genid;
    rc = sqlstat_find_get_record(&iq, trans, rec, &genid);
    if (rc != IX_FND) {
        goto abort;
    }

    char *sta = get_field_from_sqlite_stat_rec(&iq, rec, "stat");
    if (!sta) {
        ctrace("%s: cannot find field in sqlite_stat1!\n", __func__);
        goto abort;
    }

    char *endptr;
    errno = 0; /* To distinguish success/failure after call */
    val = strtoll(sta, &endptr, 10);
    if (errno != 0 || endptr == sta)
        printf("%s: Error converting '%s' '%lld'\n", __func__, sta, val);
#ifdef DEBUG
    else
        printf("table %s has %lld rows\n", tbldb->dbname, val);
#endif

abort:
    trans_abort(&iq, trans);
out:
    if (rec)
        free(rec);
    if (val == 0)
        val = 1;
    return val;
}

// print autoanalyze stats
void stat_auto_analyze(void)
{
    if (thedb->master != gbl_mynode) // refresh from saved if we are not master
        load_auto_analyze_counters();

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
    int include_updates = bdb_attr_get(thedb->bdb_attr, BDB_ATTR_AA_COUNT_UPD);

    if (NULL == get_dbtable_by_name("sqlite_stat1")) {
        logmsg(LOGMSG_USER, "ANALYZE REQUIRES sqlite_stat1 to run but table is MISSING\n");
        return;
    }

    for (int i = 0; i < thedb->num_dbs; i++) {
        struct dbtable *tbl = thedb->dbs[i];
        if (is_sqlite_stat(tbl->dbname))
            continue;

        unsigned prev = tbl->saved_write_count[RECORD_WRITE_DEL] +
                        tbl->saved_write_count[RECORD_WRITE_INS];
        unsigned curr = tbl->write_count[RECORD_WRITE_DEL] +
                        tbl->write_count[RECORD_WRITE_INS];

        if (include_updates) {
            prev += tbl->saved_write_count[RECORD_WRITE_UPD];
            curr += tbl->write_count[RECORD_WRITE_UPD];
        }

        int delta = curr - prev;
        int newautoanalyze_counter = tbl->aa_saved_counter + delta;

        double new_aa_percnt = 0;
        if (newautoanalyze_counter > 0)
            new_aa_percnt =
                (100.0 * newautoanalyze_counter) / get_num_rows_from_stat1(tbl);

        logmsg(LOGMSG_USER, "Table %s, aa counter=%d (saved %d, new %d, percent of tbl "
               "%.2f), last run time=",
               tbl->dbname, newautoanalyze_counter, tbl->aa_saved_counter, delta,
               (new_aa_percnt > 100 ? 100 : new_aa_percnt));
        loc_print_date(&tbl->aa_lastepoch);
        logmsg(LOGMSG_USER, "\n");
    }
}

/* Update counters for every table
 * if a db surpases the limit then create a new thread to run analyze
 * Counters for other tables will still be updated,
 * but there can only be one analyze going on at any given time
 */
void *auto_analyze_main(void *unused)
{
    int now = time_epoch();
    if (now - gbl_sc_last_writer_time >
        bdb_attr_get(thedb->bdb_attr, BDB_ATTR_CHK_AA_TIME)) {
#ifdef DEBUG
        ctrace("AUTOANALYZE: no writes in the last %d sec\n",
               now - gbl_sc_last_writer_time);
#endif
        return NULL; // nothing to do
    }

    if (NULL == get_dbtable_by_name("sqlite_stat1")) {
#ifdef DEBUG
        printf("ANALYZE REQUIRES sqlite_stat1 to run but table is MISSING\n");
#endif
        return NULL;
    }

    static int loc_call_counter = 0;

#ifdef DEBUG
    printf("%s: loc_call_counter %d\n", __func__, loc_call_counter);
#endif

    bdb_state_type *bdb_state = thedb->bdb_env;

    thrman_register(THRTYPE_ANALYZE);
    backend_thread_event(thedb, COMDB2_THR_EVENT_START_RDONLY);

    int save_freq = bdb_attr_get(thedb->bdb_attr, BDB_ATTR_AA_LLMETA_SAVE_FREQ);
    unsigned min_ops = bdb_attr_get(thedb->bdb_attr, BDB_ATTR_MIN_AA_OPS);
    unsigned min_time = bdb_attr_get(thedb->bdb_attr, BDB_ATTR_MIN_AA_TIME);
    int include_updates = bdb_attr_get(thedb->bdb_attr, BDB_ATTR_AA_COUNT_UPD);
    int min_percent = bdb_attr_get(thedb->bdb_attr, BDB_ATTR_AA_MIN_PERCENT);
    int min_percent_jitter =
        bdb_attr_get(thedb->bdb_attr, BDB_ATTR_AA_MIN_PERCENT_JITTER);
    loc_call_counter++;

    int strt = time_epochms();

    if (save_freq > 0)
        BDB_READLOCK(__func__);

    rdlock_schema_lk();
    // for each table update the counters
    for (int i = 0; i < thedb->num_dbs; i++) {
        if (thedb->master != gbl_mynode ||
            gbl_schema_change_in_progress) // should not be writing
            break;

        struct dbtable *tbl = thedb->dbs[i];
        if (is_sqlite_stat(tbl->dbname))
            continue;

        // should we track this table? check analyzethreshold if zero, dont
        // track
        long long thresholdvalue = 0;
        int bdberr = 0;
        int rc = bdb_get_analyzethreshold_table(NULL, tbl->dbname,
                                                &thresholdvalue, &bdberr);
        if (rc != 0)
            printf("bdb_get_analyzethreshold_table rc = %d, bdberr=%d\n", rc,
                   bdberr);
        else if (thresholdvalue == 0)
            continue;

        int curr_count[] = {tbl->write_count[RECORD_WRITE_INS],
                            tbl->write_count[RECORD_WRITE_UPD],
                            tbl->write_count[RECORD_WRITE_DEL]};
        unsigned prev = tbl->saved_write_count[RECORD_WRITE_DEL] +
                        tbl->saved_write_count[RECORD_WRITE_INS];
        unsigned curr =
            curr_count[RECORD_WRITE_DEL] + curr_count[RECORD_WRITE_INS];

        if (include_updates) {
            prev += tbl->saved_write_count[RECORD_WRITE_UPD];
            curr += curr_count[RECORD_WRITE_UPD];
        }

        int delta = curr - prev;
        int newautoanalyze_counter = tbl->aa_saved_counter + delta;
        double new_aa_percnt = 0;

        if (newautoanalyze_counter > 0) {
            long long int num = get_num_rows_from_stat1(tbl);
            new_aa_percnt =
                100.0 * (newautoanalyze_counter - min_percent_jitter) / num;
        }

        /* if there is enough change, run analyze
         * only one analyze at a time is allowed to run (auto_analyze_running)
         * we should not auto analyze if analyze_is_running (manually)
         */
        if (!auto_analyze_running && !gbl_schema_change_in_progress &&
            !analyze_is_running() &&
            ((newautoanalyze_counter > min_ops &&
              now - tbl->aa_lastepoch > min_time) ||
             (min_percent > 0 && new_aa_percnt > min_percent))) {
            if (min_percent > 0 && new_aa_percnt > min_percent &&
                !((newautoanalyze_counter > min_ops &&
                   now - tbl->aa_lastepoch > min_time)))
                ctrace("AUTOANALYZE: Forcing analyze because new_aa_percnt %f "
                       "> min_percent %d\n",
                       new_aa_percnt, min_percent);

            ctrace("AUTOANALYZE: Analyzing Table %s, counters (%d, %d); last "
                   "run time %s\n",
                   tbl->dbname, tbl->aa_saved_counter, delta,
                   ctime(&tbl->aa_lastepoch));
            auto_analyze_running = true; // will be reset by
                                         // auto_analyze_table()
            pthread_t analyze;
            char *tblname = strdup(
                (char *)tbl->dbname); // will be freed in auto_analyze_table()
            pthread_create(&analyze, &gbl_pthread_attr_detached,
                           auto_analyze_table, tblname);
        } else if (delta > 0 && save_freq > 0 &&
                   (loc_call_counter % save_freq) ==
                       0) { // save updated counter
            ctrace("AUTOANALYZE: Table %s, saving counter (%d, %d); last run "
                   "time %s\n",
                   tbl->dbname, tbl->aa_saved_counter, delta,
                   ctime(&tbl->aa_lastepoch));
            char str[12] = {0};
            sprintf(str, "%d", newautoanalyze_counter);
            bdb_set_table_parameter(NULL, tbl->dbname, aa_counter_str, str);
        }

        tbl->aa_saved_counter = newautoanalyze_counter;
        tbl->saved_write_count[RECORD_WRITE_DEL] = curr_count[RECORD_WRITE_DEL];
        tbl->saved_write_count[RECORD_WRITE_UPD] = curr_count[RECORD_WRITE_UPD];
        tbl->saved_write_count[RECORD_WRITE_INS] = curr_count[RECORD_WRITE_INS];
    }
    unlock_schema_lk();

    if (save_freq > 0)
        BDB_RELLOCK();

    ctrace("AUTOANALYZE check took %d ms\n", time_epochms() - strt);

    backend_thread_event(thedb, COMDB2_THR_EVENT_DONE_RDONLY);
    return NULL;
}

