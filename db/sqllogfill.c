/*
   Copyright 2025 Bloomberg Finance L.P.

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
#include <sqllogfill.h>
#include <cdb2api.h>
#include <cdb2api_int.h>
#include <sys_wrap.h>
#include <parse_lsn.h>
#include <unistd.h>
#include <log_info.h>
#include <comdb2.h>
#include <assert.h>
#include <dbinc/rep_types.h>
#include <build/db_int.h>

/* Flags for comdb2_transaction_logs table-valued function */
#define TRANLOG_FLAGS_BLOCK 0x01    /* Wait for new log records */
#define TRANLOG_FLAGS_SENTINEL 0x08 /* Return sentinel for missing record */

/* Tunables */
int gbl_sql_logfill = 0;
int gbl_debug_sql_logfill = 0;
int gbl_sql_logfill_stats = 1;
int gbl_sql_logfill_dedicated_apply_thread = 0;
int gbl_sql_logfill_lookahead_records = 10000;
int gbl_sql_logfill_next_timeout = 10;

static int sql_logfill_thds_created = 0;

struct log_record {
    unsigned int file;
    unsigned int offset;
    int64_t rectype;
    u_int32_t recgen;
    void *blob;
    int blob_len;
    int blob_memsz;
    u_int32_t gen;
};

/* Queued records */
struct log_record *apply_queue = NULL;
int apply_queue_head = 0;
int apply_queue_tail = 0;
u_int32_t apply_queue_gen = 0;

/* Apply queue synchronization */
static pthread_mutex_t sql_apply_queue_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t sql_apply_queue_cond = PTHREAD_COND_INITIALIZER;
static LOG_INFO last_enqueued_lsn = {0};

/* Signal mechanism from berkley */
static pthread_t sql_logfill_thd;
static pthread_t sql_logfill_apply_thd;
static pthread_mutex_t sql_logfill_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t sql_logfill_cond = PTHREAD_COND_INITIALIZER;
static int apply_thread_is_running = 0;

/* Connection state */
static char *connected_node = NULL;
static cdb2_hndl_tp *hndl = NULL;
static int is_connected = 0;

#define SQL_CMD_LEN 200

/* Externs */
extern int db_is_exiting(void);
extern char gbl_dbname[MAX_DBNAME_LENGTH];
extern int bdb_get_lsn_node(bdb_state_type *bdb_state, char *host, uint32_t *logfile, uint32_t *offset);
extern int gbl_exit;

/* Metrics */
static int64_t records_applied = 0;
static int64_t bytes_applied = 0;
static int64_t sql_finds = 0;
static int64_t sql_nexts = 0;
static int64_t enque_blocks = 0;
static int64_t apply_lock_waits = 0;
static int64_t nogap_returns = 0;

/* Forward declaration */
static void cleanup_sql_logfill(void);

static void disconnect_from_master(void)
{
    if (is_connected) {
        if (gbl_debug_sql_logfill) {
            logmsg(LOGMSG_USER, "%s: disconnecting from %s\n", __func__, connected_node);
        }
        cdb2_close(hndl);
        hndl = NULL;
        is_connected = 0;
        if (connected_node) {
            free(connected_node);
        }
        connected_node = NULL;
    }
}

/* Connect to master */
static int connect_to_master(bdb_state_type *bdb_state, const char *master)
{
    int rc;

    if (is_connected && connected_node && !strcmp(connected_node, master)) {
        return 0;
    }

    if (!master) {
        return 1;
    }

    if (is_connected) {
        disconnect_from_master();
    }

    rc = cdb2_open(&hndl, gbl_dbname, master, CDB2_DIRECT_CPU);
    if (rc != 0) {
        if (gbl_debug_sql_logfill) {
            logmsg(LOGMSG_USER, "%s: cdb2_open to master %s failed rc=%d\n", __func__, master, rc);
        }
        return 1;
    }

    cdb2_hndl_set_min_retries(hndl, 1);

    rc = cdb2_run_statement(hndl, "set transaction blocksql");
    if (rc != CDB2_OK) {
        if (gbl_debug_sql_logfill) {
            logmsg(LOGMSG_USER, "%s: cdb2_run_statement to master %s failed rc=%d\n", __func__, master, rc);
        }
        cdb2_close(hndl);
        hndl = NULL;
        return 1;
    }

    rc = cdb2_run_statement(hndl, "select 1");
    if (rc != CDB2_OK) {
        if (gbl_debug_sql_logfill) {
            logmsg(LOGMSG_USER, "%s: cdb2_run_statement to master %s failed rc=%d\n", __func__, master, rc);
        }
        cdb2_close(hndl);
        hndl = NULL;
        return 1;
    }

    is_connected = 1;
    connected_node = strdup(master);
    if (!connected_node) {
        logmsg(LOGMSG_FATAL, "%s: strdup failed\n", __func__);
        exit(1);
    }

    if (gbl_debug_sql_logfill) {
        logmsg(LOGMSG_USER, "%s: connected to master %s\n", __func__, master);
    }

    return 0;
}

static int apply_queue_size()
{
    Pthread_mutex_lock(&sql_apply_queue_lock);
    int size =
        (apply_queue_head - apply_queue_tail + gbl_sql_logfill_lookahead_records) % gbl_sql_logfill_lookahead_records;
    Pthread_mutex_unlock(&sql_apply_queue_lock);
    return size;
}

/* Caller holds sql_apply_queue_lock */
static inline int block_apply_queue(bdb_state_type *bdb_state)
{
    int counted = 0;
    while (((apply_queue_head + 1) % gbl_sql_logfill_lookahead_records) == apply_queue_tail && !db_is_exiting() &&
           !bdb_lock_desired(bdb_state)) {
        if (!counted) {
            enque_blocks++;
            counted = 1;
        }
        if (gbl_debug_sql_logfill) {
            static int lastpr = 0;
            int now = comdb2_time_epoch();
            if (now - lastpr > 0) {
                logmsg(LOGMSG_USER, "%s: apply queue full, head=%d tail=%d cnt=%" PRId64 "\n", __func__,
                       apply_queue_head, apply_queue_tail, enque_blocks);
                lastpr = now;
            }
        }
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        /* Reduced timeout from 1s to 100ms for more responsive behavior */
        ts.tv_nsec += 100000000; /* 100ms */
        if (ts.tv_nsec >= 1000000000) {
            ts.tv_sec += 1;
            ts.tv_nsec -= 1000000000;
        }
        pthread_cond_timedwait(&sql_apply_queue_cond, &sql_apply_queue_lock, &ts);
    }
    return db_is_exiting() || bdb_lock_desired(bdb_state);
}

static inline void check_apply_queue_gen(u_int32_t curgen)
{
    if (curgen != apply_queue_gen) {
        apply_queue_head = apply_queue_tail = 0;
        apply_queue_gen = curgen;
    }
}

static void enque_log_record(bdb_state_type *bdb_state, unsigned int file, unsigned int offset, int64_t rectype,
                             u_int32_t recgen, void *blob, int blob_len, u_int32_t curgen)
{
    if (blob == NULL || blob_len <= 0) {
        assert(rectype == REP_NEWFILE);
    }

    Pthread_mutex_lock(&sql_apply_queue_lock);
    check_apply_queue_gen(curgen);

    if (block_apply_queue(bdb_state) != 0) {
        Pthread_mutex_unlock(&sql_apply_queue_lock);
        return;
    }

    struct log_record *rec = &apply_queue[apply_queue_head];

    rec->file = file;
    rec->offset = offset;
    rec->rectype = rectype;
    rec->recgen = recgen;
    rec->gen = curgen;

    /* Ensure blob buffer is large enough */
    if (rec->blob_memsz < blob_len) {
        void *new_blob = realloc(rec->blob, blob_len);
        if (!new_blob) {
            logmsg(LOGMSG_FATAL, "%s: realloc(%d) failed\n", __func__, blob_len);
            abort();
        }
        rec->blob = new_blob;
        rec->blob_memsz = blob_len;
    }

    memcpy(rec->blob, blob, blob_len);
    rec->blob_len = blob_len;

    last_enqueued_lsn.file = file;
    last_enqueued_lsn.offset = offset;
    last_enqueued_lsn.gen = recgen;

    apply_queue_head = (apply_queue_head + 1) % gbl_sql_logfill_lookahead_records;
    Pthread_cond_signal(&sql_apply_queue_cond);
    Pthread_mutex_unlock(&sql_apply_queue_lock);
}

extern __thread char *rep_apply_caller;

static int handle_log(bdb_state_type *bdb_state, unsigned int file, unsigned int offset, int64_t rectype,
                      u_int32_t recgen, void *blob, int blob_len, u_int32_t curgen)
{
    if (gbl_sql_logfill_dedicated_apply_thread) {
        enque_log_record(bdb_state, file, offset, rectype, recgen, blob, blob_len, curgen);
        return 0;
    }

    rep_apply_caller = "sqllogfill";
    int rc = bdb_state->dbenv->apply_log(bdb_state->dbenv, file, offset, rectype, blob, blob_len);
    rep_apply_caller = NULL;

    records_applied++;
    bytes_applied += blob_len;

    if (bdb_state->attr->rep_debug_delay > 0)
        usleep(bdb_state->attr->rep_debug_delay * 1000);

    return rc;
}

static void printstats(void)
{
    static int lastpr = 0;
    int now = comdb2_time_epoch();
    if (now - lastpr > 0) {
        int qsz = apply_queue_size();
        logmsg(LOGMSG_USER,
               "sqllogfill records_applied=%" PRId64 " bytes_applied=%" PRId64 " finds=%" PRId64 " nexts=%" PRId64
               " enque-blocks=%" PRId64 " qsz=%d apply_lock_waits=%" PRId64 " nogap_returns=%" PRId64 "\n",
               records_applied, bytes_applied, sql_finds, sql_nexts, enque_blocks, qsz, apply_lock_waits,
               nogap_returns);
        lastpr = now;
    }
}

void sql_logfill_metrics(int64_t *applied_recs, int64_t *applied_bytes, int64_t *finds, int64_t *nexts, int64_t *qsize,
                         int64_t *blocks)
{
    *applied_recs = records_applied;
    *applied_bytes = bytes_applied;
    *finds = sql_finds;
    *nexts = sql_nexts;
    *blocks = enque_blocks;
    *qsize = apply_queue_size();
}

static void print_record_info(const char *prefix, const char *lsn)
{
    static int64_t cnt = 0;
    static int lastprint = 0;
    int now = comdb2_time_epoch();
    cnt++;
    if (now - lastprint > 0) {
        logmsg(LOGMSG_USER, "%s: lsn=%s cnt=%" PRId64 "\n", prefix, lsn, cnt);
        lastprint = now;
    }
}

static inline int gen_okay(bdb_state_type *bdb_state, int64_t recgen, u_int32_t mygen)
{
    return recgen <= mygen;
}

static int apply_record(bdb_state_type *bdb_state, const char *lsn, void *blob, int blob_len, int64_t genp,
                        LOG_INFO *last_lsn, DB_LSN *gap_lsn, u_int32_t gen)
{
    DB_LSN mylsn = {0};
    int rc;

    /* Validate blob pointer */
    if (!blob && blob_len > 0) {
        logmsg(LOGMSG_ERROR, "%s: NULL blob with non-zero size %d\n", __func__, blob_len);
        return -1;
    }

    /* Validate LSN string */
    if (!lsn) {
        logmsg(LOGMSG_ERROR, "%s: NULL lsn string\n", __func__);
        return -1;
    }

    if ((rc = char_to_lsn(lsn, &mylsn.file, &mylsn.offset)) != 0) {
        if (gbl_debug_sql_logfill) {
            logmsg(LOGMSG_ERROR, "%s: char_to_lsn failed for lsn %s\n", __func__, lsn);
        }
        return rc;
    }

    /* Handle file boundary, inject REP_NEWFILE record */
    if (last_lsn->file < mylsn.file) {
        rc = handle_log(bdb_state, last_lsn->file, get_next_offset(bdb_state->dbenv, *last_lsn), REP_NEWFILE, genp,
                        NULL, 0, gen);
        if (rc != 0 && rc != DB_REP_ISPERM && rc != DB_REP_NOTPERM) {
            logmsg(LOGMSG_FATAL, "%s error applying newfile log record, rc=%d\n", __func__, rc);
            exit(1);
        }
    }

    last_lsn->file = mylsn.file;
    last_lsn->offset = mylsn.offset;
    last_lsn->size = blob_len;

    /* Skip applying the gap LSN itself, it will be applied from repdb */
    if (!gap_lsn || log_compare(&mylsn, gap_lsn) < 0) {
        rc = handle_log(bdb_state, mylsn.file, mylsn.offset, REP_LOG, genp, blob, blob_len, gen);
        if (rc != 0 && rc != DB_REP_ISPERM && rc != DB_REP_NOTPERM) {
            logmsg(LOGMSG_FATAL, "%s error applying log record at [%u:%u], rc=%d\n", __func__, mylsn.file, mylsn.offset,
                   rc);
            exit(1);
        }
    }
    return 0;
}

static void request_logs_from_master(bdb_state_type *bdb_state)
{
    DB_LSN next_lsn = {0}, gap_lsn = {0}, last_locked = {0}, rec_lsn = {0};
    LOG_INFO last_lsn = {0};
    char sql_cmd[SQL_CMD_LEN];
    char master_name[256];
    char *lsn = NULL;
    int nrecs = 0, rc;
    u_int32_t gen, firstgen;
    int need_reconnect;

    while (!db_is_exiting() && !bdb_lock_desired(bdb_state)) {

        /* Copy master name under bdb-lock */
        BDB_READLOCK(__func__);
        if (!thedb->master || thedb->master == gbl_myhostname) {
            BDB_RELLOCK();
            sleep(1);
            return;
        }
        need_reconnect = !is_connected || (connected_node && strcmp(thedb->master, connected_node) != 0);
        if (need_reconnect) {
            snprintf(master_name, sizeof(master_name), "%s", thedb->master);
        }
        BDB_RELLOCK();

        /* Connect without holding BDB lock */
        if (need_reconnect) {
            rc = connect_to_master(bdb_state, master_name);
            if (rc) {
                sleep(1);
                return;
            }
        }

        BDB_READLOCK(__func__);

        /* Make sure master has not changed */
        if (!thedb->master || strcmp(thedb->master, connected_node) != 0) {
            BDB_RELLOCK();
            disconnect_from_master();
            return;
        }

        /* Check for rep-verify-match- returns current gen, not commit-gen */
        rc = bdb_state->dbenv->get_last_locked(bdb_state->dbenv, &last_locked, &firstgen);

        /* Returns non-0 before rep-verify-match */
        if (rc != 0) {
            BDB_RELLOCK();
            return;
        }

        /* Find gap lsn */
        rc = bdb_state->dbenv->get_rep_lsns(bdb_state->dbenv, &next_lsn, &gap_lsn, &nrecs);

        if (rc != 0) {
            BDB_RELLOCK();
            return;
        }

        /* No gap */
        if (IS_ZERO_LSN(gap_lsn)) {
            BDB_RELLOCK();
            nogap_returns++;
            return;
        }

        /* Get our current last lsn */
        last_lsn = get_last_lsn(bdb_state);

        /* Update to most recently queued */
        if (gbl_sql_logfill_dedicated_apply_thread) {
            Pthread_mutex_lock(&sql_apply_queue_lock);
            check_apply_queue_gen(firstgen);
            if (apply_queue_head != apply_queue_tail)
                last_lsn = last_enqueued_lsn;
            Pthread_mutex_unlock(&sql_apply_queue_lock);
        }

        /* Release bdb-lock before querying */
        BDB_RELLOCK();

        int timeout = gbl_sql_logfill_next_timeout;
        timeout = timeout > 0 ? timeout : 10;

        /* Query transaction log with BLOCK and SENTINEL flags */
        rc = snprintf(sql_cmd, SQL_CMD_LEN,
                      "select lsn, generation, payload from comdb2_transaction_logs('{%u:%u}', NULL, %d, %d)",
                      last_lsn.file, last_lsn.offset, TRANLOG_FLAGS_BLOCK | TRANLOG_FLAGS_SENTINEL, timeout);

        if (rc < 0 || rc >= SQL_CMD_LEN) {
            logmsg(LOGMSG_ERROR, "%s: snprintf failed, rc=%d\n", __func__, rc);
            return;
        }

        if (gbl_debug_sql_logfill) {
            logmsg(LOGMSG_USER, "%s: requesting logs from master %s: lsn %u:%u\n", __func__, connected_node,
                   last_lsn.file, last_lsn.offset);
        }

        if ((rc = cdb2_run_statement(hndl, sql_cmd)) != CDB2_OK) {
            if (gbl_debug_sql_logfill) {
                logmsg(LOGMSG_ERROR, "%s: cdb2_run_statement failed rc=%d\n", __func__, rc);
            }
            disconnect_from_master();
            return;
        }

        sql_finds++;

        if ((rc = cdb2_next_record(hndl)) != CDB2_OK) {
            if (gbl_debug_sql_logfill) {
                logmsg(LOGMSG_USER, "%s: cdb2_next_record returned rc=%d\n", __func__, rc);
            }
            disconnect_from_master();
            return;
        }

        lsn = (char *)cdb2_column_value(hndl, 0);
        if ((rc = char_to_lsn(lsn, &rec_lsn.file, &rec_lsn.offset)) != 0 || rec_lsn.file == -1) {
            if (gbl_debug_sql_logfill) {
                logmsg(LOGMSG_ERROR, "%s: char_to_lsn rc = %d for lsn %s\n", __func__, rc, lsn);
            }
            disconnect_from_master();
            return;
        }

        /* First record is the duplicate at last_lsn- skip it */
        if (gbl_debug_sql_logfill) {
            print_record_info("first-record (duplicate, skipping)", lsn);
        }
        sql_nexts++;

        int desired = 0, exiting = 0;
        while (!(desired = bdb_lock_desired(bdb_state)) && !(exiting = db_is_exiting()) &&
               (rc = cdb2_next_record(hndl)) == CDB2_OK) {

            sql_nexts++;
            if (gbl_debug_sql_logfill) {
                print_record_info("record", lsn);
            }

            BDB_READLOCK(__func__);
            bdb_state->dbenv->get_rep_gen(bdb_state->dbenv, &gen);

            int64_t *recgenp = (int64_t *)cdb2_column_value(hndl, 1);

            /* Verify gen before applying record */
            if (firstgen != gen || (recgenp != NULL && !gen_okay(bdb_state, *recgenp, gen))) {
                if (gbl_debug_sql_logfill) {
                    logmsg(LOGMSG_USER, "%s: exiting apply loop, firstgen=%u gen=%u recgen=%" PRId64 "\n", __func__,
                           firstgen, gen, recgenp ? *recgenp : 0);
                }
                BDB_RELLOCK();
                break;
            }

            void *blob = cdb2_column_value(hndl, 2);
            int blob_len = cdb2_column_size(hndl, 2);

            rc = apply_record(bdb_state, lsn, blob, blob_len, recgenp ? *recgenp : 0, &last_lsn, &gap_lsn, gen);
            if (rc != 0) {
                logmsg(LOGMSG_ERROR, "%s: apply_record failed rc=%d - exiting apply loop\n", __func__, rc);
                BDB_RELLOCK();
                return;
            }

            /* Gap-lsn can change if inmem_repdb_most_recent is enabled */
            rc = bdb_state->dbenv->get_rep_lsns(bdb_state->dbenv, &next_lsn, &gap_lsn, &nrecs);
            BDB_RELLOCK();

            DB_LSN last_db_lsn = {last_lsn.file, last_lsn.offset};

            /* Break if we have filled the gap */
            if (IS_ZERO_LSN(gap_lsn) || log_compare(&last_db_lsn, &gap_lsn) > 0) {
                break;
            }

            if (gbl_sql_logfill_stats) {
                printstats();
            }
        }

        if (gbl_debug_sql_logfill) {
            logmsg(LOGMSG_USER, "%s: done, desired=%d exiting=%d rc=%d\n", __func__, desired, exiting, rc);
        }
    }
}

static inline void sleep_for_gap_lsn(bdb_state_type *bdb_state)
{
    struct timespec ts;
    DB_LSN next_lsn = {0}, gap_lsn = {0}, last_locked = {0};
    u_int32_t gen;
    int nrecs = 0, rc;

    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += 1;

    Pthread_mutex_lock(&sql_logfill_lock);
    rc = bdb_state->dbenv->get_last_locked(bdb_state->dbenv, &last_locked, &gen);
    if (rc != 0) {
        Pthread_mutex_unlock(&sql_logfill_lock);
        sleep(1);
        return;
    }
    bdb_state->dbenv->get_rep_lsns(bdb_state->dbenv, &next_lsn, &gap_lsn, &nrecs);
    if (IS_ZERO_LSN(gap_lsn)) {
        pthread_cond_timedwait(&sql_logfill_cond, &sql_logfill_lock, &ts);
    }
    Pthread_mutex_unlock(&sql_logfill_lock);
}

static void *sql_logfill_thread(void *arg)
{
    bdb_state_type *bdb_state = (bdb_state_type *)arg;
    int desired, exiting;

    comdb2_name_thread(__func__);
    logmsg(LOGMSG_USER, "%s: starting sql-logfill-thread\n", __func__);
    bdb_thread_event(bdb_state, COMDB2_THR_EVENT_START_RDONLY);

    while (!db_is_exiting()) {
        if (thedb->master != gbl_myhostname) {
            request_logs_from_master(bdb_state);
        }

        /* Wait for new master to be resolved */
        while ((desired = bdb_lock_desired(bdb_state)) && !(exiting = db_is_exiting())) {
            if (gbl_debug_sql_logfill) {
                logmsg(LOGMSG_USER, "%s: bdb_lock_desired=%d, exiting=%d sleeping\n", __func__, desired, exiting);
            }
            sleep(1);
        }

        /* sleep and recheck */
        sleep_for_gap_lsn(bdb_state);
    }

    /* Wait for apply-thread to complete */
    while (apply_thread_is_running) {
        sleep(1);
    }

    cleanup_sql_logfill();

    bdb_thread_event(bdb_state, COMDB2_THR_EVENT_DONE_RDONLY);
    return NULL;
}

static void *sql_apply_thread(void *arg)
{
    bdb_state_type *bdb_state = (bdb_state_type *)arg;
    comdb2_name_thread(__func__);
    bdb_thread_event(bdb_state, COMDB2_THR_EVENT_START_RDONLY);

    struct log_record copy = {0};
    logmsg(LOGMSG_USER, "%s: starting sql-apply-thread\n", __func__);

    while (!db_is_exiting()) {

        int apply_log = 0;
        Pthread_mutex_lock(&sql_apply_queue_lock);
        while (apply_queue_head == apply_queue_tail && !bdb_lock_desired(bdb_state) && !db_is_exiting()) {
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_sec += 1;
            pthread_cond_timedwait(&sql_apply_queue_cond, &sql_apply_queue_lock, &ts);
            apply_lock_waits++;
        }

        if (bdb_lock_desired(bdb_state)) {
            Pthread_mutex_unlock(&sql_apply_queue_lock);
            int cnt = 0;
            while (bdb_lock_desired(bdb_state) && !db_is_exiting()) {
                if (gbl_debug_sql_logfill) {
                    logmsg(LOGMSG_USER, "%s: lock-desired, cnt=%d\n", __func__, cnt);
                }
                sleep(1);
                cnt++;
            }
            continue;
        }

        if (apply_queue_head != apply_queue_tail) {
            struct log_record *rec = &apply_queue[apply_queue_tail];
            copy.file = rec->file;
            copy.offset = rec->offset;
            copy.rectype = rec->rectype;
            copy.recgen = rec->recgen;
            copy.gen = rec->gen;
            /* Ensure copy buffer is large enough */
            if (copy.blob_memsz < rec->blob_len) {
                void *new_blob = realloc(copy.blob, rec->blob_len);
                if (!new_blob) {
                    logmsg(LOGMSG_FATAL, "%s: realloc(%d) failed in apply thread\n", __func__, rec->blob_len);
                    abort();
                }
                copy.blob = new_blob;
                copy.blob_memsz = rec->blob_len;
            }
            memcpy(copy.blob, rec->blob, rec->blob_len);
            copy.blob_len = rec->blob_len;
            apply_log = 1;
            apply_queue_tail = (apply_queue_tail + 1) % gbl_sql_logfill_lookahead_records;
            Pthread_cond_signal(&sql_apply_queue_cond);
        }

        Pthread_mutex_unlock(&sql_apply_queue_lock);

        if (apply_log) {
            /* Acquire lock only for applying this record */
            BDB_READLOCK(__func__);
            u_int32_t gen;
            bdb_state->dbenv->get_rep_gen(bdb_state->dbenv, &gen);
            if (copy.gen == gen && (copy.recgen == 0 || gen >= copy.recgen)) {
                rep_apply_caller = "sqllogfill-apply-thread";
                bdb_state->dbenv->apply_log(bdb_state->dbenv, copy.file, copy.offset, copy.rectype, copy.blob,
                                            copy.blob_len);
                rep_apply_caller = NULL;

                records_applied++;
                bytes_applied += copy.blob_len;

            } else if (gbl_debug_sql_logfill) {
                logmsg(LOGMSG_USER, "%s: skipping apply of recgen=%u mygen=%u\n", __func__, copy.recgen, gen);
            }
            /* Release lock after applying each record - allows fetch thread to make progress */
            BDB_RELLOCK();

            if (bdb_state->attr->rep_debug_delay > 0)
                usleep(bdb_state->attr->rep_debug_delay * 1000);
        }
    }
    bdb_thread_event(bdb_state, COMDB2_THR_EVENT_DONE_RDONLY);
    apply_thread_is_running = 0;
    return NULL;
}

static void create_apply_queue(bdb_state_type *bdb_state)
{
    apply_queue = calloc(gbl_sql_logfill_lookahead_records, sizeof(struct log_record));
    if (!apply_queue) {
        logmsg(LOGMSG_FATAL, "%s: calloc failed for apply queue\n", __func__);
        abort();
    }
    apply_queue_head = apply_queue_tail = 0;
}

static void cleanup_apply_queue(void)
{
    if (apply_queue) {
        for (int i = 0; i < gbl_sql_logfill_lookahead_records; i++) {
            free(apply_queue[i].blob);
        }
        free(apply_queue);
        apply_queue = NULL;
    }
}

static void create_logfill_threads(bdb_state_type *bdb_state)
{
    Pthread_mutex_lock(&sql_logfill_lock);
    if (!gbl_exit && !sql_logfill_thds_created && gbl_sql_logfill) {
        if (gbl_sql_logfill_dedicated_apply_thread) {
            create_apply_queue(bdb_state);
            apply_thread_is_running = 1;
        }
        Pthread_create(&sql_logfill_thd, NULL, sql_logfill_thread, bdb_state);
        if (gbl_sql_logfill_dedicated_apply_thread) {
            Pthread_create(&sql_logfill_apply_thd, NULL, sql_apply_thread, bdb_state);
        }
        sql_logfill_thds_created = 1;
    }
    Pthread_mutex_unlock(&sql_logfill_lock);
}

void sql_logfill_signal(bdb_state_type *bdb_state)
{
    if (!sql_logfill_thds_created)
        create_logfill_threads(bdb_state);

    Pthread_mutex_lock(&sql_logfill_lock);
    Pthread_cond_signal(&sql_logfill_cond);
    Pthread_mutex_unlock(&sql_logfill_lock);
}

void create_sql_logfill_threads(bdb_state_type *bdb_state)
{
    create_logfill_threads(bdb_state);
}

static void cleanup_sql_logfill(void)
{
    disconnect_from_master();
    if (gbl_sql_logfill_dedicated_apply_thread) {
        cleanup_apply_queue();
    }
}
