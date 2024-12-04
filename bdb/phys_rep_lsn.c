#include <build/db.h>
#include <strings.h>
#include <logmsg.h>
#include <string.h>

#include "bdb_int.h"
#include <dbinc/db_swap.h>
#include "phys_rep_lsn.h"
#include "tranlog.h"
#include <cdb2api.h>
#include <parse_lsn.h>
#include "locks.h"
#include <lockmacros.h>
#include <tohex.h>

#define physrep_logmsg(lvl, ...)                                               \
    do {                                                                       \
        logmsg(lvl, "physrep: " __VA_ARGS__);                                  \
    } while (0)

int matchable_log_type(int rectype);

extern int gbl_physrep_debug;
int gbl_physrep_exit_on_invalid_logstream = 0;
int gbl_physrep_ignore_queues = 1;

static LOG_INFO get_lsn_internal(bdb_state_type *bdb_state, int flags)
{
    int rc;

    /* get db internals */
    DB_LOGC *logc;
    DBT logrec;
    DB_LSN log_lsn;
    LOG_INFO log_info = {0};

    rc = bdb_state->dbenv->log_cursor(bdb_state->dbenv, &logc, 0);
    if (rc) {
        physrep_logmsg(LOGMSG_ERROR, "%s: Can't get log cursor rc %d\n", __func__, rc);
        return log_info;
    }
    bzero(&logrec, sizeof(DBT));
    logrec.flags = DB_DBT_MALLOC;
    rc = logc->get(logc, &log_lsn, &logrec, flags);
    if (rc) {
        physrep_logmsg(LOGMSG_ERROR, "%s: Can't get last log record rc %d\n", __func__,
                       rc);
        logc->close(logc, 0);
        return log_info;
    }
#if 0
    if (gbl_physrep_debug)
        physrep_logmsg(LOGMSG_USER, "%s: LSN %u:%u\n", __func__, log_lsn.file,
                       log_lsn.offset);
#endif

    log_info.file = log_lsn.file;
    log_info.offset = log_lsn.offset;
    log_info.size = logrec.size;

    if (logrec.data)
        free(logrec.data);

    logc->close(logc, 0);

    return log_info;
}

LOG_INFO get_last_lsn(bdb_state_type *bdb_state)
{
    return get_lsn_internal(bdb_state, DB_LAST);
}

LOG_INFO get_first_lsn(bdb_state_type *bdb_state)
{
    return get_lsn_internal(bdb_state, DB_FIRST);
}

int compare_log(DBT *logrec, void *blob, unsigned int blob_len)
{
    int rc;

    if (logrec->size != blob_len) {
        rc = 1;
    } else {
        rc = memcmp(blob, logrec->data, blob_len);
    }

    return rc;
}

int find_log_timestamp(bdb_state_type *bdb_state, time_t time,
                       unsigned int *file, unsigned int *offset)
{
    int rc;
    u_int64_t my_time;

    /* get db internals */
    DB_LOGC *logc;
    DBT logrec;
    DB_LSN rec_lsn;
    u_int32_t rectype;

    /* get last record then iterate */
    rc = bdb_state->dbenv->log_cursor(bdb_state->dbenv, &logc, 0);
    if (rc) {
        physrep_logmsg(LOGMSG_ERROR, "%s: Can't get log cursor rc %d\n", __func__, rc);
        return 1;
    }

    bzero(&logrec, sizeof(DBT));
    logrec.flags = DB_DBT_REALLOC;

    do {
        do {
            rc = logc->get(logc, &rec_lsn, &logrec, DB_PREV);
            if (rc) {
                physrep_logmsg(LOGMSG_ERROR, "%s: Can't get log record rc %d\n",
                               __func__, rc);
                logc->close(logc, 0);
                if (logrec.data)
                    free(logrec.data);
                return 1;
            }

            LOGCOPY_32(&rectype, logrec.data);
            normalize_rectype(&rectype);

        } while (!matchable_log_type(rectype));

        my_time = get_timestamp_from_matchable_record(logrec.data);
        if (gbl_physrep_debug) {
            physrep_logmsg(LOGMSG_USER, "%s my ts is %"PRIu64", {%u:%u}\n", __func__,
                           my_time, rec_lsn.file, rec_lsn.offset);
        }

    } while (time < my_time);

    if (logrec.data) {
        free(logrec.data);
        logrec.data = NULL;
    }

    if (gbl_physrep_debug) {
        physrep_logmsg(LOGMSG_USER, "%s ts is %"PRIu64", {%u:%u}\n", __func__, my_time,
                       rec_lsn.file, rec_lsn.offset);
    }

    *file = rec_lsn.file;
    *offset = rec_lsn.offset;

    return 0;
}

static int in_parent_range(DB_LSN *matchable_lsn, DB_LSN *parent_highest, DB_LSN *parent_lowest)
{
    if (!parent_highest || !parent_lowest) {
        return 1;
    }

    return log_compare(matchable_lsn, parent_highest) <= 0 && log_compare(matchable_lsn, parent_lowest) >= 0;
}

static int get_next_matchable(DB_LOGC *logc, LOG_INFO *info, int check_current, DBT *logrec, DB_LSN *parent_highest,
                              DB_LSN *parent_lowest)
{
    int rc;
    u_int32_t rectype;

    /* get db internals */
    DB_LSN match_lsn;
    LOG_INFO log_info;
    log_info.file = log_info.offset = log_info.size = 0;

    match_lsn.file = info->file;
    match_lsn.offset = info->offset;

    if (check_current) {
        if ((rc = logc->get(logc, &match_lsn, logrec, DB_SET)) != 0) {
            physrep_logmsg(LOGMSG_ERROR, "%s: Can't find log record {%d:%d}, rc %d\n",
                           __func__, info->file, info->offset, rc);
            return 1;
        }

        /* Punt if there's no possibility of matching */
        if (parent_lowest && log_compare(&match_lsn, parent_lowest) < 0) {
            if (gbl_physrep_debug) {
                physrep_logmsg(LOGMSG_USER, "%s: initial lsn {%u:%u} is below parent_lowest {%u:%u}\n", __func__,
                               match_lsn.file, match_lsn.offset, parent_lowest->file, parent_lowest->offset);
            }
            return 1;
        }

        LOGCOPY_32(&rectype, logrec->data);
        normalize_rectype(&rectype);

        if (matchable_log_type(rectype) && in_parent_range(&match_lsn, parent_highest, parent_lowest)) {
            if (gbl_physrep_debug) {
                physrep_logmsg(LOGMSG_USER, "%s: Initial rec {%u:%u} is matchable\n",
                               __func__, info->file, info->offset);
            }
            assert(info->file == match_lsn.file);
            assert(info->offset == match_lsn.offset);
            info->size = logrec->size;
            return 0;
        }
    }

    int matchable = 0;
    do {
        rc = logc->get(logc, &match_lsn, logrec, DB_PREV);
        if (rc) {
            physrep_logmsg(LOGMSG_ERROR, "%s: Can't get prev log record for {%d:%d} rc: %d\n",
                           __func__, match_lsn.file, match_lsn.offset, rc);
            free(logrec->data);
            logrec->data = NULL;
            return 1;
        }

        /* Punt if there's no possibility of matching */
        if (parent_lowest && log_compare(&match_lsn, parent_lowest) < 0) {
            if (gbl_physrep_debug) {
                physrep_logmsg(LOGMSG_USER, "%s: lsn {%u:%u} is below parent_lowest {%u:%u}\n", __func__,
                               match_lsn.file, match_lsn.offset, parent_lowest->file, parent_lowest->offset);
            }
            return 1;
        }

        LOGCOPY_32(&rectype, logrec->data);
        normalize_rectype(&rectype);
        matchable = (matchable_log_type(rectype) && in_parent_range(&match_lsn, parent_highest, parent_lowest));
    } while (!matchable);

    info->file = match_lsn.file;
    info->offset = match_lsn.offset;
    info->size = logrec->size;

    if (gbl_physrep_debug) {
        physrep_logmsg(LOGMSG_USER, "%s: Found matchable {%u:%u}\n", __func__,
                       info->file, info->offset);
    }

    return rc;
}

/* generator code */

uint32_t get_next_offset(DB_ENV *dbenv, LOG_INFO log_info)
{
    return log_info.offset + log_info.size + dbenv->get_log_header_size(dbenv);
}

int apply_log(bdb_state_type *bdb_state, unsigned int file, unsigned int offset, int64_t rectype, void *blob,
              int blob_len)
{
    BDB_READLOCK("apply_log");
    int rc = bdb_state->dbenv->apply_log(bdb_state->dbenv, file, offset, rectype, blob, blob_len);
    BDB_RELLOCK();
    return rc;
}

int truncate_log_lock(bdb_state_type *bdb_state, unsigned int file, unsigned int offset, uint32_t flags)
{
    extern int gbl_online_recovery;
    char *msg = "truncate log";
    int online = gbl_online_recovery;

    if (flags && bdb_state->repinfo->master_host != bdb_state->repinfo->myhost) {
        return send_truncate_to_master(bdb_state, file, offset);
    }

    bdb_state->dbenv->trigger_pause_all(bdb_state->dbenv);
    if (online) {
        BDB_READLOCK(msg);
    } else {
        BDB_WRITELOCK(msg);
    }
    bdb_state->dbenv->rep_verify_match(bdb_state->dbenv, file, offset, online);

    /* have to get lock for recovery */
    BDB_RELLOCK();
    bdb_state->dbenv->trigger_unpause_all(bdb_state->dbenv);

    return 0;
}

/* Limit how much a physical replicant will unwind */
int gbl_physrep_max_rollback = 0;

static inline int have_overlap(DB_LSN *parent_low, DB_LSN *parent_high, DB_LSN *my_low, DB_LSN *my_high)
{
    if (parent_low->file > my_high->file || parent_high->file < my_low->file) {
        return 0;
    }

    /* Technically 'correct', but parent_low shouldn't be mid-logfile */
    if (parent_low->file == my_high->file && parent_low->offset > my_high->offset) {
        return 0;
    }

    if (parent_high->file == my_low->file && parent_high->offset < my_low->offset) {
        return 0;
    }

    int max_rollback = gbl_physrep_max_rollback;
    if (max_rollback > 0 && my_high->file - parent_high->file > max_rollback) {
        return 0;
    }

    return 1;
}

/* Exposed to process-message for testcase */
void have_overlap_check(const char *inplow, const char *inphigh, const char *inmylow, const char *inmyhigh)
{
    DB_LSN plow = {0}, phigh = {0}, mylow = {0}, myhigh = {0};
    int rc;

    if ((rc = char_to_lsn(inplow, &plow.file, &plow.offset)) != 0) {
        physrep_logmsg(LOGMSG_ERROR, "%s error parsing parent-low lsn: %s\n", __func__, inplow);
        return;
    }

    if ((rc = char_to_lsn(inphigh, &phigh.file, &phigh.offset)) != 0) {
        physrep_logmsg(LOGMSG_ERROR, "%s error parsing parent-high lsn: %s\n", __func__, inphigh);
        return;
    }

    if ((rc = char_to_lsn(inmylow, &mylow.file, &mylow.offset)) != 0) {
        physrep_logmsg(LOGMSG_ERROR, "%s error parsing my-low lsn: %s\n", __func__, inmylow);
        return;
    }

    if ((rc = char_to_lsn(inmyhigh, &myhigh.file, &myhigh.offset)) != 0) {
        physrep_logmsg(LOGMSG_ERROR, "%s error parsing my-high lsn: %s\n", __func__, inmyhigh);
        return;
    }

    rc = have_overlap(&plow, &phigh, &mylow, &myhigh);
    logmsg(LOGMSG_USER, "%s overlap source=%s/%s mine=%s/%s result=%d\n", __func__, inplow, inphigh, inmylow, inmyhigh,
           rc);
}

static inline int find_my_range(DB_LOGC *logc, DB_LSN *lowest, DB_LSN *highest)
{
    int rc;
    DB_LSN rec_lsn;
    DBT logrec;

    bzero(&logrec, sizeof(DBT));
    logrec.flags = DB_DBT_REALLOC;

    rc = logc->get(logc, &rec_lsn, &logrec, DB_FIRST);
    if (rc) {
        physrep_logmsg(LOGMSG_ERROR, "%s: Can't get first log record rc %d\n", __func__, rc);
        return 0;
    }

    lowest->file = rec_lsn.file;
    lowest->offset = rec_lsn.offset;

    rc = logc->get(logc, &rec_lsn, &logrec, DB_LAST);
    if (rc) {
        physrep_logmsg(LOGMSG_ERROR, "%s: Can't get last log record rc %d\n", __func__, rc);
        free(logrec.data);
        return 0;
    }

    highest->file = rec_lsn.file;
    highest->offset = rec_lsn.offset;

    free(logrec.data);
    return 1;
}

static inline int find_parent_range(cdb2_hndl_tp *repl_db, DB_LSN *lowest, DB_LSN *highest)
{
    char *sql_cmd = "select * from "
                    "(select lsn from comdb2_transaction_logs('{0:0}','{0:0}', 0x0) limit 1),"
                    "(select lsn from comdb2_transaction_logs('{0:0}','{0:0}', 0x4) limit 1)";

    int rc;

    if ((rc = cdb2_run_statement(repl_db, sql_cmd)) == 0) {
        if ((rc = cdb2_next_record(repl_db)) == CDB2_OK) {
            char *lowest_lsn = (char *)cdb2_column_value(repl_db, 0);
            char *highest_lsn = (char *)cdb2_column_value(repl_db, 1);

            if ((rc = char_to_lsn(lowest_lsn, &lowest->file, &lowest->offset)) != 0) {
                physrep_logmsg(LOGMSG_ERROR, "%s error parsing lsn: %s\n", __func__, lowest_lsn);
                return 0;
            }
            if ((rc = char_to_lsn(highest_lsn, &highest->file, &highest->offset)) != 0) {
                physrep_logmsg(LOGMSG_ERROR, "%s error parsing lsn: %s\n", __func__, highest_lsn);
                return 0;
            }
            return 1;
        }
    }
    return 0;
}

LOG_INFO find_match_lsn(void *in_bdb_state, cdb2_hndl_tp *repl_db, LOG_INFO start_info)
{
    int rc;
    char sql_cmd[128];
    bdb_state_type *bdb_state = (bdb_state_type *)in_bdb_state;
    void *blob;
    char *lsn;
    int blob_len;
    int match_current = 1;
    unsigned int match_file, match_offset;
    LOG_INFO info = {0};
    DB_LOGC *logc;
    DBT logrec = {0};
    DB_LSN parent_lowest = {0}, parent_highest = {0};
    int have_parent_range = find_parent_range(repl_db, &parent_lowest, &parent_highest);

    rc = bdb_state->dbenv->log_cursor(bdb_state->dbenv, &logc, 0);
    if (rc) {
        physrep_logmsg(LOGMSG_ERROR, "%s: Can't get log cursor rc %d\n", __func__, rc);
        return info;
    }

    logrec.flags = DB_DBT_REALLOC;

    DB_LSN my_lowest = {0}, my_highest = {0};
    int have_my_range = find_my_range(logc, &my_lowest, &my_highest);

    if (gbl_physrep_debug) {
        logmsg(LOGMSG_INFO, "%s: parent-range: %d:%d - %d:%d, my-range: %d:%d - %d:%d\n", __func__, parent_lowest.file,
               parent_lowest.offset, parent_highest.file, parent_highest.offset, my_lowest.file, my_lowest.offset,
               my_highest.file, my_highest.offset);
    }

    if (have_my_range && have_parent_range && !have_overlap(&parent_lowest, &parent_highest, &my_lowest, &my_highest)) {
        physrep_logmsg(LOGMSG_WARN,
                       "%s: No overlap: parent_lowest {%d:%d}, "
                       "parent_highest {%d:%d}, my_lowest {%d:%d}, my_highest {%d:%d}\n",
                       __func__, parent_lowest.file, parent_lowest.offset, parent_highest.file, parent_highest.offset,
                       my_lowest.file, my_lowest.offset, my_highest.file, my_highest.offset);

        logc->close(logc, 0);
        return info;
    }

    while (!(rc = get_next_matchable(logc, &start_info, match_current, &logrec,
                                     have_parent_range ? &parent_highest : NULL, &parent_lowest))) {
        if (have_parent_range && (start_info.file < parent_lowest.file)) {
            if (gbl_physrep_debug) {
                physrep_logmsg(LOGMSG_USER, "%s: {%d:%d} is below parent_lowest {%d:%d}\n", __func__, start_info.file,
                               start_info.offset, parent_lowest.file, parent_lowest.offset);
            }

            physrep_logmsg(LOGMSG_WARN, "Parent logs of range: %d:%d - %d:%d, matchable = %d:%d\n", parent_lowest.file,
                           parent_lowest.offset, parent_highest.file, parent_highest.offset, start_info.file,
                           start_info.offset);
            logc->close(logc, 0);
            if (logrec.data) {
                free(logrec.data);
                logrec.data = NULL;
            }
            return info;
        }

        match_current = 0;
        snprintf(sql_cmd, sizeof(sql_cmd), "select * from comdb2_transaction_logs('{%d:%d}','{%d:%d}', 0)",
                 start_info.file, start_info.offset, start_info.file, start_info.offset);

        if ((rc = cdb2_run_statement(repl_db, sql_cmd)) == 0) {
            if ((rc = cdb2_next_record(repl_db)) == CDB2_OK) {
                lsn = (char *)cdb2_column_value(repl_db, 0);
                if (!lsn) {
                    if (gbl_physrep_debug) {
                        physrep_logmsg(LOGMSG_ERROR, "%s: null lsn for probe of {%d:%d}."
                                             " going to next record\n",
                               __func__, start_info.file, start_info.offset);
                    }
                    continue;
                }

                if ((rc = char_to_lsn(lsn, &match_file, &match_offset)) != 0) {
                    physrep_logmsg(LOGMSG_ERROR, "Could not parse lsn? %s\n", lsn);
                    continue;
                }

                /* check if lsns match, if not, then get next matchable */
                if (match_file != start_info.file ||
                    match_offset != start_info.offset) {
                    physrep_logmsg(LOGMSG_ERROR,
                           "%s not same lsn{%u:%u} vs "
                           "{%u:%u}??? \n",
                           __func__, start_info.file, start_info.offset,
                           match_file, match_offset);
                    continue;
                }

                /* here lsns match, thus we can now compare them */
                blob = cdb2_column_value(repl_db, 4);
                blob_len = cdb2_column_size(repl_db, 4);

                if ((rc = compare_log(&logrec, blob, blob_len)) == 0) {
                    info.file = match_file;
                    info.offset = match_offset;
                    info.size = blob_len;
                    int64_t *gen = (int64_t *)cdb2_column_value(repl_db, 2);
                    if (gen == NULL) {
                        info.gen = 0;
                        if (gbl_physrep_exit_on_invalid_logstream) {
                            physrep_logmsg(LOGMSG_FATAL, "Require elect-highest-committed-gen on source- exiting\n");
                            exit(1);
                        }
                        physrep_logmsg(LOGMSG_ERROR, "Require elect-highest-committed-gen source {%d:%d}\n", info.file,
                                       info.offset);
                    } else {
                        info.gen = *gen;
                    }
                    logc->close(logc, 0);
                    free(logrec.data);
                    logrec.data = NULL;
                    if (gbl_physrep_debug) {
                        physrep_logmsg(LOGMSG_USER, "%s: Found match at {%d:%d}\n",
                                       __func__, start_info.file, start_info.offset);
                    }
                    return info;
                } else {
                    if (gbl_physrep_debug) {
                        physrep_logmsg(LOGMSG_USER, "%s: memcmp failed for {%d:%d}\n",
                               __func__, start_info.file, start_info.offset);
                    }
                }
            } else {
                /* Didn't find a record: just go to previous */
                if (gbl_physrep_debug) {
                    physrep_logmsg(LOGMSG_USER, "%s: Probe of {%d:%d} failed, going to previous\n",
                                   __func__, start_info.file, start_info.offset);
                }
            }
        } else {
            /* Run statement failure: close cursor and handle & return */
            if (gbl_physrep_debug) {
                physrep_logmsg(LOGMSG_USER, "%s: %s returns %d, '%s': closing connection\n",
                               __func__, sql_cmd, rc, cdb2_errstr(repl_db));
            }
            logc->close(logc, 0);
            if (logrec.data) {
                free(logrec.data);
                logrec.data = NULL;
            }
            return info;
        }
    }

    physrep_logmsg(LOGMSG_WARN, "No matchable lsns in the log\n");
    logc->close(logc, 0);
    if (logrec.data) {
        free(logrec.data);
        logrec.data = NULL;
    }
    return info;
}

int physrep_bdb_wait_for_seqnum(bdb_state_type *bdb_state, DB_LSN *lsn, void *data) {
    u_int32_t rectype;

    LOGCOPY_32(&rectype, data);
    normalize_rectype(&rectype);
    if (!matchable_log_type(rectype)) {
        return 0;
    }

    seqnum_type seqnum = {0};
    seqnum.lsn.file = lsn->file;
    seqnum.lsn.offset = lsn->offset;
    // seqnum.issue_time = ?
    // seqnum.lease_ms = ?
    // seqnum.commit_generation = ?
    // seqnum.generation = ?

    return bdb_wait_for_seqnum_from_all(bdb_state, (seqnum_type *)&seqnum);
}

/* physrep logic to ignore replication traffic for some tables */
static hash_t *table_hash = NULL;
static pthread_mutex_t ignore_lk = PTHREAD_MUTEX_INITIALIZER;

static hash_t *ignore_table_hash()
{
    if (table_hash == NULL) {
        table_hash = hash_init_str(0);
    }
    return table_hash;
}

int physrep_add_ignore_table(char *tablename)
{
    Pthread_mutex_lock(&ignore_lk);
    hash_t *th = ignore_table_hash();
    if (hash_find(th, tablename) == NULL) {
        hash_add(th, tablename);
    } else {
        free(tablename);
    }
    Pthread_mutex_unlock(&ignore_lk);
    return 0;
}

int physrep_ignore_table(const char *tablename)
{
    int ret = 0;
    Pthread_mutex_lock(&ignore_lk);
    hash_t *th = ignore_table_hash();
    if (hash_find(th, tablename) != NULL) {
        ret = 1;
    }
    Pthread_mutex_unlock(&ignore_lk);
    return ret;
}

static int physrep_print_table(void *obj, void *arg)
{
    char *table = (char *)obj;
    physrep_logmsg(LOGMSG_INFO, "%s\n", table);
    return 0;
}

int physrep_list_ignored_tables(void)
{
    Pthread_mutex_lock(&ignore_lk);
    hash_t *th = ignore_table_hash();
    hash_for(th, physrep_print_table, NULL);
    Pthread_mutex_unlock(&ignore_lk);
    return 0;
}

int physrep_ignore_table_count()
{
    int count = 0;
    Pthread_mutex_lock(&ignore_lk);
    hash_t *th = ignore_table_hash();
    hash_info(th, NULL, NULL, NULL, NULL, &count, NULL, NULL);
    Pthread_mutex_unlock(&ignore_lk);
    return count;
}

/* Either XXX.<tablename> or full path */
static inline int nameboundry(char c)
{
    switch (c) {
    case '/':
    case '.':
        return 1;
    default:
        return 0;
    }
}

int physrep_ignore_btree(const char *filename)
{
    char *start = (char *)&filename[0];
    char *end = (char *)&filename[strlen(filename) - 1];
    while (end > start && *end != '\0' && *end != '.') {
        end--;
    }
    if (gbl_physrep_ignore_queues && !strcmp(end, ".queuedb")) {
        return 1;
    }
    end -= 17;
    if (end <= start) {
        return 0;
    }

    char *fstart = end;
    while (fstart > start && (end - fstart) <= MAXTABLELEN) {
        if (nameboundry(*(fstart - 1))) {
            char t[MAXTABLELEN + 1] = {0};
            memcpy(t, fstart, (end - fstart));
            return physrep_ignore_table(t);
        }
        fstart--;
    }
    return 0;
}
