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

int matchable_log_type(int rectype);
int normalize_rectype(u_int32_t * rectype);

extern int gbl_verbose_physrep;
int gbl_physrep_exit_on_invalid_logstream = 0;

LOG_INFO get_last_lsn(bdb_state_type *bdb_state)
{
    int rc;

    /* get db internals */
    DB_LOGC *logc;
    DBT logrec;
    DB_LSN last_log_lsn;
    LOG_INFO log_info = {0};

    rc = bdb_state->dbenv->log_cursor(bdb_state->dbenv, &logc, 0);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: can't get log cursor rc %d\n", __func__, rc);
        return log_info;
    }
    bzero(&logrec, sizeof(DBT));
    logrec.flags = DB_DBT_MALLOC;
    rc = logc->get(logc, &last_log_lsn, &logrec, DB_LAST);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: can't get last log record rc %d\n", __func__,
               rc);
        logc->close(logc, 0);
        return log_info;
    }

    if (gbl_verbose_physrep)
        logmsg(LOGMSG_USER, "%s: LSN %u:%u\n", __func__, last_log_lsn.file,
               last_log_lsn.offset);

    log_info.file = last_log_lsn.file;
    log_info.offset = last_log_lsn.offset;
    log_info.size = logrec.size;

    if (logrec.data)
        free(logrec.data);

    logc->close(logc, 0);

    return log_info;
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
        logmsg(LOGMSG_ERROR, "%s: can't get log cursor rc %d\n", __func__, rc);
        return 1;
    }

    bzero(&logrec, sizeof(DBT));
    logrec.flags = DB_DBT_REALLOC;

    do {
        do {
            rc = logc->get(logc, &rec_lsn, &logrec, DB_PREV);
            if (rc) {
                logmsg(LOGMSG_ERROR, "%s: can't get log record rc %d\n",
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
        if (gbl_verbose_physrep) {
            logmsg(LOGMSG_USER, "%s my ts is %"PRIu64", {%u:%u}\n", __func__,
                   my_time, rec_lsn.file, rec_lsn.offset);
        }

    } while (time < my_time);

    if (logrec.data) {
        free(logrec.data);
        logrec.data = NULL;
    }

    if (gbl_verbose_physrep) {
        logmsg(LOGMSG_USER, "%s ts is %"PRIu64", {%u:%u}\n", __func__, my_time,
               rec_lsn.file, rec_lsn.offset);
    }

    *file = rec_lsn.file;
    *offset = rec_lsn.offset;

    return 0;
}

static int get_next_matchable(DB_LOGC *logc, LOG_INFO *info, int check_current,
                              DBT *logrec)
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
            logmsg(LOGMSG_ERROR, "%s: can't find log record {%d:%d}, rc %d\n",
                   __func__, info->file, info->offset, rc);
            return 1;
        }
        LOGCOPY_32(&rectype, logrec->data);
        normalize_rectype(&rectype);
        if (matchable_log_type(rectype)) {
            if (gbl_verbose_physrep) {
                logmsg(LOGMSG_USER, "%s: initial rec {%u:%u} is matchable\n",
                       __func__, info->file, info->offset);
            }
            assert(info->file == match_lsn.file);
            assert(info->offset == match_lsn.offset);
            info->size = logrec->size;
            return 0;
        }
    }

    do {
        rc = logc->get(logc, &match_lsn, logrec, DB_PREV);
        if (rc) {
            logmsg(LOGMSG_ERROR,
                   "%s: can't get prev log record for {%d:%d} rc"
                   "%d\n",
                   __func__, match_lsn.file, match_lsn.offset, rc);
            free(logrec->data);
            logrec->data = NULL;
            return 1;
        }
        LOGCOPY_32(&rectype, logrec->data);
        normalize_rectype(&rectype);
    } while (!matchable_log_type(rectype));

    info->file = match_lsn.file;
    info->offset = match_lsn.offset;
    info->size = logrec->size;

    if (gbl_verbose_physrep) {
        logmsg(LOGMSG_USER, "%s: Found matchable {%u:%u}\n", __func__,
               info->file, info->offset);
    }

    return rc;
}

/* generator code */

uint32_t get_next_offset(DB_ENV *dbenv, LOG_INFO log_info)
{
    return log_info.offset + log_info.size + dbenv->get_log_header_size(dbenv);
}

int apply_log(DB_ENV *dbenv, unsigned int file, unsigned int offset,
              int64_t rectype, void *blob, int blob_len)
{
    return dbenv->apply_log(dbenv, file, offset, rectype, blob, blob_len);
}

int truncate_log_lock(bdb_state_type *bdb_state, unsigned int file,
                      unsigned int offset, uint32_t flags)
{
    extern int gbl_online_recovery;
    char *msg = "truncate log";
    int online = gbl_online_recovery;

    if (flags &&
        bdb_state->repinfo->master_host != bdb_state->repinfo->myhost) {
        return send_truncate_to_master(bdb_state, file, offset);
    }

    if (online) {
        BDB_READLOCK(msg);
    } else {
        BDB_WRITELOCK(msg);
    }
    bdb_state->dbenv->rep_verify_match(bdb_state->dbenv, file, offset, online);

    /* have to get lock for recovery */
    BDB_RELLOCK();

    return 0;
}

LOG_INFO find_match_lsn(void *in_bdb_state, cdb2_hndl_tp *repl_db,
                        LOG_INFO start_info)
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

    rc = bdb_state->dbenv->log_cursor(bdb_state->dbenv, &logc, 0);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: can't get log cursor rc %d\n", __func__, rc);
        return info;
    }

    logrec.flags = DB_DBT_REALLOC;

    while (
        !(rc = get_next_matchable(logc, &start_info, match_current, &logrec))) {
        match_current = 0;
        snprintf(
            sql_cmd, sizeof(sql_cmd),
            "select * from comdb2_transaction_logs('{%d:%d}','{%d:%d}', 0)",
            start_info.file, start_info.offset, start_info.file,
            start_info.offset);

        if ((rc = cdb2_run_statement(repl_db, sql_cmd)) == 0) {
            if ((rc = cdb2_next_record(repl_db)) == CDB2_OK) {
                lsn = (char *)cdb2_column_value(repl_db, 0);
                if (!lsn) {
                    logmsg(LOGMSG_ERROR,
                           "%s: null lsn for probe of {%d:%d}."
                           " going to next record\n",
                           __func__, start_info.file, start_info.offset);
                    continue;
                }

                if ((rc = char_to_lsn(lsn, &match_file, &match_offset)) != 0) {
                    logmsg(LOGMSG_ERROR, "Could not parse lsn? %s\n", lsn);
                    continue;
                }

                /* check if lsns match, if not, then get next matchable */
                if (match_file != start_info.file ||
                    match_offset != start_info.offset) {
                    logmsg(LOGMSG_ERROR,
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
                            logmsg(LOGMSG_FATAL, "physreps require elect-highest-committed-gen on source- exiting\n");
                            exit(1);
                        }
                        logmsg(LOGMSG_ERROR, "physreps require elect-highest-committed-gen source\n");
                    } else {
                        info.gen = *gen;
                    }
                    logc->close(logc, 0);
                    free(logrec.data);
                    logrec.data = NULL;
                    if (gbl_verbose_physrep) {
                        logmsg(LOGMSG_USER, "%s: found match at {%d:%d}\n",
                               __func__, start_info.file, start_info.offset);
                    }
                    return info;
                } else {
                    if (gbl_verbose_physrep) {
                        logmsg(LOGMSG_USER, "%s: memcmp failed for {%d:%d}\n",
                               __func__, start_info.file, start_info.offset);
                    }
                }
            } else {
                /* Didn't find a record: just go to previous */
                if (gbl_verbose_physrep) {
                    logmsg(LOGMSG_USER,
                           "%s: probe of {%d:%d} failed, going to "
                           "previous\n",
                           __func__, start_info.file, start_info.offset);
                }
            }
        } else {
            /* Run statement failure: close cursor and handle & return */
            if (gbl_verbose_physrep) {
                logmsg(LOGMSG_USER,
                       "%s: %s returns %d, '%s': closing "
                       "connection\n",
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

    logmsg(LOGMSG_WARN, "No matchable lsns in the log\n");
    logc->close(logc, 0);
    if (logrec.data) {
        free(logrec.data);
        logrec.data = NULL;
    }
    return info;
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
    logmsg(LOGMSG_INFO, "%s\n", table);
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

int physrep_ignore_btree(const char *filename)
{
    char *start = (char *)&filename[4];
    char *end = (char *)&filename[strlen(filename) - 1];
    while (end > start && *end != '\0' && *end != '.') {
        end--;
    }
    end -= 17;
    if (end <= start) {
        logmsg(LOGMSG_ERROR, "%s: unrecognized file format for %s\n", __func__, filename);
        return 0;
    }
    char t[MAXTABLELEN + 1] = {0};
    memcpy(t, start, (end - start));
    return physrep_ignore_table(t);
}
