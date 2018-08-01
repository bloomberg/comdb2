#include <build/db.h>
#include <strings.h>
#include <logmsg.h>
#include <string.h>

#include "bdb_int.h"
#include <dbinc/db_swap.h>
#include "phys_rep_lsn.h"
#include "locks.h"

extern bdb_state_type *bdb_state;

int matchable_log_type(int rectype);

LOG_INFO get_last_lsn(bdb_state_type* bdb_state)
{
    int rc; 

    /* get db internals */
    DB_LOGC* logc;
    DBT logrec;
    DB_LSN last_log_lsn;
    LOG_INFO log_info;
    log_info.file = log_info.offset = log_info.size = 0;

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

    logmsg(LOGMSG_WARN, "LSN %u:%u\n", last_log_lsn.file, last_log_lsn.offset);

    log_info.file = last_log_lsn.file;
    log_info.offset =  last_log_lsn.offset; 
    log_info.size = logrec.size;

    if (logrec.data)
        free(logrec.data);

    logc->close(logc, 0);

    return log_info;
}

int compare_log(bdb_state_type* bdb_state, unsigned int file, unsigned int offset,
        void* blob, unsigned int blob_len)
{
    /* TODO: like get_last_lsn, but need to expose the data this time */
    int rc; 

    /* get db internals */
    DB_LOGC* logc;
    DBT logrec;
    DB_LSN match_lsn;
    LOG_INFO log_info;
    log_info.file = log_info.offset = log_info.size = 0;

    match_lsn.file = file;
    match_lsn.offset = offset;

    rc = bdb_state->dbenv->log_cursor(bdb_state->dbenv, &logc, 0);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: can't get log cursor rc %d\n", __func__, rc);
        return 1;
    }
    bzero(&logrec, sizeof(DBT));
    logrec.flags = DB_DBT_MALLOC;
    rc = logc->get(logc, &match_lsn, &logrec, DB_SET);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: can't get log record rc %d\n", __func__,
                rc);
        logc->close(logc, 0);
        return 1;
    }

    logmsg(LOGMSG_WARN, "cmp: LSN %u:%u\n", match_lsn.file, match_lsn.offset);

    if (logrec.size != blob_len)
    {
        rc = 1;
    }
    else
    {
        rc = memcmp(blob, logrec.data, blob_len);
    }

    if (logrec.data)
        free(logrec.data);

    logc->close(logc, 0);

    return rc;
}

/* hacky way to create a generator */
static DB_LOGC* logc;

int get_next_matchable(LOG_INFO* info)
{
    /* TODO: like get_last_lsn, but need to expose the data this time */
    int rc; 
    u_int32_t rectype;

    /* get db internals */
    DBT logrec;
    DB_LSN match_lsn;
    LOG_INFO log_info;
    log_info.file = log_info.offset = log_info.size = 0;

    match_lsn.file = info->file;
    match_lsn.offset = info->offset;
    
    do
    { 
        bzero(&logrec, sizeof(DBT));
        logrec.flags = DB_DBT_MALLOC;
        rc = logc->get(logc, &match_lsn, &logrec, DB_PREV);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: can't get log record rc %d\n", __func__,
                    rc);
            logc->close(logc, 0);
            return 1;
        }

        LOGCOPY_32(&rectype, logrec.data);

        if (logrec.data)
            free(logrec.data);

    }
    while(!matchable_log_type(rectype));

    info->file = match_lsn.file;
    info->offset = match_lsn.offset;
    info->size = logrec.size;

    logmsg(LOGMSG_WARN, "Found matchable {%u:%u}\n", info->file, info->offset);

    return rc;
}

int open_db_cursor(bdb_state_type* bdb_state)
{
    int rc = bdb_state->dbenv->log_cursor(bdb_state->dbenv, &logc, 0);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: can't get log cursor rc %d\n", __func__, rc);
        return 1;
    }

    return 0;
}

void close_db_cursor()
{ 
    logc->close(logc, 0);
}

u_int32_t get_next_offset(DB_ENV* dbenv, LOG_INFO log_info)
{
    return log_info.offset + log_info.size + dbenv->get_log_header_size(dbenv);
}

int apply_log(DB_ENV* dbenv, unsigned int file, unsigned int offset, 
        int64_t rectype, void* blob, int blob_len)
{
    return dbenv->apply_log(dbenv, file, offset, rectype, blob, blob_len);
}

extern int gbl_online_recovery;
int truncate_log_lock(bdb_state_type* bdb_state, unsigned int file, 
        unsigned int offset)
{
    char* msg = "truncate log";
    int online = gbl_online_recovery;
    /* have to get lock for recovery */
    if (online) {
        BDB_READLOCK(msg);
    } else {
        BDB_WRITELOCK(msg);
    }

    bdb_state->dbenv->rep_verify_match(bdb_state->dbenv, file, offset, online);

    BDB_RELLOCK();

    return 0;
}


