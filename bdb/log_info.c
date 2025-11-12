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
#include <build/db.h>
#include <bdb_int.h>
#include <log_info.h>
#include <strings.h>
#include <logmsg.h>
#include <string.h>

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
        logmsg(LOGMSG_ERROR, "%s: Can't get log cursor rc %d\n", __func__, rc);
        return log_info;
    }
    bzero(&logrec, sizeof(DBT));
    logrec.flags = DB_DBT_MALLOC;
    rc = logc->get(logc, &log_lsn, &logrec, flags);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: Can't get last log record rc %d\n", __func__, rc);
        logc->close(logc, 0);
        return log_info;
    }

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

uint32_t get_next_offset(DB_ENV *dbenv, LOG_INFO log_info)
{
    return log_info.offset + log_info.size + dbenv->get_log_header_size(dbenv);
}

int log_info_compare(LOG_INFO *a, LOG_INFO *b)
{
    if (a->file < b->file)
        return -1;
    if (a->file > b->file)
        return 1;
    if (a->offset < b->offset)
        return -1;
    if (a->offset > b->offset)
        return 1;
    return 0;
}
