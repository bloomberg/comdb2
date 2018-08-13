#include "comdb2.h"
#include "bdb_int.h"
#include <time.h>
#include "truncate_log.h"
#include <phys_rep_lsn.h>

#include <parse_lsn.h>
 
extern struct dbenv* thedb;

LOG_INFO find_match_lsn(cdb2_hndl_tp* repl_db, LOG_INFO start_info);

LOG_INFO handle_truncation(cdb2_hndl_tp* repl_db, LOG_INFO latest_info)
{
    LOG_INFO match_lsn = find_match_lsn(repl_db, latest_info);

    if (match_lsn.file == 0)
    {
        logmsg(LOGMSG_WARN, "File is 0, ignoring\n");
        return latest_info;
    }

    logmsg(LOGMSG_WARN, "Rewind to lsn: {%u:%u}\n", match_lsn.file, match_lsn.offset);

    /* TODO: Actually call truncation */
    truncate_log(match_lsn.file, match_lsn.offset, 1);

    return match_lsn;
}

LOG_INFO find_match_lsn(cdb2_hndl_tp* repl_db, LOG_INFO start_info)
{
    char* sql_cmd;
    int rc;
    void* blob;
    char* lsn;
    int blob_len;
    unsigned int match_file, match_offset;
    LOG_INFO info;
    info.file = 0;
    info.offset = 0;

    /* TODO: implement optimization in checking first lsn from the master */

    if ((rc = open_db_cursor(thedb->bdb_env)) != 0)
    {
        logmsg(LOGMSG_ERROR, "Couldn't open a db cursor\n");
        close_db_cursor();
        return info;
    }

    /* TODO: hardcoded rectypes, maybe expose in a nicer way? */
    sql_cmd = "select * from comdb2_transaction_logs(NULL, NULL, 4) "
            "where rectype=15 or rectype=16 or rectype=10";
    if ((rc = cdb2_run_statement(repl_db, sql_cmd)) != CDB2_OK)
    {
        logmsg(LOGMSG_ERROR, "Couldn't query the database\n");
        return info;
    }

    /* Get head ptrs for both our db cursor and remote db cursor */

    if ((rc = cdb2_next_record(repl_db)) != CDB2_OK)
    {
        logmsg(LOGMSG_WARN, "Cursor has no more records.\n");
        return info;
    }
    lsn = (char *) cdb2_column_value(repl_db, 0);
    if ((rc = char_to_lsn(lsn, &match_file, &match_offset)) != 0)
    {
        logmsg(LOGMSG_ERROR, "Could not parse lsn? %s\n", lsn);
    }
    logmsg(LOGMSG_WARN, "start with lsn{%u:%u}\n", match_file, match_offset);

    // keep checking until we have a matching lsn
    while(!(rc = get_next_matchable(&start_info)))
    {
        while(match_file > start_info.file || match_offset > start_info.offset)
        {
            if ((rc = cdb2_next_record(repl_db)) != CDB2_OK)
            {
                logmsg(LOGMSG_WARN, "Cursor has no more records.\n");
                return info;
            }
            lsn = (char *) cdb2_column_value(repl_db, 0);
            if ((rc = char_to_lsn(lsn, &match_file, &match_offset)) != 0)
            {
                logmsg(LOGMSG_ERROR, "Could not parse lsn? %s\n", lsn);
            }

            logmsg(LOGMSG_WARN, "? lsn{%u:%u}\n", match_file, match_offset);
        }

        /* check if lsns match, if not, then get next matchable */
        if (match_file != start_info.file || 
                match_offset != start_info.offset)
        {
            logmsg(LOGMSG_WARN, "not same lsn{%u:%u}\n", match_file, match_offset);
            continue;
        }

        /* here lsns match, thus we can now compare them */

        blob = cdb2_column_value(repl_db, 4);
        blob_len = cdb2_column_size(repl_db, 4);

        if ((rc = compare_log(thedb->bdb_env, match_file, match_offset, 
                        blob, blob_len)) == 0)
        {
            info.file = match_file;
            info.offset = match_offset;
            info.size = blob_len;
            info.gen = *(int64_t *) cdb2_column_value(repl_db, 2);

            return info;
        } 
        
    }

    logmsg(LOGMSG_WARN, "No matchable lsns in the log\n");
    close_db_cursor();

    return info;
}

int truncate_timestamp(time_t timestamp) 
{
    int rc;
    unsigned int file, offset;
    if ((rc = find_log_timestamp(thedb->bdb_env, timestamp, &file, &offset)) != 0)
    {
        logmsg(LOGMSG_ERROR, "Couldn't find a record older than %ld\n", timestamp);
        return 1;
    }
    logmsg(LOGMSG_USER, "Found lsn that works {%u:%u}", file, offset);

    return truncate_log(file, offset);
}

int truncate_log(unsigned int file, unsigned int offset, uint32_t flags)
{
    return truncate_log_lock(thedb->bdb_env, file, offset, flags);
}
