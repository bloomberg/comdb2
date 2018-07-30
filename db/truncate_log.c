#include "comdb2.h"
#include "bdb_int.h"
#include "truncate_log.h"
#include <phys_rep_lsn.h>

#include <parse_lsn.h>
 
extern struct dbenv* thedb;

LOG_INFO find_match_lsn(cdb2_hndl_tp* repl_db, unsigned int file, unsigned int offset);

LOG_INFO handle_truncation(cdb2_hndl_tp* repl_db, LOG_INFO prev_info)
{
    LOG_INFO match_lsn = find_match_lsn(repl_db, prev_info.file, prev_info.offset);

    if (match_lsn.file == 0)
    {
        logmsg(LOGMSG_WARN, "File is 0, ignoring\n");
        return prev_info;
    }

    logmsg(LOGMSG_WARN, "Rewind to lsn: {%u: %u}", match_lsn.file, match_lsn.offset);

    /* TODO: Actually call truncation */
    return prev_info;
}

LOG_INFO find_match_lsn(cdb2_hndl_tp* repl_db, unsigned int file, unsigned int offset)
{
    size_t sql_cmd_len = 150;
    char* sql_cmd;
    int rc;
    void* blob;
    char* lsn;
    int blob_len;
    LOG_INFO info;

    /*TODO: hardcoded rectypes, maybe expose in a nicer way? */
    sql_cmd = "select * from comdb2_transaction_logs(NULL, NULL, 4) "
            "where rectype=15 or rectype=16 or rectype=10"; 

    // keep checking until we have a matching lsn
    while((rc = cdb2_next_record(repl_db)) == CDB2_OK)
    {
        unsigned int match_file, match_offset;

        lsn = (char *) cdb2_column_value(repl_db, 0);
        blob = cdb2_column_value(repl_db, 4);
        blob_len = cdb2_column_size(repl_db, 4);

        if ((rc = char_to_lsn(lsn, &match_file, &match_offset)) != 0)
        {
            logmsg(LOGMSG_ERROR, "Could not parse lsn? %s\n", lsn);
        }

        if ((rc = compare_log(thedb->bdb_env, match_file, match_offset, 
                        blob, blob_len)) == 0)
        {
            info.file = match_file;
            info.offset = match_offset;
            info.size = blob_len;
            return info;
        } 

    }

    logmsg(LOGMSG_WARN, "No matchable lsns in the log\n");
    info.file = 0;
    info.offset = 0;
    
    return info;
}



int truncate_log(unsigned int file, unsigned int offset)
{

    return truncate_log_lock(thedb->bdb_env, file, offset);
}
