#include "comdb2.h"
#include "bdb_int.h"
#include "truncate_log.h"
#include <phys_rep_lsn.h>
 
extern struct dbenv* thedb;

LOG_INFO find_lsn_match(cdb2_hndl_tp* repl_db, unsigned int file, unsigned int offset)
{
    size_t sql_cmd_len = 150;
    char sql_cmd[sql_cmd_len];
    int rc;
    void* blob;
    int blob_len;

    /*TODO: hardcoded rectypes, maybe expose in a nicer way? */
    rc = snprintf(sql_cmd, sql_cmd_len, 
            "select * from comdb2_transaction_logs('{%u:%u}', '{%u:%u}', 4) "
            "where rectype=15 or rectype=16 or rectype=10", 
            file, offset, file, offset);
    if (rc < 0 || rc >= sql_cmd_len)
        logmsg(LOGMSG_ERROR, "sql_cmd buffer is not long enough!\n");

    // keep checking until we have a matching lsn
    while((rc = cdb2_next_record(repl_db)) == CDB2_OK)
    {
        blob = cdb2_column_value(repl_db, 4);
        blob_len = cdb2_column_size(repl_db, 4);

        

    }

    /* TODO: fill in info */
    LOG_INFO info;
    return info;
}

int truncate_log(unsigned int file, unsigned int offset)
{

    return truncate_log_lock(thedb->bdb_env, file, offset);
}
