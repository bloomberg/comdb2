#include "truncate_log.h"
#include "comdb2.h"
#include "bdb_int.h"
#include <time.h>
#include <phys_rep_lsn.h>

#include <parse_lsn.h>

extern int gbl_physrep_debug;
extern struct dbenv *thedb;
extern int gbl_match_on_ckp;

LOG_INFO find_match_lsn(void *bdb_state, cdb2_hndl_tp *repl_db,
                        LOG_INFO start_info);

LOG_INFO handle_truncation(cdb2_hndl_tp *repl_db, LOG_INFO latest_info)
{
    LOG_INFO match_lsn = find_match_lsn(thedb->bdb_env, repl_db, latest_info);

    if (match_lsn.file == 0) {
        if (gbl_physrep_debug)
            logmsg(LOGMSG_USER, "%s: unable to find match-lsn\n", __func__);
        return match_lsn;
    }

    if (gbl_physrep_debug) {
        logmsg(LOGMSG_USER, "Rewind to lsn: {%u:%u}\n", match_lsn.file,
               match_lsn.offset);
    }

    truncate_log(match_lsn.file, match_lsn.offset, 1);

    return match_lsn;
}

int truncate_timestamp(time_t timestamp)
{
    int rc;
    unsigned int file, offset;
    if ((rc = find_log_timestamp(thedb->bdb_env, timestamp, &file, &offset)) !=
        0) {
        logmsg(LOGMSG_ERROR, "Couldn't find a record older than %ld\n",
               timestamp);
        return 1;
    }
    logmsg(LOGMSG_USER, "Found lsn that works {%u:%u}", file, offset);

    return truncate_log(file, offset, 1);
}

int truncate_log(unsigned int file, unsigned int offset, uint32_t flags)
{
    return truncate_log_lock(thedb->bdb_env, file, offset, flags);
}
