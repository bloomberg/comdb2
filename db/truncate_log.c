#include "truncate_log.h"
#include "comdb2.h"
#include "bdb_int.h"
#include <time.h>
#include <phys_rep_lsn.h>

#include <parse_lsn.h>

extern int gbl_verbose_physrep;

extern struct dbenv *thedb;

LOG_INFO find_match_lsn(cdb2_hndl_tp *repl_db, LOG_INFO start_info);

LOG_INFO handle_truncation(cdb2_hndl_tp *repl_db, LOG_INFO latest_info)
{
    LOG_INFO match_lsn = find_match_lsn(repl_db, latest_info);

    if (match_lsn.file == 0) {
        logmsg(LOGMSG_WARN, "File is 0, ignoring\n");
        return latest_info;
    }

    logmsg(LOGMSG_WARN, "Rewind to lsn: {%u:%u}\n", match_lsn.file,
           match_lsn.offset);

    truncate_log(match_lsn.file, match_lsn.offset, 1);

    return match_lsn;
}

extern int gbl_match_on_ckp;

LOG_INFO find_match_lsn(cdb2_hndl_tp *repl_db, LOG_INFO start_info)
{
    int rc;
    void *blob;
    char *lsn;
    int blob_len;
    unsigned int match_file, match_offset;
    LOG_INFO info = {0};

    /* TODO: implement optimization in checking first lsn from the master */

    if ((rc = open_db_cursor(thedb->bdb_env)) != 0) {
        logmsg(LOGMSG_ERROR, "Couldn't open a db cursor\n");
        close_db_cursor();
        return info;
    }

    while (!(rc = get_next_matchable(&start_info))) {
        char sql_cmd[128];
            snprintf(sql_cmd, sizeof(sql_cmd),
                "select * from comdb2_transaction_logs('{%d:%d}','{%d:%d}', 0)",
                start_info.file, start_info.offset, start_info.file,
                start_info.offset);

        if ((rc = cdb2_run_statement(repl_db, sql_cmd)) == CDB2_OK) {
            lsn = (char *)cdb2_column_value(repl_db, 0);
            if (!lsn) {
                if (gbl_verbose_physrep) {
                    logmsg(LOGMSG_USER, "%s null lsn for probe of {%d:%d}, "
                            "going to next record\n", __func__,
                            start_info.file,start_info.offset);
                }
                continue;
            }

            if ((rc = char_to_lsn(lsn, &match_file, &match_offset)) != 0) {
                logmsg(LOGMSG_ERROR, "Could not parse lsn? %s\n", lsn);
                abort();
            }

            /* check if lsns match, if not, then get next matchable */
            if (match_file != start_info.file ||
                    match_offset != start_info.offset) {
                if (gbl_verbose_physrep) {
                    logmsg(LOGMSG_USER, "%d not same lsn{%u:%u} vs {%u:%u}\n",
                            start_info.file, start_info.offset, match_file,
                            match_offset);
                }
                continue;
            }

            /* here lsns match, thus we can now compare them */
            blob = cdb2_column_value(repl_db, 4);
            blob_len = cdb2_column_size(repl_db, 4);

            if ((rc = compare_log(thedb->bdb_env, match_file, match_offset, blob,
                            blob_len)) == 0) {
                info.file = match_file;
                info.offset = match_offset;
                info.size = blob_len;
                info.gen = *(int64_t *)cdb2_column_value(repl_db, 2);
                close_db_cursor();
                return info;
            }
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
