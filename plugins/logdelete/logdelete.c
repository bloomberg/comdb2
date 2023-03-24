/*
   Copyright 2018 Bloomberg Finance L.P.

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

#include "comdb2_plugin.h"
#include "comdb2.h"
#include "comdb2_appsock.h"
#include <comdb2_atomic.h>

/* Forward declaration */
comdb2_appsock_t logdelete3_plugin;

static int handle_logdelete_request(comdb2_appsock_arg_t *arg)
{
    struct thr_handle *thr_self;
    struct sbuf2 *sb;
    struct log_delete_state log_delete_state;
    char recovery_command[200] = {0};
    char recovery_lsn[100] = {0};
    char line[128] = {0};
    int before_master;
    int after_master;
    int before_sc;
    int after_sc;
    int report_back = 0;
    int rc;

    thr_self = arg->thr_self;
    sb = arg->sb;

    /*
      There is no difference between log delete one and two, just that
      if the db doesn't have log delete two then the comdb2logdel.tsk
      knows that this is an old binary that won't give feedback. Make
      us a special log deletion holding thread so that we don't hold
      up bounces/schema changes.
    */
    thrman_change_type(thr_self, THRTYPE_LOGDELHOLD);

    /* Disable log file deletion until this socket gets read from again. */
    log_delete_state.filenum = 0;
    log_delete_add_state(thedb, &log_delete_state);
    log_delete_counter_change(thedb, LOG_DEL_REFRESH);

    logdelete_lock(__func__, __LINE__);
    backend_update_sync(thedb);
    logdelete_unlock(__func__, __LINE__);

    /* check for after commented out below as well
    int before_count = bdb_get_low_headroom_count(thedb->bdb_env);
    */
    before_master = ATOMIC_LOAD32(gbl_master_changes);
    before_sc = gbl_sc_commit_count;
    logmsg(LOGMSG_INFO, "Disabling log file deletion\n");

    /* respond so that comdb2logdel.tsk knows it got through. */
    sbuf2printf(sb, "log file deletion disabled\n");
    sbuf2flush(sb);

    if (strncmp(logdelete3_plugin.name, arg->cmdline,
                strlen(logdelete3_plugin.name)) == 0) {
        rc = bdb_recovery_start_lsn(thedb->bdb_env, recovery_lsn,
                                    sizeof(recovery_lsn));
        if (rc) {
            logmsg(LOGMSG_ERROR, "bdb_recovery_start_lsn rc %d\n", rc);
            snprintf(recovery_command, sizeof(recovery_command),
                     "-fullrecovery");
        } else {
            snprintf(recovery_command, sizeof(recovery_command),
                     "-recovery_lsn %s", recovery_lsn);
        }
    }

    /* read from socket until it closes */
    sbuf2settimeout(sb, 0, 0);
    while (sbuf2gets(line, sizeof(line), sb) > 0) {
        static const char *delims = " \r\t\n";
        char *lasts;
        char *tok;
        tok = strtok_r(line, delims, &lasts);
        if (!tok) {
            continue;
        } else if (strcmp(tok, "report_back") == 0) {
            report_back = 1;
            break;
        } else if (strcmp(tok, "filenum") == 0) {
            int filenum;
            tok = strtok_r(NULL, delims, &lasts);
            errno = 0;
            if (tok && (filenum = strtol(tok, &lasts, 0)) > 0 && errno == 0 &&
                lasts && *lasts == '\0') {
                log_delete_state.filenum = filenum;
                log_delete_counter_change(thedb, LOG_DEL_REFRESH);
                backend_update_sync(thedb);
            } else {
                logmsg(LOGMSG_ERROR, "logdelete2 thread got bad filenum <%s>\n",
                       tok);
                sbuf2printf(sb, "expected +ve filenum\n");
                sbuf2flush(sb);
                continue;
            }
        } else if (strcmp(tok, "recovery_options") == 0) {
            logmsg(LOGMSG_DEBUG, "sent recovery options: %s\n",
                   recovery_command);
            sbuf2printf(sb, "%s\n", recovery_command);
            sbuf2flush(sb);
        } else {
            logmsg(LOGMSG_ERROR, "logdelete2 thread got unknown token <%s>\n",
                   tok);
            /* la la la la fingers in my ears */
        }
    }

    logmsg(LOGMSG_INFO, "Reenabling log file deletion\n");
    log_delete_rem_state(thedb, &log_delete_state);
    log_delete_counter_change(thedb, LOG_DEL_REFRESH);
    backend_update_sync(thedb);
    after_master = ATOMIC_LOAD32(gbl_master_changes);
    after_sc = gbl_sc_commit_count;

    /* The text we report back here is a binary protocol so don't
     * go changing the wording without checking the logic in
     * comdb2logdel.tsk. */
    if (report_back) {
        /* If we deleted log files during that due to log file deletion
         * then report so */
        /*
           int after_count = bdb_get_low_headroom_count(thedb->bdb_env);
           if(after_count != before_count) {
           sbuf2printf(sb, "Alert: log files deleted due to low disk
           headroom\n");
           }
         */
        /* (this test is not reliable) */

        /* If the master node changed during that then report that too
         */
        if (before_master != after_master) {
            sbuf2printf(sb, "Alert: master changed during operation\n");
        }

        /* If we committed a schema change then that's ruined it too...
         */
        if (before_sc != after_sc) {
            sbuf2printf(sb,
                        "Alert: schema changes committed during operation\n");
        }

        sbuf2printf(sb, ".\n");
        sbuf2flush(sb);
    }
    return APPSOCK_RETURN_OK;
}

comdb2_appsock_t logdelete_plugin = {
    "logdelete",             /* Name */
    "",                      /* Usage info */
    0,                       /* Execution count */
    0,                       /* Flags */
    handle_logdelete_request /* Handler function */
};

comdb2_appsock_t logdelete2_plugin = {
    "logdelete2",            /* Name */
    "",                      /* Usage info */
    0,                       /* Execution count */
    0,                       /* Flags */
    handle_logdelete_request /* Handler function */
};

comdb2_appsock_t logdelete3_plugin = {
    "logdelete3",            /* Name */
    "",                      /* Usage info */
    0,                       /* Execution count */
    0,                       /* Flags */
    handle_logdelete_request /* Handler function */
};

#include "plugin.h"
