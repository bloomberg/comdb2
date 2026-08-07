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
#include "unistd.h"
#include <errno.h>
#include <string.h>
#include <poll.h>
#include <sys/socket.h>

#include <bdb_api.h>
#include <bdbglue.h>

/* For testcase demonstrating that file-delete cannot occur during a copy */
int gbl_debug_block_comdb2ar = 0;

/* How often (ms) the logdelete4 copy loop wakes to check whether an exclusive
 * operation (recovery / upgrade / downgrade) is waiting on a lock the copy is
 * holding, and to notice the copy client disconnecting. */
static int gbl_copy_poll_ms = 1000;

/* Forward declarations */
comdb2_appsock_t logdelete3_plugin;
comdb2_appsock_t logdelete4_plugin;

/* Return 1 if the peer has closed the connection.  Called only after a read
 * timeout (when the comdb2buf read buffer is drained), so a raw peek on the fd
 * is authoritative.  comdb2buf itself cannot distinguish a read timeout from an
 * EOF -- both surface as a negative return -- so we probe the socket directly. */
static int copy_peer_closed(int fd)
{
    struct pollfd pfd = {.fd = fd, .events = POLLIN};
    int rc = poll(&pfd, 1, 0);
    if (rc <= 0)
        return 0; /* no event pending -> it was a timeout, peer is alive */
    if (pfd.revents & (POLLHUP | POLLERR | POLLNVAL))
        return 1;
    if (pfd.revents & POLLIN) {
        char c;
        int r = recv(fd, &c, 1, MSG_PEEK | MSG_DONTWAIT);
        if (r == 0)
            return 1; /* orderly shutdown */
    }
    return 0;
}

/* If recovery, or a node upgrade/downgrade, is waiting on a lock the copy holds,
 * the copy is doomed: release the read locks so the operation can proceed, and
 * mark the copy aborted.  Recovery cannot rewind a page until we release, so a
 * copy that gets here before answering copy_complete is correctly failed.
 * Returns 1 if it just aborted the copy. */
static int copy_yield_if_blocked(bdb_state_type *bdb_state, int *locks_held, int *aborted)
{
    if (!*locks_held || !(bdb_lock_desired(bdb_state) || bdb_recoverlk_blocked(bdb_state)))
        return 0;
    logmsg(LOGMSG_WARN,
           "%s: releasing copy locks, an exclusive operation is waiting; "
           "copy will be failed\n",
           __func__);
    bdb_unlock_recovery(bdb_state);
    bdb_rellock(bdb_state, __func__, __LINE__);
    *locks_held = 0;
    *aborted = 1;
    return 1;
}

static int handle_logdelete_request(comdb2_appsock_arg_t *arg)
{
    struct thr_handle *thr_self;
    struct comdb2buf *sb;
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

    /* v3+ hands back recovery options; v4 additionally holds the copy's read
     * locks against recovery and answers the copy_complete handshake. */
    int is_v3 = (strncmp(logdelete3_plugin.name, arg->cmdline, strlen(logdelete3_plugin.name)) == 0);
    int is_v4 = (strncmp(logdelete4_plugin.name, arg->cmdline, strlen(logdelete4_plugin.name)) == 0);
    bdb_state_type *bdb_state = thedb->bdb_env;
    int locks_held = 0;
    int aborted = 0;

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

    backend_update_sync(thedb);

    before_master = ATOMIC_LOAD32(gbl_master_changes);
    before_sc = gbl_sc_commit_count;
    logmsg(LOGMSG_INFO, "Disabling log file deletion\n");

    /* logdelete4: make the copy and recovery mutually exclusive.  Hold the bdb
     * read lock and the recovery read lock for the copy's duration.  Recovery
     * write-locks recoverlk (in both online and offline modes) before it rewinds
     * any page, so holding recoverlk in read mode stops any recovery.  Holding
     * the bdb lock in read mode additionally blocks a node upgrade/downgrade
     * (which take the bdb write lock and can lead to a rewind).  Take the bdb
     * lock first, then recoverlk, matching recovery's own order so we cannot
     * deadlock.  Acquiring them blocks until any in-flight recovery has finished,
     * giving a consistent starting point. */
    if (is_v4) {
        bdb_get_readlock(bdb_state, 0, "copy", __func__, __LINE__);
        bdb_readlock_recovery(bdb_state);
        locks_held = 1;
    }

    while (gbl_debug_block_comdb2ar) {
        logmsg(LOGMSG_USER, "%s blocking comdb2ar for testcase\n", __func__);
        sleep(1);
    }

    /* respond so that comdb2logdel.tsk knows it got through. */
    cdb2buf_printf(sb, "log file deletion disabled\n");
    cdb2buf_flush(sb);

    if (is_v3 || is_v4) {
        rc = bdb_recovery_start_lsn(thedb->bdb_env, recovery_lsn, sizeof(recovery_lsn));
        if (rc) {
            logmsg(LOGMSG_ERROR, "bdb_recovery_start_lsn rc %d\n", rc);
            snprintf(recovery_command, sizeof(recovery_command), "-fullrecovery");
        } else {
            snprintf(recovery_command, sizeof(recovery_command), "-recovery_lsn %s", recovery_lsn);
        }
    }

    if (is_v4) {
        /* Poll-based command loop.  Wake every gbl_copy_poll_ms to (a) yield
         * the read locks if an exclusive operation is waiting -- which means the
         * copy is doomed, so we mark it aborted -- and (b) notice a disconnect.
         * The copy learns whether it was aborted via the copy_complete reply. */
        int fd = cdb2buf_fileno(sb);
        cdb2buf_settimeout(sb, gbl_copy_poll_ms, gbl_copy_poll_ms);
        while (1) {
            static const char *delims = " \r\t\n";
            char *lasts;
            char *tok;

            if (cdb2buf_gets(line, sizeof(line), sb) <= 0) {
                if (copy_peer_closed(fd))
                    break;
                /* read timeout: has an exclusive operation started waiting? */
                copy_yield_if_blocked(bdb_state, &locks_held, &aborted);
                continue;
            }

            tok = strtok_r(line, delims, &lasts);
            if (!tok) {
                continue;
            } else if (strcmp(tok, "filenum") == 0) {
                int filenum;
                tok = strtok_r(NULL, delims, &lasts);
                errno = 0;
                if (tok && (filenum = strtol(tok, &lasts, 0)) > 0 && errno == 0 && lasts && *lasts == '\0') {
                    log_delete_state.filenum = filenum;
                    log_delete_counter_change(thedb, LOG_DEL_REFRESH);
                    backend_update_sync(thedb);
                } else {
                    logmsg(LOGMSG_ERROR, "logdelete4 got bad filenum <%s>\n", tok ? tok : "");
                    cdb2buf_printf(sb, "expected +ve filenum\n");
                    cdb2buf_flush(sb);
                }
            } else if (strcmp(tok, "recovery_options") == 0) {
                cdb2buf_printf(sb, "%s\n", recovery_command);
                cdb2buf_flush(sb);
            } else if (strcmp(tok, "copy_complete") == 0) {
                /* The copy is valid iff the read locks were held continuously,
                 * i.e. no recovery/exclusive op ran during it.  Check once more
                 * here: a fast copy can send copy_complete before the poll-loop
                 * timeout runs, but if an exclusive op is (or was) waiting it is
                 * still blocked behind our read locks, so we catch it now. */
                copy_yield_if_blocked(bdb_state, &locks_held, &aborted);
                cdb2buf_printf(sb, "%s\n", aborted ? "aborted" : "ok");
                cdb2buf_flush(sb);
            } else {
                logmsg(LOGMSG_ERROR, "logdelete4 got unknown token <%s>\n", tok);
            }
        }
    } else {
        /* read from socket until it closes */
        cdb2buf_settimeout(sb, 0, 0);
        while (cdb2buf_gets(line, sizeof(line), sb) > 0) {
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
                if (tok && (filenum = strtol(tok, &lasts, 0)) > 0 && errno == 0 && lasts && *lasts == '\0') {
                    log_delete_state.filenum = filenum;
                    log_delete_counter_change(thedb, LOG_DEL_REFRESH);
                    backend_update_sync(thedb);
                } else {
                    logmsg(LOGMSG_ERROR, "logdelete2 thread got bad filenum <%s>\n", tok);
                    cdb2buf_printf(sb, "expected +ve filenum\n");
                    cdb2buf_flush(sb);
                    continue;
                }
            } else if (strcmp(tok, "recovery_options") == 0) {
                logmsg(LOGMSG_DEBUG, "sent recovery options: %s\n", recovery_command);
                cdb2buf_printf(sb, "%s\n", recovery_command);
                cdb2buf_flush(sb);
            } else {
                logmsg(LOGMSG_ERROR, "logdelete2 thread got unknown token <%s>\n", tok);
                /* la la la la fingers in my ears */
            }
        }
    }

    if (locks_held) {
        bdb_unlock_recovery(bdb_state);
        bdb_rellock(bdb_state, __func__, __LINE__);
        locks_held = 0;
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
        /* (this test is not reliable) */

        /* If the master node changed during that then report that too
         */
        if (before_master != after_master) {
            cdb2buf_printf(sb, "Alert: master changed during operation\n");
        }

        /* If we committed a schema change then that's ruined it too...
         */
        if (before_sc != after_sc) {
            cdb2buf_printf(sb, "Alert: schema changes committed during operation\n");
        }

        cdb2buf_printf(sb, ".\n");
        cdb2buf_flush(sb);
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

comdb2_appsock_t logdelete4_plugin = {
    "logdelete4",            /* Name */
    "",                      /* Usage info */
    0,                       /* Execution count */
    0,                       /* Flags */
    handle_logdelete_request /* Handler function */
};

#include "plugin.h"
