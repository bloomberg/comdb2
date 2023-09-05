/*
   Copyright 2023 Bloomberg Finance L.P.

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

/*
    'reversesql' provides a way to open a channel of communication which allows
    a remote database (D), running in a lower tier (dev) to execute commands on
    a higher tier (prod) database (P) - which otherwise is restricted.

    For safety, this request can only be initiated from the higher tier db (P),
    essentially giving permission to D to execute queries on P.

    The protocol works as follows:

    1. P opens a connection to D.
    2. P sends "reversesql" request over this connection and ..
        2.1. Waits for "newsql" request to show up back on the connection
    3. D receives and processes the "reversesql" request by ..
        3.1. Passing the connection's fd to cdb2_open(fd) and
        3.2. Executing queries via the cdb2 handle initialized above.

    Note: The implementation only supports READ-ONLY queries to
    be executed over the 'reverse' connection. This is by design.
*/

#include <comdb2.h>
#include <comdb2_appsock.h>
#include <unistd.h>

#include "phys_rep.h"
#include "reversesql.h"

extern int gbl_libevent;
extern int gbl_revsql_debug;
extern int gbl_revsql_cdb2_debug;
extern int gbl_revsql_allow_command_exec;

typedef LISTC_T(reverse_conn_handle_tp) reverse_conn_handle_list_tp;

static reverse_conn_handle_list_tp reverse_conn_wait_list;
static pthread_mutex_t reverse_conn_handle_mu = PTHREAD_MUTEX_INITIALIZER;

reverse_conn_handle_tp *wait_for_reverse_conn(int timeout /*secs*/) {
    reverse_conn_handle_tp *hndl = calloc(1, sizeof(reverse_conn_handle_tp));
    if (hndl == NULL) {
        logmsg(LOGMSG_ERROR, "%s:%d out-of-memory\n", __func__, __LINE__);
        return NULL;
    }

    pthread_mutex_init(&(hndl->mu), NULL);
    pthread_cond_init(&(hndl->cond), NULL);

    pthread_mutex_lock(&reverse_conn_handle_mu);
    {
        listc_abl(&reverse_conn_wait_list, hndl);
    }
    pthread_mutex_unlock(&reverse_conn_handle_mu);

    struct timespec deadline;
    int rc = 0;

    pthread_mutex_lock(&(hndl->mu));
    {
        while (timeout > 0 && hndl->hndl == NULL && hndl->failed == 0) {
            clock_gettime(CLOCK_REALTIME, &deadline);
            deadline.tv_sec += 1;
            rc = pthread_cond_timedwait(&(hndl->cond), &(hndl->mu), &deadline);

            // Check for real error
            if (rc != 0 && rc != ETIMEDOUT) {
                break;
            }
            --timeout;
        }
    }
    pthread_mutex_unlock(&(hndl->mu));

    if (rc != 0 && hndl != NULL) {
        free(hndl);
        hndl = NULL;
    }
    return hndl;
}

void free_reverse_conn_handle(reverse_conn_handle_tp *hndl) {
    pthread_mutex_lock(&(hndl->mu));
    free(hndl->remote_dbname);
    free(hndl->remote_host);
    hndl->done = 1;
    pthread_mutex_unlock(&(hndl->mu));
    pthread_cond_signal(&hndl->cond);
}

static int execute_rev_command(const char *dbname, const char *fd, const char *command) {
    cdb2_hndl_tp *hndl;
    int rc = 0;

    assert(gbl_revsql_allow_command_exec != 0);


    if ((rc = cdb2_open(&hndl, dbname, fd, CDB2_DIRECT_CPU|CDB2_TYPE_IS_FD)) != 0) {
        logmsg(LOGMSG_ERROR, "%s:%d Failed to connect via fd: %s\n",
               __func__, __LINE__, fd);
        cdb2_close(hndl);
        return -1;
    }

    if (gbl_revsql_cdb2_debug == 1) {
        cdb2_set_debug_trace(hndl);
    }

    if (gbl_revsql_debug == 1) {
        logmsg(LOGMSG_USER, "%s:%d Reverse connected via fd: %s\n", __func__, __LINE__, fd);
    }

    logmsg(LOGMSG_USER, "%s:%d reverse-executing: %s\n", __func__, __LINE__, command);

    if ((rc = cdb2_run_statement(hndl, command)) != CDB2_OK) {
        logmsg(LOGMSG_ERROR, "%s:%d Failed to execute command: %s (error: %s)\n",
               __func__, __LINE__, command, cdb2_errstr(hndl));
        cdb2_close(hndl);
        return -1;
    }

    int row_count = 0;
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK) {
        ++row_count;
    }
    logmsg(LOGMSG_USER, "%s:%d Command executed successfully (rows: '%d')\n",
           __func__, __LINE__, row_count);
    cdb2_close(hndl);
    return 0;
}

static int handle_reversesql_request(comdb2_appsock_arg_t *arg) {
    struct sbuf2 *sb;
    int rc = 0;
    int fd;
    cdb2_hndl_tp *hndl;
    char fd_str[100];

    sb = arg->sb;
    fd = sbuf2fileno(sb);
    snprintf(fd_str, sizeof(fd_str), "%d", fd);

    if (arg->keepsocket)
        *arg->keepsocket = 1;

    // Read dbname and host
    char remote_dbname[60] = {0};
    char remote_host[60] = {0};
    char command[120] = {0};

    int read_bytes;
    read_bytes = sbuf2gets(remote_dbname, sizeof(remote_dbname), sb);
    if (read_bytes <= 1) {
        logmsg(LOGMSG_ERROR, "%s:%d No dbname in the request\n", __func__, __LINE__);
        rc = -1;
        goto done;
    }
    remote_dbname[read_bytes-1] = 0; // discard trailing '\n'

    read_bytes = sbuf2gets(remote_host, sizeof(remote_host), sb);
    if (read_bytes <= 1) {
        logmsg(LOGMSG_ERROR, "%s:%d No host info in the request\n", __func__, __LINE__);
        rc = -1;
        goto done;
    }
    remote_host[read_bytes-1] = 0; // discard trailing '\n'

    read_bytes = sbuf2gets(command, sizeof(command), sb);
    if (read_bytes >= 1)           // Safety
        command[read_bytes-1] = 0; // discard trailing '\n'

    if (gbl_revsql_debug == 1 || command[0] != 0 /* Always log when there is a command */) {
        logmsg(LOGMSG_USER, "%s:%d Received 'reversesql' request from %s@%s",
               __func__, __LINE__, remote_dbname, remote_host);
        if (command[0] != 0) {
            logmsg(LOGMSG_USER, " to execute '%s'\n", command);
        } else {
            logmsg(LOGMSG_USER, "\n");
        }
    }

    if (command[0] != 0) {
        if (gbl_revsql_allow_command_exec == 0) {
            logmsg(LOGMSG_USER, "%s:%d Command execution over reverse connection is prohibited!\n",
                   __func__, __LINE__);
        }
        execute_rev_command(remote_dbname, fd_str, command);
        goto done;
    }

    reverse_conn_handle_tp *rev_conn_hndl = NULL;

    pthread_mutex_lock(&reverse_conn_handle_mu);
    {
        rev_conn_hndl = listc_rtl(&reverse_conn_wait_list);
        if (rev_conn_hndl == NULL) {
            if (gbl_revsql_debug == 1) {
                logmsg(LOGMSG_USER, "%s:%d No handle registered, ignoring reversesql request\n",
                       __func__, __LINE__);
            }
            pthread_mutex_unlock(&reverse_conn_handle_mu);
            goto done;
        }
    }
    pthread_mutex_unlock(&reverse_conn_handle_mu);

    pthread_mutex_lock(&rev_conn_hndl->mu);

    if ((rc = cdb2_open(&hndl, remote_dbname, fd_str, CDB2_DIRECT_CPU|CDB2_TYPE_IS_FD)) != 0) {
        logmsg(LOGMSG_ERROR, "%s:%d Failed to connect via fd: %s\n",
               __func__, __LINE__, fd_str);
        rc = -1;
        rev_conn_hndl->failed = 1;
        rev_conn_hndl->hndl = NULL;
        cdb2_close(hndl);
        pthread_mutex_unlock(&rev_conn_hndl->mu);
        pthread_cond_signal(&rev_conn_hndl->cond);
        goto done;
    }

    if (gbl_revsql_cdb2_debug == 1) {
        cdb2_set_debug_trace(hndl);
    }

    if (gbl_revsql_debug == 1) {
        logmsg(LOGMSG_USER, "%s:%d Reverse connected via fd: %s\n",
               __func__, __LINE__, fd_str);
    }

    // Run example query: fetching dbname
    char *cmd = "select comdb2_dbname(), comdb2_host()";
    if (gbl_revsql_debug == 1) {
        logmsg(LOGMSG_USER, "%s:%d Going to execute '%s' over the incoming "
                            "connection to confirm we are connected and "
                            "can execute commands\n",
               __func__, __LINE__, cmd);
    }

    if ((rc = cdb2_run_statement(hndl, cmd)) != CDB2_OK) {
        logmsg(LOGMSG_ERROR, "%s:%d Failed to execute command: %s (error: %s)\n",
               __func__, __LINE__, cmd, cdb2_errstr(hndl));
        rc = -1;
        rev_conn_hndl->failed = 1;
        rev_conn_hndl->hndl = NULL;
        cdb2_close(hndl);
        pthread_mutex_unlock(&rev_conn_hndl->mu);
        pthread_cond_signal(&rev_conn_hndl->cond);
        goto done;
    }

    while ((cdb2_next_record(hndl)) == CDB2_OK) {
        char *got_dbname = (char *) cdb2_column_value(hndl, 0);
        char *got_host = (char *) cdb2_column_value(hndl, 1);

        if (strcmp(got_dbname, remote_dbname) != 0 || strcmp(got_host, remote_host) != 0) {
            logmsg(LOGMSG_ERROR, "%s:%d got(%s:%s) expected(%s:%s)\n",
                   __func__, __LINE__, got_dbname, got_host, remote_dbname, remote_host);
            logmsg(LOGMSG_ERROR, "%s:%d test failed!\n", __func__, __LINE__);
            rev_conn_hndl->failed = 1;
            rev_conn_hndl->hndl = NULL;
            cdb2_close(hndl);
            pthread_mutex_unlock(&rev_conn_hndl->mu);
            pthread_cond_signal(&rev_conn_hndl->cond);
            goto done;
        }
    }
    if (gbl_revsql_debug == 1) {
        logmsg(LOGMSG_USER, "%s:%d test passed!\n", __func__, __LINE__);
    }

    rev_conn_hndl->hndl = hndl;
    rev_conn_hndl->remote_dbname = strdup(remote_dbname);
    rev_conn_hndl->remote_host = strdup(remote_host);
    pthread_mutex_unlock(&(rev_conn_hndl->mu));

    // Signal to waiter thread that a connection handle is ready.
    pthread_cond_signal(&rev_conn_hndl->cond);

    /* Wait for the rev_conn_hndl to be done. */
    pthread_mutex_lock(&(rev_conn_hndl->mu));
    {
	struct timespec deadline;
	time_t start_time = time(NULL);
	while (!physrep_exited() && rev_conn_hndl->done != 1) {
	    clock_gettime(CLOCK_REALTIME, &deadline);
	    deadline.tv_sec += 1;

	    rc = pthread_cond_timedwait(&rev_conn_hndl->cond, &rev_conn_hndl->mu, &deadline);
	    if (rc != 0 && rc != ETIMEDOUT) {
		logmsg(LOGMSG_ERROR, "%s:%d pthread_cond_timedwait() failed (rc: %d)\n", __func__, __LINE__, rc);
		break;
	    }

	    if (gbl_revsql_debug == 1 && ((time(NULL)-start_time) % 10 == 0)) {
		logmsg(LOGMSG_USER, "%s:%d Waiting for reversesql handle to be to be marked done (waited for %ld secs)\n",
		       __func__, __LINE__, time(NULL)-start_time);
	    }
	}
    }
    pthread_mutex_unlock(&(rev_conn_hndl->mu));

    free_reverse_conn_handle(rev_conn_hndl);

done:
    if (gbl_revsql_debug == 1)
        logmsg(LOGMSG_USER, "%s:%d Reversesql appsock handler exiting\n", __func__, __LINE__);

    close_appsock(sb);
    arg->sb = NULL;
    return (rc == 0) ? APPSOCK_RETURN_OK : APPSOCK_RETURN_ERR;
}

static int reversesql_init(void *arg)
{
    listc_init(&reverse_conn_wait_list, offsetof(reverse_conn_handle_tp, lnk));
    return 0;
}

comdb2_appsock_t reversesql_plugin = {
    "reversesql",             /* Name */
    "",                       /* Usage info */
    0,                        /* Execution count */
    0,                        /* Flags */
    handle_reversesql_request /* Handler function */
};

#include "plugin.h"
