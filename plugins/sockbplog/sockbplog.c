/*
   Copyright 2019 Bloomberg Finance L.P.

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

#include "comdb2.h"
#include "comdb2_appsock.h"
#include "osqlsession.h"
#include "osqlcomm.h"
#include "osqlsqlsocket.h"

extern int gbl_sockbplog_debug;
int handle_sockbplog_request(comdb2_appsock_arg_t *arg);

comdb2_appsock_t sockbplog_plugin = {
    "sockbplog",             /* Name */
    "",                      /* Usage info */
    0,                       /* Execution count */
    0,                       /* Flags */
    handle_sockbplog_request /* Handler function */
};

static int handle_sockbplog_request_session(SBUF2 *sb, char *host)
{
    osql_sess_t *sess = NULL;
    char *sql = NULL;
    int flags = 0;
    char tzname[CDB2_MAX_TZNAME] = {0};
    int type = OSQL_SOCK_REQ;
    uuid_t uuid;
    unsigned long long rqid = OSQL_RQID_USE_UUID;
    int rc;

    /* received the request; */
    rc = osqlcomm_req_socket(sb, &sql, tzname, &type, uuid, &flags);
    if (rc) {
        if (gbl_sockbplog_debug)
            logmsg(LOGMSG_ERROR, "Failure to receive osql request rc=%d\n", rc);
        goto err_nomsg;
    }

    if (gbl_sockbplog_debug)
        logmsg(LOGMSG_ERROR, "Received req %d sql %s\n", type, sql);

    /* create a session/bplog */
    sess = osql_sess_create_socket(sql, tzname, type, rqid, uuid, host, flags & OSQL_FLAGS_REORDER_ON,
                                   flags & OSQL_FLAGS_FINAL);
    if (!sess) {
        logmsg(LOGMSG_ERROR, "Malloc failure for new ireq\n");
        sbuf2printf(sb, "Error: Out of memory");
        sbuf2flush(sb);
        rc = APPSOCK_RETURN_ERR;
        goto err;
    }
    /* override the connection */
    init_bplog_socket_master(&sess->target, sb);

    /* collect the bplog; dispatch if bplog successful */
    rc = osqlcomm_bplog_socket(sb, sess);
    if (sess->is_cancelled) {
        /* Not an error. Just clean it up. */
        goto err_nomsg;
    } else if (rc) {
        logmsg(LOGMSG_ERROR, "Failure to receive osql bplog rc=%d\n", rc);
        goto err;
    }

    /* NOTE:
        here, the responsibility for handling the sess is delegated
        to the writer thread */
    if (gbl_sockbplog_debug)
        logmsg(LOGMSG_ERROR, "%p %s called\n", (void *)pthread_self(), __func__);

    return 0;

err:
    logmsg(LOGMSG_ERROR, "%p %s called and failed rc %d\n", (void *)pthread_self(),
           __func__, rc);
err_nomsg:
    if (sess) {
        osql_sess_remclient(sess);
        osql_sess_close(&sess, 0); /* false so we dont delete it from the repository */
    } else {
        free(sql);
    }
    return rc;
}

int handle_sockbplog_request(comdb2_appsock_arg_t *arg)
{
    struct sbuf2 *sb;
    char *host = "localhost";
    char line[128];
    int rc = 0;

    sb = arg->sb;

    while (!rc) {
        rc = handle_sockbplog_request_session(sb, host);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: failed to process session rc %d\n",
                   __func__, rc);
            break;
        }

        /* wait for next request */
        /* TODO: wait longer than 10 seconds */
        rc = sbuf2gets(line, sizeof(line), sb);
        if (rc < 0) {
            if (gbl_sockbplog_debug)
                logmsg(LOGMSG_ERROR,
                       "%s sbufgets returned rc %d, done sockbplog appsock\n",
                       __func__, rc);
            break;
        }
        if ((rc != strlen("sockbplog\n")) || strncmp(line, "sockbplog\n", 10)) {
            logmsg(LOGMSG_ERROR, "%s: received wrong request! rc=%d: %s\n",
                   __func__, rc, line);
            rc = -1;
        }
        if (gbl_sockbplog_debug)
            logmsg(LOGMSG_ERROR, "%s received another sockbplog string\n",
                   __func__);
        rc = 0;
    }

    return rc;
}

#include "plugin.h"
