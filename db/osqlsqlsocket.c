/*
   Copyright 2020 Bloomberg Finance L.P.

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

/**
 * Interface between sqlite engine and bplog. It is using a direct socket
 * connection to a remote node instead of a "net" mesh. The replicant uses an
 * appsock to communicate with the master; the receiving master stores the
 * bplog, before dispatching to a block processor writer for execution.
 * Replicant receives heartbeats to confirm the progress on the master.
 * Master catches a socket drop to detect cases when replicant fails
 *
 */
#include <sys/socket.h>

#include "comdb2.h"
#include "sql.h"
#include "tohex.h"
#include "endian_core.h"
#include "osqlsqlsocket.h"
#include "osqlblockproc.h"
#include "osqlcomm.h"

#define BPLOG_PROTO "icdb2"
#define BPLOG_APPSOCK "sockbplog"

int gbl_sockbplog_debug = 0;
extern int osql_recv_commit_rc(COMDB2BUF *sb, int timeoutms, int timeoutdeltams,
                               int *nops, struct errstat *err);

int gbl_sockbplog = 0;
int gbl_sockbplog_poll = 1000;
int gbl_sockbplog_timeout = 60000;
int gbl_sockbplog_sockpool = 0;

static int _socket_send(osql_target_t *target, int usertype, void *data,
                        int datalen, int nodelay, void *tail, int tailen);
static int osql_begin_socket(struct sqlclntstate *clnt, int type,
                             int keep_rqid);
static int osql_end_socket(struct sqlclntstate *clnt);
static int osql_wait_socket(struct sqlclntstate *clnt, int timeout,
                            struct errstat *err);

void init_bplog_socket(struct sqlclntstate *clnt)
{
    clnt->begin = osql_begin_socket;
    clnt->end = osql_end_socket;
    clnt->wait = osql_wait_socket;
}

void init_bplog_socket_master(osql_target_t *target, COMDB2BUF *sb)
{
    target->type = OSQL_OVER_SOCKET;
    target->sb = sb;
    target->send = _socket_send;
}

static int osql_begin_socket(struct sqlclntstate *clnt, int type, int keep_rqid)
{
    COMDB2BUF *sb = NULL;
    int rc;

    if (thedb->master == gbl_myhostname) {
        /* it is possible here that we are in a master swing,
        and the replicant became master! Close up any previous connection */
        if (clnt->osql.target.type == OSQL_OVER_SOCKET &&
            clnt->osql.target.sb != NULL) {
            rc = osql_end_socket(clnt);
            if (rc != 0) {
                logmsg(LOGMSG_ERROR,
                       "%s:%d closing previous socket errored rc %d\n",
                       __func__, __LINE__, rc);
            }
        }

        clnt->osql.target.type = OSQL_OVER_NET;
        return -1;
    }

    clnt->osql.target.type = OSQL_OVER_SOCKET;
    clnt->osql.target.host = thedb->master;
    clnt->osql.target.send = _socket_send;

    /* protect against no master */
    if (clnt->osql.target.host == NULL ||
        clnt->osql.target.host == db_eid_invalid)
        return 0; /* loop in caller */

    /* get a socket to target */
    clnt->osql.target.sb = sb = connect_remote_db(BPLOG_PROTO, thedb->envname, BPLOG_APPSOCK,
                                                  (char *)clnt->osql.target.host, gbl_sockbplog_sockpool, 0);
    if (!sb) {
        logmsg(LOGMSG_ERROR, "%s Failed to open socket to %s\n", __func__,
               clnt->osql.target.host);
        return -1;
    }
    /* keep alive */
    int fd = cdb2buf_fileno(sb);
    int flag = 1;

    if (setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &flag, sizeof(flag)) != 0)
        logmsg(LOGMSG_ERROR, "%s fd:%d err:%s\n", __func__, fd,
               strerror(errno));

    /* come for air every gbl_sockbplog_poll msecond */
    cdb2buf_settimeout(sb, gbl_sockbplog_poll, gbl_sockbplog_poll);

    /* appsock id */
    cdb2buf_printf(sb, BPLOG_APPSOCK);
    cdb2buf_printf(sb, "\n");
    cdb2buf_flush(sb);

    return 0;
}

static int osql_end_socket(struct sqlclntstate *clnt)
{
    if (clnt->osql.target.type != OSQL_OVER_SOCKET)
        return -1;

    disconnect_remote_db(BPLOG_PROTO, thedb->envname, BPLOG_APPSOCK,
                         (char *)clnt->osql.target.host, &clnt->osql.target.sb);
    return 0;
}

static int _socket_send(osql_target_t *target, int usertype, void *data,
                        int datalen, int nodelay, void *tail, int tailen)
{
    COMDB2BUF *sb = target->sb;
    int totallen = datalen + tailen;
    int rc;

    if (gbl_sockbplog_debug) {
        logmsg(LOGMSG_ERROR, "sending %d datalen %d tailen %d nodelay %d\n",
               usertype, datalen, tailen, nodelay);
        logmsg(LOGMSG_ERROR, "Data:\n");
        hexdump(LOGMSG_ERROR, data, datalen);
        logmsg(LOGMSG_ERROR, "Tail:\n");
        if (tailen > 0)
            hexdump(LOGMSG_ERROR, tail, tailen);
        logmsg(LOGMSG_ERROR, "Done\n");
    }

    totallen = htonl(totallen);
    rc = cdb2buf_fwrite((char *)&totallen, 1, sizeof(totallen), sb);
    if (rc != sizeof(totallen)) {
        logmsg(LOGMSG_ERROR, "%s: failed to write header rc=%d\n", __func__,
               rc);
        return -1;
    }

    rc = cdb2buf_fwrite((char *)data, 1, datalen, sb);
    if (rc != datalen) {
        logmsg(LOGMSG_ERROR, "%s: failed to write packet rc=%d\n", __func__,
               rc);
        return -1;
    }

    if (tail && tailen > 0) {
        rc = cdb2buf_fwrite((char *)tail, 1, tailen, sb);
        if (rc != tailen) {
            logmsg(LOGMSG_ERROR, "%s: failed to write packet tail rc=%d\n",
                   __func__, rc);
            return -1;
        }
    }

    if (nodelay)
        cdb2buf_flush(sb);

    return 0;
}

static int osql_wait_socket(struct sqlclntstate *clnt, int timeout,
                            struct errstat *err)
{
    if (clnt->osql.target.type != OSQL_OVER_SOCKET)
        return -1;

    assert(clnt->osql.target.sb);

    int rc;
    int nops_commit;

    rc = osql_recv_commit_rc(clnt->osql.target.sb, timeout * 1000,
                             gbl_sockbplog_poll, &nops_commit, err);
    if (rc) {
        logmsg(LOGMSG_ERROR,
               "%s failed to read commit rc from master rc %d timeout %d\n",
               __func__, rc, timeout);
    }
    if (rc == ERR_NOMASTER) {
        /* communication with the master failed, no err */
        errstat_set_rcstrf(&clnt->osql.xerr, ERR_NOMASTER,
                           "timeout receiving rc from master %s",
                           clnt->osql.target.host);
        rc = 0;
    }
    return rc;
}

int osql_read_buffer(char *p_buf, size_t p_buf_len, COMDB2BUF *sb, int *timeoutms,
                     int deltams)
{
    int rc = 0;
    int start, end;
    int initial_timeoutms = *timeoutms;

    assert(timeoutms && *timeoutms > 0);

    while (*timeoutms > 0) {
        start = comdb2_time_epochms();
        rc = cdb2buf_fread(p_buf, p_buf_len, 1, sb);
        end = comdb2_time_epochms();
        /* sbuf2 does not tell us if it was timeout or socket closed; we do not
        want to loop here if socket closed; measure the time to discriminate,
        since we use at least a 1ms timeout */
        if (rc == 0 && start == end) {
            *timeoutms = 0;
            return -1;
        }

        if (rc == 1)
            return 0;
        if (rc < 0) {
            logmsg(LOGMSG_ERROR, "%s failed to read rc from master rc %d\n",
                   __func__, rc);
            return rc;
        }

        assert(rc == 0);

        if (*timeoutms > deltams) {
            *timeoutms -= deltams;
        } else {
            logmsg(LOGMSG_ERROR, "%s timeout after %d ms\n", __func__,
                   initial_timeoutms);
            *timeoutms = 0;
            return -1;
        }
    }

    return -1;
}

int osql_read_buffer_default(char *buf, int buflen, COMDB2BUF *sb)
{
    int timeout = gbl_sockbplog_timeout;
    return osql_read_buffer(buf, buflen, sb, &timeout, gbl_sockbplog_poll);
}

#define GDATA(obj)                                                                                                     \
    do {                                                                                                               \
        rc = osql_read_buffer_default((char *)&(obj), sizeof(obj), sb);                                                \
        if (rc) {                                                                                                      \
            logmsg(LOGMSG_ERROR, "Failure to read message %d rc %d\n", __LINE__, rc);                                  \
            goto done;                                                                                                 \
        }                                                                                                              \
    } while (0)

#define GDATALEN(obj, objlen)                                                                                          \
    do {                                                                                                               \
        rc = osql_read_buffer_default((char *)obj, (objlen), sb);                                                      \
        if (rc) {                                                                                                      \
            logmsg(LOGMSG_ERROR, "Failure to read message %d rc %d\n", __LINE__, rc);                                  \
            goto done;                                                                                                 \
        }                                                                                                              \
    } while (0)

int osqlcomm_req_socket(COMDB2BUF *sb, char **sql, char tzname[DB_MAX_TZNAMEDB],
                        int *type, uuid_t uuid, int *flags)
{
    int rqlen, sqlqlen;
    int rc;
    char pad[2], unused;
    int totallen;

    *sql = NULL;

    GDATA(totallen);
    totallen = htonl(totallen);

    GDATA(*type);
    *type = htonl(*type);

    GDATA(rqlen);
    rqlen = htonl(rqlen);

    GDATA(sqlqlen);
    sqlqlen = htonl(sqlqlen);

    *sql = malloc(sqlqlen + 1);
    if (!*sql) {
        rc = ENOMEM;
        goto done;
    }
    GDATA(*flags);
    *flags = htonl(*flags);

    GDATALEN(uuid, sizeof(uuid_t));
    GDATALEN(tzname, DB_MAX_TZNAMEDB);
    GDATA(unused);
    GDATA(pad);
    GDATALEN(*sql, sqlqlen + 1 /* there is an extra byte here */);

done:
    return rc;
}

/**
 * Read the bplog body, coming from a socket
 *
 */
int osqlcomm_bplog_socket(COMDB2BUF *sb, osql_sess_t *sess)
{
    void *buf = NULL, *reallocated;
    int buflen = 0, oldbuflen = -1;
    int type;
    int rc;
    int is_msg_done = 0;

    while (!is_msg_done) {
        GDATA(buflen);
        buflen = htonl(buflen);

        if (gbl_sockbplog_debug)
            logmsg(LOGMSG_ERROR, "%p Received a packet of length %d\n",
                   (void *)pthread_self(), buflen);

        if (oldbuflen < buflen) {
            reallocated = malloc_resize(buf, buflen);
            if (!reallocated) {
                logmsg(LOGMSG_ERROR, "%s malloc buflen %d\n", __func__, buflen);
                rc = ENOMEM;
                goto done;
            }
            buf = reallocated;
            oldbuflen = buflen;
        }

        GDATALEN(buf, buflen);

        if (!(buf_get(&type, sizeof(type), buf, (char*)buf + buflen))) {
            logmsg(LOGMSG_ERROR, "%s:%s returns NULL\n", __func__,
                   "osqlcomm_uuid_rpl_type_get");
            rc = -1;
            goto done;
        }

        rc = osql_sess_rcvop_socket(sess, type, buf, buflen, &is_msg_done);
        if (rc) {
            /* failed to save into bplog; discard and be done */
            goto done;
        }
    }

done:
    free(buf);
    return rc;
}
