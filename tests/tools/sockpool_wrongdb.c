/*
   Copyright 2025 Bloomberg Finance L.P.

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
 * sockpool_wrongdb <realdb> [wrongdb] [tier]
 *
 * Checks what happens when a live connection to one database is parked in
 * sockpool under the type string of a different database.
 *
 * Steps:
 *   1. Open a cdb2api handle to <realdb> and run a query.
 *   2. Close the handle.  cdb2api donates the socket to sockpool under
 *      "comdb2/<realdb>/<tier>/newsql/<policy>".
 *   3. Take that socket back out of sockpool with our own sockpool client,
 *      then put it back under "comdb2/<wrongdb>/<tier>/newsql/<policy>".
 *   4. Open a cdb2api handle to <wrongdb>.  cdb2api asks sockpool for that
 *      type string and gets the socket that really points at <realdb>.
 *   5. Run a query on that handle.
 *
 * The server must reject that query.  The check is in newsql_loop() in
 * plugins/newsql/newsql.c: it compares the dbname in every CDB2_SQLQUERY
 * against thedb->envname and answers WRONG_DB on a mismatch.  This program
 * exits 0 when the server rejects the query and 1 when it runs it.
 *
 * cdb2api has no API to donate under a false name, so this program speaks the
 * sockpool wire protocol itself.  It does not need a modified cdb2api.
 *
 * The local connection cache would keep the socket inside this process and
 * hide it from sockpool, so the program turns that cache off.
 */

#include <errno.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>

#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>

#include <cdb2api.h>
#include <cdb2api_hndl.h>

/* ---------------------------------------------------------------- */
/* sockpool wire protocol - must match cdb2api.c and cdb2sockpool.c  */
/* ---------------------------------------------------------------- */

#define SOCKPOOL_SOCKET_NAME "/tmp/sockpool.socket"

struct sockpool_hello {
    char magic[4];
    int protocol_version;
    int pid;
    int slot;
};

struct sockpool_msg_vers0 {
    unsigned char request;
    char padding[3];
    int dbnum;
    int timeout;
    char typestr[48];
};

enum { SOCKPOOL_DONATE = 0, SOCKPOOL_REQUEST = 1 };

static int writeall(int fd, const void *buf, size_t len)
{
    const char *p = buf;
    while (len > 0) {
        ssize_t n = write(fd, p, len);
        if (n <= 0) {
            if (n == -1 && errno == EINTR)
                continue;
            return -1;
        }
        p += n;
        len -= n;
    }
    return 0;
}

/* Send a message and, if fd_to_send is not -1, one file descriptor with it. */
static int send_msg_fd(int sockfd, const void *data, size_t nbytes, int fd_to_send)
{
    const char *cdata = data;
    size_t bytesleft = nbytes;

    while (bytesleft > 0) {
        struct msghdr msg;
        struct iovec iov[1];
        union {
            struct cmsghdr cm;
            unsigned char control[CMSG_SPACE(sizeof(int))];
        } control_un;

        bzero(&msg, sizeof(msg));
        if (fd_to_send != -1) {
            struct cmsghdr *cmsgptr;
            msg.msg_control = control_un.control;
            msg.msg_controllen = sizeof(control_un.control);
            bzero(msg.msg_control, msg.msg_controllen);
            cmsgptr = CMSG_FIRSTHDR(&msg);
            cmsgptr->cmsg_len = CMSG_LEN(sizeof(int));
            cmsgptr->cmsg_level = SOL_SOCKET;
            cmsgptr->cmsg_type = SCM_RIGHTS;
            memcpy(CMSG_DATA(cmsgptr), &fd_to_send, sizeof(fd_to_send));
        }
        msg.msg_iov = iov;
        msg.msg_iovlen = 1;
        iov[0].iov_base = (caddr_t)cdata;
        iov[0].iov_len = bytesleft;

        ssize_t rc = sendmsg(sockfd, &msg, 0);
        if (rc == -1) {
            if (errno == EINTR || errno == EAGAIN)
                continue;
            fprintf(stderr, "sendmsg: %d %s\n", errno, strerror(errno));
            return -1;
        }
        if (rc == 0) {
            fprintf(stderr, "sendmsg: eof\n");
            return -1;
        }
        /* The descriptor goes out with the first successful sendmsg. */
        fd_to_send = -1;
        cdata += rc;
        bytesleft -= rc;
    }
    return 0;
}

/* Read a message and, if the peer sends one, one file descriptor with it. */
static int recv_msg_fd(int sockfd, void *data, size_t nbytes, int *fd_recvd, int timeoutms)
{
    char *cdata = data;
    size_t bytesleft = nbytes;

    *fd_recvd = -1;

    while (bytesleft > 0) {
        struct msghdr msg;
        struct iovec iov[1];
        struct cmsghdr *cmsgptr;
        union {
            struct cmsghdr cm;
            unsigned char control[CMSG_SPACE(sizeof(int))];
        } control_un;

        if (timeoutms > 0) {
            struct pollfd pol = {.fd = sockfd, .events = POLLIN};
            int pollrc = poll(&pol, 1, timeoutms);
            if (pollrc == 0) {
                fprintf(stderr, "recv_msg_fd: timeout\n");
                return -1;
            }
            if (pollrc == -1) {
                fprintf(stderr, "poll: %d %s\n", errno, strerror(errno));
                return -1;
            }
        }

        bzero(&msg, sizeof(msg));
        msg.msg_control = control_un.control;
        msg.msg_controllen = sizeof(control_un.control);
        msg.msg_iov = iov;
        msg.msg_iovlen = 1;
        iov[0].iov_base = cdata;
        iov[0].iov_len = bytesleft;

        ssize_t rc = recvmsg(sockfd, &msg, 0);
        if (rc == -1) {
            if (errno == EINTR)
                continue;
            fprintf(stderr, "recvmsg: %d %s\n", errno, strerror(errno));
            return -1;
        }
        if (rc == 0) {
            fprintf(stderr, "recvmsg: eof\n");
            return -1;
        }

        for (cmsgptr = CMSG_FIRSTHDR(&msg); cmsgptr != NULL; cmsgptr = CMSG_NXTHDR(&msg, cmsgptr)) {
            if (cmsgptr->cmsg_level == SOL_SOCKET && cmsgptr->cmsg_type == SCM_RIGHTS &&
                cmsgptr->cmsg_len == CMSG_LEN(sizeof(int))) {
                int gotfd;
                memcpy(&gotfd, CMSG_DATA(cmsgptr), sizeof(gotfd));
                if (*fd_recvd != -1)
                    close(*fd_recvd);
                *fd_recvd = gotfd;
            }
        }

        cdata += rc;
        bytesleft -= rc;
    }
    return 0;
}

/* Open a control connection to the sockpool daemon. */
static int sockpool_connect(void)
{
    const char *path = getenv("SOCKPOOL_SOCKET");
    struct sockaddr_un addr;
    struct sockpool_hello hello;
    int fd;

    if (path == NULL)
        path = SOCKPOOL_SOCKET_NAME;

    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd == -1) {
        fprintf(stderr, "socket: %d %s\n", errno, strerror(errno));
        return -1;
    }

    bzero(&addr, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, path, sizeof(addr.sun_path) - 1);
    if (connect(fd, (const struct sockaddr *)&addr, sizeof(addr)) == -1) {
        fprintf(stderr, "connect %s: %d %s\n", path, errno, strerror(errno));
        close(fd);
        return -1;
    }

    bzero(&hello, sizeof(hello));
    memcpy(hello.magic, "SQLP", 4);
    hello.protocol_version = 0;
    hello.pid = getpid();
    hello.slot = 0;
    if (writeall(fd, &hello, sizeof(hello)) != 0) {
        fprintf(stderr, "write hello: %d %s\n", errno, strerror(errno));
        close(fd);
        return -1;
    }

    return fd;
}

/* Ask sockpool for a socket of this type.  Returns the fd, or -1. */
static int sockpool_request(int sp, const char *typestr, int dbnum)
{
    struct sockpool_msg_vers0 msg;
    int fd = -1;

    bzero(&msg, sizeof(msg));
    msg.request = SOCKPOOL_REQUEST;
    msg.dbnum = dbnum;
    strncpy(msg.typestr, typestr, sizeof(msg.typestr) - 1);

    if (send_msg_fd(sp, &msg, sizeof(msg), -1) != 0)
        return -1;

    bzero(&msg, sizeof(msg));
    if (recv_msg_fd(sp, &msg, sizeof(msg), &fd, 10000) != 0)
        return -1;

    return fd;
}

/* Give a socket to sockpool under this type.  Closes fd. */
static int sockpool_donate(int sp, const char *typestr, int fd, int ttl, int dbnum)
{
    struct sockpool_msg_vers0 msg;
    int rc;

    bzero(&msg, sizeof(msg));
    msg.request = SOCKPOOL_DONATE;
    msg.dbnum = dbnum;
    msg.timeout = ttl;
    strncpy(msg.typestr, typestr, sizeof(msg.typestr) - 1);

    rc = send_msg_fd(sp, &msg, sizeof(msg), fd);
    close(fd);
    return rc;
}

/* ---------------------------------------------------------------- */

/* Replace the db name in "comdb2/<db>/<tier>/newsql/<policy>". */
static int swap_dbname(const char *typestr, const char *newdb, char *out, size_t outlen)
{
    const char *db = strchr(typestr, '/');
    const char *rest = db ? strchr(db + 1, '/') : NULL;
    int n;

    if (rest == NULL) {
        fprintf(stderr, "cannot parse type string '%s'\n", typestr);
        return -1;
    }

    n = snprintf(out, outlen, "%.*s/%s%s", (int)(db - typestr), typestr, newdb, rest);
    if (n < 0 || (size_t)n >= outlen) {
        fprintf(stderr, "type string for '%s' does not fit in %zu bytes\n", newdb, outlen);
        return -1;
    }
    return 0;
}

/* Run one statement and print every row. */
static int run_and_print(cdb2_hndl_tp *hndl, const char *sql)
{
    int rc = cdb2_run_statement(hndl, sql);
    if (rc != 0) {
        printf("    run_statement rc %d: %s\n", rc, cdb2_errstr(hndl));
        return rc;
    }
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK) {
        int ncols = cdb2_numcolumns(hndl);
        printf("   ");
        for (int i = 0; i < ncols; i++) {
            void *v = cdb2_column_value(hndl, i);
            switch (cdb2_column_type(hndl, i)) {
            case CDB2_INTEGER: printf(" %s=%lld", cdb2_column_name(hndl, i), v ? *(long long *)v : 0); break;
            case CDB2_CSTRING: printf(" %s=%s", cdb2_column_name(hndl, i), v ? (char *)v : "NULL"); break;
            default: printf(" %s=<type %d>", cdb2_column_name(hndl, i), cdb2_column_type(hndl, i)); break;
            }
        }
        printf("\n");
    }
    if (rc != CDB2_OK_DONE) {
        printf("    next_record rc %d: %s\n", rc, cdb2_errstr(hndl));
        return rc;
    }
    return 0;
}

int main(int argc, char *argv[])
{
    const char *realdb;
    const char *wrongdb = (argc > 2) ? argv[2] : "wrongdb";
    const char *tier = (argc > 3) ? argv[3] : "default";

    char real_typestr[TYPESTR_LEN];
    char wrong_typestr[TYPESTR_LEN];
    cdb2_hndl_tp *hndl = NULL;
    int sp = -1, fd = -1, dbnum, rc;

    if (argc < 2) {
        fprintf(stderr, "usage: %s <realdb> [wrongdb] [tier]\n", argv[0]);
        return 1;
    }
    realdb = argv[1];

    setvbuf(stdout, NULL, _IOLBF, 0);

    char *conf = getenv("CDB2_CONFIG");
    if (conf)
        cdb2_set_comdb2db_config(conf);

    /* Keep the socket out of the in-process cache so sockpool can see it.
       cdb2api reads this variable once, before it reads comdb2db.cfg. */
    setenv("COMDB2_CONFIG_MAX_LOCAL_CONNECTION_CACHE_ENTRIES", "0", 1);

    /* Step 1: connect to the real db and run a query. */
    printf("[1] open %s/%s and run a query\n", realdb, tier);
    rc = cdb2_open(&hndl, realdb, tier, 0);
    if (rc != 0) {
        fprintf(stderr, "cdb2_open(%s,%s) rc %d: %s\n", realdb, tier, rc, hndl ? cdb2_errstr(hndl) : "");
        return 1;
    }
    if (run_and_print(hndl, "select comdb2_dbname() as dbname") != 0) {
        cdb2_close(hndl);
        return 1;
    }

    /* Take the type string cdb2api will donate under, and build the fake one.
       Reading it from the handle keeps us right about the tier and the policy. */
    strncpy(real_typestr, hndl->newsql_typestr, sizeof(real_typestr) - 1);
    real_typestr[sizeof(real_typestr) - 1] = '\0';
    dbnum = hndl->dbnum;
    /* The fake type string has to fit in a sockpool message too. */
    if (swap_dbname(real_typestr, wrongdb, wrong_typestr, sizeof(((struct sockpool_msg_vers0 *)0)->typestr)) != 0) {
        cdb2_close(hndl);
        return 1;
    }
    printf("    real  typestr: %s\n", real_typestr);
    printf("    wrong typestr: %s\n", wrong_typestr);

    /* Step 2: close the handle.  cdb2api donates under the real name. */
    printf("[2] close the handle - cdb2api donates to sockpool\n");
    cdb2_close(hndl);
    hndl = NULL;

    /* Step 3: take the socket back and put it in under the wrong name. */
    printf("[3] re-donate that socket as '%s'\n", wrong_typestr);
    sp = sockpool_connect();
    if (sp == -1) {
        fprintf(stderr, "cannot reach sockpool - is cdb2sockpool running?\n");
        return 1;
    }
    /* The donation went out on a different sockpool connection, so give the
       daemon a moment to record it. */
    for (int attempt = 0; attempt < 20 && fd == -1; attempt++) {
        if (attempt > 0)
            usleep(50000);
        fd = sockpool_request(sp, real_typestr, dbnum);
    }
    if (fd == -1) {
        fprintf(stderr, "sockpool has no socket for '%s'\n", real_typestr);
        fprintf(stderr, "cdb2api did not donate it - is the type string too long?\n");
        close(sp);
        return 1;
    }
    printf("    got fd %d back from sockpool\n", fd);
    if (sockpool_donate(sp, wrong_typestr, fd, 600, dbnum) != 0) {
        fprintf(stderr, "donate failed\n");
        close(sp);
        return 1;
    }

    /* Step 4: open a handle to the wrong db.  cdb2api takes our socket. */
    printf("[4] open %s/%s - cdb2api must pull our socket from sockpool\n", wrongdb, tier);
    rc = cdb2_open(&hndl, wrongdb, tier, 0);
    if (rc != 0) {
        /* Not the behaviour under test.  cdb2api never got as far as asking
           sockpool, so it never used the socket we planted.  Make sure the
           config can resolve <wrongdb> to a host list. */
        fprintf(stderr, "cdb2_open(%s,%s) rc %d: %s\n", wrongdb, tier, rc, hndl ? cdb2_errstr(hndl) : "");
        fprintf(stderr, "cannot reach the sockpool path - test is inconclusive\n");
        if (hndl)
            cdb2_close(hndl);
        close(sp);
        return 1;
    }

    /* Step 5: run a query and see whether the real db serves it. */
    printf("[5] run a query on the %s handle\n", wrongdb);
    rc = run_and_print(hndl, "select comdb2_dbname() as dbname");
    if (rc == 0) {
        printf("FAIL: the query ran. The '%s' handle talks to '%s'.\n", wrongdb, realdb);
        printf("      Nothing checked the db name.\n");
    } else {
        printf("PASS: the server rejected the query: %s\n", cdb2_errstr(hndl));
    }

    cdb2_close(hndl);
    close(sp);
    return (rc == 0) ? 1 : 0;
}
