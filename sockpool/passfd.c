/*
   Copyright 2015 Bloomberg Finance L.P.

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
 * Functions to send and receive file descriptors over unix domain sockets.
 */

#include <sys/param.h>
#include <sys/poll.h>
#include <sys/socket.h>
#include <sys/socketvar.h>
#include <sys/types.h>
#include <sys/un.h>

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>

#include <passfd.h>
#include <logmsg.h>

#if defined(_IBM_SOURCE) || defined(_LINUX_SOURCE)
#define HAVE_MSGHDR_MSG_CONTROL
#endif

static int recv_fd_int(int sockfd, void *data, size_t nbytes, int *fd_recvd)
{
    ssize_t rc;
    size_t bytesleft;
    char *cdata;
    struct iovec iov[1];
    int recvfd;
#ifdef HAVE_MSGHDR_MSG_CONTROL
    union {
        struct cmsghdr cm;
        char control[CMSG_SPACE(sizeof(int))];
    } control_un;
    struct cmsghdr *cmsgptr;
#endif

    *fd_recvd = -1;
    cdata = data;
    bytesleft = nbytes;

    while (bytesleft > 0) {
        struct msghdr msg = {
#ifdef HAVE_MSGHDR_MSG_CONTROL
            .msg_control = control_un.control,
            .msg_controllen = sizeof(control_un.control),
#else
            .msg_accrights = (caddr_t)&recvfd,
            .msg_accrightslen = sizeof(int),
#endif
            .msg_name = NULL,
            .msg_namelen = 0,
            .msg_iov = iov,
            .msg_iovlen = 1};

        iov[0].iov_base = cdata;
        iov[0].iov_len = bytesleft;

        rc = recvmsg(sockfd, &msg, 0);

        if (rc == -1) {
            if (errno == EINTR)
                continue;
            return PASSFD_RECVMSG;
        }

        if (rc == 0) {
            /* Premature eof */
            return PASSFD_EOF;
        }

        cdata += rc;
        bytesleft -= rc;

/* See if we got a descriptor with this message */
#ifdef HAVE_MSGHDR_MSG_CONTROL
        cmsgptr = CMSG_FIRSTHDR(&msg);
        if (cmsgptr) {
            if (cmsgptr->cmsg_len != CMSG_LEN(sizeof(int)) ||
                cmsgptr->cmsg_level != SOL_SOCKET ||
                cmsgptr->cmsg_type != SCM_RIGHTS) {
                return PASSFD_BADCTRL;
            }
            recvfd = *((int *)CMSG_DATA(cmsgptr));
            if (*fd_recvd != -1) {
                if (close(recvfd) == -1) {
                    logmsg(LOGMSG_ERROR, "%s: error closing second fd %d: %d %s\n",
                            __func__, recvfd, errno, strerror(errno));
                }
                return PASSFD_2FDS;
            }
            *fd_recvd = recvfd;

            if (CMSG_NXTHDR(&msg, cmsgptr)) {
                return PASSFD_BADCTRL;
            }
        }
#else
        if (msg.msg_accrightslen == sizeof(int)) {
            if (*fd_recvd != -1) {
                if (close(recvfd) == -1) {
                    logmsg(LOGMSG_ERROR, "%s: error closing second fd %d: %d %s\n",
                            __func__, recvfd, errno, strerror(errno));
                }
                return PASSFD_2FDS;
            }
            *fd_recvd = recvfd;
        }
#endif
    }

    return PASSFD_SUCCESS;
}

/* This wrapper ensures that on error we close any file descriptor that we
 * may have received before the error occured.  Alse we make sure that we
 * preserve the value of errno which may be needed if the error was
 * PASSFD_RECVMSG. */
int recv_fd(int sockfd, void *data, size_t nbytes, int *fd_recvd)
{
    int rc;
    rc = recv_fd_int(sockfd, data, nbytes, fd_recvd);
    if (rc != 0 && *fd_recvd != -1) {
        int errno_save = errno;
        if (close(*fd_recvd) == -1) {
            logmsg(LOGMSG_ERROR, "%s: close(%d) error: %d %s\n", __func__, *fd_recvd,
                    errno, strerror(errno));
        }
        *fd_recvd = -1;
        errno = errno_save;
    }
    return rc;
}

int send_fd(int sockfd, const void *data, size_t nbytes, int fd_to_send)
{
    return send_fd_to(sockfd, data, nbytes, fd_to_send, 0);
}

int send_fd_to(int sockfd, const void *data, size_t nbytes, int fd_to_send,
               int timeoutms)
{
    ssize_t rc;
    size_t bytesleft;
    struct iovec iov[1];
    const char *cdata;
#ifdef HAVE_MSGHDR_MSG_CONTROL
    union {
        struct cmsghdr cm;
        char control[CMSG_SPACE(sizeof(int))];
    } control_un;
    struct cmsghdr *cmsgptr;
#endif

    bytesleft = nbytes;
    cdata = data;

    while (bytesleft > 0) {
        if (timeoutms > 0) {
            struct pollfd pol;
            int pollrc;
            pol.fd = sockfd;
            pol.events = POLLOUT;
            pollrc = poll(&pol, 1, timeoutms);
            if (pollrc == 0) {
                return PASSFD_TIMEOUT;
            } else if (pollrc == -1) {
                /* error will be in errno */
                return PASSFD_POLL;
            }
        }

        iov[0].iov_base = (caddr_t)cdata;
        iov[0].iov_len = bytesleft;

        if (fd_to_send != -1) {
            struct msghdr msg = {
#ifdef HAVE_MSGHDR_MSG_CONTROL
                .msg_control = control_un.control,
                .msg_controllen = sizeof(control_un.control),
#else
                .msg_accrights = (caddr_t)&fd_to_send,
                .msg_accrightslen = sizeof(int),
#endif
                .msg_name = NULL,
                .msg_namelen = 0,
                .msg_iov = iov,
                .msg_iovlen = 1};

#ifdef HAVE_MSGHDR_MSG_CONTROL
            cmsgptr = CMSG_FIRSTHDR(&msg);
            cmsgptr->cmsg_len = CMSG_LEN(sizeof(int));
            cmsgptr->cmsg_level = SOL_SOCKET;
            cmsgptr->cmsg_type = SCM_RIGHTS;
            *((int *)CMSG_DATA(cmsgptr)) = fd_to_send;
#endif
            rc = sendmsg(sockfd, &msg, 0);
        } else {
            struct msghdr msg = {
#ifdef HAVE_MSGHDR_MSG_CONTROL
                .msg_control = NULL,
                .msg_controllen = 0,
#else
                .msg_accrights = NULL,
                .msg_accrightslen = 0,
#endif
                .msg_name = NULL,
                .msg_namelen = 0,
                .msg_iov = iov,
                .msg_iovlen = 1};

            rc = sendmsg(sockfd, &msg, 0);
        }

        if (rc == -1) {
            if (errno == EINTR)
                continue;
            return PASSFD_SENDMSG;
        }

        if (rc == 0) {
            return PASSFD_EOF;
        }

        /* We didn't get an error, so the fd must have been sent. */
        fd_to_send = -1;

        cdata += rc;
        bytesleft -= rc;
    }

    return PASSFD_SUCCESS;
}

int passfd_errv(char *s, size_t slen, int passfd_rc)
{
    switch (passfd_rc) {
    default:
        return snprintf(s, slen, "unknown send_fd/recv_fd rcode %d", passfd_rc);

    case PASSFD_SUCCESS:
        return snprintf(s, slen, "success");

    case PASSFD_RECVMSG:
        return snprintf(s, slen, "recvmsg() error %d %s", errno,
                        strerror(errno));

    case PASSFD_SENDMSG:
        return snprintf(s, slen, "sendmsg() error %d %s", errno,
                        strerror(errno));

    case PASSFD_POLL:
        return snprintf(s, slen, "poll() error %d %s", errno, strerror(errno));

    case PASSFD_EOF:
        return snprintf(s, slen, "eof");

    case PASSFD_2FDS:
        return snprintf(s, slen, "received unexpected extra fd's");

    case PASSFD_BADCTRL:
        return snprintf(s, slen, "received unexpected bad ctrl msg");

    case PASSFD_TIMEOUT:
        return snprintf(s, slen, "timed out");
    }
}
