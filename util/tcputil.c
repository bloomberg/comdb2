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

/* The module formerly known as "tcplib".  This provides a bunch of useful
 * utilies for making tcp connections and reading/writing messages. */

#include <tcputil.h>

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/errno.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

#include <bb_oscompat.h>
#include <logmsg.h>

static int get_sndrcv_bufsize(void)
{
    static int once = 0;
    static int retval = 0;

    if (!once) {
        char *s = getenv("TCPLIB_SNDRCV_BUFSIZE");
        if (s) {
            errno = 0;
            retval = strtol(s, 0, 10);
            if (errno != 0)
                retval = 0;
        }
        once = 1;
    }
    return retval;
}

static int get_somaxconn(void)
{
    static int once = 0;
    static int retval = SOMAXCONN;

    if (!once) {
        char *s = getenv("TCPLIB_SOMAXCONN");
        if (s) {
            errno = 0;
            retval = strtol(s, 0, 10);
            if (errno != 0 || retval < 0)
                retval = SOMAXCONN;
        }

        once = 1;
    }
    return retval;
}

/* Closing the socket fd on failure to prevent
 * file descriptor leaks  */
int tcplisten(int port)
{
    int fd, reuse_addr, keep_alive;
    struct sockaddr_in sin;
    int sndrcvbufsize;
    int somaxconn;

    memset((char *)&sin, 0, sizeof(sin));
    sin.sin_family = AF_INET;
    sin.sin_addr.s_addr = htonl(INADDR_ANY);
    sin.sin_port = htons(port);
    if ((fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        logmsgperror("tcplisten:socket");
        return -1;
    }
    reuse_addr = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (char *)&reuse_addr,
                   sizeof(reuse_addr)) != 0) {
        logmsgperror("tcplisten:setsockopt: SO_REUSEADDR");
        close(fd);
        return -1;
    }
    if ((sndrcvbufsize = get_sndrcv_bufsize())) {
        if (setsockopt(fd, SOL_SOCKET, SO_SNDBUF, (char *)&sndrcvbufsize,
                       sizeof(int)) != 0) {
            logmsgperror("tcplisten:setsockopt: SO_SNDBUF");
        }
        if (setsockopt(fd, SOL_SOCKET, SO_RCVBUF, (char *)&sndrcvbufsize,
                       sizeof(int)) != 0) {
            logmsgperror("tcplisten:setsockopt: SO_RCVBUF");
        }
    }
    /*
        linger_data.l_onoff = 1;
        linger_data.l_linger = 0;
        if (setsockopt (fd, SOL_SOCKET, SO_LINGER,
        (char *) &linger_data, sizeof (linger_data)) != 0)
        {
                perror ("tcplisten:setsockopt: SO_LINGER");
                    close(fd);
                return -1;
        }
    */
    keep_alive = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, (char *)&keep_alive,
                   sizeof(keep_alive)) != 0) {
        logmsgperror("tcplisten:setsockopt: SO_KEEPALIVE");
        close(fd);
        return -1;
    }
    /*
    ** bind an address to the socket
    */
    if (bind(fd, (struct sockaddr *)&sin, sizeof(sin)) < 0) {
        logmsgperror("tcplisten:bind");
        close(fd);
        return -1;
    }

    somaxconn = get_somaxconn();
    if (listen(fd, somaxconn) < 0) {
        logmsgperror("tcplisten:listen");
        close(fd);
        return -1;
    }
    return fd;
}

/* ACCEPT CONNECTIONS */
int tcpaccept(int listen_fd, struct sockaddr_in *client_addr)
{
    int newfd;
    struct sockaddr_in laddr;
#if defined __hpux
    int client_addr_len;
#else
    socklen_t client_addr_len;
#endif
    client_addr_len = sizeof(struct sockaddr_in);
    if (client_addr == 0)
        client_addr = &laddr;
    while (1) {
        newfd =
            accept(listen_fd, (struct sockaddr *)client_addr, &client_addr_len);
        if (newfd < 0) {
            logmsgperror("tcpaccept");
            if (errno == EINTR)
                continue;
            return -1;
        }
        return newfd;
    }
}

/*PORT IS SET ONLY If SPECIFIED IN *HOST, ELSE LEFT UNCHANGED*/
int tcpresolve(const char *host, struct in_addr *in, int *port)
{
    /*RESOLVE AN ADDRESS*/
    in_addr_t inaddr;

    int len;
    char tok[128], *cc;
    cc = strchr(host, (int)':');
    if (cc == 0) {
        len = strlen(host);
        if (len >= sizeof(tok))
            return -2;
        memcpy(tok, host, len);
        tok[len] = 0;
    } else {
        *port = atoi(cc + 1);
        len = (int)(cc - host);
        if (len >= sizeof(tok))
            return -2;
        memcpy(tok, host, len);
        tok[len] = 0;
    }
    if ((inaddr = inet_addr(tok)) != (in_addr_t)-1) {
        /* it's dotted-decimal */
        memcpy(&in->s_addr, &inaddr, sizeof(inaddr));
    } else {
        struct hostent *hp = comdb2_gethostbyname(tok);
        if (hp == NULL)
            return -1;
        memcpy(&in->s_addr, hp->h_addr, hp->h_length);
    }
    return 0;
}

static void print_connect_error(const struct sockaddr *name, const char *func)
{
    char addr_str[50];
    int errnocpy = errno;
    const char *p_str =
        inet_ntop(((struct sockaddr_in *)name)->sin_family,
                  (const void *)&((struct sockaddr_in *)name)->sin_addr,
                  addr_str, sizeof(addr_str));
    logmsg(LOGMSG_ERROR, "tcplib:%s:connect failed to %s errno %s\n", func,
            p_str ? addr_str : "unknown", strerror(errnocpy));
    errno = errnocpy;
}

static int lclconn(int s, const struct sockaddr *name, int namelen,
                   int timeoutms)
{
    /* connect with timeout */
    struct pollfd pfd;
    int flags, rc;
    int err;
#if defined(_AIX) || defined(_LINUX_SOURCE)
    socklen_t len;
#else
    int len;
#endif
    if (timeoutms <= 0)
        return connect(s, name, namelen); /*no timeout specified*/
    flags = fcntl(s, F_GETFL, 0);
    if (flags < 0)
        return -1;
    if (fcntl(s, F_SETFL, flags | O_NONBLOCK) < 0) {
        logmsgperror("tcplib:lclconn:fcntl");
        return -1;
    }
    rc = connect(s, name, namelen);
    /* EAGAIN from connect() is undocumented but, trust me, it can happen. */
    if (rc == -1 && (errno == EINPROGRESS || errno == EAGAIN)) {
        /*wait for connect event */
        do {
            pfd.fd = s;
            pfd.events = POLLOUT;
            rc = poll(&pfd, 1, timeoutms);
        } while (rc == -1 && errno == EINTR);

        if (rc == 0) {
            /*timeout*/
            /*fprintf(stderr,"connect timed out\n");*/
            return -2;
        }
        if (rc != 1) {
            /*poll failed?*/
            logmsgperror("tcplib:lclconn:poll");
            return -1;
        }
        /* On hp and linux machines we will get POLLERR event in case of connect
         * failure */
        if ((pfd.revents & (POLLOUT | POLLERR)) == 0) { /*wrong event*/
            /*fprintf(stderr,"poll event %d\n",pfd.revents);*/
            return -1;
        }
    } else if (rc == -1) {
        /*connect failed?*/
        logmsgperror("tcplib:lclconn:connect");
        return -1;
    }
    if (fcntl(s, F_SETFL, flags) < 0) {
        logmsgperror("tcplib:lclconn:fcntl2");
        return -1;
    }
    len = sizeof(err);
    if (getsockopt(s, SOL_SOCKET, SO_ERROR, &err, &len)) {
        logmsgperror("tcplib:lclconn:getsockopt");
        return -1;
    }
    errno = err;
    if (errno != 0) {
        print_connect_error(name, __func__);
        return -1;
    }
    return 0;
}

/* Non blocking connect() */
static int lclconn_nb(int s, const struct sockaddr *name, int namelen)
{
    int flags, rc;

    flags = fcntl(s, F_GETFL, 0);
    if (flags < 0)
        return -1;
    if (fcntl(s, F_SETFL, flags | O_NONBLOCK) < 0) {
        logmsgperror("tcplib:lclconn_nb:fcntl");
        return -1;
    }
    rc = connect(s, name, namelen);
    if (rc == -1 && errno != EINPROGRESS) {
        print_connect_error(name, __func__);
        return -1;
    }

    /* connected, or connecting */
    return s;
}

static int do_tcpconnect(struct in_addr in, int port, int myport, int timeoutms,
                         int nb, char tos)
{
    int sockfd, rc;
    int sendbuff, sndrcvbufsize;
    struct sockaddr_in tcp_srv_addr; /* server's Internet socket addr */
    struct sockaddr_in my_addr;      /* my Internet address */
    bzero((char *)&tcp_srv_addr, sizeof tcp_srv_addr);
    tcp_srv_addr.sin_family = AF_INET;
    if (port <= 0) {
        logmsg(LOGMSG_ERROR, "do_tcpconnect: must specify remote port\n");
        return -1;
    }
    tcp_srv_addr.sin_port = htons(port);
    memcpy(&tcp_srv_addr.sin_addr, &in.s_addr, sizeof(in.s_addr));
    if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        logmsgperror("do_tcpconnect: can't create TCP socket");
        return -1;
    }
    if ((sndrcvbufsize = get_sndrcv_bufsize())) {
        if (setsockopt(sockfd, SOL_SOCKET, SO_SNDBUF, (char *)&sndrcvbufsize,
                       sizeof(int)) != 0) {
            logmsgperror("do_tcpconnect:setsockopt: SO_SNDBUF");
        }
        if (setsockopt(sockfd, SOL_SOCKET, SO_RCVBUF, (char *)&sndrcvbufsize,
                       sizeof(int)) != 0) {
            logmsgperror("do_tcpconnect:setsockopt: SO_RCVBUF");
        }
    }

    /* Set type of service / differentiated services byte */
    if (tos != -1) {
        int itos = tos;

        if (setsockopt(sockfd, IPPROTO_IP, IP_TOS, &itos, sizeof(itos)) != 0) {
            logmsgperror("do_tcpconnect: setsockopt IP_TOS");
        }
    }
    /*
        ling.l_onoff = 1;
        ling.l_linger = 0;
        if (setsockopt( sockfd, SOL_SOCKET, SO_LINGER, (char *)&ling,
        sizeof(ling)) < 0)
        {
                fprintf(stderr,"tcpconnect_to: setsockopt failure:%s",
                strerror(errno) );
                close( sockfd );
                return -1;
        }
    */
    if (myport > 0) { /* want to use specific port on local host */
        /* Allow connect port to be re-used */
        sendbuff = 1; /* enable option */
        if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, (char *)&sendbuff,
                       sizeof sendbuff) < 0) {
            logmsgperror("do_tcpconnect: setsockopt failure");
            close(sockfd);
            return -1;
        }

        bzero((char *)&my_addr, sizeof my_addr);
        my_addr.sin_family = AF_INET;
        my_addr.sin_addr.s_addr = INADDR_ANY;
        my_addr.sin_port = htons((unsigned short)myport);
        if (bind(sockfd, (struct sockaddr *)&my_addr, sizeof my_addr) < 0) {
            logmsg(LOGMSG_ERROR, "do_tcpconnect: bind failed on local port %d: %s",
                    myport, strerror(errno));
            close(sockfd);
            return -1;
        }
    }
    /* Connect to the server.  */
    if (nb)
        rc = lclconn_nb(sockfd, (struct sockaddr *)&tcp_srv_addr,
                        sizeof(tcp_srv_addr));
    else
        rc = lclconn(sockfd, (struct sockaddr *)&tcp_srv_addr,
                     sizeof(tcp_srv_addr), timeoutms);

    if (rc < 0) {
        close(sockfd);
        return rc;
    }
    return (sockfd); /* all OK */
}

int tcpconnect_to(struct in_addr in, int port, int myport, int timeoutms)
{
    return do_tcpconnect(in, port, myport, timeoutms, 0, -1 /* no tos */);
}

int tcpconnect_to_tos(struct in_addr in, int port, int myport, int timeoutms,
                      char tos)
{
    return do_tcpconnect(in, port, myport, timeoutms, 0, tos);
}

/* Non-blocking version of tcpconnect.  Socket will be left in a non-blocking
 * state */
int tcpconnect_nb(struct in_addr in, int port, int myport)
{
    return do_tcpconnect(in, port, myport, 0, 1 /* nb */, -1 /* no tos */);
}

int tcpconnect(struct in_addr in, int port, int myport)
{
    return tcpconnect_to(in, port, myport, 0);
}

int tcpconnect_tos(struct in_addr in, int port, int myport, char tos)
{
    return do_tcpconnect(in, port, myport, 0, 0, tos);
}

int tcpconnecth(const char *host, int port, int myport)
{
    int rc;
    struct in_addr in;
    if ((rc = tcpresolve(host, &in, &port)) != 0)
        return rc;
    return tcpconnect(in, port, myport);
}

int tcpconnecth_to(const char *host, int port, int myport, int timeoutms)
{
    int rc;
    struct in_addr in;
    if ((rc = tcpresolve(host, &in, &port)) != 0)
        return rc;
    return tcpconnect_to(in, port, myport, timeoutms);
}

/*THIS DUPS SD AND RETURNS INPUT/OUTPUT FILE DESCRIPTOR
  RETURNS OUTPUT SD or -1  & closed file if failed */
int tcpfdopen(int insd, FILE **ainfil, char *inbuf, int linbuf, int ibtyp,
              FILE **aoutfil, char *outbuf, int loutbuf, int obtyp)
{
    int outsd, rc;
    FILE *infil, *outfil;
    if ((outsd = dup(insd)) < 0) {
        logmsgperror("emsg:tcpfdopen:failed dup insd");
        close(insd);
        return -1;
    }
    infil = fdopen(insd, "r");
    if (infil == 0) {
        logmsgperror("emsg:tcpfdopen:failed fdopen r");
        close(insd);
        close(outsd);
        return -2;
    }
    outfil = fdopen(outsd, "w");
    if (outfil == 0) {
        logmsgperror("emsg:tcpfdopen:failed fdopen w");
        fclose(infil);
        close(outsd);
        return -3;
    }
    if ((rc = setvbuf(infil, inbuf, ibtyp, linbuf)) != 0) {
        logmsg(LOGMSG_ERROR, "emsg:tcpfdopen:failed setvbuf r typ "
                        "%d sz %d\n",
                ibtyp, linbuf);
        fclose(infil);
        fclose(outfil);
        return -4;
    }
    if ((rc = setvbuf(outfil, outbuf, obtyp, loutbuf)) != 0) {
        logmsg(LOGMSG_ERROR, "emsg:tcpfdopen:failed setvbuf w typ "
                        "%d sz %d\n",
                obtyp, loutbuf);
        fclose(infil);
        fclose(outfil);
        return -5;
    }
    *ainfil = infil;
    *aoutfil = outfil;
    return outsd;
}

static int tcpwaitwrite(int fd, int timeoutms)
{
    struct pollfd pol;
    pol.fd = fd;
    pol.events = POLLOUT;
    int rc = poll(&pol, 1, timeoutms);
    if (rc <= 0)
        return rc; /*timed out or error*/
    if ((pol.revents & POLLOUT) == 0)
        return -1;
    return 1;
}

int tcpwrite(int fd, const void *cc, int len, int timeoutms)
{
    /*returns 0 if timed out*/
    struct pollfd pol;
    if (timeoutms > 0) {
        int rc = tcpwaitwrite(fd, timeoutms);
        if (rc <= 0)
            return rc;
        /*can write*/
    }
    return write(fd, cc, len);
}

int tcpread(int fd, void *cc, int len, int timeoutms)
{
    /*returns 0 if timed out*/
    struct pollfd pol;
    if (timeoutms > 0) {
        pol.fd = fd;
        pol.events = POLLIN;
        int rc = poll(&pol, 1, timeoutms);
        if (rc <= 0)
            return rc; /*timed out or error*/
        if ((pol.revents & POLLIN) == 0)
            return -1;
        /*something to read*/
    }
    return read(fd, cc, len);
}

int tcpwritemsg(int fd, const void *cc, int len, int timeoutms)
{
    /*writes whole message, advances through buf for partials*/
    int ii, left, rc, zero = 0;
    char *ccc = (char *)cc;
    for (ii = 0; ii < len; ii += rc) {
        left = len - ii;
        while ((rc = tcpwrite(fd, &ccc[ii], left, timeoutms)) < 0) {
            if (errno != EINTR && errno != EAGAIN)
                return -1;
        }
        if (rc == 0) {
            logmsg(LOGMSG_ERROR, "tcpwritemsg:write 0 fd %d\n", fd);
            zero++;
            if (zero > 5)
                return -2;
        }
    }
    return len;
}

int tcpwritemsgv(int fd, int niov, const struct iovec *iniov, int timeoutms)
{
    int curiov, curoff, left, sent, tot, zero;
    char *base;
    struct iovec iov[12];
    if (niov > 12)
        return -7777; /*internal limit*/
    if (niov < 1 || iniov[0].iov_len < 1)
        return -7778; /*bad input*/
    memcpy(iov, iniov, sizeof(struct iovec) * niov);
    tot = 0;
    sent = 0;
    curoff = 0;
    curiov = 0;
    zero = 0;
    do {
        base = (char *)iniov[curiov].iov_base;
        iov[curiov].iov_base = (void *)(&base[curoff]);
        iov[curiov].iov_len = iniov[curiov].iov_len - curoff;

        sent = 1;
        if (timeoutms > 0)
            sent = tcpwaitwrite(fd, timeoutms);
        if (sent > 0)
            sent = writev(fd, &iov[curiov], niov - curiov);

        if (sent == 0) {
            if (zero++ < 5)
                continue;
            logmsg(LOGMSG_ERROR, "tcpwritemsgv:write 0 fd %d\n", fd);
            return -2;
        }
        if (sent < 0) {
            if (errno == EINTR)
                continue;
            if (errno == EAGAIN)
                continue;
            return -1;
        }
        zero = 0;
        tot += sent;
        while (sent > 0) {
            left = iniov[curiov].iov_len - curoff;
            if (left <= sent) {
                /*advance to next entry*/
                sent -= left;
                curiov++;
                curoff = 0;
                continue;
            }
            curoff += sent;
            break;
        }
    } while (curiov < niov);
    return tot;
}

int tcpreadmsg(int fd, void *cc, int len, int timeoutms)
{
    /*reads whole message, advances through buf for partials*/
    int ii, left, rc;
    char *ccc = (char *)cc;
    for (ii = 0; ii < len; ii += rc) {
        left = len - ii;
        while ((rc = tcpread(fd, &ccc[ii], left, timeoutms)) < 0) {
            if (errno != EINTR && errno != EAGAIN)
                return -1;
        }
        if (rc == 0)
            return 0;
    }
    return len;
}

int tcpreadline(int fd, void *vptr, size_t maxlen, int timeoutms)
{
    int n;
    char c, *ptr;
    int rc;

    ptr = vptr;
    for (n = 1; n < maxlen; n++) {
    again:
        /* tcp read may return an error without setting errno, so clear it
           first */
        errno = 0;
        if ((rc = tcpread(fd, &c, 1, timeoutms)) == 1) {
            *ptr++ = c;
            if (c == '\n') /* store newlink like fgets */
                break;
        } else if (rc == 0) {
            if (n == 1)
                return 0;
            else
                break;
        } else if (-1 == rc && EINTR == errno)
            goto again;
        else
            return -1;
    }
    *ptr = 0; /* null terminate */
    return n;
}

#ifdef TCPLIB_TESTPROGRAM

int main(int argc, char **argv)
{
    struct servent *sent;
    char request[128] = "HELP\n";
    char response[1024];
    int port;
    int fd;
    int bytes_read;
    int rc;

    sent = comdb2_getservbyname("ftp", "tcp");
    if (sent == NULL) {
        perror("getservbyname");
        return -1;
    }

    /* port comes back in network order from getservbyname;
     * tcpconnecth expects it in host order so it can convert it back to network
     * order */
    port = ntohs(sent->s_port);
    printf("THE SERVICE IS AT PORT %d\n", port);

    fd = tcpconnecth("ibm1", port, 0);
    if (fd == -1) {
        perror("tcpconnecth");
        return -2;
    }

    rc = tcpwritemsg(fd, request, strlen(request), 0);
    if (rc != strlen(request)) {
        perror("tcpwritemsg");
        return -3;
    }

    while ((bytes_read = tcpreadline(fd, response, sizeof(response), 0)) > 0) {
        printf("%s", response);
    }

    if (bytes_read == -1) {
        perror("tcpreadline");
        return -4;
    }

    return 0;
}

#endif
