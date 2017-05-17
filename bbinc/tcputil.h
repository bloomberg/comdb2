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

#ifndef INCLUDED_TCPUTIL
#define INCLUDED_TCPUTIL

/* The module formerly known as "tcplib".  This provides a bunch of useful
 * utilies for making tcp connections and reading/writing messages. */

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdio.h>
#include <sys/socket.h>
#include <sys/types.h>

#if defined __cplusplus
extern "C" {
#endif

/* OPEN LISTEN SOCKET ON PORT*/
extern int tcplisten(int port);

/* ACCEPT CONNECTIONS */
extern int tcpaccept(int listen_fd, struct sockaddr_in *client_addr);

/*PORT IS SET ONLY If SPECIFIED IN *HOST, ELSE LEFT UNCHANGED*/
extern int tcpresolve(const char *host, struct in_addr *in, int *port);

/*OPEN CONNECTION TO in:port OPTIONAL BIND TO myport*/
extern int tcpconnect(struct in_addr in, int port, int myport);

/*OPEN CONNECTION TO in:port OPTIONAL BIND TO myport
 *OPTIONAL setsockopt IP_TOS to tos parameter if not -1*/
extern int tcpconnect_tos(struct in_addr in, int port, int myport, char tos);

/*OPEN CONNECTION TO in:port OPTIONAL BIND TO myport OPTIONAL connect timeout*/
extern int tcpconnect_to(struct in_addr in, int port, int myport, int timeout);

/*OPEN CONNECTION TO in:port OPTIONAL BIND TO myport OPTIONAL connect timeout
 *OPTIONAL setsockopt IP_TOS to tos parameter if not -1*/
extern int tcpconnect_to_tos(struct in_addr in, int port, int myport,
                             int timeoutms, char tos);

/* Non-blocking version of tcpconnect.  Socket will be left in a non-blocking
 * state */
extern int tcpconnect_nb(struct in_addr in, int port, int myport);

/*OPEN CONNECTION TO "inet:port" OPTIONAL BIND TO myport*/
extern int tcpconnecth(const char *host, int port, int myport);

/*OPEN CONNECTION TO "inet:port" OPTIONAL BIND TO myport OPTIONAL connect
 * timeout*/
extern int tcpconnecth_to(const char *host, int port, int myport,
                          int timeoutms);

/*THIS DUPS SD AND RETURNS INPUT/OUTPUT FILE DESCRIPTOR
  RETURNS OUTPUT SD or -1  & closed file if failed */
extern int tcpfdopen(int insd, FILE **ainfil, char *inbuf, int linbuf,
                     int ibtyp, FILE **aoutfil, char *outbuf, int loutbuf,
                     int obtyp);

/* READ/WRITE WITH OPTIONAL TIMEOUT. RETURNS 0 IF TIMED OUT.
 * These just call read()/write() if timeoutms is 0.
 * If timeoutms>0 then these first call poll().
 * if poll()/read()/write() fail then these preserve the value of errno for
 * the caller to inspect.  The return value is the number of bytes read/written,
 * or -1 on error. */
extern int tcpread(int fd, void *cc, int len, int timeoutms);
extern int tcpwrite(int fd, const void *cc, int len, int timeoutms);
/* READ/WRITE WHOLE MESSAGE. NO PARTIALS.
 * Note: for non-blocking sockets, these functions should not be used
 * except with a timeout value.
 * These return the number of bytes read/written *if* the entire message was
 * read/written.  On error -1 is returned and the caller can get the Unix error
 * code from errno.
 *
 * tcpreadmsg() will return 0 on EOF or timeout - the caller cannot distinguish
 * the two cases.
 *
 * tcpwritemsg()/tcpwritemsgv() will return -2 on timeout and print trace.
 * For some reason, these functions will print "write 0 fd #" five times before
 * exiting with a timeout error, and will actually observe a timeout 5 times
 * longer than that provided.
 *
 * tcpwritemsgv() allows a maximum of 12 io vectors, and will return -7777 or
 * -7778 on invalid input.  IO vectors that provide no bytes to write are
 * considered invalid input.
 *
 * These functions will restart on EINTR.
 *
 * Note that, aside from the strange behaviour with tcpwritemsg() and
 * tcpwritemsgv() wrt timeouts, these functions only loosely observe the timrout
 * provided because each new i/o operation restarts the timer.
 */
extern int tcpreadmsg(int fd, void *cc, int len, int timeoutms);
extern int tcpwritemsg(int fd, const void *cc, int len, int timeoutms);
extern int tcpwritemsgv(int fd, int niov, const struct iovec *iniov,
                        int timeoutms);

extern int tcpreadline(int fd, void *vptr, size_t maxlen, int timeoutms);

#if defined __cplusplus
}
#endif

#endif
