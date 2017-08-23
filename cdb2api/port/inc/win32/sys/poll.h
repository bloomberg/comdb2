/*
   Copyright 2017, Bloomberg Finance L.P.

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

#ifndef _INCLUDED_PORT_WIN32_SYS_POLL_H_
#define _INCLUDED_PORT_WIN32_SYS_POLL_H_

#include <sys/socket.h>

#if defined(_WIN32_WINNT_VISTA) && WINVER >= _WIN32_WINNT_VISTA
#define poll(fds, nfds, tm) WSAPoll(fds, nfds, tm)
#else /* Windows XP or below: use our own. */
#define POLLIN 0x0001
#define POLLPRI 0x0002
#define POLLOUT 0x0004
#define POLLERR 0x0008
#define POLLHUP 0x0010
#define POLLRDNORM 0x0020
#define POLLRDBAND 0x0040
#define POLLWRNORM 0x0080
#define POLLWRBAND 0x0100

struct pollfd {
    int fd;        /* file descriptor */
    short events;  /* requested events */
    short revents; /* returned events */
};

typedef unsigned long int nfds_t;

int poll(struct pollfd *fds, nfds_t nfds, int timeout);

#endif

#endif
