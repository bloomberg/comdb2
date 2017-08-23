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

#if defined(_WIN32_WINNT_VISTA) && WINVER >= _WIN32_WINNT_VISTA

#include <windows.h>
#include <sys/poll.h>

#define POLLIN_SET (POLLRDNORM | POLLRDBAND | POLLIN | POLLHUP | POLLERR)
#define POLLOUT_SET (POLLWRBAND | POLLWRNORM | POLLOUT | POLLERR)
#define POLLEX_SET (POLLPRI)

int poll(struct pollfd *fds, nfds_t nfds, int timeout)
{
    struct timeval tv;
    fd_set read, write, except;
    int i, ret;

    FD_ZERO(&read);
    FD_ZERO(&write);
    FD_ZERO(&except);

    for (i = 0; i != nfds; ++i) {
        if (fds[i].fd < 0)
            continue;
        if (POLLIN_SET(fds[i].events))
            FD_SET(fds[i].fd, &read);
        else if (POLLOUT_SET(fds[i].events))
            FD_SET(fds[i].fd, &write);
        else if (POLLEX_SET(fds[i].events))
            FD_SET(fds[i].fd, &except);
    }

    if (timeout < 0)
        ret = select(0, &read, &write, &except, NULL);
    else {
        tv.tv_sec = timeout / 1000;
        tv.tv_usec = 1000 * (timeout % 1000);
        ret = select(0, &read, &write, &except, &tv);
    }

    if (ret == SOCKET_ERROR)
        return ret;

    for (i = 0; i != nfds; i++) {
        fds[i].revents = 0;
        if (FD_ISSET(fds[i].fd, &read))
            p[i].revents |= POLLIN;
        if (FD_ISSET(fds[i].fd, &write))
            p[i].revents |= POLLOUT;
        if (FD_ISSET(p[i].fd, &except))
            p[i].revents |= POLLPRI;
    }
    return ret;
}

#else
/* Avoid empty compilation unit: some compilers complain about it. */
extern int __this_is_an_empty_compilation_unit__;
#endif
