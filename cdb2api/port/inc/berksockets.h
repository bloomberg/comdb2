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

#ifndef _INCLUDED_PORT_OS_H_
#error "Use #include <os.h> instead."
#endif

#ifndef _INCLUDED_PORT_BERKSOCKETS_H_
#define _INCLUDED_PORT_BERKSOCKETS_H_

typedef int SOCKET;
#define SOCKFMTTYPE "%d"

#define INVALID_SOCKET -1
#define SOCKET_ERROR -1
#define INADDR_NONE ((in_addr_t)-1)

#include <fcntl.h>
#define fcntlnonblocking(s, flag)                                              \
    (((flags = fcntl(s, F_GETFL, 0)) < 0)                                      \
         ? SOCKET_ERROR                                                        \
         : ((fcntl(s, F_SETFL, ((int)flags) | O_NONBLOCK) < 0) ? SOCKET_ERROR  \
                                                               : 0))

#define fcntlblocking(s, flag) fcntl(s, F_SETFL, (int)flags)

#include <netdb.h>
#define cdb2_gethostbyname(hp, nm)                                             \
    do {                                                                       \
        hp = gethostbyname(nm);                                                \
    } while (0)

#include <errno.h>
/* To be consistent with Windows WSASetLastError(). */
#define seterrno(err)                                                          \
    do {                                                                       \
        errno = err;                                                           \
    } while (0)
#endif
