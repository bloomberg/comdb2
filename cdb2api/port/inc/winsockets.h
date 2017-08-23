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

#if !defined(_INCLUDED_PORT_OS_H_) &&                                          \
    !defined(_INCLUDED_PORT_WIN32_SYS_SOCKET_H_)
#error "Use #include <os.h> or system headers instead."
#endif

#ifndef _INCLUDED_PORT_WINSOCKETS_H_
#define _INCLUDED_PORT_WINSOCKETS_H_

#define _WINSOCK_DEPRECATED_NO_WARNINGS
#include <winsock2.h>
#include <ws2tcpip.h>
#include <inttypes.h>

#define SOCKFMTTYPE "%" PRIdPTR

typedef unsigned long int in_addr_t;

#include <io.h>
#define close(s) closesocket(s)
#define write(s, b, l) send(s, b, l, 0)
#define read(s, b, l) recv(s, b, l, 0)

#define fcntlnonblocking(s, flag) (flag = 1, ioctlsocket(s, FIONBIO, &flag))
#define fcntlblocking(s, flag) (flag = 0, ioctlsocket(s, FIONBIO, &flag))

/* Error codes set by Windows Sockets are
   not made available through the errno variable.
   Use our own. */

#include <errno.h>
#undef errno
#define errno WSAGetLastError()

/* Please forgive the ugliness: WSAGetLastError() is not a valid l-value
   thus it is impossible to do something like `errno = EINVAL'. */
#define seterrno(err) WSASetLastError(err)

#include <string.h>
char *WSAStrError(int err);
#undef strerror
#define strerror(err) WSAStrError(err)

/* Map WinSock error codes to Berkeley errors */
#undef EINPROGRESS
#define EINPROGRESS WSAEWOULDBLOCK
#undef EAGAIN
#define EAGAIN WSAEWOULDBLOCK
#undef ECONNRESET
#define ECONNRESET WSAECONNRESET
#undef EINTR
#define EINTR WSAEINPROGRESS
#undef EIO
#define EIO WSAECONNABORTED
#undef EACCES
#define EACCES WSAEACCES

#endif
