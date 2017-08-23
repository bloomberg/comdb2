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

#ifndef _INCLUDED_PORT_LINUX_H_
#define _INCLUDED_PORT_LINUX_H_

#include "posix.h"

#undef cdb2_gethostbyname
#define cdb2_gethostbyname(hp, nm)                                             \
    do {                                                                       \
        struct hostent hostbuf;                                                \
        char tmp[8192];                                                        \
        int tmplen = 8192;                                                     \
        int herr;                                                              \
        gethostbyname_r(nm, &hostbuf, tmp, tmplen, &hp, &herr);                \
    } while (0)

#include <endian.h>
#if __BYTE_ORDER == __LITTLE_ENDIAN
#define __LITTLE_ENDIAN__ 1
#else
#define __LITTLE_ENDIAN__ 0
#endif

#define HAVE_MSGHDR_MSG_CONTROL

#endif
