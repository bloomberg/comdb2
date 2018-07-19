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

#include <stddef.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

#include <bb_oscompat.h>

int comdb2_gethostbyname(const char *name, struct in_addr *addr)
{
    int rc;
    struct addrinfo *res, hints = {0};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    if ((rc = getaddrinfo(name, NULL, &hints, &res)) != 0) {
        return rc;
    }
    struct addrinfo *n, *p;
    n = p = res->ai_next;
    while (p) {
        p = p->ai_next;
        freeaddrinfo(n);
        n = p;
    }
    if (addr) {
        *addr = ((struct sockaddr_in*)res->ai_addr)->sin_addr;
    }
    freeaddrinfo(res);
    return 0;
}

int comdb2_getservbyname(const char *name, const char *proto, short *port)
{
    struct servent result_buf, *result;
    char buf[1024];
    int rc;
    rc = getservbyname_r(name, proto, &result_buf, buf, sizeof(buf), &result);
    if (rc == 0 && result) {
        *port = result->s_port;
    }
    return rc;
}

int bb_readdir(DIR *d, void *buf, struct dirent **dent) {
#ifdef _LINUX_SOURCE
    struct dirent *rv;
    *dent = rv = readdir(d);
    if (rv == NULL)
        return errno;
    /* rv->d_reclen is the actual size of rv.
       It may not match sizeof(struct dirent). */
    memcpy(buf, rv, rv->d_reclen);
    return 0;
#else
    int rc;
    return readdir_r(d, buf, dent);
#endif
}
