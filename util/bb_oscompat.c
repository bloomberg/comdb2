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

#include <stdio.h>
#include <stddef.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <limits.h>
#include <stdlib.h>

#include <bb_oscompat.h>
#include <logmsg.h>
#include <mem_util.h>
#include <mem_override.h>

static int os_gethostbyname(char **name_ptr, struct in_addr *addr)
{
    const char *name = *name_ptr;
    struct addrinfo *res = NULL, hints = {0};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    if (getaddrinfo(name, NULL, &hints, &res) != 0 || res == NULL) {
        return -1;
    }
    if (addr) {
        *addr = ((struct sockaddr_in*)res->ai_addr)->sin_addr;
    }
    freeaddrinfo(res);
    return 0;
}

static hostbyname *hostbyname_impl = os_gethostbyname;

int comdb2_gethostbyname(char **name, struct in_addr *addr)
{
    return hostbyname_impl(name, addr);
}

hostbyname *get_os_hostbyname(void)
{
    return os_gethostbyname;
}

void set_hostbyname(hostbyname *impl)
{
    hostbyname_impl = impl;
}

#ifdef _IBM_SOURCE
#include <pthread.h>
#include <locks_wrap.h>
static pthread_mutex_t servbyname_lk = PTHREAD_MUTEX_INITIALIZER;
#endif

void comdb2_getservbyname(const char *name, const char *proto, short *port)
{
    struct servent result_buf, *result = NULL;
    char buf[1024];
#   if defined(__APPLE__) // Should be first, as _LINUX_SOURCE is also defined.
    result = getservbyname(name, proto);
#   elif defined(_LINUX_SOURCE)
    getservbyname_r(name, proto, &result_buf, buf, sizeof(buf), &result);
#   elif defined(_SUN_SOURCE)
    result = getservbyname_r(name, proto, &result_buf, buf, sizeof(buf));
#   elif defined(_IBM_SOURCE)
    Pthread_mutex_lock(&servbyname_lk);
    result = getservbyname(name, proto);
    if (result) {
        result_buf = *result;
        result = &result_buf;
    }
    Pthread_mutex_unlock(&servbyname_lk);
#   endif
    if (result) {
        *port = result->s_port;
    }
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

char *comdb2_realpath(const char *path, char *resolved_path)
{
    char *rv, *rpath;
    rpath = resolved_path;
    if (rpath == NULL) {
        rpath = malloc(PATH_MAX + 1);
        if (rpath == NULL) {
            logmsgperror("malloc");
            return NULL;
        }
    }
    rv = realpath(path, rpath);
    if (rv == NULL && resolved_path == NULL)
        free(rpath);
    return rv;
}
