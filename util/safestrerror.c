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

#include <safestrerror.h>

/* On Linux by default we seem to get the non-standard GNU version of
 * strerror_r which returns a char* rather than the POSIX standard version
 * which returns an int. */
#ifdef _LINUX_SOURCE
#ifdef _GNU_SOURCE
#undef _GNU_SOURCE
#endif
#ifdef _POSIX_C_SOURCE
#error "_POSIX_C_SOURCE is already defined!"
#endif
#define _POSIX_C_SOURCE 200112L
#endif

#include <errno.h>
#include <stdio.h>
#include <string.h>

char *safestrerror(int errnum, char *buf, size_t buflen)
{
    char *retbuf = buf;

    int pos = snprintf(buf, buflen, "%d ", errnum);

    if (pos >= buflen)
        return buf;

    buf += pos;
    buflen -= pos;

    int rc = strerror_r(errnum, buf, buflen);

    if (ERANGE == rc) {
        snprintf(buf, buflen, "!strerror_r ERANGE, buflen %zu", buflen);

    } else if (0 != rc && EINVAL != rc) {
        snprintf(buf, buflen, "!strerror_r %d", rc);
    }

    return retbuf;
}

#ifdef TEST_SAFESTRERROR

int main(int argc, char *argv[])
{
    char buf[STRERROR_BUFLEN];
    for (int errnum = 0; errnum < 200; errnum++) {
        printf("error %d: %s\n", errnum, strerror_m(errnum, buf));
    }

    printf("too small: %s\n", safestrerror(EINTR, buf, 10));
}

#endif /* TEST_SAFESTRERROR */
