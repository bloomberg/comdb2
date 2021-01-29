/*
   Copyright 2019 Bloomberg Finance L.P.

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

#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <errno.h>
#include "logmsg.h"
#include <epochlib.h>

static int64_t starttime;

void comdb2_time_init(void)
{
    int rc;
    struct timeval tv;
    rc = gettimeofday(&tv, NULL);
    if (rc) {
        logmsg(LOGMSG_FATAL, "gettimeofday rc %d %s\n", rc, strerror(errno));
        abort();
    }
    starttime = tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

int comdb2_time_epoch(void)
{
    return time(NULL);
}

int64_t comdb2_time_epochus(void)
{
    struct timeval tv;
    int rc;
    rc = gettimeofday(&tv, NULL);
    if (rc) {
        logmsg(LOGMSG_FATAL, "gettimeofday rc %d %s\n", rc, strerror(errno));
        abort();
    }
    return (((int64_t)tv.tv_sec) * 1000000 + tv.tv_usec);
}

int comdb2_time_epochms(void)
{
    struct timeval tv;
    int rc;
    rc = gettimeofday(&tv, NULL);
    if (rc) {
        logmsg(LOGMSG_FATAL, "gettimeofday rc %d %s\n", rc, strerror(errno));
        abort();
    }
    return (tv.tv_sec * 1000 + tv.tv_usec / 1000) - starttime;
}
