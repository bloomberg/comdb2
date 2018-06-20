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

#include <bbhrtime.h>

#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#ifdef _AIX
#include <sys/systemcfg.h>
#endif

#include <bb_stdint.h>
#include <logmsg.h>

#define ONEBILLION 1000000000ULL
#define ONENS_US 1E-3L
#define ONENS_MS 1E-6L
#define ONENS_S 1E-9L

int getbbhrtime(bbhrtime_t *t)
{
#if defined(__sun) || defined(_HP_SOURCE)
    *t = gethrtime();
    return 0;
#elif defined(_AIX)
    return read_wall_time(t, TIMEBASE_SZ);
#elif defined(_LINUX_SOURCE)
    return clock_gettime(CLOCK_MONOTONIC, t);
#else
    logmsg(LOGMSG_ERROR, "getbbhrtime: Not defined for arch.\n");
    return -1;
#endif
}

int getbbhrvtime(bbhrtime_t *t)
{
#if defined(__sun)
    *t = gethrvtime();
    return 0;
#elif defined(_AIX) || defined(_HP_SOURCE) || defined(_LINUX_SOURCE)
    return getbbhrtime(t);
#else
    #error "getbbhrvtime: not defined for platform"
#endif
}

bbint64_t diff_bbhrtime(bbhrtime_t *end, bbhrtime_t *start)
{
    int64_t endns = bbhrtimens(end);
    int64_t startns = bbhrtimens(start);
    if (endns == -1 || startns == -1)
        return -1;
    return endns - startns;
}

int64_t bbhrtimens(const bbhrtime_t *t_)
{
#if defined(__sun) || defined(_HP_SOURCE)
    return *t_;
#elif defined(_AIX)
    int64_t secs, nsecs;
    bbhrtime_t t;
    memcpy(&t, t_, TIMEBASE_SZ);
    time_base_to_time(&t, TIMEBASE_SZ);

    secs = t.tb_high;
    nsecs = t.tb_low;
    if (nsecs < 0) {
        --secs;
        nsecs += ONEBILLION;
    }
    return ONEBILLION * secs + nsecs;
#elif defined(_LINUX_SOURCE)
    return ONEBILLION * t_->tv_sec + t_->tv_nsec;
#else
    logmsg(LOGMSG_ERROR, "%s: Not defined for arch.\n", __func__);
    return -1;
#endif
}

double bbhrtimeus(const bbhrtime_t *t_)
{
    int64_t t = bbhrtimens(t_);
    if (t == -1)
        return -1.0L;
    return t * ONENS_US;
}

double bbhrtimems(const bbhrtime_t *t_)
{
    int64_t t = bbhrtimens(t_);
    if (t == -1)
        return -1.0L;
    return t * ONENS_MS;
}

double bbhrtime(const bbhrtime_t *t_)
{
    int64_t t = bbhrtimens(t_);
    if (t == -1)
        return -1.0L;
    return t * ONENS_S;
}

int getbbtime(bbtime_t *t) { return gettimeofday(t, NULL); }

bbint64_t diff_bbtime(bbtime_t *end, bbtime_t *start)
{
    return (((end->tv_sec - start->tv_sec) * 1000000LL) +
            (end->tv_usec - start->tv_usec));
}

#ifdef BBHRTIME_TESTDRIVER

static int test_bbhrtime()
{
    bbhrtime_t start, end;

    getbbhrtime(&start);
    sleep(2);
    getbbhrtime(&end);

    printf("sleep(2) called at t=%lfs=%lfms=%lfus=%lldns\n", bbhrtime(&start),
           bbhrtimems(&start), bbhrtimeus(&start), bbhrtimens(&start));
    printf("sleep(2) returned at t=%lfs=%lfms=%lfus=%lldns\n", bbhrtime(&end),
           bbhrtimems(&end), bbhrtimeus(&end), bbhrtimens(&end));
    printf("%lld nsec elapsed\n", diff_bbhrtime(&end, &start));
    return 0;
}

static int test_bbtime()
{
    bbtime_t start, end;

    getbbtime(&start);
    sleep(2);
    getbbtime(&end);
    printf("%lld usec elapsed\n", diff_bbtime(&end, &start));
    return 0;
}

int main(int argc, char **argv)
{
    test_bbhrtime();
    test_bbtime();
    return 0;
}

#endif
