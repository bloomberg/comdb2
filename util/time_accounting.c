/*
   Copyright 2019 Bloomberg Finance L.P.

   Licensed under the Apache License, Version 2.0 (the "License");
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */


#include <pthread.h>
#include <sys/time.h>
#include "locks_wrap.h"
#ifdef TIMING_ACCOUNTING

#include "plhash.h"
#include "intern_strings.h"

pthread_mutex_t hlock = PTHREAD_MUTEX_INITIALIZER;
static hash_t *htimes = NULL;
unsigned long long totaltime;
unsigned long long totalcount;

const char *CHR_IXADDK = "CHR_IXADDK";
const char *CHR_DATADD = "CHR_DATADD";
const char *CHR_TMPSVOP = "CHR_TMPSVOP";


typedef struct {
    const char *name;
    unsigned long long utime;
    unsigned long long worstutime;
    unsigned long long count;
} name_time_pair_t;

__thread int already_timing;

// return timediff in microseconds (us) from tv passed in
int chrono_stop(struct timeval *tv)
{
    struct timeval tmp;
    gettimeofday(&tmp, NULL);
    int sec_part = (tmp.tv_sec - tv->tv_sec)*1000000;
    int usec_part = (tmp.tv_usec - tv->tv_usec);

    return sec_part + usec_part;
}

// add time accounting to appropriate slot
void accumulate_time(const char *name, int us)
{
    //printf("%s adding %s\n", __func__, name);
    const char *iptr = intern(name);
    pthread_mutex_lock(&hlock);
    if (!htimes) {
        htimes = hash_init_o(offsetof(name_time_pair_t, name), sizeof(const char *));
    }
    name_time_pair_t *ptr;
    if ((ptr = hash_find_readonly(htimes, &iptr)) == 0) {
        ptr = malloc(sizeof(name_time_pair_t));
        ptr->name = iptr;
        ptr->utime = 0;
        ptr->worstutime = 0;
        ptr->count = 0;
        hash_add(htimes, ptr);
    }
    ptr->utime += us;
    ptr->count++;
    if (ptr->worstutime < us) ptr->worstutime = us;
    pthread_mutex_unlock(&hlock);
}

void reset_time_accounting(const char *name)
{
    if (!htimes) {
        return;
    }
    const char *iptr = intern(name);
    pthread_mutex_lock(&hlock);
    name_time_pair_t *ptr;
    if ((ptr = hash_find_readonly(htimes, &iptr)) != 0) {
        ptr->utime = 0;
        ptr->worstutime = 0;
        ptr->count = 0;
    }
    pthread_mutex_unlock(&hlock);
}
static int reset_time(void *obj, void *unused)
{
    name_time_pair_t *ptr = obj;
    ptr->utime = 0;
    ptr->worstutime = 0;
    ptr->count = 0;
    return 0;

}
void reset_all_time_accounting()
{
    if (!htimes) {
        return;
    }
    hash_for(htimes, reset_time, NULL);
}


void print_time_accounting(const char *name)
{
    if (!htimes) {
        return;
    }
    pthread_mutex_lock(&hlock);
    name_time_pair_t *ptr;
    const char *iptr = intern(name);
    if ((ptr = hash_find_readonly(htimes, &iptr)) != 0) {
        logmsg(LOGMSG_USER, "Timing information for %s: %lluus\n", ptr->name, ptr->utime);
    }
    pthread_mutex_unlock(&hlock);
}

static int print_name_time_pair(void *obj, void *unused)
{
    name_time_pair_t *ptr = obj;
    logmsg(LOGMSG_USER, "name=%s time=%lluus count=%llu worst=%lluus, avg=%lfus\n", ptr->name, ptr->utime, ptr->count, ptr->worstutime, (double)ptr->utime/ptr->count);
    totaltime += ptr->utime;
    totalcount += ptr->count;
    return 0;

}

void print_all_time_accounting()
{
    if (!htimes) {
        logmsg(LOGMSG_USER, "No timing information is available\n");
        return;
    }
    totaltime = 0;
    totalcount = 0;
    logmsg(LOGMSG_USER, "Timing information:\n");
    pthread_mutex_lock(&hlock);
    hash_for(htimes, print_name_time_pair, NULL);
    pthread_mutex_unlock(&hlock);
    logmsg(LOGMSG_USER, "totaltime %llu, totalcount %llu\n", totaltime, totalcount);
}

static int free_name_time_pair(void *obj, void *unused)
{
    free(obj);
    return 0;
}

void cleanup_time_accounting()
{
    if (!htimes) {
        return;
    }
    pthread_mutex_lock(&hlock);
    hash_for(htimes, free_name_time_pair, NULL);
    hash_clear(htimes);
    hash_free(htimes);
    htimes = NULL;
    pthread_mutex_unlock(&hlock);
}

#else

#ifndef NDEBUG


#include "comdb2_atomic.h"
#include "time_accounting.h"

const char *CHR_NAMES[] = {"ix_addk", "dat_add", "temp_table_saveop"};

unsigned long long gbl_chron_times[CHR_MAX];

// add time accounting to appropriate slot
void accumulate_time(int el, int us)
{
    ATOMIC_ADD(gbl_chron_times[el], us);
}

void reset_time_accounting(int el)
{
    XCHANGE(gbl_chron_times[el], 0);
}

void print_time_accounting(int el)
{
    logmsg(LOGMSG_USER, "Timing information for %s: %lluus\n", CHR_NAMES[el],
           gbl_chron_times[el]);
}

void print_all_time_accounting()
{
    logmsg(LOGMSG_USER, "Timing information:\n");
    for (int i = 0; i < CHR_MAX; i++) {
        logmsg(LOGMSG_USER, "%s: %lluus\n", CHR_NAMES[i], gbl_chron_times[i]);
    }
}

#endif

#endif
