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
#include "plhash.h"
#include "intern_strings.h"

pthread_mutex_t hlock = PTHREAD_MUTEX_INITIALIZER;
static hash_t *htimes = NULL;

typedef struct {
    const char *name;
    unsigned long long utime;
} name_time_pair_t;

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
    Pthread_mutex_lock(&hlock);
    if (!htimes) {
        htimes = hash_init_o(offsetof(name_time_pair_t, name), sizeof(const char *));
    }
    name_time_pair_t *ptr;
    const char *iptr = intern(name);
    if ((ptr = hash_find_readonly(htimes, &iptr)) == 0) {
        ptr = malloc(sizeof(name_time_pair_t));
        ptr->name = intern(name);
        ptr->utime = 0;
        hash_add(htimes, ptr);
    }
    ptr->utime += us;
    Pthread_mutex_unlock(&hlock);
}

void reset_time_accounting(const char *name)
{
    if (!htimes) {
        return;
    }
    Pthread_mutex_lock(&hlock);
    name_time_pair_t *ptr;
    const char *iptr = intern(name);
    if ((ptr = hash_find_readonly(htimes, &iptr)) != 0) {
        ptr->utime = 0;
    }
    Pthread_mutex_unlock(&hlock);
}
static int reset_time(void *obj, void *unused)
{
    name_time_pair_t *ptr = obj;
    ptr->utime = 0;
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
    Pthread_mutex_lock(&hlock);
    name_time_pair_t *ptr;
    const char *iptr = intern(name);
    if ((ptr = hash_find_readonly(htimes, &iptr)) != 0) {
        logmsg(LOGMSG_USER, "Timing information for %s: %lluus\n", ptr->name, ptr->utime);
    }
    Pthread_mutex_unlock(&hlock);
}

static int print_name_time_pair(void *obj, void *unused)
{
    name_time_pair_t *ptr = obj;
    logmsg(LOGMSG_USER, "name=%s time=%lluus\n", ptr->name, ptr->utime);
    return 0;

}

void print_all_time_accounting()
{
    if (!htimes) {
        return;
    }
    logmsg(LOGMSG_USER, "Timing information:\n");
    Pthread_mutex_lock(&hlock);
    hash_for(htimes, print_name_time_pair, NULL);
    Pthread_mutex_unlock(&hlock);
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
    Pthread_mutex_lock(&hlock);
    hash_for(htimes, free_name_time_pair, NULL);
    hash_clear(htimes);
    hash_free(htimes);
    htimes = 0;
    Pthread_mutex_unlock(&hlock);
}
