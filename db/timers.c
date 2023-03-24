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

#include <pthread.h>
#include <sys/time.h>
#include <errno.h>
#include <timers.h>
#include "epochlib.h"
#include "logmsg.h"
#include "locks_wrap.h"
#include "thrman.h"
#include "thread_util.h"

extern int db_is_exiting();

/* timer traps */
static pthread_mutex_t timerlk = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t timerwait = PTHREAD_COND_INITIALIZER;
struct timer {
    int next;
    int ms;
    int parm;
    int oneshot;
};
#define MAXTIMERS 32
static int ntimers = 0;
static struct timer timers[MAXTIMERS];

static void (*timer_func)(struct timer_parm *) = NULL;

void timer_init(void (*func)(struct timer_parm *))
{
    if (func != NULL)
        timer_func = func;
    comdb2_time_init();
}


#define left(n) (((n + 1) * 2) - 1)
#define right(n) (((n + 1) * 2))
#define parent(n) ((n + 1) / 2 - 1)

static void fixup(int timer)
{
    struct timer t;
    while (timer) {
        if (timers[timer].next < timers[parent(timer)].next) {
            t = timers[timer];
            timers[timer] = timers[parent(timer)];
            timers[parent(timer)] = t;
            timer = parent(timer);
        } else
            break;
    }
}

static void fixdown(int timer)
{
    for (;;) {
        int n = -1;
        if (left(timer) < ntimers && right(timer) < ntimers) {
            int smaller;
            if (timers[left(timer)].next < timers[right(timer)].next)
                smaller = left(timer);
            else
                smaller = right(timer);
            if (timers[smaller].next < timers[timer].next)
                n = smaller;
        } else if (left(timer) < ntimers) {
            n = left(timer);
        } else
            break;

        if (n == -1)
            break;

        if (timers[n].next < timers[timer].next) {
            struct timer t;
            t = timers[n];
            timers[n] = timers[timer];
            timers[timer] = t;
            timer = n;
        } else
            break;
    }
}

static int new_timer(int ms, int parm, int oneshot, int dolock)
{
    struct timer t;

    if (dolock)
        Pthread_mutex_lock(&timerlk);
    if (ntimers == MAXTIMERS) {
        if (dolock)
            Pthread_mutex_unlock(&timerlk);
        return -1;
    }

    t.next = comdb2_time_epochms() + ms;
    t.ms = ms;
    t.parm = parm;
    t.oneshot = oneshot;

    timers[ntimers] = t;
    fixup(ntimers);
    ntimers++;

    Pthread_cond_signal(&timerwait);
    if (dolock)
        Pthread_mutex_unlock(&timerlk);
    return 0;
}

int comdb2_timprm(int ms, int parm)
{
    return new_timer(ms, parm, 0, 1);
}

int remove_timer(int parm, int dolock)
{
    if (dolock)
        Pthread_mutex_lock(&timerlk);
    for (int i = 0; i < ntimers; i++) {
        if (timers[i].parm == parm) {
            if (i != ntimers - 1) {
                timers[i] = timers[ntimers - 1];
                fixdown(i);
            }
            ntimers--;
            if (dolock) {
                Pthread_cond_signal(&timerwait);
                Pthread_mutex_unlock(&timerlk);
            }
            return 0;
        }
    }
    if (dolock)
        Pthread_mutex_unlock(&timerlk);
    return -1;
}

// send signal on db exit to allow timer_thread() to exit
void comdb2_signal_timer()
{
    Pthread_cond_signal(&timerwait);
}

int comdb2_cantim(int parm)
{
    return remove_timer(parm, 1);
}

int comdb2_timer(int ms, int parm)
{
    return new_timer(ms, parm, 1, 1);
}

void *timer_thread(void *p)
{
    comdb2_name_thread(__func__);
    int tnow;
    struct timer t;
    struct timer_parm waitft_parm;
    int rc;

    thrman_register(THRTYPE_GENERIC);
    thread_started("timer_thread");

    while (!db_is_exiting()) {
        tnow = comdb2_time_epochms();
        Pthread_mutex_lock(&timerlk);
        while (ntimers == 0)
            Pthread_cond_wait(&timerwait, &timerlk);
        t = timers[0];
        tnow = comdb2_time_epochms();
        if (t.next > tnow) {
            int nexttrap;
            struct timespec ts, now;

            nexttrap = t.next - tnow;

            rc = clock_gettime(CLOCK_REALTIME, &now);
            if (rc) {
                logmsg(LOGMSG_ERROR, "clock_gettime rc %d %s\n", rc,
                       strerror(errno));
            }

            ts = now;
            ts.tv_sec += nexttrap / 1000;
            ts.tv_nsec += ((nexttrap % 1000) * 1000);
            if (ts.tv_nsec > 1000000000) {
                ts.tv_sec += ts.tv_nsec / 1000000000;
                ts.tv_nsec = ts.tv_nsec % 1000000000;
            }

            pthread_cond_timedwait(&timerwait, &timerlk, &ts);
        } else {
            waitft_parm.parm = timers[0].parm;
            waitft_parm.epoch = comdb2_time_epoch();
            waitft_parm.epochms = tnow;

            timer_func(&waitft_parm);
            remove_timer(waitft_parm.parm, 0);
            if (!timers[0].oneshot) {
                new_timer(timers[0].ms, waitft_parm.parm, 0, 0);
            }
        }
        Pthread_mutex_unlock(&timerlk);
    }
    return NULL;
}
