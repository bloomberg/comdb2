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

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/time.h>
#include <sys/types.h>
#include <poll.h>
#include <unistd.h>

#include "comdb2.h"
#include "machclass.h"
#include "logmsg.h"
#include <locks_wrap.h>

enum FASTSEEDPARAMS {
    MCHSHIFT = 18,
    MAXNODE = (1 << (32 - MCHSHIFT)) - 1 /* max node = 16383 */
    ,
    MAXDUP = (1 << MCHSHIFT) - 1 /* max dupe = 262143 */
};

static pthread_mutex_t fastseedlk = PTHREAD_MUTEX_INITIALIZER;
static int fastseed_last_epoch = 0;
static int fastseed_dup = 0;

static inline int fastseed_get_lastepoch(void) { return fastseed_last_epoch; }

static inline void fastseed_set_lastepoch(int epoch)
{
    fastseed_last_epoch = epoch;
}

static inline int fastseed_get_dup(void) { return fastseed_dup; }

static inline void fastseed_set_dup(int dup) { fastseed_dup = dup; }

extern int gbl_mynodeid;

uint64_t (*external_fastseed)(void) = NULL;

static void _track(int srcid);

/* TODO: fastseed - compatibility mode only - remove */
uint64_t comdb2fastseed(int srcid)
{
    int epoch, node, dup;
    int firstepoch = 0;
    int retries;
    int seed[2];
    uint64_t out;

    _track(srcid);

    if (external_fastseed)
        return external_fastseed();

    node = gbl_mynodeid;
    if (node < 1 || node > MAXNODE) {
        logmsg(LOGMSG_ERROR, 
                "err:fastseed:bad machine number %d, must be 1<=x<=%d\n", node,
                MAXNODE);
        seed[0] = seed[1] = 0;
        return -1;
    }

    retries = 0;
    do {
        Pthread_mutex_lock(&fastseedlk);
        epoch = comdb2_time_epoch();
        if (epoch == 0) /* uh oh.. something broken */
        {
            Pthread_mutex_unlock(&fastseedlk);
            logmsg(LOGMSG_ERROR, "err:fastseed:zero epoch! epoch can't be 0!\n");
            seed[0] = seed[1] = 0;
            return -1;
        }
        if (fastseed_get_lastepoch() != epoch) {
            /* this is different epoch, so we're good */
            dup = 0;
            fastseed_set_dup(0);
            fastseed_set_lastepoch(epoch);
            break; /* got one*/
        }
        dup = fastseed_get_dup() + 1;
        if (dup < MAXDUP) {
            /* requested more than once in this second, so increment dupe */
            fastseed_set_dup(dup);
            break;
        }
        Pthread_mutex_unlock(&fastseedlk);

        epoch = comdb2_time_epoch();
        if (retries == 0)
            firstepoch = epoch;

        if (retries > 8 && firstepoch == epoch) {
            logmsg(LOGMSG_ERROR, "fastseed():ERROR! EPOCH IS NOT UPDATING!!! "
                            "original %d now %d\n",
                    firstepoch, epoch);
            seed[0] = seed[1] = 0;
            return -99;
        }
        if (retries > 20) {
            logmsg(LOGMSG_ERROR, 
                "fastseed():ERROR! HIGH CONTENTION, CANNOT GET A SEED!\n");
            seed[0] = seed[1] = 0;
            return -98;
        }

        logmsg(LOGMSG_ERROR, "warn:fastseed():reached dupe limit, waiting for next "
                        "second, retries %d epoch %d.\n",
                retries, epoch);
        poll(NULL, 0, 250);
        ++retries;
    } while (1);

    /* if here, got 1, and lock is still held */

    Pthread_mutex_unlock(&fastseedlk);

    seed[0] = epoch;
    seed[1] = (node << MCHSHIFT) | (dup & MAXDUP);

    memcpy(&out, seed, 8);
    return flibc_ntohll(out);
}

/* hack alert: this is indexed by sources, which gets their own next available
 * int id good enough to track this code that is going away
 */
static int track[5];
const char *track_names[5] = {"osql", "remsql", "remtran", "seqno", "sqlfunc"};

static void _track(int srcid)
{
    if (srcid < 0 || srcid >= sizeof(track) / sizeof(track[0]))
        srcid = 0;
    track[srcid]++;
}

void report_fastseed_users(int lvl)
{
    int i;
    for (i = 0; i < sizeof(track) / sizeof(track[0]); i++) {
        if (track[i])
            logmsg(lvl, "COMDB2FASTSEED \"%s\" count %d\n", track_names[i],
                   track[i]);
    }
}
