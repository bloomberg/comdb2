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

#include <lockassert.h>
#include <plink.h>
#include "plbitlib.h"

#include "comdb2.h"
#include "comdb2_shm.h"
#include "machclass.h"
#include "rtcpu.h"
#include "logmsg.h"

int comdb2_shm_clear_and_set_flags(int db, int shmflags)
{
    thedb->shmflags = shmflags;
    return 0;
}

int comdb2_shm_set_flag(int db, int flag)
{
    if (db == thedb->dbnum) {
        thedb->shmflags |= flag;
    } else {
        struct dbtable *sdb = getdbbynum(db);
        if (sdb == NULL) {
            logmsg(LOGMSG_ERROR, "no db %d in environment\n", db);
            return 1;
        }
        sdb->shmflags |= flag;
    }
    return 0;
}

int comdb2_shm_clr_flag(int db, int flag)
{
    if (db == thedb->dbnum) {
        thedb->shmflags &= (~flag);
    } else {
        struct dbtable *sdb = getdbbynum(db);
        if (sdb == NULL) {
            logmsg(LOGMSG_ERROR, "no db %d in environment\n", db);
            return 1;
        }
        sdb->shmflags &= (~flag);
    }
    return 0;
}

int comdb2_shm_get_flags(int db, int *flags)
{
    if (db == thedb->dbnum) {
        *flags = thedb->shmflags;
    } else {
        struct dbtable *sdb = getdbbynum(db);
        if (sdb == NULL) {
            logmsg(LOGMSG_ERROR, "no db %d in environment\n", db);
            return 1;
        }
        *flags = thedb->shmflags;
    }
    return 0;
}

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

/* TODO: fastseed - compatibility mode only - remove */
uint64_t comdb2fastseed(void)
{
    int epoch, node, dup;
    int firstepoch = 0;
    int retries;
    int seed[2];
    uint64_t out;

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
        assert_pthread_mutex_lock(&fastseedlk);
        epoch = time_epoch();
        if (epoch == 0) /* uh oh.. something broken */
        {
            assert_pthread_mutex_unlock(&fastseedlk);
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
        assert_pthread_mutex_unlock(&fastseedlk);

        epoch = time_epoch();
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

    assert_pthread_mutex_unlock(&fastseedlk);

    seed[0] = epoch;
    seed[1] = (node << MCHSHIFT) | (dup & MAXDUP);

    memcpy(&out, seed, 8);
    return flibc_ntohll(out);
}

extern char *___plink_constants[PLINK_____END];
const char *plink_constant(int which)
{
    if (which < 0 || which >= PLINK_____END)
        return NULL;
    return ___plink_constants[which];
}

int getlclbfpoolwidthbigsnd(void) { return 16 * 1024 - 1; }

char *machine(void) { return gbl_mynode; }
