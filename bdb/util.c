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

/* common utility code needed across modules in bdb */

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <strings.h>
#include <poll.h>
#include <alloca.h>
#include <limits.h>

#include <epochlib.h>
#include <build/db.h>

#include "net.h"
#include "bdb_int.h"
#include "locks.h"
#include "list.h"
#include <plbitlib.h> /* for bset/btst */

#include <memory_sync.h>
#include <logmsg.h>

int bdb_keycontainsgenid(bdb_state_type *bdb_state, int ixnum)
{
    return ((bdb_state->ixdups[ixnum]) ||
            ((!bdb_state->ixdups[ixnum] && bdb_state->ixnulls[ixnum])));
}

int bdb_maybe_use_genid_for_key(
                               bdb_state_type *bdb_state, DBT *p_dbt_key,
                               void *ixdta, int ixnum,
                               unsigned long long genid, int isnull,
                               void **ppKeyMaxBuf)
{
    int rc = 0;

    assert(ppKeyMaxBuf);

    /* set up the dbt_key */
    memset(p_dbt_key, 0, sizeof(DBT));

    /* indexes with dupes get a genid tacked on, same for indexes that
       dont allow dupes but allow for nulls.  the genid is the sanitized
       'search' genid.  */

    if (bdb_keycontainsgenid(bdb_state, ixnum)) {
        unsigned long long tmpgenid;

        tmpgenid = get_search_genid(bdb_state, genid);

        /* use 0 as the genid if no null values to keep it unique */
        if (bdb_state->ixnulls[ixnum] && !isnull)
            tmpgenid = 0;
        else
            rc = 1;

        assert(bdb_state->ixlen[ixnum] <= BDB_KEY_MAX);

        *ppKeyMaxBuf = malloc(BDB_KEY_MAX + sizeof(unsigned long long));
        memcpy(*ppKeyMaxBuf, ixdta, bdb_state->ixlen[ixnum]);

        p_dbt_key->data = *ppKeyMaxBuf;
        p_dbt_key->size = bdb_state->ixlen[ixnum];

        memcpy(*ppKeyMaxBuf + bdb_state->ixlen[ixnum], &tmpgenid,
               sizeof(unsigned long long));
        p_dbt_key->size += sizeof(unsigned long long);
    } else {
        /* in place if we dont have dups */
        *ppKeyMaxBuf = 0;

        p_dbt_key->data = ixdta;
        p_dbt_key->size = bdb_state->ixlen[ixnum];
    }

    return rc;
}

void timeval_to_timespec(struct timeval *tv, struct timespec *ts)
{
    ts->tv_sec = tv->tv_sec;
    ts->tv_nsec = tv->tv_usec * 1000;
}

#define MILLION 1000000
#define BILLION 1000000000

void add_millisecs_to_timespec(struct timespec *orig, int millisecs)
{
    int nanosecs = orig->tv_nsec;
    int secs = orig->tv_sec;

    secs += (millisecs / 1000);
    millisecs = (millisecs % 1000);

    nanosecs += (millisecs * MILLION);
    secs += (nanosecs / BILLION);
    nanosecs = (nanosecs % BILLION);
    orig->tv_sec = secs;
    orig->tv_nsec = nanosecs;
    return;
}

int setup_waittime(struct timespec *waittime, int waitms)
{
#ifndef HAS_CLOCK_GETTIME
    struct timeval tv;
#endif
    int rc;

#ifdef HAS_CLOCK_GETTIME
    rc = clock_gettime(CLOCK_REALTIME, waittime);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "clock_gettime err %d %s\n", errno, strerror(errno));
        return -1;
    }
#else
    rc = gettimeofday(&tv, NULL);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "gettimeofday failed\n");
        return -1;
    }

    timeval_to_timespec(&tv, waittime);
#endif

    add_millisecs_to_timespec(waittime, waitms);

    return 0;
}

/* Given a berkeley db lockid (i.e. some bytes of data), try to get
 * a human readable name for it.  This is based on __lock_printlock
 * in lock/lock_stat.c */
void bdb_lock_name(bdb_state_type *bdb_state, char *s, size_t slen,
                   void *lockid, size_t lockid_len)
{
    db_pgno_t pgno;
    u_int32_t *fidp;
    u_int32_t type;
    u_int8_t *ptr = lockid;
    char *namep;

    if (lockid_len == sizeof(struct __db_ilock)) {
        extern int __dbreg_get_name(DB_ENV * dbenv, u_int8_t * fid, char **);

        memcpy(&pgno, lockid, sizeof(db_pgno_t));
        fidp = (u_int32_t *)(ptr + sizeof(db_pgno_t));
        type = *(u_int32_t *)(ptr + sizeof(db_pgno_t) + DB_FILE_ID_LEN);

        if (__dbreg_get_name(bdb_state->dbenv, (u_int8_t *)fidp, &namep) != 0) {
            namep = NULL;
        }
        if (namep == NULL) {
            snprintf(s, slen, "(%lx %lx %lx %lx %lx) %-7s %7lu",
                     (u_long)fidp[0], (u_long)fidp[1], (u_long)fidp[2],
                     (u_long)fidp[3], (u_long)fidp[4],
                     type == DB_PAGE_LOCK ? "page" : type == DB_RECORD_LOCK
                                                         ? "record"
                                                         : "handle",
                     (u_long)pgno);
        } else {
            snprintf(s, slen, "%-25s %-7s %7lu", namep,
                     type == DB_PAGE_LOCK ? "page" : type == DB_RECORD_LOCK
                                                         ? "record"
                                                         : "handle",
                     (u_long)pgno);
        }

    } else {
        const unsigned char *cptr = lockid;
        snprintf(s, slen, "lockid_leen=%u", (unsigned)lockid_len);
    }
}

int bdb_write_preamble(bdb_state_type *bdb_state, int *bdberr)
{
    bdb_state_type *parent;

    *bdberr = BDBERR_NOERROR;

    if (bdb_state->parent)
        parent = bdb_state->parent;
    else
        parent = bdb_state;

    if (!bdb_state->read_write) {
        *bdberr = BDBERR_READONLY;
        return 1;
    }

    return 0;
}
