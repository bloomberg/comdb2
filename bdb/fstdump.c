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

/* It's a bit late now, but probably all the IO should be in the main
 * database, with just the fast iteration in here. */

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <sys/uio.h>
#include <sys/socket.h>
#include <sys/socketvar.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>
#include <errno.h>
#include <limits.h>
#include <stdarg.h>
#include <sys/poll.h>
#include <sys/select.h>

#include <tcputil.h>

#include <epochlib.h>
#include <build/db.h>
#include "debug_switches.h"

#include <ctrace.h>
#include <sys_wrap.h>

#include <net.h>
#include "bdb_int.h"
#include "locks.h"

#include <str0.h>

#include <sbuf2.h>
#include "logmsg.h"
#include "thread_stats.h"

struct error_extension {
    uint32_t length; /* length of this struct in bytes */
    int32_t bdberr;
    uint32_t msglen;
    /* Error string follows length bytes after start of struct.  String is
     * exactly msglen bytes - no \0 terminator */
};

/* fstdump thread coordination structure */
typedef struct {
    bdb_state_type *bdb_state;
    bdb_state_type *bdb_parent_state;
    pthread_mutex_t lock; /* must lock on this before writing data to fd */
    int fd;               /* fd to write to */
    int bdberr;           /* check/set this under lock.  indicates a thread
                             hit an error so all threads should stop. */
    char errmsg[128];     /* also check under lock - gives error to return to
                             client. */
    size_t sendrecsz;     /* record size that we will send */
    bdb_fstdumpdta_callback_t convert_callback;
    int callback_flags; /* Used for linux-clients. */
    void *userptr;
    void *userptr2;
    int timeoutms;
    unsigned long long count; /* how many records dumped in total, gets updated
                                 under lock. */
    int epoch_start;          /* when this dump started */

    int close_cursor; /* whether to close the cursor in between socket sends.
                         Defaults to 1. If off, performance will be slightly
                         higher,
                         but the socket has a short timeout (5s) due to page
                         locking
                         by bdb. So if you turn this off, make sure the dumping
                         application
                         doesn't idle on the socket for too long. */

    const char *tzname; /* pointer to constant tzname store on the stack of
                           dumping thread
                           creating thread, hah */
} fstdump_t;

typedef struct {
    fstdump_t *common;
    DB *dbp; /* what db to dump */
    pthread_t tid;
    int get_genids;
    unsigned long long count; /* how many records dumped in this thread */
    int real_thread;          /* 1 if this really is a thread, in which case
                                 it needs to call bdb_thread_event() */
} fstdump_per_thread_t;

struct fstdump_thread2_arg {
    pthread_mutex_t mutex;
    fstdump_per_thread_t perthread[MAXDTAFILES];
    int num_stripes;
    int num_done;
};

static int write_records(fstdump_per_thread_t *fstdump, DBT *data,
                         void *sendrec, unsigned char **retkey_p);

static void *fstdump_thread_inner(fstdump_per_thread_t *fstdump, void *sendrec,
                                  void *databuf, size_t buffer_length);

static int bdb_fstdumpdta_sendsz_int(bdb_state_type *bdb_state, SBUF2 *sb,
                                     size_t sendrecsz,
                                     bdb_fstdumpdta_callback_t convert_callback,
                                     int callback_flags, void *userptr,
                                     void *userptr2, int timeoutms,
                                     int safe_mode, int *bdberr,
                                     const char *tzname, int get_genids);

static int close_retry(DBC *dbcp, fstdump_t *common);

static int get_retry(DBC *dbcp, fstdump_t *common, DBT *key, DBT *data,
                     u_int32_t flags);

static int open_retry(DBC **dbcp, fstdump_per_thread_t *fstdump,
                      fstdump_t *common);

/* from comdb2.c - current value is 500 */

extern int gbl_maxretries;

/* in fstdump,
   after this many retries resulting in deadlocks,
   start sleeping between calls to c_get and c_open */
static int deadlock_sleep_start = 10;

/* as above -
   sleep this long between calls to c_get and c_open
(ms) */
static int deadlock_sleep_amt = 100 * 1000;

/* Hijacked from tcplib.c */
static int tcpwritev(int fd, int niov, struct iovec *iniov, int timeoutms)
{
    /*returns 0 if timed out*/
    int rc;
    struct pollfd pol;
    if (timeoutms > 0) {
        pol.fd = fd;
        pol.events = POLLOUT;
        rc = poll(&pol, 1, timeoutms);
        if (rc <= 0)
            return rc; /*timed out or error*/
        if ((pol.revents & POLLOUT) == 0)
            return -1;
        /*can write*/
    }
    return writev(fd, iniov, niov);
}

/* TODO does this conflict with tcplib? We at least include tcputil.h so this
 * definition here seems suspect */
static int stcpwritemsgv(int fd, int niov, struct iovec *iniov, int timeoutms)
{
    int curiov, curoff, left, sent, tot;
    char *base;
    struct iovec iov[12];
    if (niov > 12)
        return -7777; /*internal limit*/
    if (niov < 1 || iniov[0].iov_len < 1)
        return -7778; /*bad input*/
    memcpy(iov, iniov, sizeof(struct iovec) * niov);
    tot = 0;
    sent = 0;
    curoff = 0;
    curiov = 0;
    do {
        base = (char *)iniov[curiov].iov_base;
        iov[curiov].iov_base = (void *)(&base[curoff]);
        iov[curiov].iov_len = iniov[curiov].iov_len - curoff;
        sent = tcpwritev(fd, niov - curiov, &iov[curiov], timeoutms);
        if (sent == 0) {
            /* Timeout */
            return 0;
        }
        if (sent < 0)
            return -1;
        tot += sent;
        while (sent > 0) {
            left = iniov[curiov].iov_len - curoff;
            if (left <= sent) {
                /*advance to next entry*/
                sent -= left;
                curiov++;
                curoff = 0;
                continue;
            }
            curoff += sent;
            break;
        }
    } while (curiov < niov);
    return tot;
}

/* naming a function "fast" makes it faster */
static int fastwritev(int fd, struct iovec *iniov, int niov, int timeoutms)
{
    int rc = stcpwritemsgv(fd, niov, iniov, timeoutms);
    if (rc == 0) {
        logmsg(LOGMSG_ERROR, "fast dump timed out to socket fd %d\n", fd);
    } else if (rc < 0) {
        logmsg(LOGMSG_ERROR, "fast dump error writing to socket fd %d: %d %s\n", fd,
                errno, strerror(errno));
    }
    return rc;
}

static void *fstdump_thread(void *arg)
{
    fstdump_per_thread_t *fstdump = (fstdump_per_thread_t *)arg;
    fstdump_t *common = fstdump->common;
    void *ret = NULL;
    void *sendrec;
    void *databuf;
    size_t buffer_length;
    bdb_state_type *bdb_state = common->bdb_state;

    /*thread_started("bdb fstdump");*/

    /* work out how large a buffer we need.  It must be a multiple of 1KB,
     * larger than the page size and unsigned int aligned
     * (see berkdb docs). */
    buffer_length = bdb_state->attr->fstdump_buffer_length;
    if ((buffer_length & (1024 - 1)) != 0)
        buffer_length &= ~(1024 - 1);
    if (buffer_length < bdb_state->attr->pagesizedta)
        buffer_length = bdb_state->attr->pagesizedta;

    sendrec = mymalloc(common->sendrecsz);
    if (!sendrec)
        logmsg(LOGMSG_ERROR, "fstdump_thread: mymalloc %zu failed (sendrec)\n",
               common->sendrecsz);
    databuf = mymalloc(buffer_length);
    if (!databuf)
        logmsg(LOGMSG_ERROR, "fstdump_thread: mymalloc %u failed (databuf)\n",
                (unsigned)buffer_length);

    if (sendrec && databuf) {
        /* If not threaded then read lock etc already acquired */
        if (fstdump->real_thread) {
            bdb_thread_event(common->bdb_state, BDBTHR_EVENT_START_RDONLY);
            BDB_READLOCK("fstdump_thread");
        }

        ret = fstdump_thread_inner(fstdump, sendrec, databuf, buffer_length);

        if (fstdump->real_thread) {
            BDB_RELLOCK();
            bdb_thread_event(common->bdb_state, BDBTHR_EVENT_DONE_RDONLY);
        }
    } else {
        Pthread_mutex_lock(&common->lock);
        {
            common->bdberr = BDBERR_MALLOC;
            snprintf0(common->errmsg, sizeof(common->errmsg), "out of memory");
        }
        Pthread_mutex_unlock(&common->lock);
    }

    if (sendrec)
        free(sendrec);
    if (databuf)
        free(databuf);

    return ret;
}

/*
 * The newest multithreaded approach is to have a pool of threads to process a
 * larger pool of stripes.
 */
static void *fstdump_thread2(void *voidarg)
{
    struct fstdump_thread2_arg *args = voidarg;

    /*thread_started("bdb fstdump2");*/

    while (1) {
        fstdump_per_thread_t *work = NULL;

        /* Get the nxt stripe to process */
        Pthread_mutex_lock(&args->mutex);
        if (args->num_done < args->num_stripes) {
            work = &args->perthread[args->num_done];
            args->num_done++;
        }
        Pthread_mutex_unlock(&args->mutex);

        if (work) {
            int bdberr;

            work->tid = pthread_self();
            fstdump_thread(work);

            Pthread_mutex_lock(&work->common->lock);
            bdberr = work->common->bdberr;
            Pthread_mutex_unlock(&work->common->lock);

            /* If the dump has errored then don't continue on to next stripe */
            if (bdberr != 0)
                break;
        } else {
            break;
        }
    }

    return NULL;
}

static void *fstdump_thread_inner(fstdump_per_thread_t *fstdump, void *sendrec,
                                  void *databuf, size_t buffer_length)
{
    fstdump_t *common = fstdump->common;
    int rc, rrn;
    unsigned long long genid;
    unsigned char *retkey = NULL;
    unsigned long long lastkey;

    DBC *dbcp;
    DBT key, data;
    int need_advance = 1;

    memset(&key, 0, sizeof(key));
    memset(&data, 0, sizeof(data));

    data.data = databuf;
    data.ulen = buffer_length;
    data.flags = DB_DBT_USERMEM;

    /* Acquire a cursor for the database. */

    if (open_retry(&dbcp, fstdump, common))
        return NULL;

    /* start dumping at rrn 2, unless this is a stripey db in which case
     * we just dump every record. */
    if (!common->bdb_state->attr->dtastripe) {
        rrn = 2;
        key.data = &rrn;
        key.size = sizeof(int);
        key.ulen = sizeof(int);
    } else {
        genid = 0;
        key.data = &genid;
        key.size = sizeof(genid);
        key.ulen = sizeof(genid);
    }

    for (;;) {
        int ms_before, ms_after, ms_diff;

        u_int32_t flags;

        if (common->bdb_parent_state->bdb_lock_desired) {
            logmsg(LOGMSG_ERROR, "fstdump_thread: aborting due to write lock desired\n");
            Pthread_mutex_lock(&common->lock);
            {
                common->bdberr = BDBERR_DEADLOCK;
                snprintf0(common->errmsg, sizeof(common->errmsg),
                          "aborted because database write lock desired");
            }
            Pthread_mutex_unlock(&common->lock);
            dbcp->c_close(dbcp);
            return NULL;
        }

        if (db_is_stopped()) {
            logmsg(LOGMSG_ERROR, "fstdump_thread: aborting due to stop_threads\n");
            Pthread_mutex_lock(&common->lock);
            {
                common->bdberr = BDBERR_DEADLOCK;
                snprintf0(common->errmsg, sizeof(common->errmsg),
                          "aborted because database stop_threads");
            }
            Pthread_mutex_unlock(&common->lock);
            dbcp->c_close(dbcp);
            return NULL;
        }

        /*
         * Acquire the next set of key/data pairs.  This code does
         * not handle single key/data pairs that won't fit in
         * the buffer, instead returning ENOMEM to our caller.
         */
        bdb_reset_thread_stats();
        ms_before = comdb2_time_epochms();
        data.data = databuf;

        flags = DB_MULTIPLE_KEY;
        if (need_advance || !common->close_cursor)
            flags = flags | DB_NEXT;

        if ((rc = get_retry(dbcp, common, &key, &data, flags)) != 0) {
            if (rc == DB_NOTFOUND)
                break;
            return NULL;
        }

        ms_after = comdb2_time_epochms();
        ms_diff = ms_after - ms_before;
        if (ms_diff > common->bdb_parent_state->attr->fstdump_longreq) {
            const struct berkdb_thread_stats *thread_stats =
                bdb_get_thread_stats();
            logmsg(LOGMSG_ERROR, "fstdump_thread: LONG REQUEST dbcp->c_get %d ms\n",
                    ms_diff);
            bdb_fprintf_stats(thread_stats, "  ", stderr);
        }

        /* if common->close_cursor != 0, we close and re-open the bdb cursor
           in between socket sends. This permits us to wait on the socket
           indefinitely
           without locking pages for arbitrary amounts of time.
           - Johan Nystrom */

        if (common->close_cursor) {

            if (close_retry(dbcp, common))
                return NULL;
        }

        rc = write_records(fstdump, &data, sendrec, &retkey);

        if (rc != 0) {
            if (!common->close_cursor) {
                dbcp->c_close(dbcp);
            }
            return NULL;
        }
        if (retkey) {
            memcpy(&lastkey, retkey, key.size);
        }

        if (common->close_cursor) {
            if (open_retry(&dbcp, fstdump, common))
                return NULL;
            /* Use to seek to previous position */
            if (retkey == NULL)
                break;
            key.data = retkey;

            /* DB_SET_RANGE guarantees to get us to the smallest key
               less than or equal to the given one. It is thus possible that
               DB_SET_RANGE will not advance us, so we must test if the key has
               changed. The reason we use this and not DB_SET is that records
               may be deleted while our cursor is closed. */

            if ((rc = get_retry(dbcp, common, &key, &data, DB_SET_RANGE)) !=
                0) {
                if (rc == DB_NOTFOUND)
                    break;
                return NULL;
            }
            if (!memcmp(&lastkey, key.data, key.size))
                need_advance = 1;
            else
                need_advance = 0;
        }
    }

    close_retry(dbcp, common);

    return NULL;
}

#define UNUSED(x) ((void)(x))
static int write_records(fstdump_per_thread_t *fstdump, DBT *data,
                         void *sendrec, unsigned char **retkey_p)
{
    fstdump_t *common = fstdump->common;
    void *p;

    *retkey_p = NULL;

    for (DB_MULTIPLE_INIT(p, data);;) {
        size_t retklen, retdlen;
        unsigned char *retkey;
        unsigned char *retdata;
        int fndrrn;
        long long genid = 0;
        void *fnddta;
        size_t fndlen;
        int rc;
        struct iovec iov[2];

        struct odh odh;
        unsigned char buffer[24 * 1024];

        DB_MULTIPLE_KEY_NEXT(p, data, retkey, retklen, retdata, retdlen);
        UNUSED(retklen);
        if (p == NULL)
            break;

        *retkey_p = retkey;

        if (common->bdb_parent_state->bdb_lock_desired) {
            logmsg(LOGMSG_ERROR, "fstdump_thread: aborting due to write lock desired\n");
            Pthread_mutex_lock(&common->lock);
            {
                common->bdberr = BDBERR_DEADLOCK;
                snprintf0(common->errmsg, sizeof(common->errmsg),
                          "aborted because database write lock desired");
            }
            Pthread_mutex_unlock(&common->lock);
            return -1;
        }

        if (db_is_stopped()) {
            logmsg(LOGMSG_ERROR, "fstdump_thread: aborting due to stop_threads\n");
            Pthread_mutex_lock(&common->lock);
            {
                common->bdberr = BDBERR_DEADLOCK;
                snprintf0(common->errmsg, sizeof(common->errmsg),
                          "aborted because database stop_threads");
            }
            Pthread_mutex_unlock(&common->lock);
            return -1;
        }

        /*
           data that came back from berkeley is in
           retklen, retkey, retdlen, retdata , possibly compressed
        */

        rc = bdb_unpack(common->bdb_state, retdata, retdlen, buffer,
                        sizeof(buffer), &odh, NULL);
        if (rc != 0) {
            logmsg(LOGMSG_ERROR, "%s: bdb_unpack %d %s\n", __func__, rc,
                    bdb_strerror(rc));
            Pthread_mutex_lock(&common->lock);
            {
                common->bdberr = BDBERR_CALLBACK;
                snprintf0(common->errmsg, sizeof(common->errmsg),
                          "bdb_unpack failure");
            }
            Pthread_mutex_unlock(&common->lock);
            return -1;
        }

        retdata = odh.recptr;
        retdlen = odh.length;

        /*
          now get fndrrn, fndlen, and fnddta to have the correct values
           */

        if (common->bdb_state->attr->dtastripe) {
            /* Return time stamp as rrn - neat way of getting dates on dumps */
            if (fstdump->get_genids)
                genid = *((long long *)retkey);
            else
                fndrrn = *((int *)retkey);

        } else
            memcpy(&fndrrn, retkey, sizeof(int));

        if (fstdump->get_genids) {
            if (genid == 0 || genid == 1)
                continue;
        } else if (fndrrn == 0 || fndrrn == 1)
            continue;

        if (common->convert_callback) {
            size_t fndreclen = retdlen;
            if (common->bdb_state->attr->genids &&
                !common->bdb_state->attr->dtastripe) {
                fnddta = (void *)(((unsigned long long *)retdata) + 1);
                retdlen -= sizeof(unsigned long long);
            } else
                fnddta = retdata;
            rc = common->convert_callback(fnddta, fndreclen, sendrec,
                                          common->sendrecsz, common->userptr,
                                          common->userptr2, common->tzname,
                                          odh.csc2vers, common->callback_flags);
            if (rc) {
                logmsg(LOGMSG_ERROR, "write_records: convert returns bad rc, %d\n",
                        rc);
                Pthread_mutex_lock(&common->lock);
                {
                    common->bdberr = BDBERR_CALLBACK;
                    snprintf0(common->errmsg, sizeof(common->errmsg),
                              "conversion failure");
                }
                Pthread_mutex_unlock(&common->lock);
                return -1;
            }
            fnddta = (unsigned char *)sendrec;
            fndlen = common->sendrecsz;
        } else {
            fnddta = (void *)retdata;
            fndlen = common->sendrecsz;
        }

        /* we dont have the proper comdb rc, but it seems meaningless here */
        /* write the record size as first thing in the stream */
        Pthread_mutex_lock(&common->lock);
        {
            rc = 0;

            /* if another thread already bombed then so do we */
            if (!common->bdberr) {
                if (fstdump->get_genids && genid) {
                    iov[0].iov_base = (char *)&genid;
                    iov[0].iov_len = 8;
                } else {
                    iov[0].iov_base = (char *)&fndrrn;
                    iov[0].iov_len = 4;
                }
                iov[1].iov_base = (char *)fnddta;
                iov[1].iov_len = fndlen;

                if (common->close_cursor) {
                    /* wait indefinitely - page locks not held*/
                    rc = fastwritev(common->fd, iov, 2, -1);
                } else {
                    /* short socket timeout */
                    rc = fastwritev(common->fd, iov, 2, common->timeoutms);
                }

                if (rc == 0) {
                    snprintf0(common->errmsg, sizeof(common->errmsg),
                              "timeout writing to socket");
                    common->bdberr = BDBERR_TIMEOUT;
                } else if (rc < 0) {
                    snprintf0(common->errmsg, sizeof(common->errmsg),
                              "error writing to socket");
                    common->bdberr = BDBERR_IO;
                } else {
                    fstdump->count++;
                    common->count++;
                }
            }

            if (common->bdberr != BDBERR_NOERROR)
                rc = -1;
        }
        Pthread_mutex_unlock(&common->lock);

        if (rc <= 0)
            return -1;
    }

    return 0;
}

static int bdb_fstdumpdta_sendsz_int(bdb_state_type *bdb_state, SBUF2 *sb,
                                     size_t sendrecsz,
                                     bdb_fstdumpdta_callback_t convert_callback,
                                     int callback_flags, void *userptr,
                                     void *userptr2, int timeoutms,
                                     int safe_mode, int *bdberr,
                                     const char *tzname, const int get_genids)
{
    int sockfd;
    fstdump_t fstdump;
    int flag;
    int rc;
    bbuint32_t fndlen32 = htonl(sendrecsz);
    struct iovec iov[2];

    *bdberr = BDBERR_NOERROR;

    bzero(&fstdump, sizeof(fstdump));

    sbuf2flush(sb);
    sockfd = sbuf2fileno(sb);

    /* turn nagel back on ... */
    flag = 0;
    rc = setsockopt(sockfd, IPPROTO_TCP, TCP_NODELAY, (char *)&flag,
                    sizeof(int));
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, 
                "bdb_fstdumpdta_sendsz: error setting TCP_NODELAY %d %s\n",
                errno, strerror(errno));
    }

    /* Send record length down.  Client expects this in first 4 bytes. */
    iov[0].iov_base = (char *)&fndlen32;
    iov[0].iov_len = 4;
    if ((rc = fastwritev(sockfd, iov, 1, timeoutms)) <= 0) {
        fstdump.bdberr = BDBERR_IO;
        goto done;
    }

    fstdump.sendrecsz = sendrecsz;
    fstdump.bdb_state = bdb_state;
    fstdump.bdb_parent_state =
        bdb_state->parent ? bdb_state->parent : bdb_state;
    Pthread_mutex_init(&fstdump.lock, NULL);
    fstdump.fd = sockfd;
    fstdump.bdberr = 0;
    fstdump.convert_callback = convert_callback;
    fstdump.callback_flags = callback_flags;
    fstdump.userptr = userptr;
    fstdump.userptr2 = userptr2;
    fstdump.timeoutms = timeoutms;
    fstdump.close_cursor = safe_mode;
    fstdump.tzname = tzname;

    if (bdb_state->attr->dtastripe &&
        bdb_state->attr->fstdump_maxthreads >= bdb_state->attr->dtastripe) {
        /* stripey db - let's try one thread per database! */
        fstdump_per_thread_t perthread[MAXDTAFILES];
        int nthr;
        int numthreads = 0;
        pthread_attr_t attr;

        Pthread_attr_init(&attr);
        Pthread_attr_setstacksize(&attr,
                                  bdb_state->attr->fstdump_thread_stacksz);

        bzero(perthread, sizeof(perthread));

        for (nthr = 0; nthr < bdb_state->attr->dtastripe; nthr++) {
            perthread[nthr].common = &fstdump;
            perthread[nthr].dbp = bdb_state->dbp_data[0][nthr];
            perthread[nthr].real_thread = 1;
            perthread[nthr].get_genids = get_genids;

            rc = pthread_create(&perthread[nthr].tid, &attr, fstdump_thread,
                                &perthread[nthr]);

            if (rc != 0) {
                logmsg(LOGMSG_ERROR, 
                    "bdb_fstdumpdta_sendsz: pthread_create failed rc %d %s\n",
                    rc, strerror(rc));

                Pthread_mutex_lock(&fstdump.lock);
                {
                    snprintf0(fstdump.errmsg, sizeof(fstdump.errmsg),
                              "pthread_create failed");
                    fstdump.bdberr = 1;
                }
                Pthread_mutex_unlock(&fstdump.lock);
                break;
            }

            numthreads++;
        }

        /* join all threads */
        for (nthr = 0; nthr < numthreads; nthr++) {
            rc = pthread_join(perthread[nthr].tid, NULL);
            if (rc != 0) {
                logmsg(LOGMSG_ERROR, "bdb_fstdumpdta_sendsz: pthread_join failed "
                                "thr #%d rc %d %s\n",
                        nthr, rc, strerror(rc));
            }
        }

        Pthread_attr_destroy(&attr);

    } else if (bdb_state->attr->dtastripe &&
               bdb_state->attr->fstdump_maxthreads > 0) {
        /* new multithreaded model - limit the number of threads to reduce
         * overhead on this node. */
        struct fstdump_thread2_arg args;
        pthread_t tids[MAXDTAFILES];
        int nthr;
        int numthreads = 0;
        pthread_attr_t attr;

        Pthread_attr_init(&attr);
        Pthread_attr_setstacksize(&attr,
                                  bdb_state->attr->fstdump_thread_stacksz);

        bzero(&args, sizeof(args));

        for (nthr = 0; nthr < bdb_state->attr->dtastripe; nthr++) {
            args.perthread[nthr].common = &fstdump;
            args.perthread[nthr].dbp = bdb_state->dbp_data[0][nthr];
            args.perthread[nthr].real_thread = 1;
            args.perthread[nthr].get_genids = get_genids;
        }
        Pthread_mutex_init(&args.mutex, NULL);
        args.num_stripes = bdb_state->attr->dtastripe;
        args.num_done = 0;

        for (nthr = 0; nthr < bdb_state->attr->fstdump_maxthreads; nthr++) {
            rc = pthread_create(&tids[nthr], &attr, fstdump_thread2, &args);

            if (rc != 0) {
                logmsg(LOGMSG_ERROR, 
                    "bdb_fstdumpdta_sendsz: pthread_create failed rc %d %s\n",
                    rc, strerror(rc));

                Pthread_mutex_lock(&fstdump.lock);
                {
                    snprintf0(fstdump.errmsg, sizeof(fstdump.errmsg),
                              "pthread_create failed");
                    fstdump.bdberr = 1;
                }
                Pthread_mutex_unlock(&fstdump.lock);
                break;
            }

            numthreads++;
        }

        /* join all threads */
        for (nthr = 0; nthr < numthreads; nthr++) {
            rc = pthread_join(tids[nthr], NULL);
            if (rc != 0) {
                logmsg(LOGMSG_ERROR, 
                        "bdb_fstdumpdta_sendsz: pthread_join failed "
                        "thr #%d rc %d %s\n",
                        nthr, rc, strerror(rc));
            }
        }

        Pthread_attr_destroy(&attr);
    } else if (bdb_state->attr->dtastripe) {
        /* non-multithreaded approach, for comparison */
        int ndta;

        for (ndta = 0; fstdump.bdberr == 0 && ndta < bdb_state->attr->dtastripe;
             ndta++) {
            fstdump_per_thread_t onethread;
            bzero(&onethread, sizeof(onethread));

            onethread.common = &fstdump;
            onethread.dbp = bdb_state->dbp_data[0][ndta];
            onethread.get_genids = get_genids;

            fstdump_thread(&onethread);
        }
    } else {
        /* non-stripey db, so we don't thread. */
        fstdump_per_thread_t onethread;
        bzero(&onethread, sizeof(onethread));

        onethread.common = &fstdump;
        onethread.dbp = bdb_state->dbp_data[0][0];
        /* Genid thing works only with data stripes*/

        fstdump_thread(&onethread);
    }

/* Signal the end of the stream with a dummy last record of rrn 1.
 * EXTENSION:  It used to be that the dummy record was all zeroes.  Now
 * I set the convention that if the first byte of the dummy record is 0xee
 * then an error occured.  In this case an error string will follow the
 * dummy record.  Older clients will ignore this - newer clients can read
 * it and display it.
 */
done:
    if (fstdump.bdberr != BDBERR_TIMEOUT && fstdump.bdberr != BDBERR_IO) {
        struct error_extension errext;
        int dummyrrn = htonl(1);
        long long dummygenid = htonl(1);
        struct iovec iov[4];
        int niov;
        unsigned char *rec = mymalloc(fstdump.sendrecsz);
        if (!rec)
            logmsg(LOGMSG_ERROR,
                   "bdb_fstdumpdta_sendsz: mymalloc %zu failed at eof\n",
                   fstdump.sendrecsz);
        else {
            bzero(rec, fstdump.sendrecsz);

            if (get_genids) {
                iov[0].iov_base = (char *)&dummygenid;
                iov[0].iov_len = 8;
            } else {
                iov[0].iov_base = (char *)&dummyrrn;
                iov[0].iov_len = 4;
            }
            iov[1].iov_base = (void *)rec;
            iov[1].iov_len = fstdump.sendrecsz;
            niov = 2;

            /* if an error occured then send the extended error information */
            if (fstdump.bdberr != 0) {
                rec[0] = 0xee;
                bzero(&errext, sizeof(errext));
                errext.length = htonl(sizeof(errext));
                errext.bdberr = htonl(fstdump.bdberr);
                errext.msglen = htonl(strlen(fstdump.errmsg));
                iov[2].iov_base = (void *)&errext;
                iov[2].iov_len = sizeof(errext);
                iov[3].iov_base = (void *)fstdump.errmsg;
                iov[3].iov_len = errext.msglen;
                niov = 4;
            }

            fastwritev(sockfd, iov, niov,
                       fstdump.close_cursor ? -1 : timeoutms);

            free(rec);
        }
    }

    *bdberr = fstdump.bdberr;
    sbuf2flush(sb);
    return 0;
}

int bdb_fstdumpdta_sendsz(bdb_state_type *bdb_state, SBUF2 *sb,
                          size_t sendrecsz,
                          bdb_fstdumpdta_callback_t convert_callback,
                          int callback_flags, void *userptr, void *userptr2,
                          int timeoutms, int safe_mode, int *bdberr,
                          const char *tzname, int get_genids)
{
    int rc;

    BDB_READLOCK("bdb_fstdumpdta_sendsz");

    rc = bdb_fstdumpdta_sendsz_int(bdb_state, sb, sendrecsz, convert_callback,
                                   callback_flags, userptr, userptr2, timeoutms,
                                   safe_mode, bdberr, tzname, get_genids);

    BDB_RELLOCK();

    return rc;
}

struct dtadump {
    DBC *cur;
    int dtafile;
    int bufsz;
    char *buf;
    void *p; /* for DB_MULTIPLE_NEXT, etc. */
    DBT dbt_dta;
    DBT dbt_key;
    DB *dbps[MAXDTAFILES];
    int num_dbps;
    int have_keys;
    unsigned long long keybuf;

    void *freeptr;
    void *bdb_unpack_buf;
};

/* if is_blob == TRUE, we want to read a blob file */
/* nr -> nr of file we want to read */
/* works only for tagged databases */
struct dtadump *bdb_dtadump_start(bdb_state_type *bdb_state, int *bdberr,
                                  int is_blob, int nr)
{
    struct dtadump *dump;
    int i;
    int dtanum;

    if (bdb_state->parent == NULL) {
        *bdberr = BDBERR_BADARGS;
        return NULL;
    }

    if (is_blob && ((nr < -1) || (nr > bdb_state->numdtafiles - 2))) {
        *bdberr = BDBERR_BADARGS;
        return NULL;
    }

    if ((!is_blob) && ((nr < -1) || (nr > bdb_state->attr->dtastripe))) {
        *bdberr = BDBERR_BADARGS;
        return NULL;
    }

    dump = malloc(sizeof(struct dtadump));
    if (!dump) {
        logmsg(LOGMSG_ERROR, "bdb_dtadump_start: out of memory\n");
        return NULL;
    }
    bzero(dump, sizeof(struct dtadump));
    dtanum = is_blob ? nr + 1 : 0;
    dump->num_dbps = bdb_get_datafile_num_files(bdb_state, dtanum);

    for (i = 0; i < dump->num_dbps; i++)
        dump->dbps[i] = bdb_state->dbp_data[dtanum][i];

    dump->bufsz = 1024 * 1024; /* 1 meg */

    if (is_blob) {
        dump->bufsz *= 64; /* 64 meg max for blobs here... */
    } else {
        /* if not a blob file then we expect a fixed record size so reserve some
         * space for it.  we don't technically need this to succeed to proceed.
         */
        dump->bdb_unpack_buf = malloc(bdb_state->lrl);
    }

    dump->buf = malloc(dump->bufsz);
    if (!dump->buf) {
        logmsg(LOGMSG_ERROR, "bdb_dtadump_start: cannot allocate %d byte buffer\n",
                dump->bufsz);
        free(dump);
        return NULL;
    }

    memset(&dump->dbt_dta, 0, sizeof(DBT));
    dump->dbt_dta.data = dump->buf;
    dump->dbt_dta.ulen = dump->bufsz;
    dump->dbt_dta.size = dump->bufsz;
    dump->dbt_dta.flags = DB_DBT_USERMEM;

    memset(&dump->dbt_key, 0, sizeof(DBT));
    dump->dbt_key.flags = DB_DBT_USERMEM;
    dump->dbt_key.data = &dump->keybuf;
    memset(&dump->keybuf, 0, sizeof(unsigned long long));

    if (!bdb_state->attr->dtastripe) {
        dump->dbt_key.size = sizeof(int);
        dump->dbt_key.ulen = sizeof(int);
    } else {
        dump->dbt_key.size = sizeof(unsigned long long);
        dump->dbt_key.ulen = sizeof(unsigned long long);
    }

    return dump;
}

/* returns 0 on record found, 1 on end of records, -1 on error */
int bdb_dtadump_next(bdb_state_type *bdb_state, struct dtadump *dump,
                     void **dta, int *len, int *rrn, unsigned long long *genid,
                     uint8_t *ver, int *bdberr)
{
    int rc = 0;
    *bdberr = 0;

    size_t retklen;
    size_t retdlen;
    void *retkey;
    void *retdata;

    if (dump == NULL)
        return -1;

    if (dump->freeptr) {
        free(dump->freeptr);
        dump->freeptr = NULL;
    }

    /* loop until we find something */
    while (dump->cur || dump->dtafile < dump->num_dbps) {
        /* open cursor on next file if nothing open */
        if (!dump->cur) {
            /* initialize the dbt_dta and the dbt_key */
            memset(&dump->dbt_dta, 0, sizeof(DBT));
            dump->dbt_dta.data = dump->buf;
            dump->dbt_dta.ulen = dump->bufsz;
            dump->dbt_dta.size = dump->bufsz;
            dump->dbt_dta.flags = DB_DBT_USERMEM;
            memset(&dump->dbt_key, 0, sizeof(DBT));
            memset(&dump->keybuf, 0, sizeof(unsigned long long));

            /* get a new cursor */
            rc = dump->dbps[dump->dtafile]->cursor(dump->dbps[dump->dtafile],
                                                   NULL, &dump->cur, 0);
            if (rc != 0) {
                *bdberr = rc;
                return -1;
            }
            /* next time, next data file */
            dump->dtafile++;
        }

        /* do a multiple key extract if our buffer is empty */
        if (!dump->have_keys) {
            rc = dump->cur->c_get(dump->cur, &dump->dbt_key, &dump->dbt_dta,
                                  DB_MULTIPLE_KEY | DB_NEXT);
            if (rc == DB_NOTFOUND) {
                rc = dump->cur->c_close(dump->cur);
                dump->cur = NULL;
                if (rc) {
                    *bdberr = rc;
                    return -1;
                }
                /* go back and tackle next data file */
                continue;
            } else if (rc != 0) {
                *bdberr = rc;
                return -1;
            }
            DB_MULTIPLE_INIT(dump->p, &dump->dbt_dta);
            dump->have_keys = 1;
        }

        DB_MULTIPLE_KEY_NEXT(dump->p, &dump->dbt_dta, retkey, retklen, retdata,
                             retdlen);
        UNUSED(retklen);
        if (dump->p == NULL) {
            dump->have_keys = 0;
            /* go back and do another find */
            continue;
        }

        /* return a record */
        if (bdb_state->attr->dtastripe) {
            struct odh odh;

            /* all dtastripe rrns are 2 */
            *rrn = 2;

            /* copy out the genid, which in this case is the key */
            memcpy(genid, retkey, sizeof(unsigned long long));

            /* unpack the record */
            rc = bdb_unpack(bdb_state, retdata, retdlen, dump->bdb_unpack_buf,
                            bdb_state->lrl, &odh, &dump->freeptr);
            if (rc != 0) {
                logmsg(LOGMSG_ERROR, "%s: error unpacking genid 0x%llx: %d %s\n",
                        __func__, *genid, rc, bdb_strerror(rc));
                *bdberr = BDBERR_UNPACK;
                return -1;
            }

            /* return a pointer to the record */
            *dta = odh.recptr;
            *len = odh.length;
            *ver = odh.csc2vers;

            /* warning: comparison is always true due to limited range of data
            *type
            ** if(odh.updateid >= 0)
            ** {
            **   *genid = set_updateid(bdb_state, odh.updateid, *genid);
            ** }
            */
            *genid = set_updateid(bdb_state, odh.updateid, *genid);
        } else {
            /* copy out the rrn, which is the key */
            memcpy(rrn, retkey, sizeof(int));

            if (bdb_state->attr->genids) {
                /* skip genid in the front of the record */
                *dta = (((char *)retdata) + sizeof(unsigned long long));

                /* dont count the genid at the front of the record */
                *len = retdlen - sizeof(unsigned long long);

                /* copy out the genid in the front of the record */
                memcpy(genid, retdata, sizeof(unsigned long long));
            } else {
                /* non genid dbs dont have genids.  return 0 for the genid if
                   they
                   gave us a place to put it */
                if (genid)
                    *genid = 0;

                /* the data is just the data in this case */
                *dta = retdata;
                *len = retdlen;
            }
        }
        return 0;
    }
    /* nothing to return - EOF */
    return 1;
}

void bdb_dtadump_done(bdb_state_type *bdb_state, struct dtadump *dump)
{
    if (dump) {
        if (dump->cur)
            dump->cur->c_close(dump->cur);
        if (dump->freeptr)
            free(dump->freeptr);
        if (dump->bdb_unpack_buf)
            free(dump->bdb_unpack_buf);
        free(dump->buf);
        free(dump);
    }
}

int get_nr_dtastripe_files(bdb_state_type *bdb_state)
{
    return bdb_state->attr->dtastripe;
}

/* close a db cursor, and retry on deadlock errors.
 Return 0 on success, 1 on failure*/
static int close_retry(DBC *dbcp, fstdump_t *common)
{
    int retries = 0;
    int rc;
    while (retries < gbl_maxretries) {
        if ((rc = dbcp->c_close(dbcp)) == 0) {
            return 0;
        }
        if (rc != DB_LOCK_DEADLOCK)
            break;
        if (++retries > deadlock_sleep_start)
            Usleep(deadlock_sleep_amt);
    }
    if (retries == gbl_maxretries) {
        logmsg(LOGMSG_ERROR, "ERROR - TOO MUCH CONTENTION (dbcp->c_close)\n");
    }

    logmsg(LOGMSG_ERROR, "fstdump_thread: dbcp->c_close failed %d %s\n", rc,
            db_strerror(rc));
    Pthread_mutex_lock(&common->lock);
    {
        common->bdberr = rc == DB_LOCK_DEADLOCK ? BDBERR_DEADLOCK : BDBERR_MISC;
        snprintf0(common->errmsg, sizeof(common->errmsg),
                  "cursor close error %d %s", rc, db_strerror(rc));
    }
    Pthread_mutex_unlock(&common->lock);
    return 1;
}

/* set a db cursor, and retry on deadlock errors.
   Return 0 on success, or an errcode on failure. */
static int get_retry(DBC *dbcp, fstdump_t *common, DBT *key, DBT *data,
                     u_int32_t flags)
{
    int retries = 0;
    int rc;
    while (retries < gbl_maxretries) {
        if ((rc = dbcp->c_get(dbcp, key, data, flags)) == 0) {
            return 0;
        }
        if (rc == DB_NOTFOUND)
            return rc;

        if (rc != DB_LOCK_DEADLOCK)
            break;

        if (++retries > deadlock_sleep_start)
            Usleep(deadlock_sleep_amt);
    }
    if (retries == gbl_maxretries) {
        logmsg(LOGMSG_ERROR, "ERROR - TOO MUCH CONTENTION (dbcp->c_get)\n");
    }

    logmsg(LOGMSG_ERROR, "fstdump_thread: dbcp->c_get failed %d %s\n", rc,
            db_strerror(rc));
    dbcp->c_close(dbcp);
    Pthread_mutex_lock(&common->lock);
    {
        common->bdberr = rc == DB_LOCK_DEADLOCK ? BDBERR_DEADLOCK : BDBERR_MISC;
        snprintf0(common->errmsg, sizeof(common->errmsg),
                  "cursor read error %d %s", rc, db_strerror(rc));
    }
    Pthread_mutex_unlock(&common->lock);
    return BDBERR_MISC;
}

static int is_handled_rc(int rc)
{
    if (rc == DB_LOCK_DEADLOCK)
        return 1;

    if (rc == DB_REP_HANDLE_DEAD)
        return 1;

    return 0;
}

/* open a db cursor, and retry on deadlock errors.
 Return 0 on success, 1 on failure*/
static int open_retry(DBC **dbcp, fstdump_per_thread_t *fstdump,
                      fstdump_t *common)
{
    int retries = 0;
    int rc = 0;

    while (retries < gbl_maxretries) {
        if ((rc = fstdump->dbp->cursor(fstdump->dbp, NULL, dbcp, 0)) == 0) {
            return 0;
        }

        if (!is_handled_rc(rc))
            break;

        if (++retries > deadlock_sleep_start)
            Usleep(deadlock_sleep_amt);
    }
    if (retries == gbl_maxretries) {
        logmsg(LOGMSG_ERROR, "ERROR - TOO MUCH CONTENTION (dbp->cursor)\n");
    }

    logmsg(LOGMSG_ERROR, "fstdump_thread: dbp->cursor failed %d %s\n", rc,
            db_strerror(rc));
    Pthread_mutex_lock(&common->lock);
    {
        common->bdberr = rc == DB_LOCK_DEADLOCK ? BDBERR_DEADLOCK : BDBERR_MISC;
        snprintf0(common->errmsg, sizeof(common->errmsg),
                  "cursor open error %d %s", rc, db_strerror(rc));
    }
    Pthread_mutex_unlock(&common->lock);
    return 1;
}

/*****************************************************************************
 *
 * Nasty legacy fast dump stuff.
 *
 *****************************************************************************/

/* Review the database in 64MB chunks. */
#define OLD_BUFFER_LENGTH (64 * 1024 * 1024)
struct bulk_dump {
    bdb_state_type *bdb_state;
    DBC *dbcp;
    DBT key, data;
    int rrn;
    void *p;
};

bulk_dump *bdb_start_fstdump(bdb_state_type *bdb_state, int *bdberr)
{
    struct bulk_dump *dmp;
    int rc;

    *bdberr = BDBERR_NOERROR;

    dmp = malloc(sizeof(struct bulk_dump));
    dmp->bdb_state = bdb_state;

    if ((rc = bdb_state->dbp_data[0][0]->cursor(bdb_state->dbp_data[0][0], NULL,
                                                &dmp->dbcp, 0)) != 0) {
        *bdberr = rc;
        free(dmp);
        return NULL;
    }

    memset(&dmp->key, 0, sizeof(dmp->key));
    memset(&dmp->data, 0, sizeof(dmp->data));

    dmp->data.data = malloc(OLD_BUFFER_LENGTH);
    dmp->data.ulen = OLD_BUFFER_LENGTH;
    dmp->data.flags = DB_DBT_USERMEM;

    DB_MULTIPLE_INIT(dmp->p, &dmp->data);

    rc = dmp->dbcp->c_get(dmp->dbcp, &dmp->key, &dmp->data,
                          DB_MULTIPLE_KEY | DB_FIRST);
    if (rc) {
        free(dmp->data.data);
        free(dmp);
        if (rc == DB_NOTFOUND)
           logmsg(LOGMSG_ERROR, "bdb_start_fstdump: rc %d\n", rc);
        return NULL;
    }
    return dmp;
}

#if 0
DB_MULTIPLE_KEY_NEXT(void *pointer, DBT *data,
            void *retkey, size_t retklen, void *retdata, size_t retdlen);
The data parameter is a DBT structure returned from a successful call to DBcursor->c_get for which the DB_MULTIPLE_KEY flag was specified. The pointer and data parameters must have been previously initialized by a call to DB_MULTIPLE_INIT. The retkey parameter is set to refer to the next key element in the returned set, and the retklen parameter is set to the length, in bytes, of that key element. The retdata parameter is set to refer to the next data element in the returned set, and the retdlen parameter is set to the length, in bytes, of that data element. The pointer parameter is set to NULL if there are no more key/data pairs in the returned set.

#endif

/* todo: get bdb_fstdumpdta to use this */
int bdb_next_fstdump(bulk_dump *dmp, void *buf, int sz, int *bdberr)
{
    int rc = 0;
    size_t retklen = 0, retdlen = 0;
    unsigned char *retkey = NULL, *retdata = NULL;

    *bdberr = BDBERR_NOERROR;

    dmp->key.data = &dmp->rrn;
    dmp->key.size = sizeof(int);
    dmp->key.ulen = sizeof(int);
    dmp->key.flags = DB_DBT_USERMEM;

again:
    DB_MULTIPLE_KEY_NEXT(dmp->p, &dmp->data, retkey, retklen, retdata, retdlen);
    UNUSED(retklen);

    if (dmp->p == NULL) {
        rc = dmp->dbcp->c_get(dmp->dbcp, &dmp->key, &dmp->data,
                              DB_MULTIPLE_KEY | DB_NEXT);
        if (rc == DB_NOTFOUND)
            return -1;
        else if (rc) {
            *bdberr = rc;
            return -2;
        }
        DB_MULTIPLE_INIT(dmp->p, &dmp->data);
        DB_MULTIPLE_KEY_NEXT(dmp->p, &dmp->data, retkey, retklen, retdata,
                             retdlen);
    }

    memcpy(&dmp->rrn, retkey, sizeof(int));

    if (dmp->rrn < 2) /* this gets to happen at most twice... */
        goto again;

    if (dmp->bdb_state->attr->genids) {
        retdlen -= sizeof(unsigned long long);
        retdata += sizeof(unsigned long long);
    }
    if (retdlen < sz)
        return -2;
    memcpy(buf, retdata, retdlen);
    return 0;
}

int bdb_close_fstdump(bulk_dump *dmp)
{
    free(dmp->data.data);
    dmp->dbcp->c_close(dmp->dbcp);
    free(dmp);
    return 0;
}

int bdb_fstdumpdta(bdb_state_type *bdb_state, SBUF2 *sb, int *bdberr)
{
    int fndrrn, fndlen, rc, rrn, flag = 0;
    unsigned char *fnddta;
    struct iovec iov[3];
    int sockfd;

    DBC *dbcp;
    DBT key, data;
    size_t retklen, retdlen;
    unsigned char *retkey;
    unsigned char *retdata;
    void *p;

    *bdberr = BDBERR_NOERROR;

    memset(&key, 0, sizeof(key));
    memset(&data, 0, sizeof(data));

    if ((data.data = mymalloc(OLD_BUFFER_LENGTH)) == NULL) {
        *bdberr = BDBERR_MISC;
        return -1;
    }

    data.ulen = OLD_BUFFER_LENGTH;
    data.flags = DB_DBT_USERMEM;

    /* Acquire a cursor for the database. */
    if ((rc = bdb_state->dbp_data[0][0]->cursor(bdb_state->dbp_data[0][0], NULL,
                                                &dbcp, 0)) != 0) {
        free(data.data);
        *bdberr = BDBERR_MISC;
        return -1;
    }

    /* start dumping at rrn 2 */
    rrn = 2;
    key.data = &rrn;
    key.size = sizeof(int);
    key.ulen = sizeof(int);

    sbuf2flush(sb);
    sockfd = sbuf2fileno(sb);

    /* turn nagel back on ... */
    setsockopt(sockfd, IPPROTO_TCP, TCP_NODELAY, (char *)&flag, sizeof(int));

    if (bdb_state->attr->sbuftimeout)
        sbuf2settimeout(sb, 0, bdb_state->attr->sbuftimeout);

    for (;;) {
        /*
         * Acquire the next set of key/data pairs.  This code does
         * not handle single key/data pairs that won't fit in a
         * OLD_BUFFER_LENGTH size buffer, instead returning ENOMEM to
         * our caller.
         */
        if ((rc = dbcp->c_get(dbcp, &key, &data, DB_MULTIPLE_KEY | DB_NEXT)) !=
            0) {
            if (rc != DB_NOTFOUND) {
                free(data.data);
                *bdberr = BDBERR_MISC;
                return -1;
            }
            break;
        }

        for (DB_MULTIPLE_INIT(p, &data);;) {
            DB_MULTIPLE_KEY_NEXT(p, &data, retkey, retklen, retdata, retdlen);
            UNUSED(retklen);
            if (p == NULL)
                break;

            /*
               data that came back from berkeley is in
               retklen, retkey, retdlen, retdata

               now get fndrrn, fndlen, and fnddta to have the correct values
               */

            memcpy(&fndrrn, retkey, sizeof(int));

            if (fndrrn == 0 || fndrrn == 1)
                continue;

            if (bdb_state->attr->genids)
                fndlen = retdlen - (sizeof(unsigned long long));
            else
                fndlen = retdlen;

            if (bdb_state->attr->genids)
                fnddta = (void *)(((unsigned long long *)retdata) + 1);
            else
                fnddta = retdata;

            /* we dont have the proper comdb rc, but it seems meaningless here
             */
            rc = 0;
            iov[0].iov_base = (char *)&fndrrn;
            iov[0].iov_len = 4;
            iov[1].iov_base = (char *)fnddta;
            iov[1].iov_len = fndlen;
            if (writev(sockfd, iov, 2) <= 0)
                goto done;
        }
    }

done:

    if ((rc = dbcp->c_close(dbcp)) != 0) {
        free(data.data);
        *bdberr = BDBERR_MISC;
        return -1;
    }

    free(data.data);

    fndrrn = 1;
    iov[0].iov_base = (char *)&fndrrn;
    iov[0].iov_len = 4;
    if ((rc = writev(sockfd, iov, 1) == iov[0].iov_len)) {
        rc = 0;
    } else {
        rc = -1;
    }
    sbuf2flush(sb);
    return rc;
}
/* dump dta contents of bdb_handle to stream sb */
int bdb_dumpdta(bdb_state_type *bdb_state, SBUF2 *sb, int *bdberr)
{
    int fndrrn, fndlen, ii, rc, rrn;
    unsigned char *fnddta;
    static char *hexchars = "0123456789ABCDEF";
    unsigned char u;

    DBC *dbcp;
    DBT key, data;
    size_t retklen, retdlen;
    unsigned char *retkey;
    unsigned char *retdata;
    void *p;

    *bdberr = BDBERR_NOERROR;

    memset(&key, 0, sizeof(key));
    memset(&data, 0, sizeof(data));

    if ((data.data = mymalloc(OLD_BUFFER_LENGTH)) == NULL) {
        *bdberr = BDBERR_MISC;
        return -1;
    }

    data.ulen = OLD_BUFFER_LENGTH;
    data.flags = DB_DBT_USERMEM;

    /* Acquire a cursor for the database. */
    if ((rc = bdb_state->dbp_data[0][0]->cursor(bdb_state->dbp_data[0][0], NULL,
                                                &dbcp, 0)) != 0) {
        free(data.data);
        *bdberr = BDBERR_MISC;
        return -1;
    }

    /* start dumping at rrn 2 */
    rrn = 2;
    key.data = &rrn;
    key.size = sizeof(int);
    key.ulen = sizeof(int);

    if (bdb_state->attr->sbuftimeout)
        sbuf2settimeout(sb, 0, bdb_state->attr->sbuftimeout);

    for (;;) {
        /*
         * Acquire the next set of key/data pairs.  This code does
         * not handle single key/data pairs that won't fit in a
         * OLD_BUFFER_LENGTH size buffer, instead returning ENOMEM to
         * our caller.
         */
        if ((rc = dbcp->c_get(dbcp, &key, &data, DB_MULTIPLE_KEY | DB_NEXT)) !=
            0) {
            if (rc != DB_NOTFOUND) {
                free(data.data);
                *bdberr = BDBERR_MISC;
                return -1;
            }
            break;
        }

        for (DB_MULTIPLE_INIT(p, &data);;) {
            DB_MULTIPLE_KEY_NEXT(p, &data, retkey, retklen, retdata, retdlen);
            UNUSED(retklen);
            if (p == NULL)
                break;

            /*
               data that came back from berkeley is in
               retklen, retkey, retdlen, retdata

               now get fndrrn, fndlen, and fnddta to have the correct values
               */

            memcpy(&fndrrn, retkey, sizeof(int));

            if (fndrrn == 0 || fndrrn == 1)
                continue;

            if (bdb_state->attr->genids)
                fndlen = retdlen - (sizeof(unsigned long long));
            else
                fndlen = retdlen;

            if (bdb_state->attr->genids)
                fnddta = (void *)(((unsigned long long *)retdata) + 1);
            else
                fnddta = retdata;

            /* we dont have the proper comdb rc, but it seems meaningless here
             */
            rc = 0;

            if (sbuf2printf(sb, "%d rrn %d len %d ", rc, fndrrn, fndlen) < 0)
                goto done;

            for (ii = 0; ii < fndlen; ii++) {
                u = fnddta[ii];
                if (sbuf2putc(sb, hexchars[(u >> 4) & 0xf]) < 0)
                    goto done;

                if (sbuf2putc(sb, hexchars[u & 0xf]) < 0)
                    goto done;
            }
            sbuf2putc(sb, '\n');
        }
    }

done:

    if ((rc = dbcp->c_close(dbcp)) != 0) {
        free(data.data);
        *bdberr = BDBERR_MISC;
        return -1;
    }

    free(data.data);

    sbuf2printf(sb, "%d #done\n", 3); /* simulate FINDNEXT rc of 3 for EOF */
    sbuf2flush(sb);

    return 0;
}
