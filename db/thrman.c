/*
   Copyright 2015, 2017, Bloomberg Finance L.P.

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

/*
 * Utilities to manage all the different threads we have lying around.
 *
 */

#include <sys/socket.h>
#ifdef _IBM_SOURCE
#include <sys/thread.h>
#endif
#include <sys/types.h>
#include <sys/time.h>

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stddef.h>
#include <time.h>

#include <list.h>
#include <epochlib.h>
#include <memory_sync.h>

#include "lockmacros.h"
#include "comdb2.h"
#include "util.h"
#include "thrman.h"
#include "thdpool.h"
#include "thread_util.h"
#include "osqlrepository.h"
#include "logmsg.h"
#include "reqlog.h"
#include "str0.h"
#include "phys_rep.h"

extern struct thdpool *gbl_loadcache_thdpool;

struct thr_handle {
    pthread_t tid;

    arch_tid archtid;

    enum thrtype type;

    /* sql pool threads can specify which pool they belong to */
    sqlpool_t *sqlpool;

    /* String that says what the thread is doing.  This pointer gets
     * updated locklessly. */
    const char *where;

    /* Used for more complex where strings.  Note that we take great
     * pains to ensure that we never overwrite the final byte with anything
     * other than a zero, so that it is always safe to assume that this string
     * is \0 terminated even if doing stuff locklessly. */
    char where_buf[1024];

    /* An id string.  For appsock connections you can set the id with the
     * id command. */
    char id[128];

    /* File descriptor associated with this thread. */
    int fd;

    /* Request logger associated with this thread. */
    struct reqlogger *reqlogger;

    /* Origin of the request. */
    char corigin[80];

    enum thrsubtype subtype;

    LINKC_T(struct thr_handle) linkv;
};

static pthread_key_t thrman_key;

int gbl_thrman_trace = 0;

/* This is useful to lots of things */
pthread_attr_t gbl_pthread_attr_detached;

/* All these are protected by a mutex */
static pthread_mutex_t mutex;
static pthread_cond_t cond;
static LISTC_T(struct thr_handle) thr_list;
static int thr_type_counts[THRTYPE_MAX] = {0};

static void thrman_destructor(void *param);

void thrman_init(void)
{
    Pthread_mutex_init(&mutex, NULL);
    Pthread_key_create(&thrman_key, thrman_destructor);
    Pthread_cond_init(&cond, NULL);
    Pthread_attr_init(&gbl_pthread_attr_detached);
    Pthread_attr_setdetachstate(&gbl_pthread_attr_detached, PTHREAD_CREATE_DETACHED);
    /* 4 meg stack - there should be a better solution for this..
       some huge sql queries (it's happened) blow out stack during the parsing
       phase. */
    Pthread_attr_setstacksize(&gbl_pthread_attr_detached, 4 * 1024 * 1024);

    listc_init(&thr_list, offsetof(struct thr_handle, linkv));
}

/* Register the current thread with the thead manager.  There's no need to
 * de-register later on, but you can if you like.  Returns a handle by which
 * the current thread will be known. */
struct thr_handle *thrman_register(enum thrtype type)
{
    struct thr_handle *thr;
    if (type < 0 || type >= THRTYPE_MAX) {
        logmsg(LOGMSG_ERROR, "thrman_register: type=%d out of range\n", type);
        return NULL;
    }

    thr = pthread_getspecific(thrman_key);
    if (thr) {
        char buf[1024];
        logmsg(LOGMSG_FATAL, "thrman_register(%s): thread already registered: %s\n",
                thrman_type2a(type), thrman_describe(thr, buf, sizeof(buf)));
        abort();
    }

    thr = calloc(1, sizeof(struct thr_handle));
    if (!thr) {
        logmsg(LOGMSG_ERROR, "thrman_register: out of memory\n");
        return NULL;
    }

    thr->tid = pthread_self();
    thr->archtid = getarchtid();
    thr->type = type;
    thr->fd = -1;

    Pthread_setspecific(thrman_key, thr);
    Pthread_mutex_lock(&mutex);
    listc_abl(&thr_list, thr);
    thr_type_counts[type]++;
    if (gbl_thrman_trace) {
        char buf[1024];
       logmsg(LOGMSG_ERROR, "thrman_register: %s\n", thrman_describe(thr, buf, sizeof(buf)));
    }
    Pthread_cond_broadcast(&cond);
    Pthread_mutex_unlock(&mutex);

    return thr;
}

enum thrtype thrman_get_type(struct thr_handle *thr)
{
    if (thr)
        return thr->type;
    else
        return THRTYPE_UNKNOWN;
}

enum thrsubtype thrman_get_subtype(struct thr_handle *thr)
{
    if (thr)
        return thr->subtype;
    else
        return THRSUBTYPE_UNKNOWN;
}

/* Change the type of the given thread. */
void thrman_change_type(struct thr_handle *thr, enum thrtype newtype)
{
    enum thrtype oldtype;

    oldtype = thr->type;

    Pthread_mutex_lock(&mutex);
    thr_type_counts[thr->type]--;
    thr->type = newtype;
    thr_type_counts[thr->type]++;
    if (gbl_thrman_trace) {
        char buf[1024];
       logmsg(LOGMSG_USER, "thrman_change_type: from %s -> %s\n", thrman_type2a(oldtype),
               thrman_describe(thr, buf, sizeof(buf)));
    }
    Pthread_cond_broadcast(&cond);
    Pthread_mutex_unlock(&mutex);
}

/* Called from the thrman_key destructor when the thread exits, or manually
 * via thrman_unregister(). */
static void thrman_destructor(void *param)
{
    struct thr_handle *thr = param;

    if (!thr) {
        logmsg(LOGMSG_ERROR, "thrman_destructor: thr==NULL\n");
        return;
    }

    Pthread_mutex_lock(&mutex);
    listc_rfl(&thr_list, thr);
    thr_type_counts[thr->type]--;
    if (gbl_thrman_trace) {
        char buf[1024];
        logmsg(LOGMSG_USER, "thrman_destructor: %s\n",
               thrman_describe(thr, buf, sizeof(buf)));
    }
    Pthread_cond_broadcast(&cond);
    Pthread_mutex_unlock(&mutex);

    if (thr->reqlogger) {
        reqlog_free(thr->reqlogger);
    }

    free(thr);
}

/* Get the current thread's handle.  Returns NULL if it is not registered. */
struct thr_handle *thrman_self(void)
{
    struct thr_handle *thr;
    thr = pthread_getspecific(thrman_key);
    return thr;
}

/* Unregister this thread, if it was registered. */
void thrman_unregister(void)
{
    struct thr_handle *thr;
    thr = thrman_self();
    if (thr) {
        Pthread_setspecific(thrman_key, NULL);
        thrman_destructor(thr);
    }
}

void thrman_origin(struct thr_handle *thr, const char *origin)
{
    if (thr) {
        if (origin) {
            strncpy0(thr->corigin, origin, sizeof(thr->corigin));
            thr->corigin[sizeof(thr->corigin) - 1] = 0;
        } else
            thr->corigin[0] = 0;
    }
}

/* Sets description of where the thread is.  The passed in pointer should
 * be a string literal; it may be referenced at any time in the future. */
void thrman_where(struct thr_handle *thr, const char *where)
{
    if (thr)
        thr->where = where;
}

/* Like thrman_where+printf.  You can use it when your where string isn't as
 * simple as a string literal. */
void thrman_wheref(struct thr_handle *thr, const char *fmt, ...)
{
    if (thr) {
        va_list args;
        va_start(args, fmt);
        /* Note that we use sizeof(buf) - 1.  This is to ensure that the
         * final byte in the buffer stays zero.  The string will always be
         * null terminated. */
        vsnprintf(thr->where_buf, sizeof(thr->where_buf) - 1, fmt, args);
        thr->where = thr->where_buf;
        va_end(args);
    }
}

/* Set the sql pool associated with this thread. */
void thrman_set_sqlpool(struct thr_handle *thr, sqlpool_t *sqlpool)
{
    thr->sqlpool = sqlpool;
}

void thrman_setid(struct thr_handle *thr, const char *idstr)
{
    /* Don't touch last byte in string so it will always be a zero */
    strncpy(thr->id, idstr, sizeof(thr->id) - 1);
}

/* Set associated file descriptor.  -1 indicates no file descriptor. */
void thrman_setfd(struct thr_handle *thr, int fd) { thr->fd = fd; }

void thrman_set_subtype(struct thr_handle *thr, enum thrsubtype subtype)
{
    thr->subtype = subtype;
}

const char *thrman_type2a(enum thrtype type)
{
    switch (type) {
    case THRTYPE_UNKNOWN:
        return "unknown";
    case THRTYPE_OSQL:
        return "blocksql";
    case THRTYPE_APPSOCK:
        return "appsock";
    case THRTYPE_APPSOCK_POOL:
        return "appsock-pool";
    case THRTYPE_APPSOCK_SQL:
        return "appsock-pool-sql";
    case THRTYPE_SQLPOOL:
        return "sqlpool";
    case THRTYPE_SQLENGINEPOOL:
        return "sql-engine-pool";
    case THRTYPE_SQL:
        return "sql";
    case THRTYPE_REQ:
        return "req";
    case THRTYPE_CONSUMER:
        return "consumer";
    case THRTYPE_PURGEBLKSEQ:
        return "purge-old-blkseq";
    case THRTYPE_PREFAULT:
        return "prefault";
    case THRTYPE_VERIFY:
        return "verify";
    case THRTYPE_SCHEMACHANGE:
        return "schema-change";
    case THRTYPE_ANALYZE:
        return "analyze";
    case THRTYPE_PUSHLOG:
        return "pushlogs";
    case THRTYPE_BBIPC_WAITFT:
        return "bbipc-waitft";
    case THRTYPE_LOGDELHOLD:
        return "log-del-hold";
    case THRTYPE_COORDINATOR:
        return "transaction-coordinator";
    case THRTYPE_MTRAP:
        return "mtrap";
    case THRTYPE_QSTAT:
        return "queue-stat";
    case THRTYPE_PURGEFILES:
        return "purge-old-files";
    case THRTYPE_TRIGGER:
        return "lua-trigger";
    case THRTYPE_PGLOGS_ASOF:
        return "pglogs-asof";
    case THRTYPE_CLEANEXIT:
        return "cleanexit";
    case THRTYPE_GENERIC:
        return "generic";
    default:
        return "??";
    }
}

/* Populate the buffer with a description of the thread.  Returns a pointer
 * to the buffer. */
char *thrman_describe(struct thr_handle *thr, char *buf, size_t szbuf)
{
    if (!thr)
        snprintf(buf, szbuf, "thr==NULL");
    else {
        const char *where = thr->where;
        int fd = thr->fd;
        int pos = 0;

        SNPRINTF(buf, szbuf, pos, "tid %" PRIxPTR ":%s", (intptr_t)thr->archtid,
                 thrman_type2a(thr->type));

        if (fd >= 0) {
            /* Get the IP address of this socket connection.  We assume that
             * fd is a socket and not something else, and we assume that it
             * is still valid.  Because we're lockless we may be wrong (very
             * unlikely, but possible).  The worst that can happen is we'll
             * get an error, or wrong information. */
            struct sockaddr_in peeraddr;
            socklen_t len = sizeof(peeraddr);
            char addrstr[64];
            if (getpeername(fd, (struct sockaddr *)&peeraddr, &len) < 0)
                SNPRINTF(buf, szbuf, pos, ", fd %d (getpeername:%s)", fd,
                         strerror(errno))
            else if (inet_ntop(peeraddr.sin_family, &peeraddr.sin_addr, addrstr,
                               sizeof(addrstr)) == NULL)
                SNPRINTF(buf, szbuf, pos, ", fd %d (inet_ntop:%s)", fd,
                         strerror(errno))
            else
                SNPRINTF(buf, szbuf, pos, ", fd %d (%s)", fd, addrstr)
        }
        if (thr->corigin[0])
            SNPRINTF(buf, szbuf, pos, ", %s", thr->corigin)
        if (thr->id[0])
            SNPRINTF(buf, szbuf, pos, ", %s", thr->id)
        if (where != NULL)
            SNPRINTF(buf, szbuf, pos, ": %s", where)
    }

done:
    return buf;
}

static void thrman_dump_ll(void)
{
    struct thr_handle *thr;
    struct thr_handle *temp;
    int count;

    logmsg(LOGMSG_USER, "%d registered threads running:-\n",
           listc_size(&thr_list));
    count = 0;
    LISTC_FOR_EACH_SAFE(&thr_list, thr, temp, linkv)
    {
        char buf[1024];
        logmsg(LOGMSG_USER, "  %2d) %s\n", count,
               thrman_describe(thr, buf, sizeof(buf)));
        count++;
    }
    logmsg(LOGMSG_USER, "------\n");
}

/* Dump all active threads */
void thrman_dump(void)
{
    Pthread_mutex_lock(&mutex);
    thrman_dump_ll();
    Pthread_mutex_unlock(&mutex);
}

/* stop sql connections.  this is needed to stop blocked
   persistent connections. */
void thrman_stop_sql_connections(void)
{
    struct thr_handle *thr;
    struct thr_handle *temp;

    Pthread_mutex_lock(&mutex);
    LISTC_FOR_EACH_SAFE(&thr_list, thr, temp, linkv)
    {
        if (thr->type == THRTYPE_SQLPOOL || thr->type == THRTYPE_SQL ||
            thr->type == THRTYPE_SQLENGINEPOOL ||
            thr->type == THRTYPE_APPSOCK_SQL)
            shutdown(thr->fd, 0);
    }
    Pthread_mutex_unlock(&mutex);
}

/* See if all threads are gone (or all but myself) */
static int thrman_check_threads_stopped_ll(void *context)
{
    int all_gone = 0;
    struct thr_handle *self = thrman_self();

    if (self)
        thr_type_counts[self->type]--;

    if (0 ==
        thr_type_counts[THRTYPE_OSQL] + thr_type_counts[THRTYPE_APPSOCK] +
            thr_type_counts[THRTYPE_APPSOCK_POOL] +
            thr_type_counts[THRTYPE_APPSOCK_SQL] +
            thr_type_counts[THRTYPE_CONSUMER] + thr_type_counts[THRTYPE_SQL] +
            thr_type_counts[THRTYPE_SQLPOOL] +
            thr_type_counts[THRTYPE_SQLENGINEPOOL] +
            thr_type_counts[THRTYPE_VERIFY] + thr_type_counts[THRTYPE_ANALYZE] +
            thr_type_counts[THRTYPE_PURGEBLKSEQ] +
            thr_type_counts[THRTYPE_PGLOGS_ASOF] +
            thr_type_counts[THRTYPE_TRIGGER])
        all_gone = 1;

    /* if we're exiting then we don't want a schema change thread running */
    if (db_is_exiting() && 0 != thr_type_counts[THRTYPE_SCHEMACHANGE])
        all_gone = 0;

    if (self)
        thr_type_counts[self->type]++;

    return all_gone;
}

/* See if all threads of a given type are gone */
static int thrman_check_threads_gone_ll(void *context)
{
    enum thrtype *ptype = context;
    enum thrtype type = *ptype;
    int target = 0;
    struct thr_handle *self = thrman_self();

    if (self && self->type == type)
        target = 1;

    if (thr_type_counts[type] == target)
        return 1;

    return 0;
}

/* Wait for some condition to happen.  The condition will be checked by the
 * passed in function, which expects to be called under lock. */
static void thrman_wait(const char *descr, int (*check_fn_ll)(void *),
                        void *context)
{
    Pthread_mutex_lock(&mutex);
    while (1) {
        struct timespec ts;
        struct timeval tp;
        int rc;

        if (check_fn_ll(context))
            break;

        gettimeofday(&tp, NULL);
        ts.tv_sec = tp.tv_sec + 1;
        ts.tv_nsec = tp.tv_usec * 1000;

        /* Wait for something to change. */
        rc = pthread_cond_timedwait(&cond, &mutex, &ts);
        if (rc == ETIMEDOUT) {
            printf("Waiting for %s\n", descr);
            thrman_dump_ll();
            sql_dump_running_statements();
        } else if (rc != 0) {
            perror_errnum("thrman_coalesce:pthread_cond_timedwait", rc);
        }
    }
    Pthread_mutex_unlock(&mutex);
}

/* Stop all database threads.  Different thread types get stopped in different
 * ways. */
void stop_threads(struct dbenv *dbenv)
{
    /* watchdog makes sure we don't get stuck trying to stop threads */
    LOCK(&stop_thds_time_lk)
    {
        gbl_stop_thds_time = comdb2_time_epoch();
    }
    UNLOCK(&stop_thds_time_lk);

    block_new_requests(dbenv);
    dbenv->no_more_sql_connections = 1;

    stop_physrep_threads();

    if (gbl_appsock_thdpool)
        thdpool_stop(gbl_appsock_thdpool);
    stop_all_sql_pools();
    if (gbl_osqlpfault_thdpool)
        thdpool_stop(gbl_osqlpfault_thdpool);
    if (gbl_udppfault_thdpool)
        thdpool_stop(gbl_udppfault_thdpool);
    if (gbl_pgcompact_thdpool)
        thdpool_stop(gbl_pgcompact_thdpool);
    if (gbl_loadcache_thdpool)
        thdpool_stop(gbl_loadcache_thdpool);

    /* Membar ensures that all other threads will now see that db is stopping */
    MEMORY_SYNC;

    /* interrupt connections (they may have no timeout) */
    thrman_stop_sql_connections();

    /* Wake up the queue consumer threads */
    dbqueuedb_coalesce(dbenv);

    /* Wait until the regular request threads are idle with no queue.  Note
     * that they still continue regardless. */
    thd_coalesce(dbenv);

    /* Wait for all registered threads to exit.  This takes care of appsock
     * and queue consumers. */
    thrman_wait("threads to stop", thrman_check_threads_stopped_ll, NULL);

    LOCK(&stop_thds_time_lk) { gbl_stop_thds_time = 0; }
    UNLOCK(&stop_thds_time_lk);
}

void resume_threads(struct dbenv *dbenv)
{
    if (gbl_appsock_thdpool)
        thdpool_resume(gbl_appsock_thdpool);
    resume_all_sql_pools();
    if (gbl_osqlpfault_thdpool)
        thdpool_resume(gbl_osqlpfault_thdpool);
    allow_new_requests(dbenv);
    dbenv->no_more_sql_connections = 0;
    MEMORY_SYNC;
    if (!dbenv->purge_old_blkseq_is_running ||
        !dbenv->purge_old_files_is_running)
        create_old_blkseq_thread(dbenv);
    /*watchdog_enable();*/
}

/* Wait for all threads of a given type to exit */
int thrman_wait_type_exit(enum thrtype type)
{
    char descr[100];
    snprintf(descr, sizeof(descr), "all %s threads to exit",
             thrman_type2a(type));
    thrman_wait(descr, thrman_check_threads_gone_ll, &type);
    return 0;
}

/* Count the number of threads of the given type already running. */
int thrman_count_type(enum thrtype type)
{
    int count = 0;
    if (type >= 0 && type < THRTYPE_MAX) {
        Pthread_mutex_lock(&mutex);
        count = thr_type_counts[type];
        Pthread_mutex_unlock(&mutex);
    }
    return count;
}

/* Get the request logging object associated with this thread; create one if
 * we need to. */
struct reqlogger *thrman_get_reqlogger(struct thr_handle *thr)
{
    if (thr) {
        if (!thr->reqlogger)
            thr->reqlogger = reqlog_alloc();
        reqlog_reset(thr->reqlogger);
        return thr->reqlogger;
    } else {
        return NULL;
    }
}

const char *thrman_get_where(struct thr_handle *thr)
{
    return thr->where;
}
