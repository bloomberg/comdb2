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

/*
 * Socket pool library.  This is so that we can reuse sockets to comdb2
 * databases automatically and save SQL.
 *
 * Sockets are donated to the pool with a type string.  The type string
 * should follow a format suitable for the application e.g. for comdb2 we
 * could have comdb2/dbname/sql/73/readonly for a SET TRANSACTION READONLY
 * sql handle to a comdb2 on sundev1.
 */

#include <sockpool.h>
#include <sockpool_p.h>

#include <sys/param.h>
#include <sys/socket.h>
#include <sys/socketvar.h>
#include <sys/types.h>
#include <sys/un.h>
#include <arpa/inet.h>

#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>

#include <plhash_glue.h>
#include <list.h>
#include <pool.h>
#include <passfd.h>
#include <syslog.h>
#include <sys_wrap.h>

//#define SOCKET_POOL_DEBUG

#ifdef SOCKET_POOL_DEBUG
#define DBG(x) printf x
#else
#define DBG(x)
#endif

pthread_mutex_t sockpool_lk = PTHREAD_MUTEX_INITIALIZER;

/* This bit gets set by sockpool.tsk when it starts up to indicate that it is
 * available to receive sockets. */
#define SOCKPOOL_ENABLED() 1

static int comdb2_time_epoch_sp()
{
    return time(0);
}

struct itemtype;

/* Some stats.  Everybody loves stats. */
struct stats {
    unsigned n_donated;
    unsigned n_reused;
    unsigned n_duped;
    unsigned n_trimmed;
    unsigned n_timeouts;
    unsigned peak_n_held;
};

/* We keep the item struct in our hash table even after the file descriptor
 * has been reused with an fd of -1.  This means we don't need to reallocate
 * memory when we recyle the same thing again (not a huge concern as this is
 * always gonna be faster than a socket open) and it means that we can keep
 * stats on a per item basis, which might be useful.
 *
 * CAREFUL we don't calloc or bzero this on allocation - if you add new fields
 * pay attention to their initialisation. */
struct item {
    int fd;            /* can be -1 */
    int timeout_secs;  /* if >0 then this socket times out */
    int dbnum;         /* if >0 this is associated with a database number */
    int flags;         /* passed in at donation time */
    int donation_time; /* epoch time of donation */
    fd_destructor_fn destructor;
    void *destructor_arg;

    /* Link in list of items for the item type. */
    LINKC_T(struct item) typestr_items_linkv;

    /* Link in lru list of all items. */
    LINKC_T(struct item) lru_linkv;

    /* Chain back to parent. */
    struct itemtype *type;
};

/* For each unique typestr we keep one of these at the head of the list. */
struct itemtype {
    /* Items, with most recently donated at the bottom of the list. */
    LISTC_T(struct item) item_list;
    struct stats stats;
    struct stats dbgstats; /* for dbglog */
    char typestr[1]; /* must be last thing in struct; this is hash table key */
};

static hash_t *hash = NULL;
static pool_t *pool = NULL;
static struct stats stats;
static const struct stats empty_stats = {0};

/* List of active file descriptors; bottom of list is most recently added */
static LISTC_T(struct item) lru_list;

static unsigned max_active_fds = 16;

static unsigned max_fds_per_typestr = 10;

/* Default on by now.  Whether or not comdb2 uses this will be controlled
 * at the comdb2 api level. */
static unsigned enabled = 1;

/* Per task on/off switch for global socket pool.  We also use
 * SOCKPOOL_ENABLED() which is the machine wide switch. */
static unsigned sockpool_enabled = 1;

/* The global socket pool file descriptor.  For now we will use a single
 * per task connection.  If this doesn't scale we can be cleverer (use socket
 * pooling maybe!).  This is protected by our bb_mutex of course. */
static int sockpool_fd = -1;

/* to be called ONLY from pekludgl_fork() */
void socket_pool_reset_connection_(void)
{
    if (sockpool_fd != -1)
        Close(sockpool_fd);

    sockpool_fd = -1;
}

void socket_pool_set_max_fds(unsigned new_max) { max_active_fds = new_max; }

void socket_pool_set_max_fds_per_typestr(unsigned new_max)
{
    max_fds_per_typestr = new_max;
}

void socket_pool_enable(void)
{
    enabled = 1;
    syslog(LOG_INFO, "Socket pool enabled\n");
}

void socket_pool_enable_(void) { socket_pool_enable(); }

void socket_pool_disable(void)
{
    enabled = 0;
    syslog(LOG_INFO, "Socket pool disabled\n");
    socket_pool_free_all();
}

void socket_pool_disable_(void) { socket_pool_disable(); }

void sockpool_enable(void) { sockpool_enabled = 1; }

void sockpool_enable_(void) { sockpool_enable(); }

void sockpool_disable(void) { sockpool_enabled = 0; }

void sockpool_disable_(void) { sockpool_disable(); }

/* Because we maintain a long lived connection to sockpool we run the risk
 * that sockpool may crash and that we will receive SIGPIPE the next time we
 * write to this.  Sadly neither sun nor ibm support the ability to tell the
 * O/S that it's ok, for this fd we can deal with a broken pipe, no need to
 * put me against the wall and shoot me.  So instead we do bonkers stuff with
 * signals.  Before writing we block sigpipe, and after writing we catch any
 * SIGPIPEs that were raised and then unblock it.  This works because SIGPIPE
 * is a synchronous signal and so it should be delivered to the thread which
 * caused it (i.e. it doesn't matter that other threads may not be blocking
 * it).  On Solaris 10 and AIX this seems to work fine.  However Solaris 9
 * was wrong in this respect - it would merrily deliver the SIGPIPE to random
 * other threads.  We're phasing out Solaris 9 so I'm just going to ignore
 * this problem in the interests of sanity.  I don't think there's any other
 * library level solution.
 */
static void hold_sigpipe_ll(int on)
{
    static int sset_init = 0;
    static sigset_t sset, oset;
    int rc;
    if (!sset_init) {
        sigemptyset(&sset);
        sigaddset(&sset, SIGPIPE);
        sset_init = 1;
    }
    if (on) {
        /* block SIGPIPE */
        rc = pthread_sigmask(SIG_BLOCK, &sset, &oset);
        if (rc != 0) {
            fprintf(stderr, "%s:pthread_sigmask(%d): %d %s\n", __func__, on, rc,
                    strerror(rc));
        }
    } else if (!sigismember(&oset, SIGPIPE)) {
        /* unblock SIGPIPE (if it wasn't blocked before).
         * Also catch any pending sigpipes. */
#       ifndef __APPLE__
        struct timespec timeout = {0, 0};
        if (sigtimedwait(&sset, NULL, &timeout) == -1 && errno != EAGAIN) {
            fprintf(stderr, "%s:sigtimedwait: %d %s\n", __func__, errno,
                    strerror(errno));
        }
#       endif
        rc = pthread_sigmask(SIG_SETMASK, &oset, NULL);
        if (rc != 0) {
            fprintf(stderr, "%s:pthread_sigmask(%d): %d %s\n", __func__, on, rc,
                    strerror(rc));
        }
    }
}

static int open_sockpool_ll(void)
{

    int fd;
    const char *ptr;
    size_t bytesleft;
    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd == -1) {
        fprintf(stderr, "%s:socket: %d %s\n", __func__, errno, strerror(errno));
        return -1;
    }

    struct sockaddr_un addr = {.sun_family = AF_UNIX};
    strncpy(addr.sun_path, SOCKPOOL_SOCKET_NAME, sizeof(addr.sun_path) - 1);

    if (connect(fd, (const struct sockaddr *)&addr, sizeof(addr)) == -1) {
        fprintf(stderr, "%s:connect(%s): %d %s\n", __func__,
                SOCKPOOL_SOCKET_NAME, errno, strerror(errno));
        Close(fd);
        return -1;
    }

    /* Connected - write hello message */
    struct sockpool_hello hello = {
        .protocol_version = 0, .pid = getpid(), .slot = 0};
    memcpy(hello.magic, "SQLP", 4);

    ptr = (const char *)&hello;
    bytesleft = sizeof(hello);
    while (bytesleft > 0) {
        ssize_t nbytes;
        nbytes = write(fd, ptr, bytesleft);
        if (nbytes == -1) {
            fprintf(stderr, "%s:error writing hello: %d %s\n", __func__, errno,
                    strerror(errno));
            Close(fd);
            return -1;
        } else if (nbytes == 0) {
            fprintf(stderr, "%s:unexpected eof writing hello\n", __func__);
            Close(fd);
            return -1;
        }
        bytesleft -= nbytes;
        ptr += nbytes;
    }

    return fd;
}

static void default_destructor(enum socket_pool_event event,
                               const char *typestr, int fd, int dbnum,
                               int flags, int ttl, void *voidarg)
{
    if (event != SOCKET_POOL_EVENT_DONATE) {

        if ((flags & SOCKET_POOL_DONATE_GLOBAL) && (ttl > 0) &&
            (event == SOCKET_POOL_EVENT_CLOSE ||
             event == SOCKET_POOL_EVENT_TRIM ||
             event == SOCKET_POOL_EVENT_DUP ||
             event == SOCKET_POOL_EVENT_ENDEVENT) &&
            sockpool_enabled && SOCKPOOL_ENABLED()) {
            /* Donate this socket to the global socket pool.  We know that the
             * mutex is held. */
            hold_sigpipe_ll(1);
            if (sockpool_fd == -1) {
                sockpool_fd = open_sockpool_ll();
                if (sockpool_fd != -1) {
                    /*fdtrack_ignore_fd(__func__, sockpool_fd);*/
                }
            }
            if (sockpool_fd != -1) {
                int rc;
                struct sockpool_msg_vers0 msg = {
                    .request = SOCKPOOL_DONATE, .dbnum = dbnum, .timeout = ttl};
                strncpy(msg.typestr, typestr, sizeof(msg.typestr) - 1);
                msg.typestr[sizeof(msg.typestr) - 1] = 0;

                errno = 0;
                rc = send_fd(sockpool_fd, &msg, sizeof(msg), fd);
                if (rc != PASSFD_SUCCESS) {
                    fprintf(stderr, "%s: send_fd rc %d errno %d %s\n", __func__,
                            rc, errno, strerror(errno));
                    /*fdtrack_end_ignore_fd(__func__, sockpool_fd);*/
                    Close(sockpool_fd);
                    sockpool_fd = -1;
                }
                DBG(("%s: donate fd %d for %s to sockpool with ttl %d dbnum %d "
                     "rc %d\n",
                     __func__, fd, typestr, ttl, dbnum, rc));
            }
            hold_sigpipe_ll(0);
        }

        /* Close the local file descriptor regardless of whether or not it
         * was donated. */
        DBG(("%s: close fd %d for %s\n", __func__, fd, typestr));
        if (Close(fd) == -1) {
            fprintf(stderr, "%s: close error for '%s' fd %d: %d %s\n", __func__,
                    typestr, fd, errno, strerror(errno));
        }
    }
}

static void destroy_item_ll(enum socket_pool_event event, struct item *item)
{
    int ttl =
        item->timeout_secs - (comdb2_time_epoch_sp() - item->donation_time);
    if (ttl < 0)
        ttl = 0;
    DBG(("%s: event %d for %s fd %d\n", __func__, event, item->type->typestr,
         item->fd));
    item->destructor(event, item->type->typestr, item->fd, item->dbnum,
                     item->flags, ttl, item->destructor_arg);
}

static void socket_pool_trim_ll(unsigned max, enum socket_pool_event event)
{
    if (hash && lru_list.count > max) {
        struct item *tmpp, *item;
        /* The lru list has the most recent donations at its end, so this
         * way we free the oldest sockets first. */
        LISTC_FOR_EACH_SAFE(&lru_list, item, tmpp, lru_linkv)
        {
            DBG(("%s: closing fd %d for %s\n", __func__, item->fd,
                 item->type->typestr));
            destroy_item_ll(event, item);
            item->type->stats.n_trimmed++;
            stats.n_trimmed++;
            listc_rfl(&lru_list, item);
            listc_rfl(&item->type->item_list, item);
            pool_relablk(pool, item);
            if (lru_list.count <= max) {
                break;
            }
        }
    }
}

/* Close all sockets in the pool. */
void socket_pool_close_all(void)
{

    Pthread_mutex_lock(&sockpool_lk);
    if (hash) {
        socket_pool_trim_ll(0, SOCKET_POOL_EVENT_CLOSE);
    }
    Pthread_mutex_unlock(&sockpool_lk);
}

void socket_pool_close_all_(void) { socket_pool_close_all(); }

static int socket_pool_free_callback(void *obj, void *voidarg)
{
    free(obj);
    return 0;
}

void socket_pool_end_event_(void) { socket_pool_end_event(); }

void socket_pool_end_event(void)
{
    Pthread_mutex_lock(&sockpool_lk);
    if (hash) {
        socket_pool_trim_ll(0, SOCKET_POOL_EVENT_ENDEVENT);
    }
    Pthread_mutex_unlock(&sockpool_lk);
}

/* Close all pooled sockets and free all memory used by the pool. */
void socket_pool_free_all(void)
{
    Pthread_mutex_lock(&sockpool_lk);
    if (hash) {
        socket_pool_trim_ll(0, SOCKET_POOL_EVENT_CLOSE);
        hash_for(hash, socket_pool_free_callback, NULL);
        hash_free(hash);
        pool_free(pool);
        hash = NULL;
        pool = NULL;
    }
    Pthread_mutex_unlock(&sockpool_lk);
}

void socket_pool_free_all_(void) { socket_pool_free_all(); }

/* Check for sockets that have timed out and close them */
void socket_pool_timeout(void)
{
    Pthread_mutex_lock(&sockpool_lk);
    if (hash) {
        int now;
        struct item *tmpp, *item;
        now = comdb2_time_epoch_sp();
        LISTC_FOR_EACH_SAFE(&lru_list, item, tmpp, lru_linkv)
        {
            if (item->timeout_secs > 0 &&
                now >= item->donation_time + item->timeout_secs) {
                DBG(("%s: closing fd %d for %s\n", __func__, item->fd,
                     item->type->typestr));
                destroy_item_ll(SOCKET_POOL_EVENT_TIMEOUT, item);
                item->type->stats.n_timeouts++;
                stats.n_timeouts++;
                listc_rfl(&lru_list, item);
                listc_rfl(&item->type->item_list, item);
                pool_relablk(pool, item);
            }
        }
    }
    Pthread_mutex_unlock(&sockpool_lk);
}

void socket_pool_timeout_(void) { socket_pool_timeout(); }

struct stats_args {
    int all;
    int reset;
    int syslog;
    FILE *fh;
};


static int socket_pool_stats_callback(void *obj, void *voidarg)
{
    struct stats_args *args = voidarg;
    struct itemtype *type = obj;
    if (args->all ||
        memcmp(&type->stats, &empty_stats, sizeof(struct stats)) != 0) {
        if (args->syslog) {
            syslog(LOG_INFO, "%-32s [%2d/%2u] %5u, %5u (%u, %u, %u)\n",
                    type->typestr, listc_size(&type->item_list),
                    type->stats.peak_n_held, type->stats.n_donated,
                    type->stats.n_reused, type->stats.n_duped,
                    type->stats.n_trimmed, type->stats.n_timeouts);
        } else {
            fprintf(args->fh, "%-32s [%2d/%2u] %5u, %5u (%u, %u, %u)\n",
                    type->typestr, listc_size(&type->item_list),
                    type->stats.peak_n_held, type->stats.n_donated,
                    type->stats.n_reused, type->stats.n_duped,
                    type->stats.n_trimmed, type->stats.n_timeouts);
        }
        if (args->reset) {
            bzero(&type->stats, sizeof(type->stats));
        }
    }
    return 0;
}

/* Write stats to dbglog and reset all stats */
void socket_pool_dump_stats(FILE *fh, int reset, int all)
{
    socket_pool_dump_stats_ex(fh, reset, all, 1);
}

void socket_pool_dump_stats_syslog(int reset, int all)
{
    syslog(LOG_INFO, "Socket pool stats, enabled=%d, sockpool enabled=%d\n", enabled,
            sockpool_enabled);
    Pthread_mutex_lock(&sockpool_lk);
    if (hash) {
        struct stats_args args = {all, reset, 1, NULL};
        hash_for(hash, socket_pool_stats_callback, &args);
        syslog(LOG_INFO, "%-32s [%2d/%2u] %5u, %5u (%u, %u, %u)\n", "Global stats:",
                listc_size(&lru_list), stats.peak_n_held, stats.n_donated,
                stats.n_reused, stats.n_duped, stats.n_trimmed,
                stats.n_timeouts);
        syslog(LOG_INFO, "Counters are: [fds held/peak held] num donated, num "
                    "reused (num duped, trimmed, timed out)\n");

    } else {
        syslog(LOG_INFO, "Socket pool unused\n");
    }
    Pthread_mutex_unlock(&sockpool_lk);
}

void socket_pool_dump_stats_ex(FILE *fh, int reset, int all,
                               int incl_hash_stats)
{
    fprintf(fh, "Socket pool stats, enabled=%d, sockpool enabled=%d\n", enabled,
            sockpool_enabled);
    Pthread_mutex_lock(&sockpool_lk);
    if (hash) {
        struct stats_args args = {all, reset, 0, fh};
        hash_for(hash, socket_pool_stats_callback, &args);
        fprintf(fh, "%-32s [%2d/%2u] %5u, %5u (%u, %u, %u)\n", "Global stats:",
                listc_size(&lru_list), stats.peak_n_held, stats.n_donated,
                stats.n_reused, stats.n_duped, stats.n_trimmed,
                stats.n_timeouts);
        fprintf(fh, "Counters are: [fds held/peak held] num donated, num "
                    "reused (num duped, trimmed, timed out)\n");
        /* hash_dump uses printf... */
        if (incl_hash_stats) {
            fprintf(fh, "socket pool hash table statistics:\n");
            hash_dump_stats(hash, fh, NULL);
        }
    } else {
        fprintf(fh, "Socket pool unused\n");
    }
    Pthread_mutex_unlock(&sockpool_lk);
}

void socket_pool_dump_stats_(const int *reset, const int *all)
{
    socket_pool_dump_stats(stdout, *reset, *all);
}

int socket_pool_get(const char *typestr)
{
    return socket_pool_get_ext(typestr, 0, 0, NULL, NULL);
}

/* Get the file descriptor of a socket matching the given type string from
 * the pool.  Returns -1 if none is available or the file descriptor on
 * success. */
static int
socket_pool_get_ext_ll(const char *typestr, int dbnum, int flags,
                       socket_pool_try_global_callback_t try_global_callback,
                       void *context, int *hint)
{
    int fd = -1;
    if (enabled) {
        Pthread_mutex_lock(&sockpool_lk);
        if (hash) {
            struct item *fnd_item;
            struct itemtype *fnd_type;
            fnd_type = hash_find(hash, typestr);
            /* Try least recently donated items first. */
            while (fnd_type && fd == -1 &&
                   (fnd_item = listc_rbl(&fnd_type->item_list)) != NULL) {
                if (fnd_item->timeout_secs > 0 &&
                    comdb2_time_epoch_sp() >=
                        fnd_item->timeout_secs + fnd_item->donation_time) {
                    /* Socket timed out so is unusable.  close it. */
                    DBG(("%s: fd %d timed out for %s\n", __func__, fnd_item->fd,
                         fnd_type->typestr));
                    DBG(("fnd_item->timeout_secs=%d fnd_item->donation_time=%d "
                         "now=%d\n",
                         fnd_item->timeout_secs, fnd_item->donation_time,
                         comdb2_time_epoch_sp()));
                    destroy_item_ll(SOCKET_POOL_EVENT_TIMEOUT, fnd_item);
                    fnd_type->stats.n_timeouts++;
                    stats.n_timeouts++;
                } else {
                    fd = fnd_item->fd;
                    destroy_item_ll(SOCKET_POOL_EVENT_DONATE, fnd_item);
                    fnd_type->stats.n_reused++;
                    stats.n_reused++;
                    DBG(("%s: fd %d for %s\n", __func__, fd,
                         fnd_type->typestr));
                }
                listc_rfl(&lru_list, fnd_item);
                pool_relablk(pool, fnd_item);
            }
        }
        Pthread_mutex_unlock(&sockpool_lk);
    }
    /* If we couldn't get this socket locally it may be available from the
     * global socket pool. */
    if (fd == -1 && (flags & SOCKET_POOL_GET_GLOBAL) && sockpool_enabled &&
        SOCKPOOL_ENABLED()) {
        if (try_global_callback != NULL) {
            if (!try_global_callback(typestr, dbnum, flags, context)) {
                return fd;
            }
        }

        Pthread_mutex_lock(&sockpool_lk);
        hold_sigpipe_ll(1);
        if (sockpool_fd == -1) {
            sockpool_fd = open_sockpool_ll();
            if (sockpool_fd != -1) {
                /*fdtrack_ignore_fd(__func__, sockpool_fd);*/
            }
        }
        if (sockpool_fd != -1) {
            int rc;
            struct sockpool_msg_vers0 msg = {.request = SOCKPOOL_REQUEST,
                                             .dbnum = dbnum};

            strncpy(msg.typestr, typestr, sizeof(msg.typestr) - 1);

            /* Please may I have a file descriptor */
            errno = 0;
            rc = send_fd(sockpool_fd, &msg, sizeof(msg), -1);
            if (rc != PASSFD_SUCCESS) {
                fprintf(stderr, "%s: send_fd rc %d errno %d %s\n", __func__, rc,
                        errno, strerror(errno));
                /*fdtrack_end_ignore_fd(__func__, sockpool_fd);*/
                Close(sockpool_fd);
                sockpool_fd = -1;
                fd = -1;
            } else {
                /* Read reply from server.  It can legitimately not send
                 * us a file descriptor. */
                errno = 0;
                rc = recv_fd(sockpool_fd, &msg, sizeof(msg), &fd);
                if (rc != PASSFD_SUCCESS) {
                    fprintf(stderr, "%s: recv_fd rc %d errno %d %s\n", __func__,
                            rc, errno, strerror(errno));
                    /*fdtrack_end_ignore_fd(__func__, sockpool_fd);*/
                    Close(sockpool_fd);
                    sockpool_fd = -1;
                    fd = -1;
                } else {
                    /* try to extract hint if available */
                    if (fd == -1 && hint && 1) {
                        short rcvhint;
                        memcpy((char *)&rcvhint, (char *)&msg.padding[1], 2);
                        *hint = ntohs(rcvhint);
                        DBG(("%s: no socket for %s, retrieved port hint %d\n",
                             __func__, typestr, *hint));
                    }
                }
                DBG(("%s: received fd %d from sockpool for %s\n", __func__, fd,
                     typestr));
            }
        }
        hold_sigpipe_ll(0);
        Pthread_mutex_unlock(&sockpool_lk);
    }
    return fd;
}

int socket_pool_get_ext(const char *typestr, int dbnum, int flags,
                        socket_pool_try_global_callback_t try_global_callback,
                        void *context)
{
    return socket_pool_get_ext_ll(typestr, dbnum, flags, try_global_callback,
                                  context, NULL);
}

int socket_pool_get_ext_hints(
    const char *typestr, int dbnum, int flags,
    socket_pool_try_global_callback_t try_global_callback, void *context,
    int *hint)
{
    return socket_pool_get_ext_ll(typestr, dbnum, flags, try_global_callback,
                                  context, hint);
}

void socket_pool_donate(const char *typestr, int fd, int timeout_secs)
{
    socket_pool_donate_ext(typestr, fd, timeout_secs, 0, 0, NULL, NULL);
}

/* Donate a file descriptor to the pool.  If the pool is disabled or too full
 * then the file descriptor may get close()'d; in any event, this call
 * effectively transfers ownership to the pool. */
void socket_pool_donate_ext(const char *typestr, int fd, int timeout_secs,
                            int dbnum, int flags, fd_destructor_fn destructor,
                            void *destructor_arg)
{
    int pooled = 0;
    if (!destructor)
        destructor = default_destructor;
    if (enabled && !(flags & SOCKET_POOL_DONATE_NOLOCAL)) {
        Pthread_mutex_lock(&sockpool_lk);
        if (!hash) {
            hash = hash_init_str(offsetof(struct itemtype, typestr));
            if (!hash) {
                fprintf(stderr, "%s: cannot init hash table\n", __func__);
            } else {
                pool = pool_init(sizeof(struct item), 0);
                if (!pool) {
                    fprintf(stderr, "%s: cannot init pool\n", __func__);
                    hash_free(hash);
                    hash = NULL;
                }
                listc_init(&lru_list, offsetof(struct item, lru_linkv));
            }
        }
        if (hash) {
            struct item *item;
            struct itemtype *type;

            /* First find or allocate an item type head */
            type = hash_find(hash, typestr);
            if (!type) {
                int len;
                len = strlen(typestr);
                type = malloc(offsetof(struct itemtype, typestr) + len + 1);
                if (!type) {
                    fprintf(stderr, "%s(%s): malloc failed\n", __func__,
                            typestr);
                } else {
                    listc_init(&type->item_list,
                               offsetof(struct item, typestr_items_linkv));
                    bzero(&type->stats, sizeof(type->stats));
                    bzero(&type->dbgstats, sizeof(type->dbgstats));
                    memcpy(type->typestr, typestr, len + 1);
                    if (hash_add(hash, type) != 0) {
                        free(type);
                        type = NULL;
                        fprintf(stderr, "%s(%s):hash_add failed\n", __func__,
                                typestr);
                    }
                }
            }

            if (type) {
                /* If this type already has too many fds then release the
                 * oldest one (least likely to still be reusable). */
                while (max_fds_per_typestr > 0 &&
                       listc_size(&type->item_list) + 1 > max_fds_per_typestr &&
                       (item = listc_rtl(&type->item_list))) {
                    DBG(("%s: closing fd %d for %s\n", __func__, item->fd,
                         item->type->typestr));
                    destroy_item_ll(SOCKET_POOL_EVENT_DUP, item);
                    type->stats.n_duped++;
                    stats.n_duped++;
                    listc_rfl(&lru_list, item);
                    pool_relablk(pool, item);
                }

                item = pool_getablk(pool);
                if (!item) {
                    fprintf(stderr, "%s(%s): pool_getablk failed\n", __func__,
                            typestr);
                } else {
                    unsigned num_fds;
                    DBG(("%s: pooled fd %d for %s dbnum %d timeout %d\n",
                         __func__, fd, typestr, dbnum, timeout_secs));
                    listc_abl(&type->item_list, item);
                    listc_abl(&lru_list, item);
                    type->stats.n_donated++;
                    stats.n_donated++;
                    item->timeout_secs = timeout_secs;
                    item->donation_time = comdb2_time_epoch_sp();
                    item->flags = flags;
                    item->dbnum = dbnum;
                    item->destructor = destructor;
                    item->destructor_arg = destructor_arg;
                    item->fd = fd;
                    item->type = type;
                    num_fds = listc_size(&type->item_list);
                    if (num_fds > type->stats.peak_n_held) {
                        type->stats.peak_n_held = num_fds;
                    }
                    if (num_fds > stats.peak_n_held) {
                        stats.peak_n_held = num_fds;
                    }
                    DBG(("item->timeout_secs=%d item->donation_time=%d\n",
                         item->timeout_secs, item->donation_time));
                    pooled = 1;
                }
            }

            /* If we've got too many active file descriptors then go
             * and close out the old ones.  Do this after the donation since
             * we may have been on the edge but the donation may not tip us
             * over if we close an old file descriptor for this typestr. */
            if (max_active_fds > 0) {
                socket_pool_trim_ll(max_active_fds, SOCKET_POOL_EVENT_TRIM);
            }
        }
        Pthread_mutex_unlock(&sockpool_lk);
    }

    /* if it wasn't pooled then it must be closed. */
    if (!pooled) {
        Pthread_mutex_lock(&sockpool_lk);
        destructor(SOCKET_POOL_EVENT_CLOSE, typestr, fd, dbnum, flags,
                   timeout_secs, destructor_arg);
        Pthread_mutex_unlock(&sockpool_lk);
    }
}
