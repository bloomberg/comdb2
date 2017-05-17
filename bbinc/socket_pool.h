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
 *
 * These apis are threadsafe.
 *
 */

#ifndef INCLUDED_SOCKET_POOL_H
#define INCLUDED_SOCKET_POOL_H

#include <stdio.h>

#if defined __cplusplus
extern "C" {
#endif

enum socket_pool_event {
    SOCKET_POOL_EVENT_DONATE = 0,   /* socket is being donated */
    SOCKET_POOL_EVENT_CLOSE = 1,    /* socket needs to be closed for
                                       unspecified reason */
    SOCKET_POOL_EVENT_TRIM = 2,     /* socket is being discarded because the
                                       pool is being trimmed */
    SOCKET_POOL_EVENT_DUP = 3,      /* socket is being discarded because it
                                       is a duplicate of another socket with
                                       the same type string */
    SOCKET_POOL_EVENT_ENDEVENT = 4, /* socket is being discarded at the end
                                       of the event */
    SOCKET_POOL_EVENT_TIMEOUT = 5   /* socket has timed out */
};

enum socket_pool_flags {
    SOCKET_POOL_DONATE_GLOBAL = 1, /* allow this fd to be passed to the
                                      machine wide sockpool task. */
    SOCKET_POOL_GET_GLOBAL = 2,
    SOCKET_POOL_DONATE_NOLOCAL = 4 /* don't allow donation to local socket
                                      pool for this fd (goes straight to
                                      global pool if allowed, or nowhere)
                                      */
};

/* Destructor function.  This is called by the socket pool code when it
 * discards of a file descriptor for whatever reason (including donation).
 * Note that the socket pool bb mutex may or may not be held when this is
 * called so this should not call back in to socket pool apis. */
typedef void (*fd_destructor_fn)(enum socket_pool_event event,
                                 const char *typestr, int fd, int dbnum,
                                 int flags, int ttl, void *voidarg);

/* On/off.  Currently this stuff is off by default, but it will soon be on
 * by default. */
void socket_pool_enable(void);
void socket_pool_enable_(void);
void socket_pool_disable(void);
void socket_pool_disable_(void);

/* On/off for the global socket pool, sockpool.  Default is on. */
void sockpool_enable(void);
void sockpool_enable_(void);
void sockpool_disable(void);
void sockpool_disable_(void);

/* Set limit on number of fds stored - 0 means no limit. */
void socket_pool_set_max_fds(unsigned new_max);

/* Set the limit on fds per type string - 0 means no limit, default is 1. */
void socket_pool_set_max_fds_per_typestr(unsigned new_max);

/* Donate a file descriptor to the pool.  If the pool is disabled or too full
 * then the file descriptor may get close()'d; in any event, this call
 * effectively transfers ownership to the pool.
 * timeout_secs can be zero (no timeout) or the number of seconds that the
 * socket can live for before it is considered worthless.
 * If dbnum is non-zero then the socket is associated with whichever server
 * uses the db number.  Currently this is used by the global socket pool
 * sockpool.
 * If flags has SOCKET_POOL_DONATE_GLOBAL set then the socket may be passed
 * to the global socket pool sockpool rather than being closed.  It will be
 * pooled locally in preference if the local socket pool is enabled. */
void socket_pool_donate(const char *typestr, int fd, int timeout_secs);
void socket_pool_donate_ext(const char *typestr, int fd, int timeout_secs,
                            int dbnum, int flags, fd_destructor_fn destructor,
                            void *destructor_arg);

/* Get the file descriptor of a socket matching the given type string from
 * the pool.  Returns -1 if none is available or the file descriptor on
 * success.  On success the file descriptor is removed from the pool and
 * effectively owned by the caller.
 * The _ext variant allows a callback to be passed in.  If the local pool
 * is disabled or does not have a suitable fd, then the callback is called
 * to determine if we should try the global pool. */
typedef int (*socket_pool_try_global_callback_t)(const char *typestr, int dbnum,
                                                 int flags, void *context);
int socket_pool_get(const char *typestr);
int socket_pool_get_ext(const char *typestr, int dbnum, int flags,
                        socket_pool_try_global_callback_t try_global_callback,
                        void *context);
int socket_pool_get_ext_hints(
    const char *typestr, int dbnum, int flags,
    socket_pool_try_global_callback_t try_global_callback, void *context,
    int *hint);

/* Close all sockets in the pool. */
void socket_pool_close_all(void);
void socket_pool_close_all_(void);

/* Close all pooled sockets and free all memory used by the pool. */
void socket_pool_free_all(void);
void socket_pool_free_all_(void);

/* Check for sockets that have timed out and close them */
void socket_pool_timeout(void);
void socket_pool_timeout_(void);

/* Event based request processors e.g. bigs should call this after each
 * event.  Based on paulbit settings we will either close all our sockets
 * or just timeout older sockets.  Also dump dbglog stats if dbg logging
 * is enabled. */
void socket_pool_end_event(void);
void socket_pool_end_event_(void);

/* Write stats to dbglog and reset all stats.  Called by the bigs
 * in routerwrap. */
void socket_pool_dbglog_stats(void);
void socket_pool_dbglog_stats_(void);

/* Write stats to output stream and optionally reset them.  These stats
 * include the hash table's inner stats. */
void socket_pool_dump_stats_syslog(int reset, int all);
void socket_pool_dump_stats(FILE *fh, int reset, int all);
void socket_pool_dump_stats_(const int *, const int *);
void socket_pool_dump_stats_ex(FILE *fh, int reset, int all,
                               int incl_hash_stats);

void socket_pool_reset_connection_(void);

#if defined __cplusplus
}
#endif

#endif
