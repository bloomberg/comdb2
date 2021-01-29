/*
   Copyright 2021 Bloomberg Finance L.P.

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

#include <errno.h>
#include <stdlib.h>
#include <unistd.h>

#include <event2/buffer.h>
#include <event2/event.h>

#include <akbuf.h>
#include <compile_time_assert.h>
#include <locks_wrap.h>
#include <logmsg.h>
#include <net_appsock.h>
#include <sqlwriter.h>

#define KB(x)   (x * 1024)
#define MB(x)   (x * 1024 * 1024)
#define MSEC(x) (x * 1000) /* millisecond -> microsecond */

//heartbeat will tick every
static struct timeval heartbeat_time = {.tv_sec = 0, .tv_usec = MSEC(10)};

//send heartbeat if no data every (seconds)
#define min_hb_time 1

//writer will block if outstanding data hits:
#define max_buf MB(1)

//once blocked, writer drains to:
#define resume_buf KB(128)

BB_COMPILE_TIME_ASSERT(resume_max_buf, resume_buf < max_buf);

struct sql_timeout {
    void *arg;
    struct event *ev;
    timeout_fn *fn;
};

struct sqlwriter {
    struct sqlclntstate *clnt;
    struct evbuffer *wr_buf;
    struct sql_timeout *timeout;
    pthread_mutex_t wr_lock;
    struct event *flush_ev;
    struct event *heartbeat_ev;
    struct event *heartbeat_trickle_ev;
    struct event_base *wr_base;
    time_t sent_at;
    sql_pack_fn *pack_hb;
    size_t hb_sz;
    unsigned bad : 1;
    unsigned done : 1;
    unsigned flush : 1;
    unsigned released_locks : 1;
    unsigned do_timeout : 1;
    unsigned timed_out : 1;
    unsigned wr_continue : 1;
};

static void sql_enable_trickle(struct sqlwriter *writer)
{
    event_del(writer->heartbeat_ev);
    event_add(writer->heartbeat_trickle_ev, NULL);
}

static void sql_disable_trickle(struct sqlwriter *writer)
{
    event_add(writer->heartbeat_ev, &heartbeat_time);
    event_del(writer->heartbeat_trickle_ev);
}

static void sql_enable_flush(struct sqlwriter *writer)
{
    sql_disable_heartbeat(writer);
    struct timeval timeout = {.tv_sec = 1};
    event_add(writer->flush_ev, &timeout);
}

static void sql_disable_flush(struct sqlwriter *writer)
{
    writer->flush = 0;
    writer->released_locks = 0;
    writer->wr_continue = 1;
    event_del(writer->flush_ev);
}

static void sql_timeout_cb(int fd, short what, void *arg)
{
    struct sqlwriter *writer = arg;
    Pthread_mutex_lock(&writer->wr_lock);
    if (!writer->bad && !writer->done) {
        writer->do_timeout = 1;
        writer->timeout->fn(writer->timeout->arg);
    }
    event_del(writer->timeout->ev);
    Pthread_mutex_unlock(&writer->wr_lock);
}

void sql_enable_heartbeat(struct sqlwriter *writer)
{
    event_add(writer->heartbeat_ev, &heartbeat_time);
    event_del(writer->heartbeat_trickle_ev);
}

void sql_disable_heartbeat(struct sqlwriter *writer)
{
    event_del(writer->heartbeat_ev);
    event_del(writer->heartbeat_trickle_ev);
}

void sql_enable_timeout(struct sqlwriter *writer, int timeout_sec, timeout_fn *fn, void *arg)
{
    if (!writer->timeout) {
        writer->timeout = malloc(sizeof(struct sql_timeout));
        writer->timeout->ev = event_new(appsock_timer_base, -1, EV_TIMEOUT, sql_timeout_cb, writer);
    }
    writer->timeout->fn = fn;
    writer->timeout->arg = arg;
    struct timeval timeout = {.tv_sec = timeout_sec};
    event_add(writer->timeout->ev, &timeout);
}

void sql_disable_timeout(struct sqlwriter *writer)
{
    if (writer->timeout) {
        event_del(writer->timeout->ev);
    }
}

static int sql_recover_deadlock(struct sqlwriter *writer)
{
    int rc = 0;
    if (!writer->released_locks) {
        rc = recover_deadlock_evbuffer(writer->clnt);
        writer->released_locks = 1;
    }
    return rc;
}

static void sql_flush_cb(int fd, short what, void *arg)
{
    struct sqlwriter *writer = arg;
    if (what & EV_TIMEOUT) {
        sql_recover_deadlock(writer);
    }
    if (!(what & EV_WRITE)) {
        return;
    }
    int n;
    Pthread_mutex_lock(&writer->wr_lock);
    int outstanding = evbuffer_get_length(writer->wr_buf);
    if (outstanding == 0) {
        sql_disable_flush(writer);
        Pthread_mutex_unlock(&writer->wr_lock);
        return;
    }
    const int min = (writer->done || writer->flush) ? 0 : resume_buf;
    while (1) {
        if ((n = evbuffer_write(writer->wr_buf, fd)) <= 0) {
            break;
        }
        writer->sent_at = time(NULL);
        outstanding -= n;
        if (outstanding <= min) {
            break;
        }
    }
    if (n <= 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
        writer->bad = 1;
        event_del(writer->flush_ev);
    } else if (outstanding <= min) {
        sql_disable_flush(writer);
    } else if (evbuffer_get_contiguous_space(writer->wr_buf) < KB(8)) {
        evbuffer_pullup(writer->wr_buf, KB(64));
    }
    Pthread_mutex_unlock(&writer->wr_lock);
}

static int sql_flush_int(struct sqlwriter *writer)
{
    sql_enable_flush(writer);
    while (!writer->wr_continue && !writer->bad) {
        event_base_dispatch(writer->wr_base);
    }
    if (writer->wr_continue && !writer->bad) {
        if (!writer->done && !writer->timed_out) {
            sql_enable_heartbeat(writer);
        }
        return 0;
    }
    return -1;
}

int sql_flush(struct sqlwriter *writer)
{
    Pthread_mutex_lock(&writer->wr_lock);
    if (writer->bad) {
        Pthread_mutex_unlock(&writer->wr_lock);
        return -1;
    }
    writer->flush = 1;
    writer->wr_continue = 0;
    Pthread_mutex_unlock(&writer->wr_lock);
    return sql_flush_int(writer);
}

static int lock_writer_evbuffer(struct sqlwriter *writer)
{
    if (writer->do_timeout && pthread_equal(pthread_self(), appsock_timer_thd)) {
        return 0; /* maxquerytime has locked already */
    }
    Pthread_mutex_lock(&writer->wr_lock);
    return 1;
}

static void unlock_writer_evbuffer(struct sqlwriter *writer, int locked)
{
    if (locked) {
        Pthread_mutex_unlock(&writer->wr_lock);
    }
}

int sql_write(struct sqlwriter *writer, int total, sql_pack_fn *pack, void *pack_arg, int flush)
{
    const int locked = lock_writer_evbuffer(writer);
    if (writer->bad || writer->timed_out) {
        unlock_writer_evbuffer(writer, locked);
        return -1;
    }
    struct iovec v[1];
    evbuffer_reserve_space(writer->wr_buf, total, v, 1);
    v[0].iov_len = total;
    writer->done = pack(v[0].iov_base, pack_arg);
    evbuffer_commit_space(writer->wr_buf, v, 1);
    int outstanding = evbuffer_get_length(writer->wr_buf);
    if (!locked) {
        writer->timed_out = writer->do_timeout;
    }
    if ((!flush && outstanding < max_buf) || writer->timed_out) {
        unlock_writer_evbuffer(writer, locked);
        return 0;
    }
    writer->flush = flush;
    writer->wr_continue = 0;
    unlock_writer_evbuffer(writer, locked);
    return sql_flush_int(writer);
}

int sql_writev(struct sqlwriter *writer, struct iovec *v, int n)
{
    int rc = 0;
    Pthread_mutex_lock(&writer->wr_lock);
    for (int i = 0; i < n; ++i) {
        rc = evbuffer_add(writer->wr_buf, v[i].iov_base, v[i].iov_len);
        if (rc) break;
    }
    Pthread_mutex_unlock(&writer->wr_lock);
    return rc;
}

static void sql_trickle_int(struct sqlwriter *writer, int fd)
{
    if (!pthread_equal(pthread_self(), appsock_timer_thd)) {
        abort();
    }
    if (!writer->wr_continue || writer->bad || writer->done) {
        sql_disable_heartbeat(writer);
        return;
    }
    const int outstanding = evbuffer_get_length(writer->wr_buf);
    if (!outstanding) {
        if (difftime(time(NULL), writer->sent_at) >= min_hb_time && !writer->timed_out) {
            struct iovec v[1];
            evbuffer_reserve_space(writer->wr_buf, writer->hb_sz, v, 1);
            v[0].iov_len = writer->hb_sz;
            writer->pack_hb(v[0].iov_base, writer->clnt);
            evbuffer_commit_space(writer->wr_buf, v, 1);
        } else {
            sql_disable_trickle(writer);
            return;
        }
    }
    const int n = evbuffer_write(writer->wr_buf, fd);
    if (n <= 0) {
        writer->bad = 1;
        logmsg(LOGMSG_ERROR, "%s write failed fd:%d rc:%d err:%s\n", __func__,
               fd, n, strerror(errno));
        sql_disable_heartbeat(writer);
        return;
    }
    writer->sent_at = time(NULL);
    int left = outstanding - n;
    if (left) {
        sql_disable_trickle(writer);
    } else if (evbuffer_get_contiguous_space(writer->wr_buf) < KB(1)) {
        evbuffer_pullup(writer->wr_buf, KB(4));
    }
}

void sql_trickle(int fd, short what, void *arg)
{
    if (!(what & EV_WRITE)) {
        abort();
    }
    struct sqlwriter *writer = arg;
    Pthread_mutex_lock(&writer->wr_lock);
    sql_trickle_int(writer, fd);
    Pthread_mutex_unlock(&writer->wr_lock);
}

void sql_heartbeat(int fd, short what, void *arg)
{
    if (!pthread_equal(pthread_self(), appsock_timer_thd)) {
        abort();
    }
    struct sqlwriter *writer = arg;
    Pthread_mutex_lock(&writer->wr_lock);
    int len = evbuffer_get_length(writer->wr_buf);
    if (len || difftime(time(NULL), writer->sent_at) >= min_hb_time) {
        sql_enable_trickle(writer);
    }
    Pthread_mutex_unlock(&writer->wr_lock);
}

void sql_reset(struct sqlwriter *writer)
{
    writer->bad = 0;
    writer->done = 0;
    writer->flush = 0;
    writer->released_locks = 0;
    writer->timed_out = 0;
    writer->wr_continue = 1;
    writer->sent_at = time(NULL);
}

int sql_peer_check(struct sqlwriter *writer)
{
    return writer->bad;
}

int sql_done(struct sqlwriter *writer)
{
    Pthread_mutex_lock(&writer->wr_lock);
    writer->done = 1;
    if (writer->bad) {
        Pthread_mutex_unlock(&writer->wr_lock);
        return -1;
    } else if (evbuffer_get_length(writer->wr_buf)) {
        Pthread_mutex_unlock(&writer->wr_lock);
        return sql_flush(writer);
    }
    Pthread_mutex_unlock(&writer->wr_lock);
    return 0;
}

struct event_base *sql_wrbase(struct sqlwriter *writer)
{
    return writer->wr_base;
}

struct evbuffer *sql_wrbuf(struct sqlwriter *writer)
{
    return writer->wr_buf;
}

void sqlwriter_free(struct sqlwriter *writer)
{
    if (!pthread_equal(pthread_self(), appsock_rd_thd)) {
        abort();
    }
    if (writer->heartbeat_ev) {
        event_free(writer->heartbeat_ev);
        writer->heartbeat_ev = NULL;
    }
    if (writer->timeout) {
        event_free(writer->timeout->ev);
        free(writer->timeout);
        writer->timeout = NULL;
    }
    if (writer->heartbeat_trickle_ev) {
        event_free(writer->heartbeat_trickle_ev);
        writer->heartbeat_trickle_ev = NULL;
    }
    if (writer->flush_ev) {
        event_free(writer->flush_ev);
        writer->flush_ev = NULL;
    }
    if (writer->wr_buf) {
        evbuffer_free(writer->wr_buf);
        writer->wr_buf = NULL;
    }
    if (writer->wr_base) {
        event_base_free(writer->wr_base);
        writer->wr_base = NULL;
    }
    Pthread_mutex_destroy(&writer->wr_lock);
    free(writer);
}

struct sqlwriter *sqlwriter_new(struct sqlclntstate *clnt, int fd, sql_pack_fn *pack, size_t sz)
{
    struct sqlwriter *writer = calloc(1, sizeof(struct sqlwriter));
    Pthread_mutex_init(&writer->wr_lock, NULL);
    writer->clnt = clnt;
    writer->pack_hb = pack;
    writer->hb_sz = sz;
    writer->wr_continue = 1;
    writer->wr_buf = evbuffer_new();

    struct event_config *cfg = event_config_new();
    event_config_set_flag(cfg, EVENT_BASE_FLAG_NOLOCK);
    writer->wr_base = event_base_new_with_config(cfg);
    event_config_free(cfg);

    int flush_flags = EV_WRITE | EV_TIMEOUT | EV_PERSIST;
    writer->flush_ev = event_new(writer->wr_base, fd, flush_flags, sql_flush_cb, writer);

    int hb_flags = EV_TIMEOUT | EV_PERSIST;
    writer->heartbeat_ev = event_new(appsock_timer_base, -1, hb_flags, sql_heartbeat, writer);

    int trickle_flags = EV_WRITE | EV_PERSIST;
    writer->heartbeat_trickle_ev = event_new(appsock_timer_base, fd, trickle_flags, sql_trickle, writer);

    return writer;
}
