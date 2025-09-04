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

#include <compile_time_assert.h>
#include <epochlib.h>
#include <sys_wrap.h>
#include <logmsg.h>
#include <net_appsock.h>
#include <sqlwriter.h>
#include <ssl_evbuffer.h>

//send heartbeat if no data every (msec)
#define min_hb_time 500

struct sqlwriter {
    sql_dispatch_timeout_fn *dispatch_timeout;
    struct sqlclntstate *clnt;
    struct evbuffer *wr_buf;
    pthread_mutex_t wr_lock;
    struct event *flush_ev;
    struct event *heartbeat_ev;
    struct event *heartbeat_trickle_ev;
    struct event *timeout_ev;
    struct event_base *timer_base;
    pthread_t timer_thd;
    struct event_base *wr_base;
    struct timeval sent_at;
    int64_t blocked_at;
    sql_pack_fn *pack;
    sql_pack_fn *pack_hb;
    unsigned bad : 1;
    unsigned done : 1;
    unsigned do_timeout : 1;
    unsigned timed_out : 1;
    unsigned wr_continue : 1;
    unsigned packing : 1; /* 1 if writer is in sql_pack_response and wr_lock is held. */
    struct ssl_data *ssl_data;
    int (*wr_evbuffer_fn)(struct sqlwriter *, int);
};

static void sql_trickle_cb(int fd, short what, void *arg);

static void sql_enable_flush(struct sqlwriter *writer)
{
    struct timeval recover_ddlk_timeout = {.tv_sec = 1};
    event_add(writer->flush_ev, &recover_ddlk_timeout);
}

static void sql_disable_flush(struct sqlwriter *writer)
{
    writer->wr_continue = 1;
    event_del(writer->flush_ev);
}

static void sql_timeout_cb(int fd, short what, void *arg)
{
    struct sqlwriter *writer = arg;
    check_thd(writer->timer_thd);
    if (pthread_mutex_trylock(&writer->wr_lock)) {
        struct timeval retry = {.tv_usec = 100 * 1000};
        event_add(writer->timeout_ev, &retry);
        return;
    }
    if (!writer->bad && !writer->done) {
        writer->do_timeout = 1;
        maxquerytime_cb(writer->clnt);
        writer->do_timeout = 0;
        writer->timed_out = 1;
    }
    Pthread_mutex_unlock(&writer->wr_lock);
}

static void sql_heartbeat_cb(int fd, short what, void *arg);

void sql_enable_heartbeat(struct sqlwriter *writer)
{
    writer->pack_hb(writer, writer->clnt); /* newsql_pack_hb */
    struct timeval heartbeat_time = {.tv_usec = 100000 }; // 100ms
    event_add(writer->heartbeat_ev, &heartbeat_time);
}

void sql_disable_heartbeat(struct sqlwriter *writer)
{
    event_del(writer->heartbeat_ev);
    event_del(writer->heartbeat_trickle_ev);
}

void sql_enable_timeout(struct sqlwriter *writer, int timeout_sec)
{
    check_thd(writer->timer_thd);
    if (writer->timeout_ev) {
        abort();
    }
    writer->timeout_ev = event_new(writer->timer_base, -1, 0, sql_timeout_cb, writer);
    struct timeval timeout = {.tv_sec = timeout_sec};
    event_add(writer->timeout_ev, &timeout);
}

static void do_sql_disable_timeout(void *arg)
{
    struct sqlwriter *writer = arg;
    check_thd(writer->timer_thd);
    event_free(writer->timeout_ev);
    writer->timeout_ev = NULL;
}

void sql_disable_timeout(struct sqlwriter *writer)
{
    if (!writer->timeout_ev) return;
    run_on_base(writer->timer_base, do_sql_disable_timeout, writer);
}

static int wr_evbuffer_ciphertext(struct sqlwriter *writer, int fd)
{
    return wr_ssl_evbuffer(writer->ssl_data, writer->wr_buf);
}

static int wr_evbuffer_plaintext(struct sqlwriter *writer, int fd)
{
    return evbuffer_write(writer->wr_buf, fd);
}

static int wr_evbuffer(struct sqlwriter *writer, int fd)
{
    int rc = writer->wr_evbuffer_fn(writer, fd);
    if (writer->timed_out && writer->dispatch_timeout) {
        /* Exceeded MAXQUERYTIME waiting for leader-election */
        writer->dispatch_timeout(writer->clnt); /* -> timed_out_waiting_for_leader */
        writer->dispatch_timeout = NULL;
    }
    return rc;
}

enum write_state {
    WRITE_BLOCKED,
    WRITE_SUCCEEDED,
    WRITE_FAILED
};

static void update_writer_state(struct sqlwriter *writer, enum write_state state)
{
    int64_t elapsed = comdb2_time_epochus();

    switch (state) {
    case WRITE_SUCCEEDED:
    case WRITE_FAILED:
        if (writer->blocked_at > 0) {
            clnt_increase_netwaitus(writer->clnt, elapsed - writer->blocked_at);
            writer->blocked_at = 0;
        }
        break;
    case WRITE_BLOCKED:
        if (writer->blocked_at == 0)
            writer->blocked_at = elapsed;
        break;
    }
}

/*
 * If a writer is packing a protobuf response (which may invoke sql_flush()
 * multiple times to write out partially serialized data), we don't give up
 * wr_lock. Heartbeat would become a nop; timeout would retry to enqueue an
 * error message. The 2 macros below make sure that we do not double-lock
 * wr_lock, and that we do not release wr_lock when we're not supposed to.
 */
#define LOCK_WR_LOCK_ONLY_IF_NOT_PACKING(w)                                                                            \
    do {                                                                                                               \
        if (!(w)->packing)                                                                                             \
            Pthread_mutex_lock(&(w)->wr_lock);                                                                         \
    } while (0)

#define UNLOCK_WR_LOCK_ONLY_IF_NOT_PACKING(w)                                                                          \
    do {                                                                                                               \
        if (!(w)->packing)                                                                                             \
            Pthread_mutex_unlock(&(w)->wr_lock);                                                                       \
    } while (0)

static void sql_flush_cb(int fd, short what, void *arg)
{
    struct sqlwriter *writer = arg;
    if (what & EV_TIMEOUT) {
        recover_deadlock_evbuffer(writer->clnt);
    }
    if (!(what & EV_WRITE)) {
        return;
    }
    int n;
    LOCK_WR_LOCK_ONLY_IF_NOT_PACKING(writer);
    while (evbuffer_get_length(writer->wr_buf)) {
        if ((n = wr_evbuffer(writer, fd)) <= 0) break;
        gettimeofday(&writer->sent_at, NULL);
        update_writer_state(writer, WRITE_SUCCEEDED);
    }
    if (evbuffer_get_length(writer->wr_buf) == 0) {
        sql_disable_flush(writer);
    } else if (n <= 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            update_writer_state(writer, WRITE_BLOCKED);
        } else {
            writer->bad = 1;
            event_del(writer->flush_ev);
            update_writer_state(writer, WRITE_FAILED);
        }
    }
    UNLOCK_WR_LOCK_ONLY_IF_NOT_PACKING(writer);
}

static int sql_flush_int(struct sqlwriter *writer)
{
    sql_enable_flush(writer);
    update_writer_state(writer, WRITE_BLOCKED);
    event_base_dispatch(writer->wr_base);
    return (writer->wr_continue && !writer->bad) ? 0 : -1;
}

int sql_flush(struct sqlwriter *writer)
{
    LOCK_WR_LOCK_ONLY_IF_NOT_PACKING(writer);
    if (writer->bad) {
        UNLOCK_WR_LOCK_ONLY_IF_NOT_PACKING(writer);
        return -1;
    }
    writer->wr_continue = 0;
    UNLOCK_WR_LOCK_ONLY_IF_NOT_PACKING(writer);
    return sql_flush_int(writer);
}

static int from_timeout_cb(struct sqlwriter *writer)
{
    return writer->do_timeout && pthread_equal(pthread_self(), writer->timer_thd);
}

static int sql_pack_response(struct sqlwriter *writer, void *arg)
{
    int rc;
    writer->packing = 1;

    rc = writer->pack(writer, arg); /* newsql_pack */
    /*
     * rc > 0 : done
     *    = 0 : not done yet
     *    < 0 : error
     */
    if (rc >= 0) {
        writer->done = (rc > 0);
        rc = 0;
    }

    writer->packing = 0;
    return rc;
}

/*
 * An 'append' callback to consume 'packed' data as it is being generated. The
 * function flushes every `SQLWRITER_MAX_BUF' many bytes, hence keeps the
 * memory use of the writer's evbuffer under `SQLWRITER_MAX_BUF'.
 *
 * This is how the function gets invoked:
 * sql_write -> sql_pack_response -> newsql_pack -> sql_append_packed
 */
int sql_append_packed(struct sqlwriter *writer, const void *data, size_t len)
{
    size_t nleft = len;
    struct evbuffer *wr_buf = writer->wr_buf;
    const uint8_t *ptr = data;
    int rc;

    while (nleft > 0) {
        if (evbuffer_get_length(wr_buf) >= SQLWRITER_MAX_BUF) {
            /* We've accumulated enough bytes, flush now. */
            rc = sql_flush(writer);

            /*
             * After reacquiring `wr_lock', check again whether we should proceed,
             * as the writer might have been marked bad by other callbacks
             * (heartbeat, trickle, etc.).
             */
            if (rc != 0 || writer->bad || writer->timed_out)
                return -1;
        }

        int cap = SQLWRITER_MAX_BUF - evbuffer_get_length(wr_buf);
        if (cap >= nleft) {
            /* We're about to exit the loop. Do a copy here. */
            rc = evbuffer_add(wr_buf, ptr + (len - nleft), nleft);
            if (rc != 0)
                return -1;
            nleft = 0;
        } else {
            /*
             * We know we'll continue the loop and do a flush, so we add
             * a reference here to avoid copying the data.
             */
            rc = evbuffer_add_reference(wr_buf, ptr + (len - nleft), cap, NULL, NULL);
            if (rc != 0)
                return -1;
            nleft -= cap;
        }
    }

    return 0;
}

int sql_write(struct sqlwriter *writer, void *arg, int flush)
{
    if (from_timeout_cb(writer)) { /* TODO FIXME : I don't like this special case */
        /* We're holding wr_lock from sql_timeout_cb() */
        return sql_pack_response(writer, arg);
    }
    Pthread_mutex_lock(&writer->wr_lock);
    if (writer->bad || writer->timed_out) {
        Pthread_mutex_unlock(&writer->wr_lock);
        return -1;
    }
    if (sql_pack_response(writer, arg) != 0) {
        Pthread_mutex_unlock(&writer->wr_lock);
        return -1;
    }
    int outstanding = evbuffer_get_length(writer->wr_buf);
    if ((outstanding < SQLWRITER_MAX_BUF) && !flush) {
        Pthread_mutex_unlock(&writer->wr_lock);
        return 0;
    }
    writer->wr_continue = 0;
    int orig_packing = writer->packing;
    writer->packing = 1; /* to skip locking in sql_flush_cb */
    sql_flush_cb(event_get_fd(writer->flush_ev), EV_WRITE, writer);
    writer->packing = orig_packing;
    outstanding = evbuffer_get_length(writer->wr_buf);
    Pthread_mutex_unlock(&writer->wr_lock);
    if (outstanding) return sql_flush(writer);
    return 0;
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

int sql_write_buffer(struct sqlwriter *writer, struct evbuffer *buf)
{
    Pthread_mutex_lock(&writer->wr_lock);
    int rc = evbuffer_add_buffer(writer->wr_buf, buf);
    Pthread_mutex_unlock(&writer->wr_lock);
    return rc;
}

static int sql_pack_heartbeat(struct sqlwriter *writer)
{
    /* nop if writer is still packing a response. */
    return writer->packing ? 0 : writer->pack_hb(writer, writer->clnt);
}

static void sql_trickle_int(struct sqlwriter *writer, int fd)
{
    if (writer->bad || writer->done) {
        sql_disable_heartbeat(writer);
        return;
    }
    if (!writer->wr_continue) {
        return;
    }
    const int outstanding = evbuffer_get_length(writer->wr_buf);
    if (!outstanding) {
        struct timeval now, diff;
        gettimeofday(&now, NULL);
        timersub(&now, &writer->sent_at, &diff);
        int diffms = timeval_to_ms(diff);
        if (diffms >= min_hb_time && !writer->timed_out) {
            sql_pack_heartbeat(writer);
        } else {
            return;
        }
    }
    const int n = wr_evbuffer(writer, fd);
    if (n <= 0) {
        writer->bad = 1;
        logmsg(LOGMSG_ERROR, "%s write failed fd:%d rc:%d err:%s\n", __func__, fd, n, strerror(errno));
        sql_disable_heartbeat(writer);
        return;
    }
    update_writer_state(writer, WRITE_SUCCEEDED);
    gettimeofday(&writer->sent_at, NULL);
}

static void sql_trickle_cb(int fd, short what, void *arg)
{
    if (!(what & EV_WRITE)) {
        abort();
    }
    struct sqlwriter *writer = arg;
    if (pthread_mutex_trylock(&writer->wr_lock) == 0) {
        sql_trickle_int(writer, fd);
        Pthread_mutex_unlock(&writer->wr_lock);
    }
}

static void sql_heartbeat_cb(int fd, short what, void *arg)
{
    struct sqlwriter *writer = arg;
    if (pthread_mutex_trylock(&writer->wr_lock)) return;
    if (writer->wr_continue) {
        int len = evbuffer_get_length(writer->wr_buf);
        struct timeval now, diff;
        gettimeofday(&now, NULL);
        timersub(&now, &writer->sent_at, &diff);
        int diffms = timeval_to_ms(diff);
        if (len || diffms >= min_hb_time) {
            event_add(writer->heartbeat_trickle_ev, NULL);
        }
    }
    Pthread_mutex_unlock(&writer->wr_lock);
}

void sql_reset(struct sqlwriter *writer)
{
    writer->bad = 0;
    writer->dispatch_timeout = NULL;
    writer->done = 0;
    gettimeofday(&writer->sent_at, NULL);
    writer->timed_out = 0;
    writer->wr_continue = 1;
}

int sql_peer_check(struct sqlwriter *writer)
{
    return writer->bad;
}

int sql_done(struct sqlwriter *writer)
{
    struct sqlclntstate *clnt = writer->clnt;
    if (done_cb_evbuffer(clnt) != 0) {
        return -1;
    }
    Pthread_mutex_lock(&writer->wr_lock);
    writer->done = 1;
    if (writer->bad) {
        Pthread_mutex_unlock(&writer->wr_lock);
        return -1;
    }
    sql_disable_heartbeat(writer);
    sql_disable_timeout(writer);
    if (evbuffer_get_length(writer->wr_buf)) {
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
    if (writer->heartbeat_ev) {
        event_free(writer->heartbeat_ev);
        writer->heartbeat_ev = NULL;
    }
    if (writer->timeout_ev) {
        event_free(writer->timeout_ev);
        writer->timeout_ev = NULL;
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

struct sqlwriter *sqlwriter_new(struct sqlwriter_arg *arg)
{
    struct sqlwriter *writer = calloc(1, sizeof(struct sqlwriter));
    Pthread_mutex_init(&writer->wr_lock, NULL);
    writer->clnt = arg->clnt;
    writer->pack = arg->pack;
    writer->pack_hb = arg->pack_hb;
    writer->timer_base = arg->timer_base;
    writer->timer_thd = pthread_self();
    writer->wr_continue = 1;
    writer->wr_buf = evbuffer_new();

    struct event_config *cfg = event_config_new();
    event_config_set_flag(cfg, EVENT_BASE_FLAG_NOLOCK);
    writer->wr_base = event_base_new_with_config(cfg);
    event_config_free(cfg);

    writer->wr_evbuffer_fn = wr_evbuffer_plaintext;
    writer->flush_ev = event_new(writer->wr_base, arg->fd, EV_WRITE | EV_PERSIST, sql_flush_cb, writer);
    writer->heartbeat_ev = event_new(writer->timer_base, arg->fd, EV_PERSIST, sql_heartbeat_cb, writer);
    writer->heartbeat_trickle_ev = event_new(writer->timer_base, arg->fd, EV_WRITE, sql_trickle_cb, writer);

    return writer;
}

void sql_enable_ssl(struct sqlwriter *writer, struct ssl_data *ssl_data)
{
    writer->ssl_data = ssl_data;
    writer->wr_evbuffer_fn = wr_evbuffer_ciphertext;
}

void sql_disable_ssl(struct sqlwriter *writer)
{
    writer->ssl_data = NULL;
    writer->wr_evbuffer_fn = wr_evbuffer_plaintext;
}

void sql_wait_for_leader(struct sqlwriter *writer, sql_dispatch_timeout_fn *fn)
{
    writer->dispatch_timeout = fn;
}
