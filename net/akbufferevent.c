/*
   Copyright 2020 Bloomberg Finance L.P.

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
#include <pthread.h>
#include <stdlib.h>

#include <akbufferevent.h>
#include <locks_wrap.h>
#include <logmsg.h>

struct akbufferevent {
    pthread_mutex_t lk;
    struct evbuffer *buf;
    struct event *pending;
    struct event_base *base;
    event_callback_fn errorcb;
    event_callback_fn flushcb;
    void *arg;
    int fd;
};

static void flushcb(int, short, void *);

static void flush_evbuffer_int(struct akbufferevent *a)
{
    int rc;
    int want = 0;
    int total = 0;
    do {
        rc = evbuffer_write(a->buf, a->fd);
        if (rc > 0) total += rc;
        want = evbuffer_get_length(a->buf);
    } while (want && rc > 0);
    if (rc <= 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
        logmsg(LOGMSG_ERROR, "writev failed %d:%s\n", errno, strerror(errno));
        evbuffer_free(a->buf);
        a->buf = NULL;
        if (a->pending) {
            event_del(a->pending);
            a->pending = NULL;
        }
        if (a->errorcb) {
            a->errorcb(0, a->fd, a->arg);
        } else {
            logmsg(LOGMSG_USER, "%s have error but no callback fd:%d\n", __func__, a->fd);
            abort();
        }
        return;
    }
    if (want) {
       if (!a->pending) {
            a->pending = event_new(a->base, a->fd, EV_WRITE|EV_PERSIST, flushcb, a);
            event_add(a->pending, NULL);
       }
    } else if (a->pending) {
        event_del(a->pending);
        a->pending = NULL;
    }
    if (a->flushcb) {
        a->flushcb(total, a->fd, a->arg);
    }
}

static void flushcb(int fd, short what, void *data)
{
    struct akbufferevent *a = data;
    Pthread_mutex_lock(&a->lk);
    if (!a->buf) {
        abort();
    }
    if (fd != a->fd) {
        abort();
    }
    flush_evbuffer_int(a);
    Pthread_mutex_unlock(&a->lk);
}

/********************/
/* Public Interface */
/********************/

struct akbufferevent *akbufferevent_new(struct event_base *base,
                                        event_callback_fn errorcb,
                                        event_callback_fn flushcb, void *arg)
{
    struct akbufferevent *a = calloc(1, sizeof(struct akbufferevent));
    Pthread_mutex_init(&a->lk, NULL);
    a->base = base;
    a->fd = -1;
    a->errorcb = errorcb;
    a->flushcb = flushcb;
    a->arg = arg;
    return a;
}

void akbufferevent_set_errorcb(struct akbufferevent *a, event_callback_fn errorcb)
{
    a->errorcb = errorcb;
}

void akbufferevent_set_flushcb(struct akbufferevent *a, event_callback_fn flushcb)
{
    a->flushcb = flushcb;
}

void akbufferevent_free(struct akbufferevent *a)
{
    akbufferevent_disable(a);
    Pthread_mutex_destroy(&a->lk);
    free(a);
}

void akbufferevent_disable(struct akbufferevent *a)
{
    Pthread_mutex_lock(&a->lk);
    if (a->buf) {
        evbuffer_free(a->buf);
        a->buf = NULL;
    }
    if (a->pending) {
        event_del(a->pending);
        a->pending = NULL;
    }
    a->fd = -1;
    Pthread_mutex_unlock(&a->lk);
}

void akbufferevent_enable(struct akbufferevent *a, int fd)
{
    Pthread_mutex_lock(&a->lk);
    if (a->buf || a->pending || a->fd != -1) {
        /* Should be cleaned up prior */
        abort();
    }
    a->buf = evbuffer_new();
    a->fd = fd;
    Pthread_mutex_unlock(&a->lk);
}

void akbufferevent_add(struct akbufferevent *a, const void *b, size_t s)
{
    Pthread_mutex_lock(&a->lk);
    evbuffer_add(a->buf, b, s);
    Pthread_mutex_unlock(&a->lk);
}

void akbufferevent_add_buffer(struct akbufferevent *a, struct evbuffer *buf)
{
    Pthread_mutex_lock(&a->lk);
    if (a->fd != -1 && a->buf) {
        evbuffer_add_buffer(a->buf, buf);
        if (!a->pending) {
            a->pending = event_new(a->base, a->fd, EV_WRITE | EV_PERSIST, flushcb, a);
            event_add(a->pending, NULL);
        }
    }
    Pthread_mutex_unlock(&a->lk);
}

size_t akbufferevent_get_length(struct akbufferevent *a)
{
    return evbuffer_get_length(a->buf);
}
