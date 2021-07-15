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

#include <akbuf.h>
#include <locks_wrap.h>
#include <logmsg.h>
#include <timer_util.h>

/* This allows waiting for async call to finish */
struct run_base_func_info {
    pthread_mutex_t lock;
    pthread_cond_t cond;
    run_on_base_func func;
    void *arg;
};

static void run_base_func(int dummyfd, short what, void *data)
{
    struct run_base_func_info *info = data;
    info->func(info->arg);
    Pthread_mutex_lock(&info->lock);
    Pthread_cond_signal(&info->cond);
    Pthread_mutex_unlock(&info->lock);
}

void run_on_base(struct event_base *base, run_on_base_func func, void *arg)
{
    struct run_base_func_info info;
    info.func = func;
    info.arg = arg;
    Pthread_mutex_init(&info.lock, NULL);
    Pthread_cond_init(&info.cond, NULL);
    Pthread_mutex_lock(&info.lock);
    if (event_base_once(base, -1, EV_TIMEOUT, run_base_func, &info, NULL) != 0) abort();
    Pthread_cond_wait(&info.cond, &info.lock);
    Pthread_mutex_unlock(&info.lock);
    Pthread_mutex_destroy(&info.lock);
    Pthread_cond_destroy(&info.cond);
}

/*********/
/* akbuf */
/*********/

struct akbuf {
    int have_max;
    struct timeval max_time;
    size_t outstanding;
    pthread_mutex_t lk;
    struct evbuffer *buf;
    struct event *pending;
    struct event_base *base;
    event_callback_fn errorcb;
    event_callback_fn flushcb;
    event_callback_fn emptycb;
    void *arg;
    int fd;
};

static int akbuf_flush_evbuffer(struct evbuffer *buf, int fd, struct timeval *max)
{
    int rc;
    int want = 0;
    int total = 0;
    int counter = 0;
    struct timeval start, end, diff;
    if (max) {
        gettimeofday(&start, NULL);
    }
    do {
        ++counter;
        rc = evbuffer_write(buf, fd);
        if (rc > 0) total += rc;
        want = evbuffer_get_length(buf);
        if (want && max) {
            gettimeofday(&end, NULL);
            timersub(&end, &start, &diff);
            if (timercmp(&diff, max, >)) {
                break;
            }
        }
    } while (want && rc > 0);
    if (rc <= 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
        logmsg(LOGMSG_ERROR, "writev rc:%d fd:%d %d:%s\n", rc, fd, errno, strerror(errno));
        return -1;
    }
    return total;
}

static void akbuf_flushcb(int fd, short what, void *data)
{
    struct akbuf *a = data;
    Pthread_mutex_lock(&a->lk);
    if (!a->buf) {
        abort();
    }
    if (fd != a->fd) {
        abort();
    }
    if (evbuffer_get_length(a->buf) == 0) {
        event_free(a->pending);
        a->pending = NULL;
        Pthread_mutex_unlock(&a->lk);
        return;
    }
    struct evbuffer *buf = a->buf;
    a->buf = evbuffer_new();
    void *arg = a->arg;
    a->outstanding = evbuffer_get_length(buf);
    Pthread_mutex_unlock(&a->lk);
    int total = akbuf_flush_evbuffer(buf, fd, a->have_max ? &a->max_time : NULL);
    Pthread_mutex_lock(&a->lk);
    if (total <= 0) {
        event_free(a->pending);
        a->pending = NULL;
        Pthread_mutex_unlock(&a->lk);
        if (a->errorcb) {
            a->errorcb(fd, 0, arg);
        }
    } else {
        evbuffer_add_buffer(buf, a->buf);
        evbuffer_free(a->buf);
        a->buf = buf;
        int do_flushcb = 0, do_emptycb = 0;
        if ((a->outstanding = evbuffer_get_length(a->buf)) == 0) {
            event_free(a->pending);
            a->pending = NULL;
        }
        if (a->flushcb) {
            do_flushcb = 1;
        }
        if (a->emptycb && a->pending == NULL) {
            do_emptycb = 1;
        }
        Pthread_mutex_unlock(&a->lk);
        if (do_flushcb) {
            a->flushcb(fd, 0, arg);
        }
        if (do_emptycb) {
            a->emptycb(fd, 0, arg);
        }
    }
}

static size_t get_length(struct akbuf *a)
{
    return evbuffer_get_length(a->buf) + a->outstanding;
}

/********************/
/* Public Interface */
/********************/

struct akbuf *akbuf_new(struct event_base *base, void *arg)
{
    struct akbuf *a = calloc(1, sizeof(struct akbuf));
    Pthread_mutex_init(&a->lk, NULL);
    a->base = base;
    a->fd = -1;
    a->arg = arg;
    return a;
}

void akbuf_set_errorcb(struct akbuf *a, event_callback_fn errorcb)
{
    Pthread_mutex_lock(&a->lk);
    a->errorcb = errorcb;
    Pthread_mutex_unlock(&a->lk);
}

void akbuf_set_flushcb(struct akbuf *a, event_callback_fn flushcb)
{
    Pthread_mutex_lock(&a->lk);
    a->flushcb = flushcb;
    Pthread_mutex_unlock(&a->lk);
}

void akbuf_set_emptycb(struct akbuf *a, event_callback_fn emptycb)
{
    int empty = 0;
    Pthread_mutex_lock(&a->lk);
    a->emptycb = emptycb;
    if (emptycb && get_length(a) == 0) {
        empty = 1;
    }
    Pthread_mutex_unlock(&a->lk);
    if (empty) {
        //emptycb(a->fd, 0, a->arg);
        printf("%s - direct call \n", __func__);
        event_once(a->base, emptycb, a->arg);
    }
}

void akbuf_free(struct akbuf *a)
{
    akbuf_disable(a);
    Pthread_mutex_destroy(&a->lk);
    free(a);
}

void akbuf_disable(struct akbuf *a)
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
    a->outstanding = 0;
    a->fd = -1;
    Pthread_mutex_unlock(&a->lk);
}

void akbuf_free_on_base(struct akbuf *a)
{
    run_on_base(a->base,(run_on_base_func)akbuf_free, a);
}

void akbuf_disable_on_base(struct akbuf *a)
{
    run_on_base(a->base, (run_on_base_func)akbuf_disable, a);
}

void akbuf_enable(struct akbuf *a, int fd)
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

size_t akbuf_add(struct akbuf *a, const void *b, size_t s)
{
    struct evbuffer *buf = evbuffer_new();
    evbuffer_add(buf, b, s);
    size_t sz = akbuf_add_buffer(a, buf);
    evbuffer_free(buf);
    return sz;
}

size_t akbuf_add_buffer(struct akbuf *a, struct evbuffer *buf)
{
    Pthread_mutex_lock(&a->lk);
    if (a->fd != -1 && a->buf) {
        evbuffer_add_buffer(a->buf, buf);
        if (!a->pending) {
            a->pending = event_new(a->base, a->fd, EV_WRITE | EV_PERSIST, akbuf_flushcb, a);
            event_add(a->pending, NULL);
        }
    }
    size_t sz = evbuffer_get_length(a->buf);
    Pthread_mutex_unlock(&a->lk);
    return sz;
}

size_t akbuf_get_length(struct akbuf *a)
{
    Pthread_mutex_lock(&a->lk);
    size_t len = get_length(a);
    Pthread_mutex_unlock(&a->lk);
    return len;
}

void akbuf_set_max_flush_time(struct akbuf *a, struct timeval max)
{
    a->have_max = 1;
    a->max_time = max;
}
