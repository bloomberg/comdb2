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

#include <pthread.h>
#include <stdlib.h>

#include <akq.h>
#include <dbinc/queue.h>
#include <locks_wrap.h>

struct akq_work {
    TAILQ_ENTRY(akq_work) entry;
};
TAILQ_HEAD(akq_work_list, akq_work);

struct akq_chunk {
    TAILQ_ENTRY(akq_chunk) chunk_entry;
};
TAILQ_HEAD(akq_chunk_list, akq_chunk);

struct akq {
    int stop_work;
    size_t size;
    size_t offset;
    pthread_t thd;
    pthread_mutex_t lock;
    pthread_cond_t cond;
    akq_callback start;
    akq_callback stop;
    akq_callback func;
    struct akq_work_list work_list;
    struct akq_work_list free_list;
    struct akq_work_list reuse_list;
    struct akq_chunk_list chunk_list;
};

static void akq_new_chunk(struct akq *q)
{
#   define NUM 2000
    size_t chunksz = sizeof(struct akq_chunk) + (NUM * q->size);
    struct akq_chunk *c = (struct akq_chunk *)malloc(chunksz);
    TAILQ_INSERT_TAIL(&q->chunk_list, c, chunk_entry);
    char *buf = (char *)(++c);
    for (int i = 0; i < NUM; ++i) {
        struct akq_work *w = (struct akq_work *)(buf + q->offset);
        TAILQ_INSERT_TAIL(&q->free_list, w, entry);
        buf += q->size;
    }
}

static void akq_worker_int(struct akq *q)
{
    struct akq_work_list work_list;
    TAILQ_INIT(&work_list);
    while (1) {
        Pthread_mutex_lock(&q->lock);
        if (q->stop_work) {
            Pthread_mutex_unlock(&q->lock);
            return;
        }
        TAILQ_CONCAT(&q->reuse_list, &work_list, entry);
        if (TAILQ_EMPTY(&q->work_list)) {
            Pthread_cond_wait(&q->cond, &q->lock);
        }
        TAILQ_CONCAT(&work_list, &q->work_list, entry);
        Pthread_mutex_unlock(&q->lock);
        struct akq_work *w;
        TAILQ_FOREACH(w, &work_list, entry) {
            void *work = ((char *)w - q->offset);
            q->func(work);
        }
    }
}

static void *akq_worker(void *arg)
{
    struct akq *q = (struct akq *)arg;
    if (q->start) q->start(q);
    akq_worker_int(q);
    if (q->stop) q->stop(q);
    return NULL;
}

void akq_enqueue(struct akq *q, void *arg)
{
    struct akq_work *w = (struct akq_work *)((char *)arg + q->offset);
    Pthread_mutex_lock(&q->lock);
    int was_empty = TAILQ_EMPTY(&q->work_list);
    TAILQ_INSERT_TAIL(&q->work_list, w, entry);
    if (was_empty) {
        Pthread_cond_signal(&q->cond);
    }
    Pthread_mutex_unlock(&q->lock);
}

void akq_truncate(struct akq *q, akq_callback del)
{
    Pthread_mutex_lock(&q->lock);
    q->stop_work = 1;
    Pthread_cond_signal(&q->cond);
    Pthread_mutex_unlock(&q->lock);
    void *ret;
    Pthread_join(q->thd, &ret);
    struct akq_work *w;
    int i = 0;
    TAILQ_FOREACH(w, &q->work_list, entry) {
        ++i;
        void *work = ((char *)w - q->offset);
        del(work);
    }
    printf("akq freed items:%d\n", i);
    TAILQ_CONCAT(&q->reuse_list, &q->work_list, entry);
    q->stop_work = 0;
    Pthread_create(&q->thd, NULL, akq_worker, q);
}

void akq_stop(struct akq *q)
{
    Pthread_mutex_lock(&q->lock);
    q->stop_work = 1;
    Pthread_cond_signal(&q->cond);
    Pthread_mutex_unlock(&q->lock);
    void *ret;
    Pthread_join(q->thd, &ret);
    struct akq_chunk *c, *tmp;
    TAILQ_FOREACH_SAFE(c, &q->chunk_list, chunk_entry, tmp) {
        free(c);
    }
    free(q);
}

struct akq *akq_new(size_t s, akq_callback func, akq_callback start, akq_callback stop)
{
    struct akq *q = (struct akq *)calloc(1, sizeof(struct akq));
    q->size = s + sizeof(struct akq_work);
    q->offset = s;
    q->start = start;
    q->stop = stop;
    q->func = func;
    TAILQ_INIT(&q->work_list);
    TAILQ_INIT(&q->free_list);
    TAILQ_INIT(&q->reuse_list);
    TAILQ_INIT(&q->chunk_list);
    akq_new_chunk(q);
    Pthread_mutex_init(&q->lock, NULL);
    Pthread_cond_init(&q->cond, NULL);
    Pthread_create(&q->thd, NULL, akq_worker, q);
    return q;
}

void *akq_work_new(struct akq *q)
{
    if (!TAILQ_EMPTY(&q->free_list)) {
        struct akq_work *w = TAILQ_FIRST(&q->free_list);
        TAILQ_REMOVE(&q->free_list, w, entry);
        return ((char *)w - q->offset);
    }
    Pthread_mutex_lock(&q->lock);
    if (TAILQ_EMPTY(&q->reuse_list)) {
        akq_new_chunk(q);
    } else {
        TAILQ_CONCAT(&q->free_list, &q->reuse_list, entry);
    }
    Pthread_mutex_unlock(&q->lock);
    return akq_work_new(q);
}
