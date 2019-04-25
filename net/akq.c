/*
   Copyright 2019 Bloomberg Finance L.P.

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

#include <stddef.h>
#include <stdlib.h>
#include <pthread.h>

#include <locks_wrap.h>
#include <akq.h>

TAILQ_HEAD(akq_work_list, akq_work);

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
};

static void akq_worker_int(struct akq *q)
{
    struct akq_work_list work_list;
    while (1) {
        Pthread_mutex_lock(&q->lock);
        if (q->stop_work) {
            Pthread_mutex_unlock(&q->lock);
            return;
        }
        if (TAILQ_EMPTY(&q->work_list)) {
            Pthread_cond_wait(&q->cond, &q->lock);
        }
        work_list = q->work_list;
        TAILQ_INIT(&q->work_list);
        Pthread_mutex_unlock(&q->lock);
        struct akq_work *w, *tmp;
        TAILQ_FOREACH_SAFE(w, &work_list, entry, tmp) {
            void *work = ((char *)w - q->offset);
            q->func(work);
        }
    }
}

void *akq_worker(void *arg)
{
    struct akq *q = arg;
    if (q->start) q->start(q);
    akq_worker_int(arg);
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

void akq_stop(struct akq *q)
{
    Pthread_mutex_lock(&q->lock);
    q->stop_work = 1;
    Pthread_cond_signal(&q->cond);
    Pthread_mutex_unlock(&q->lock);
    void *ret;
    Pthread_join(q->thd, &ret);
}

struct akq *akq_new(size_t s, size_t o, akq_callback func, akq_callback start,
                    akq_callback stop)
{
    struct akq *q = calloc(1, sizeof(struct akq));
    q->size = s;
    q->offset = o;
    q->start = start;
    q->stop = stop;
    q->func = func;
    TAILQ_INIT(&q->work_list);
    Pthread_mutex_init(&q->lock, NULL);
    Pthread_cond_init(&q->cond, NULL);
    Pthread_create(&q->thd, NULL, akq_worker, q);
    return q;
}

void *akq_work_new(struct akq *q)
{
    return malloc((q)->size);
}
