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

#include <stdlib.h>
#include <stddef.h>

#include "pool.h"
#include "list.h"
#include "mem_util.h"
#include "mem_override.h"

typedef struct {
    LISTC_T(struct queue_entry_tag) lst;
    pool_t *pent;
    int max;
} queue_type;

typedef struct queue_entry_tag {
    LINKC_T(queue_type) lnk;
    void *obj;
} queue_entry_type;

queue_type *queue_new()
{
    queue_type *q;
    q = malloc(sizeof(queue_type));
    if (q == 0)
        return 0;
    q->pent = pool_init(sizeof(queue_entry_type), 64);
    if (q->pent == 0) {
        free(q);
        return 0;
    }
    listc_init(&q->lst, offsetof(queue_entry_type, lnk));
    return q;
}

/*return all resources for queue*/
void queue_free(queue_type *q)
{
    pool_free(q->pent);
    q->pent = 0;
    free(q);
}

/*add to end of queue*/
int queue_add(queue_type *q, void *obj)
{
    queue_entry_type *ee;
    ee = pool_getablk(q->pent);
    if (ee == 0)
        return -1;
    ee->obj = obj;
    listc_abl(&q->lst, ee);
    return 0;
}

/*insert in head of queue*/
int queue_insert(queue_type *q, void *obj)
{
    queue_entry_type *ee;
    ee = pool_getablk(q->pent);
    if (ee == 0)
        return -1;
    ee->obj = obj;
    listc_atl(&q->lst, ee);
    return 0;
}

/*dequeue next item off queue*/
void *queue_next(queue_type *q)
{
    void *obj;
    queue_entry_type *ee;
    ee = listc_rtl(&q->lst);
    if (ee == 0)
        return 0;
    obj = ee->obj;
    pool_relablk(q->pent, ee);
    return obj;
}

/*dequeue last item off queue*/
void *queue_last(queue_type *q)
{
    void *obj;
    queue_entry_type *ee;
    ee = listc_rbl(&q->lst);
    if (ee == 0)
        return 0;
    obj = ee->obj;
    pool_relablk(q->pent, ee);
    return obj;
}

/*return num items on queue*/
int queue_count(queue_type *q) { return q->lst.count; }

#ifdef TESTQUEUE
#include <stdio.h>

int main(void)
{
    int ii, rc;
    queue_type *q;

    q = queue_new();
    if (!q)
        abort();

    for (ii = 0; ii < 10; ii++) {
        int *z = malloc(sizeof(int));
        *z = ii;
        if (rc = queue_add(q, z)) {
            printf("error queue_add, ii %d rc %d\n", ii, rc);
            abort();
        }
        printf("queued up %d\n", ii);
    }

    for (ii = 0; ii < 10; ii++) {
        int *z = queue_next(q);
        if (z == 0) {
            printf("error dequeue\n");
            abort();
        }
        printf("dequeued %d\n", *z);
        free(z);
    }

    for (ii = 0; ii < 10; ii++) {
        int *z = (int *)malloc(sizeof(int));
        *z = ii;
        if (rc = queue_insert(q, z)) {
            printf("error queue_ins, ii %d rc %d\n", ii, rc);
            abort();
        }
        printf("inserted %d\n", ii);
    }

    for (ii = 0; ii < 10; ii++) {
        int *z = queue_next(q);
        if (z == 0) {
            printf("error dequeue\n");
            abort();
        }
        printf("dequeued %d\n", *z);
        free(z);
    }

    for (ii = 0; ii < 10; ii++) {
        int *z = (int *)malloc(sizeof(int));
        *z = ii;
        if (ii & 1) {
            if (rc = queue_insert(q, z)) {
                printf("error queue_ins, ii %d rc %d\n", ii, rc);
                abort();
            }
            printf("inserted %d\n", ii);
        } else {
            if (rc = queue_add(q, z)) {
                printf("error queue_add, ii %d rc %d\n", ii, rc);
                abort();
            }
            printf("appended %d\n", ii);
        }
    }

    for (ii = 0; ii < 10; ii++) {
        int *z = queue_next(q);
        if (z == 0) {
            printf("error dequeue\n");
            abort();
        }
        printf("dequeued %d\n", *z);
        free(z);
    }

    for (ii = 0; ii < 10; ii++) {
        int *z = queue_next(q);
        if (z == 0) {
            printf("null dequeue (this is good.)\n");
        } else {
            printf("error, dequeued %d\n", *z);
            abort();
        }
        free(z);
    }

    queue_free(q);
}

#endif
