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

#ifndef __PRIORITY_QUEUE_H___
#define __PRIORITY_QUEUE_H___

#include <limits.h>
#include <list.h>

typedef long long priority_t;

/* TODO: Maybe consider changing this value in the future? */
#ifndef PRIORITY_T_ADJUSTMENT_MAXIMUM
#define PRIORITY_T_ADJUSTMENT_MAXIMUM ((priority_t)1000000LL)
#endif

#ifndef PRIORITY_T_ADJUSTMENT_INITIAL
#define PRIORITY_T_ADJUSTMENT_INITIAL ((priority_t)(PRIORITY_T_ADJUSTMENT_MAXIMUM>>1LL))
#endif

#ifndef PRIORITY_T_INVALID
#define PRIORITY_T_INVALID ((priority_t)-1LL)
#endif /* PRIORITY_T_INVALID */

#ifndef PRIORITY_T_HIGHEST
#define PRIORITY_T_HIGHEST ((priority_t)0LL)
#endif /* PRIORITY_T_HIGHEST */

#ifndef PRIORITY_T_LOWEST
#define PRIORITY_T_LOWEST ((priority_t)(LLONG_MAX>>1LL))
#endif /* PRIORITY_T_LOWEST */

#ifndef PRIORITY_T_INITIAL
#define PRIORITY_T_INITIAL ((priority_t)(PRIORITY_T_HIGHEST+PRIORITY_T_ADJUSTMENT_INITIAL))
#endif /* PRIORITY_T_INITIAL */

#ifndef PRIORITY_T_HEAD
#define PRIORITY_T_HEAD ((priority_t)LLONG_MAX-3LL)
#endif /* PRIORITY_T_HEAD */

#ifndef PRIORITY_T_TAIL
#define PRIORITY_T_TAIL ((priority_t)LLONG_MAX-2LL)
#endif /* PRIORITY_T_TAIL */

#ifndef PRIORITY_T_DEFAULT
#define PRIORITY_T_DEFAULT ((priority_t)LLONG_MAX-1LL)
#endif /* PRIORITY_T_DEFAULT */

struct priority_queue_item_tag {
  priority_t priority;
  void *pData;
  LINKC_T(struct priority_queue_item_tag) link;
};

struct priority_queue_tag {
  LISTC_T(struct priority_queue_item_tag) list;
};

typedef struct priority_queue_item_tag priority_queue_item_t;
typedef struct priority_queue_tag priority_queue_t;
typedef void (*priority_queue_foreach_fn)(void *p1, void *pData, void *p2);

/* check if a value is valid for the priority_t type */
int priority_is_valid(priority_t priority, int bSpecial);

/* create a queue */
priority_queue_t *priority_queue_new();

/* initialize a queue */
void priority_queue_initialize(priority_queue_t *q);

/* return all resources for queue */
void priority_queue_clear(priority_queue_t *q);

/* return all resources for queue, free mem */
void priority_queue_free(priority_queue_t **pq);

/* add to end of queue.  0==success */
int priority_queue_add(priority_queue_t *q, priority_t p, void *o);

/* dequeue next item off queue.  null if empty queue */
void *priority_queue_next(priority_queue_t *q);

/* return the priority of the highest priority item */
priority_t priority_queue_highest(priority_queue_t *q);

/* return num items on queue */
int priority_queue_count(priority_queue_t *q);

/* call function for each item in queue */
void priority_queue_foreach(priority_queue_t *q, void *p1, priority_queue_foreach_fn fn, void *p2);

#endif /* __PRIORITY_QUEUE_H___ */
