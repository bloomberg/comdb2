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

#include <list.h>

typedef int priority_t;

typedef struct priority_queue_item_tag {
  priority_t priority;
  void *pData;
  LINKC_T(struct priority_queue_item_tag) link;
} priority_queue_item_t;

typedef struct priority_queue_tag {
  LISTC_T(struct priority_queue_tag) list;
} priority_queue_t;

/* create a queue */
priority_queue_t *priority_queue_new();

/* return all resources for queue, free mem */
void priority_queue_free(priority_queue_t **pq);

/* add to end of queue.  0==success */
int priority_queue_add(priority_queue_t *q, priority_t p, void *o);

/* dequeue next item off queue.  null if empty queue */
void *priority_queue_next(priority_queue_t *q);

/* return num items on queue */
int priority_queue_count(priority_queue_t *q);

#endif /* __PRIORITY_QUEUE_H___ */
