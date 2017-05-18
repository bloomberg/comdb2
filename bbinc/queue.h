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

#ifndef __QUEUE_H___
#define __QUEUE_H___

#include <list.h>
#include <pool.h>

/* Implements FIFO

   queue_type *q = queue_new();       // creates a queue

   struct whatever *w = malloc(sizeof(struct whatever));

   queue_add(q, w);       // add whatever to the queue.

   w = malloc(sizeof(struct whatever));

   queue_add(q, w);       // add another to the queue.

   while ( w = queue_next(q) )
   {
           process_whatever(w);   // process each one in FIFO order
   }

*/

typedef struct {
    LISTC_T(struct queue_entry_tag) lst;
    pool_t *pent;
    int max;
} queue_type;

typedef struct queue_entry_tag {
    LINKC_T(queue_type) lnk;
    void *obj;
} queue_entry_type;

/* create a queue */
queue_type *queue_new();

/*return all resources for queue, free mem*/
void queue_free(queue_type *q);

/*add to end of queue.  0==success*/
int queue_add(queue_type *q, void *obj);

/*insert in head of queue.  0==success*/
int queue_insert(queue_type *q, void *obj);

/*dequeue next item off queue.  Null if empty queue*/
void *queue_next(queue_type *q);

/*dequeue last item off queue.  Null if empty queue*/
void *queue_last(queue_type *q);

/*return num items on queue*/
int queue_count(queue_type *q);

#endif
