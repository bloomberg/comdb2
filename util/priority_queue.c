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

#include <assert.h>
#include <stdlib.h>
#include <errno.h>
#include "priority_queue.h"

int priority_is_valid(priority_t priority, int bSpecial)
{
  /*
  ** WARNING: This code assumes that higher priority values have
  **          lower numerical values.
  */
  if ((priority >= PRIORITY_T_HIGHEST) && (priority <= PRIORITY_T_LOWEST)) {
    return 1; /* within basic range, always valid. */
  }
  if (bSpecial) { /* allow special values? */
    switch (priority) {
      case PRIORITY_T_HEAD:
      case PRIORITY_T_TAIL:
      case PRIORITY_T_DEFAULT:
        return 1;
    }
  }
  return 0; /* any other value is invalid, e.g. PRIORITY_T_INVALID */
}

priority_queue_t *priority_queue_new()
{
  priority_queue_t *q = calloc(1, sizeof(priority_queue_t));
  priority_queue_initialize(q);
  return q;
}

void priority_queue_initialize(priority_queue_t *q){
  if (q != NULL) {
    listc_init(&q->list, offsetof(priority_queue_item_t, link));
  }
}

void priority_queue_clear(priority_queue_t *q){
  if (q != NULL) {
    priority_queue_item_t *tmp, *iter;

    LISTC_FOR_EACH_SAFE(&q->list, iter, tmp, link)
    {
      listc_rfl(&q->list, iter);
      free(iter);
    }
  }
}

void priority_queue_free(priority_queue_t **pq){
  if (pq != NULL) {
    priority_queue_t *q = *pq;

    if (q != NULL) {
      priority_queue_clear(q);
      free(q);
    }

    *pq = NULL;
  }
}

int priority_queue_add(
  priority_queue_t *q,
  priority_t p,
  void *o
){
  /*
  ** WARNING: This code assumes that higher priority values have
  **          lower numerical values.
  */
  assert(priority_is_valid(p, 1));

  if ((q == NULL) || (o == NULL)) return EINVAL;

  priority_queue_item_t *i = calloc(1, sizeof(priority_queue_item_t));

  if (i == NULL) return ENOMEM;

  i->pData = o;

  if (p == PRIORITY_T_HEAD)
  {
    i->priority = PRIORITY_T_HIGHEST;
    listc_atl(&q->list, i);
    return 0;
  }

  if (p == PRIORITY_T_TAIL)
  {
    i->priority = PRIORITY_T_LOWEST;
    listc_abl(&q->list, i);
    return 0;
  }

  if (p == PRIORITY_T_DEFAULT)
    p = PRIORITY_T_LOWEST;

  assert(priority_is_valid(p, 0));

  priority_queue_item_t *tmp, *iter;

  LISTC_FOR_EACH_SAFE(&q->list, iter, tmp, link)
  {
    assert(priority_is_valid(iter->priority, 0));
    if ((iter != NULL) && (iter->priority >= p)) {
      i->priority = p;
      listc_add_before(&q->list, i, iter);
      return 0;
    }
  }

  i->priority = p;
  listc_abl(&q->list, i);
  return 0;
}

void *priority_queue_next(
  priority_queue_t *q
){
  if (q == NULL) return NULL;
  priority_queue_item_t *i = listc_rtl(&q->list);
  if (i == NULL) return NULL;
  return i->pData;
}

priority_t priority_queue_highest(
  priority_queue_t *q
){
  if (q == NULL) return PRIORITY_T_INVALID;
  priority_queue_item_t *i = LISTC_TOP(&q->list);
  if (i == NULL) return PRIORITY_T_INVALID;
  return i->priority;
}

int priority_queue_count(
  priority_queue_t *q
){
  if (q == NULL) return -1; /* invalid count */
  return listc_size(&q->list);
}

void priority_queue_foreach(
  priority_queue_t *q,
  void *p1,
  priority_queue_foreach_fn fn,
  void *p2
){
  if (q == NULL) return;
  priority_queue_item_t *tmp, *iter;
  LISTC_FOR_EACH_SAFE(&q->list, iter, tmp, link)
  {
    if (iter != NULL) {
      (fn)(p1, iter->pData, p2);
    }
  }
}
