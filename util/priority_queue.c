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

#include <errno.h>
#include "priority_queue.h"

priority_queue_t *priority_queue_new()
{
  priority_queue_t *q = calloc(1, sizeof(priority_queue_t));

  if (q != NULL) {
    listc_init(&q->list, offsetof(priority_queue_item_t, link));
  }

  return q;
}

void priority_queue_free(priority_queue_t **pq){
  if (pq != NULL) {
    priority_queue_t *q = *pq;

    if (q != NULL) {
      priority_queue_item_t *tmp, *iter;

      LISTC_FOR_EACH_SAFE(&q->list, iter, tmp, link)
      {
        listc_rfl(&q->list, iter);
        free(iter);
      }

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
  if ((q == NULL) || (o == NULL)) return EINVAL;

  priority_queue_item_t *i = calloc(1, sizeof(priority_queue_item_t));

  if (i == NULL) return ENOMEM;

  i->priority = p;
  i->pData = o;

  priority_queue_item_t *tmp, *iter;

  LISTC_FOR_EACH_SAFE(&q->list, iter, tmp, link)
  {
    if (iter->priority >= p) {
      listc_add_before(&q->list, i, iter);
      return 0;
    }
  }

  listc_abl(&q->list, i);
  return 0;
}

void *priority_queue_next(
  priority_queue_t *q
){
  if ((q == NULL) || (q->list == NULL)) return NULL;
  return listc_rtl(&q->list);
}

int priority_queue_count(
  priority_queue_t *q
){
  if ((q == NULL) || (q->list == NULL)) return -1;
  return listc_size(&q->list);
}
