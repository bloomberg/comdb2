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

#ifndef AKQ_H
#define AKQ_H

#include <stddef.h>
struct akq;
typedef void (*akq_callback)(void *);
typedef int (*akq_truncate_callback)(void *, void *);
void *akq_work_new(struct akq *);
void akq_enqueue(struct akq *, void *);
void akq_enqueue_work(struct akq *, void *); /* akq_work_new + akq_enqueue */
void akq_truncate(struct akq *, akq_callback);
void akq_truncate_if(struct akq *, akq_truncate_callback, void *);
void akq_stop(struct akq *);
struct akq *akq_new(char *, size_t, akq_callback, akq_callback, akq_callback);

#endif /* AKQ_H */
