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

#ifndef AKBUFFEREVENT_H
#define AKBUFFEREVENT_H

#include <event2/buffer.h>
#include <event2/event.h>

struct akbufferevent;
void akbufferevent_add(struct akbufferevent *, const void *, size_t);
void akbufferevent_add_buffer(struct akbufferevent *, struct evbuffer *);
void akbufferevent_disable(struct akbufferevent *);
void akbufferevent_free(struct akbufferevent *);
void akbufferevent_enable(struct akbufferevent *, int);
size_t akbufferevent_get_length(struct akbufferevent *);
struct akbufferevent *akbufferevent_new(struct event_base *, event_callback_fn, event_callback_fn, void *);
void akbufferevent_set_errorcb(struct akbufferevent *, event_callback_fn);
void akbufferevent_set_flushcb(struct akbufferevent *, event_callback_fn);

#endif /* AKBUFFEREVENT_H */
