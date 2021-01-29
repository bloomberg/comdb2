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

#ifndef AKBUF_H
#define AKBUF_H

#include <event2/buffer.h>
#include <event2/event.h>

struct akbuf;
size_t akbuf_add(struct akbuf *, const void *, size_t);
size_t akbuf_add_buffer(struct akbuf *, struct evbuffer *);
void akbuf_disable(struct akbuf *);
void akbuf_enable(struct akbuf *, int);
void akbuf_free(struct akbuf *);
size_t akbuf_get_length(struct akbuf *);
struct akbuf *akbuf_new(struct event_base *, void *);
void akbuf_set_emptycb(struct akbuf *, event_callback_fn);
void akbuf_set_errorcb(struct akbuf *, event_callback_fn);
void akbuf_set_flushcb(struct akbuf *, event_callback_fn);
void akbuf_set_max_flush_time(struct akbuf *, struct timeval);
void akbuf_disable_on_base(struct akbuf *);
void akbuf_free_on_base(struct akbuf *);


typedef void(*run_on_base_func)(void *);
void run_on_base(struct event_base *run_base, run_on_base_func func, void *arg);

#define event_once(a, b, c)                                                    \
    ({                                                                         \
        int erc;                                                               \
        if ((erc = event_base_once(a, -1, EV_TIMEOUT, b, c, NULL)) != 0) {     \
            logmsg(LOGMSG_ERROR, "%s:%d event_base_once failed\n", __func__,   \
                   __LINE__);                                                  \
        }                                                                      \
        erc;                                                                   \
    })


#endif /* AKBUF_H */
