/*
   Copyright 2023 Bloomberg Finance L.P.

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

#ifndef INCLUDED_SSLWRITER_H
#define INCLUDED_SSLWRITER_H

struct ssl_st;
struct evbuffer;
struct event_base;
typedef void (ssl_evbuffer_cb)(void *);
void accept_evbuffer_ssl(struct ssl_st **, int, struct event_base *, ssl_evbuffer_cb *on_error, ssl_evbuffer_cb *on_success, void *);
void connect_evbuffer_ssl(struct ssl_st **, int fd, struct event_base *base, ssl_evbuffer_cb *error_cb, ssl_evbuffer_cb *success_cb, void *data);
int rd_evbuffer_ssl(struct evbuffer *, struct ssl_st *, int *eof);
int wr_evbuffer_ssl(struct ssl_st *, struct evbuffer *);

#endif /* INCLUDED_SSLWRITER_H */
