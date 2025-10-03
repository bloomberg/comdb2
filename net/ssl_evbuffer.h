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

struct event;
struct evbuffer;
struct event_base;
struct ssl_data;

typedef void (ssl_evbuffer_cb)(void *);
void accept_ssl_evbuffer(struct ssl_data *, struct event_base *, ssl_evbuffer_cb *err, ssl_evbuffer_cb *accepted, void *);
void connect_ssl_evbuffer(struct ssl_data *, struct event_base *, ssl_evbuffer_cb *err, ssl_evbuffer_cb *connected, void *);
int rd_ssl_evbuffer(struct evbuffer *, struct ssl_data *, int *eof);
int wr_ssl_evbuffer(struct ssl_data *, struct evbuffer *);
int ssl_data_has_ssl(struct ssl_data *);
int ssl_data_has_cert(struct ssl_data *);
int ssl_data_cert(struct ssl_data *, int nid, void *out, int outsz);
void ssl_data_free(struct ssl_data *);
struct ssl_data *ssl_data_new(int fd, char *origin);
int ssl_data_pending(struct ssl_data *);

#endif /* INCLUDED_SSLWRITER_H */
