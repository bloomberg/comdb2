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

#ifndef __OSQLSQLSOCKET_H__
#define __OSQLSQLSOCKET_H__


/**
 * Fwd declare types
 */

struct osql_target;
struct comdb2buf;
struct sqlclntstate;

/**
 * Refcounted holder for a sockbplog appsock buffer.
 *
 * The master writes a session's reply over the very buffer its appsock thread
 * is reading the next request from, and the two run independently: the appsock
 * returns as soon as the connection drops, while the block processor may still
 * have a reply to write.  Neither side can close the buffer on its own, so both
 * hold a reference -- the appsock one for the connection, and one per session it
 * dispatches -- and the last one out calls close_appsock().
 *
 * The appsock must also set *arg->keepsocket so that the framework leaves the
 * buffer to us instead of closing it when the handler returns.
 */
struct bplog_sock;
struct bplog_sock *bplog_sock_new(struct comdb2buf *sb);
struct comdb2buf *bplog_sock_buf(struct bplog_sock *bsock);
void bplog_sock_deref(struct bplog_sock *bsock);

/**
 * Initialize a client to use a socket instead of net
 * for bplog transfer to master
 * Second prototype is for the master receiver; it takes a reference on bsock
 * which osql_sess_close() releases.
 *
 */
void init_bplog_socket(struct sqlclntstate *clnt);
void init_bplog_socket_master(struct osql_target *, struct bplog_sock *bsock);

/**
 * Read buffer over the socket with timeout and default timeout
 *
 */
int osql_read_buffer(char *p_buf, size_t p_buf_len, struct comdb2buf *, int *timeoutms, int deltams);
int osql_read_buffer_default(char *buf, int buflen, struct comdb2buf *);

#endif
