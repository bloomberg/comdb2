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
 * Initialize a client to use a socket instead of net
 * for bplog transfer to master
 * Second prototype is for the master receiver
 *
 */
void init_bplog_socket(struct sqlclntstate *clnt);
void init_bplog_socket_master(struct osql_target *, struct comdb2buf *);

/**
 * Read buffer over the socket with timeout and default timeout
 *
 */
int osql_read_buffer(char *p_buf, size_t p_buf_len, struct comdb2buf *, int *timeoutms, int deltams);
int osql_read_buffer_default(char *buf, int buflen, struct comdb2buf *);

#endif
