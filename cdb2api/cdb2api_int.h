/*
   Copyright 2025 Bloomberg Finance L.P.

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

/*
 * CDB2API INT
 *
 */

#ifndef INCLUDED_CDB2API_INT_H
#define INCLUDED_CDB2API_INT_H

#include <comdb2buf.h>
#include "cdb2api.h"

#if defined __cplusplus
extern "C" {
#endif

COMDB2BUF *cdb2_socket_pool_get(cdb2_hndl_tp *hndl, const char *typestr, int dbnum, int *port, int *was_from_local_cache);
// use this one for fastsql since client_sbuf and server_sbuf are different
int cdb2_socket_pool_get_fd(cdb2_hndl_tp *hndl, const char *typestr, int dbnum, int *port);
void cdb2_socket_pool_donate_ext(const cdb2_hndl_tp *hndl, const char *typestr, int fd, int ttl, int dbnum);

int cdb2_send_2pc(cdb2_hndl_tp *hndl, char *dbname, char *pname, char *ptier, char *source, unsigned int op,
                  char *dist_txnid, int rcode, int outrc, char *errmsg, int async);

COMDB2BUF *cdb2_cdb2buf_openread(const char *filename);
int cdb2_read_line(char *line, int maxlen, COMDB2BUF *s, const char *buf, int *chrno);

void cdb2_setIdentityBlob(cdb2_hndl_tp *hndl, void *id);

void cdb2_hndl_set_max_retries(cdb2_hndl_tp *hndl, int max_retries);
void cdb2_hndl_set_min_retries(cdb2_hndl_tp *hndl, int min_retries);

int cdb2_get_comdb2db(char **comdb2db_name, char **comdb2db_class);

void cdb2_set_debug_trace(cdb2_hndl_tp *hndl);

struct cdb2_identity {
    void (*resetIdentity_start)();
    void (*resetIdentity_end)(int);
    void *(*getIdentity)(cdb2_hndl_tp *, int);
    void (*set_identity)(cdb2_hndl_tp *, const void *);
    void (*identity_create)();
    void (*identity_destroy)(int);
    int (*identity_valid)();
};

struct cdb2_publish_event {
    int (*publish_event)(char *, char *, int, char *, char *, char *, char *);
};

#if defined __cplusplus
}
#endif

#endif
