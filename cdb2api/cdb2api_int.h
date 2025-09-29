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

#include "cdb2api.h"

int cdb2_socket_pool_get(cdb2_hndl_tp *hndl, const char *typestr, int dbnum, int *port);
void cdb2_socket_pool_donate_ext(cdb2_hndl_tp *hndl, const char *typestr, int fd, int ttl, int dbnum);

int cdb2_send_2pc(cdb2_hndl_tp *hndl, char *dbname, char *pname, char *ptier, char *source, unsigned int op,
                  char *dist_txnid, int rcode, int outrc, char *errmsg, int async);

#endif
