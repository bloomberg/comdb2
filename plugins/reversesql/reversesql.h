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

#ifndef INCLUDED_REVERSESQL_H
#define INCLUDED_REVERSESQL_H

typedef struct reverse_conn_handle_st {
    char *remote_dbname;
    char *remote_host;
    cdb2_hndl_tp *hndl;
    int done;
    int onqueue;
    int failed;

    pthread_mutex_t mu;
    pthread_cond_t cond;
    LINKC_T(struct reverse_conn_handle_st) lnk;
} reverse_conn_handle_tp;

reverse_conn_handle_tp *wait_for_reverse_conn(int timeout /*sec*/);

#endif /* INCLUDED_REVERSESQL_H */
