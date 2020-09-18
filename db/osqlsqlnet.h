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

#ifndef INCLUDED_OSQLSQLNET_H
#define INCLUDED_OSQLSQLNET_H

/**
 * Interface between sqlite engine and bplog. It is the legacy mode of
 * intra-cluster bplog transfer to master.  It multiplexes the transactions over
 * the existing "net" fully connected mesh. Multiplexing is supported on
 * replicants by a checkboard hashing the running sessions, and on master by a
 * repository
 *
 */

/**
 * Init bplog over net master side
 *
 */
void init_bplog_net(osql_target_t *target);

/**
 * Handle to registration of thread for net multiplex purposes
 *
 */
int osql_begin_net(struct sqlclntstate *clnt, int type, int keep_rqid);

/**
 * End the osql transaction
 * Unregisted the sql thread
 *
 */
int osql_end_net(struct sqlclntstate *clnt);

/**
 * Send messages over net
 *
 */
int offload_net_send(const char *host, int usertype, void *data, int datalen,
                     int nodelay, void *tail, int tailen);

#endif /* !INCLUDED_OSQLSQLNET_H */
