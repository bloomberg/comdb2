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

/**
 * Interface between sqlite engine and bplog. It is the legacy mode of
 * intra-cluster bplog transfer to master.  It multiplexes the transactions over
 * the existing "net" fully connected mesh. Multiplexing is supported on
 * replicants by a checkboard hashing the running sessions, and on master by a
 * repository
 *
 */

#include "sql.h"
#include "osqlcheckboard.h"
#include "osqlcomm.h"

/**
 * Handle to registration of thread for net multiplex purposes
 *
 */ /* loop in caller */
int osql_begin_net(struct sqlclntstate *clnt, int type, int keep_rqid)
{
    osqlstate_t *osql = &clnt->osql;
    int rc;

    /* register the session */
    osql->target_host = thedb->master;

    /* protect against no master */
    if (osql->target_host == NULL || osql->target_host == db_eid_invalid)
        return 0; /* loop in caller */

    if (!keep_rqid) {
        /* register this new member */
        rc = osql_register_sqlthr(clnt, type);
    } else {
        /* this is a replay with same rqid, already registered */
        /* sets to the same node */
        rc = osql_reuse_sqlthr(clnt, osql->target_host);
    }
    if (rc) {
        sql_debug_logf(clnt, __func__, __LINE__, "fail to %s rc %d\n",
                       keep_rqid ? "reuse" : "register", rc);
        return -1;
    }

    return 0;
}

/**
 * End the osql transaction
 * Unregisted the sql thread
 *
 */
int osql_end_net(struct sqlclntstate *clnt)
{
    return osql_unregister_sqlthr(clnt);
}
