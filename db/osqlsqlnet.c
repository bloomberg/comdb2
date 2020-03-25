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
#include "osqlbundled.h"

static int _send(osql_target_t *target, int usertype, void *data, int datalen, int nodelay, void *tail, int tailen,
                 int unused1, int unused2);

/**
 * Init bplog over net master side
 *
 */
void init_bplog_net(osql_target_t *target)
{
    target->type = OSQL_OVER_NET;
    target->sb = NULL;
    target->send = _send;
}

/**
 * Handle to registration of thread for net multiplex purposes
 *
 */ /* loop in caller */
int osql_begin_net(struct sqlclntstate *clnt, int type, int keep_rqid)
{
    osqlstate_t *osql = &clnt->osql;
    int rc;

    /* register the session */
    osql->target.type = OSQL_OVER_NET;
    osql->target.host = thedb->master;
    osql->target.send = _send;
    assert(osql->target.sb == NULL);

    init_bplog_bundled(&osql->target);

    /* protect against no master */
    if (osql->target.host == NULL || osql->target.host == db_eid_invalid)
        return 0; /* loop in caller */

    if (!keep_rqid) {
        /* register this new member */
        rc = osql_register_sqlthr(clnt, type);
    } else {
        /* this is a replay with same rqid, already registered */
        /* sets to the same node */
        rc = osql_reuse_sqlthr(clnt, osql->target.host);
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

static int _send(osql_target_t *target, int usertype, void *data, int datalen, int nodelay, void *tail, int tailen,
                 int unused1, int unused2)
{
    (void)unused1;
    (void)unused2;
    return offload_net_send(target->host, usertype, data, datalen, nodelay,
                            tail, tailen);
}
