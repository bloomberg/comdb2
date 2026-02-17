/*
   Copyright 2017 Bloomberg Finance L.P.

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

#include "comdb2.h"
#include "comdb2_plugin.h"
#include "comdb2_appsock.h"
#include "net.h"

int gbl_forbid_remote_repopnewlrl = 1;

static int handle_repopnewlrl_request(comdb2_appsock_arg_t *arg)
{
    struct comdb2buf *sb;
    char lrl_fname_out[256];
    int rc;

    sb = arg->sb;

    if (gbl_forbid_remote_repopnewlrl && !is_connection_local(sb)) {
        logmsg(LOGMSG_ERROR, "%s: rejecting remote repopnewlrl request\n",
                __func__);
        arg->error = -1;
        return APPSOCK_RETURN_ERR;
    }

    if (((rc = cdb2buf_gets(lrl_fname_out, sizeof(lrl_fname_out), sb)) <= 0) ||
        (lrl_fname_out[rc - 1] != '\n')) {
        logmsg(LOGMSG_ERROR, "%s: I/O error reading out lrl fname\n", __func__);
        arg->error = -1;
        return APPSOCK_RETURN_ERR;
    }
    lrl_fname_out[rc - 1] = '\0';

    if (repopulate_lrl(lrl_fname_out)) {
        logmsg(LOGMSG_ERROR, "%s: repopulate_lrl failed\n", __func__);
        arg->error = -1;
        return APPSOCK_RETURN_ERR;
    }

    if (cdb2buf_printf(sb, "OK\n") < 0 || cdb2buf_flush(sb) < 0) {
        logmsg(LOGMSG_ERROR, "%s: failed to send done ack text\n", __func__);
        arg->error = -1;
        return APPSOCK_RETURN_ERR;
    }

    return APPSOCK_RETURN_OK;
}

comdb2_appsock_t repopnewlrl_plugin = {
    "repopnewlrl",             /* Name */
    "",                        /* Usage info */
    0,                         /* Execution count */
    0,                         /* Flags */
    handle_repopnewlrl_request /* Handler function */
};

#include "plugin.h"
