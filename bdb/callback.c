/*
   Copyright 2015 Bloomberg Finance L.P.

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

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/socketvar.h>
#include <sys/uio.h>
#include <unistd.h>
#include <stddef.h>
#include <inttypes.h>
#include <stdint.h>

#include <build/db.h>
#include <epochlib.h>

#include <ctrace.h>

#include <net.h>
#include "bdb_int.h"
#include "locks.h"

bdb_callback_type *bdb_callback_create(void)
{
    bdb_callback_type *bdb_callback;

    bdb_callback = mymalloc(sizeof(bdb_callback_type));
    bzero(bdb_callback, sizeof(bdb_callback_type));

    return bdb_callback;
}

void bdb_callback_set(bdb_callback_type *bdb_callback, int callback_type,
                      BDBFP callback_rtn)
{
    switch (callback_type) {
    case BDB_CALLBACK_NODEUP:
        bdb_callback->nodeup_rtn = callback_rtn;
        break;
    case BDB_CALLBACK_GETROOM:
        bdb_callback->getroom_rtn = callback_rtn;
        break;
    case BDB_CALLBACK_WHOISMASTER:
        bdb_callback->whoismaster_rtn = callback_rtn;
        break;
    case BDB_CALLBACK_REPFAIL:
        bdb_callback->repfail_rtn = callback_rtn;
        break;
    case BDB_CALLBACK_APPSOCK:
        bdb_callback->appsock_rtn = callback_rtn;
        break;
    case BDB_CALLBACK_ADMIN_APPSOCK:
        bdb_callback->admin_appsock_rtn = callback_rtn;
        break;
    case BDB_CALLBACK_PRINT:
        bdb_callback->print_rtn = (PRINTFP)callback_rtn;
        break;
    case BDB_CALLBACK_ELECTSETTINGS:
        bdb_callback->electsettings_rtn = callback_rtn;
        break;
    case BDB_CALLBACK_CATCHUP:
        bdb_callback->catchup_rtn = (BDBCATCHUPFP)callback_rtn;
        break;
    case BDB_CALLBACK_THREADDUMP:
        bdb_callback->threaddump_rtn = (BDBTHREADDUMPFP)callback_rtn;
        break;

    case BDB_CALLBACK_SCDONE:
        bdb_callback->scdone_rtn = (SCDONEFP)callback_rtn;
        break;

    case BDB_CALLBACK_SCABORT:
        bdb_callback->scabort_rtn = (SCABORTFP)callback_rtn;
        break;

    case BDB_CALLBACK_NODE_IS_DOWN:
        bdb_callback->nodedown_rtn = (NODEDOWNFP)callback_rtn;
        break;
    case BDB_CALLBACK_SERIALCHECK:
        bdb_callback->serialcheck_rtn = (SERIALCHECK)callback_rtn;
        break;

    case BDB_CALLBACK_SYNCMODE:
        bdb_callback->syncmode_rtn = (SYNCMODE)callback_rtn;
        break;

    case BDB_CALLBACK_NODEUP_DRTEST:
        bdb_callback->nodeup_drtest_rtn = (NODEUP_DRTEST)callback_rtn;
        break;

    case BDB_CALLBACK_SIGNAL_LOGFILL:
        bdb_callback->signal_logfill_rtn = (SIGNAL_LOGFILL)callback_rtn;
        break;

    default:
        break;
    }
}
