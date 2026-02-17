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

/* code needed to support various comdb2 interfaces to the sql engine */

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stddef.h>
#include <pthread.h>
#include <epochlib.h>

#include <plhash_glue.h>
#include <segstr.h>

#include <list.h>

#include <comdb2buf.h>
#include <bdb_api.h>

#include "comdb2.h"
#include "types.h"
#include "tag.h"
#include "block_internal.h"
#include "socket_interfaces.h"
#include <endian_core.h>

#include <sys/time.h>
#include <logmsg.h>
#include <net_appsock.h>

extern int cdb2buf_write(char *ptr, int nbytes, COMDB2BUF *sb);

static int send_ack_reply(COMDB2BUF *sb, int rcode, u_char *buf, int buflen);

static const uint8_t *sockreq_type_get(sockreq_t *p_sockreq,
                                       const uint8_t *p_buf,
                                       const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || SOCKREQ_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_sockreq->request), sizeof(p_sockreq->request), p_buf,
                    p_buf_end);
    p_buf = buf_get(&(p_sockreq->flags), sizeof(p_sockreq->flags), p_buf,
                    p_buf_end);
    p_buf =
        buf_get(&(p_sockreq->parm), sizeof(p_sockreq->parm), p_buf, p_buf_end);
    p_buf = buf_get(&(p_sockreq->fromcpu), sizeof(p_sockreq->fromcpu), p_buf,
                    p_buf_end);
    p_buf = buf_no_net_get(&(p_sockreq->fromtask), sizeof(p_sockreq->fromtask),
                           p_buf, p_buf_end);
    p_buf = buf_get(&(p_sockreq->followlen), sizeof(p_sockreq->followlen),
                    p_buf, p_buf_end);

    return p_buf;
}

static uint8_t *sockrsp_type_put(const sockrsp_t *p_sockrsp, uint8_t *p_buf,
                                 const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || SOCKRSP_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_sockrsp->response), sizeof(p_sockrsp->response), p_buf,
                    p_buf_end);
    p_buf = buf_put(&(p_sockrsp->flags), sizeof(p_sockrsp->flags), p_buf,
                    p_buf_end);
    p_buf = buf_put(&(p_sockrsp->rcode), sizeof(p_sockrsp->rcode), p_buf,
                    p_buf_end);
    p_buf =
        buf_put(&(p_sockrsp->parm), sizeof(p_sockrsp->parm), p_buf, p_buf_end);
    p_buf = buf_put(&(p_sockrsp->followlen), sizeof(p_sockrsp->followlen),
                    p_buf, p_buf_end);

    return p_buf;
}

static int send_ack_reply(COMDB2BUF *sb, int rcode, u_char *buf, int buflen)
{
    uint8_t rspbf[SOCKRSP_LEN];
    uint8_t *p_hdr_buf;
    uint8_t *p_hdr_buf_end;
    sockrsp_t rrsp;
    int rc = 0;
    memset(&rrsp, 0, sizeof(sockrsp_t));
    rrsp.response = 0;
    rrsp.flags = 0;

    if (active_appsock_conns >=
        bdb_attr_get(thedb->bdb_attr, BDB_ATTR_MAXSOCKCACHED))
        rrsp.flags |= FRESP_FLAG_CLOSE;

    switch (rcode) {
    /* this may need to get refined..attempt to simulate retries */
    case ERR_NESTED:
    case ERR_NOMASTER:
    case ERR_CORRUPT:
    case ERR_SC_COMMIT:
    case ERR_INTERNAL:
    case ERR_REJECTED:
        rrsp.rcode = RC_TRAN_CLIENT_RETRY;
        break;
    default:
        rrsp.rcode = rcode;
        break;
    }

    rrsp.parm = 0;
    if (buflen > 0)
        rrsp.followlen = buflen;
    p_hdr_buf = (uint8_t *)rspbf;
    p_hdr_buf_end = p_hdr_buf + SOCKRSP_LEN * sizeof(uint8_t);

    if (!(sockrsp_type_put(&rrsp, p_hdr_buf, p_hdr_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s: sockrsp_type_put returns NULL!\n", __func__);
        return -1;
    }

    rc = cdb2buf_write((char *)&rspbf, sizeof(rspbf), sb);
    if (rc != sizeof(rrsp)) {
        return -1;
    }
    if (buflen > 0) {
        rc = cdb2buf_write((char *)buf, buflen, sb);
        if (rc != buflen) {
            return -1;
        }
    }
    rc = cdb2buf_flush(sb);
    return 0;
}

int sndbak_socket(COMDB2BUF *sb, u_char *buf, int buflen, int rc)
{
    int rcd = 0;
    rcd = send_ack_reply(sb, rc, buf, buflen);
    close_appsock(sb);
    return rcd;
}

int sndbak_open_socket(COMDB2BUF *sb, u_char *buf, int buflen, int rc)
{
    return send_ack_reply(sb, rc, buf, buflen);
}
