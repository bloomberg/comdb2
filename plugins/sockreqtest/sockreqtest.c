/*
   Copyright 2026 Bloomberg Finance L.P.

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
  Test-only appsock that submits a legacy block request as a REQ_SOCKREQUEST,
  the way Bloomberg's socket_request plugin does.  That request type is the
  only one a replicant forwards to the master (offload_comm_send_blockreq), so
  without it the forward-and-wait-for-reply path has no caller in the
  open-source tree and cannot be tested end to end.

  One request per invocation.  The client sends

      sockreqtest\n
      <int32, big-endian, length of the request>
      <the legacy block request>

  and reads back a sockrsp_t header followed by followlen bytes of block
  response -- the reply framing sndbak_open_socket() already produces.
*/

#include <pthread.h>
#include <time.h>

#include "comdb2.h"
#include "comdb2_appsock.h"
#include "comdb2_plugin.h"
#include "comdb2buf.h"
#include "pool.h"
#include "socket_interfaces.h"

extern pthread_mutex_t buf_lock;
extern pool_t *p_slocks;

uint8_t *get_bigbuf(void);

/* Seconds to wait for the block processor, including the round trip to the
   master when we are not it. */
#define MAX_WAIT_SEC 60

/* Reads the request into p_buf (always MAX_BUFFER_SIZE).  Returns its length,
   or -1 if the client sent a bad length or went away. */
static int read_request(COMDB2BUF *sb, uint8_t *p_buf)
{
    uint8_t lenbuf[sizeof(int)];
    int len;

    if (cdb2buf_fread((char *)lenbuf, 1, sizeof(lenbuf), sb) != sizeof(lenbuf))
        return -1;
    buf_get(&len, sizeof(len), lenbuf, lenbuf + sizeof(lenbuf));
    if (len <= 0 || len > MAX_BUFFER_SIZE)
        return -1;
    if (cdb2buf_fread((char *)p_buf, 1, len, sb) != len)
        return -1;
    return len;
}

/* Called with req_lock held.  Returns 0 once the block processor has written
   the reply to our socket, -1 if it never did. */
static int wait_for_reply(struct buf_lock_t *p_slock)
{
    int nsec = 0;

    while (p_slock->reply_state != REPLY_STATE_DONE) {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += 1;
        pthread_cond_timedwait(&p_slock->wait_cond, &p_slock->req_lock, &ts);
        if (++nsec > MAX_WAIT_SEC && p_slock->reply_state != REPLY_STATE_DONE) {
            p_slock->reply_state = REPLY_STATE_DISCARD;
            logmsg(LOGMSG_ERROR, "%s: timed out waiting for block reply\n", __func__);
            return -1;
        }
    }
    return 0;
}

int handle_sockreqtest_request(comdb2_appsock_arg_t *arg)
{
    char fromtask[] = "sockreqtest";
    COMDB2BUF *sb = arg->sb;
    struct buf_lock_t *p_slock;
    int rc, timedout = 0;

    if (arg->keepsocket)
        *arg->keepsocket = 0;

    Pthread_mutex_lock(&buf_lock);
    p_slock = pool_getablk(p_slocks);
    Pthread_mutex_unlock(&buf_lock);

    if (p_slock == NULL) {
        arg->error = -1;
        return APPSOCK_RETURN_ERR;
    }

    Pthread_mutex_init(&p_slock->req_lock, NULL);
    Pthread_cond_init(&p_slock->wait_cond, NULL);
    p_slock->sb = sb;
    p_slock->reply_state = REPLY_STATE_NA;
    p_slock->bigbuf = get_bigbuf();

    if (p_slock->bigbuf == NULL) {
        rc = ERR_INTERNAL;
        goto err;
    }

    if (read_request(sb, p_slock->bigbuf) < 0) {
        rc = ERR_BADREQ;
        goto err;
    }

    Pthread_mutex_lock(&p_slock->req_lock);
    /* The block processor builds its reply in the same buffer, so hand it the
       whole thing rather than just the bytes the client sent. */
    rc = handle_buf_main(thedb, sb, p_slock->bigbuf, p_slock->bigbuf + MAX_BUFFER_SIZE - 4, 0,
                         get_origin_mach_by_buf(sb), 0, fromtask, NULL, REQ_SOCKREQUEST, p_slock, 0, 0, NULL);
    if (rc == 0) {
        timedout = (wait_for_reply(p_slock) != 0);
        /* Dispatched: the block processor owns bigbuf and has either freed it
           or, if we gave up, will free it along with p_slock itself. */
        p_slock->bigbuf = NULL;
    }
    Pthread_mutex_unlock(&p_slock->req_lock);

    if (timedout) {
        arg->error = -1;
        return APPSOCK_RETURN_ERR;
    }

    cleanup_lock_buffer(p_slock);
    if (rc != 0) {
        /* handle_buf_main() has already sent the error reply. */
        arg->error = -1;
        return APPSOCK_RETURN_ERR;
    }
    return APPSOCK_RETURN_CONT;

err:
    sndbak_open_socket(sb, NULL, 0, rc);
    cleanup_lock_buffer(p_slock);
    arg->error = -1;
    return APPSOCK_RETURN_ERR;
}

APPSOCK_PLUGIN_DESC(sockreqtest);

#include "plugin.h"
