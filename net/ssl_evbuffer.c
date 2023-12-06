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

#include <string.h>

#include <event2/buffer.h>
#include <event2/event.h>
#include <openssl/err.h>
#include <openssl/ssl.h>

#include <comdb2_atomic.h>
#include <logmsg.h>

#include <net_appsock.h>
#include <ssl_evbuffer.h>

extern SSL_CTX *gbl_ssl_ctx;
extern uint64_t gbl_ssl_num_full_handshakes;
extern uint64_t gbl_ssl_num_partial_handshakes;

struct ssl_handshake_arg {
    struct event_base *base;
    void *data;
    ssl_evbuffer_cb *error_cb;
    ssl_evbuffer_cb *success_cb;
    SSL *ssl;
};

static void ssl_handshake_evbuffer(int fd, short what, void *data)
{
    struct ssl_handshake_arg *arg = data;
    SSL *ssl = arg->ssl;
    ERR_clear_error();
    int rc = SSL_do_handshake(ssl);
    if (rc == 1) {
        if (SSL_session_reused(ssl)) {
            ATOMIC_ADD64(gbl_ssl_num_partial_handshakes, 1);
        } else {
            ATOMIC_ADD64(gbl_ssl_num_full_handshakes, 1);
        }
        arg->success_cb(arg->data);
        free(arg);
        return;
    }
    int err = SSL_get_error(ssl, rc);
    switch (err) {
    case SSL_ERROR_WANT_READ:
        event_base_once(arg->base, fd, EV_READ, ssl_handshake_evbuffer, arg, NULL);
        return;
    case SSL_ERROR_WANT_WRITE:
        event_base_once(arg->base, fd, EV_WRITE, ssl_handshake_evbuffer, arg, NULL);
        return;
    case SSL_ERROR_SYSCALL:
        logmsg(LOGMSG_ERROR, "%s:%d SSL_do_handshake rc:%d err:%d errno:%d [%s]\n",
               __func__, __LINE__, rc, err, errno, strerror(errno));
        break;
    default:
        logmsg(LOGMSG_ERROR, "%s:%d SSL_do_handshake rc:%d err:%d error_string:%s]\n",
               __func__, __LINE__, rc, err, ERR_error_string(err, NULL));
        break;
    }
    arg->error_cb(arg->data);
    free(arg);
}

static struct ssl_handshake_arg *new_ssl_handshake(SSL **ssl, int fd, struct event_base *base, ssl_evbuffer_cb *error_cb, ssl_evbuffer_cb *success_cb, void *data)
{
    struct ssl_handshake_arg *arg = malloc(sizeof(*arg));
    arg->base = base;
    arg->data = data;
    arg->error_cb = error_cb;
    arg->success_cb = success_cb;
    arg->ssl = *ssl = SSL_new(gbl_ssl_ctx);
    SSL_set_mode(arg->ssl, SSL_MODE_ENABLE_PARTIAL_WRITE);
    SSL_set_fd(arg->ssl, fd);
    return arg;
}

void accept_evbuffer_ssl(SSL **ssl, int fd, struct event_base *base, ssl_evbuffer_cb *error_cb, ssl_evbuffer_cb *success_cb, void *data)
{
    struct ssl_handshake_arg *arg = new_ssl_handshake(ssl, fd, base, error_cb, success_cb, data);
    SSL_set_accept_state(arg->ssl);
    ssl_handshake_evbuffer(fd, EV_READ, arg);
}

void connect_evbuffer_ssl(SSL **ssl, int fd, struct event_base *base, ssl_evbuffer_cb *error_cb, ssl_evbuffer_cb *success_cb, void *data)
{
    struct ssl_handshake_arg *arg = new_ssl_handshake(ssl, fd, base, error_cb, success_cb, data);
    SSL_set_connect_state(arg->ssl);
    ssl_handshake_evbuffer(fd, EV_READ, arg);
}

int rd_evbuffer_ssl(struct evbuffer *rd_buf, SSL *ssl, int *eof)
{
    *eof = 0;
    int len = KB(16);
    struct iovec v = {0};
    if (evbuffer_reserve_space(rd_buf, len, &v, 1) == -1) {
        return -1;
    }
    ERR_clear_error();
    int rc = SSL_read(ssl, v.iov_base, len);
    if (rc > 0) {
        v.iov_len = rc;
        evbuffer_commit_space(rd_buf, &v, 1);
        return rc;
    }
    int err = SSL_get_error(ssl, rc);
    switch (err) {
    case SSL_ERROR_ZERO_RETURN: *eof = 1; /* fallthrough */
    case SSL_ERROR_WANT_READ: return 1;
    case SSL_ERROR_WANT_WRITE:
        logmsg(LOGMSG_ERROR, "%s:%d SSL_read rc:%d err:%d SSL_ERROR_WANT_WRITE]\n",
               __func__, __LINE__, rc, err);
        break;
    case SSL_ERROR_SYSCALL:
        logmsg(LOGMSG_ERROR, "%s:%d SSL_read rc:%d err:%d errno:%d [%s]\n",
               __func__, __LINE__, rc, err, errno, strerror(errno));
        break;
    default:
        logmsg(LOGMSG_ERROR, "%s:%d SSL_read rc:%d err:%d [%s]\n",
               __func__, __LINE__, rc, err, ERR_error_string(err, NULL));
        break;
    }
    return rc;
}

int wr_evbuffer_ssl(SSL *ssl, struct evbuffer *wr_buf)
{
    int len = evbuffer_get_length(wr_buf);
    if (len > KB(16)) len = KB(16);
    const void *buf = evbuffer_pullup(wr_buf, len);
    ERR_clear_error();
    int rc = SSL_write(ssl, buf, len);
    if (rc > 0) {
        evbuffer_drain(wr_buf, rc);
        return rc;
    }
    int err = SSL_get_error(ssl, rc);
    switch (err) {
    case SSL_ERROR_WANT_WRITE:
        errno = EAGAIN;
        break;
    case SSL_ERROR_WANT_READ:
        logmsg(LOGMSG_ERROR, "%s:%d SSL_write rc:%d err:%d SSL_ERROR_WANT_READ]\n",
               __func__, __LINE__, rc, err);
        break;
    case SSL_ERROR_SYSCALL:
        logmsg(LOGMSG_ERROR, "%s:%d SSL_write rc:%d err:%d errno:%d [%s]\n",
               __func__, __LINE__, rc, err, errno, strerror(errno));
        break;
    default:
        logmsg(LOGMSG_ERROR, "%s:%d SSL_write rc:%d err:%d [%s]\n",
               __func__, __LINE__, rc, err, ERR_error_string(err, NULL));
        break;
    }
    return rc;
}
