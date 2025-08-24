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
#include <ssl_glue.h>
#include <ssl_support.h>

extern char gbl_dbname[];
extern int gbl_nid_dbname;
extern SSL_CTX *gbl_ssl_ctx;
extern ssl_mode gbl_client_ssl_mode;
extern uint64_t gbl_ssl_num_full_handshakes;
extern uint64_t gbl_ssl_num_partial_handshakes;

extern int gbl_ssl_print_io_errors;

struct ssl_data {
    struct event *ev;
    int fd;
    int do_shutdown;
    char *origin;
    SSL *ssl;
    X509 *cert;
};

struct ssl_handshake_arg {
    struct event_base *base;
    struct ssl_data *ssl_data;
    ssl_evbuffer_cb *error_cb;
    ssl_evbuffer_cb *success_cb;
    void *data;
};

static void ssl_handshake_evbuffer(int fd, short what, void *data)
{
    struct ssl_handshake_arg *arg = data;
    struct ssl_data *ssl_data = arg->ssl_data;
    SSL *ssl = ssl_data->ssl;

    if (ssl_data->ev) {
        event_free(ssl_data->ev);
        ssl_data->ev = NULL;
    }
    ERR_clear_error();
    int rc = SSL_do_handshake(ssl);
    if (rc == 1) {
        if (SSL_session_reused(ssl)) {
            ATOMIC_ADD64(gbl_ssl_num_partial_handshakes, 1);
        } else {
            ATOMIC_ADD64(gbl_ssl_num_full_handshakes, 1);
        }
        ssl_data->cert = SSL_get_peer_certificate(ssl);
        ssl_data->do_shutdown = 0;
        arg->success_cb(arg->data); /* newsql_accept_ssl_success, net_accept_ssl_success, net_connect_ssl_success */
        free(arg);
        return;
    }
    int err = SSL_get_error(ssl, rc);
    switch (err) {
    case SSL_ERROR_WANT_READ:
        ssl_data->ev = event_new(arg->base, fd, EV_READ, ssl_handshake_evbuffer, arg);
        event_add(ssl_data->ev, NULL);
        return;
    case SSL_ERROR_WANT_WRITE:
        ssl_data->ev = event_new(arg->base, fd, EV_WRITE, ssl_handshake_evbuffer, arg);
        event_add(ssl_data->ev, NULL);
        return;
    case SSL_ERROR_SYSCALL:
        if (gbl_ssl_print_io_errors) {
            if (errno == 0) /* openssl 1.x bug: an errno 0 reported under SSL_ERROR_SYSCALL means EOF from peer */
                errno = ECONNRESET;
            logmsg(LOGMSG_ERROR, "%s:%d SSL_do_handshake fd:%d rc:%d err:%d errno:%d [%s]\n", __func__, __LINE__, fd,
                   rc, err, errno, strerror(errno));
        }
        break;
    default:
        if (gbl_ssl_print_io_errors)
            logmsg(LOGMSG_ERROR, "%s:%d SSL_do_handshake fd:%d rc:%d err:%d error_string:%s]\n", __func__, __LINE__, fd,
                   rc, err, ERR_error_string(ERR_get_error(), NULL));
        break;
    }
    arg->error_cb(arg->data); /* ssl_error_cb, accept_ssl_error_cb */
    free(arg);
}

static struct ssl_handshake_arg *new_ssl_handshake(struct ssl_data *ssl_data, struct event_base *base, ssl_evbuffer_cb *error_cb, ssl_evbuffer_cb *success_cb, void *data)
{
    ssl_data->ssl = SSL_new(gbl_ssl_ctx);
    if (!ssl_data->ssl) return NULL;

    SSL_set_mode(ssl_data->ssl, SSL_MODE_ENABLE_PARTIAL_WRITE);
    SSL_set_fd(ssl_data->ssl, ssl_data->fd);

    struct ssl_handshake_arg *arg = malloc(sizeof(*arg));
    arg->base = base;
    arg->data = data;
    arg->error_cb = error_cb;
    arg->success_cb = success_cb;
    arg->ssl_data = ssl_data;
    return arg;
}

void accept_ssl_evbuffer(struct ssl_data *ssl_data, struct event_base *base, ssl_evbuffer_cb *error_cb, ssl_evbuffer_cb *success_cb, void *data)
{
    struct ssl_handshake_arg *arg = new_ssl_handshake(ssl_data, base, error_cb, success_cb, data);
    if (!arg) {
        error_cb(data);
        return;
    }
    SSL_set_accept_state(ssl_data->ssl);
    ssl_handshake_evbuffer(ssl_data->fd, EV_READ, arg);
}

void connect_ssl_evbuffer(struct ssl_data *ssl_data, struct event_base *base, ssl_evbuffer_cb *error_cb, ssl_evbuffer_cb *success_cb, void *data)
{
    struct ssl_handshake_arg *arg = new_ssl_handshake(ssl_data, base, error_cb, success_cb, data);
    if (!arg) {
        error_cb(data);
        return;
    }
    SSL_set_connect_state(ssl_data->ssl);
    ssl_handshake_evbuffer(ssl_data->fd, EV_READ, arg);
}

int rd_ssl_evbuffer(struct evbuffer *rd_buf, struct ssl_data *ssl_data, int *eof)
{
    SSL *ssl = ssl_data->ssl;
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
    case SSL_ERROR_ZERO_RETURN:
        *eof = 1;
        /* Do not perform shutdown unless client has done a clean shutdown first */
        ssl_data->do_shutdown = 1;
        /* fallthrough */
    case SSL_ERROR_WANT_READ: return 1;
    case SSL_ERROR_WANT_WRITE:
        logmsg(LOGMSG_ERROR, "%s:%d SSL_read rc:%d err:%d SSL_ERROR_WANT_WRITE]\n",
               __func__, __LINE__, rc, err);
        break;
    case SSL_ERROR_SYSCALL:
        if (gbl_ssl_print_io_errors) {
            if (errno == 0)
                errno = ECONNRESET;
            logmsg(LOGMSG_ERROR, "%s:%d SSL_read rc:%d err:%d errno:%d [%s]\n",
                   __func__, __LINE__, rc, err, errno, strerror(errno));
        }
        break;
    default:
        if (gbl_ssl_print_io_errors)
            logmsg(LOGMSG_ERROR, "%s:%d SSL_read rc:%d err:%d [%s]\n", __func__, __LINE__, rc, err,
                   ERR_error_string(ERR_get_error(), NULL));
        break;
    }
    return rc;
}

int wr_ssl_evbuffer(struct ssl_data *ssl_data, struct evbuffer *wr_buf)
{
    SSL *ssl = ssl_data->ssl;
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
        if (gbl_ssl_print_io_errors) {
            if (errno == 0)
                errno = ECONNRESET;
            logmsg(LOGMSG_ERROR, "%s:%d SSL_write rc:%d err:%d errno:%d [%s]\n",
                   __func__, __LINE__, rc, err, errno, strerror(errno));
        }
        break;
    default:
        if (gbl_ssl_print_io_errors)
            logmsg(LOGMSG_ERROR, "%s:%d SSL_write rc:%d err:%d [%s]\n", __func__, __LINE__, rc, err,
                   ERR_error_string(ERR_get_error(), NULL));
        break;
    }
    return rc;
}

static int verify_dbname(X509 *cert)
{
    return cert ? ssl_verify_dbname(cert, gbl_dbname, gbl_nid_dbname) : -1;
}

static int verify_hostname(int fd, X509 *cert)
{
    return cert ? ssl_verify_hostname(cert, fd) : -1;
}

int verify_ssl_evbuffer(struct ssl_data *ssl_data, ssl_mode mode)
{
    X509 *cert = ssl_data->cert;
    if (ssl_whitelisted(ssl_data->origin)) {
        return 0; /* skip certificate check for local connections */
    }
    switch (mode) {
    case SSL_PREFER_VERIFY_DBNAME:
    case SSL_VERIFY_DBNAME: if (verify_dbname(cert) != 0) return -1; // fallthrough
    case SSL_PREFER_VERIFY_HOSTNAME:
    case SSL_VERIFY_HOSTNAME: if (verify_hostname(ssl_data->fd, cert) != 0) return -1; // fallthrough
    case SSL_PREFER_VERIFY_CA:
    case SSL_VERIFY_CA: if (!cert) return -1; // fallthrough
    default: return 0;
    }
}

void ssl_data_free(struct ssl_data *ssl_data)
{
    if (!ssl_data) return;
    if (ssl_data->ev) event_free(ssl_data->ev);
    if (!ssl_data->do_shutdown) /* fail fast and make session reusable */
        SSL_set_shutdown(ssl_data->ssl, SSL_SENT_SHUTDOWN);
    else if (SSL_shutdown(ssl_data->ssl) == 0)
        SSL_shutdown(ssl_data->ssl);

    SSL_free(ssl_data->ssl);
    X509_free(ssl_data->cert);
    free(ssl_data);
}

struct ssl_data *ssl_data_new(int fd, char *origin)
{
    struct ssl_data *ssl_data = calloc(1, sizeof(struct ssl_data));
    ssl_data->fd = fd;
    ssl_data->origin = origin;
    return ssl_data;
}

int ssl_data_has_ssl(struct ssl_data *ssl_data)
{
    return ssl_data && ssl_data->ssl;
}

int ssl_data_has_cert(struct ssl_data *ssl_data)
{
    return ssl_data && ssl_data->cert;
}

int ssl_data_cert(struct ssl_data *ssl_data, int nid, void *out, int outsz)
{
    if (!ssl_data || !ssl_data->cert) return EINVAL;
    return ssl_x509_get_attr(ssl_data->cert, nid, out, outsz);
}

int ssl_data_pending(struct ssl_data *ssl_data)
{
    return ssl_data && ssl_data->ssl && SSL_pending(ssl_data->ssl);
}
