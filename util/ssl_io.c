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

#include <alloca.h>
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>
#include <fcntl.h>

#include <hostname_support.h>
#include <ssl_glue.h>

static int is_certificate_error(unsigned long err)
{
    int sslerrreason = ERR_GET_REASON(err);
    switch (sslerrreason) {
    case SSL_R_CERTIFICATE_VERIFY_FAILED:
    case SSL_R_SSLV3_ALERT_UNSUPPORTED_CERTIFICATE:
    case SSL_R_SSLV3_ALERT_BAD_CERTIFICATE:
    case SSL_R_SSLV3_ALERT_CERTIFICATE_EXPIRED:
    case SSL_R_SSLV3_ALERT_CERTIFICATE_REVOKED:
    case SSL_R_SSLV3_ALERT_CERTIFICATE_UNKNOWN:
    case SSL_R_SSLV3_ALERT_NO_CERTIFICATE:
        return 1;
    default:
        return 0;
    }
}

static void handle_ssl_lib_errors(COMDB2BUF *sb, int sslliberr, int nwritten)
{
    switch (sslliberr) {
    case SSL_ERROR_ZERO_RETURN:
        /* Peer has done a clean shutdown. */
        sslio_close(sb, 1);
        break;
    case SSL_ERROR_SYSCALL:
        sb->protocolerr = 0;
        if (nwritten == 0 || errno == 0 || errno == ECONNRESET || errno == EPIPE) {
            ssl_sfeprint(sb->sslerr, sizeof(sb->sslerr), my_ssl_eprintln, "Unexpected EOF observed.");
            errno = ECONNRESET;
        } else {
            ssl_sfeprint(sb->sslerr, sizeof(sb->sslerr), my_ssl_eprintln, "IO error. errno %d.", errno);
        }
        sslio_free(sb);
        break;
    case SSL_ERROR_SSL:
        errno = EIO;
        /* OpenSSL may throw random SSL_ERROR_SSL errors (e.g., SSL_R_SSL_HANDSHAKE_FAILURE)
           during turnaround when peer is being brought down. Let API retry on those errors.
           Treat only a certificate error as a protocol error. */
        sb->protocolerr = is_certificate_error(ERR_peek_error());
        ssl_sfliberrprint(sb->sslerr, sizeof(sb->sslerr), my_ssl_eprintln, "A failure in SSL library occured");
        sslio_free(sb);
        break;
    default: /* Unhandled errors */
        errno = EIO;
        sb->protocolerr = 1;
        ssl_sfeprint(sb->sslerr, sizeof(sb->sslerr), my_ssl_eprintln,
                     "Failed to establish connection with peer. SSL error = %d.", sslliberr);
        break;
    }
}

static int handle_sslio_rw_errors(COMDB2BUF *sb, int err, int nwritten, int *wantread, int *wantwrite)
{
    switch (err) {
    case SSL_ERROR_WANT_READ:
        sb->protocolerr = 0;
        errno = EAGAIN;
        if (wantread)
            *wantread = 1;
        if (wantwrite)
            *wantwrite = 0;
        return 0;
    case SSL_ERROR_WANT_WRITE:
        sb->protocolerr = 0;
        errno = EAGAIN;
        if (wantread)
            *wantread = 0;
        if (wantwrite)
            *wantwrite = 1;
        return 0;
    default:
        handle_ssl_lib_errors(sb, err, nwritten);
        return -1;
    }
}

SSL *CDB2BUF_FUNC(sslio_get_ssl)(COMDB2BUF *sb)
{
    return sb->ssl;
}

int CDB2BUF_FUNC(sslio_has_ssl)(COMDB2BUF *sb)
{
    return (sb != NULL && sb->ssl != NULL);
}

int CDB2BUF_FUNC(sslio_has_x509)(COMDB2BUF *sb)
{
    return (sb != NULL && sb->cert != NULL);
}

static int sslio_pollin(COMDB2BUF *sb)
{
    int rc;
    struct pollfd pol;

    if (SSL_pending(sb->ssl) > 0)
        return 1;

    do {
        pol.fd = sb->fd;
        pol.events = POLLIN;
        /* Don't wait for remote if the flag is set */
        rc = poll(&pol, 1, sb->nowait ? 0 : (sb->readtimeout == 0 ? -1 : sb->readtimeout));
    } while (rc == -1 && errno == EINTR);

    if (rc <= 0) /* timedout or error. */
        return rc;
    if ((pol.revents & POLLIN) == 0)
        return -100000 + pol.revents;

    /* Can read. */
    return 1;
}

static int sslio_pollout(COMDB2BUF *sb)
{
    int rc;
    struct pollfd pol;

    do {
        pol.fd = sb->fd;
        pol.events = POLLOUT;
        /* A writetimeout of 0 actually means an infinite poll timeout. */
        rc = poll(&pol, 1, sb->writetimeout == 0 ? -1 : sb->writetimeout);
    } while (rc == -1 && errno == EINTR);

    if (rc <= 0) /* timedout or error. */
        return rc;
    if ((pol.revents & POLLOUT) == 0)
        return -100000 + pol.revents;

    /* Can write. */
    return 1;
}

int CDB2BUF_FUNC(sslio_x509_attr)(COMDB2BUF *sb, int nid, char *out, size_t len)
{
    if (sb == NULL || sb->cert == NULL)
        return EINVAL;
    return ssl_x509_get_attr(sb->cert, nid, out, len);
}

static int ssl_verify(COMDB2BUF *sb, ssl_mode mode, const char *dbname, int nid)
{
    int rc = 0;
#if SBUF2_SERVER
    extern int gbl_ssl_allow_localhost;
    if (gbl_ssl_allow_localhost) {
        char host[NI_MAXHOST];
        if (get_hostname_by_fileno_v2(sb->fd, host, sizeof(host)) == 0) {
            if (ssl_whitelisted(host)) {
                /* skip certificate check for local connections */
                return 0;
            }
        }
    }
#endif
    if (sb->ssl != NULL && SSL_NEEDS_VERIFICATION(mode)) {
        /* Convert SSL_PREFER_VERIFY_XXX to SSL_VERIFY_XXX */
        if (SSL_IS_OPTIONAL(mode))
            mode += (SSL_REQUIRE - SSL_PREFER);
        sb->cert = SSL_get_peer_certificate(sb->ssl);
        if (sb->cert == NULL) {
            ssl_sfeprint(sb->sslerr, sizeof(sb->sslerr), my_ssl_eprintln,
                         "Could not get peer certificate.");
            rc = EIO;
        } else if (mode >= SSL_VERIFY_HOSTNAME && ssl_verify_hostname(sb->cert, sb->fd) != 0) {
            /* set rc to error out. */
            rc = EACCES;
            ssl_sfeprint(sb->sslerr, sizeof(sb->sslerr), my_ssl_eprintln,
                         "Certificate does not match host name.");
        } else if (mode >= SSL_VERIFY_DBNAME &&
                   ssl_verify_dbname(sb->cert, dbname, nid) != 0) {
            rc = EACCES;
            ssl_sfeprint(sb->sslerr, sizeof(sb->sslerr), my_ssl_eprintln,
                         "Certificate does not match database name.");
        }
    }
    return rc;
}

#ifdef SSL_DEBUG
static void my_apps_ssl_info_callback(const SSL *s, int where, int ret)
{
    const char *str;
    int w;

    w = where & ~SSL_ST_MASK;

    if (w & SSL_ST_CONNECT)
        str = "SSL_connect";
    else if (w & SSL_ST_ACCEPT)
        str = "SSL_accept";
    else
        str = "undefined";

    if (where & SSL_CB_LOOP) {
        fprintf(stderr, "%s:%s\n", str, SSL_state_string_long(s));
    } else if (where & SSL_CB_ALERT) {
        str = (where & SSL_CB_READ) ? "read" : "write";
        fprintf(stderr, "SSL3 alert %s:%s:%s\n", str,
                SSL_alert_type_string_long(ret),
                SSL_alert_desc_string_long(ret));
    } else if (where & SSL_CB_EXIT) {
        if (ret == 0)
            fprintf(stderr, "%s:failed in %s\n", str, SSL_state_string_long(s));
        else if (ret < 0) {
            fprintf(stderr, "%s:error in %s\n", str, SSL_state_string_long(s));
        }
    }
}
#endif

static int sslio_accept_or_connect(COMDB2BUF *sb, SSL_CTX *ctx,
                                   int (*SSL_func)(SSL *), ssl_mode verify,
                                   const char *dbname, int nid,
                                   SSL_SESSION *sess, int close_on_verify_error)
{
    int rc, ioerr, fd, flags;

    /* If SSL does not exist, return an error. */
    if (ctx == NULL) {
        ssl_sfeprint(sb->sslerr, sizeof(sb->sslerr), my_ssl_eprintln,
                     "SSL context does not exist.");
        return EPERM;
    }

    if (sb->ssl != NULL) {
        ssl_sfeprint(sb->sslerr, sizeof(sb->sslerr), my_ssl_eprintln,
                     "SSL connection has been established already.");
        return EPERM;
    }

    /* Create an SSL connection. */
    sb->ssl = SSL_new(ctx);
    if (sb->ssl == NULL) {
        ssl_sfliberrprint(sb->sslerr, sizeof(sb->sslerr), my_ssl_eprintln,
                          "Failed to create SSL connection");
        rc = ERR_get_error();
        goto error;
    }

    /* Set fd. */
    fd = cdb2buf_fileno(sb);
    if ((flags = fcntl(fd, F_GETFL, 0)) < 0 ||
        (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0)) {
        ssl_sfeprint(sb->sslerr, sizeof(sb->sslerr), my_ssl_eprintln,
                     "fcntl: (%d) %s", errno, strerror(errno));
        rc = -1;
        goto error;
    }
    rc = SSL_set_fd(sb->ssl, fd);
    if (rc != 1) {
        ssl_sfliberrprint(sb->sslerr, sizeof(sb->sslerr), my_ssl_eprintln,
                          "Failed to set fd");
        goto error;
    }

#ifdef SSL_DEBUG
    SSL_set_info_callback(sb->ssl, my_apps_ssl_info_callback);
#endif

    if (sess != NULL)
        SSL_set_session(sb->ssl, sess);

    /* accept/connect SSL connection. */
re_accept_or_connect:
    ERR_clear_error();
    rc = SSL_func(sb->ssl);
    if (rc != 1) {
        /* Handle SSL error code. */
        ioerr = SSL_get_error(sb->ssl, rc);

        switch (ioerr) {
        case SSL_ERROR_WANT_READ: /* Renegotiate */
            rc = sslio_pollin(sb);
            if (rc > 0)
                goto re_accept_or_connect;
            sb->protocolerr = 0;
            break;
        case SSL_ERROR_WANT_WRITE: /* Renegotiate */
            rc = sslio_pollout(sb);
            if (rc > 0)
                goto re_accept_or_connect;
            sb->protocolerr = 0;
            break;
        default:
            handle_ssl_lib_errors(sb, ioerr, rc);
            break;
        }
    } else if (ssl_verify(sb, verify, dbname, nid) != 0) {
        sb->protocolerr = 1;
        rc = EACCES;
    } else {
        sb->protocolerr = 0;
    }
    /* Put blocking back. */
    if (fcntl(fd, F_SETFL, flags) < 0) {
        ssl_sfeprint(sb->sslerr, sizeof(sb->sslerr), my_ssl_eprintln,
                     "fcntl: (%d) %s", errno, strerror(errno));
        return -1;
    }
    if (rc != 1 && close_on_verify_error) {
    error:
        if (sb->ssl != NULL) {
            SSL_shutdown(sb->ssl);
            SSL_free(sb->ssl);
            sb->ssl = NULL;
        }
        if (sb->cert) {
            X509_free(sb->cert);
            sb->cert = NULL;
        }
    }

    return rc;
}

int CDB2BUF_FUNC(sslio_accept)(COMDB2BUF *sb, SSL_CTX *ctx, ssl_mode mode,
                             const char *dbname, int nid,
                             int close_on_verify_error)
{
    return sslio_accept_or_connect(sb, ctx, SSL_accept, mode, dbname, nid, NULL,
                                   close_on_verify_error);
}

#if SBUF2_SERVER
int CDB2BUF_FUNC(sslio_connect)(COMDB2BUF *sb, SSL_CTX *ctx, ssl_mode mode,
                              const char *dbname, int nid,
                              int close_on_verify_error)
{
    return sslio_accept_or_connect(sb, ctx, SSL_connect, mode, dbname, nid,
                                   NULL, close_on_verify_error);
}
#else
int CDB2BUF_FUNC(sslio_connect)(COMDB2BUF *sb, SSL_CTX *ctx, ssl_mode mode,
                              const char *dbname, int nid, SSL_SESSION *sess)
{
    return sslio_accept_or_connect(sb, ctx, SSL_connect, mode, dbname, nid,
                                   sess, 1);
}
#endif

int CDB2BUF_FUNC(sslio_read)(COMDB2BUF *sb, char *cc, int len)
{
    int n, ioerr, wantread;

    wantread = 1;

reread:
    ERR_clear_error();
    n = wantread ? sslio_pollin(sb) : sslio_pollout(sb);
    if (n <= 0)
        return n;

    n = SSL_read(sb->ssl, cc, len);
    if (n <= 0) {
        ioerr = SSL_get_error(sb->ssl, n);
        if (handle_sslio_rw_errors(sb, ioerr, n, &wantread, NULL) == 0)
            goto reread;
    }
    return n;
}

int CDB2BUF_FUNC(sslio_write)(COMDB2BUF *sb, const char *cc, int len)
{
    int n, ioerr, wantwrite;

    wantwrite = 1;

rewrite:
    ERR_clear_error();
    n = wantwrite ? sslio_pollout(sb) : sslio_pollin(sb);
    if (n <= 0)
        return n;

    n = SSL_write(sb->ssl, cc, len);
    if (n <= 0) {
        ioerr = SSL_get_error(sb->ssl, n);
        if (handle_sslio_rw_errors(sb, ioerr, n, NULL, &wantwrite) == 0)
            goto rewrite;
    }
    return n;
}

int CDB2BUF_FUNC(sslio_close)(COMDB2BUF *sb, int reuse)
{
    /* Upon success, the 1st call to SSL_shutdown
       returns 0, and the 2nd returns 1. */
    int rc = 0;
    if (sb->ssl == NULL)
        return 0;

    if (!reuse)
        SSL_set_shutdown(sb->ssl, SSL_SENT_SHUTDOWN);
    else {
        rc = SSL_shutdown(sb->ssl);
        if (rc == 0)
            rc = SSL_shutdown(sb->ssl);
        if (rc == 1)
            rc = 0;
    }

    sslio_free(sb);
    return rc;
}

int CDB2BUF_FUNC(sslio_pending)(COMDB2BUF *sb)
{
    return SSL_pending(sb->ssl);
}

void CDB2BUF_FUNC(sslio_free)(COMDB2BUF *sb)
{
    if (sb->ssl == NULL)
        return;
    X509_free(sb->cert);
    sb->cert = NULL;
    SSL_free(sb->ssl);
    sb->ssl = NULL;
}
