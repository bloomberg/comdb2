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

SSL *SBUF2_FUNC(sslio_get_ssl)(SBUF2 *sb)
{
    return sb->ssl;
}

int SBUF2_FUNC(sslio_has_ssl)(SBUF2 *sb)
{
    return (sb != NULL && sb->ssl != NULL);
}

int SBUF2_FUNC(sslio_has_x509)(SBUF2 *sb)
{
    return (sb != NULL && sb->cert != NULL);
}

static int sslio_pollin(SBUF2 *sb)
{
    int rc;
    struct pollfd pol;

    if (SSL_pending(sb->ssl) > 0)
        return 1;

    do {
        pol.fd = sb->fd;
        pol.events = POLLIN;
        /* A readtimeout of 0 actually means an infinite poll timeout. */
        rc = poll(&pol, 1, sb->readtimeout == 0 ? -1 : sb->readtimeout);
    } while (rc == -1 && errno == EINTR);

    if (rc <= 0) /* timedout or error. */
        return rc;
    if ((pol.revents & POLLIN) == 0)
        return -100000 + pol.revents;

    /* Can read. */
    return 1;
}

static int sslio_pollout(SBUF2 *sb)
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

int SBUF2_FUNC(sslio_x509_attr)(SBUF2 *sb, int nid, char *out, size_t len)
{
    if (sb == NULL || sb->cert == NULL)
        return EINVAL;
    return ssl_x509_get_attr(sb->cert, nid, out, len);
}

static int ssl_verify(SBUF2 *sb, ssl_mode mode, const char *dbname, int nid)
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

static int sslio_accept_or_connect(SBUF2 *sb, SSL_CTX *ctx,
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
    fd = sbuf2fileno(sb);
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
        case SSL_ERROR_SYSCALL:
            sb->protocolerr = 0;
            if (rc == 0) {
                ssl_sfeprint(sb->sslerr, sizeof(sb->sslerr), my_ssl_eprintln,
                             "Unexpected EOF observed.");
                errno = ECONNRESET;
            } else {
                ssl_sfeprint(sb->sslerr, sizeof(sb->sslerr), my_ssl_eprintln,
                             "IO error. errno %d.", errno);
            }
            break;
        case SSL_ERROR_SSL:
            errno = EIO;
            sb->protocolerr = 1;
            ssl_sfliberrprint(sb->sslerr, sizeof(sb->sslerr), my_ssl_eprintln,
                              "A failure in SSL library occured");
            break;
        default:
            errno = EIO;
            sb->protocolerr = 1;
            ssl_sfeprint(sb->sslerr, sizeof(sb->sslerr), my_ssl_eprintln,
                         "Failed to establish connection with peer. "
                         "SSL error = %d.",
                         ioerr);
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

int SBUF2_FUNC(sslio_accept)(SBUF2 *sb, SSL_CTX *ctx, ssl_mode mode,
                             const char *dbname, int nid,
                             int close_on_verify_error)
{
    return sslio_accept_or_connect(sb, ctx, SSL_accept, mode, dbname, nid, NULL,
                                   close_on_verify_error);
}

#if SBUF2_SERVER
int SBUF2_FUNC(sslio_connect)(SBUF2 *sb, SSL_CTX *ctx, ssl_mode mode,
                              const char *dbname, int nid,
                              int close_on_verify_error)
{
    return sslio_accept_or_connect(sb, ctx, SSL_connect, mode, dbname, nid,
                                   NULL, close_on_verify_error);
}
#else
int SBUF2_FUNC(sslio_connect)(SBUF2 *sb, SSL_CTX *ctx, ssl_mode mode,
                              const char *dbname, int nid, SSL_SESSION *sess)
{
    return sslio_accept_or_connect(sb, ctx, SSL_connect, mode, dbname, nid,
                                   sess, 1);
}
#endif

int SBUF2_FUNC(sslio_read)(SBUF2 *sb, char *cc, int len)
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
        switch (ioerr) {
        case SSL_ERROR_WANT_READ:
            sb->protocolerr = 0;
            errno = EAGAIN;
            wantread = 1;
            goto reread;
        case SSL_ERROR_WANT_WRITE:
            sb->protocolerr = 0;
            errno = EAGAIN;
            wantread = 0;
            goto reread;
        case SSL_ERROR_ZERO_RETURN:
            /* Peer has done a clean shutdown. */
            SSL_shutdown(sb->ssl);
            SSL_free(sb->ssl);
            sb->ssl = NULL;
            if (sb->cert) {
                X509_free(sb->cert);
                sb->cert = NULL;
            }
            break;
        case SSL_ERROR_SYSCALL:
            sb->protocolerr = 0;
            if (n == 0) {
                ssl_sfeprint(sb->sslerr, sizeof(sb->sslerr), my_ssl_eprintln,
                             "Unexpected EOF observed.");
                errno = ECONNRESET;
            } else {
                ssl_sfeprint(sb->sslerr, sizeof(sb->sslerr), my_ssl_eprintln,
                             "IO error. errno %d.", errno);
            }
            break;
        case SSL_ERROR_SSL:
            errno = EIO;
            sb->protocolerr = 1;
            ssl_sfliberrprint(sb->sslerr, sizeof(sb->sslerr), my_ssl_eprintln,
                              "A failure in SSL library occured");
            break;
        default:
            errno = EIO;
            sb->protocolerr = 1;
            ssl_sfeprint(sb->sslerr, sizeof(sb->sslerr), my_ssl_eprintln,
                         "Failed to establish connection with peer. "
                         "SSL error = %d.",
                         ioerr);
            break;
        }
    }

    return n;
}

int SBUF2_FUNC(sslio_write)(SBUF2 *sb, const char *cc, int len)
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
        switch (ioerr) {
        case SSL_ERROR_WANT_READ:
            sb->protocolerr = 0;
            errno = EAGAIN;
            wantwrite = 0;
            goto rewrite;
        case SSL_ERROR_WANT_WRITE:
            sb->protocolerr = 0;
            errno = EAGAIN;
            wantwrite = 1;
            goto rewrite;
        case SSL_ERROR_ZERO_RETURN:
            /* Peer has done a clean shutdown. */
            SSL_shutdown(sb->ssl);
            SSL_free(sb->ssl);
            sb->ssl = NULL;
            if (sb->cert) {
                X509_free(sb->cert);
                sb->cert = NULL;
            }
            break;
        case SSL_ERROR_SYSCALL:
            sb->protocolerr = 0;
            if (n == 0) {
                ssl_sfeprint(sb->sslerr, sizeof(sb->sslerr), my_ssl_eprintln,
                             "Unexpected EOF observed.");
                errno = ECONNRESET;
            } else {
                ssl_sfeprint(sb->sslerr, sizeof(sb->sslerr), my_ssl_eprintln,
                             "IO error. errno %d.", errno);
            }
        case SSL_ERROR_SSL:
            errno = EIO;
            sb->protocolerr = 1;
            ssl_sfliberrprint(sb->sslerr, sizeof(sb->sslerr), my_ssl_eprintln,
                              "A failure in SSL library occured");
            break;
        default:
            errno = EIO;
            sb->protocolerr = 1;
            ssl_sfeprint(sb->sslerr, sizeof(sb->sslerr), my_ssl_eprintln,
                         "Failed to establish connection with peer. "
                         "SSL error = %d.",
                         ioerr);
            break;
        }
    }

    return n;
}

int SBUF2_FUNC(sslio_close)(SBUF2 *sb, int reuse)
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

    if (sb->cert) {
        X509_free(sb->cert);
        sb->cert = NULL;
    }

    SSL_free(sb->ssl);
    sb->ssl = NULL;
    return rc;
}
