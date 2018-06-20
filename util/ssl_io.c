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

/* extra OpenSSL headers */
#include <openssl/asn1.h>
#include <openssl/x509v3.h>

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

/* This is rougly in line with RFC6125, Section 6.4.3.
   (https://tools.ietf.org/html/rfc6125#section-6.4.3) */
static int hostname_wildcard_match(const char *s, const char *p)
{
    const char *asterisk = NULL;
    const char *ts;
    const char *dotasterisk;

    /* Use optimized libc function first.
       If no exact match, we use wildcard matching
       and accept the overhead. */
    if (strcasecmp(s, p) == 0)
        return 0;

    /* RFC6125 Rule 1 */
    dotasterisk = strstr(p, ".*");
    if (dotasterisk != NULL)
        return 1;

    /* RFC6125 Rule 3 */
    if (strlen(p) < 3 ||
            p[0] != '*' ||
            p[1] != '.')
        return 1;

    ts = s;
    while (*s) {
        if (tolower(*p) == tolower(*s)) {
            ++s;
            ++p;
        } else if (*p == '*') {
            asterisk = p++;
            ts = s;
        } else if (asterisk && *s != '.') {
            p = asterisk + 1;
            s = ++ts;
        } else {
            return 1;
        }
    }
    for (; *p == '*'; ++p)
        ;

    /* No wildcard match */
    return (*p != 0);
}

#if (OPENSSL_VERSION_NUMBER < 0x10100000L) || defined(LIBRESSL_VERSION_NUMBER)
#    define ASN1_STRING_get0_data ASN1_STRING_data
#endif

/* 0: okay. -1: no san. 1: no match. */
static int ssl_verify_san(const char *hostname, const X509 *cert)
{
    STACK_OF(GENERAL_NAME) *peersan;
    const GENERAL_NAME *name;
    const char *dnsname;
    int rc, ii, len;

    peersan = (STACK_OF(GENERAL_NAME) *)
        X509_get_ext_d2i((X509 *)cert, NID_subject_alt_name, NULL, NULL);
    if (peersan == NULL)
        return -1;

    len = sk_GENERAL_NAME_num(peersan);
    for (ii = 0, rc = 1; ii != len; ++ii) {
        name = sk_GENERAL_NAME_value(peersan, ii);
        if (name->type != GEN_DNS)
            continue;
        dnsname = (const char *)ASN1_STRING_get0_data(name->d.dNSName);

        /* CVE-2009-4034 */
        if (ASN1_STRING_length(name->d.dNSName) != strlen(dnsname)) {
            rc = 1;
            break;
        }

        if (hostname_wildcard_match(hostname, dnsname) == 0) {
            rc = 0;
            break;
        }
    }

    sk_GENERAL_NAME_pop_free(peersan, GENERAL_NAME_free);
    return rc;
}

static int ssl_x509_get_attr(const X509 *cert, int nid, char *out, size_t len)
{
    int fldindx;
    X509_NAME  *certname;
    X509_NAME_ENTRY *fld;
    ASN1_STRING *fldasn1;
    const char *fldstr;

    /* Fast return if nid is undefined */
    if (nid == NID_undef)
        return EINVAL;

    certname = X509_get_subject_name((X509 *)cert);
    if (certname == NULL)
        return EINVAL;

    fldindx = X509_NAME_get_index_by_NID(certname, nid, -1);
    if (fldindx < 0)
        return EINVAL;

    fld = X509_NAME_get_entry(certname, fldindx);
    if (fld == NULL)
        return EINVAL;

    fldasn1 = X509_NAME_ENTRY_get_data(fld);
    if (fldasn1 == NULL)
        return EINVAL;

    fldstr = (const char *)ASN1_STRING_get0_data(fldasn1);
    /* CVE-2009-4034 */
    if (ASN1_STRING_length(fldasn1) != strlen(fldstr))
        return EINVAL;

    strncpy(out, fldstr, len);
    return 0;
}

int SBUF2_FUNC(sslio_x509_attr)(SBUF2 *sb, int nid, char *out, size_t len)
{
    if (sb == NULL || sb->cert == NULL)
        return EINVAL;
    return ssl_x509_get_attr(sb->cert, nid, out, len);
}

static int ssl_verify_cn(const char *hostname, const X509 *cert)
{
    int rc;
    /* RFC 2181 */
    char cn[256];
    rc = ssl_x509_get_attr(cert, NID_commonName, cn, sizeof(cn));
    if (rc != 0)
        return 1;
    cn[255] = 0;
    return hostname_wildcard_match(hostname, cn);
}

static int ssl_verify_ca(SBUF2 *sb, char *err, size_t n)
{
    /*
    ** 1) Perform a reverse DNS lookup to get the hostname
    **    associated with the source address.
    ** 2) Perform a forward DNS lookup to get a list of addresses
    **    associated with the hostname.
    ** 3) If the source address is in the list, proceed;
    **    otherwise, return 1 immediately.
    ** 4) Perform SAN/CN validation.
    **
    ** The forward DNS lookup is necessary in case an attacker is
    ** in control of reverse DNS for the source IP.
    */
    const char *peerhost;
    struct sockaddr_in peeraddr;
    struct in_addr *peer_in_addr, **p_fwd_in_addr;
    socklen_t len = sizeof(struct sockaddr_in);
    int rc, found_addr, herr;
    char buf[8192];
    struct hostent hostbuf, *hp = NULL;

    /* Reverse lookup the hostname */
    peerhost = get_origin_mach_by_buf(sb);

    if (strcmp(peerhost, "???") == 0) {
        ssl_sfeprint(err, n, my_ssl_eprintln,
                     "Could not obtain peer host name.");
        return 1;
    }

    /* Should always succeed as get_origin_mach_by_buf()
       returns a valid hostname. */
    getpeername(sb->fd, (struct sockaddr *)&peeraddr, &len);

/* Forward lookup the IPs */

#if defined(__APPLE__)
    hp = gethostbyname(peerhost);
#elif defined(_LINUX_SOURCE)
    gethostbyname_r(peerhost, &hostbuf, buf, sizeof(buf), &hp, &herr);
#elif defined(_SUN_SOURCE)
    hp = gethostbyname_r(peerhost, &hostbuf, buf, sizeof(buf), &herr);
#else
    hp = gethostbyname(peerhost);
#endif

    if (hp == NULL) {
        ssl_sfeprint(err, n, my_ssl_eprintln,
                     "Failed to perform forward DNS lookup.");
        return 1;
    }

    /* Find the source address in the address list returned
       by the forward DNS lookup. */
    for (found_addr = 0, peer_in_addr = &peeraddr.sin_addr,
        p_fwd_in_addr = (struct in_addr **)hp->h_addr_list;
         *p_fwd_in_addr != NULL; ++p_fwd_in_addr) {
        if (peer_in_addr->s_addr == (*p_fwd_in_addr)->s_addr) {
            found_addr = 1;
            break;
        }
    }

    /* Suspicious PTR record. Reject it. */
    if (!found_addr)
        return 1;

    /* Trust localhost */
    if (strcasecmp(peerhost, "localhost") == 0 ||
            strcasecmp(peerhost, "localhost.localdomain") == 0)
        return 0;

    /* Per RFC 6125, If SANs are presented, they must be used and
       the Comman Name must be ignored. */
    rc = ssl_verify_san(peerhost, sb->cert);
    if (rc == -1)
        rc = ssl_verify_cn(peerhost, sb->cert);

    return rc;
}

static int dbname_wildcard_match(const char *s, const char *p)
{
    const char *asterisk = NULL;
    const char *ts = s;
    const char *pc = p;

    /* A pattern can't be all wildcard characters */
    for (; *pc && (*pc == '?' || *pc == '*'); ++pc)
        ;
    if (*pc == 0)
        return 1;

    while (*s) {
        if ((*p == '?') || (tolower(*p) == tolower(*s))) {
            ++s;
            ++p;
        } else if (*p == '*') {
            asterisk = p++;
            ts = s;
        } else if (asterisk) {
            p = asterisk + 1;
            s = ++ts;
        } else {
            return 1;
        }
    }
    for (; *p == '*'; ++p)
        ;
    return (*p == 0) ? 0 : 1;
}

static int ssl_verify_dbname(SBUF2 *sb, const char *dbname, int nid)
{
    size_t sz = strlen(dbname) + 1;
    char *dbname_in_cert = alloca(sz);
    int rc = sslio_x509_attr(sb, nid, dbname_in_cert, sz);

    if (rc != 0)
        return rc;
    if (strncasecmp(dbname_in_cert, dbname, sz) == 0)
        return 0;
    dbname_in_cert[sz - 1] = 0;
    return dbname_wildcard_match(dbname, dbname_in_cert);
}

static int ssl_verify(SBUF2 *sb, ssl_mode mode, const char *dbname, int nid,
                      char *err, size_t n)
{
    int rc = 0;
    if (sb->ssl != NULL && mode >= SSL_VERIFY_CA) {
        sb->cert = SSL_get_peer_certificate(sb->ssl);
        if (sb->cert == NULL) {
            ssl_sfeprint(err, n, my_ssl_eprintln,
                         "Could not get peer certificate.");
            rc = EIO;
        } else if (mode >= SSL_VERIFY_HOSTNAME &&
                   ssl_verify_ca(sb, err, n) != 0) {
            /* set rc to error out. */
            rc = EACCES;
            ssl_sfeprint(err, n, my_ssl_eprintln,
                         "Certificate does not match host name.");
        } else if (mode >= SSL_VERIFY_DBNAME &&
                   ssl_verify_dbname(sb, dbname, nid) != 0) {
            rc = EACCES;
            ssl_sfeprint(err, n, my_ssl_eprintln,
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
                                   const char *dbname, int nid, char *err,
                                   size_t n, SSL_SESSION *sess,
                                   int *unrecoverable,
                                   int close_on_verify_error)
{
    int rc, ioerr, fd, flags;

    *unrecoverable = 1;

    /* If SSL does not exist, return an error. */
    if (ctx == NULL) {
        ssl_sfeprint(err, n, my_ssl_eprintln, "SSL context does not exist.");
        return EPERM;
    }

    if (sb->ssl != NULL) {
        ssl_sfeprint(err, n, my_ssl_eprintln,
                     "SSL connection has been established already.");
        return EPERM;
    }

    /* Create an SSL connection. */
    sb->ssl = SSL_new(ctx);
    if (sb->ssl == NULL) {
        ssl_sfliberrprint(err, n, my_ssl_eprintln,
                          "Failed to create SSL connection");
        rc = ERR_get_error();
        goto error;
    }

    /* Set fd. */
    fd = sbuf2fileno(sb);
    if ((flags = fcntl(fd, F_GETFL, 0)) < 0 ||
        (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0)) {
        ssl_sfeprint(err, n, my_ssl_eprintln,
                     "fcntl: (%d) %s", errno, strerror(errno));
        rc = -1;
        goto error;
    }
    rc = SSL_set_fd(sb->ssl, fd);
    if (rc != 1) {
        ssl_sfliberrprint(err, n, my_ssl_eprintln,
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
            *unrecoverable = 0;
            break;
        case SSL_ERROR_WANT_WRITE: /* Renegotiate */
            rc = sslio_pollout(sb);
            if (rc > 0)
                goto re_accept_or_connect;
            *unrecoverable = 0;
            break;
        case SSL_ERROR_SYSCALL:
            *unrecoverable = 0;
            if (rc == 0) {
                ssl_sfeprint(err, n, my_ssl_eprintln,
                             "Unexpected EOF observed.");
                errno = ECONNRESET;
            } else {
                ssl_sfeprint(err, n, my_ssl_eprintln,
                             "IO error. errno %d.", errno);
            }
            break;
        case SSL_ERROR_SSL:
            errno = EIO;
            ssl_sfliberrprint(err, n, my_ssl_eprintln,
                              "A failure in SSL library occured");
            break;
        default:
            errno = EIO;
            ssl_sfeprint(err, n, my_ssl_eprintln,
                         "Failed to establish connection with peer. "
                         "SSL error = %d.", ioerr);
            break;
        }
    } else if (ssl_verify(sb, verify, dbname, nid, err, n) != 0) {
        rc = EACCES;
    } else {
        *unrecoverable = 0;
    }
    /* Put blocking back. */
    if (fcntl(fd, F_SETFL, flags) < 0) {
        ssl_sfeprint(err, n, my_ssl_eprintln,
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
                             const char *dbname, int nid, char *err, size_t n,
                             int close_on_verify_error)
{
    int dummy;

    return sslio_accept_or_connect(sb, ctx, SSL_accept, mode, dbname, nid, err,
                                   n, NULL, &dummy, close_on_verify_error);
}

#if SBUF2_SERVER
int SBUF2_FUNC(sslio_connect)(SBUF2 *sb, SSL_CTX *ctx, ssl_mode mode,
                              const char *dbname, int nid, char *err, size_t n,
                              int close_on_verify_error)
{
    int dummy;
    return sslio_accept_or_connect(sb, ctx, SSL_connect, mode, dbname, nid, err,
                                   n, NULL, &dummy, close_on_verify_error);
}
#else
int SBUF2_FUNC(sslio_connect)(SBUF2 *sb, SSL_CTX *ctx, ssl_mode mode,
                              const char *dbname, int nid, char *err, size_t n,
                              SSL_SESSION *sess, int *unrecoverable)
{
    return sslio_accept_or_connect(sb, ctx, SSL_connect, mode, dbname, nid, err,
                                   n, sess, unrecoverable, 1);
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
            errno = EAGAIN;
            wantread = 1;
            goto reread;
        case SSL_ERROR_WANT_WRITE:
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
            if (n == 0)
                errno = ECONNRESET;
            break;
        case SSL_ERROR_SSL:
            PRINT_SSL_ERRSTR_MT(my_ssl_eprintln,
                                "A failure in SSL library occured");
            /* Fall through */
        default:
            errno = EIO;
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
            errno = EAGAIN;
            wantwrite = 0;
            goto rewrite;
        case SSL_ERROR_WANT_WRITE:
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
            if (n == 0)
                errno = ECONNRESET;
            break;
        case SSL_ERROR_SSL:
            PRINT_SSL_ERRSTR_MT(my_ssl_eprintln,
                                "A failure in SSL library occured");
            /* Fall through */
        default:
            errno = EIO;
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
