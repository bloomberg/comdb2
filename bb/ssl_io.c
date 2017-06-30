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

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>

/* extra OpenSSL headers */
#include <openssl/asn1.h>
#include <openssl/x509v3.h>

/* I need the get_origin_mach_by_fd() routine here
   to support SSL verify-name mode. */

#if SBUF2_SERVER
#  include <lockmacro.h> /* LOCK & UNLOCK */
#  include <plhash.h> /* hash_t */
static pthread_mutex_t peer_lk = PTHREAD_MUTEX_INITIALIZER;
static hash_t *peer_hash = NULL;
struct peer_info {
    /* Key (must come first in struct) */
    struct in_addr addr;
    short family;
    /* (don't forget to bzero the key area as the member fields may not
     * be aligned) */

    /* Data */
    char *host;
};
#else
/* Don't cache host info in client mode. */
#  ifdef LOCK
#    undef LOCK
#  endif
#  define LOCK(arg)
#  ifdef UNLOCK
#    undef UNLOCK
#  endif
#  define UNLOCK(arg)
#endif

/* Given an fd, work out which machine the connection is from.
   Cache hostname info only in server mode. */
char *SBUF2_FUNC(get_origin_mach_by_buf)(SBUF2 *sb)
{
    int fd;
    char *funcname;
    struct sockaddr_in peeraddr;
    int len = sizeof(peeraddr);
    char *host;
#if SBUF2_SERVER
    struct peer_info *info;
    struct peer_info key;
#else
    void *info = NULL;
#endif

    if (sb == NULL)
        return "???";

    fd = sb->fd;

    if (fd == -1)
        return "???";

    bzero(&peeraddr, sizeof(peeraddr));
    if (getpeername(fd, (struct sockaddr *)&peeraddr, &len) < 0) {
        loge("%s:getpeername failed fd %d: %d %s\n",
               __func__, fd, errno, strerror(errno));
        return "???";
    }

#if SBUF2_SERVER 
    bzero(&key, offsetof(struct peer_info, host));
    memcpy(&key.addr, &peeraddr.sin_addr, sizeof(key.addr));
    key.family = peeraddr.sin_family;
#endif

    LOCK(&peer_lk)
    {
#if SBUF2_SERVER
        if (!peer_hash) {
            peer_hash = hash_init(offsetof(struct peer_info, host));
            if (!peer_hash) {
                loge("%s:hash_init failed\n", __func__);
                errUNLOCK(&peer_lk);
                return "???";
            }
        }

        info = hash_find(peer_hash, &key);
#endif
        if (!info) {
            /* Do a slow lookup of this internet address in the host database
             * to get a hostname, and then search the bigsnd node list to
             * map this to a node number. */
            struct hostent *hp = NULL, rslt;
            char hnm[256] = {0};
            char *h_name = NULL;
            int node, rc;
            int error_num = 0;
            int goodrc = 0;

#ifdef _LINUX_SOURCE
            funcname = "getnameinfo";
            rc = getnameinfo((struct sockaddr *)&peeraddr, sizeof(peeraddr),
                             hnm, sizeof(hnm), NULL, 0, 0);

            if (0 == rc) {
                goodrc = 1;
                h_name = hnm;
            } else {
                error_num = errno;
            }
#else
            funcname = "getipnodebyaddr";
            hp = getipnodebyaddr(&peeraddr.sin_addr, sizeof(peeraddr.sin_addr),
                                 peeraddr.sin_family, &error_num);
            if (hp) {
                goodrc = 1;
                h_name = hp->h_name;
            }
#endif

            if (0 == goodrc) {
                char addrstr[64] = "";
                inet_ntop(peeraddr.sin_family, &peeraddr.sin_addr, addrstr,
                          sizeof(addrstr));
                loge("%s:%s failed fd %d (%s): error_num %d",
                       __func__, funcname, fd, addrstr, error_num);
                switch (error_num) {
                case HOST_NOT_FOUND:
                    loge(" HOST_NOT_FOUND\n");
                    break;
                case NO_DATA:
                    loge(" NO_DATA\n");
                    break;
                case NO_RECOVERY:
                    loge(" NO_RECOVERY\n");
                    break;
                case TRY_AGAIN:
                    loge(" TRY_AGAIN\n");
                    break;
                default:
                    loge(" ???\n");
                    break;
                }
                host = strdup(addrstr);
            } else {
                host = strdup(h_name);
            }

#if SBUF2_SERVER
            info = calloc(1, sizeof(struct peer_info));
            if (!info) {
                errUNLOCK(&peer_lk);
                loge("%s: out of memory\n", __func__);
                free(host);
                host = NULL;
                return "???";
            }

            memcpy(info, &key, sizeof(key));
            info->host = host;
            if (hash_add(peer_hash, info) != 0) {
                errUNLOCK(&peer_lk);
                loge("%s: hash_add failed\n", __func__);
                free(info);
                return host;
            }
#endif
        }
    }
    UNLOCK(&peer_lk);

#if SBUF2_SERVER
    return (info != NULL) ? info->host : "???";
#else
    return (host != NULL) ? host : "???";
#endif
}

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
    for (ii = 0; ii != len; ++ii) {
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
        rc = 1;
    }

    sk_GENERAL_NAME_pop_free(peersan, GENERAL_NAME_free);
    return rc;
}

static int ssl_verify_cn(const char *hostname, const X509 *cert)
{
    int cnindx;
    X509_NAME  *certname;
    X509_NAME_ENTRY *cnentry;
    ASN1_STRING *cnasn1;
    const char *cnstr;

    certname = X509_get_subject_name((X509 *)cert);
    if (certname == NULL)
        return 1;

    cnindx = X509_NAME_get_index_by_NID(certname, NID_commonName, -1);
    if (cnindx < 0)
        return 1;

    cnentry = X509_NAME_get_entry(certname, cnindx);
    if (cnentry == NULL)
        return 1;

    cnasn1 = X509_NAME_ENTRY_get_data(cnentry);
    if (cnasn1 == NULL)
        return 1;

    cnstr = (const char *)ASN1_STRING_get0_data(cnasn1);
    /* CVE-2009-4034 */
    if (ASN1_STRING_length(cnasn1) != strlen(cnstr))
        return 1;

    return hostname_wildcard_match(hostname, cnstr);
}

static int ssl_verify_ca(SBUF2 *sb, char *err, size_t n)
{
    const char *peerhost;
    int rc;

    peerhost = get_origin_mach_by_buf(sb);

    if (strcmp(peerhost, "???") == 0) {
        ssl_sfeprint(err, n, my_ssl_eprintln,
                     "Could not obtain peer host name.");
        return 1;
    }

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

int SBUF2_FUNC(sslio_verify)(SBUF2 *sb, ssl_mode mode, char *err, size_t n)
{
    int rc = 0;
    if (sb->ssl != NULL && mode >= SSL_VERIFY_CA) {
        sb->cert = SSL_get_peer_certificate(sb->ssl);
        if (sb->cert == NULL) {
            ssl_sfeprint(err, n, my_ssl_eprintln,
                         "Could not get peer certificate");
            rc = EIO;
        } else if (mode == SSL_VERIFY_HOSTNAME
                   && ssl_verify_ca(sb, err, n) != 0) {
            /* set rc to error out. */
            rc = EACCES;
            ssl_sfeprint(err, n, my_ssl_eprintln,
                         "Certificate does not match host name.");
        }
    }
    return rc;
}

static int sslio_accept_or_connect(SBUF2 *sb,
                                   SSL_CTX *ctx, int (*SSL_func)(SSL *),
                                   ssl_mode verify, char *err, size_t n,
                                   SSL_SESSION *sess, int *unrecoverable)
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
    } else if (sslio_verify(sb, verify, err, n) != 0) {
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

    if (rc != 1) {
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

int SBUF2_FUNC(sslio_accept)(SBUF2 *sb, SSL_CTX *ctx,
                           ssl_mode mode, char *err, size_t n)
{
    int dummy;
    return sslio_accept_or_connect(sb, ctx, SSL_accept,
                                   mode, err, n, NULL, &dummy);
}

#if SBUF2_SERVER
int SBUF2_FUNC(sslio_connect)(SBUF2 *sb, SSL_CTX *ctx,
                            ssl_mode mode, char *err, size_t n)
{
    int dummy;
    return sslio_accept_or_connect(sb, ctx, SSL_connect,
                                   mode, err, n, NULL, &dummy);
}
#else
int SBUF2_FUNC(sslio_connect)(SBUF2 *sb, SSL_CTX *ctx,
                            ssl_mode mode, char *err, size_t n,
                            SSL_SESSION *sess, int *unrecoverable)
{
    return sslio_accept_or_connect(sb, ctx, SSL_connect,
                                   mode, err, n, sess, unrecoverable);
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
